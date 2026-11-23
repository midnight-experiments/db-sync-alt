//! The service's two HTTP routes, `GET /health` and `GET /status`, on a plain
//! TCP listener, and the probe of the `health` command. Every answer comes
//! from the documents last rendered into a watch channel: a request never
//! reaches the database. Each connection carries one request and is closed
//! after the answer; at most 16 are open at once, a request must arrive
//! complete within 2 seconds and its head may not exceed 8 KiB.

use std::{future::Future, sync::Arc, time::Duration};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Semaphore, watch},
    task::JoinSet,
    time::{Instant, timeout, timeout_at},
};

use crate::status::Documents;

/// Connections answered at once; one more is closed on accept.
pub const MAX_CONNECTIONS: usize = 16;
/// A request not complete within this time is dropped without an answer.
pub const REQUEST_DEADLINE: Duration = Duration::from_secs(2);
/// The largest request line and header block.
pub const MAX_REQUEST_HEAD: usize = 8 * 1024;
/// The largest answer the probe reads.
const MAX_ANSWER: usize = 1024 * 1024;

/// Serves the two routes until `stop` resolves. Errors of one connection
/// end only that connection.
pub async fn serve(
    listener: TcpListener,
    documents: watch::Receiver<Documents>,
    stop: impl Future<Output = ()>,
) {
    let permits = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let mut connections = JoinSet::new();
    tokio::pin!(stop);
    loop {
        tokio::select! {
            () = &mut stop => break,
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
            accepted = listener.accept() => {
                let stream = match accepted {
                    Ok((stream, _)) => stream,
                    // Out of descriptors or a connection reset before accept:
                    // try again shortly.
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                };
                match Arc::clone(&permits).try_acquire_owned() {
                    Ok(permit) => {
                        let documents = documents.clone();
                        connections.spawn(async move {
                            answer(stream, &documents).await;
                            drop(permit);
                        });
                    }
                    Err(_) => drop(stream),
                }
            }
        }
    }
    connections.shutdown().await;
}

enum Head {
    Complete(Vec<u8>),
    TooLarge,
    Closed,
}

/// Reads up to the end of the header block.
async fn read_head(stream: &mut TcpStream) -> std::io::Result<Head> {
    let mut head = Vec::with_capacity(1024);
    let mut buffer = [0_u8; 1024];
    loop {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            return Ok(Head::Closed);
        }
        let start = head.len().saturating_sub(3);
        head.extend_from_slice(&buffer[..read]);
        if let Some(end) = head[start..]
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
        {
            let end = start + end + 4;
            if end > MAX_REQUEST_HEAD {
                return Ok(Head::TooLarge);
            }
            head.truncate(end);
            return Ok(Head::Complete(head));
        }
        if head.len() > MAX_REQUEST_HEAD {
            return Ok(Head::TooLarge);
        }
    }
}

fn response(code: u16, body: &str) -> Vec<u8> {
    let reason = match code {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        431 => "Request Header Fields Too Large",
        _ => "Service Unavailable",
    };
    let allow = if code == 405 { "Allow: GET\r\n" } else { "" };
    let mut bytes = format!(
        "HTTP/1.1 {code} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
         Cache-Control: no-store\r\nConnection: close\r\n{allow}\r\n",
        body.len()
    )
    .into_bytes();
    bytes.extend_from_slice(body.as_bytes());
    bytes
}

/// The answer to one request head.
fn route(head: &[u8], documents: &watch::Receiver<Documents>) -> Vec<u8> {
    let line = head
        .split(|byte| *byte == b'\n')
        .next()
        .and_then(|line| std::str::from_utf8(line).ok())
        .map(|line| line.trim_end_matches('\r'));
    let parts = line.map(|line| line.split(' ').collect::<Vec<_>>());
    let Some([method, target, version]) = parts.as_deref() else {
        return response(400, r#"{"error_code":"invalid_http_request"}"#);
    };
    if !version.starts_with("HTTP/1.") || method.is_empty() || target.is_empty() {
        return response(400, r#"{"error_code":"invalid_http_request"}"#);
    }
    if !matches!(*target, "/health" | "/status") {
        return response(404, r#"{"error_code":"not_found"}"#);
    }
    if *method != "GET" {
        return response(405, r#"{"error_code":"method_not_allowed"}"#);
    }
    let documents = documents.borrow().clone();
    let code = if *target == "/health" && !documents.healthy {
        503
    } else {
        200
    };
    response(code, &documents.body)
}

/// One connection: read the request within the deadline, answer, close.
async fn answer(mut stream: TcpStream, documents: &watch::Receiver<Documents>) {
    let deadline = Instant::now() + REQUEST_DEADLINE;
    let reply = match timeout_at(deadline, read_head(&mut stream)).await {
        Ok(Ok(Head::Complete(head))) => route(&head, documents),
        Ok(Ok(Head::TooLarge)) => response(431, r#"{"error_code":"request_head_too_large"}"#),
        Ok(Ok(Head::Closed) | Err(_)) | Err(_) => return,
    };
    if timeout(REQUEST_DEADLINE, stream.write_all(&reply))
        .await
        .is_err()
    {
        return;
    }
    // Close the sending side, then read what the client still sends for a
    // short while, so that unread bytes do not turn the close into a reset
    // that discards the answer.
    let _ = stream.shutdown().await;
    let mut sink = [0_u8; 1024];
    let mut drained = 0;
    let _ = timeout(Duration::from_millis(500), async {
        while drained < 64 * 1024 {
            match stream.read(&mut sink).await {
                Ok(0) | Err(_) => break,
                Ok(read) => drained += read,
            }
        }
    })
    .await;
}

/// Where the `health` command asks: `http://HOST[:PORT]PATH`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Target {
    pub host: String,
    pub port: u16,
    pub path: String,
}

impl Target {
    /// Accepts `http://` URLs with a host name, an IPv4 address or a
    /// bracketed IPv6 address, an optional port (default 80) and a path.
    pub fn parse(url: &str) -> Result<Self, &'static str> {
        let rest = url.strip_prefix("http://").ok_or("invalid_health_url")?;
        let (authority, path) = match rest.find('/') {
            Some(slash) => (&rest[..slash], &rest[slash..]),
            None => (rest, "/"),
        };
        if authority.is_empty()
            || authority.contains('@')
            || path.bytes().any(|byte| byte <= b' ' || byte == 0x7f)
        {
            return Err("invalid_health_url");
        }
        let (host, port) = if let Some(bracketed) = authority.strip_prefix('[') {
            let (host, rest) = bracketed.split_once(']').ok_or("invalid_health_url")?;
            (host, rest.strip_prefix(':'))
        } else {
            match authority.rsplit_once(':') {
                Some((host, port)) => (host, Some(port)),
                None => (authority, None),
            }
        };
        let port = match port {
            None => 80,
            Some(port) => port.parse().map_err(|_| "invalid_health_url")?,
        };
        if host.is_empty() || port == 0 {
            return Err("invalid_health_url");
        }
        Ok(Self {
            host: host.to_owned(),
            port,
            path: path.to_owned(),
        })
    }
}

/// Why the probe has no answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProbeError {
    Unreachable,
    Timeout,
    InvalidAnswer,
}

impl ProbeError {
    pub fn code(self) -> &'static str {
        match self {
            Self::Unreachable => "service_unreachable",
            Self::Timeout => "service_timeout",
            Self::InvalidAnswer => "invalid_http_answer",
        }
    }
}

/// Sends one `GET` to `target` and returns the HTTP status of the answer,
/// all within `deadline`.
pub async fn probe(target: &Target, deadline: Duration) -> Result<u16, ProbeError> {
    let exchange = async {
        let mut stream = TcpStream::connect((target.host.as_str(), target.port))
            .await
            .map_err(|_| ProbeError::Unreachable)?;
        let host = if target.host.contains(':') {
            format!("[{}]:{}", target.host, target.port)
        } else {
            format!("{}:{}", target.host, target.port)
        };
        let request = format!(
            "GET {} HTTP/1.1\r\nHost: {host}\r\nUser-Agent: db-sync-alt\r\nConnection: close\r\n\r\n",
            target.path
        );
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|_| ProbeError::Unreachable)?;
        let mut answer = Vec::new();
        let mut buffer = [0_u8; 4096];
        loop {
            let read = stream
                .read(&mut buffer)
                .await
                .map_err(|_| ProbeError::InvalidAnswer)?;
            answer.extend_from_slice(&buffer[..read]);
            if read == 0 || answer.contains(&b'\n') || answer.len() > MAX_ANSWER {
                break;
            }
        }
        let line = answer
            .split(|byte| *byte == b'\n')
            .next()
            .and_then(|line| std::str::from_utf8(line).ok())
            .ok_or(ProbeError::InvalidAnswer)?;
        let mut parts = line.split(' ');
        match (parts.next(), parts.next()) {
            (Some(version), Some(code)) if version.starts_with("HTTP/1.") && code.len() == 3 => {
                code.parse().map_err(|_| ProbeError::InvalidAnswer)
            }
            _ => Err(ProbeError::InvalidAnswer),
        }
    };
    timeout(deadline, exchange)
        .await
        .unwrap_or(Err(ProbeError::Timeout))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_urls_are_parsed_or_refused() {
        let target = Target::parse("http://127.0.0.1:18080/health").unwrap();
        assert_eq!(
            (target.host.as_str(), target.port, target.path.as_str()),
            ("127.0.0.1", 18080, "/health")
        );
        let target = Target::parse("http://[::1]:9/status").unwrap();
        assert_eq!((target.host.as_str(), target.port), ("::1", 9));
        let target = Target::parse("http://service").unwrap();
        assert_eq!((target.port, target.path.as_str()), (80, "/"));
        for url in [
            "https://127.0.0.1/health",
            "http://",
            "http://user@host/health",
            "http://host:0/health",
            "http://host:99999/health",
            "http://host:port/health",
            "http://host/he alth",
            "127.0.0.1:18080/health",
        ] {
            assert!(Target::parse(url).is_err(), "{url}");
        }
    }
}
