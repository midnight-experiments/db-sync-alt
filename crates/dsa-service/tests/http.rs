//! The two HTTP routes against real sockets: routes, refusals, the request
//! deadline, the connection limit, the head limit, steady resources over
//! many requests, and the probe of the `health` command. DSA_HTTP_REQUESTS
//! sets the number of sequential requests (default 200).

use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use dsa_service::{
    http::{MAX_CONNECTIONS, ProbeError, Target, probe, serve},
    status::Documents,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{oneshot, watch},
    task::JoinHandle,
};

struct Server {
    address: SocketAddr,
    documents: watch::Sender<Documents>,
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

impl Server {
    async fn start(healthy: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (documents, receiver) = watch::channel(documents(healthy, "first"));
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(serve(listener, receiver, async {
            let _ = stopped.await;
        }));
        Self {
            address,
            documents,
            stop: Some(stop),
            task,
        }
    }

    async fn stop(mut self) {
        let _ = self.stop.take().unwrap().send(());
        self.task.await.unwrap();
    }
}

fn documents(healthy: bool, marker: &str) -> Documents {
    Documents {
        healthy,
        body: format!(r#"{{"healthy":{healthy},"marker":"{marker}"}}"#).into(),
    }
}

struct Answer {
    status: Option<u16>,
    headers: String,
    body: String,
}

/// Sends `raw` and reads until the server closes.
async fn exchange(address: SocketAddr, raw: &[u8]) -> Answer {
    let mut stream = TcpStream::connect(address).await.unwrap();
    // A connection the server closes at once may refuse the write.
    let _ = stream.write_all(raw).await;
    let mut bytes = Vec::new();
    let _ = stream.read_to_end(&mut bytes).await;
    let text = String::from_utf8_lossy(&bytes).into_owned();
    let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
    Answer {
        status: head
            .strip_prefix("HTTP/1.1 ")
            .and_then(|rest| rest.get(..3))
            .and_then(|code| code.parse().ok()),
        headers: head.to_owned(),
        body: body.to_owned(),
    }
}

async fn get(address: SocketAddr, path: &str) -> Answer {
    exchange(
        address,
        format!("GET {path} HTTP/1.1\r\nHost: test\r\n\r\n").as_bytes(),
    )
    .await
}

#[tokio::test]
async fn both_routes_answer_from_the_last_documents() {
    let server = Server::start(true).await;
    let status = get(server.address, "/status").await;
    assert_eq!(status.status, Some(200));
    assert_eq!(status.body, r#"{"healthy":true,"marker":"first"}"#);
    for header in [
        "Content-Type: application/json",
        "Connection: close",
        "Cache-Control: no-store",
    ] {
        assert!(status.headers.contains(header), "{}", status.headers);
    }
    assert!(
        status
            .headers
            .contains(&format!("Content-Length: {}", status.body.len()))
    );
    assert_eq!(get(server.address, "/health").await.status, Some(200));
    server.documents.send(documents(false, "second")).unwrap();
    let health = get(server.address, "/health").await;
    assert_eq!(health.status, Some(503));
    assert_eq!(health.body, r#"{"healthy":false,"marker":"second"}"#);
    let status = get(server.address, "/status").await;
    assert_eq!(status.status, Some(200));
    assert_eq!(status.body, health.body);
    server.stop().await;
}

#[tokio::test]
async fn other_paths_and_methods_are_refused() {
    let server = Server::start(true).await;
    for path in ["/", "/status/", "/status?full=1", "/healthz", "/STATUS"] {
        let answer = get(server.address, path).await;
        assert_eq!(answer.status, Some(404), "{path}");
        assert!(answer.body.contains("not_found"), "{path}");
    }
    for method in ["POST", "HEAD", "PUT", "DELETE", "OPTIONS"] {
        let answer = exchange(
            server.address,
            format!("{method} /status HTTP/1.1\r\nHost: test\r\n\r\n").as_bytes(),
        )
        .await;
        assert_eq!(answer.status, Some(405), "{method}");
        assert!(answer.headers.contains("Allow: GET"), "{method}");
    }
    for raw in [
        "GET /status\r\n\r\n",
        "GET /status HTTP/1.1 extra\r\n\r\n",
        "GET /status SMTP/1.0\r\n\r\n",
        "\r\n\r\n",
    ] {
        let answer = exchange(server.address, raw.as_bytes()).await;
        assert_eq!(answer.status, Some(400), "{raw:?}");
    }
    server.stop().await;
}

#[tokio::test]
async fn a_slow_client_is_dropped_at_the_deadline_while_others_are_answered() {
    let server = Server::start(true).await;
    let address = server.address;
    let slow = tokio::spawn(async move {
        let started = Instant::now();
        let mut stream = TcpStream::connect(address).await.unwrap();
        let mut received = Vec::new();
        for byte in b"GET /status HTTP/1.1\r\nHost: test\r\n\r\n" {
            if stream.write_all(&[*byte]).await.is_err() {
                break;
            }
            let mut buffer = [0_u8; 64];
            match tokio::time::timeout(Duration::from_secs(1), stream.read(&mut buffer)).await {
                Ok(Ok(0) | Err(_)) => break,
                Ok(Ok(read)) => received.extend_from_slice(&buffer[..read]),
                Err(_) => {}
            }
        }
        (started.elapsed(), received)
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    let started = Instant::now();
    let answer = get(address, "/status").await;
    let fast = started.elapsed();
    assert_eq!(answer.status, Some(200));
    assert!(fast < Duration::from_millis(100), "answered in {fast:?}");
    let (lasted, received) = slow.await.unwrap();
    assert!(received.is_empty(), "the slow client got an answer");
    assert!(
        lasted >= Duration::from_millis(1900) && lasted < Duration::from_millis(3200),
        "the slow client was dropped after {lasted:?}"
    );
    eprintln!(
        "{}",
        serde_json::json!({"event":"http_slow_client","dropped_after_seconds":lasted.as_secs_f64(),"other_answered_seconds":fast.as_secs_f64()})
    );
    server.stop().await;
}

#[tokio::test]
async fn a_connection_beyond_the_limit_is_closed_at_once() {
    let server = Server::start(true).await;
    let mut held = Vec::new();
    for _ in 0..MAX_CONNECTIONS {
        held.push(TcpStream::connect(server.address).await.unwrap());
    }
    // The server has accepted every held connection before the next one.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let started = Instant::now();
    let extra = exchange(
        server.address,
        b"GET /status HTTP/1.1\r\nHost: test\r\n\r\n",
    )
    .await;
    assert_eq!(extra.status, None, "the extra connection was answered");
    assert!(started.elapsed() < Duration::from_millis(500));
    drop(held);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(get(server.address, "/status").await.status, Some(200));
    // Sixteen at once are answered.
    let address = server.address;
    let answers = futures_util::future::join_all(
        (0..MAX_CONNECTIONS).map(|_| async move { get(address, "/status").await.status }),
    )
    .await;
    assert!(answers.iter().all(|status| *status == Some(200)));
    server.stop().await;
}

#[tokio::test]
async fn a_head_above_eight_kibibytes_is_refused() {
    let server = Server::start(true).await;
    let line = "GET /status HTTP/1.1\r\nHost: test\r\n";
    let fill = |size: usize| {
        format!(
            "{line}X-Fill: {}\r\n\r\n",
            "a".repeat(size - line.len() - "X-Fill: \r\n\r\n".len())
        )
    };
    let at_limit = fill(8 * 1024);
    assert_eq!(at_limit.len(), 8 * 1024);
    assert_eq!(
        exchange(server.address, at_limit.as_bytes()).await.status,
        Some(200)
    );
    let above = fill(8 * 1024 + 1);
    let answer = exchange(server.address, above.as_bytes()).await;
    assert_eq!(answer.status, Some(431));
    let huge = fill(64 * 1024);
    assert_eq!(
        exchange(server.address, huge.as_bytes()).await.status,
        Some(431)
    );
    server.stop().await;
}

fn resident_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find(|line| line.starts_with("VmRSS:"))
                .and_then(|line| line.split_whitespace().nth(1))
                .and_then(|value| value.parse().ok())
        })
        .unwrap_or(0)
}

#[tokio::test]
async fn sequential_requests_leave_tasks_and_memory_flat() {
    let requests: usize = std::env::var("DSA_HTTP_REQUESTS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(200);
    let server = Server::start(true).await;
    let metrics = tokio::runtime::Handle::current().metrics();
    for _ in 0..50 {
        assert_eq!(get(server.address, "/status").await.status, Some(200));
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let tasks_before = metrics.num_alive_tasks();
    let memory_before = resident_kib();
    let started = Instant::now();
    let mut slowest = Duration::ZERO;
    for index in 0..requests {
        let path = if index % 2 == 0 { "/status" } else { "/health" };
        let one = Instant::now();
        assert_eq!(get(server.address, path).await.status, Some(200));
        slowest = slowest.max(one.elapsed());
    }
    let elapsed = started.elapsed();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let tasks_after = metrics.num_alive_tasks();
    let memory_after = resident_kib();
    eprintln!(
        "{}",
        serde_json::json!({"event":"http_sequential","requests":requests,"seconds":elapsed.as_secs_f64(),
            "mean_milliseconds":elapsed.as_secs_f64()*1000.0/requests as f64,"slowest_milliseconds":slowest.as_secs_f64()*1000.0,
            "tasks_before":tasks_before,"tasks_after":tasks_after,"resident_kib_before":memory_before,"resident_kib_after":memory_after})
    );
    assert_eq!(tasks_after, tasks_before, "connection tasks remain");
    assert!(
        memory_after <= memory_before + 2048,
        "resident memory grew from {memory_before} KiB to {memory_after} KiB"
    );
    server.stop().await;
}

#[tokio::test]
async fn the_probe_reads_the_status_code_within_its_deadline() {
    let server = Server::start(true).await;
    let target = Target::parse(&format!("http://{}/health", server.address)).unwrap();
    assert_eq!(probe(&target, Duration::from_secs(1)).await, Ok(200));
    server.documents.send(documents(false, "x")).unwrap();
    assert_eq!(probe(&target, Duration::from_secs(1)).await, Ok(503));
    let missing = Target::parse(&format!("http://{}/other", server.address)).unwrap();
    assert_eq!(probe(&missing, Duration::from_secs(1)).await, Ok(404));
    let address = server.address;
    server.stop().await;
    // Nothing listens any more.
    let target = Target::parse(&format!("http://{address}/health")).unwrap();
    assert_eq!(
        probe(&target, Duration::from_secs(1)).await,
        Err(ProbeError::Unreachable)
    );
    // A listener that never answers.
    let silent = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = Target::parse(&format!("http://{}/health", silent.local_addr().unwrap())).unwrap();
    let started = Instant::now();
    assert_eq!(
        probe(&target, Duration::from_millis(300)).await,
        Err(ProbeError::Timeout)
    );
    assert!(started.elapsed() < Duration::from_millis(600));
}
