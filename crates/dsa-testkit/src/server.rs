//! A loopback HTTP(S) server whose answers come from a handler function. It
//! records every request and the largest number answered at once.

use std::{
    convert::Infallible,
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use http_body_util::{BodyExt, Full, StreamBody, combinators::BoxBody};
use hyper::{
    Request, Response,
    body::{Frame, Incoming},
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpListener,
    task::JoinHandle,
    time::Instant,
};
use tokio_rustls::TlsAcceptor;

/// The synthetic provider key the test clients send.
pub const KEY: &str = "synthetic-blockfrost-project-id";

/// One answer. The default is `200` with the body as JSON.
#[derive(Clone, Debug)]
pub struct Behavior {
    pub status: u16,
    pub body: String,
    pub delay: Duration,
    pub retry_after: Option<String>,
    pub location: Option<String>,
    /// Close the connection before any answer.
    pub dropped: bool,
    /// Send the body in a chunked stream instead of with a length.
    pub chunked: bool,
    /// Send this many body bytes, then wait this long before the rest.
    pub stall: Option<(usize, Duration)>,
    /// Send this many body bytes in a chunked stream, then break the connection.
    pub break_after: Option<usize>,
}

impl Behavior {
    pub fn json(value: Value) -> Self {
        Self::raw(value.to_string())
    }
    pub fn raw(body: String) -> Self {
        Self {
            status: 200,
            body,
            delay: Duration::ZERO,
            retry_after: None,
            location: None,
            dropped: false,
            chunked: false,
            stall: None,
            break_after: None,
        }
    }
    /// A provider refusal with the backend's error document.
    pub fn status(status: u16) -> Self {
        Self {
            status,
            ..Self::json(
                json!({"status_code":status,"error":"Synthetic","message":"Synthetic failure"}),
            )
        }
    }
    pub fn dropped() -> Self {
        Self {
            dropped: true,
            ..Self::raw(String::new())
        }
    }
    pub fn delayed(mut self, millis: u64) -> Self {
        self.delay = Duration::from_millis(millis);
        self
    }
    pub fn retry_after(mut self, value: &str) -> Self {
        self.retry_after = Some(value.to_owned());
        self
    }
    /// The first `bytes` of the body, as a complete answer.
    pub fn truncated(mut self, bytes: usize) -> Self {
        self.body.truncate(bytes);
        self
    }
    pub fn stalled(mut self, after: usize, wait: Duration) -> Self {
        self.stall = Some((after, wait));
        self
    }
    pub fn broken_after(mut self, bytes: usize) -> Self {
        self.break_after = Some(bytes);
        self
    }
}

#[derive(Clone, Debug)]
pub struct Seen {
    pub path: String,
    pub at: Instant,
    pub had_key: bool,
}

#[derive(Default)]
pub struct State {
    pub requests: Mutex<Vec<Seen>>,
    pub connections: AtomicUsize,
    pub active: AtomicUsize,
    pub maximum_active: AtomicUsize,
}

impl State {
    pub fn paths(&self) -> Vec<String> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .map(|seen| seen.path.clone())
            .collect()
    }
    pub fn count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}

pub type Handler = Arc<dyn Fn(&str, usize) -> Behavior + Send + Sync>;
type Body = BoxBody<Bytes, io::Error>;

pub struct Server {
    /// The provider base URL, ending in `/api/v0`.
    pub url: String,
    pub state: Arc<State>,
    /// The PEM certificate of a TLS server.
    pub certificate: Option<String>,
    task: JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Active(Arc<State>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

fn body(behavior: Behavior) -> Body {
    let bytes = Bytes::from(behavior.body);
    if let Some(after) = behavior.break_after {
        let head = bytes.slice(..after.min(bytes.len()));
        return StreamBody::new(futures_util::stream::iter(vec![
            Ok(Frame::data(head)),
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "synthetic break")),
        ]))
        .boxed();
    }
    if let Some((after, wait)) = behavior.stall {
        let after = after.min(bytes.len());
        let (head, tail) = (bytes.slice(..after), bytes.slice(after..));
        let stream = futures_util::stream::unfold(0, move |step| {
            let (head, tail) = (head.clone(), tail.clone());
            async move {
                match step {
                    0 => Some((Ok::<_, io::Error>(Frame::data(head)), 1)),
                    1 => {
                        tokio::time::sleep(wait).await;
                        Some((Ok(Frame::data(tail)), 2))
                    }
                    _ => None,
                }
            }
        });
        return StreamBody::new(stream).boxed();
    }
    if behavior.chunked {
        return StreamBody::new(futures_util::stream::iter(vec![Ok::<_, io::Error>(
            Frame::data(bytes),
        )]))
        .boxed();
    }
    Full::new(bytes)
        .map_err(|never: Infallible| match never {})
        .boxed()
}

async fn answer(
    request: Request<Incoming>,
    state: Arc<State>,
    handler: Handler,
) -> Result<Response<Body>, io::Error> {
    let path = request.uri().to_string();
    let index = {
        let mut requests = state.requests.lock().unwrap();
        requests.push(Seen {
            path: path.clone(),
            at: Instant::now(),
            had_key: request
                .headers()
                .get("project_id")
                .is_some_and(|value| value == KEY),
        });
        requests.len() - 1
    };
    let active = state.active.fetch_add(1, Ordering::SeqCst) + 1;
    state.maximum_active.fetch_max(active, Ordering::SeqCst);
    let _active = Active(state);
    let behavior = handler(&path, index);
    tokio::time::sleep(behavior.delay).await;
    if behavior.dropped {
        return Err(io::Error::new(
            io::ErrorKind::ConnectionAborted,
            "synthetic drop",
        ));
    }
    let mut response = Response::builder()
        .status(behavior.status)
        .header("content-type", "application/json");
    if let Some(value) = &behavior.retry_after {
        response = response.header("retry-after", value);
    }
    if let Some(value) = &behavior.location {
        response = response.header("location", value);
    }
    Ok(response.body(body(behavior)).unwrap())
}

async fn connection<S>(stream: S, state: Arc<State>, handler: Handler)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let service = service_fn(move |request| answer(request, state.clone(), handler.clone()));
    let _ = hyper::server::conn::http1::Builder::new()
        .serve_connection(TokioIo::new(stream), service)
        .await;
}

impl Server {
    /// A plain HTTP server on a loopback port above 10000.
    pub async fn start(handler: impl Fn(&str, usize) -> Behavior + Send + Sync + 'static) -> Self {
        Self::build(Arc::new(handler), None).await
    }

    pub async fn with_handler(handler: Handler) -> Self {
        Self::build(handler, None).await
    }

    /// An HTTPS server whose self-signed certificate names `names`.
    pub async fn tls(
        handler: impl Fn(&str, usize) -> Behavior + Send + Sync + 'static,
        names: Vec<String>,
    ) -> Self {
        let generated = rcgen::generate_simple_self_signed(names).unwrap();
        let pem = generated.cert.pem();
        let key =
            rustls::pki_types::PrivatePkcs8KeyDer::from(generated.signing_key.serialize_der());
        let config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![generated.cert.der().clone()], key.into())
        .unwrap();
        Self::build(
            Arc::new(handler),
            Some((TlsAcceptor::from(Arc::new(config)), pem)),
        )
        .await
    }

    async fn build(handler: Handler, tls: Option<(TlsAcceptor, String)>) -> Self {
        let listener = loop {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            if listener.local_addr().unwrap().port() > 10000 {
                break listener;
            }
        };
        let port = listener.local_addr().unwrap().port();
        let state = Arc::new(State::default());
        let shared = state.clone();
        let certificate = tls.as_ref().map(|(_, certificate)| certificate.clone());
        let scheme = if tls.is_some() { "https" } else { "http" };
        let task = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                shared.connections.fetch_add(1, Ordering::SeqCst);
                let state = shared.clone();
                let handler = handler.clone();
                let acceptor = tls.as_ref().map(|(acceptor, _)| acceptor.clone());
                tokio::spawn(async move {
                    match acceptor {
                        Some(acceptor) => {
                            if let Ok(stream) = acceptor.accept(stream).await {
                                connection(stream, state, handler).await;
                            }
                        }
                        None => connection(stream, state, handler).await,
                    }
                });
            }
        });
        Self {
            url: format!("{scheme}://127.0.0.1:{port}/api/v0"),
            state,
            certificate,
            task,
        }
    }
}
