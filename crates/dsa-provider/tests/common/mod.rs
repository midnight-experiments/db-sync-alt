use std::{
    convert::Infallible,
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use dsa_provider::{
    AttemptRecorder, Clock, Config, DailyCount, Provider, RecordFuture, RecorderError,
};
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

pub const KEY: &str = "synthetic-blockfrost-project-id";
pub const NOW: u64 = 1795000000;

#[derive(Default)]
pub struct Recorder {
    pub counts: Mutex<Vec<DailyCount>>,
    pub reserved_at: Mutex<Vec<Instant>>,
    pub fail: AtomicBool,
}
impl AttemptRecorder for Recorder {
    fn record(&self, count: DailyCount) -> RecordFuture<'_> {
        Box::pin(async move {
            if self.fail.load(Ordering::SeqCst) {
                return Err(RecorderError);
            }
            self.counts.lock().unwrap().push(count);
            self.reserved_at.lock().unwrap().push(Instant::now());
            Ok(())
        })
    }
}

pub struct TestClock(pub AtomicU64);
impl Clock for TestClock {
    fn unix_seconds(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}
impl TestClock {
    pub fn set(&self, time: u64) {
        self.0.store(time, Ordering::SeqCst);
    }
}

pub fn client(
    url: &str,
    configure: impl FnOnce(&mut Config),
) -> (Provider, Arc<TestClock>, Arc<Recorder>) {
    let key = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(key.path(), KEY).unwrap();
    let mut config = Config::new(url, key.path());
    config.requests_per_second = 10.0;
    configure(&mut config);
    let recorder = Arc::new(Recorder::default());
    let clock = Arc::new(TestClock(AtomicU64::new(NOW)));
    let provider = Provider::with_clock(config, recorder.clone(), vec![], clock.clone()).unwrap();
    (provider, clock, recorder)
}

pub fn hash(character: char) -> String {
    character.to_string().repeat(64)
}

pub fn header(height: u32) -> Value {
    json!({"hash":format!("{height:064x}"),"height":height,"slot":height as u64*20,
        "epoch":667,"time":NOW,"tx_count":1,"size":400,
        "previous_block":format!("{:064x}",height.saturating_sub(1)),
        "next_block":null,"confirmations":1,"slot_leader":"pool1example","output":"0","fees":"0",
        "unknown":{"nested":[1,2,3]}})
}

pub fn genesis() -> Value {
    json!({"network_magic":764824073,"security_param":2160,"epoch_length":432000,
        "system_start":1506203091,"slot_length":1,"active_slots_coefficient":0.05,
        "update_quorum":5,"max_lovelace_supply":"45000000000000000",
        "slots_per_kes_period":129600,"max_kes_evolutions":62})
}

#[derive(Clone)]
pub struct Behavior {
    pub status: u16,
    pub body: String,
    pub delay: Duration,
    pub retry_after: Option<String>,
    pub location: Option<String>,
    pub disconnect: bool,
    pub chunked: bool,
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
            disconnect: false,
            chunked: false,
        }
    }
    pub fn status(status: u16) -> Self {
        Self {
            status,
            ..Self::json(
                json!({"status_code":status,"error":"Mock provider refusal","message":"Synthetic failure"}),
            )
        }
    }
    pub fn delayed(mut self, millis: u64) -> Self {
        self.delay = Duration::from_millis(millis);
        self
    }
}

#[derive(Clone)]
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

type Handler = Arc<dyn Fn(&str, usize) -> Behavior + Send + Sync>;
type Body = BoxBody<Bytes, io::Error>;

pub struct Server {
    pub url: String,
    pub state: Arc<State>,
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

async fn answer(
    request: Request<Incoming>,
    state: Arc<State>,
    handler: Handler,
) -> Result<Response<Body>, io::Error> {
    let at = Instant::now();
    let path = request.uri().to_string();
    let index = {
        let mut requests = state.requests.lock().unwrap();
        let index = requests.len();
        requests.push(Seen {
            path: path.clone(),
            at,
            had_key: request
                .headers()
                .get("project_id")
                .is_some_and(|value| value == KEY),
        });
        index
    };
    let active = state.active.fetch_add(1, Ordering::SeqCst) + 1;
    state.maximum_active.fetch_max(active, Ordering::SeqCst);
    let _active = Active(state);
    let behavior = handler(&path, index);
    tokio::time::sleep(behavior.delay).await;
    if behavior.disconnect {
        return Err(io::Error::new(
            io::ErrorKind::ConnectionAborted,
            "synthetic disconnect",
        ));
    }
    let body: Body = if behavior.chunked {
        StreamBody::new(futures_util::stream::iter(vec![Ok::<_, io::Error>(
            Frame::data(Bytes::from(behavior.body)),
        )]))
        .boxed()
    } else {
        Full::new(Bytes::from(behavior.body))
            .map_err(|never: Infallible| match never {})
            .boxed()
    };
    let mut response = Response::builder()
        .status(behavior.status)
        .header("content-type", "application/json");
    if let Some(value) = behavior.retry_after {
        response = response.header("retry-after", value);
    }
    if let Some(value) = behavior.location {
        response = response.header("location", value);
    }
    Ok(response.body(body).unwrap())
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
    pub async fn start(handler: impl Fn(&str, usize) -> Behavior + Send + Sync + 'static) -> Self {
        Self::build(handler, None).await
    }
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
        Self::build(handler, Some((TlsAcceptor::from(Arc::new(config)), pem))).await
    }
    async fn build(
        handler: impl Fn(&str, usize) -> Behavior + Send + Sync + 'static,
        tls: Option<(TlsAcceptor, String)>,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        assert!(port > 10000, "test port must exceed10000");
        let state = Arc::new(State::default());
        let state_for_task = state.clone();
        let certificate = tls.as_ref().map(|(_, certificate)| certificate.clone());
        let secure = tls.is_some();
        let handler: Handler = Arc::new(handler);
        let task = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                state_for_task.connections.fetch_add(1, Ordering::SeqCst);
                let state = state_for_task.clone();
                let handler = handler.clone();
                let tls = tls.as_ref().map(|(acceptor, _)| acceptor.clone());
                tokio::spawn(async move {
                    if let Some(acceptor) = tls {
                        if let Ok(stream) = acceptor.accept(stream).await {
                            connection(stream, state, handler).await;
                        }
                    } else {
                        connection(stream, state, handler).await;
                    }
                });
            }
        });
        let url = format!(
            "{}://127.0.0.1:{port}/api/v0",
            if secure { "https" } else { "http" }
        );
        eprintln!("loopback mock {url}");
        Self {
            url,
            state,
            certificate,
            task,
        }
    }
}
