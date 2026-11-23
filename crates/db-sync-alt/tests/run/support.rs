//! Support of the service tests: a clock that runs faster than the wall
//! clock and can jump, a TCP proxy in front of PostgreSQL that can refuse and
//! drop connections, the fake provider on that clock, HTTP reads of the
//! service, and the `run` command as a child process.

use std::{
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use dsa_provider::{Clock, Config, Provider};
use dsa_service::retry::Time;
use dsa_testkit::{
    chain::Chain,
    server::{Behavior, Handler, KEY, Server},
};
use serde_json::Value;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    task::AbortHandle,
};

/// A clock that runs `rate` times faster than the wall clock from `start`
/// (Unix seconds), and jumps forward on request. Its sleeps end once the
/// clock reaches their end, also after a jump.
pub struct Scaled {
    start: u64,
    offset: AtomicU64,
    began: std::time::Instant,
    rate: u64,
}

impl Scaled {
    pub fn new(start: u64, rate: u64) -> Arc<Self> {
        Arc::new(Self {
            start,
            offset: AtomicU64::new(0),
            began: std::time::Instant::now(),
            rate,
        })
    }

    pub fn now(&self) -> u64 {
        let elapsed = self.began.elapsed().as_millis() as u64 * self.rate / 1000;
        self.start + self.offset.load(Ordering::SeqCst) + elapsed
    }

    /// Moves the clock to `at` when it is earlier.
    pub fn jump_to(&self, at: u64) {
        let now = self.now();
        if at > now {
            self.offset.fetch_add(at - now, Ordering::SeqCst);
        }
    }

    /// Real time for `seconds` of this clock.
    pub fn real(&self, seconds: u64) -> Duration {
        Duration::from_millis(seconds * 1000 / self.rate)
    }
}

impl Clock for Scaled {
    fn unix_seconds(&self) -> u64 {
        self.now()
    }
}

impl Time for Scaled {
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let end = self.now() + duration.as_secs() + u64::from(duration.subsec_nanos() > 0);
        Box::pin(async move {
            while self.now() < end {
                let left = self.real(end - self.now()).max(Duration::from_millis(1));
                tokio::time::sleep(left.min(Duration::from_millis(20))).await;
            }
        })
    }
}

/// A TCP proxy in front of PostgreSQL: while refusing, a new connection is
/// closed at once; `drop_all` ends every open one, as a restart of the server
/// does.
pub struct Proxy {
    pub port: u16,
    open: Arc<AtomicBool>,
    connections: Arc<Mutex<Vec<AbortHandle>>>,
    _task: tokio::task::JoinHandle<()>,
}

impl Proxy {
    pub async fn start(host: String, port: u16) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local = listener.local_addr().unwrap().port();
        let open = Arc::new(AtomicBool::new(true));
        let connections: Arc<Mutex<Vec<AbortHandle>>> = Arc::default();
        let task = tokio::spawn({
            let (open, connections) = (open.clone(), connections.clone());
            async move {
                loop {
                    let Ok((mut client, _)) = listener.accept().await else {
                        continue;
                    };
                    if !open.load(Ordering::SeqCst) {
                        drop(client);
                        continue;
                    }
                    let target = (host.clone(), port);
                    let handle = tokio::spawn(async move {
                        if let Ok(mut server) = TcpStream::connect(target).await {
                            let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
                        }
                    });
                    let mut list = connections.lock().unwrap();
                    list.retain(|handle| !handle.is_finished());
                    list.push(handle.abort_handle());
                }
            }
        });
        Self {
            port: local,
            open,
            connections,
            _task: task,
        }
    }

    pub fn refuse(&self) {
        self.open.store(false, Ordering::SeqCst);
        self.drop_all();
    }

    pub fn accept(&self) {
        self.open.store(true, Ordering::SeqCst);
    }

    pub fn drop_all(&self) {
        for handle in self.connections.lock().unwrap().drain(..) {
            handle.abort();
        }
    }

    /// A connection configuration through the proxy: the user and database
    /// of `config`, without TLS.
    pub fn config(&self, config: &tokio_postgres::Config) -> tokio_postgres::Config {
        let mut through = tokio_postgres::Config::new();
        through
            .host("127.0.0.1")
            .port(self.port)
            .user(config.get_user().unwrap_or("postgres"))
            .dbname(config.get_dbname().unwrap_or("postgres"))
            .ssl_mode(tokio_postgres::config::SslMode::Disable);
        through
    }
}

/// Answers a request before the chain does; `None` lets the chain answer.
pub type Hook = Arc<dyn Fn(&str, &mut Chain, u64) -> Option<Behavior> + Send + Sync>;

/// The fake provider on a given clock, every request logged with the time
/// it arrived at.
pub struct Fake {
    pub chain: Arc<Mutex<Chain>>,
    pub server: Server,
    pub log: Arc<Mutex<Vec<(String, u64)>>>,
    pub hook: Arc<Mutex<Option<Hook>>>,
    pub key: tempfile::TempDir,
}

impl Fake {
    pub async fn start(chain: Chain, clock: Arc<dyn Clock + Send + Sync>) -> Self {
        let chain = Arc::new(Mutex::new(chain));
        let log = Arc::new(Mutex::new(Vec::new()));
        let hook: Arc<Mutex<Option<Hook>>> = Arc::default();
        let handler: Handler = {
            let (chain, log, hook) = (chain.clone(), log.clone(), hook.clone());
            Arc::new(move |path, _| {
                let now = clock.unix_seconds();
                log.lock().unwrap().push((path.to_owned(), now));
                let hook = hook.lock().unwrap().clone();
                let mut chain = chain.lock().unwrap();
                if let Some(answer) = hook.and_then(|hook| hook(path, &mut chain, now)) {
                    return answer;
                }
                chain.answer(path)
            })
        };
        let server = Server::with_handler(handler).await;
        let key = tempfile::tempdir().unwrap();
        std::fs::write(key.path().join("key"), KEY).unwrap();
        Self {
            chain,
            server,
            log,
            hook,
            key,
        }
    }

    pub fn key_file(&self) -> std::path::PathBuf {
        self.key.path().join("key")
    }

    pub fn provider(&self, clock: Arc<dyn Clock + Send + Sync>) -> Provider {
        let mut config = Config::new(self.server.url.clone(), self.key_file());
        config.requests_per_second = 10.0;
        config.max_in_flight = 4;
        config.idle_timeout = Duration::from_secs(5);
        config.request_timeout = Duration::from_secs(10);
        Provider::with_clock(config, clock).unwrap()
    }

    pub fn set_hook(&self, hook: Option<Hook>) {
        *self.hook.lock().unwrap() = hook;
    }

    pub fn requests(&self) -> usize {
        self.log.lock().unwrap().len()
    }
}

/// One GET: the status code and the body as JSON.
pub async fn get(address: SocketAddr, path: &str) -> Option<(u16, Value)> {
    let exchange = async {
        let mut stream = TcpStream::connect(address).await.ok()?;
        stream
            .write_all(format!("GET {path} HTTP/1.1\r\nHost: test\r\n\r\n").as_bytes())
            .await
            .ok()?;
        let mut answer = String::new();
        stream.read_to_string(&mut answer).await.ok()?;
        let (head, body) = answer.split_once("\r\n\r\n")?;
        Some((
            head.get(9..12)?.parse().ok()?,
            serde_json::from_str(body).ok()?,
        ))
    };
    tokio::time::timeout(Duration::from_secs(3), exchange)
        .await
        .ok()
        .flatten()
}

/// The `run` command as a child process: its standard error collected
/// line by line while it runs.
pub struct Child {
    pub child: tokio::process::Child,
    pub pid: u32,
    pub lines: Arc<Mutex<Vec<String>>>,
    reader: tokio::task::JoinHandle<()>,
}

impl Child {
    /// Starts `db-sync-alt` with `arguments` and only the given `DSA_`
    /// variables.
    pub fn start(arguments: &[String], environment: &[(&str, String)]) -> Self {
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_db-sync-alt"));
        command.args(arguments);
        for (name, _) in std::env::vars().filter(|(name, _)| name.starts_with("DSA_")) {
            command.env_remove(name);
        }
        for (name, value) in environment {
            command.env(name, value);
        }
        let mut child = command
            .kill_on_drop(true)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let pid = child.id().unwrap();
        let lines: Arc<Mutex<Vec<String>>> = Arc::default();
        let stderr = child.stderr.take().unwrap();
        let reader = tokio::spawn({
            let lines = lines.clone();
            async move {
                let mut stderr = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = stderr.next_line().await {
                    lines.lock().unwrap().push(line);
                }
            }
        });
        Self {
            child,
            pid,
            lines,
            reader,
        }
    }

    /// The structured lines of standard error so far.
    pub fn events(&self) -> Vec<Value> {
        self.lines
            .lock()
            .unwrap()
            .iter()
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect()
    }

    /// The address the service listens on, once it has logged it.
    pub async fn address(&self) -> SocketAddr {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(address) = self
                .events()
                .iter()
                .find(|event| event["event"] == "listening")
                .and_then(|event| event["address"].as_str()?.parse().ok())
            {
                return address;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "no listening line: {:?}",
                self.lines.lock().unwrap()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    pub fn signal(&self, name: &str) {
        let sent = std::process::Command::new("kill")
            .args([&format!("-{name}"), &self.pid.to_string()])
            .status()
            .unwrap();
        assert!(sent.success());
    }

    /// Waits for the end: exit status, standard output, standard error.
    pub async fn finish(mut self) -> (Option<i32>, String, Vec<String>) {
        let mut stdout = String::new();
        if let Some(mut out) = self.child.stdout.take() {
            out.read_to_string(&mut stdout).await.unwrap();
        }
        let status = self.child.wait().await.unwrap();
        let _ = self.reader.await;
        let lines = self.lines.lock().unwrap().clone();
        (status.code(), stdout, lines)
    }
}

/// Peak resident memory of a process, sampled until it exits (kibibytes).
pub fn watch_memory(pid: u32) -> tokio::task::JoinHandle<u64> {
    tokio::spawn(async move {
        let mut peak = 0;
        while let Ok(status) = std::fs::read_to_string(format!("/proc/{pid}/status")) {
            if let Some(kib) = status
                .lines()
                .find_map(|line| line.strip_prefix("VmHWM:"))
                .and_then(|value| {
                    value
                        .trim()
                        .trim_end_matches("kB")
                        .trim()
                        .parse::<u64>()
                        .ok()
                })
            {
                peak = peak.max(kib);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        peak
    })
}
