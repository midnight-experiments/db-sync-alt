//! Support of the follower tests: a simulated clock, the fake provider over
//! a scripted chain with a request log, and runs of the loop.

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use dsa_chain::profiles::Profile;
use dsa_provider::{Clock, Config, Provider};
use dsa_service::{
    epoch::stake_prefix,
    follow::{self, Connect, Context, Progress, Report, Settings},
    retry::{Stop, Time},
};
use dsa_store::Store;
use dsa_testkit::{
    chain::{Block, Chain},
    generate::Generated,
    server::{Behavior, Handler, KEY, Server},
};
use tokio::sync::watch;

/// The retention of the followed stores: the smallest allowed.
pub const DAY: i64 = 86_400;

/// Midday of a UTC day.
pub const START: u64 = 1_795_046_400 + 43_200;

/// A clock whose sleeps return at once after moving it forward.
pub struct Simulated(AtomicU64);

impl Simulated {
    pub fn new() -> Arc<Self> {
        Arc::new(Self(AtomicU64::new(START)))
    }
    pub fn now(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
    pub fn advance(&self, seconds: u64) {
        self.0.fetch_add(seconds, Ordering::SeqCst);
    }
}

impl Clock for Simulated {
    fn unix_seconds(&self) -> u64 {
        self.now()
    }
}

impl Time for Simulated {
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        self.advance(duration.as_secs() + u64::from(duration.subsec_nanos() > 0));
        Box::pin(tokio::task::yield_now())
    }
}

/// Answers a request before the chain does; `None` lets the chain answer.
pub type Hook = Arc<dyn Fn(&str, &mut Chain, u64) -> Option<Behavior> + Send + Sync>;

/// The fake provider: a scripted chain served over HTTP, every request
/// logged with the simulated time it arrived at.
pub struct Fake {
    pub chain: Arc<Mutex<Chain>>,
    pub server: Server,
    pub clock: Arc<Simulated>,
    pub log: Arc<Mutex<Vec<(String, u64)>>>,
    _key: tempfile::TempDir,
}

impl Fake {
    /// Serves the whole generated chain.
    pub async fn start(g: &Generated, profile: &Profile) -> Self {
        Self::with_chain(
            Chain::from_generated(g, stake_prefix(&profile.network)),
            None,
        )
        .await
    }

    pub async fn with_chain(chain: Chain, hook: Option<Hook>) -> Self {
        let chain = Arc::new(Mutex::new(chain));
        let clock = Simulated::new();
        let log = Arc::new(Mutex::new(Vec::new()));
        let handler: Handler = {
            let (chain, clock, log) = (chain.clone(), clock.clone(), log.clone());
            Arc::new(move |path, _| {
                let now = clock.now();
                log.lock().unwrap().push((path.to_owned(), now));
                let mut chain = chain.lock().unwrap();
                if let Some(answer) = hook.as_ref().and_then(|hook| hook(path, &mut chain, now)) {
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
            clock,
            log,
            _key: key,
        }
    }

    /// Replaces the served blocks.
    pub fn serve(&self, blocks: Vec<Block>) {
        *self.chain.lock().unwrap().blocks_mut() = blocks;
    }

    pub fn provider(&self) -> Provider {
        let mut config = Config::new(self.server.url.clone(), self._key.path().join("key"));
        config.requests_per_second = 10.0;
        config.max_in_flight = 4;
        config.idle_timeout = Duration::from_secs(5);
        config.request_timeout = Duration::from_secs(10);
        Provider::with_clock(config, self.clock.clone()).unwrap()
    }

    /// Requests whose path contains `part`.
    pub fn requests(&self, part: &str) -> usize {
        self.log
            .lock()
            .unwrap()
            .iter()
            .filter(|(path, _)| path.contains(part))
            .count()
    }

    /// The simulated times of the requests whose path contains `part`.
    pub fn times(&self, part: &str) -> Vec<u64> {
        self.log
            .lock()
            .unwrap()
            .iter()
            .filter(|(path, _)| path.contains(part))
            .map(|(_, at)| *at)
            .collect()
    }

    pub fn paths(&self) -> Vec<String> {
        self.log
            .lock()
            .unwrap()
            .iter()
            .map(|(path, _)| path.clone())
            .collect()
    }
}

/// The generated chain through `height`.
pub fn served(g: &Generated, height: u32) -> Vec<Block> {
    g.blocks
        .iter()
        .take_while(|block| block.height <= height)
        .cloned()
        .collect()
}

pub fn connect(config: &tokio_postgres::Config) -> Connect {
    let config = config.clone();
    Box::new(move || {
        let config = config.clone();
        Box::pin(async move { Store::connect(config, None).await })
    })
}

pub fn context<'a>(
    profile: &'a Profile,
    fake: &Fake,
    config: &tokio_postgres::Config,
    settings: Settings,
    stop: Stop,
    progress: watch::Sender<Progress>,
) -> Context<'a> {
    Context {
        profile,
        provider: fake.provider(),
        connect: connect(config),
        settings,
        time: fake.clock.clone(),
        stop,
        progress,
    }
}

/// One run of the loop until its stop target.
pub async fn follow_to(
    profile: &Profile,
    fake: &Fake,
    config: &tokio_postgres::Config,
    settings: Settings,
) -> Result<Report, follow::Error> {
    let (_stopper, stop) = Stop::new();
    let (progress, _view) = watch::channel(Progress::default());
    follow::follow(context(profile, fake, config, settings, stop, progress)).await
}
