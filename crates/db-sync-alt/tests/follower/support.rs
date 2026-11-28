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

use dsa_chain::{block::BlockOutput, profiles::Profile};
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

use crate::{
    common::Database,
    harness::{self, K, Source, chain, convert, derive, header, named, profile, same},
};

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

/// A converted store on a generated chain, with the chain's full history as
/// a schema-5 source for the conversion oracle.
pub struct Start {
    pub profile: Profile,
    pub g: Generated,
    pub history: Vec<BlockOutput>,
    pub source: Source,
    pub database: Database,
    pub converted: Database,
    pub target: u32,
}

impl Start {
    /// The chain from height 1, converted at target `target` and horizon
    /// `target - 2`.
    pub async fn new(seed: u64, blocks: u32, target: u32) -> harness::Result<Self> {
        let profile = profile(seed);
        let g = chain(&profile, seed, 1, blocks, false).known_from_start();
        Self::from_chain(profile, g, target).await
    }

    pub async fn from_chain(profile: Profile, g: Generated, target: u32) -> harness::Result<Self> {
        let history = derive(&profile, &g).await?;
        let mut source = Source::new().await?;
        let headers = (1..=target + K)
            .map(|height| header(g.block(height).unwrap()))
            .collect::<Vec<_>>();
        source.start(&g, &history, target, &headers).await?;
        let database = Database::new().await?;
        convert(&database, &source.database, &profile, target - 2, DAY).await?;
        Ok(Self {
            profile,
            g,
            history,
            source,
            database,
            converted: Database::new().await?,
            target,
        })
    }

    pub async fn store(&self) -> harness::Result<Store> {
        Ok(Store::connect(self.database.config.clone(), None).await?)
    }

    /// The followed store equals a conversion of the full history at its
    /// horizon, target and tip, and verifies.
    pub async fn check(&mut self, context: &str) -> harness::Result {
        let mut store = self.store().await?;
        let state = store.follow_state(&self.profile).await.map_err(named)?;
        let target = state.target.height;
        let above = store.headers_above(target).await.map_err(named)?;
        let headers = (self.source.through + 1..=target)
            .map(|height| header(self.g.block(height).unwrap()))
            .chain(above)
            .collect::<Vec<_>>();
        self.source
            .extend(&self.g, &self.history, target, &headers)
            .await?;
        let conversion = convert(
            &self.converted,
            &self.source.database,
            &self.profile,
            state.horizon.height,
            DAY,
        )
        .await?;
        let verified = store.verify(&self.profile).await.map_err(named)?;
        same(&verified, &conversion, context)
    }

    pub async fn cleanup(self) -> harness::Result {
        self.database.cleanup().await?;
        self.converted.cleanup().await?;
        self.source.database.cleanup().await
    }
}

/// `serve.block` as (height, hash), in height order.
pub async fn block_rows(config: &tokio_postgres::Config) -> harness::Result<Vec<(i64, String)>> {
    Ok(crate::common::connect(config)
        .await?
        .query(
            "SELECT id::bigint,encode(hash,'hex') FROM serve.block ORDER BY id",
            &[],
        )
        .await?
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect())
}

/// Settings that stop at `target`.
pub fn until(target: u32) -> Settings {
    Settings {
        stop_at_target: Some(target),
        ..Settings::default()
    }
}
