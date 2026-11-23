//! The stake distribution read by its two methods against the fake provider.

use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use dsa_provider::{Clock, Config, Provider};
use dsa_service::{
    epoch::{self, Method, StakeError, Waits},
    follow::Requests,
    retry::{Stop, Time},
};
use dsa_testkit::{
    chain::{Block, Chain, Pools, pool_id},
    server::{Behavior, KEY, Server},
};
use serde_json::json;

fn pool(name: &str) -> String {
    dsa_testkit::tx::hash(name.as_bytes())[..56].to_owned()
}

/// A chain whose latest block is in `epoch`.
fn blocks(epoch: u32) -> Vec<Block> {
    let mut chain = Chain::new(1, &[0, 0]);
    for block in chain.blocks_mut() {
        block.epoch = epoch;
    }
    chain.blocks().to_vec()
}

struct Fake {
    chain: Arc<Mutex<Chain>>,
    server: Server,
    _key: tempfile::TempDir,
}

impl Fake {
    async fn start(pools: Pools, epoch: u32) -> Self {
        let mut chain = Chain::from_blocks(blocks(epoch));
        chain.pools = pools;
        let chain = Arc::new(Mutex::new(chain));
        let server = Server::with_handler(Chain::handler(chain.clone())).await;
        let key = tempfile::tempdir().unwrap();
        std::fs::write(key.path().join("key"), KEY).unwrap();
        Self {
            chain,
            server,
            _key: key,
        }
    }

    fn provider(&self) -> Provider {
        self.provider_on(Simulated::new())
    }

    /// A client on `clock`, whose requests time out after half a second.
    fn provider_on(&self, clock: Arc<Simulated>) -> Provider {
        let mut config = Config::new(self.server.url.clone(), self._key.path().join("key"));
        config.requests_per_second = 10.0;
        config.idle_timeout = Duration::from_millis(500);
        config.request_timeout = Duration::from_millis(500);
        Provider::with_clock(config, clock).unwrap()
    }

    fn requests(&self) -> usize {
        self.server.state.count()
    }
}

/// A clock whose sleeps return at once after moving it forward.
struct Simulated(AtomicU64);

impl Simulated {
    fn new() -> Arc<Self> {
        Arc::new(Self(AtomicU64::new(1_795_046_400)))
    }
    fn now(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

impl Clock for Simulated {
    fn unix_seconds(&self) -> u64 {
        self.now()
    }
}

impl Time for Simulated {
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        self.0.fetch_add(
            duration.as_secs() + u64::from(duration.subsec_nanos() > 0),
            Ordering::SeqCst,
        );
        Box::pin(tokio::task::yield_now())
    }
}

/// The waits of a read on `clock`, without a stop.
fn waits(clock: Arc<Simulated>) -> Waits {
    Waits {
        time: clock,
        stop: Stop::new().1,
    }
}

/// Pools of epochs 9 and 10: A registered; B retired in epoch 9 (inside the
/// retired window of epoch 10) and still holding stake; C retired in epoch 8
/// and registered again; D registered with zero stake and a delegated
/// address in epoch 10; Z registered without any stake; R retired long
/// before the window.
fn scripted() -> Pools {
    let (a, b, c, d, z, r) = (
        pool("a"),
        pool("b"),
        pool("c"),
        pool("d"),
        pool("z"),
        pool("r"),
    );
    Pools {
        stake: BTreeMap::from([
            (
                9,
                BTreeMap::from([(a.clone(), 1_000), (b.clone(), 2_000), (c.clone(), 3_000)]),
            ),
            (
                10,
                BTreeMap::from([
                    (a.clone(), 1_100),
                    (b.clone(), 2_100),
                    (c.clone(), 3_100),
                    (d.clone(), 0),
                ]),
            ),
        ]),
        registered: [a, c.clone(), d, z].into_iter().collect(),
        retired: BTreeMap::from([(b, 9), (c, 8), (r, 5)]),
        stake_prefix: "stake_test1".to_owned(),
        ..Pools::default()
    }
}

/// Both methods give the same per-pool stake on a set with a
/// retired, a re-registered and a zero-stake pool; a sum that differs from
/// the epoch's active stake is refused.
#[tokio::test]
async fn both_stake_methods_agree_and_a_wrong_total_is_refused() {
    let fake = Fake::start(scripted(), 10).await;
    let provider = fake.provider();
    let requests = Requests::default();
    let current = epoch::current_stake(
        &provider,
        &requests,
        &waits(Simulated::new()),
        10,
        "stake_test1",
    )
    .await
    .unwrap();
    let history = epoch::historical_stake(&provider, &requests, &waits(Simulated::new()), 10)
        .await
        .unwrap();
    assert_eq!(current, history);
    let expected = BTreeMap::from([
        (pool("a"), 1_100),
        (pool("b"), 2_100),
        (pool("c"), 3_100),
        (pool("d"), 0),
    ]);
    assert_eq!(current, expected);

    let read = epoch::read_stake(
        &provider,
        &requests,
        &waits(Simulated::new()),
        10,
        "preprod",
    )
    .await
    .unwrap();
    assert_eq!((read.method, read.active_stake), (Method::Current, 6_300));
    assert_eq!(read.pools, expected);
    let ended = epoch::read_stake(&provider, &requests, &waits(Simulated::new()), 9, "preprod")
        .await
        .unwrap();
    assert_eq!((ended.method, ended.active_stake), (Method::History, 6_000));
    assert_eq!(ended.pools.len(), 3);

    fake.chain
        .lock()
        .unwrap()
        .pools
        .reported_total
        .insert(10, 9_999);
    assert_eq!(
        epoch::read_stake(
            &provider,
            &requests,
            &waits(Simulated::new()),
            10,
            "preprod"
        )
        .await,
        Err(StakeError::Total {
            expected: 9_999,
            actual: 6_300
        })
    );
    // An epoch after the latest block's is not ready.
    assert_eq!(
        epoch::read_stake(
            &provider,
            &requests,
            &waits(Simulated::new()),
            11,
            "preprod"
        )
        .await,
        Err(StakeError::NotReady)
    );
    let counted = requests.get();
    assert!(counted["pools"] > 0 && counted["epochs"] > 0 && counted["headers"] > 0);
}

/// In an ended epoch a history row with stake and a delegator count of zero
/// is taken, a row with zero stake and zero delegators is skipped, and one
/// with zero stake and a delegator is taken; the sum then matches.
#[tokio::test]
async fn history_rows_are_taken_by_their_stake_and_delegators() {
    let mut pools = scripted();
    let (a, e, f) = (pool("a"), pool("e"), pool("f"));
    pools.registered.extend([e.clone(), f.clone()]);
    let ended = pools.stake.get_mut(&9).unwrap();
    ended.insert(e.clone(), 0);
    ended.insert(f.clone(), 0);
    pools
        .delegators
        .insert(9, BTreeMap::from([(a.clone(), 0), (e, 0)]));
    let fake = Fake::start(pools, 10).await;
    let provider = fake.provider();
    let requests = Requests::default();
    let read = epoch::read_stake(&provider, &requests, &waits(Simulated::new()), 9, "preprod")
        .await
        .unwrap();
    assert_eq!((read.method, read.active_stake), (Method::History, 6_000));
    assert_eq!(
        read.pools,
        BTreeMap::from([(a, 1_000), (pool("b"), 2_000), (pool("c"), 3_000), (f, 0),])
    );
}

/// Each stake refusal names its cause: a pool listed twice names the pool
/// and the list, a retired pool the provider does not know names the pool and
/// the check, and a sum that does not match names both numbers.
#[tokio::test]
async fn every_stake_refusal_names_its_cause() {
    let requests = Requests::default();
    let fake = Fake::start(scripted(), 10).await;
    let a = pool_id(&pool("a"));
    fake.chain
        .lock()
        .unwrap()
        .route("/pools", json!([a.clone(), a.clone()]));
    let error = epoch::read_stake(
        &fake.provider(),
        &requests,
        &waits(Simulated::new()),
        9,
        "preprod",
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.report(),
        json!({"error": "provider_duplicate_pool", "pool": a, "list": "pools"})
    );
    let fake = Fake::start(scripted(), 10).await;
    let b = pool_id(&pool("b"));
    fake.chain
        .lock()
        .unwrap()
        .fault(&format!("/pools/{b}"), Behavior::status(404), 1);
    let error = epoch::read_stake(
        &fake.provider(),
        &requests,
        &waits(Simulated::new()),
        10,
        "preprod",
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.report(),
        json!({"error": "provider_invalid_stake_row", "pool": b, "check": "retired_pool_unknown"})
    );
    fake.chain
        .lock()
        .unwrap()
        .pools
        .reported_total
        .insert(10, 9_999);
    let error = epoch::read_stake(
        &fake.provider(),
        &requests,
        &waits(Simulated::new()),
        10,
        "preprod",
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.report(),
        json!({"error": "provider_epoch_total_mismatch", "active_stake": "9999", "sum": "6300"})
    );
}

/// A read of ended epoch 9 on a fresh fake provider with `fault` on the
/// history of pool c, on `clock`: its result and the requests it sent.
async fn read_with(
    fault: Option<(Behavior, usize)>,
    clock: Arc<Simulated>,
) -> (Result<epoch::Distribution, StakeError>, usize, usize) {
    let fake = Fake::start(scripted(), 10).await;
    let history = format!("/pools/{}/history", pool_id(&pool("c")));
    if let Some((behavior, times)) = fault {
        fake.chain.lock().unwrap().fault(&history, behavior, times);
    }
    let read = epoch::read_stake(
        &fake.provider_on(clock.clone()),
        &Requests::default(),
        &waits(clock),
        9,
        "preprod",
    )
    .await;
    let of_c = fake
        .server
        .state
        .paths()
        .iter()
        .filter(|path| path.contains(&history))
        .count();
    (read, fake.requests(), of_c)
}

/// A request that times out in the middle of a history read is sent again
/// after the client's wait: one extra request, not a second read.
#[tokio::test]
async fn one_timeout_in_a_history_read_costs_one_request() {
    let (clean, sent, _) = read_with(None, Simulated::new()).await;
    let clean = clean.unwrap();
    let timeout = Behavior::status(500).delayed(1_500);
    let (read, with_timeout, of_c) = read_with(Some((timeout, 1)), Simulated::new()).await;
    assert_eq!(read.unwrap(), clean);
    assert_eq!(with_timeout, sent + 1, "one request more than {sent}");
    assert_eq!(of_c, 2);
}

/// Five timeouts of the same request end the read with the transport code.
#[tokio::test]
async fn five_timeouts_of_one_request_end_the_read() {
    let timeout = Behavior::status(500).delayed(1_500);
    let (read, _, of_c) = read_with(Some((timeout, 5)), Simulated::new()).await;
    let error = read.unwrap_err();
    assert_eq!(error.code(), "provider_timeout", "{}", error.report());
    assert_eq!(of_c, 5);
}

/// A 429 with a resume time in the middle of a read is waited out on the
/// read's clock, and the same request is sent again.
#[tokio::test]
async fn a_rate_limit_in_a_read_is_waited_out() {
    let (clean, sent, _) = read_with(None, Simulated::new()).await;
    let clean = clean.unwrap();
    let clock = Simulated::new();
    let began = clock.now();
    let limited = Behavior::status(429).retry_after("120");
    let (read, with_limit, of_c) = read_with(Some((limited, 1)), clock.clone()).await;
    assert_eq!(read.unwrap(), clean);
    assert!(clock.now() >= began + 120, "{}", clock.now() - began);
    assert_eq!(with_limit, sent + 1);
    assert_eq!(of_c, 2);
}
