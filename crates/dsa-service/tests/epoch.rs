//! The stake distribution read by its two methods against the fake provider.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use dsa_provider::{Config, Provider};
use dsa_service::{
    epoch::{self, Method, StakeError},
    follow::Requests,
};
use dsa_testkit::{
    chain::{Block, Chain, Pools},
    server::{KEY, Server},
};

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
        let mut config = Config::new(self.server.url.clone(), self._key.path().join("key"));
        config.requests_per_second = 10.0;
        Provider::new(config).unwrap()
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
    let current = epoch::current_stake(&provider, &requests, 10, "stake_test1")
        .await
        .unwrap();
    let history = epoch::historical_stake(&provider, &requests, 10)
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

    let read = epoch::read_stake(&provider, &requests, 10, "preprod")
        .await
        .unwrap();
    assert_eq!((read.method, read.active_stake), (Method::Current, 6_300));
    assert_eq!(read.pools, expected);
    let ended = epoch::read_stake(&provider, &requests, 9, "preprod")
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
        epoch::read_stake(&provider, &requests, 10, "preprod").await,
        Err(StakeError::Total {
            expected: 9_999,
            actual: 6_300
        })
    );
    // An epoch after the latest block's is not ready.
    assert_eq!(
        epoch::read_stake(&provider, &requests, 11, "preprod").await,
        Err(StakeError::NotReady)
    );
    let counted = requests.get();
    assert!(counted["pools"] > 0 && counted["epochs"] > 0 && counted["headers"] > 0);
}
