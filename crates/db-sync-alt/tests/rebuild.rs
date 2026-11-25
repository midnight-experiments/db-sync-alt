//! The build from the provider alone against PostgreSQL in DSA_TEST_DATABASE
//! and the fake provider of the testkit: a generated chain converted from
//! its full history at horizon H and target T, and the same chain built from
//! the provider's listings at the same H and T, give the same content and
//! state digests, supply and position. DSA_REBUILD_CASES sets the number of
//! generated chains of the property test (default 8), DSA_REBUILD_PARALLEL
//! how many run at once (default 4), DSA_REBUILD_KILL_POINTS where a build
//! is stopped and continued (default 6) and DSA_REBUILD_FOLLOW_CASES the
//! chains followed on after their build (default 4).

#[path = "common/legacy.rs"]
#[allow(dead_code)]
mod common;
#[path = "follow/harness.rs"]
#[allow(dead_code)]
mod harness;
#[path = "follower/support.rs"]
#[allow(dead_code)]
mod support;

use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use common::Database;
use dsa_chain::{block::BlockOutput, profiles::Profile};
use dsa_service::{
    epoch::stake_prefix,
    follow::{self, Progress, Settings},
    rebuild::{self, Options, Report},
    retry::Stop,
};
use dsa_testkit::{
    chain::{Block, Chain},
    generate::{self, Counts, Generated},
    server::Behavior,
    tx::{Datum, Output, Tx, enterprise_address, hash, int_datum},
};
use harness::{K, Random, Result, Source, chain, convert, derive, header, profile, reset, same};
use serde_json::json;
use support::{DAY, Fake, Hook, connect};
use tokio::sync::watch;

fn setting(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// A store database emptied for a build: the closed layout without rows and
/// without an earlier build's schema.
async fn empty(database: &Database, profile: &Profile) -> Result {
    reset(database, profile).await?;
    common::connect(&database.config)
        .await?
        .batch_execute("DROP SCHEMA IF EXISTS dsa_rebuild CASCADE")
        .await?;
    Ok(())
}

/// Requests a build sends at once (`Settings::max_in_flight` of `build`):
/// a stopped build may have counted these without the provider answering.
const IN_FLIGHT: i64 = 4;

/// The phase a build in `database` recorded, if any.
async fn stored_phase(database: &Database) -> Option<String> {
    common::connect(&database.config)
        .await
        .ok()?
        .query_opt("SELECT phase FROM dsa_rebuild.progress WHERE id=1", &[])
        .await
        .ok()
        .flatten()
        .map(|row| row.get(0))
}

/// One build into `database` from `fake`, at H and T.
async fn build(
    profile: &Profile,
    fake: &Fake,
    database: &Database,
    horizon: u32,
    target: u32,
    stop: Stop,
) -> std::result::Result<Report, follow::Error> {
    let (progress, _view) = watch::channel(Progress::default());
    rebuild::rebuild(rebuild::Context {
        profile,
        provider: fake.provider(),
        connect: connect(&database.config),
        settings: Settings {
            max_in_flight: 4,
            fatal_after: Duration::from_secs(600),
            ..Settings::default()
        },
        options: Options {
            horizon_block: Some(horizon),
            target_block: Some(target),
            window_seconds: None,
            retention_seconds: DAY,
        },
        time: fake.clock.clone(),
        stop,
        progress,
    })
    .await
}

/// The heights of a chain from the first block of its third epoch on: from
/// there epoch(H) − 2 has blocks, as on a real network.
fn third_epoch(g: &Generated) -> u32 {
    let first = g.blocks[0].epoch;
    g.blocks
        .iter()
        .find(|block| block.epoch >= first + 2)
        .map_or(g.blocks[0].height, |block| block.height)
}

#[derive(Default, Debug, Clone)]
struct Totals {
    chains: u64,
    quiet_windows: u64,
    markers: u64,
    revealed_at_spend: usize,
    invalid: usize,
    requests: BTreeMap<String, u64>,
    calls: i64,
}

/// A generated chain, its full history and a source written through T.
struct Case {
    profile: Profile,
    g: Generated,
    history: Vec<BlockOutput>,
    horizon: u32,
    target: u32,
}

impl Case {
    async fn new(seed: u64, random: &mut Random) -> Result<Self> {
        let profile = profile(seed);
        let blocks = 45 + random.below(30) as u32;
        let g = chain(&profile, seed, 1, blocks, seed.is_multiple_of(4)).known_from_start();
        let history = derive(&profile, &g).await?;
        let last_target = blocks - K;
        let low = third_epoch(&g).min(last_target);
        let target = low + random.below(u64::from(last_target - low + 1)) as u32;
        let horizon = low + random.below(u64::from(target - low + 1)) as u32;
        Ok(Self {
            profile,
            g,
            history,
            horizon,
            target,
        })
    }

    /// Converts the full history at H and T into `converted`.
    async fn convert(
        &self,
        source: &mut Source,
        converted: &Database,
    ) -> Result<dsa_store::verify::Verification> {
        let headers = (1..=self.target + K)
            .map(|height| header(self.g.block(height).unwrap()))
            .collect::<Vec<_>>();
        source
            .start(&self.g, &self.history, self.target, &headers)
            .await?;
        convert(
            converted,
            &source.database,
            &self.profile,
            self.horizon,
            DAY,
        )
        .await
    }
}

async fn marker_count(database: &Database) -> Result<i64> {
    Ok(common::connect(&database.config)
        .await?
        .query_one("SELECT count(*) FROM dsa.consumed_before_horizon", &[])
        .await?
        .get(0))
}

/// One property case: the build equals the conversion.
async fn equivalence(
    seed: u64,
    source: &mut Source,
    converted: &Database,
    rebuilt: &Database,
    totals: &mut Totals,
) -> Result {
    let mut random = Random::new(seed ^ 0x0d2b_d2b0);
    let case = Case::new(seed, &mut random).await?;
    let conversion = case.convert(source, converted).await?;
    empty(rebuilt, &case.profile).await?;
    let fake = Fake::start(&case.g, &case.profile).await;
    let (_stopper, stop) = Stop::new();
    let context = format!("seed {seed} at H {} T {}", case.horizon, case.target);
    let report = build(
        &case.profile,
        &fake,
        rebuilt,
        case.horizon,
        case.target,
        stop,
    )
    .await
    .map_err(|error| format!("{context}: {}", error.report()))?;
    if let Err(error) = same(&report.verified, &conversion, &context) {
        let rows = harness::diff(&rebuilt.config, &converted.config).await?;
        return Err(format!("{error}; rows (rebuild as follower) {rows}").into());
    }
    totals.chains += 1;
    let window_spent = common::connect(&rebuilt.config)
        .await?
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM serve.tx_in WHERE id>=$1::bigint*16777216)",
            &[&i64::from(case.horizon)],
        )
        .await?
        .get::<_, bool>(0);
    totals.quiet_windows += u64::from(!window_spent);
    totals.markers += marker_count(rebuilt).await? as u64;
    totals.revealed_at_spend += case.g.counts.revealed_at_spend;
    totals.invalid += case.g.counts.invalid;
    totals.calls += report.calls;
    for (family, count) in &report.requests {
        *totals.requests.entry(family.clone()).or_default() += count;
    }
    Ok(())
}

/// Property test: built from the provider's listings, each generated chain
/// has the content digest, state digest, supply and position of a
/// conversion of its full history at the same horizon and target.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn a_build_from_the_provider_equals_a_conversion() -> Result {
    let cases = setting("DSA_REBUILD_CASES", 8);
    let parallel = setting("DSA_REBUILD_PARALLEL", 4).clamp(1, 16);
    let start = setting("DSA_REBUILD_SEED_START", 0);
    let began = Instant::now();
    let workers = (0..parallel).map(|worker| async move {
        let mut totals = Totals::default();
        let mut source = Source::new().await?;
        let converted = Database::new().await?;
        let rebuilt = Database::new().await?;
        let mut seed = start + worker;
        while seed < start + cases {
            equivalence(seed, &mut source, &converted, &rebuilt, &mut totals).await?;
            if (totals.chains * parallel).is_multiple_of(50) {
                println!(
                    "{}",
                    json!({"worker":worker,"chains":totals.chains,"seed":seed,"seconds":began.elapsed().as_secs_f64()})
                );
            }
            seed += parallel;
        }
        source.database.cleanup().await?;
        converted.cleanup().await?;
        rebuilt.cleanup().await?;
        Result::<Totals>::Ok(totals)
    });
    let mut totals = Totals::default();
    for result in futures_util::future::join_all(workers).await {
        let result = result?;
        totals.chains += result.chains;
        totals.quiet_windows += result.quiet_windows;
        totals.markers += result.markers;
        totals.revealed_at_spend += result.revealed_at_spend;
        totals.invalid += result.invalid;
        totals.calls += result.calls;
        for (family, count) in result.requests {
            *totals.requests.entry(family).or_default() += count;
        }
    }
    println!(
        "{}",
        json!({"event":"rebuild_equivalence","cases":cases,"chains":totals.chains,
            "quiet_windows":totals.quiet_windows,"markers":totals.markers,
            "datums_revealed_at_spend":totals.revealed_at_spend,"invalid_transactions":totals.invalid,
            "calls":totals.calls,"requests":totals.requests,"seconds":began.elapsed().as_secs_f64()})
    );
    assert_eq!(totals.chains, cases);
    Ok(())
}

// ----- a scripted chain for the listing edges ------------------------------

/// Blocks of hand-built transactions, eight to an epoch, from height 1.
struct Script {
    g: Generated,
    next_fund: u64,
}

impl Script {
    fn new() -> Self {
        Self {
            g: Generated {
                blocks: Vec::new(),
                datums: BTreeMap::new(),
                metadata: BTreeMap::new(),
                epochs: BTreeMap::new(),
                stake: BTreeMap::new(),
                facts: BTreeMap::new(),
                counts: Counts::default(),
            },
            next_fund: 0,
        }
    }

    /// A transaction funded by an input the chain does not know.
    fn tx(&mut self) -> Tx {
        self.next_fund += 1;
        Tx::new()
            .input(
                &hash(format!("script fund {}", self.next_fund).as_bytes()),
                0,
            )
            .fee(170_000 + self.next_fund)
    }

    fn block(&mut self, transactions: Vec<Tx>) -> Vec<String> {
        let height = 1 + self.g.blocks.len() as u32;
        let epoch = 200 + (height - 1) / 8;
        self.g
            .epochs
            .entry(epoch)
            .or_insert_with(|| (hash(format!("nonce {epoch}").as_bytes()), 16_384));
        self.g.stake.entry(epoch).or_insert_with(|| {
            (0..2_u64)
                .map(|n| {
                    (
                        hash(format!("script pool {n}").as_bytes())[..56].to_owned(),
                        1_000_000 + n * 1_000 + u64::from(epoch),
                    )
                })
                .collect()
        });
        let mut built = Vec::new();
        for tx in transactions {
            let facts = tx.facts();
            let done = tx.build();
            for output in facts.outputs.iter().chain(facts.collateral_return.iter()) {
                if let Datum::Inline(bytes) = &output.datum {
                    self.g
                        .datums
                        .entry(hash(bytes))
                        .or_insert((height, bytes.clone()));
                }
            }
            self.g.facts.insert(done.hash.clone(), facts);
            built.push(done);
        }
        let previous = self.g.blocks.last().map(|block| block.hash.clone());
        let slot = 1_000 + u64::from(height) * 20;
        let mut seed = format!("script block {height} {previous:?}").into_bytes();
        for tx in &built {
            seed.extend(tx.hash.as_bytes());
        }
        let hashes = built.iter().map(|tx| tx.hash.clone()).collect();
        self.g.blocks.push(Block {
            hash: hash(&seed),
            height,
            slot,
            epoch,
            time: 1_596_059_091 + slot,
            previous,
            transactions: built,
        });
        hashes
    }
}

/// The scripted chain's horizon and target.
const EDGE_HORIZON: u32 = 33;
const EDGE_TARGET: u32 = 40;
const EDGE_END: u32 = 60;

/// What the edge chain's tests refer to by name.
struct Edges {
    g: Generated,
    /// The transaction at height 36 that spends the governance output of a
    /// name outside the fixed descriptors.
    governance_spend: String,
    cnight: String,
}

/// Mainnet profile, k 12. Height 1: one transaction with 250 cNIGHT outputs
/// (one height with more than 100 rows); 2: 130 auth outputs at the mapping
/// address (more than 100 of one asset at one address); 3: exactly 100
/// outputs at the reserve address; 4: the permissioned-candidates token,
/// one holder; 5: a council record with its datum; 6: a council-policy token
/// of another name at an ordinary address; 7: a newer council-policy output
/// at the council address without a datum (K5 skips it); 8: a mapping
/// output whose datum is never revealed; 9–30: thirty cNIGHT outputs a
/// block; from 33 on: spends of the earlier outputs in every block, the
/// governance token of height 6 spent at 36, outputs of 41 spent at 45.
fn edges(profile: &Profile) -> Edges {
    let key = |index: u8| {
        let mut key = [0x5a; 28];
        key[0] = index;
        key
    };
    let ordinary = |index: u8| enterprise_address(1, key(index));
    let unit = |asset: &dsa_chain::profiles::Asset| (asset.policy.clone(), asset.name.clone());
    let (cn_policy, cn_name) = unit(&profile.assets.cnight);
    let (auth_policy, auth_name) = unit(&profile.assets.auth);
    let (pc_policy, pc_name) = unit(&profile.assets.permissioned_candidates);
    let council_policy = profile.assets.council.policy.clone();
    let council_name = profile.assets.council.name.clone();
    let addresses = &profile.addresses;
    let mut s = Script::new();
    // 1
    let mut tx = s.tx().mint(&cn_policy, &cn_name, 250);
    for index in 0..250_u32 {
        tx = tx.output(
            Output::new(ordinary((index % 7) as u8), 1_500_000 + u64::from(index))
                .token(&cn_policy, &cn_name, 1),
        );
    }
    let wide = s.block(vec![tx])[0].clone();
    // 2
    let mut tx = s.tx().mint(&auth_policy, &auth_name, 130);
    for index in 0..130_i64 {
        tx = tx.output(
            Output::to(&addresses.mapping, 1_700_000)
                .token(&auth_policy, &auth_name, 1)
                .inline_datum(int_datum(index)),
        );
    }
    let mapping = s.block(vec![tx])[0].clone();
    // 3
    let mut tx = s.tx();
    for index in 0..100_u64 {
        tx = tx.output(Output::to(&addresses.reserve, 1_200_000 + index));
    }
    s.block(vec![tx]);
    // 4
    let tx = s.tx().mint(&pc_policy, &pc_name, 1).output(
        Output::new(ordinary(7), 2_000_000)
            .token(&pc_policy, &pc_name, 1)
            .inline_datum(int_datum(5_000)),
    );
    s.block(vec![tx]);
    // 5
    let tx = s.tx().mint(&council_policy, &council_name, 1).output(
        Output::to(&addresses.council, 2_000_000)
            .token(&council_policy, &council_name, 1)
            .inline_datum(int_datum(1_001)),
    );
    s.block(vec![tx]);
    // 6
    let tx = s
        .tx()
        .mint(&council_policy, "01", 1)
        .output(Output::new(ordinary(8), 2_000_000).token(&council_policy, "01", 1));
    let governance = s.block(vec![tx])[0].clone();
    // 7
    let tx = s
        .tx()
        .mint(&council_policy, "02", 1)
        .output(Output::to(&addresses.council, 2_000_000).token(&council_policy, "02", 1));
    s.block(vec![tx]);
    // 8
    let tx = s.tx().mint(&auth_policy, &auth_name, 1).output(
        Output::to(&addresses.mapping, 1_700_000)
            .token(&auth_policy, &auth_name, 1)
            .datum_hash(&hash(&int_datum(999_999))),
    );
    s.block(vec![tx]);
    // 9..=30
    for height in 9..=30_u16 {
        let spent = 2 * (height - 9);
        let mut tx = s
            .tx()
            .input(&wide, spent)
            .input(&wide, spent + 1)
            .mint(&cn_policy, &cn_name, 28);
        for index in 0..30_u8 {
            tx = tx.output(
                Output::new(ordinary(index % 5), 1_300_000 + u64::from(height))
                    .token(&cn_policy, &cn_name, 1),
            );
        }
        s.block(vec![tx]);
    }
    // 31, 32: empty blocks.
    s.block(Vec::new());
    s.block(Vec::new());
    let mut governance_spend = String::new();
    let mut created_41 = String::new();
    for height in 33..=EDGE_END {
        let spent = 100 + 2 * (height as u16 - 33);
        let mut transactions = vec![
            s.tx()
                .input(&wide, spent)
                .input(&wide, spent + 1)
                .output(Output::new(ordinary(3), 3_000_000).token(&cn_policy, &cn_name, 2)),
        ];
        match height {
            35 | 55 | 58 => {
                let first = match height {
                    35 => 0,
                    55 => 10,
                    _ => 20,
                };
                transactions.push(
                    s.tx()
                        .input(&mapping, first)
                        .input(&mapping, first + 1)
                        .output(
                            Output::to(&addresses.mapping, 1_800_000)
                                .token(&auth_policy, &auth_name, 2)
                                .inline_datum(int_datum(77_000 + i64::from(height))),
                        ),
                );
            }
            36 => transactions.push(
                s.tx()
                    .input(&governance, 0)
                    .output(Output::new(ordinary(9), 2_000_000).token(&council_policy, "01", 1)),
            ),
            45 => transactions.push(
                s.tx()
                    .input(&created_41, 0)
                    .output(Output::new(ordinary(4), 2_900_000).token(&cn_policy, &cn_name, 2)),
            ),
            _ => {}
        }
        let hashes = s.block(transactions);
        if height == 36 {
            governance_spend = hashes[1].clone();
        }
        if height == 41 {
            created_41 = hashes[0].clone();
        }
    }
    Edges {
        g: s.g,
        governance_spend,
        cnight: profile.assets.cnight.unit(),
    }
}

/// The edge chain converted at its horizon and target into `converted`.
async fn edge_conversion(
    profile: &Profile,
    g: &Generated,
    converted: &Database,
) -> Result<(Source, dsa_store::verify::Verification)> {
    let history = derive(profile, g).await?;
    let mut source = Source::new().await?;
    let headers = (1..=EDGE_TARGET + K)
        .map(|height| header(g.block(height).unwrap()))
        .collect::<Vec<_>>();
    source.start(g, &history, EDGE_TARGET, &headers).await?;
    let conversion = convert(converted, &source.database, profile, EDGE_HORIZON, DAY).await?;
    Ok((source, conversion))
}

fn edge_chain(profile: &Profile, g: &Generated, through: u32) -> Chain {
    let mut chain = Chain::from_generated(g, stake_prefix(&profile.network));
    chain.blocks_mut().retain(|block| block.height <= through);
    chain
}

/// Whether the database serves: the gate of a build that did not publish
/// raises SQLSTATE 55000.
async fn serving(database: &Database) -> Result<bool> {
    let client = common::connect(&database.config).await?;
    let position: bool = client
        .query_one("SELECT serving FROM dsa.position WHERE id=1", &[])
        .await?
        .get(0);
    let gate = client.query_one("SELECT dsa.gate()", &[]).await;
    if gate.is_ok() != position {
        return Err("the gate and the position disagree".into());
    }
    if let Err(error) = gate {
        assert_eq!(error.code().map(|code| code.code()), Some("55000"));
    }
    Ok(position)
}

/// The listings' paging edges give the conversion's content: one
/// height with 250 outputs of one asset (read through its transactions),
/// 130 outputs of one
/// asset at one address, exactly 100 outputs at an address (the second page
/// is read and empty), one holder of an asset, a governance output without a
/// datum, a datum never revealed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn listing_edges_give_the_conversion() -> Result {
    let profile = profile(1);
    let edges = edges(&profile);
    let converted = Database::new().await?;
    let (source, conversion) = edge_conversion(&profile, &edges.g, &converted).await?;
    let rebuilt = Database::new().await?;
    empty(&rebuilt, &profile).await?;
    let fake = Fake::with_chain(edge_chain(&profile, &edges.g, EDGE_END), None).await;
    let (_stopper, stop) = Stop::new();
    let report = build(&profile, &fake, &rebuilt, EDGE_HORIZON, EDGE_TARGET, stop)
        .await
        .map_err(|error| error.report().to_string())?;
    if let Err(error) = same(&report.verified, &conversion, "edges") {
        let rows = harness::diff(&rebuilt.config, &converted.config).await?;
        return Err(format!("{error}; rows {rows}").into());
    }
    let paths = fake.paths();
    let reserve = format!(
        "/addresses/{}/utxos?count=100&page=2&order=asc",
        profile.addresses.reserve
    );
    let mapping = format!(
        "/addresses/{}/utxos?count=100&page=2&order=asc",
        profile.addresses.mapping
    );
    let single = format!(
        "/assets/{}/transactions?count=100&page=1&order=asc&from=1&to=1",
        edges.cnight
    );
    for wanted in [&reserve, &mapping, &single] {
        assert!(
            paths.iter().any(|path| path.contains(wanted.as_str())),
            "no request {wanted}"
        );
    }
    let client = common::connect(&rebuilt.config).await?;
    let council: i64 = client
        .query_one(
            "SELECT count(*) FROM serve.tx_out o WHERE o.address=$1",
            &[&profile.addresses.council],
        )
        .await?
        .get(0);
    let unrevealed: i64 = client
        .query_one(
            "SELECT count(*) FROM serve.tx_out o WHERE o.data_hash=$1",
            &[&hex::decode(hash(&int_datum(999_999)))?],
        )
        .await?
        .get(0);
    let unrevealed_datum: i64 = client
        .query_one(
            "SELECT count(*) FROM serve.datum WHERE hash=$1",
            &[&hex::decode(hash(&int_datum(999_999)))?],
        )
        .await?
        .get(0);
    println!(
        "{}",
        json!({"event":"listing_edges","requests":fake.paths().len(),"calls":report.calls,
            "council_outputs":council,"unrevealed_outputs":unrevealed,"unrevealed_datum_rows":unrevealed_datum})
    );
    assert_eq!((council, unrevealed, unrevealed_datum), (2, 1, 0));
    rebuilt.cleanup().await?;
    converted.cleanup().await?;
    source.database.cleanup().await
}

/// Blocks arrive and a branch replaces the provider's tip while
/// the listings are read; outputs listed on one page are spent before the
/// next one, outputs are created and spent again. The build still equals
/// the conversion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn movement_while_listing_keeps_the_conversion() -> Result {
    let profile = profile(1);
    let edges = edges(&profile);
    let converted = Database::new().await?;
    let (source, conversion) = edge_conversion(&profile, &edges.g, &converted).await?;
    let rebuilt = Database::new().await?;
    empty(&rebuilt, &profile).await?;
    let all = edges.g.blocks.clone();
    let start = EDGE_TARGET + K;
    let seen = Arc::new(AtomicUsize::new(0));
    let moves = Arc::new(Mutex::new(Vec::<String>::new()));
    let hook: Hook = {
        let (seen, moves) = (seen.clone(), moves.clone());
        Arc::new(move |path: &str, chain: &mut Chain, _| {
            let listing = path.contains("/utxos?") || path.contains("/transactions?");
            if !listing {
                return None;
            }
            let count = seen.fetch_add(1, Ordering::SeqCst) + 1;
            if count % 2 != 0 {
                return None;
            }
            let blocks = chain.blocks_mut();
            let height = blocks.last().map_or(0, |block| block.height);
            let canonical = blocks.last().map(|b| b.hash.clone())
                == all.get(height as usize - 1).map(|b| b.hash.clone());
            let mut moves = moves.lock().unwrap();
            if !canonical {
                // The branch is abandoned: the canonical blocks come back.
                blocks.truncate(blocks.len() - 2);
                blocks.extend(all[blocks.len()..height as usize].iter().cloned());
                moves.push(format!("restored {height}"));
            } else if height == start + 3 && !moves.iter().any(|m| m.starts_with("branch")) {
                let keep = blocks.len() - 2;
                let branch = generate::branch(&blocks[keep..], 77);
                blocks.truncate(keep);
                blocks.extend(branch);
                moves.push(format!("branch at {height}"));
            } else if (height as usize) < all.len() {
                blocks.push(all[height as usize].clone());
                moves.push(format!("block {}", height + 1));
            }
            None
        })
    };
    let fake = Fake::with_chain(edge_chain(&profile, &edges.g, start), Some(hook)).await;
    let (_stopper, stop) = Stop::new();
    let report = build(&profile, &fake, &rebuilt, EDGE_HORIZON, EDGE_TARGET, stop)
        .await
        .map_err(|error| error.report().to_string())?;
    if let Err(error) = same(&report.verified, &conversion, "movement") {
        let rows = harness::diff(&rebuilt.config, &converted.config).await?;
        return Err(format!("{error}; rows {rows}").into());
    }
    let moves = moves.lock().unwrap().clone();
    println!(
        "{}",
        json!({"event":"movement_while_listing","listing_requests":seen.load(Ordering::SeqCst),
            "moves":moves,"calls":report.calls})
    );
    assert!(moves.iter().any(|m| m.starts_with("branch")));
    assert!(moves.iter().any(|m| m.starts_with("restored")));
    assert!(moves.iter().filter(|m| m.starts_with("block")).count() >= 5);
    rebuilt.cleanup().await?;
    converted.cleanup().await?;
    source.database.cleanup().await
}

/// One refused build of the edge chain under `hook`: the error and a
/// database that does not serve.
async fn refused(profile: &Profile, g: &Generated, hook: Hook) -> Result<(follow::Error, bool)> {
    let rebuilt = Database::new().await?;
    empty(&rebuilt, profile).await?;
    let fake = Fake::with_chain(edge_chain(profile, g, EDGE_END), Some(hook)).await;
    let (_stopper, stop) = Stop::new();
    let error = build(profile, &fake, &rebuilt, EDGE_HORIZON, EDGE_TARGET, stop)
        .await
        .err()
        .ok_or("the build was not refused")?;
    let serves = serving(&rebuilt).await?;
    rebuilt.cleanup().await?;
    Ok((error, serves))
}

fn code(error: &follow::Error) -> String {
    error.report()["error"]
        .as_str()
        .unwrap_or_default()
        .to_owned()
}

/// A listing that lacks an output (the supply proof), a list of
/// transactions that lacks one whose input the window spends (conservation)
/// and a provider total that does not match (the supply proof) each refuse
/// the build; the gate stays closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn incomplete_answers_refuse_the_build_and_nothing_serves() -> Result {
    let profile = profile(1);
    let edges = edges(&profile);
    let cnight = edges.cnight.clone();
    // A row missing from the listing of an asset.
    let listing = format!("/assets/{cnight}/utxos?");
    let missing_row: Hook = Arc::new(move |path: &str, chain: &mut Chain, _| {
        if !path.contains(&listing) {
            return None;
        }
        let answer = chain.answer(path);
        let mut rows: Vec<serde_json::Value> = serde_json::from_str(&answer.body).ok()?;
        if rows.is_empty() || rows.len() >= 100 {
            return None;
        }
        rows.pop();
        Some(Behavior::json(serde_json::Value::Array(rows)))
    });
    // A transaction from H on missing from every list.
    let hidden = edges.governance_spend.clone();
    let missing_transaction: Hook = Arc::new(move |path: &str, chain: &mut Chain, _| {
        if !path.contains("/transactions?") {
            return None;
        }
        let answer = chain.answer(path);
        let rows: Vec<serde_json::Value> = serde_json::from_str(&answer.body).ok()?;
        let kept: Vec<_> = rows
            .into_iter()
            .filter(|row| row["tx_hash"] != hidden.as_str())
            .collect();
        Some(Behavior::json(serde_json::Value::Array(kept)))
    });
    // A total one unit above the outputs.
    let total = format!("/api/v0/assets/{cnight}");
    let wrong_total: Hook = Arc::new(move |path: &str, chain: &mut Chain, _| {
        if path != total {
            return None;
        }
        let mut answer: serde_json::Value = serde_json::from_str(&chain.answer(path).body).ok()?;
        let quantity: u128 = answer["quantity"].as_str()?.parse().ok()?;
        answer["quantity"] = json!((quantity + 1).to_string());
        Some(Behavior::json(answer))
    });
    let mut outcomes = Vec::new();
    for (name, hook, expected) in [
        (
            "missing_listed_output",
            missing_row,
            "rebuild_supply_mismatch",
        ),
        (
            "missing_transaction",
            missing_transaction,
            "asset_conservation",
        ),
        (
            "wrong_provider_total",
            wrong_total,
            "rebuild_supply_mismatch",
        ),
    ] {
        let (error, serves) = refused(&profile, &edges.g, hook).await?;
        outcomes.push(json!({"case":name,"error":error.report(),"serving":serves}));
        assert_eq!(code(&error), expected, "{name}: {}", error.report());
        assert!(!serves, "{name}: the database serves");
    }
    println!("{}", json!({"event":"refusals","outcomes":outcomes}));
    Ok(())
}

/// A provider of another network ends a build at its first request,
/// before the database changes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn a_provider_of_another_network_ends_the_build_before_it_starts() -> Result {
    let seed = 7;
    let mut random = Random::new(seed ^ 0x0d2b_d2b0);
    let case = Case::new(seed, &mut random).await?;
    let (expected, other) = if case.profile.network == "mainnet" {
        (764_824_073, 1)
    } else {
        (1, 764_824_073)
    };
    let rebuilt = Database::new().await?;
    empty(&rebuilt, &case.profile).await?;
    let fake = Fake::start(&case.g, &case.profile).await;
    fake.chain.lock().unwrap().network_magic = Some(other);
    let (_stopper, stop) = Stop::new();
    let error = build(
        &case.profile,
        &fake,
        &rebuilt,
        case.horizon,
        case.target,
        stop,
    )
    .await
    .err()
    .ok_or("the build ran on a provider of another network")?;
    let report = error.report();
    assert_eq!(code(&error), "provider_network_mismatch", "{report}");
    assert_eq!(report["detail"]["expected_magic"], expected, "{report}");
    assert_eq!(report["detail"]["received_magic"], other, "{report}");
    assert_eq!(fake.paths(), vec!["/api/v0/genesis".to_owned()]);
    // The new database is still blank: no layout, no build.
    let blank: bool = common::connect(&rebuilt.config)
        .await?
        .query_one(
            "SELECT to_regclass('dsa.identity') IS NULL AND to_regnamespace('dsa_rebuild') IS NULL",
            &[],
        )
        .await?
        .get(0);
    assert!(blank, "the build changed the database");
    rebuilt.cleanup().await
}

/// What the other commands make of a stopped build, before its rows below
/// the horizon are written and after: the service refuses to start on it as
/// not serving with the reason `rebuilding`, also with a snapshot file to
/// restore; a restore refuses it as not empty; the status document calls it
/// unready.
async fn refused_while_building(
    case: &Case,
    database: &Database,
    file: &std::path::Path,
) -> Result {
    use dsa_service::{config, retry::WallTime, run, status, upkeep::IndexPolicy};
    let open: run::Open = {
        let config = database.config.clone();
        Arc::new(move |_| {
            let config = config.clone();
            Box::pin(async move { dsa_store::Store::connect(config, None).await })
        })
    };
    let settings = config::Settings::check(config::Input {
        listen: "127.0.0.1:0".to_owned(),
        reader: database.reader.clone(),
        snapshot: Some(file.to_owned()),
        expect_content_digest: None,
        retention_seconds: None,
        change_retention: false,
        snapshot_dir: None,
        snapshot_keep: 3,
        snapshot_at: "00:30".to_owned(),
        health_max_lag_blocks: 9,
        database_fatal_after_seconds: 60,
    })
    .map_err(|invalid| format!("{invalid:?}"))?;
    let fake = Fake::start(&case.g, &case.profile).await;
    let (_stopper, stop) = Stop::new();
    let service = run::run(run::Context {
        profile: case.profile.clone(),
        provider: fake.provider(),
        open,
        settings,
        follower: Settings::default(),
        index_policy: IndexPolicy::default(),
        refresh_interval: Duration::from_secs(20),
        upkeep_tick: Duration::from_secs(60),
        listener: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
        time: Arc::new(WallTime),
        stop,
    });
    let error = tokio::time::timeout(Duration::from_secs(120), service)
        .await?
        .err()
        .ok_or("the service started on a database being built")?;
    assert_eq!(code(&error), "database_not_serving", "{}", error.report());
    assert_eq!(error.report()["detail"]["reason"], "rebuilding");
    assert!(fake.paths().is_empty(), "the service asked the provider");
    let mut store = dsa_store::Store::connect(database.config.clone(), None).await?;
    let refused = store
        .restore(&case.profile, file, None)
        .await
        .err()
        .ok_or("a restore loaded a database being built")?;
    assert_eq!(refused.report()["error"], "destination_not_empty");
    let view = store.service_view(&case.profile, None).await?;
    let document = serde_json::to_value(status::compose(
        Some(&view),
        None,
        &status::Settings::new(&case.profile.network),
        0,
    ))?;
    assert_eq!(document["serving"], "unready");
    assert_eq!(document["position"]["reason"], "rebuilding");
    assert_eq!(document["identity"]["origin"]["kind"], "rebuilding");
    Ok(())
}

/// A build stopped at any point continues to the conversion's
/// digest; until it publishes the database never serves, and dropping it
/// discards the build.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn a_build_killed_at_any_point_continues_to_the_same_digest() -> Result {
    let seed = setting("DSA_REBUILD_KILL_SEED", 5);
    let mut random = Random::new(seed ^ 0x0d2b_d2b0);
    let case = Case::new(seed, &mut random).await?;
    let mut source = Source::new().await?;
    let converted = Database::new().await?;
    let conversion = case.convert(&mut source, &converted).await?;
    // An uninterrupted build counts the requests.
    let whole = Database::new().await?;
    empty(&whole, &case.profile).await?;
    let fake = Fake::start(&case.g, &case.profile).await;
    let (_stopper, stop) = Stop::new();
    build(
        &case.profile,
        &fake,
        &whole,
        case.horizon,
        case.target,
        stop,
    )
    .await
    .map_err(|error| error.report().to_string())?;
    let total = fake.paths().len();
    whole.cleanup().await?;
    let points = setting("DSA_REBUILD_KILL_POINTS", 6) as usize;
    let mut outcomes = Vec::new();
    for point in 1..=points {
        let kill_at = (total * point / (points + 1)).max(1);
        let rebuilt = Database::new().await?;
        empty(&rebuilt, &case.profile).await?;
        let seen = Arc::new(AtomicUsize::new(0));
        let (killer, mut killed) = watch::channel(false);
        let hook: Hook = {
            let seen = seen.clone();
            Arc::new(move |_: &str, _: &mut Chain, _| {
                if seen.fetch_add(1, Ordering::SeqCst) + 1 == kill_at {
                    let _ = killer.send(true);
                }
                None
            })
        };
        let fake = Fake::with_chain(
            Chain::from_generated(&case.g, stake_prefix(&case.profile.network)),
            Some(hook),
        )
        .await;
        let (_stopper, stop) = Stop::new();
        let first = tokio::select! {
            result = build(&case.profile, &fake, &rebuilt, case.horizon, case.target, stop) => Some(result),
            _ = killed.wait_for(|killed| *killed) => None,
        };
        if first.is_some() {
            return Err(format!("kill point {kill_at} of {total} came after the end").into());
        }
        let client = common::connect(&rebuilt.config).await?;
        let phase: Option<String> = client
            .query_opt("SELECT phase FROM dsa_rebuild.progress WHERE id=1", &[])
            .await
            .ok()
            .flatten()
            .map(|row| row.get(0));
        let serves = serving(&rebuilt).await.unwrap_or(false);
        assert!(!serves, "kill at {kill_at}: the database serves");
        // What `status` prints of a build: the stored service state.
        let stored = client
            .query_opt(
                "SELECT state,reason,detail FROM dsa.service_state WHERE id=1",
                &[],
            )
            .await?
            .map(|row| {
                json!({"state": row.get::<_, String>(0), "reason": row.get::<_, String>(1),
                    "detail": row.get::<_, serde_json::Value>(2)})
            });
        let (_stopper, stop) = Stop::new();
        let report = build(
            &case.profile,
            &fake,
            &rebuilt,
            case.horizon,
            case.target,
            stop,
        )
        .await
        .map_err(|error| format!("resume after {kill_at}: {}", error.report()))?;
        same(
            &report.verified,
            &conversion,
            &format!("killed at request {kill_at}"),
        )?;
        assert!(serving(&rebuilt).await?);
        // A request is counted once: never more than the provider answered.
        let answered = fake.paths().len() as i64;
        assert!(
            report.calls <= answered + IN_FLIGHT,
            "kill at {kill_at}: {} requests counted, {answered} answered",
            report.calls
        );
        let outcome = json!({"kill_at_request":kill_at,"phase":phase,"stored_state":stored,
            "resumed":report.resumed,"calls":report.calls,"answered":answered});
        println!("{}", json!({"event":"kill","outcome":outcome}));
        outcomes.push(outcome);
        rebuilt.cleanup().await?;
    }
    // A build stopped by request, before the window and inside it, then
    // continued, reports the requests the provider answered, and at most
    // those in flight at the stop more. While it is stopped, the service, a
    // restore of a snapshot file of the conversion and the status document
    // treat it as a database being built.
    let files = tempfile::tempdir()?;
    let file = files.path().join("conversion.snapshot");
    dsa_store::Store::connect(converted.config.clone(), None)
        .await?
        .snapshot(&case.profile, &file)
        .await
        .map_err(harness::named)?;
    for in_window in [false, true] {
        let rebuilt = Database::new().await?;
        empty(&rebuilt, &case.profile).await?;
        let fake = Fake::start(&case.g, &case.profile).await;
        let (stopper, stop) = Stop::new();
        let run = build(
            &case.profile,
            &fake,
            &rebuilt,
            case.horizon,
            case.target,
            stop,
        );
        let stopping = async {
            let mut window_from = None;
            loop {
                let answered = fake.paths().len();
                let due = if in_window {
                    if window_from.is_none()
                        && stored_phase(&rebuilt).await.as_deref() == Some("window")
                    {
                        window_from = Some(answered);
                    }
                    window_from.is_some_and(|from| answered >= from + 5)
                } else {
                    answered >= total / 2
                };
                if due {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let _ = stopper.send(true);
        };
        let (result, ()) = tokio::join!(run, stopping);
        let error = result.err().ok_or("the stopped build published")?;
        assert_eq!(code(&error), "stop_requested");
        let phase = stored_phase(&rebuilt).await;
        refused_while_building(&case, &rebuilt, &file)
            .await
            .map_err(|error| format!("stopped in {phase:?}: {error}"))?;
        let (_stopper, stop) = Stop::new();
        let report = build(
            &case.profile,
            &fake,
            &rebuilt,
            case.horizon,
            case.target,
            stop,
        )
        .await
        .map_err(|error| format!("continued after a stop in {phase:?}: {}", error.report()))?;
        same(
            &report.verified,
            &conversion,
            &format!("stopped in {phase:?}"),
        )?;
        let answered = fake.paths().len() as i64;
        assert!(
            (answered..=answered + IN_FLIGHT).contains(&report.calls),
            "stopped in {phase:?}: {} requests counted, {answered} answered",
            report.calls
        );
        assert_eq!(in_window, phase.as_deref() == Some("window"));
        println!(
            "{}",
            json!({"event":"stopped_and_continued","phase":phase,"calls":report.calls,"answered":answered})
        );
        rebuilt.cleanup().await?;
    }
    // A discarded build: stopped, then the database dropped.
    let discarded = Database::new().await?;
    empty(&discarded, &case.profile).await?;
    let fake = Fake::start(&case.g, &case.profile).await;
    let (stopper, stop) = Stop::new();
    let run = build(
        &case.profile,
        &fake,
        &discarded,
        case.horizon,
        case.target,
        stop,
    );
    let stopping = async {
        while fake.paths().len() < total / 2 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let _ = stopper.send(true);
    };
    let (result, ()) = tokio::join!(run, stopping);
    let error = result.err().ok_or("the stopped build published")?;
    assert_eq!(code(&error), "stop_requested");
    assert!(!serving(&discarded).await?);
    let stopped: (String, String) = common::connect(&discarded.config)
        .await?
        .query_one("SELECT state,reason FROM dsa.service_state WHERE id=1", &[])
        .await
        .map(|row| (row.get(0), row.get(1)))?;
    assert_eq!(stopped, ("stopped".to_owned(), "stop_requested".to_owned()));
    discarded.cleanup().await?;
    println!(
        "{}",
        json!({"event":"kills","requests_whole_build":total,"points":outcomes.len(),
            "discarded_state":stopped})
    );
    assert_eq!(outcomes.len(), points);
    converted.cleanup().await?;
    source.database.cleanup().await
}

/// The fixed `multi_asset` IDs of Mainnet and Preprod that a build writes
/// are the IDs the published snapshot files hold: each `*.snapshot` file of
/// the directory in DSA_RELEASE_FILES is restored and its fixed descriptors
/// compared with the network's table. Without the variable the test says so
/// and passes: the files are not part of the repository.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn fixed_descriptor_ids_are_those_of_the_published_files() -> Result {
    let Ok(directory) = std::env::var("DSA_RELEASE_FILES") else {
        println!("DSA_RELEASE_FILES is not set: no published file is compared");
        return Ok(());
    };
    let mut compared = Vec::new();
    for entry in std::fs::read_dir(&directory)? {
        let path = entry?.path();
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default()
            .to_owned();
        if !name.ends_with(".snapshot") {
            continue;
        }
        let network = name.split('-').next().unwrap_or_default();
        let profile = Profile::load(network)?;
        let database = Database::new().await?;
        let mut store = dsa_store::Store::connect(database.config.clone(), None).await?;
        store
            .restore(&profile, &path, None)
            .await
            .map_err(harness::named)?;
        let stored: BTreeMap<String, i64> = common::connect(&database.config)
            .await?
            .query(
                "SELECT encode(policy,'hex')||encode(name,'hex'),id FROM serve.multi_asset",
                &[],
            )
            .await?
            .into_iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();
        let table = dsa_chain::profiles::fixed_descriptor_ids(&profile);
        let differing: Vec<_> = table
            .iter()
            .filter(|(unit, id)| stored.get(unit) != Some(id))
            .map(|(unit, id)| json!({"unit": unit, "table": id, "file": stored.get(unit)}))
            .collect();
        compared.push(json!({"file": name, "fixed": table.len(), "differing": differing}));
        drop(store);
        database.cleanup().await?;
        assert!(differing.is_empty(), "{name}: {differing:?}");
    }
    println!(
        "{}",
        json!({"event":"fixed_descriptor_ids","compared":compared})
    );
    assert!(!compared.is_empty(), "no snapshot file in {directory}");
    Ok(())
}

/// A rebuilt database follows on: after the build, the follower makes more
/// blocks final while the oldest heights age past the retention, so that
/// the horizon moves and the outputs spent before it stay recorded; after
/// each round the database equals a conversion of the full history at its
/// new horizon and target. DSA_REBUILD_FOLLOW_CASES chains (default 4).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn a_rebuilt_database_follows_through_a_horizon_advance() -> Result {
    let cases = setting("DSA_REBUILD_FOLLOW_CASES", 4);
    let parallel = setting("DSA_REBUILD_PARALLEL", 4).clamp(1, 16);
    let workers = (0..parallel).map(|worker| async move {
        let mut source = Source::new().await?;
        let converted = Database::new().await?;
        let rebuilt = Database::new().await?;
        let (mut chains, mut advances, mut rounds) = (0_u64, 0_u64, 0_u64);
        let mut seed = 1_000 + worker;
        while seed < 1_000 + cases {
            let mut random = Random::new(seed ^ 0x0d2b_d2b0);
            let case = Case::new(seed, &mut random).await?;
            let conversion = case.convert(&mut source, &converted).await?;
            empty(&rebuilt, &case.profile).await?;
            let fake = Fake::start(&case.g, &case.profile).await;
            let (_stopper, stop) = Stop::new();
            let report = build(&case.profile, &fake, &rebuilt, case.horizon, case.target, stop)
                .await
                .map_err(|error| format!("seed {seed}: {}", error.report()))?;
            same(&report.verified, &conversion, &format!("seed {seed} built"))?;
            let last_target = case.g.blocks.last().unwrap().height - K;
            let mut target = case.target;
            while target < last_target {
                let next = (target + 1 + random.below(8) as u32).min(last_target);
                let aged = target.saturating_sub(random.below(4) as u32);
                common::connect(&rebuilt.config)
                    .await?
                    .execute(
                        "UPDATE dsa.first_served SET first_served_at=first_served_at-interval '2 days' WHERE height<=$1",
                        &[&(aged as i32)],
                    )
                    .await?;
                let mut store = dsa_store::Store::connect(rebuilt.config.clone(), None).await?;
                let before = store
                    .follow_state(&case.profile)
                    .await
                    .map_err(harness::named)?
                    .horizon
                    .height;
                support::follow_to(
                    &case.profile,
                    &fake,
                    &rebuilt.config,
                    Settings {
                        prune_interval: Duration::from_secs(60),
                        ..support::until(next)
                    },
                )
                .await
                .map_err(|error| format!("seed {seed} to {next}: {}", error.report()))?;
                let state = store
                    .follow_state(&case.profile)
                    .await
                    .map_err(harness::named)?;
                advances += u64::from(state.horizon.height > before);
                let above = store
                    .headers_above(state.target.height)
                    .await
                    .map_err(harness::named)?;
                let headers = (source.through + 1..=state.target.height)
                    .map(|height| header(case.g.block(height).unwrap()))
                    .chain(above)
                    .collect::<Vec<_>>();
                source
                    .extend(&case.g, &case.history, state.target.height, &headers)
                    .await?;
                let conversion = convert(
                    &converted,
                    &source.database,
                    &case.profile,
                    state.horizon.height,
                    DAY,
                )
                .await?;
                let verified = store.verify(&case.profile).await.map_err(harness::named)?;
                let context = format!(
                    "seed {seed} followed to H {} T {}",
                    state.horizon.height, state.target.height
                );
                if let Err(error) = same(&verified, &conversion, &context) {
                    let rows = harness::diff(&rebuilt.config, &converted.config).await?;
                    return Err(format!("{error}; rows {rows}").into());
                }
                target = next;
                rounds += 1;
            }
            chains += 1;
            seed += parallel;
        }
        source.database.cleanup().await?;
        converted.cleanup().await?;
        rebuilt.cleanup().await?;
        Result::<(u64, u64, u64)>::Ok((chains, rounds, advances))
    });
    let (mut chains, mut rounds, mut advances) = (0, 0, 0);
    for result in futures_util::future::join_all(workers).await {
        let (c, r, a) = result?;
        chains += c;
        rounds += r;
        advances += a;
    }
    println!(
        "{}",
        json!({"event":"rebuilt_then_followed","chains":chains,"rounds":rounds,"horizon_advances":advances})
    );
    assert_eq!(chains, cases);
    assert!(advances > 0, "no horizon moved");
    Ok(())
}

/// The command refuses a window longer than the retention and a horizon
/// above the target before it connects anywhere.
#[test]
fn the_command_checks_its_positions_before_connecting() {
    for (arguments, error, flag) in [
        (
            vec!["--retention-seconds", "86400", "--window-seconds", "172800"],
            "window_above_retention",
            "--window-seconds",
        ),
        (
            vec!["--horizon-block", "10", "--target-block", "5"],
            "horizon_above_target",
            "--horizon-block",
        ),
    ] {
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_db-sync-alt"))
            .args([
                "rebuild",
                "--database",
                "host=127.0.0.1 port=1 user=nobody dbname=none",
                "--network",
                "preprod",
                "--provider-key-file",
                "/nonexistent",
            ])
            .args(&arguments)
            .env_clear()
            .output()
            .expect("the binary runs");
        assert_eq!(output.status.code(), Some(2), "{arguments:?}");
        let refusal: serde_json::Value =
            serde_json::from_slice(output.stderr.split(|byte| *byte == b'\n').next().unwrap())
                .expect("a JSON refusal");
        assert_eq!(
            refusal,
            json!({"error": error, "flag": flag}),
            "{arguments:?}"
        );
    }
}
