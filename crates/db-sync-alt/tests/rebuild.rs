//! The build from the provider alone against PostgreSQL in DSA_TEST_DATABASE
//! and the fake provider of the testkit: a generated chain converted from
//! its full history at horizon H and target T, and the same chain built from
//! the provider's listings at the same H and T, give the same content and
//! state digests, supply and position. DSA_REBUILD_CASES sets the number of
//! generated chains of the property test (default 16), DSA_REBUILD_PARALLEL
//! how many run at once (default 4), DSA_REBUILD_KILL_POINTS how many more
//! points, spread over the requests, a build is stopped at besides one in
//! every phase (default 0) and DSA_REBUILD_FOLLOW_CASES the
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
        atomic::{AtomicBool, AtomicUsize, Ordering},
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
use futures_util::StreamExt;
use harness::{
    K, Random, Result, Source, convert, derive, generated, header, profile, reset, same,
};
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

/// Requests a continued run sends before it reaches the phase it stopped
/// in: the provider's network, and the tip a stopped window starts from.
const RUN_START: usize = 4;

/// The choices of the kill test's builds: its case's H and T, `batch` items
/// stored at once (0: the default).
fn kill_options(case: &Case, batch: usize) -> Options {
    Options {
        horizon_block: Some(case.horizon),
        target_block: Some(case.target),
        window_seconds: None,
        retention_seconds: DAY,
        shortest_window_seconds: 0,
        batch_items: batch,
    }
}

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
    let options = Options {
        horizon_block: Some(horizon),
        target_block: Some(target),
        window_seconds: None,
        retention_seconds: DAY,
        shortest_window_seconds: 0,
        batch_items: 0,
    };
    build_with(profile, fake, database, options, stop).await
}

/// One build into `database` from `fake` with the given choices.
async fn build_with(
    profile: &Profile,
    fake: &Fake,
    database: &Database,
    options: Options,
    stop: Stop,
) -> std::result::Result<Report, follow::Error> {
    let (progress, _view) = watch::channel(Progress::default());
    build_observed(profile, fake, database, options, stop, progress).await
}

/// `build_with`, the build's progress (its phase until the window) sent to
/// `progress`.
async fn build_observed(
    profile: &Profile,
    fake: &Fake,
    database: &Database,
    options: Options,
    stop: Stop,
    progress: watch::Sender<Progress>,
) -> std::result::Result<Report, follow::Error> {
    rebuild::rebuild(rebuild::Context {
        profile,
        provider: fake.provider(),
        connect: connect(&database.config),
        settings: Settings {
            max_in_flight: 4,
            fatal_after: Duration::from_secs(600),
            ..Settings::default()
        },
        options,
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
    /// The flagged rows of the generated chains.
    flagged: Flagged,
    burns: Burns,
    requests: BTreeMap<String, u64>,
    calls: i64,
    /// The chains whose build was refused or differs from the conversion.
    failures: Vec<serde_json::Value>,
}

/// The rows of the generated chains that a transaction's answer flags or
/// that an invalid transaction does not spend.
#[derive(Default, Debug, Clone, Copy)]
struct Flagged {
    collateral_also_spent: usize,
    collateral_not_spent: usize,
    unspent_returns: usize,
    references: usize,
    invalid_relevant_inputs: usize,
}

impl Flagged {
    fn of(counts: &Counts) -> Self {
        Self {
            collateral_also_spent: counts.collateral_also_spent,
            collateral_not_spent: counts.collateral_not_spent,
            unspent_returns: counts.unspent_returns,
            references: counts.references,
            invalid_relevant_inputs: counts.invalid_relevant_inputs,
        }
    }

    fn json(&self) -> serde_json::Value {
        json!({"collateral_also_spent": self.collateral_also_spent,
            "collateral_not_spent": self.collateral_not_spent,
            "unspent_returns": self.unspent_returns, "references": self.references,
            "invalid_relevant_inputs": self.invalid_relevant_inputs})
    }

    fn add(&mut self, other: Flagged) {
        self.collateral_also_spent += other.collateral_also_spent;
        self.collateral_not_spent += other.collateral_not_spent;
        self.unspent_returns += other.unspent_returns;
        self.references += other.references;
        self.invalid_relevant_inputs += other.invalid_relevant_inputs;
    }
}

/// Complete burns of an eligible unit (a valid transaction that burns it and
/// creates no output holding it), by their height against H and T.
#[derive(Default, Debug, Clone, Copy)]
struct Burns {
    below_horizon: usize,
    in_window: usize,
    after_target: usize,
    /// At or after H, of an output created at or after H.
    created_since_horizon: usize,
}

impl Burns {
    fn add(&mut self, other: Burns) {
        self.below_horizon += other.below_horizon;
        self.in_window += other.in_window;
        self.after_target += other.after_target;
        self.created_since_horizon += other.created_since_horizon;
    }

    fn of(case: &Case) -> Burns {
        let rule = dsa_chain::assets::AssetRule::from_profile(&case.profile);
        let mut created_at: BTreeMap<&str, u32> = BTreeMap::new();
        let mut burns = Burns::default();
        for block in &case.g.blocks {
            for built in &block.transactions {
                created_at.insert(&built.hash, block.height);
                let Some(facts) = case.g.facts.get(&built.hash) else {
                    continue;
                };
                if !facts.valid {
                    continue;
                }
                let mut net: BTreeMap<(&str, &str), i128> = BTreeMap::new();
                for token in &facts.mint {
                    *net.entry((&token.policy, &token.name)).or_default() += token.quantity;
                }
                for ((policy, name), quantity) in net {
                    let held = facts.created().iter().any(|(_, output)| {
                        output.tokens.iter().any(|token| {
                            token.policy == policy && token.name == name && token.quantity > 0
                        })
                    });
                    if quantity >= 0 || held || !rule.allows(policy, name) {
                        continue;
                    }
                    if block.height < case.horizon {
                        burns.below_horizon += 1;
                    } else {
                        if block.height <= case.target {
                            burns.in_window += 1;
                        } else {
                            burns.after_target += 1;
                        }
                        let young = facts.spent().iter().any(|input| {
                            created_at
                                .get(input.hash.as_str())
                                .is_some_and(|height| *height >= case.horizon)
                        });
                        burns.created_since_horizon += usize::from(young);
                    }
                }
            }
        }
        burns
    }
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
        let g =
            generated(&profile, seed, 1, blocks, seed.is_multiple_of(4), true).known_from_start();
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

/// One property case: the build equals the conversion. A build that is
/// refused or differs is recorded in `totals.failures`; an error of the test's
/// own databases ends the case with an error.
async fn equivalence(
    seed: u64,
    source: &mut Source,
    converted: &Database,
    rebuilt: &Database,
    totals: &mut Totals,
) -> Result {
    let mut random = Random::new(seed ^ 0x0d2b_d2b0);
    let case = Case::new(seed, &mut random).await?;
    let burns = Burns::of(&case);
    let conversion = case.convert(source, converted).await?;
    empty(rebuilt, &case.profile).await?;
    let fake = Fake::start(&case.g, &case.profile).await;
    let (_stopper, stop) = Stop::new();
    let context = format!("seed {seed} at H {} T {}", case.horizon, case.target);
    let failure = |kind: &str, code: &str, detail: serde_json::Value| {
        json!({"seed": seed, "horizon": case.horizon, "target": case.target, "kind": kind,
            "code": code, "burns": format!("{burns:?}"), "detail": detail})
    };
    let report = match build(
        &case.profile,
        &fake,
        rebuilt,
        case.horizon,
        case.target,
        stop,
    )
    .await
    {
        Ok(report) => report,
        Err(error) => {
            let report = error.report();
            let failed = failure("refused", &code(&error), report);
            println!(
                "{}",
                json!({"event":"rebuild_equivalence_failure","failure":failed})
            );
            totals.failures.push(failed);
            return Ok(());
        }
    };
    // The digests, then every row with its ID: the digest holds natural keys
    // only, so the IDs and ordinals of the rows below H are compared here,
    // and the descriptor IDs held to the build's rule.
    let rows = harness::diff(&rebuilt.config, &converted.config).await?;
    let ids = descriptor_ids(&case.profile, rebuilt).await?;
    let compared = same(&report.verified, &conversion, &context)
        .map_err(|error| error.to_string())
        .and_then(|()| {
            if rows == json!({}) && ids == json!([]) {
                Ok(())
            } else {
                Err("rows differ".to_owned())
            }
        });
    if let Err(error) = compared {
        let failed = failure(
            "differs",
            "differs",
            json!({"same": error, "rows": rows, "descriptor_ids": ids}),
        );
        println!(
            "{}",
            json!({"event":"rebuild_equivalence_failure","failure":failed})
        );
        totals.failures.push(failed);
        return Ok(());
    }
    totals.chains += 1;
    totals.burns.add(burns);
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
    totals.flagged.add(Flagged::of(&case.g.counts));
    totals.calls += report.calls;
    for (family, count) in &report.requests {
        *totals.requests.entry(family.clone()).or_default() += count;
    }
    Ok(())
}

/// The descriptor IDs a build wrote, which `harness::diff` leaves out (it
/// renders an asset row's descriptor by its policy and name). A conversion
/// of a test chain numbers the descriptors in the order its source first
/// saw them, which is the order of the network's table of fixed descriptors
/// only on the real networks; so a build is held to its own rule: each unit
/// of the table has the table's ID, every other descriptor an ID above the
/// table's. Returns the descriptors that break it, `[]` when none does.
async fn descriptor_ids(profile: &Profile, rebuilt: &Database) -> Result<serde_json::Value> {
    let table: BTreeMap<String, i64> = dsa_chain::profiles::fixed_descriptor_ids(profile)
        .into_iter()
        .collect();
    let highest = table.values().copied().max().unwrap_or(0);
    let rows = common::connect(&rebuilt.config)
        .await?
        .query(
            "SELECT id,encode(policy,'hex')||encode(name,'hex') FROM serve.multi_asset ORDER BY id",
            &[],
        )
        .await?;
    let breaking: Vec<serde_json::Value> = rows
        .iter()
        .filter_map(|row| {
            let (id, unit): (i64, String) = (row.get(0), row.get(1));
            let fixed = table.get(&unit).copied();
            let holds = fixed.map_or(id > highest, |fixed| id == fixed);
            (!holds).then(|| json!({"unit": unit, "id": id, "table": fixed}))
        })
        .collect();
    Ok(json!(breaking))
}

/// Property test: built from the provider's listings, each generated chain
/// has the content digest, state digest, supply and position of a
/// conversion of its full history at the same horizon and target.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn a_build_from_the_provider_equals_a_conversion() -> Result {
    let cases = setting("DSA_REBUILD_CASES", 16);
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
            let ran = totals.chains + totals.failures.len() as u64;
            if (ran * parallel).is_multiple_of(50) {
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
        totals.flagged.add(result.flagged);
        totals.burns.add(result.burns);
        totals.calls += result.calls;
        for (family, count) in result.requests {
            *totals.requests.entry(family).or_default() += count;
        }
        totals.failures.extend(result.failures);
    }
    let mut by_code: BTreeMap<String, u64> = BTreeMap::new();
    for failure in &totals.failures {
        *by_code
            .entry(failure["code"].as_str().unwrap_or_default().to_owned())
            .or_default() += 1;
    }
    let burns = totals.burns;
    println!(
        "{}",
        json!({"event":"rebuild_equivalence","cases":cases,"chains":totals.chains,
            "failed":totals.failures.len(),"failed_by_code":by_code,
            "quiet_windows":totals.quiet_windows,"markers":totals.markers,
            "datums_revealed_at_spend":totals.revealed_at_spend,"invalid_transactions":totals.invalid,
            "flagged_rows":totals.flagged.json(),
            "complete_burns":{"below_horizon":burns.below_horizon,"in_window":burns.in_window,
                "after_target":burns.after_target,"of_outputs_created_since_horizon":burns.created_since_horizon},
            "calls":totals.calls,"requests":totals.requests,"seconds":began.elapsed().as_secs_f64()})
    );
    assert!(
        totals.failures.is_empty(),
        "{} of {cases} chains refused or differ: {by_code:?}",
        totals.failures.len()
    );
    assert_eq!(totals.chains, cases);
    // The paths the build's design names are reached at the size the
    // integration job runs (sixteen chains), so that a change of the
    // generator cannot take one of them out unnoticed.
    if cases >= 16 {
        let f = totals.flagged;
        for (path, count) in [
            ("window without a spend", totals.quiet_windows as usize),
            (
                "output spent before the horizon marked",
                totals.markers as usize,
            ),
            ("datum revealed at its spend", totals.revealed_at_spend),
            ("phase-2-invalid transaction", totals.invalid),
            ("complete burn below the horizon", burns.below_horizon),
            ("complete burn after the target", burns.after_target),
            ("collateral input also spent", f.collateral_also_spent),
            ("collateral input not spent", f.collateral_not_spent),
            (
                "collateral return of a valid transaction",
                f.unspent_returns,
            ),
            ("reference input at a profile address", f.references),
            (
                "relevant input of an invalid transaction",
                f.invalid_relevant_inputs,
            ),
        ] {
            assert!(count > 0, "no {path} in {cases} chains");
        }
    }
    Ok(())
}

// ----- a scripted chain for the listing edges ------------------------------

/// Blocks of hand-built transactions, eight to an epoch, from height 1.
/// Block 1 ends with a transaction that mints one of every unit of the
/// network's table of fixed descriptors, as the real networks hold each of
/// them, into an output of no profile.
struct Script {
    g: Generated,
    next_fund: u64,
    /// Slots from one block to the next.
    step: u64,
    table: Vec<String>,
    network: u8,
    /// Epochs left without a block so far.
    skipped: u32,
}

impl Script {
    fn new(profile: &Profile) -> Self {
        Self::with_step(profile, 20)
    }

    fn with_step(profile: &Profile, step: u64) -> Self {
        let table: std::collections::BTreeSet<String> =
            dsa_chain::profiles::fixed_descriptor_ids(profile)
                .into_iter()
                .map(|(unit, _)| unit)
                .collect();
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
            step,
            table: table.into_iter().collect(),
            network: u8::from(profile.network == "mainnet"),
            skipped: 0,
        }
    }

    /// The epoch of `height`, eight blocks to an epoch from 200, after the
    /// skipped ones.
    fn epoch(&self, height: u32) -> u32 {
        200 + (height - 1) / 8 + self.skipped
    }

    /// The next block starts an epoch one later than it would: the epoch
    /// between has no block, and so, as db-sync writes them at an epoch's
    /// first block, no parameters and no stake.
    fn skip_epoch(&mut self) {
        self.skipped += 1;
    }

    fn register(&mut self, epoch: u32) {
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

    fn block(&mut self, mut transactions: Vec<Tx>) -> Vec<String> {
        let height = 1 + self.g.blocks.len() as u32;
        if height == 1 {
            let mut output = Output::new(enterprise_address(self.network, [0x7e; 28]), 2_000_000);
            let mut tx = self.tx();
            for unit in self.table.clone() {
                tx = tx.mint(&unit[..56], &unit[56..], 1);
                output = output.token(&unit[..56], &unit[56..], 1);
            }
            transactions.push(tx.output(output));
        }
        let epoch = self.epoch(height);
        self.register(epoch);
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
        let slot = 1_000 + u64::from(height) * self.step;
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
    let mut s = Script::new(profile);
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
    conversion_at(profile, g, converted, EDGE_HORIZON, EDGE_TARGET).await
}

/// A scripted chain converted at `horizon` and `target` into `converted`.
async fn conversion_at(
    profile: &Profile,
    g: &Generated,
    converted: &Database,
    horizon: u32,
    target: u32,
) -> Result<(Source, dsa_store::verify::Verification)> {
    conversion_with(profile, g, converted, (horizon, target), DAY).await
}

/// A scripted chain converted at `horizon` and `target` with a retention of
/// `retention_seconds` into `converted`.
async fn conversion_with(
    profile: &Profile,
    g: &Generated,
    converted: &Database,
    (horizon, target): (u32, u32),
    retention_seconds: i64,
) -> Result<(Source, dsa_store::verify::Verification)> {
    let history = derive(profile, g).await?;
    let mut source = Source::new().await?;
    let headers = (1..=target + K)
        .map(|height| header(g.block(height).unwrap()))
        .collect::<Vec<_>>();
    source.start(g, &history, target, &headers).await?;
    let conversion = convert(
        converted,
        &source.database,
        profile,
        horizon,
        retention_seconds,
    )
    .await?;
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
    // A build stopped before it laid out the database has no gate.
    let laid_out: bool = client
        .query_one("SELECT to_regclass('dsa.position') IS NOT NULL", &[])
        .await?
        .get(0);
    if !laid_out {
        return Ok(false);
    }
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

// ----- complete burns --------------------------------------------------------

/// Where a scripted chain burns a whole cNIGHT output: a transaction that
/// spends it, burns all of it and creates no output holding cNIGHT. Such a
/// transaction is in no transaction list of cNIGHT, only in its history.
#[derive(Clone, Copy, Debug)]
enum BurnAt {
    /// In the window, H … T.
    Window,
    /// After T, before the listing reads the provider.
    AfterTarget,
    /// The output is minted from H on and burnt from H on.
    MintedSinceHorizon,
    /// Below H, the newest relevant spend before a window without one.
    BelowHorizon,
}

/// Mainnet profile, k 12, the edge chain's H, T and length. Height 1 mints
/// 10 cNIGHT into five outputs of 2 at ordinary addresses; when the window
/// holds no relevant spend, height 10 spends one of them into a new cNIGHT
/// output; the other blocks are empty except for the case's transactions.
/// Returns the chain and the burning transaction.
fn burn_chain(profile: &Profile, at: BurnAt) -> (Generated, String) {
    let (s, burn) = burn_script(profile, at);
    (s.g, burn)
}

/// The script of `burn_chain`, to which a test may add blocks.
fn burn_script(profile: &Profile, at: BurnAt) -> (Script, String) {
    let ordinary = |index: u8| {
        let mut key = [0x6b; 28];
        key[0] = index;
        enterprise_address(1, key)
    };
    let policy = profile.assets.cnight.policy.clone();
    let name = profile.assets.cnight.name.clone();
    let mut s = Script::new(profile);
    let mut tx = s.tx().mint(&policy, &name, 10);
    for index in 0..5_u8 {
        tx = tx.output(Output::new(ordinary(index), 1_500_000).token(&policy, &name, 2));
    }
    let minted = s.block(vec![tx])[0].clone();
    let (mut young, mut burn) = (String::new(), String::new());
    for height in 2..=EDGE_END {
        let burning = matches!(
            (at, height),
            (BurnAt::Window, 35)
                | (BurnAt::AfterTarget, 45)
                | (BurnAt::BelowHorizon, 20)
                | (BurnAt::MintedSinceHorizon, 37)
        );
        let transaction = match (at, height) {
            // An older relevant spend that the list of cNIGHT names (a
            // conversion needs one at or below T).
            (BurnAt::BelowHorizon | BurnAt::AfterTarget, 10) => Some(
                s.tx()
                    .input(&minted, 1)
                    .output(Output::new(ordinary(5), 1_400_000).token(&policy, &name, 2)),
            ),
            (BurnAt::MintedSinceHorizon, 34) => Some(
                s.tx()
                    .mint(&policy, &name, 3)
                    .output(Output::new(ordinary(7), 1_600_000).token(&policy, &name, 3)),
            ),
            (BurnAt::MintedSinceHorizon, 37) => Some(
                s.tx()
                    .input(&young, 0)
                    .mint(&policy, &name, -3)
                    .output(Output::new(ordinary(6), 1_300_000)),
            ),
            _ if burning => Some(
                s.tx()
                    .input(&minted, 0)
                    .mint(&policy, &name, -2)
                    .output(Output::new(ordinary(6), 1_300_000)),
            ),
            _ => None,
        };
        let hashes = s.block(transaction.into_iter().collect());
        if matches!((at, height), (BurnAt::MintedSinceHorizon, 34)) {
            young = hashes[0].clone();
        }
        if burning {
            burn = hashes[0].clone();
        }
    }
    (s, burn)
}

/// The burn chain of `at` converted at H and T and built from the provider
/// at the same H and T: equal content and state digests, supply and
/// position. The build reads cNIGHT's history and asks for the burning
/// transaction, which no list names.
async fn burn_case(at: BurnAt) -> Result {
    let profile = profile(1);
    let (g, burn) = burn_chain(&profile, at);
    let name = format!("{at:?}");
    let converted = Database::new().await?;
    let (source, conversion) = edge_conversion(&profile, &g, &converted).await?;
    let rebuilt = Database::new().await?;
    empty(&rebuilt, &profile).await?;
    let fake = Fake::with_chain(edge_chain(&profile, &g, EDGE_END), None).await;
    let (_stopper, stop) = Stop::new();
    let report = build(&profile, &fake, &rebuilt, EDGE_HORIZON, EDGE_TARGET, stop)
        .await
        .map_err(|error| format!("{name}: {}", error.report()))?;
    if let Err(error) = same(&report.verified, &conversion, &name) {
        let rows = harness::diff(&rebuilt.config, &converted.config).await?;
        return Err(format!("{error}; rows {rows}").into());
    }
    let cnight = profile.assets.cnight.unit();
    let paths = fake.paths();
    let history = format!("/assets/{cnight}/history?");
    assert!(
        paths.iter().any(|path| path.contains(&history)),
        "{name}: the history of cNIGHT was not read"
    );
    // The burn is placed by its own answer in the lists' phase, before the
    // epochs are read; the search for the newest spend below H, which asks
    // the same path, comes after the window.
    let placed = paths
        .iter()
        .position(|path| path.ends_with(&format!("/txs/{burn}")));
    let epochs = paths.iter().position(|path| path.contains("/epochs/"));
    assert!(
        placed.is_some() && placed < epochs,
        "{name}: the burning transaction was not placed by its own answer: {placed:?} {epochs:?}"
    );
    // The burn's input is a row of the built database: in the window, or
    // below H as the newest relevant spend (the keep rule's clause K10).
    let inputs: i64 = common::connect(&rebuilt.config)
        .await?
        .query_one(
            "SELECT count(*) FROM serve.tx_in i JOIN serve.tx t ON t.id=i.tx_in_id WHERE t.hash=$1",
            &[&hex::decode(&burn)?],
        )
        .await?
        .get(0);
    let expected = i64::from(!matches!(at, BurnAt::AfterTarget));
    println!(
        "{}",
        json!({"event":"complete_burn","case":name,"burn":burn,"burn_input_rows":inputs,
            "requests":paths.len(),"calls":report.calls})
    );
    assert_eq!(inputs, expected, "{name}: rows of the burn's input");
    rebuilt.cleanup().await?;
    converted.cleanup().await?;
    source.database.cleanup().await
}

/// A transaction in the window spends a cNIGHT output and burns all of it:
/// the build holds the output at H, and the window's conservation check
/// passes on the burn.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn a_complete_burn_in_the_window_gives_the_conversion() -> Result {
    burn_case(BurnAt::Window).await
}

/// A transaction after the target burns a whole cNIGHT output before the
/// listing: the output is unspent at H and T, and the build holds it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn a_complete_burn_after_the_target_gives_the_conversion() -> Result {
    burn_case(BurnAt::AfterTarget).await
}

/// An output minted from H on and burnt whole from H on: the supply proof
/// counts both the mint and the burn.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn a_mint_and_a_complete_burn_since_the_horizon_give_the_conversion() -> Result {
    burn_case(BurnAt::MintedSinceHorizon).await
}

/// A complete burn below H is the newest relevant spend before a window
/// without one: the build keeps it as the conversion does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn a_complete_burn_below_the_horizon_gives_the_conversion() -> Result {
    burn_case(BurnAt::BelowHorizon).await
}

// ----- the readings of the units ------------------------------------------------

/// The scripted chain `g` converted at the edge chain's H and T and built
/// from `fake` into a new database: equal content and state digests,
/// supply and position, and the same rows table by table.
async fn built_equal(profile: &Profile, g: &Generated, fake: &Fake, name: &str) -> Result<Report> {
    let converted = Database::new().await?;
    let (source, conversion) = edge_conversion(profile, g, &converted).await?;
    let rebuilt = Database::new().await?;
    empty(&rebuilt, profile).await?;
    let (_stopper, stop) = Stop::new();
    let report = build(profile, fake, &rebuilt, EDGE_HORIZON, EDGE_TARGET, stop)
        .await
        .map_err(|error| format!("{name}: {}", error.report()))?;
    let rows = harness::diff(&rebuilt.config, &converted.config).await?;
    if let Err(error) = same(&report.verified, &conversion, name) {
        return Err(format!("{error}; rows {rows}").into());
    }
    if rows != json!({}) {
        return Err(format!("{name}: rows differ: {rows}").into());
    }
    let ids = descriptor_ids(profile, &rebuilt).await?;
    if ids != json!([]) {
        return Err(format!("{name}: descriptor IDs: {ids}").into());
    }
    rebuilt.cleanup().await?;
    converted.cleanup().await?;
    source.database.cleanup().await?;
    Ok(report)
}

/// A hook that rewrites the rows of a list answer whose path holds `part`
/// while `on` is set.
fn rows_hook(
    part: String,
    on: Arc<AtomicBool>,
    change: impl Fn(&mut Vec<serde_json::Value>) + Send + Sync + 'static,
) -> Hook {
    Arc::new(move |path: &str, chain: &mut Chain, _| {
        if !on.load(Ordering::SeqCst) || !path.contains(&part) {
            return None;
        }
        let mut rows: Vec<serde_json::Value> =
            serde_json::from_str(&chain.answer(path).body).ok()?;
        change(&mut rows);
        Some(Behavior::json(serde_json::Value::Array(rows)))
    })
}

/// A history answered without its burn disagrees with the quantity in
/// every reading: after five the build ends with
/// `provider_mint_history_mismatch` and nothing serves; the same command
/// with honest answers then publishes the conversion's digest.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn a_history_without_its_burn_refuses_until_it_is_whole() -> Result {
    let profile = profile(1);
    let (g, burn) = burn_chain(&profile, BurnAt::AfterTarget);
    let converted = Database::new().await?;
    let (source, conversion) = edge_conversion(&profile, &g, &converted).await?;
    let rebuilt = Database::new().await?;
    empty(&rebuilt, &profile).await?;
    let history = format!("/assets/{}/history?", profile.assets.cnight.unit());
    let cut = Arc::new(AtomicBool::new(true));
    let hook = rows_hook(history.clone(), cut.clone(), move |rows| {
        rows.retain(|row| row["tx_hash"] != burn.as_str());
    });
    let fake = Fake::with_chain(edge_chain(&profile, &g, EDGE_END), Some(hook)).await;
    let (_stopper, stop) = Stop::new();
    let error = build(&profile, &fake, &rebuilt, EDGE_HORIZON, EDGE_TARGET, stop)
        .await
        .err()
        .ok_or("a history without its burn was taken")?;
    let readings = fake.requests(&history);
    assert_eq!(
        code(&error),
        "provider_mint_history_mismatch",
        "{}",
        error.report()
    );
    assert!(!serving(&rebuilt).await?, "the database serves");
    assert_eq!(readings, 5, "readings of the history");
    cut.store(false, Ordering::SeqCst);
    let (_stopper, stop) = Stop::new();
    let report = build(&profile, &fake, &rebuilt, EDGE_HORIZON, EDGE_TARGET, stop)
        .await
        .map_err(|error| format!("continued: {}", error.report()))?;
    same(
        &report.verified,
        &conversion,
        "continued with honest answers",
    )?;
    println!(
        "{}",
        json!({"event":"history_without_its_burn","refusal":error.report(),"readings":readings,
            "continued_digest":report.verified.content_digest})
    );
    rebuilt.cleanup().await?;
    converted.cleanup().await?;
    source.database.cleanup().await
}

/// A mint joins the chain between the first reading of cNIGHT's history
/// and its quantity: the two disagree, cNIGHT is read again, and the build
/// equals the conversion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn an_event_between_a_history_and_its_quantity_is_read_again() -> Result {
    let profile = profile(1);
    let (mut s, _) = burn_script(&profile, BurnAt::AfterTarget);
    let cnight = profile.assets.cnight.clone();
    let tx = s.tx().mint(&cnight.policy, &cnight.name, 3).output(
        Output::new(enterprise_address(1, [0x2e; 28]), 1_700_000).token(
            &cnight.policy,
            &cnight.name,
            3,
        ),
    );
    s.block(vec![tx]);
    let g = s.g;
    let late = g.blocks.last().expect("a block").clone();
    let quantity = format!("/api/v0/assets/{}", cnight.unit());
    let joined = Arc::new(AtomicBool::new(false));
    let hook: Hook = {
        let joined = joined.clone();
        Arc::new(move |path: &str, chain: &mut Chain, _| {
            if path == quantity && !joined.swap(true, Ordering::SeqCst) {
                chain.blocks_mut().push(late.clone());
            }
            None
        })
    };
    let fake = Fake::with_chain(edge_chain(&profile, &g, EDGE_END), Some(hook)).await;
    let report = built_equal(&profile, &g, &fake, "event between the readings").await?;
    let history = fake.requests(&format!("/assets/{}/history?", cnight.unit()));
    println!(
        "{}",
        json!({"event":"event_between_readings","history_readings":history,"calls":report.calls})
    );
    assert!(joined.load(Ordering::SeqCst), "no block joined");
    assert_eq!(history, 2, "readings of the history");
    Ok(())
}

/// Mainnet profile, k 12, the edge chain's H, T and length. Heights 1–17:
/// 101 transactions that each mint 1 cNIGHT into an output of their own at
/// an ordinary address, so that cNIGHT's history has two pages; at each
/// height of `burns` a transaction spends one of those outputs and burns it
/// whole. Returns the chain and the burning transactions in that order.
fn mints_chain(profile: &Profile, burns: &[u32]) -> (Generated, Vec<String>) {
    let ordinary = |index: u8| {
        let mut key = [0x1f; 28];
        key[0] = index;
        enterprise_address(1, key)
    };
    let cnight = profile.assets.cnight.clone();
    let mut s = Script::new(profile);
    let mut minted = Vec::new();
    for block in 0..17_u32 {
        let mut transactions = Vec::new();
        for slot in 0..6_u32 {
            let n = block * 6 + slot;
            if n >= 101 {
                break;
            }
            transactions.push(s.tx().mint(&cnight.policy, &cnight.name, 1).output(
                Output::new(ordinary((n % 7) as u8), 1_500_000 + u64::from(n)).token(
                    &cnight.policy,
                    &cnight.name,
                    1,
                ),
            ));
        }
        minted.extend(s.block(transactions));
    }
    let mut burnt = Vec::new();
    for height in 18..=EDGE_END {
        let transaction = burns.contains(&height).then(|| {
            s.tx()
                .input(&minted[burnt.len()], 0)
                .mint(&cnight.policy, &cnight.name, -1)
                .output(Output::new(ordinary(8), 1_300_000))
        });
        let hashes = s.block(transaction.into_iter().collect());
        if burns.contains(&height) {
            burnt.push(hashes[0].clone());
        }
    }
    (s.g, burnt)
}

/// A history of two pages: 101 mints below H and a complete burn at H + 2.
/// Both pages are read and the build equals the conversion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn a_history_of_two_pages_gives_the_conversion() -> Result {
    let profile = profile(1);
    let (g, _) = mints_chain(&profile, &[EDGE_HORIZON + 2]);
    let fake = Fake::with_chain(edge_chain(&profile, &g, EDGE_END), None).await;
    built_equal(&profile, &g, &fake, "history of two pages").await?;
    let second = format!(
        "/assets/{}/history?count=100&page=2",
        profile.assets.cnight.unit()
    );
    assert!(fake.requests(&second) > 0, "no request {second}");
    Ok(())
}

/// A history in which the burn after T is answered as a second copy of the
/// burn below H, of the same amount: the count and the sum agree with the
/// quantity, a transaction occurs twice, and the build refuses.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn a_history_with_a_row_answered_twice_refuses() -> Result {
    let profile = profile(1);
    let (g, burns) = mints_chain(&profile, &[20, 45]);
    let (below, after) = (burns[0].clone(), burns[1].clone());
    let hook = rows_hook(
        format!("/assets/{}/history?", profile.assets.cnight.unit()),
        Arc::new(AtomicBool::new(true)),
        move |rows| {
            let copy = rows
                .iter()
                .find(|row| row["tx_hash"] == below.as_str())
                .cloned();
            if let (Some(copy), Some(row)) = (
                copy,
                rows.iter_mut().find(|row| row["tx_hash"] == after.as_str()),
            ) {
                *row = copy;
            }
        },
    );
    let (error, serves) = refused(&profile, &g, hook).await?;
    println!(
        "{}",
        json!({"event":"history_row_twice","refusal":error.report(),"serving":serves})
    );
    assert_eq!(
        code(&error),
        "provider_mint_history_mismatch",
        "{}",
        error.report()
    );
    assert_eq!(error.report()["detail"]["distinct_transactions"], false);
    assert!(!serves, "the database serves");
    Ok(())
}

/// A history answered without one mint of 1 and without the burn of 1 after
/// T (a burn of 1 in the window stays): its transactions are distinct and
/// its amounts still add up to the quantity, but it has two rows fewer than
/// the quantity's answer counts. Taken, it would let the build publish a
/// database without the output burnt after T, whose amount the missing mint
/// makes up for. Every reading is refused by that count alone, and after
/// five the build ends with `provider_mint_history_mismatch` while nothing
/// serves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn a_history_short_of_a_mint_and_a_burn_of_one_refuses() -> Result {
    let profile = profile(1);
    let (g, burns) = mints_chain(&profile, &[38, 45]);
    let burn = burns[1].clone();
    let history = format!("/assets/{}/history?", profile.assets.cnight.unit());
    let hook = rows_hook(
        history.clone(),
        Arc::new(AtomicBool::new(true)),
        move |rows| {
            if let Some(at) = rows.iter().position(|row| row["tx_hash"] == burn.as_str()) {
                rows.remove(at);
                if let Some(mint) = rows.iter().position(|row| row["amount"] == "1") {
                    rows.remove(mint);
                }
            }
        },
    );
    let (error, serves) = refused(&profile, &g, hook).await?;
    let report = error.report();
    println!(
        "{}",
        json!({"event":"history_short_of_two_rows","refusal":report,"serving":serves})
    );
    assert_eq!(
        report["error"], "provider_mint_history_mismatch",
        "{report}"
    );
    let detail = &report["detail"];
    let count = detail["mint_or_burn_count"].as_u64().ok_or("no count")?;
    assert_eq!(detail["history_rows"], count - 2, "{detail}");
    assert_eq!(detail["history_sum"], detail["quantity"], "{detail}");
    assert_eq!(detail["distinct_transactions"], true, "{detail}");
    assert_eq!(detail["readings"], 5, "{detail}");
    assert!(!serves, "the database serves");
    Ok(())
}

/// The burn after T is placed by its own answer at a height above the
/// provider's tip: its amount is still taken from the quantity, its input is
/// read, and the build equals the conversion. Answered 404 for its inputs, it
/// refuses the build instead of publishing one without the burnt output;
/// the same command, with its inputs answered, equals the conversion row by
/// row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn a_burn_placed_above_the_tip_is_counted() -> Result {
    let profile = profile(1);
    let (g, burn) = burn_chain(&profile, BurnAt::AfterTarget);
    let placed = format!("/api/v0/txs/{burn}");
    let higher: Hook = {
        let placed = placed.clone();
        Arc::new(move |path: &str, chain: &mut Chain, _| {
            if path != placed {
                return None;
            }
            let mut answer: serde_json::Value =
                serde_json::from_str(&chain.answer(path).body).ok()?;
            answer["block_height"] = json!(EDGE_END + 1);
            Some(Behavior::json(answer))
        })
    };
    let fake = Fake::with_chain(edge_chain(&profile, &g, EDGE_END), Some(higher.clone())).await;
    built_equal(&profile, &g, &fake, "burn placed above the tip").await?;
    assert!(fake.requests(&placed) > 0, "the burn was not asked for");
    let converted = Database::new().await?;
    let (source, conversion) = edge_conversion(&profile, &g, &converted).await?;
    let on = Arc::new(AtomicBool::new(true));
    let unanswered: Hook = {
        let (inputs, on) = (format!("{placed}/utxos"), on.clone());
        Arc::new(move |path: &str, chain: &mut Chain, now| {
            if path == inputs && on.load(Ordering::SeqCst) {
                return Some(Behavior::status(404));
            }
            higher(path, chain, now)
        })
    };
    let outcome = refused_then_continued(
        &profile,
        &g,
        EDGE_END,
        (unanswered, on),
        &converted,
        &conversion,
    )
    .await?;
    println!(
        "{}",
        json!({"event":"burn_above_the_tip","unanswered":outcome})
    );
    refused_then_equal(&outcome, "provider_transaction_unknown");
    converted.cleanup().await?;
    source.database.cleanup().await
}

/// A council-policy token of another name at an ordinary address, never
/// spent, missing from its unit's unspent listing: the proof runs for every
/// unit that has a reading and refuses naming that unit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn a_governance_output_missing_from_its_listing_refuses_the_build() -> Result {
    let profile = profile(1);
    let ordinary = |index: u8| {
        let mut key = [0x48; 28];
        key[0] = index;
        enterprise_address(1, key)
    };
    let cnight = profile.assets.cnight.clone();
    let council = profile.assets.council.policy.clone();
    let mut s = Script::new(&profile);
    let mut tx = s.tx().mint(&cnight.policy, &cnight.name, 10);
    for index in 0..5_u8 {
        tx = tx.output(Output::new(ordinary(index), 1_500_000).token(
            &cnight.policy,
            &cnight.name,
            2,
        ));
    }
    let minted = s.block(vec![tx])[0].clone();
    for height in 2..=EDGE_END {
        let transaction = match height {
            6 => Some(
                s.tx()
                    .mint(&council, "01", 1)
                    .output(Output::new(ordinary(8), 2_000_000).token(&council, "01", 1)),
            ),
            10 | 35 => Some(s.tx().input(&minted, (height / 10) as u16).output(
                Output::new(ordinary(5), 1_400_000).token(&cnight.policy, &cnight.name, 2),
            )),
            _ => None,
        };
        s.block(transaction.into_iter().collect());
    }
    let unit = format!("{council}01");
    let hook = rows_hook(
        format!("/assets/{unit}/utxos?"),
        Arc::new(AtomicBool::new(true)),
        |rows| rows.clear(),
    );
    let (error, serves) = refused(&profile, &s.g, hook).await?;
    println!(
        "{}",
        json!({"event":"governance_output_missing","refusal":error.report(),"serving":serves})
    );
    assert_eq!(
        code(&error),
        "rebuild_supply_mismatch",
        "{}",
        error.report()
    );
    assert_eq!(error.report()["detail"]["unit"], unit.as_str());
    assert!(!serves, "the database serves");
    Ok(())
}

/// The newest relevant spend below H, in a window without one, burns the
/// only council-policy token of another name: the spent output's asset row
/// needs a descriptor that nothing else of the build stores. The build
/// writes it with the row, and equals the conversion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn the_newest_spend_below_the_horizon_brings_its_descriptor() -> Result {
    let profile = profile(1);
    let ordinary = |index: u8| {
        let mut key = [0x52; 28];
        key[0] = index;
        enterprise_address(1, key)
    };
    let cnight = profile.assets.cnight.clone();
    let council = profile.assets.council.policy.clone();
    let mut s = Script::new(&profile);
    let mut tx = s.tx().mint(&cnight.policy, &cnight.name, 10);
    for index in 0..5_u8 {
        tx = tx.output(Output::new(ordinary(index), 1_500_000).token(
            &cnight.policy,
            &cnight.name,
            2,
        ));
    }
    s.block(vec![tx]);
    let mut token = String::new();
    for height in 2..=EDGE_END {
        let transaction = match height {
            6 => Some(
                s.tx()
                    .mint(&council, "01", 1)
                    .output(Output::new(ordinary(8), 2_000_000).token(&council, "01", 1)),
            ),
            20 => Some(
                s.tx()
                    .input(&token, 0)
                    .mint(&council, "01", -1)
                    .output(Output::new(ordinary(9), 1_800_000)),
            ),
            _ => None,
        };
        let hashes = s.block(transaction.into_iter().collect());
        if height == 6 {
            token = hashes[0].clone();
        }
    }
    let fake = Fake::with_chain(edge_chain(&profile, &s.g, EDGE_END), None).await;
    built_equal(&profile, &s.g, &fake, "newest spend with its descriptor").await?;
    Ok(())
}

/// The newest relevant spend below H, before a window without one, is a
/// valid transaction at height 20 that spends a cNIGHT output and also names
/// a collateral input and a reference input whose transactions sort before
/// it: the input row it keeps has the ordinal of the spent output among the
/// transaction's inputs, the flagged rows not counted, and the build equals
/// the conversion row by row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn the_newest_spend_ranks_its_input_among_its_inputs_only() -> Result {
    let profile = profile(1);
    let ordinary = |index: u8| {
        let mut key = [0x37; 28];
        key[0] = index;
        enterprise_address(1, key)
    };
    let cnight = profile.assets.cnight.clone();
    let mut s = Script::new(&profile);
    let mut tx = s.tx().mint(&cnight.policy, &cnight.name, 10);
    for index in 0..5_u8 {
        tx = tx.output(Output::new(ordinary(index), 1_500_000).token(
            &cnight.policy,
            &cnight.name,
            2,
        ));
    }
    let minted = s.block(vec![tx])[0].clone();
    let first = |last: char| format!("{}{last}", "0".repeat(63));
    for height in 2..=EDGE_END {
        let transaction = (height == 20).then(|| {
            s.tx()
                .input(&minted, 0)
                .collateral(&first('1'), 0)
                .reference(&first('2'), 0)
                .collateral_return(Output::new(ordinary(7), 1_000_000))
                .output(Output::new(ordinary(6), 1_400_000).token(&cnight.policy, &cnight.name, 2))
        });
        s.block(transaction.into_iter().collect());
    }
    let fake = Fake::with_chain(edge_chain(&profile, &s.g, EDGE_END), None).await;
    built_equal(&profile, &s.g, &fake, "flagged rows of the newest spend").await?;
    Ok(())
}

/// Mainnet profile, k 12, the edge chain's H, T and length: cNIGHT minted
/// at height 1 and spent at 10 and 36; at height 35, 110 transactions that
/// create cNIGHT outputs; with `empty_epoch`, the epoch of heights 25–32
/// starts one later, so that epoch(H) − 2 has no block.
fn shapes_chain(profile: &Profile, empty_epoch: bool) -> Script {
    let ordinary = |index: u8| {
        let mut key = [0x2b; 28];
        key[0] = index;
        enterprise_address(1, key)
    };
    let cnight = profile.assets.cnight.clone();
    let mut s = Script::new(profile);
    let mut tx = s.tx().mint(&cnight.policy, &cnight.name, 10);
    for index in 0..5_u8 {
        tx = tx.output(Output::new(ordinary(index), 1_500_000).token(
            &cnight.policy,
            &cnight.name,
            2,
        ));
    }
    let minted = s.block(vec![tx])[0].clone();
    for height in 2..=EDGE_END {
        if height == 25 && empty_epoch {
            s.skip_epoch();
        }
        let transactions =
            match height {
                10 => vec![s.tx().input(&minted, 1).output(
                    Output::new(ordinary(5), 1_400_000).token(&cnight.policy, &cnight.name, 2),
                )],
                35 => (0..110_u64)
                    .map(|n| {
                        s.tx().mint(&cnight.policy, &cnight.name, 1).output(
                            Output::new(ordinary((n % 5) as u8), 1_200_000 + n).token(
                                &cnight.policy,
                                &cnight.name,
                                1,
                            ),
                        )
                    })
                    .collect(),
                36 => vec![s.tx().input(&minted, 2).output(
                    Output::new(ordinary(6), 1_400_000).token(&cnight.policy, &cnight.name, 2),
                )],
                _ => Vec::new(),
            };
        s.block(transactions);
    }
    s
}

/// 110 transactions that create cNIGHT outputs at one height from H on: the
/// list from H on is read for that height on its own, two pages, until two
/// readings agree. The build equals the conversion row by row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn a_crowded_height_since_the_horizon_gives_the_conversion() -> Result {
    let profile = profile(1);
    let s = shapes_chain(&profile, false);
    let fake = Fake::with_chain(edge_chain(&profile, &s.g, EDGE_END), None).await;
    built_equal(&profile, &s.g, &fake, "crowded height").await?;
    let crowded = format!(
        "/assets/{}/transactions?count=100&page=2&order=asc&from=35&to=35",
        profile.assets.cnight.unit()
    );
    println!(
        "{}",
        json!({"event":"crowded_height","second_pages":fake.requests(&crowded)})
    );
    assert!(fake.requests(&crowded) > 0, "no request {crowded}");
    Ok(())
}

/// An epoch without a block two epochs before H's has no newest block and,
/// at a provider fed by db-sync, no parameters: the build asks once and
/// refuses with `provider_epoch_blocks_missing` naming the epoch, and
/// nothing serves. (Every epoch of Mainnet and Preprod has blocks.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn an_epoch_without_a_block_below_the_horizon_refuses_the_build() -> Result {
    let profile = profile(1);
    let s = shapes_chain(&profile, true);
    let empty = s.epoch(EDGE_HORIZON) - 2;
    assert!(s.g.blocks.iter().all(|block| block.epoch != empty));
    let (error, serves) =
        refused(&profile, &s.g, Arc::new(|_: &str, _: &mut Chain, _| None)).await?;
    println!(
        "{}",
        json!({"event":"empty_epoch","epoch":empty,"refusal":error.report(),"serving":serves})
    );
    assert_eq!(
        code(&error),
        "provider_epoch_blocks_missing",
        "{}",
        error.report()
    );
    assert_eq!(error.report()["detail"], json!({"epoch": empty}));
    assert!(!serves, "the database serves");
    Ok(())
}

// ----- phase-2-invalid transactions ------------------------------------------

/// What the invalid-transaction chain's test refers to.
struct Invalids {
    g: Generated,
    /// The invalid transaction at height 3, beside 120 outputs of cNIGHT.
    wide: String,
    /// The invalid transaction whose collateral return is the newest council
    /// record.
    record: String,
    /// The invalid transaction whose collateral is the newest relevant spend
    /// below H.
    newest: String,
}

/// Mainnet profile, k 12, the edge chain's H, T and length; no relevant
/// transaction from H on. Height 1 mints 10 cNIGHT into five outputs of 2 at
/// ordinary addresses; 2: a council record at the council address; 3: 120
/// outputs of cNIGHT and, in the same block, an invalid transaction whose
/// collateral return holds 2 cNIGHT (one height with more than 100 outputs
/// of cNIGHT, read through its transactions); 10: a valid spend of a cNIGHT
/// output; 12: an invalid transaction whose collateral is the council record
/// and whose collateral return is a newer record at the council address;
/// 20: an invalid transaction whose collateral holds cNIGHT, the newest
/// relevant spend below H.
fn invalid_chain(profile: &Profile) -> Invalids {
    let ordinary = |index: u8| {
        let mut key = [0x4c; 28];
        key[0] = index;
        enterprise_address(1, key)
    };
    let policy = profile.assets.cnight.policy.clone();
    let name = profile.assets.cnight.name.clone();
    let council = profile.assets.council.clone();
    let council_address = profile.addresses.council.clone();
    let mut s = Script::new(profile);
    let mut tx = s.tx().mint(&policy, &name, 10);
    for index in 0..5_u8 {
        tx = tx.output(Output::new(ordinary(index), 1_500_000).token(&policy, &name, 2));
    }
    let minted = s.block(vec![tx])[0].clone();
    let tx = s.tx().mint(&council.policy, &council.name, 1).output(
        Output::to(&council_address, 2_000_000)
            .token(&council.policy, &council.name, 1)
            .inline_datum(int_datum(2_001)),
    );
    let first_record = s.block(vec![tx])[0].clone();
    let mut many = s.tx().mint(&policy, &name, 120);
    for index in 0..120_u32 {
        many = many.output(
            Output::new(ordinary((index % 5) as u8), 1_200_000 + u64::from(index))
                .token(&policy, &name, 1),
        );
    }
    let failing = s
        .tx()
        .invalid()
        .collateral(&minted, 2)
        .output(Output::new(ordinary(8), 2_000_000).token(&policy, &name, 3))
        .collateral_return(Output::new(ordinary(9), 1_400_000).token(&policy, &name, 2));
    let wide = s.block(vec![many, failing])[1].clone();
    let (mut record, mut newest) = (String::new(), String::new());
    for height in 4..=EDGE_END {
        let transaction = match height {
            10 => Some(
                s.tx()
                    .input(&minted, 1)
                    .output(Output::new(ordinary(5), 1_400_000).token(&policy, &name, 2)),
            ),
            12 => Some(
                s.tx()
                    .invalid()
                    .collateral(&first_record, 0)
                    .output(Output::new(ordinary(8), 2_000_000))
                    .collateral_return(
                        Output::to(&council_address, 1_900_000)
                            .token(&council.policy, &council.name, 1)
                            .inline_datum(int_datum(2_002)),
                    ),
            ),
            20 => Some(
                s.tx()
                    .invalid()
                    .collateral(&minted, 0)
                    .output(Output::new(ordinary(8), 2_000_000))
                    .collateral_return(
                        Output::new(ordinary(9), 1_300_000).token(&policy, &name, 2),
                    ),
            ),
            _ => None,
        };
        let hashes = s.block(transaction.into_iter().collect());
        match height {
            12 => record = hashes[0].clone(),
            20 => newest = hashes[0].clone(),
            _ => {}
        }
    }
    Invalids {
        g: s.g,
        wide,
        record,
        newest,
    }
}

/// Phase-2-invalid transactions as the backend renders them (their
/// collateral inputs as ordinary inputs, their collateral return as their
/// one ordinary output): a return at a height with more than 100 outputs of
/// cNIGHT is found through its transaction, a return at the council address
/// is the newest council record (K5), and an invalid transaction's
/// collateral is the newest relevant spend below H (K10). The build equals
/// the conversion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn invalid_transactions_are_read_as_the_backend_renders_them() -> Result {
    let profile = profile(1);
    let chain = invalid_chain(&profile);
    let converted = Database::new().await?;
    let (source, conversion) = edge_conversion(&profile, &chain.g, &converted).await?;
    let rebuilt = Database::new().await?;
    empty(&rebuilt, &profile).await?;
    let fake = Fake::with_chain(edge_chain(&profile, &chain.g, EDGE_END), None).await;
    let (_stopper, stop) = Stop::new();
    let report = build(&profile, &fake, &rebuilt, EDGE_HORIZON, EDGE_TARGET, stop)
        .await
        .map_err(|error| error.report().to_string())?;
    if let Err(error) = same(&report.verified, &conversion, "invalid") {
        let rows = harness::diff(&rebuilt.config, &converted.config).await?;
        return Err(format!("{error}; rows {rows}").into());
    }
    let client = common::connect(&rebuilt.config).await?;
    let outputs = "SELECT count(*) FROM serve.tx_out o JOIN serve.tx t ON t.id=o.tx_id \
        WHERE t.hash=$1 AND NOT t.valid_contract";
    let inputs = "SELECT count(*) FROM serve.tx_in i JOIN serve.tx t ON t.id=i.tx_in_id \
        WHERE t.hash=$1 AND NOT t.valid_contract";
    let mut found = Vec::new();
    for (sql, transaction) in [
        (outputs, &chain.wide),
        (outputs, &chain.record),
        (inputs, &chain.newest),
    ] {
        let hash = hex::decode(transaction)?;
        found.push(client.query_one(sql, &[&hash]).await?.get::<_, i64>(0));
    }
    println!(
        "{}",
        json!({"event":"invalid_transactions","return_beside_many_outputs":found[0],
            "council_record_return":found[1],"newest_spend_collateral":found[2],
            "requests":fake.paths().len(),"calls":report.calls})
    );
    assert_eq!(found, [1, 1, 1]);
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

/// The last height of the moving chain.
const MOVING_END: u32 = 100;

/// Mainnet profile, k 12, the edge chain's H and T. Heights 1 to 60 as the
/// reserve chain without its spend at 61: cNIGHT minted at 1 into five
/// outputs at ordinary addresses, five outputs at the reserve address at 2,
/// spends of reserve outputs at 35 and 50 and of a cNIGHT output at 36.
/// From 61 to `MOVING_END` every block spends the newest output at the
/// reserve address that is still unspent (first the reserve output 2) and
/// creates another there; height 65 also mints 4 cNIGHT into an output at
/// an ordinary address, height 70 also spends the cNIGHT output 2 of height
/// 1 and burns its 2 cNIGHT whole.
fn moving_chain(profile: &Profile) -> Generated {
    let ordinary = |index: u8| {
        let mut key = [0x5c; 28];
        key[0] = index;
        enterprise_address(1, key)
    };
    let policy = profile.assets.cnight.policy.clone();
    let name = profile.assets.cnight.name.clone();
    let reserve = profile.addresses.reserve.clone();
    let mut s = Script::new(profile);
    let mut tx = s.tx().mint(&policy, &name, 10);
    for index in 0..5_u8 {
        tx = tx.output(Output::new(ordinary(index), 1_500_000).token(&policy, &name, 2));
    }
    let minted = s.block(vec![tx])[0].clone();
    let mut tx = s.tx();
    for index in 0..5_u64 {
        tx = tx.output(Output::to(&reserve, 3_000_000 + index));
    }
    let creator = s.block(vec![tx])[0].clone();
    let mut newest = (creator.clone(), 2_u16);
    for height in 3..=MOVING_END {
        let mut transactions = Vec::new();
        match height {
            35 | 50 => transactions.push(
                s.tx()
                    .input(&creator, if height == 35 { 0 } else { 1 })
                    .output(Output::new(ordinary(9), 2_500_000)),
            ),
            36 => transactions.push(
                s.tx()
                    .input(&minted, 1)
                    .output(Output::new(ordinary(5), 1_400_000).token(&policy, &name, 2)),
            ),
            _ => {}
        }
        if height > EDGE_END {
            transactions.push(
                s.tx()
                    .input(&newest.0, newest.1)
                    .output(Output::to(&reserve, 2_000_000 + u64::from(height))),
            );
        }
        if height == 65 {
            transactions.push(
                s.tx()
                    .mint(&policy, &name, 4)
                    .output(Output::new(ordinary(6), 1_600_000).token(&policy, &name, 4)),
            );
        }
        if height == 70 {
            transactions.push(
                s.tx()
                    .input(&minted, 2)
                    .mint(&policy, &name, -2)
                    .output(Output::new(ordinary(8), 1_300_000)),
            );
        }
        let hashes = s.block(transactions);
        if height > EDGE_END {
            newest = (hashes[0].clone(), 0);
        }
    }
    s.g
}

/// The provider's chain moves while the build reads: served through 60 at
/// first, a block joins before every answer of the reserve address's
/// listing, each moving an output at that address, so that no two of its
/// eight reads agree and the build takes their union; then a block joins
/// before every answer of a list from H on (the burn at 70 among them, after
/// every history was read). The build equals the conversion row by row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn movement_during_the_address_passes_and_the_lists_keeps_the_conversion() -> Result {
    let profile = profile(1);
    let g = moving_chain(&profile);
    let all = g.blocks.clone();
    let listing = format!("/addresses/{}/utxos?", profile.addresses.reserve);
    let during_passes = Arc::new(AtomicUsize::new(0));
    let during_lists = Arc::new(AtomicUsize::new(0));
    let hook: Hook = {
        let (listing, during_passes, during_lists) =
            (listing.clone(), during_passes.clone(), during_lists.clone());
        Arc::new(move |path: &str, chain: &mut Chain, _| {
            let joined = if path.contains(&listing) {
                &during_passes
            } else if path.contains("/transactions?") && path.contains("from=") {
                &during_lists
            } else {
                return None;
            };
            let blocks = chain.blocks_mut();
            let height = blocks.last().map_or(0, |block| block.height);
            if let Some(block) = all.get(height as usize) {
                blocks.push(block.clone());
                joined.fetch_add(1, Ordering::SeqCst);
            }
            None
        })
    };
    let fake = Fake::with_chain(edge_chain(&profile, &g, EDGE_END), Some(hook)).await;
    let report = built_equal(&profile, &g, &fake, "movement during passes and lists").await?;
    let reads = fake.requests(&listing);
    let (during_passes, during_lists) = (
        during_passes.load(Ordering::SeqCst),
        during_lists.load(Ordering::SeqCst),
    );
    let served = fake
        .chain
        .lock()
        .unwrap()
        .blocks_mut()
        .last()
        .map_or(0, |block| block.height);
    println!(
        "{}",
        json!({"event":"movement_during_passes_and_lists","reserve_listings":reads,
            "blocks_during_address_passes":during_passes,"blocks_during_lists":during_lists,
            "served_through":served,"calls":report.calls})
    );
    // Eight reads of one page each, every one after a block that moved an
    // output at the address: no two agreed, so the union was taken.
    assert_eq!(reads, 8, "reads of the reserve address's listing");
    assert_eq!(during_passes, 8, "blocks joined during the reads");
    assert!(
        during_lists >= 2,
        "blocks joined during the lists from H on"
    );
    assert!(served >= 70, "the burn at 70 did not join");
    Ok(())
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

/// A listing that lacks an output (the supply proof), the lists lacking the
/// window's spend of a council-policy token of another name (the proof of
/// that unit, before the window) and a provider total that does not match
/// the asset's history (the reading) each refuse the build; the gate stays
/// closed.
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
    // A total one unit above the outputs: the history does not add up to
    // it, so the reading refuses before the proof.
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
            "rebuild_supply_mismatch",
        ),
        (
            "wrong_provider_total",
            wrong_total,
            "provider_mint_history_mismatch",
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

// ----- outputs relevant by their address alone -------------------------------

/// What the reserve chain's tests refer to.
struct Reserve {
    g: Generated,
    /// Height 2: five outputs at the reserve address, without tokens.
    creator: String,
    /// Height 35, in the window: spends the reserve output 0.
    spender: String,
}

/// Mainnet profile, k 12, the edge chain's H and T. Height 1 mints 10
/// cNIGHT into five outputs at ordinary addresses; 2: five outputs at the
/// reserve address without tokens, which only the address makes relevant;
/// 35: a spend of the reserve output 0; 36: a spend of a cNIGHT output (the
/// window holds a spend whatever the reserve's); 50, after T: a spend of the
/// reserve output 1; with `tip_spend`, height 61 spends the reserve output
/// 3. The other blocks are empty.
fn reserve_chain(profile: &Profile, tip_spend: bool) -> Reserve {
    let ordinary = |index: u8| {
        let mut key = [0x3d; 28];
        key[0] = index;
        enterprise_address(1, key)
    };
    let policy = profile.assets.cnight.policy.clone();
    let name = profile.assets.cnight.name.clone();
    let reserve = profile.addresses.reserve.clone();
    let mut s = Script::new(profile);
    let mut tx = s.tx().mint(&policy, &name, 10);
    for index in 0..5_u8 {
        tx = tx.output(Output::new(ordinary(index), 1_500_000).token(&policy, &name, 2));
    }
    let minted = s.block(vec![tx])[0].clone();
    let mut tx = s.tx();
    for index in 0..5_u64 {
        tx = tx.output(Output::to(&reserve, 3_000_000 + index));
    }
    let creator = s.block(vec![tx])[0].clone();
    let mut spender = String::new();
    let end = if tip_spend { EDGE_END + 1 } else { EDGE_END };
    for height in 3..=end {
        let spent = match height {
            35 => Some(0),
            50 => Some(1),
            61 => Some(3),
            _ => None,
        };
        let transaction = match (height, spent) {
            (36, _) => Some(
                s.tx()
                    .input(&minted, 1)
                    .output(Output::new(ordinary(5), 1_400_000).token(&policy, &name, 2)),
            ),
            (_, Some(index)) => Some(
                s.tx()
                    .input(&creator, index)
                    .output(Output::new(ordinary(9), 2_500_000)),
            ),
            _ => None,
        };
        let hashes = s.block(transaction.into_iter().collect());
        if height == 35 {
            spender = hashes[0].clone();
        }
    }
    Reserve {
        g: s.g,
        creator,
        spender,
    }
}

/// `hook` while `on` is set; after, the chain answers alone.
fn while_on(on: Arc<AtomicBool>, hook: Hook) -> Hook {
    Arc::new(move |path: &str, chain: &mut Chain, now| {
        if on.load(Ordering::SeqCst) {
            hook(path, chain, now)
        } else {
            None
        }
    })
}

/// A build of `g`, served through `through`, refused under `hook` at the
/// edge chain's H and T; then `on` is cleared, which the hook reads, and
/// the same command runs again on the same database. The outcome: the
/// refusal's code and report, whether the refused database served, the
/// requests of each run, and the continued build: its code when it was
/// refused, else whether it continued the stored build, its difference
/// from `conversion` (`null` when the digests, supply and position are
/// equal) and the rows only one side holds (`harness::diff`).
async fn refused_then_continued(
    profile: &Profile,
    g: &Generated,
    through: u32,
    hook: (Hook, Arc<AtomicBool>),
    converted: &Database,
    conversion: &dsa_store::verify::Verification,
) -> Result<serde_json::Value> {
    let (outcome, _) =
        refused_then_continued_paths(profile, g, through, hook, converted, conversion).await?;
    Ok(outcome)
}

/// `refused_then_continued`, with the paths the continued build asked.
async fn refused_then_continued_paths(
    profile: &Profile,
    g: &Generated,
    through: u32,
    (hook, on): (Hook, Arc<AtomicBool>),
    converted: &Database,
    conversion: &dsa_store::verify::Verification,
) -> Result<(serde_json::Value, Vec<String>)> {
    let rebuilt = Database::new().await?;
    empty(&rebuilt, profile).await?;
    let fake = Fake::with_chain(edge_chain(profile, g, through), Some(hook)).await;
    let (_stopper, stop) = Stop::new();
    let refusal = build(profile, &fake, &rebuilt, EDGE_HORIZON, EDGE_TARGET, stop).await;
    let first = fake.paths().len();
    let serves = serving(&rebuilt).await?;
    on.store(false, Ordering::SeqCst);
    let (_stopper, stop) = Stop::new();
    let continued = match build(profile, &fake, &rebuilt, EDGE_HORIZON, EDGE_TARGET, stop).await {
        Ok(report) => {
            let rows = harness::diff(&rebuilt.config, &converted.config).await?;
            let ids = descriptor_ids(profile, &rebuilt).await?;
            let differs = same(&report.verified, conversion, "continued")
                .err()
                .map(|error| error.to_string());
            json!({"code":null,"resumed":report.resumed,"differs":differs,"rows":rows,
                "descriptor_ids":ids})
        }
        Err(error) => json!({"code":code(&error),"error":error.report()}),
    };
    let paths = fake.paths().split_off(first);
    let requests = json!({"refused":first,"continued":paths.len()});
    rebuilt.cleanup().await?;
    let outcome = match refusal {
        Ok(_) => json!({"code":null,"serving":serves,"requests":requests,"continued":continued}),
        Err(error) => json!({"code":code(&error),"error":error.report(),"serving":serves,
            "requests":requests,"continued":continued}),
    };
    Ok((outcome, paths))
}

/// Paths among `paths` that contain `part`.
fn holding(paths: &[String], part: &str) -> usize {
    paths.iter().filter(|path| path.contains(part)).count()
}

/// The outcome of `refused_then_continued` is the refusal `expected` with
/// the gate closed, followed by a build that continued the stored one and
/// equals the conversion row by row.
fn refused_then_equal(outcome: &serde_json::Value, expected: &str) {
    assert_eq!(outcome["code"], expected, "{outcome}");
    assert_eq!(outcome["serving"], false, "{outcome}");
    let continued = &outcome["continued"];
    assert!(continued["code"].is_null(), "continued: {outcome}");
    assert_eq!(continued["resumed"], true, "continued: {outcome}");
    assert!(continued["differs"].is_null(), "continued: {outcome}");
    assert_eq!(continued["rows"], json!({}), "continued: {outcome}");
    assert_eq!(
        continued["descriptor_ids"],
        json!([]),
        "continued: {outcome}"
    );
}

/// Lists and answers that miss an output relevant by its address alone
/// refuse the build, and the gate stays closed: the spend in the window
/// missing from the address's list from H on (the address's own
/// derivation from its full list below H keeps the output), the creating
/// transaction missing from the address's list below H (no list places
/// the listed outputs), and the spend in the window answered 404 every time
/// it is asked. After each refusal the same command, answered honestly,
/// equals the conversion row by row: whichever answer missed the output,
/// the listings and the address's full list below H are read again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn listings_that_miss_an_address_output_refuse_the_build() -> Result {
    let profile = profile(1);
    let chain = reserve_chain(&profile, false);
    let converted = Database::new().await?;
    let (source, conversion) = edge_conversion(&profile, &chain.g, &converted).await?;
    let list = format!("/addresses/{}/transactions?", profile.addresses.reserve);
    let without = |hidden: String, since: bool| -> Hook {
        let list = list.clone();
        Arc::new(move |path: &str, chain: &mut Chain, _| {
            if !path.contains(&list) || path.contains("from=") != since {
                return None;
            }
            let rows: Vec<serde_json::Value> =
                serde_json::from_str(&chain.answer(path).body).ok()?;
            let kept: Vec<_> = rows
                .into_iter()
                .filter(|row| row["tx_hash"] != hidden.as_str())
                .collect();
            Some(Behavior::json(serde_json::Value::Array(kept)))
        })
    };
    let unanswered: Hook = {
        let path = format!("/txs/{}/utxos", chain.spender);
        Arc::new(move |asked: &str, _: &mut Chain, _| {
            asked.ends_with(&path).then(|| Behavior::status(404))
        })
    };
    let mut outcomes = Vec::new();
    for (name, hook, expected) in [
        (
            "spend_missing_from_the_list_since_the_horizon",
            without(chain.spender.clone(), true),
            "rebuild_address_outputs_mismatch",
        ),
        (
            "creator_missing_from_the_list_below_the_horizon",
            without(chain.creator.clone(), false),
            "rebuild_listing_incomplete",
        ),
        (
            "spend_answered_404",
            unanswered,
            "provider_transaction_unknown",
        ),
    ] {
        let on = Arc::new(AtomicBool::new(true));
        let mut outcome = refused_then_continued(
            &profile,
            &chain.g,
            EDGE_END,
            (while_on(on.clone(), hook), on),
            &converted,
            &conversion,
        )
        .await?;
        outcome["case"] = json!(name);
        outcomes.push((expected, outcome));
    }
    println!(
        "{}",
        json!({"event":"address_refusals","outcomes":outcomes.iter().map(|(_, o)| o).collect::<Vec<_>>()})
    );
    for (expected, outcome) in &outcomes {
        refused_then_equal(outcome, expected);
    }
    converted.cleanup().await?;
    source.database.cleanup().await
}

/// When the reserve address's two derivations differ, the refusal names
/// each differing output with what the provider said of it: which
/// derivation found it, the `consumed_by_tx` that its creating
/// transaction's answer gave and whether the address's listing shows it.
/// Three answers of the creating transaction at height 2 that contradict
/// the chain: output 0 unspent (`consumed_by_tx` null) while its spend in
/// the window is missing from the list from H on; output 3 spent by a
/// transaction of the list below H while the listing shows it; output 4
/// with other lovelace than the listing's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn an_address_outputs_mismatch_names_what_the_provider_said() -> Result {
    let profile = profile(1);
    let chain = reserve_chain(&profile, false);
    let creator = chain.creator.clone();
    let outputs = format!("/txs/{creator}/utxos");
    let rewritten = |index: u64, change: fn(&mut serde_json::Value, &str)| -> Hook {
        let (outputs, creator) = (outputs.clone(), creator.clone());
        Arc::new(move |path: &str, chain: &mut Chain, _| {
            if !path.ends_with(&outputs) {
                return None;
            }
            let mut answer: serde_json::Value =
                serde_json::from_str(&chain.answer(path).body).ok()?;
            for output in answer["outputs"].as_array_mut()? {
                if output["output_index"] == index {
                    change(output, &creator);
                }
            }
            Some(Behavior::json(answer))
        })
    };
    let since = format!("/addresses/{}/transactions?", profile.addresses.reserve);
    let spender = chain.spender.clone();
    let hide_spend: Hook = Arc::new(move |path: &str, chain: &mut Chain, _| {
        if !path.contains(&since) || !path.contains("from=") {
            return None;
        }
        let rows: Vec<serde_json::Value> = serde_json::from_str(&chain.answer(path).body).ok()?;
        let kept = rows
            .into_iter()
            .filter(|row| row["tx_hash"] != spender.as_str())
            .collect();
        Some(Behavior::json(serde_json::Value::Array(kept)))
    });
    let unspent_output = rewritten(0, |output, _| output["consumed_by_tx"] = json!(null));
    let both: Hook = Arc::new(move |path: &str, chain: &mut Chain, now| {
        hide_spend(path, chain, now).or_else(|| unspent_output(path, chain, now))
    });
    let point = |index: u16| format!("{creator}#{index}");
    let mut outcomes = Vec::new();
    for (name, hook, expected) in [
        (
            "unspent_in_the_history_spent_by_no_list",
            both,
            json!({"output": point(0), "found_by": "address_history", "in_address_history": true,
                "consumed_by_tx": null, "listed": false}),
        ),
        (
            "spent_below_the_horizon_in_the_history_and_listed",
            rewritten(3, |output, creator| {
                output["consumed_by_tx"] = json!(creator)
            }),
            json!({"output": point(3), "found_by": "listings", "in_address_history": true,
                "consumed_by_tx": creator.clone(), "listed": true}),
        ),
        (
            "other_lovelace_in_the_history",
            rewritten(4, |output, _| {
                output["amount"][0]["quantity"] = json!("2999999")
            }),
            json!({"output": point(4), "found_by": "both", "in_address_history": true,
                "consumed_by_tx": null, "listed": true}),
        ),
    ] {
        let (error, serves) = refused(&profile, &chain.g, hook).await?;
        let report = error.report();
        outcomes.push(json!({"case":name,"error":report,"serving":serves}));
        assert_eq!(
            report["error"], "rebuild_address_outputs_mismatch",
            "{name}: {report}"
        );
        assert!(!serves, "{name}: the database serves");
        let detail = &report["detail"];
        assert_eq!(detail["address"], profile.addresses.reserve, "{name}");
        assert_eq!(detail["differing"], 1, "{name}: {detail}");
        assert_eq!(detail["outputs"], json!([expected]), "{name}: {detail}");
    }
    println!(
        "{}",
        json!({"event":"address_mismatch_detail","outcomes":outcomes})
    );
    Ok(())
}

/// The provider's tip block is replaced each time the build checks it
/// after the lists from H on: the address listings and the lists are read
/// again three times, and the fourth reading ends the build with
/// `provider_tip_left_chain` while nothing serves. The same command, with
/// the tip left in place, reads the address listings and the lists from H
/// on again, keeps the unit listings and the readings, and equals the
/// conversion row by row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn a_tip_that_keeps_leaving_the_chain_refuses_until_it_stays() -> Result {
    let profile = profile(1);
    let chain = reserve_chain(&profile, true);
    let converted = Database::new().await?;
    let (source, conversion) = edge_conversion(&profile, &chain.g, &converted).await?;
    let replaced = Arc::new(AtomicUsize::new(0));
    let tip = format!("/blocks/{}", EDGE_END + 1);
    let hook: Hook = {
        let replaced = replaced.clone();
        Arc::new(move |path: &str, chain: &mut Chain, _| {
            if path.ends_with(&tip) {
                let salt = replaced.fetch_add(1, Ordering::SeqCst) as u64 + 1;
                let blocks = chain.blocks_mut();
                let last = blocks.pop().expect("a tip");
                blocks.push(generate::alternative(&last, salt, 0));
            }
            None
        })
    };
    let on = Arc::new(AtomicBool::new(true));
    let (outcome, paths) = refused_then_continued_paths(
        &profile,
        &chain.g,
        EDGE_END + 1,
        (while_on(on.clone(), hook), on),
        &converted,
        &conversion,
    )
    .await?;
    let replaced = replaced.load(Ordering::SeqCst);
    let cnight = profile.assets.cnight.unit();
    let (unit_listing, history) = (
        format!("/assets/{cnight}/utxos?"),
        format!("/assets/{cnight}/history?"),
    );
    let reserve = format!("/addresses/{}/utxos?", profile.addresses.reserve);
    let continued = json!({"unit_listings":holding(&paths, &unit_listing),
        "histories":holding(&paths, &history),"reserve_listings":holding(&paths, &reserve)});
    println!(
        "{}",
        json!({"event":"tip_left_chain_four_times","outcome":outcome,"tip_replaced":replaced,
            "continued_paths":continued})
    );
    refused_then_equal(&outcome, "provider_tip_left_chain");
    assert_eq!(outcome["error"]["detail"]["reads"], 4, "{outcome}");
    assert_eq!(replaced, 4, "checks of the tip");
    // The continuation reads the address listings and the lists from H on
    // again, and keeps the unit listings and the readings.
    assert_eq!(continued["unit_listings"], 0, "{continued}");
    assert_eq!(continued["histories"], 0, "{continued}");
    assert!(
        continued["reserve_listings"].as_u64() > Some(0),
        "{continued}"
    );
    converted.cleanup().await?;
    source.database.cleanup().await
}

/// What the ten chain's tests refer to.
struct Ten {
    g: Generated,
    /// Heights 2–11: ten transactions below H that each mint cNIGHT into
    /// one output at an ordinary address, 2 cNIGHT in the first, 3 in the
    /// second, 1 in each other.
    creators: Vec<String>,
    /// Height 35, in the window: spends the first creator's output (A).
    x: String,
}

/// Mainnet profile, k 12, the edge chain's H and T: ten creating
/// transactions below H with one output each, so that the build asks each
/// one's validity, and X and Y in the window spending A (2 cNIGHT) and B
/// (3 cNIGHT). The other blocks are empty.
fn ten_chain(profile: &Profile) -> Ten {
    let ordinary = |index: u8| {
        let mut key = [0x4e; 28];
        key[0] = index;
        enterprise_address(1, key)
    };
    let policy = profile.assets.cnight.policy.clone();
    let name = profile.assets.cnight.name.clone();
    let mut s = Script::new(profile);
    s.block(Vec::new());
    let mut creators = Vec::new();
    for n in 0..10_u8 {
        let amount = match n {
            0 => 2,
            1 => 3,
            _ => 1,
        };
        let tx = s.tx().mint(&policy, &name, amount).output(
            Output::new(ordinary(n), 1_500_000 + u64::from(n)).token(&policy, &name, amount),
        );
        creators.push(s.block(vec![tx])[0].clone());
    }
    let mut x = String::new();
    for height in 12..=EDGE_END {
        let spent = match height {
            35 => Some((0, 2)),
            36 => Some((1, 3)),
            _ => None,
        };
        let transaction = spent.map(|(creator, amount)| {
            s.tx().input(&creators[creator], 0).output(
                Output::new(ordinary(20 + creator as u8), 1_400_000).token(&policy, &name, amount),
            )
        });
        let hashes = s.block(transaction.into_iter().collect());
        if height == 35 {
            x = hashes[0].clone();
        }
    }
    Ten {
        g: s.g,
        creators,
        x,
    }
}

/// A hook that answers 404 for the path that ends with `path`.
fn unknown(path: String) -> Hook {
    Arc::new(move |asked: &str, _: &mut Chain, _| {
        asked.ends_with(&path).then(|| Behavior::status(404))
    })
}

/// `/txs/{hash}` of a creating transaction below H answers 404 while the
/// build asks the transactions' validity: after `fatal_after` the build
/// ends with `provider_transaction_unknown` and nothing serves. Listing
/// again does not cure an unknown transaction below H, so the build
/// discards nothing: the same command, answered honestly, asks for the
/// transaction again, reads no unit listing again and equals the
/// conversion row by row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn an_unknown_transaction_below_the_horizon_keeps_the_listings() -> Result {
    let profile = profile(1);
    let ten = ten_chain(&profile);
    let converted = Database::new().await?;
    let (source, conversion) = edge_conversion(&profile, &ten.g, &converted).await?;
    let on = Arc::new(AtomicBool::new(true));
    let hook = while_on(on.clone(), unknown(format!("/txs/{}", ten.creators[4])));
    let (outcome, paths) = refused_then_continued_paths(
        &profile,
        &ten.g,
        EDGE_END,
        (hook, on),
        &converted,
        &conversion,
    )
    .await?;
    let unit_listing = format!("/assets/{}/utxos?", profile.assets.cnight.unit());
    let asked = holding(&paths, &format!("/txs/{}", ten.creators[4]));
    println!(
        "{}",
        json!({"event":"unknown_below_the_horizon","outcome":outcome,
            "continued_unit_listings":holding(&paths, &unit_listing),"asked_again":asked})
    );
    refused_then_equal(&outcome, "provider_transaction_unknown");
    assert_eq!(
        holding(&paths, &unit_listing),
        0,
        "the unit listings were read again"
    );
    assert!(asked > 0, "the unknown transaction was not asked again");
    converted.cleanup().await?;
    source.database.cleanup().await
}

/// `/txs/{hash}/utxos` of X, a transaction in the window that the unit's
/// list from H on names, answers 404 while the build reads the
/// transactions from H on: after `fatal_after` the build ends with
/// `provider_transaction_unknown`. The lists from H on may have named a
/// transaction that left the chain, so the build discards the listings and
/// the lists from H on: the same command, answered honestly, reads the unit
/// listings again and equals the conversion row by row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn an_unknown_transaction_since_the_horizon_reads_the_listings_again() -> Result {
    let profile = profile(1);
    let ten = ten_chain(&profile);
    let converted = Database::new().await?;
    let (source, conversion) = edge_conversion(&profile, &ten.g, &converted).await?;
    let on = Arc::new(AtomicBool::new(true));
    let hook = while_on(on.clone(), unknown(format!("/txs/{}/utxos", ten.x)));
    let (outcome, paths) = refused_then_continued_paths(
        &profile,
        &ten.g,
        EDGE_END,
        (hook, on),
        &converted,
        &conversion,
    )
    .await?;
    let unit_listing = format!("/assets/{}/utxos?", profile.assets.cnight.unit());
    println!(
        "{}",
        json!({"event":"unknown_since_the_horizon","outcome":outcome,
            "continued_unit_listings":holding(&paths, &unit_listing)})
    );
    refused_then_equal(&outcome, "provider_transaction_unknown");
    assert!(
        holding(&paths, &unit_listing) > 0,
        "the unit listings were not read again"
    );
    converted.cleanup().await?;
    source.database.cleanup().await
}

/// A tip answered 404 once and then with its block is asked again within
/// the outcome rules: it has not left the chain, so the address listings
/// and the lists from H on are not read again, and the build equals the
/// conversion. The reserve chain's tip, height 61, spends a reserve output.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn a_tip_answered_404_once_is_asked_again() -> Result {
    let profile = profile(1);
    let chain = reserve_chain(&profile, true);
    let tip = format!("/blocks/{}", EDGE_END + 1);
    let asked = |fake: &Fake| {
        fake.paths()
            .iter()
            .filter(|path| path.ends_with(&tip))
            .count()
    };
    let listing = format!("/addresses/{}/utxos?", profile.addresses.reserve);
    let plain = Fake::with_chain(edge_chain(&profile, &chain.g, EDGE_END + 1), None).await;
    built_equal(&profile, &chain.g, &plain, "tip answered").await?;
    let answered = Arc::new(AtomicBool::new(false));
    let hook: Hook = {
        let (answered, tip) = (answered.clone(), tip.clone());
        Arc::new(move |path: &str, _: &mut Chain, _| {
            (path.ends_with(&tip) && !answered.swap(true, Ordering::SeqCst))
                .then(|| Behavior::status(404))
        })
    };
    let fake = Fake::with_chain(edge_chain(&profile, &chain.g, EDGE_END + 1), Some(hook)).await;
    let report = built_equal(&profile, &chain.g, &fake, "tip answered 404 once").await?;
    println!(
        "{}",
        json!({"event":"tip_answered_404_once","tip_asked":asked(&fake),
            "tip_asked_without_404":asked(&plain),"reserve_listings":fake.requests(&listing),
            "reserve_listings_without_404":plain.requests(&listing),"calls":report.calls})
    );
    assert!(
        answered.load(Ordering::SeqCst),
        "the tip was not answered 404"
    );
    assert!(asked(&fake) >= 2, "the tip was asked once");
    assert_eq!(asked(&fake), asked(&plain) + 1, "the tip asked again once");
    assert_eq!(
        fake.requests(&listing),
        plain.requests(&listing),
        "the address listings were read again"
    );
    Ok(())
}

/// A tip block that spends an output relevant by its address alone is
/// served while the listings are read and replaced for good when the lists
/// from H on begin: the address listing did not show the output, which was
/// spent when it was read, and no list names the spend. The tip the lists were read up to has left the
/// chain, the address listings and the lists from H on are read again, and
/// the build equals the conversion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn a_spend_that_leaves_the_chain_while_listing_gives_the_conversion() -> Result {
    let profile = profile(1);
    let chain = reserve_chain(&profile, true);
    let converted = Database::new().await?;
    let (source, conversion) = edge_conversion(&profile, &chain.g, &converted).await?;
    let rebuilt = Database::new().await?;
    empty(&rebuilt, &profile).await?;
    let replaced = Arc::new(AtomicUsize::new(0));
    let hook: Hook = {
        let replaced = replaced.clone();
        Arc::new(move |path: &str, chain: &mut Chain, _| {
            if path.contains("/transactions?")
                && path.contains("from=33")
                && replaced.fetch_add(1, Ordering::SeqCst) == 0
            {
                let blocks = chain.blocks_mut();
                let tip = blocks.pop().expect("a tip");
                blocks.push(generate::alternative(&tip, 7, 0));
            }
            None
        })
    };
    let fake = Fake::with_chain(edge_chain(&profile, &chain.g, EDGE_END + 1), Some(hook)).await;
    let (_stopper, stop) = Stop::new();
    let report = build(&profile, &fake, &rebuilt, EDGE_HORIZON, EDGE_TARGET, stop)
        .await
        .map_err(|error| error.report().to_string())?;
    if let Err(error) = same(&report.verified, &conversion, "tip spend left the chain") {
        let rows = harness::diff(&rebuilt.config, &converted.config).await?;
        return Err(format!("{error}; rows {rows}").into());
    }
    let listings = fake.requests(&format!(
        "/addresses/{}/utxos?count=100&page=1",
        profile.addresses.reserve
    ));
    println!(
        "{}",
        json!({"event":"tip_spend_left_the_chain","reserve_listings":listings,
            "tip_checks":fake.requests("/blocks/61"),"calls":report.calls})
    );
    assert!(
        replaced.load(Ordering::SeqCst) > 0,
        "the tip was not replaced"
    );
    // Two passes that agree, then two more after the tip left the chain.
    assert!(listings >= 4, "the reserve address was not listed again");
    rebuilt.cleanup().await?;
    converted.cleanup().await?;
    source.database.cleanup().await
}

/// A listing that lacks a cNIGHT output refuses the build in its proof; the
/// same command, now answered honestly, reads the listings, the readings of
/// the units and the lists from H on again, keeps the places and validity
/// answers below H, and publishes the conversion's digest.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn a_refused_proof_is_read_again_by_the_same_command() -> Result {
    let profile = profile(1);
    let edges = edges(&profile);
    let converted = Database::new().await?;
    let (source, conversion) = edge_conversion(&profile, &edges.g, &converted).await?;
    let rebuilt = Database::new().await?;
    empty(&rebuilt, &profile).await?;
    let listing = format!("/assets/{}/utxos?", edges.cnight);
    let cut = Arc::new(AtomicBool::new(true));
    let hook = rows_hook(listing.clone(), cut.clone(), |rows| {
        if !rows.is_empty() && rows.len() < 100 {
            rows.pop();
        }
    });
    let fake = Fake::with_chain(edge_chain(&profile, &edges.g, EDGE_END), Some(hook)).await;
    let (_stopper, stop) = Stop::new();
    let error = build(&profile, &fake, &rebuilt, EDGE_HORIZON, EDGE_TARGET, stop)
        .await
        .err()
        .ok_or("a listing without an output was published")?;
    assert_eq!(
        code(&error),
        "rebuild_supply_mismatch",
        "{}",
        error.report()
    );
    assert!(!serving(&rebuilt).await?, "the database serves");
    let first = fake.paths();
    cut.store(false, Ordering::SeqCst);
    let (_stopper, stop) = Stop::new();
    let report = build(&profile, &fake, &rebuilt, EDGE_HORIZON, EDGE_TARGET, stop)
        .await
        .map_err(|error| format!("continued: {}", error.report()))?;
    same(&report.verified, &conversion, "read again")?;
    let second: Vec<String> = fake.paths()[first.len()..].to_vec();
    let count = |part: &str, paths: &[String]| paths.iter().filter(|p| p.contains(part)).count();
    // A list below H read for places: `to` below H without `from`.
    let below = format!("&to={}", EDGE_HORIZON - 1);
    let places = |paths: &[String]| {
        paths
            .iter()
            .filter(|p| p.contains("/transactions?") && p.contains(&below) && !p.contains("from="))
            .count()
    };
    // `/txs/{hash}` without `/utxos`: places and validity of single transactions.
    let single = |paths: &[String]| {
        paths
            .iter()
            .filter(|p| p.contains("/api/v0/txs/") && !p.ends_with("/utxos"))
            .count()
    };
    // The address listings, the lists from H on, and the provider's tip
    // those lists are read up to, asked before the first of them.
    let addresses = |paths: &[String]| {
        paths
            .iter()
            .filter(|p| p.contains("/addresses/") && p.contains("/utxos?"))
            .count()
    };
    let from_horizon = format!("from={EDGE_HORIZON}&");
    let since = |paths: &[String]| {
        paths
            .iter()
            .filter(|p| p.contains("/transactions?") && p.contains(&from_horizon))
            .count()
    };
    let tip_before_lists = |paths: &[String]| {
        paths
            .iter()
            .position(|p| p.contains("/transactions?") && p.contains(&from_horizon))
            .is_some_and(|at| paths[..at].iter().any(|p| p.ends_with("/blocks/latest")))
    };
    let outcome = json!({"refusal":error.report(),"resumed":report.resumed,
        "first_run":{"requests":first.len(),"listing":count(&listing, &first),
            "histories":count("/history?", &first),"lists_below_h":places(&first),"single_transactions":single(&first),
            "address_listings":addresses(&first),"lists_from_h":since(&first),"tip_before_lists":tip_before_lists(&first)},
        "second_run":{"requests":second.len(),"listing":count(&listing, &second),
            "histories":count("/history?", &second),"lists_below_h":places(&second),"single_transactions":single(&second),
            "address_listings":addresses(&second),"lists_from_h":since(&second),"tip_before_lists":tip_before_lists(&second)}});
    println!("{}", json!({"event":"read_again","outcome":outcome}));
    assert!(report.resumed);
    assert!(
        count(&listing, &second) > 0,
        "the listing was not read again"
    );
    assert!(
        count("/history?", &second) > 0,
        "the readings were not read again"
    );
    assert!(
        addresses(&second) >= 2 * profile.addresses.iter().count(),
        "the address listings were not read again: {outcome}"
    );
    assert_eq!(
        since(&second),
        since(&first),
        "the lists from H on were not read again: {outcome}"
    );
    assert!(
        tip_before_lists(&second),
        "the tip was not read again before the lists: {outcome}"
    );
    assert_eq!(places(&second), 0, "the places below H were read again");
    // Only the first mints of the fixed units are asked again: the
    // validity answers are kept.
    assert!(
        single(&second) <= dsa_chain::assets::fixed_units(&profile).len(),
        "validity was read again: {outcome}"
    );
    rebuilt.cleanup().await?;
    converted.cleanup().await?;
    source.database.cleanup().await
}

/// Mainnet profile, k 12, sixty blocks two hours apart. Height 1 mints 10
/// cNIGHT into five outputs at ordinary addresses; heights 10 and 40 each
/// spend one of them into a new cNIGHT output.
fn slow_chain(profile: &Profile) -> Generated {
    let ordinary = |index: u8| {
        let mut key = [0x6e; 28];
        key[0] = index;
        enterprise_address(1, key)
    };
    let cnight = profile.assets.cnight.clone();
    let mut s = Script::with_step(profile, 7_200);
    let mut tx = s.tx().mint(&cnight.policy, &cnight.name, 10);
    for index in 0..5_u8 {
        tx = tx.output(Output::new(ordinary(index), 1_500_000).token(
            &cnight.policy,
            &cnight.name,
            2,
        ));
    }
    let minted = s.block(vec![tx])[0].clone();
    for height in 2..=EDGE_END {
        let transaction = matches!(height, 10 | 40).then(|| {
            s.tx()
                .input(&minted, (height / 10) as u16)
                .output(Output::new(ordinary(5), 1_400_000).token(&cnight.policy, &cnight.name, 2))
        });
        s.block(transaction.into_iter().collect());
    }
    s.g
}

/// The command as an operator types it, without a position: the target is
/// the provider's tip less k (60 − 12 = 48) and the horizon the newest block
/// at or before the target's time less the retention of one day (twelve
/// blocks of two hours: 36). The build equals a conversion at those
/// positions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn a_build_without_positions_takes_them_from_the_provider() -> Result {
    let profile = profile(1);
    let g = slow_chain(&profile);
    let (horizon, target) = (36, EDGE_END - K);
    let converted = Database::new().await?;
    let (source, conversion) = conversion_at(&profile, &g, &converted, horizon, target).await?;
    let rebuilt = Database::new().await?;
    empty(&rebuilt, &profile).await?;
    let fake = Fake::with_chain(edge_chain(&profile, &g, EDGE_END), None).await;
    let (_stopper, stop) = Stop::new();
    let options = Options {
        horizon_block: None,
        target_block: None,
        window_seconds: None,
        retention_seconds: DAY,
        shortest_window_seconds: DAY,
        batch_items: 0,
    };
    let report = build_with(&profile, &fake, &rebuilt, options, stop)
        .await
        .map_err(|error| error.report().to_string())?;
    println!(
        "{}",
        json!({"event":"positions_from_the_provider","plan":report.plan,"calls":report.calls})
    );
    assert_eq!((report.plan.horizon, report.plan.target), (horizon, target));
    if let Err(error) = same(&report.verified, &conversion, "positions from the provider") {
        let rows = harness::diff(&rebuilt.config, &converted.config).await?;
        return Err(format!("{error}; rows {rows}").into());
    }
    rebuilt.cleanup().await?;
    converted.cleanup().await?;
    source.database.cleanup().await
}

/// `--window-seconds` below the retention, without a position: the target is
/// 48 as above; the horizon is the newest block at or before the target's
/// time less 90,000 seconds (twelve and a half blocks of two hours: 35, not
/// 36, the oldest at or after it), and the plan and the database's origin
/// hold the window as the chain time from 35 to 48, 93,600 seconds. The
/// build equals a conversion at those positions with the build's retention
/// of two days.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn a_window_below_the_retention_starts_at_the_newest_block_before_it() -> Result {
    let profile = profile(1);
    let g = slow_chain(&profile);
    let (horizon, target) = (35, EDGE_END - K);
    let converted = Database::new().await?;
    let (source, conversion) =
        conversion_with(&profile, &g, &converted, (horizon, target), 2 * DAY).await?;
    let rebuilt = Database::new().await?;
    empty(&rebuilt, &profile).await?;
    let fake = Fake::with_chain(edge_chain(&profile, &g, EDGE_END), None).await;
    let (_stopper, stop) = Stop::new();
    let options = Options {
        horizon_block: None,
        target_block: None,
        window_seconds: Some(90_000),
        retention_seconds: 2 * DAY,
        shortest_window_seconds: DAY,
        batch_items: 0,
    };
    let report = build_with(&profile, &fake, &rebuilt, options, stop)
        .await
        .map_err(|error| error.report().to_string())?;
    let origin: serde_json::Value = common::connect(&rebuilt.config)
        .await?
        .query_one("SELECT origin FROM dsa.identity WHERE id=1", &[])
        .await?
        .get(0);
    let rows = harness::diff(&rebuilt.config, &converted.config).await?;
    let ids = descriptor_ids(&profile, &rebuilt).await?;
    println!(
        "{}",
        json!({"event":"window_below_the_retention","plan":report.plan,"origin":origin,
            "rows":rows,"descriptor_ids":ids,"calls":report.calls})
    );
    assert_eq!((report.plan.horizon, report.plan.target), (horizon, target));
    assert_eq!(report.plan.window_seconds, 93_600, "the stored window");
    assert_eq!(origin["window_seconds"], 93_600, "the origin's window");
    same(
        &report.verified,
        &conversion,
        "a window below the retention",
    )
    .map_err(|error| format!("{error}; rows {rows}"))?;
    assert_eq!(rows, json!({}), "rows differ");
    assert_eq!(ids, json!([]), "descriptor IDs");
    rebuilt.cleanup().await?;
    converted.cleanup().await?;
    source.database.cleanup().await
}

/// Positions a build refuses before it changes the database: a target above
/// the provider's tip less k, a window shorter than a day or longer than the
/// retention, a horizon of 0 or above the target.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn positions_out_of_range_refuse_the_build_before_it_starts() -> Result {
    let profile = profile(1);
    let g = slow_chain(&profile);
    let options = |horizon, target, window| Options {
        horizon_block: horizon,
        target_block: target,
        window_seconds: window,
        retention_seconds: 2 * DAY,
        shortest_window_seconds: DAY,
        batch_items: 0,
    };
    let mut outcomes = Vec::new();
    for (name, choices, expected) in [
        (
            "target above the provider's tip less k",
            options(None, Some(EDGE_END - K + 1), None),
            "target_above_provider_final",
        ),
        (
            "window shorter than a day",
            options(None, None, Some(DAY - 1)),
            "invalid_window",
        ),
        (
            "window longer than the retention",
            options(None, None, Some(2 * DAY + 1)),
            "invalid_window",
        ),
        (
            "horizon 0",
            options(Some(0), Some(40), None),
            "invalid_horizon",
        ),
        (
            "horizon above the target",
            options(Some(41), Some(40), None),
            "invalid_horizon",
        ),
    ] {
        let rebuilt = Database::new().await?;
        empty(&rebuilt, &profile).await?;
        let fake = Fake::with_chain(edge_chain(&profile, &g, EDGE_END), None).await;
        let (_stopper, stop) = Stop::new();
        let error = build_with(&profile, &fake, &rebuilt, choices, stop)
            .await
            .err()
            .ok_or_else(|| format!("{name}: the build ran"))?;
        let blank: bool = common::connect(&rebuilt.config)
            .await?
            .query_one(
                "SELECT to_regclass('dsa.identity') IS NULL AND to_regnamespace('dsa_rebuild') IS NULL",
                &[],
            )
            .await?
            .get(0);
        outcomes.push(json!({"case":name,"error":error.report(),"blank":blank}));
        assert_eq!(code(&error), expected, "{name}: {}", error.report());
        assert!(blank, "{name}: the build changed the database");
        rebuilt.cleanup().await?;
    }
    println!(
        "{}",
        json!({"event":"positions_refused","outcomes":outcomes})
    );
    Ok(())
}

/// Facts the provider must have and denies refuse the build, and the gate
/// stays closed: a unit of the network's table of fixed descriptors (every
/// request about the permissioned-candidates unit answered 404), the
/// parameters of epoch(H) − 2, and the blocks of the epochs whose newest
/// block the build keeps.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn facts_the_provider_denies_refuse_the_build() -> Result {
    let profile = profile(1);
    let edges = edges(&profile);
    let permissioned = format!("/assets/{}", profile.assets.permissioned_candidates.unit());
    let unknown_unit: Hook = Arc::new(move |path: &str, _: &mut Chain, _| {
        path.contains(&permissioned).then(|| Behavior::status(404))
    });
    let horizon_epoch = edges
        .g
        .block(EDGE_HORIZON)
        .map(|block| block.epoch)
        .ok_or("no horizon block")?;
    let parameters = format!("/epochs/{}/parameters", horizon_epoch - 2);
    let no_parameters: Hook = Arc::new(move |path: &str, _: &mut Chain, _| {
        path.ends_with(&parameters).then(|| Behavior::status(404))
    });
    let no_blocks: Hook = Arc::new(move |path: &str, _: &mut Chain, _| {
        (path.contains("/epochs/") && path.contains("/blocks?")).then(|| Behavior::json(json!([])))
    });
    let mut outcomes = Vec::new();
    for (name, hook, expected) in [
        ("fixed unit unknown", unknown_unit, "provider_unit_unknown"),
        (
            "parameters of epoch(H) - 2 missing",
            no_parameters,
            "provider_epoch_parameters_missing",
        ),
        (
            "epochs without blocks",
            no_blocks,
            "provider_epoch_blocks_missing",
        ),
    ] {
        let outcome = match refused(&profile, &edges.g, hook).await {
            Ok((error, serves)) => {
                json!({"case":name,"code":code(&error),"error":error.report(),"serving":serves})
            }
            Err(error) => json!({"case":name,"code":null,"failure":error.to_string()}),
        };
        outcomes.push((expected, outcome));
    }
    println!(
        "{}",
        json!({"event":"denied_facts","outcomes":outcomes.iter().map(|(_, o)| o).collect::<Vec<_>>()})
    );
    for (expected, outcome) in &outcomes {
        assert_eq!(outcome["code"], *expected, "{outcome}");
        assert_eq!(outcome["serving"], false, "{outcome}");
    }
    Ok(())
}

/// The request counts a build in `database` stored, by key.
async fn stored_requests(database: &Database) -> Result<Vec<(String, serde_json::Value)>> {
    Ok(common::connect(&database.config)
        .await?
        .query(
            "SELECT key,value FROM dsa_rebuild.item WHERE kind='requests' ORDER BY key",
            &[],
        )
        .await?
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect())
}

/// A build stopped in its window continues only with the choices it began
/// with and by the version that began it: another version is
/// `rebuild_version_mismatch` and leaves the build's stored request counts
/// as they are; another retention, window or horizon is
/// `rebuild_plan_mismatch`. Continued with the same choices it equals
/// the conversion, and its report holds the real window, the base's counts
/// and the requests of the whole build by family, which add up to `calls`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn a_stopped_build_continues_only_as_it_began() -> Result {
    let profile = profile(1);
    let edges = edges(&profile);
    let converted = Database::new().await?;
    let (source, conversion) = edge_conversion(&profile, &edges.g, &converted).await?;
    let rebuilt = Database::new().await?;
    empty(&rebuilt, &profile).await?;
    let fake = Fake::with_chain(edge_chain(&profile, &edges.g, EDGE_END), None).await;
    let choices = |retention: i64, window: Option<i64>, horizon: u32| Options {
        horizon_block: Some(horizon),
        target_block: Some(EDGE_TARGET),
        window_seconds: window,
        retention_seconds: retention,
        shortest_window_seconds: 0,
        batch_items: 0,
    };
    let (stopper, stop) = Stop::new();
    let run = build_with(
        &profile,
        &fake,
        &rebuilt,
        choices(DAY, None, EDGE_HORIZON),
        stop,
    );
    let stopping = async {
        let mut from = None;
        loop {
            let answered = fake.paths().len();
            if from.is_none() && stored_phase(&rebuilt).await.as_deref() == Some("window") {
                from = Some(answered);
            }
            if from.is_some_and(|from| answered >= from + 5) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let _ = stopper.send(true);
    };
    let (result, ()) = tokio::join!(run, stopping);
    assert_eq!(
        code(&result.err().ok_or("the stopped build published")?),
        "stop_requested"
    );
    // Another version refuses the build and changes none of its stored
    // request counts.
    let client = common::connect(&rebuilt.config).await?;
    let other_version = format!("{}+other", env!("CARGO_PKG_VERSION"));
    client
        .execute(
            "UPDATE dsa_rebuild.progress SET plan=jsonb_set(plan,'{version}',to_jsonb($1::text)) WHERE id=1",
            &[&other_version],
        )
        .await?;
    let before_version = stored_requests(&rebuilt).await?;
    let (_stopper, stop) = Stop::new();
    let outcome = match build_with(
        &profile,
        &fake,
        &rebuilt,
        choices(DAY, None, EDGE_HORIZON),
        stop,
    )
    .await
    {
        Ok(_) => json!({"case":"version","code":null}),
        Err(error) => json!({"case":"version","code":code(&error),"error":error.report()}),
    };
    let after_version = stored_requests(&rebuilt).await?;
    let mut outcomes = vec![("rebuild_version_mismatch", outcome)];
    client
        .execute(
            "UPDATE dsa_rebuild.progress SET plan=jsonb_set(plan,'{version}',to_jsonb($1::text)) WHERE id=1",
            &[&env!("CARGO_PKG_VERSION")],
        )
        .await?;
    for (name, other, expected) in [
        (
            "retention",
            choices(2 * DAY, None, EDGE_HORIZON),
            "rebuild_plan_mismatch",
        ),
        (
            "window",
            choices(DAY, Some(DAY), EDGE_HORIZON),
            "rebuild_plan_mismatch",
        ),
        (
            "horizon",
            choices(DAY, None, EDGE_HORIZON + 1),
            "rebuild_plan_mismatch",
        ),
    ] {
        let (_stopper, stop) = Stop::new();
        let outcome = match build_with(&profile, &fake, &rebuilt, other, stop).await {
            Ok(_) => json!({"case":name,"code":null}),
            Err(error) => json!({"case":name,"code":code(&error),"error":error.report()}),
        };
        outcomes.push((expected, outcome));
    }
    let (_stopper, stop) = Stop::new();
    let report = build_with(
        &profile,
        &fake,
        &rebuilt,
        choices(DAY, None, EDGE_HORIZON),
        stop,
    )
    .await
    .map_err(|error| format!("continued: {}", error.report()))?;
    same(&report.verified, &conversion, "continued as it began")?;
    let time = |height: u32| edges.g.block(height).map(|block| block.time).unwrap_or(0);
    let window = (time(EDGE_TARGET) - time(EDGE_HORIZON)) as i64;
    let requests: u64 = report.requests.values().sum();
    println!(
        "{}",
        json!({"event":"continued_as_it_began","refused":outcomes.iter().map(|(_, o)| o).collect::<Vec<_>>(),
            "window_seconds":report.plan.window_seconds,"counts":report.counts,
            "requests":report.requests,"requests_total":requests,"calls":report.calls,
            "answered":fake.paths().len(),
            "stored_requests":{"before_other_version":format!("{before_version:?}"),
                "after_other_version":format!("{after_version:?}")}})
    );
    for (expected, outcome) in &outcomes {
        assert_eq!(outcome["code"], *expected, "{outcome}");
    }
    assert!(
        before_version.iter().any(|(key, _)| key == "run"),
        "the stopped run recorded no requests"
    );
    assert_eq!(
        before_version, after_version,
        "another version changed the stored request counts"
    );
    assert!(report.resumed);
    assert_eq!(report.plan.window_seconds, window, "the real window");
    assert!(
        report.counts["unspent_at_horizon"]
            .as_u64()
            .is_some_and(|n| n > 0),
        "counts {}",
        report.counts
    );
    assert_eq!(
        requests as i64, report.calls,
        "requests by family against those sent"
    );
    rebuilt.cleanup().await?;
    converted.cleanup().await?;
    source.database.cleanup().await
}

/// The endpoint family a request path is counted in by the client.
fn family_of(path: &str) -> &'static str {
    let path = path.split('?').next().unwrap_or(path);
    let path = path.split_once("/v0/").map_or(path, |(_, rest)| rest);
    let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    match parts.as_slice() {
        ["assets", "policy", ..] | ["assets", _, "utxos" | "transactions"] | ["addresses", ..] => {
            "listings"
        }
        ["assets", ..] => "assets",
        ["blocks", _, "txs", "cbor"] => "transactions",
        ["blocks", ..] => "headers",
        ["txs", _, "utxos"] => "outputs",
        ["txs", _, "metadata"] => "metadata",
        ["txs", ..] => "transactions",
        ["scripts", "datum", ..] => "datums",
        ["epochs", ..] => "epochs",
        ["pools", ..] => "pools",
        ["genesis"] => "genesis",
        _ => "unknown",
    }
}

/// The requests of a build by family are those the provider received: four
/// requests in flight at ten a second, the reserve address's list below H
/// shorter than one page (the pages asked with page 1 are dropped when it
/// ends, sent or not) and one 429 with `retry-after: 1` that every request
/// in flight waits out. Each family of the report equals the fake's tally of
/// the paths it received, and the families add up to `calls`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn requests_by_family_are_those_the_provider_received() -> Result {
    let profile = profile(1);
    let chain = reserve_chain(&profile, false);
    let limited = Arc::new(AtomicBool::new(false));
    let hook: Hook = {
        let limited = limited.clone();
        Arc::new(move |path: &str, _: &mut Chain, _| {
            (path.contains("/txs/") && !limited.swap(true, Ordering::SeqCst))
                .then(|| Behavior::status(429).retry_after("1"))
        })
    };
    let fake = Fake::with_chain(edge_chain(&profile, &chain.g, EDGE_END), Some(hook)).await;
    let report = built_equal(&profile, &chain.g, &fake, "requests by family").await?;
    let mut received: BTreeMap<String, u64> = BTreeMap::new();
    for path in fake.paths() {
        *received.entry(family_of(&path).to_owned()).or_default() += 1;
    }
    let list = format!("/addresses/{}/transactions?", profile.addresses.reserve);
    println!(
        "{}",
        json!({"event":"requests_by_family","report":report.requests,"received":received,
            "calls":report.calls,"reserve_list_pages":fake.requests(&list)})
    );
    assert!(limited.load(Ordering::SeqCst), "no request was refused");
    assert_eq!(
        report.requests, received,
        "families against the paths received"
    );
    let total: u64 = report.requests.values().sum();
    assert_eq!(total as i64, report.calls, "families against calls");
    Ok(())
}

/// The window of a given horizon is the chain time from it to the target:
/// stored as such, refused below the shortest, and allowed above the
/// retention. Without a horizon, a chain younger than the window is
/// refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn the_window_of_a_build_is_its_chain_time() -> Result {
    let profile = profile(1);
    let slow = slow_chain(&profile);
    let target = EDGE_END - K;
    let converted = Database::new().await?;
    let (source, conversion) = conversion_at(&profile, &slow, &converted, 20, target).await?;
    let rebuilt = Database::new().await?;
    empty(&rebuilt, &profile).await?;
    let fake = Fake::with_chain(edge_chain(&profile, &slow, EDGE_END), None).await;
    let (_stopper, stop) = Stop::new();
    let long = Options {
        horizon_block: Some(20),
        target_block: Some(target),
        window_seconds: None,
        retention_seconds: DAY,
        shortest_window_seconds: DAY,
        batch_items: 0,
    };
    let report = build_with(&profile, &fake, &rebuilt, long, stop)
        .await
        .map_err(|error| format!("a window above the retention: {}", error.report()))?;
    same(
        &report.verified,
        &conversion,
        "a window above the retention",
    )?;
    let long_window = report.plan.window_seconds;
    rebuilt.cleanup().await?;
    converted.cleanup().await?;
    source.database.cleanup().await?;
    let edges = edges(&profile);
    let mut outcomes = Vec::new();
    for (name, g, options) in [
        (
            "a given horizon less than a day below the target",
            &edges.g,
            Options {
                horizon_block: Some(EDGE_HORIZON),
                target_block: Some(EDGE_TARGET),
                window_seconds: None,
                retention_seconds: DAY,
                shortest_window_seconds: DAY,
                batch_items: 0,
            },
        ),
        (
            "a chain younger than the window",
            &slow,
            Options {
                horizon_block: None,
                target_block: None,
                window_seconds: None,
                retention_seconds: 7 * DAY,
                shortest_window_seconds: DAY,
                batch_items: 0,
            },
        ),
    ] {
        let rebuilt = Database::new().await?;
        empty(&rebuilt, &profile).await?;
        let fake = Fake::with_chain(edge_chain(&profile, g, EDGE_END), None).await;
        let (_stopper, stop) = Stop::new();
        let outcome = match build_with(&profile, &fake, &rebuilt, options, stop).await {
            Ok(report) => json!({"case":name,"code":null,"plan":report.plan}),
            Err(error) => json!({"case":name,"code":code(&error),"error":error.report()}),
        };
        rebuilt.cleanup().await?;
        outcomes.push(outcome);
    }
    println!(
        "{}",
        json!({"event":"window_is_chain_time","above_retention":long_window,"refused":outcomes})
    );
    assert_eq!(long_window, 28 * 7_200, "the window above the retention");
    for outcome in &outcomes {
        assert_eq!(outcome["code"], "invalid_window", "{outcome}");
    }
    Ok(())
}

/// Stops a build of the edge chain into `rebuilt` five requests into its
/// window.
async fn stopped_in_window(profile: &Profile, fake: &Fake, rebuilt: &Database) -> Result {
    let (stopper, stop) = Stop::new();
    let run = build(profile, fake, rebuilt, EDGE_HORIZON, EDGE_TARGET, stop);
    let stopping = async {
        let mut from = None;
        loop {
            let answered = fake.paths().len();
            if from.is_none() && stored_phase(rebuilt).await.as_deref() == Some("window") {
                from = Some(answered);
            }
            if from.is_some_and(|from| answered >= from + 5) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let _ = stopper.send(true);
    };
    let (result, ()) = tokio::join!(run, stopping);
    let error = result.err().ok_or("the stopped build published")?;
    assert_eq!(code(&error), "stop_requested");
    Ok(())
}

/// The code of a store's refusal.
fn refused_with(error: &dsa_store::Error) -> String {
    error.report()["error"]
        .as_str()
        .unwrap_or_default()
        .to_owned()
}

/// Every other command against a database whose build stopped in its
/// window, through the functions the commands run (the test profile's k is
/// not the network's, so the binary would refuse the profile first):
/// `verify` and `snapshot` meet the closed gate, `restore` and `convert`
/// refuse a database that is not empty, `grant-reader` grants and the reader
/// still cannot read, `run` and `follow` refuse a database that does not
/// serve, `compare` cannot read side A, and `status` calls it unready. A
/// second build beside a running one is refused by the writer lock, and a
/// build into a database that serves as not empty. The stopped build then
/// continues to the conversion's digest.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated PostgreSQL server"]
async fn the_other_commands_refuse_a_database_being_built() -> Result {
    use dsa_service::{config, retry::WallTime, run, status, upkeep::IndexPolicy};
    use dsa_store::{Store, compare};
    let profile = profile(1);
    let edges = edges(&profile);
    let converted = Database::new().await?;
    let (source, conversion) = edge_conversion(&profile, &edges.g, &converted).await?;
    let files = tempfile::tempdir()?;
    let file = files.path().join("conversion.snapshot");
    Store::connect(converted.config.clone(), None)
        .await?
        .snapshot(&profile, &file)
        .await
        .map_err(harness::named)?;
    let rebuilt = Database::new().await?;
    empty(&rebuilt, &profile).await?;
    let fake = Fake::with_chain(edge_chain(&profile, &edges.g, EDGE_END), None).await;
    stopped_in_window(&profile, &fake, &rebuilt).await?;
    let mut outcomes: BTreeMap<&str, String> = BTreeMap::new();
    // The whole report of the refusals that wrap the gate's.
    let mut reports: BTreeMap<&str, serde_json::Value> = BTreeMap::new();
    let mut store = Store::connect(rebuilt.config.clone(), None).await?;
    let outcome = |result: std::result::Result<(), dsa_store::Error>| match result {
        Ok(()) => "ok".to_owned(),
        Err(error) => refused_with(&error),
    };
    if let Err(error) = store.verify(&profile).await {
        reports.insert("verify", error.report());
    }
    if let Err(error) = store
        .snapshot(&profile, &files.path().join("built.snapshot"))
        .await
    {
        reports.insert("snapshot", error.report());
    }
    outcomes.insert(
        "restore",
        outcome(store.restore(&profile, &file, None).await.map(|_| ())),
    );
    outcomes.insert(
        "grant-reader",
        outcome(
            store
                .grant_reader(&profile, &rebuilt.reader)
                .await
                .map(|_| ()),
        ),
    );
    drop(store);
    let reader_gate = common::connect(&rebuilt.reader_config())
        .await?
        .query_one("SELECT count(*) FROM block", &[])
        .await
        .err()
        .and_then(|error| error.code().map(|code| code.code().to_owned()));
    outcomes.insert("reader after grant-reader", reader_gate.unwrap_or_default());
    // `convert` itself, not the harness's helper, which empties its
    // destination first.
    let mut legacy =
        dsa_store::convert::LegacySource::connect(source.database.reader_config(), None).await?;
    let converting = Store::connect(rebuilt.config.clone(), None)
        .await?
        .convert(
            &profile,
            &mut legacy,
            &dsa_store::convert::ConvertOptions {
                horizon_block: Some(EDGE_HORIZON),
                retention_seconds: DAY,
                ..dsa_store::convert::ConvertOptions::default()
            },
        )
        .await;
    outcomes.insert(
        "convert",
        converting
            .err()
            .map_or("ok".to_owned(), |error| refused_with(&error)),
    );
    let followed = support::follow_to(
        &profile,
        &fake,
        &rebuilt.config,
        support::until(EDGE_TARGET),
    )
    .await;
    outcomes.insert(
        "follow",
        followed.err().map_or("ok".to_owned(), |error| code(&error)),
    );
    let open: run::Open = {
        let config = rebuilt.config.clone();
        Arc::new(move |_| {
            let config = config.clone();
            Box::pin(async move { Store::connect(config, None).await })
        })
    };
    let settings = config::Settings::check(config::Input {
        listen: "127.0.0.1:0".to_owned(),
        reader: rebuilt.reader.clone(),
        snapshot: None,
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
    let (_stopper, stop) = Stop::new();
    let service = run::run(run::Context {
        profile: profile.clone(),
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
    outcomes.insert(
        "run",
        tokio::time::timeout(Duration::from_secs(120), service)
            .await?
            .err()
            .map_or("ok".to_owned(), |error| code(&error)),
    );
    // Side B, the conversion, is open to its reader.
    Store::connect(converted.config.clone(), None)
        .await?
        .grant_reader(&profile, &converted.reader)
        .await
        .map_err(harness::named)?;
    let compared = compare::compare_databases(
        compare::Request {
            profile: profile.clone(),
            a: compare::SideA::Database {
                config: Box::new(rebuilt.reader_config()),
                root: None,
            },
            b: converted.reader_config(),
            b_root: None,
            positions: None,
            horizon_block: Some(EDGE_HORIZON),
            max_pairs: 50,
            statement_timeout_seconds: None,
            idle_transaction_timeout_seconds: None,
            a_verify_unbounded: false,
            candidate_address: None,
            max_differing_rows: 100,
        },
        |_| {},
    )
    .await;
    if let Err(error) = compared {
        reports.insert("compare", error.report());
    }
    let view = Store::connect(rebuilt.config.clone(), None)
        .await?
        .service_view(&profile, None)
        .await?;
    let document = serde_json::to_value(status::compose(
        Some(&view),
        None,
        &status::Settings::new(&profile.network),
        0,
    ))?;
    outcomes.insert(
        "status",
        format!(
            "{} {}",
            document["serving"].as_str().unwrap_or_default(),
            document["position"]["reason"].as_str().unwrap_or_default()
        ),
    );
    // A second build beside a running one: the first holds the writer
    // lock while its listing waits four seconds for its first answer, longer
    // than the second waits for the lock.
    let beside = Database::new().await?;
    empty(&beside, &profile).await?;
    let slowed = Arc::new(AtomicBool::new(false));
    let slow: Hook = Arc::new(move |path: &str, chain: &mut Chain, _| {
        (path.contains("/assets/policy/") && !slowed.swap(true, Ordering::SeqCst)).then(|| {
            Behavior {
                delay: Duration::from_secs(4),
                ..chain.answer(path)
            }
        })
    });
    let slow_fake = Fake::with_chain(edge_chain(&profile, &edges.g, EDGE_END), Some(slow)).await;
    let (_stopper, stop) = Stop::new();
    let first = build(
        &profile,
        &slow_fake,
        &beside,
        EDGE_HORIZON,
        EDGE_TARGET,
        stop,
    );
    let second = async {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let (_stopper, stop) = Stop::new();
        build(
            &profile,
            &slow_fake,
            &beside,
            EDGE_HORIZON,
            EDGE_TARGET,
            stop,
        )
        .await
    };
    let (first, second) = tokio::join!(first, second);
    outcomes.insert(
        "second rebuild beside a running one",
        second.err().map_or("ok".to_owned(), |error| code(&error)),
    );
    let first = first.map_err(|error| format!("the first build: {}", error.report()))?;
    same(
        &first.verified,
        &conversion,
        "the build beside which another ran",
    )?;
    beside.cleanup().await?;
    // A build into a database that serves.
    let (_stopper, stop) = Stop::new();
    outcomes.insert(
        "rebuild into a serving database",
        build(&profile, &fake, &converted, EDGE_HORIZON, EDGE_TARGET, stop)
            .await
            .err()
            .map_or("ok".to_owned(), |error| code(&error)),
    );
    println!(
        "{}",
        json!({"event":"other_commands","outcomes":outcomes,"reports":reports})
    );
    // `verify`, `snapshot` and `compare` (on side A) meet the gate's own
    // refusal.
    assert_eq!(reports["compare"]["side"], "a", "{}", reports["compare"]);
    for command in ["verify", "snapshot", "compare"] {
        let report = reports
            .get(command)
            .map(ToString::to_string)
            .unwrap_or_default();
        assert!(
            report.contains("55000") || report.contains("not_serving"),
            "{command}: {report}"
        );
    }
    let expected = BTreeMap::from([
        ("restore", "destination_not_empty"),
        ("convert", "destination_not_empty"),
        ("grant-reader", "ok"),
        ("reader after grant-reader", "55000"),
        ("follow", "database_not_serving"),
        ("run", "database_not_serving"),
        ("status", "unready rebuilding"),
        ("second rebuild beside a running one", "writer_busy"),
        ("rebuild into a serving database", "destination_not_empty"),
    ]);
    for (command, code) in expected {
        assert_eq!(outcomes[command], code, "{command}: {outcomes:?}");
    }
    // The stopped build continues.
    let (_stopper, stop) = Stop::new();
    let report = build(&profile, &fake, &rebuilt, EDGE_HORIZON, EDGE_TARGET, stop)
        .await
        .map_err(|error| format!("continued: {}", error.report()))?;
    same(
        &report.verified,
        &conversion,
        "continued after the commands",
    )?;
    rebuilt.cleanup().await?;
    converted.cleanup().await?;
    source.database.cleanup().await
}

/// A provider of another network ends a build at its first request,
/// before the database changes; the command logs it as `rebuild_failed`.
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
    // The command logs the build's failure as `rebuild_failed`, with the
    // code and its detail, and ends with exit status 1.
    let key = tempfile::NamedTempFile::new()?;
    std::fs::write(key.path(), dsa_testkit::server::KEY)?;
    let output = rebuilt
        .start_cli(
            "rebuild",
            &case.profile.network,
            &[
                "--provider-url",
                &fake.server.url,
                "--provider-key-file",
                key.path().to_str().ok_or("a key path")?,
            ],
        )?
        .wait_with_output()
        .await?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    let failed = stderr
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|line| {
            line["event"]
                .as_str()
                .is_some_and(|event| event.ends_with("_failed"))
        })
        .collect::<Vec<_>>();
    assert_eq!(failed.len(), 1, "{stderr}");
    assert_eq!(failed[0]["event"], "rebuild_failed", "{stderr}");
    assert_eq!(
        failed[0]["error"]["error"], "provider_network_mismatch",
        "{stderr}"
    );
    assert_eq!(
        failed[0]["error"]["detail"]["received_magic"], other,
        "{stderr}"
    );
    assert_eq!(fake.paths(), vec!["/api/v0/genesis".to_owned(); 2]);
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
    // An uninterrupted build: the phase each request was sent in.
    let whole = Database::new().await?;
    empty(&whole, &case.profile).await?;
    let (progress, view) = watch::channel(Progress::default());
    let sent_in = Arc::new(Mutex::new(Vec::<String>::new()));
    let hook: Hook = {
        let sent_in = sent_in.clone();
        Arc::new(move |_: &str, _: &mut Chain, _| {
            let phase = view.borrow().reason.clone();
            sent_in.lock().unwrap().push(if phase.is_empty() {
                "plan".to_owned()
            } else {
                phase
            });
            None
        })
    };
    let fake = Fake::with_chain(
        Chain::from_generated(&case.g, stake_prefix(&case.profile.network)),
        Some(hook),
    )
    .await;
    let (_stopper, stop) = Stop::new();
    build_observed(
        &case.profile,
        &fake,
        &whole,
        kill_options(&case, 0),
        stop,
        progress,
    )
    .await
    .map_err(|error| error.report().to_string())?;
    let sent_in = sent_in.lock().unwrap().clone();
    let total = sent_in.len();
    whole.cleanup().await?;
    // The phases in their order, with the first request of each.
    let mut phases: Vec<(String, usize, usize)> = Vec::new();
    for (index, phase) in sent_in.iter().enumerate() {
        match phases.last_mut() {
            Some((last, _, end)) if last == phase => *end = index,
            _ => phases.push((phase.clone(), index, index)),
        }
    }
    // A kill in the middle of every phase at the default batch size, one in
    // the middle of each phase that stores its answers in batches with a
    // batch of 2 (a continuation from a partly stored phase), and the
    // points DSA_REBUILD_KILL_POINTS spreads over the requests.
    let mut points: Vec<(usize, usize)> = phases
        .iter()
        .map(|(_, start, end)| (1 + start + (end - start) / 2, 0))
        .collect();
    for (phase, start, end) in &phases {
        if ["transactions", "addresses", "validity"].contains(&phase.as_str()) && end - start >= 3 {
            points.push((1 + start + (end - start) / 2, 2));
        }
    }
    let extra = setting("DSA_REBUILD_KILL_POINTS", 0) as usize;
    points.extend((1..=extra).map(|point| ((total * point / (extra + 1)).max(1), 0)));
    let point = |(kill_at, batch): (usize, usize)| {
        let (case, conversion, phases, sent_in) = (&case, &conversion, &phases, &sent_in);
        async move {
            let phase = sent_in[kill_at - 1].clone();
            let phase_start = phases
                .iter()
                .find(|(name, start, end)| {
                    *name == phase && (*start..=*end).contains(&(kill_at - 1))
                })
                .map_or(0, |(_, start, _)| *start);
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
                result = build_with(&case.profile, &fake, &rebuilt, kill_options(case, batch), stop) => Some(result),
                _ = killed.wait_for(|killed| *killed) => None,
            };
            if first.is_some() {
                return Err(format!("kill point {kill_at} of {total} came after the end").into());
            }
            let client = common::connect(&rebuilt.config).await?;
            let stored_phase: Option<String> = client
                .query_opt("SELECT phase FROM dsa_rebuild.progress WHERE id=1", &[])
                .await
                .ok()
                .flatten()
                .map(|row| row.get(0));
            assert!(
                !serving(&rebuilt).await?,
                "kill at {kill_at}: the database serves"
            );
            // What `status` prints of a build: the stored service state.
            let stored = client
                .query_opt(
                    "SELECT state,reason,detail FROM dsa.service_state WHERE id=1",
                    &[],
                )
                .await
                .ok()
                .flatten()
                .map(|row| {
                    json!({"state": row.get::<_, String>(0), "reason": row.get::<_, String>(1),
                    "detail": row.get::<_, serde_json::Value>(2)})
                });
            let (_stopper, stop) = Stop::new();
            let report = build_with(
                &case.profile,
                &fake,
                &rebuilt,
                kill_options(case, batch),
                stop,
            )
            .await
            .map_err(|error| format!("resume after {kill_at}: {}", error.report()))?;
            same(
                &report.verified,
                conversion,
                &format!("killed at request {kill_at}"),
            )?;
            assert!(serving(&rebuilt).await?);
            let answered = fake.paths().len();
            // A request is counted once: never more than the provider answered.
            assert!(
                report.calls <= answered as i64 + IN_FLIGHT,
                "kill at {kill_at}: {} requests counted, {answered} answered",
                report.calls
            );
            // A build that stopped before it wrote anything starts anew; any
            // other continues, and asks again at most what its phase had asked
            // before the kill, the requests in flight and the few a run starts
            // with.
            let asked_again = answered.saturating_sub(total);
            let bound = (kill_at - phase_start) + IN_FLIGHT as usize + RUN_START;
            let outcome = json!({"kill_at_request":kill_at,"phase":phase,"batch":batch,
            "stored_phase":stored_phase,"stored_state":stored,"resumed":report.resumed,
            "calls":report.calls,"answered":answered,"asked_again":asked_again,"bound":bound});
            println!("{}", json!({"event":"kill","outcome":outcome}));
            assert_eq!(report.resumed, stored_phase.is_some(), "{outcome}");
            assert!(asked_again <= bound, "{outcome}");
            rebuilt.cleanup().await?;
            Result::<serde_json::Value>::Ok(outcome)
        }
    };
    // Four points at once: each has its own database and provider.
    let outcomes = futures_util::stream::iter(points.clone())
        .map(point)
        .buffer_unordered(4)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    let covered: std::collections::BTreeSet<String> = outcomes
        .iter()
        .map(|outcome| outcome["phase"].as_str().unwrap_or_default().to_owned())
        .collect();
    println!(
        "{}",
        json!({"event":"kill_points","phases":phases.iter().map(|(p, s, e)| json!([p, s, e])).collect::<Vec<_>>(),
            "covered":covered,"points":outcomes.len()})
    );
    assert_eq!(
        covered.len(),
        phases
            .iter()
            .map(|(name, _, _)| name)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        "a phase without a kill"
    );
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
        // The requests recorded per day, spread over ten days with the same
        // total, as a build whose runs span ten days records them: the
        // client keeps the counts of eight days, the build's total all.
        common::connect(&rebuilt.config)
            .await?
            .batch_execute(
                "CREATE TEMP TABLE spread AS SELECT max(day) AS day,sum(requests)::bigint AS n FROM dsa.calls;
                 DELETE FROM dsa.calls;
                 INSERT INTO dsa.calls(day,requests)
                 SELECT s.day-g,CASE WHEN g=0 THEN s.n-9*(s.n/10) ELSE s.n/10 END
                 FROM spread s CROSS JOIN generate_series(0,9) g",
            )
            .await?;
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
    assert_eq!(outcomes.len(), points.len());
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
        let ids: BTreeMap<&String, Option<&i64>> = table
            .iter()
            .map(|(unit, _)| (unit, stored.get(unit)))
            .collect();
        compared
            .push(json!({"file": name, "fixed": table.len(), "differing": differing, "ids": ids}));
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

/// The table of fixed descriptor IDs of Mainnet and Preprod is the one read
/// from the published snapshot files (`fixtures/published-fixed-descriptors.json`,
/// recorded by the test above with DSA_RELEASE_FILES).
#[test]
fn the_fixed_descriptor_ids_are_those_recorded_from_the_published_files() {
    let recorded: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/published-fixed-descriptors.json"))
            .expect("the fixture");
    let files = recorded["files"].as_object().expect("the files");
    for network in ["mainnet", "preprod"] {
        let profile = Profile::load(network).expect("the profile");
        let table: BTreeMap<String, i64> = dsa_chain::profiles::fixed_descriptor_ids(&profile)
            .into_iter()
            .collect();
        let (name, ids) = files
            .iter()
            .find(|(name, _)| name.starts_with(&format!("{network}-")))
            .expect("a file of the network");
        let ids: BTreeMap<String, i64> =
            serde_json::from_value(ids.clone()).expect("the recorded IDs");
        assert_eq!(table, ids, "{name}");
    }
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

/// The command refuses a window longer than the retention, a horizon above
/// the target and a window given with a horizon before it connects
/// anywhere.
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
        (
            vec!["--horizon-block", "10", "--window-seconds", "86400"],
            "window_with_horizon",
            "--window-seconds",
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
