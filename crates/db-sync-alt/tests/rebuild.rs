//! The build from the provider alone against PostgreSQL in DSA_TEST_DATABASE
//! and the fake provider of the testkit: a generated chain converted from
//! its full history at horizon H and target T, and the same chain built from
//! the provider's listings at the same H and T, give the same content and
//! state digests, supply and position. DSA_REBUILD_CASES sets the number of
//! generated chains of the property test (default 8) and
//! DSA_REBUILD_PARALLEL how many run at once (default 4).

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
    time::{Duration, Instant},
};

use common::Database;
use dsa_chain::{block::BlockOutput, profiles::Profile};
use dsa_service::{
    follow::{self, Progress, Settings},
    rebuild::{self, Options, Report},
    retry::Stop,
};
use dsa_testkit::generate::Generated;
use harness::{K, Random, Result, Source, chain, convert, derive, header, profile, reset, same};
use serde_json::json;
use support::{DAY, Fake, connect};
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
    carried_spent: u64,
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
