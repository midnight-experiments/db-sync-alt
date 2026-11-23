//! The follower loop against PostgreSQL 17 in DSA_TEST_DATABASE and the fake
//! provider of the testkit, on a simulated clock. DSA_FOLLOW_HTTP_CASES sets
//! the number of generated chains of the equivalence test (default 4) and
//! DSA_FOLLOW_HTTP_PARALLEL how many are followed at once (default 4).

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
use dsa_service::{block::Limits, follow::Settings};
use dsa_store::Store;
use dsa_testkit::generate;
use harness::{K, Random, Result, Source, chain, convert, derive, header, named, profile, same};
use serde_json::json;
use support::{DAY, Fake, follow_to, served};

fn setting(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

#[derive(Default, Debug, Clone)]
struct Totals {
    chains: u64,
    rounds: u64,
    updates: u64,
    forks: u64,
    advances: u64,
    requests: BTreeMap<String, u64>,
    same_block_spends: usize,
    quiet_blocks: usize,
    revealed_at_spend: usize,
    bridge_metadata: usize,
}

impl Totals {
    fn add(&mut self, other: &Totals) {
        self.chains += other.chains;
        self.rounds += other.rounds;
        self.updates += other.updates;
        self.forks += other.forks;
        self.advances += other.advances;
        for (family, count) in &other.requests {
            *self.requests.entry(family.clone()).or_default() += count;
        }
        self.same_block_spends += other.same_block_spends;
        self.quiet_blocks += other.quiet_blocks;
        self.revealed_at_spend += other.revealed_at_spend;
        self.bridge_metadata += other.bridge_metadata;
    }
}

/// One generated chain followed through the provider in rounds: each round
/// the provider serves the chain to a new height, sometimes with another
/// branch at the tip, some first-served times are moved back so that the
/// horizon advances, and a fresh run of the loop with random batch limits
/// and poll timing stops at a target. After every round the store equals a
/// conversion of the full history at the same horizon, target and tip.
async fn equivalence(
    seed: u64,
    source: &mut Source,
    followed: &Database,
    converted: &Database,
    totals: &mut Totals,
) -> Result {
    let profile = profile(seed);
    let mut random = Random::new(seed ^ 0x00f3_b0f3);
    let blocks = 50 + random.below(25) as u32;
    let first = 1;
    let g = chain(&profile, seed, first, blocks, seed.is_multiple_of(4)).known_from_start();
    let history = derive(&profile, &g).await?;
    let end = first + blocks - 1;
    let last_target = end - K;
    let t0 = first + 8 + random.below(9) as u32;
    let h0 = t0 - random.below(6) as u32;
    let headers = (first..=t0 + K)
        .map(|height| header(g.block(height).unwrap()))
        .collect::<Vec<_>>();
    source.start(&g, &history, t0, &headers).await?;
    convert(followed, &source.database, &profile, h0, DAY).await?;
    let fake = Fake::start(&g, &profile).await;
    let mut target = t0;
    let mut salt = seed << 8;
    while target < last_target {
        salt += 1;
        let next = (target + 1 + random.below(u64::from((last_target - target).min(20))) as u32)
            .min(last_target);
        // The provider's chain: canonical through the new target, then up
        // to the end of the chain, sometimes another branch at the top.
        let height = (next + K + random.below(u64::from(end - next - K + 1)) as u32).min(end);
        let mut blocks = served(&g, height);
        if random.chance(3) {
            let above = height - next;
            let depth = 1 + random.below(u64::from(above.min(K))) as usize;
            let keep = blocks.len() - depth;
            let other = generate::branch(&blocks[keep..], salt);
            blocks.truncate(keep);
            blocks.extend(other);
            totals.forks += 1;
        }
        fake.serve(blocks);
        // Time passes: the oldest served heights become deletable.
        if random.chance(3) {
            let client = common::connect(&followed.config).await?;
            let aged = target.saturating_sub(random.below(8) as u32);
            client
                .execute(
                    "UPDATE dsa.first_served SET first_served_at=first_served_at-interval '2 days' WHERE height<=$1",
                    &[&(aged as i32)],
                )
                .await?;
        }
        let before = Store::connect(followed.config.clone(), None)
            .await?
            .follow_state(&profile)
            .await
            .map_err(named)?
            .horizon
            .height;
        let settings = Settings {
            poll_interval: Duration::from_secs(1 + random.below(30)),
            prune_interval: Duration::from_secs(60),
            limits: Limits {
                blocks: 1 + random.below(8) as u32,
                cbor_bytes: 2_048 + random.below(32_768) as usize,
                rows: 5 + random.below(200) as usize,
            },
            max_in_flight: 1 + random.below(4) as usize,
            stop_at_target: Some(next),
            ..Settings::default()
        };
        let report = follow_to(&profile, &fake, &followed.config, settings)
            .await
            .map_err(|error| format!("seed {seed} round to {next}: {}", error.report()))?;
        assert_eq!(report.position.target.height, next, "seed {seed}");
        target = next;
        totals.rounds += 1;
        totals.updates += report.updates;
        for (family, count) in &report.requests {
            *totals.requests.entry(family.clone()).or_default() += count;
        }
        let mut store = Store::connect(followed.config.clone(), None).await?;
        let state = store.follow_state(&profile).await.map_err(named)?;
        totals.advances += u64::from(state.horizon.height > before);
        let above = store.headers_above(target).await.map_err(named)?;
        let headers = (source.through + 1..=target)
            .map(|height| header(g.block(height).unwrap()))
            .chain(above)
            .collect::<Vec<_>>();
        source.extend(&g, &history, target, &headers).await?;
        let conversion = convert(
            converted,
            &source.database,
            &profile,
            state.horizon.height,
            DAY,
        )
        .await?;
        let context = format!(
            "seed {seed} at H {} T {} P {}",
            state.horizon.height, state.target.height, state.tip.height
        );
        let verified = match store.verify(&profile).await {
            Ok(verified) => verified,
            Err(error) => {
                let rows = harness::diff(&followed.config, &converted.config).await?;
                return Err(format!("{context}: verify {}; rows {rows}", error.report()).into());
            }
        };
        if let Err(error) = same(&verified, &conversion, &context) {
            let rows = harness::diff(&followed.config, &converted.config).await?;
            return Err(format!("{error}; rows {rows}").into());
        }
    }
    totals.chains += 1;
    totals.same_block_spends += g.counts.same_block_spends;
    totals.quiet_blocks += g.counts.quiet_blocks;
    totals.revealed_at_spend += g.counts.revealed_at_spend;
    totals.bridge_metadata += g.counts.bridge_metadata;
    Ok(())
}

/// Test 1: following through the provider equals a conversion of the full
/// history at the same position, with random batch limits, poll timing,
/// branches at the tip, horizon advances and a restart every round.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated PostgreSQL 17"]
async fn following_through_the_provider_equals_a_conversion() -> Result {
    let cases = setting("DSA_FOLLOW_HTTP_CASES", 4);
    let parallel = setting("DSA_FOLLOW_HTTP_PARALLEL", 4).clamp(1, 16);
    let start = setting("DSA_FOLLOW_HTTP_SEED_START", 0);
    let began = Instant::now();
    let workers = (0..parallel).map(|worker| async move {
        let mut totals = Totals::default();
        let mut source = Source::new().await?;
        let followed = Database::new().await?;
        let converted = Database::new().await?;
        let mut seed = start + worker;
        while seed < start + cases {
            equivalence(seed, &mut source, &followed, &converted, &mut totals).await?;
            if (totals.chains * parallel).is_multiple_of(20) {
                println!(
                    "{}",
                    json!({"worker":worker,"chains":totals.chains,"seed":seed,"seconds":began.elapsed().as_secs_f64()})
                );
            }
            seed += parallel;
        }
        source.database.cleanup().await?;
        followed.cleanup().await?;
        converted.cleanup().await?;
        Result::<Totals>::Ok(totals)
    });
    let mut totals = Totals::default();
    for result in futures_util::future::join_all(workers).await {
        totals.add(&result?);
    }
    println!(
        "{}",
        json!({"event":"follow_http_equivalence","cases":cases,"chains":totals.chains,"rounds":totals.rounds,
            "updates":totals.updates,"branches_at_the_tip":totals.forks,"horizon_advances":totals.advances,
            "requests":totals.requests,"same_block_spends":totals.same_block_spends,"quiet_blocks":totals.quiet_blocks,
            "datums_revealed_at_spend":totals.revealed_at_spend,"bridge_metadata":totals.bridge_metadata,
            "seconds":began.elapsed().as_secs_f64()})
    );
    assert_eq!(totals.chains, cases);
    if cases >= 4 {
        assert!(totals.forks > 0 && totals.advances > 0 && totals.same_block_spends > 0);
    }
    Ok(())
}
