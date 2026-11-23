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
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use common::Database;
use dsa_service::{
    block::Limits,
    follow::{self, Progress, Settings},
    retry::Stop,
};
use dsa_store::Store;
use dsa_testkit::{
    chain::{self as scripted, Block, Chain},
    generate,
    server::Behavior,
    tx::{self, Built, Output, Tx},
};
use harness::{K, Random, Result, Source, chain, convert, derive, header, named, profile, same};
use serde_json::{Value, json};
use support::{DAY, Fake, Hook, Start, block_rows, context, follow_to, served, until};
use tokio::sync::watch;
use tokio_postgres::error::SqlState;

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

/// Waits (in real time, at most two minutes) until `done` holds for the
/// published progress.
async fn progress_until(view: &mut watch::Receiver<Progress>, done: impl Fn(&Progress) -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    while !done(&view.borrow()) {
        if tokio::time::timeout_at(deadline, view.changed())
            .await
            .is_err()
        {
            panic!("progress never reached the condition: {:?}", *view.borrow());
        }
    }
}

/// Waits (in real time, at most two minutes) until `done` holds.
async fn until_true(done: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(120);
    while !done() {
        assert!(Instant::now() < deadline, "condition never reached");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Requests for a block by height (`/blocks/{height}`).
fn height_requests(fake: &Fake) -> usize {
    fake.paths()
        .iter()
        .filter(|path| {
            path.strip_prefix("/api/v0/blocks/")
                .is_some_and(|rest| rest.bytes().all(|byte| byte.is_ascii_digit()))
        })
        .count()
}

/// A provider that answers another hash for the block at `height`: once, or
/// every time.
fn contradiction(height: u32, once: bool) -> Hook {
    let path = format!("/api/v0/blocks/{height}");
    let fired = AtomicBool::new(false);
    Arc::new(move |requested, chain: &mut Chain, _| {
        if requested != path || (once && fired.swap(true, Ordering::SeqCst)) {
            return None;
        }
        let mut header = chain.at(height)?.header();
        header["hash"] = json!("ab".repeat(32));
        Some(Behavior::json(header))
    })
}

fn times_of(fake: &Fake, exact: &str) -> Vec<u64> {
    fake.log
        .lock()
        .unwrap()
        .iter()
        .filter(|(path, _)| path == exact)
        .map(|(_, at)| *at)
        .collect()
}

/// Test 2: forks of depth 1, 10 and k − 1 replace only the headers above
/// the fork point; a target contradicted once is read again a minute later
/// and following goes on; contradicted twice, the gate closes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated PostgreSQL 17"]
async fn tip_forks_replace_only_headers_and_a_contradicted_target_closes_the_gate() -> Result {
    let mut start = Start::new(11, 80, 20).await?;
    let fake = Fake::start(&start.g, &start.profile).await;
    let mut target = start.target;
    for depth in [1, 10, K - 1] {
        let tip = target + K;
        let mut blocks = served(&start.g, tip + 1);
        let keep = blocks.len() - (depth as usize + 1);
        let other = generate::branch(&blocks[keep..], u64::from(depth));
        blocks.truncate(keep);
        blocks.extend(other);
        let provider: BTreeMap<i64, String> = blocks
            .iter()
            .map(|block| (i64::from(block.height), block.hash.clone()))
            .collect();
        fake.serve(blocks);
        let before = block_rows(&start.database.config).await?;
        let searches = height_requests(&fake);
        let report = follow_to(
            &start.profile,
            &fake,
            &start.database.config,
            until(target + 1),
        )
        .await
        .map_err(|error| error.report().to_string())?;
        assert_eq!(report.position.target.height, target + 1);
        let after = block_rows(&start.database.config).await?;
        let fork_point = i64::from(tip - depth);
        let below = |rows: &[(i64, String)]| {
            rows.iter()
                .filter(|(height, _)| *height <= fork_point)
                .cloned()
                .collect::<Vec<_>>()
        };
        assert_eq!(
            below(&before),
            below(&after),
            "depth {depth}: rows at or below the fork point"
        );
        let replaced = after
            .iter()
            .filter(|(height, _)| *height > fork_point)
            .collect::<Vec<_>>();
        assert_eq!(replaced.len() as i64, i64::from(tip + 1) - fork_point);
        for (height, hash) in replaced {
            assert_eq!(
                provider.get(height),
                Some(hash),
                "depth {depth}: header {height}"
            );
        }
        // The target is checked, then a binary search over the k stored headers.
        let used = height_requests(&fake) - searches;
        assert!(
            used <= 1 + (f64::from(K + 1)).log2().ceil() as usize,
            "depth {depth}: {used} requests by height"
        );
        target += 1;
        start.check(&format!("fork of depth {depth}")).await?;
    }

    // Contradicted once, then the stored hash: following goes on.
    let once = Fake::with_chain(
        Chain::from_generated(
            &start.g,
            dsa_service::epoch::stake_prefix(&start.profile.network),
        ),
        Some(contradiction(target, true)),
    )
    .await;
    once.serve(served(&start.g, target + K + 1));
    follow_to(
        &start.profile,
        &once,
        &start.database.config,
        until(target + 1),
    )
    .await
    .map_err(|error| error.report().to_string())?;
    let times = times_of(&once, &format!("/api/v0/blocks/{target}"));
    assert_eq!(times.len(), 2, "{times:?}");
    assert!(times[1] - times[0] >= 60, "{times:?}");
    target += 1;
    start.check("a single contradiction").await?;

    // Contradicted twice, a minute apart: the gate closes.
    let twice = Fake::with_chain(
        Chain::from_generated(
            &start.g,
            dsa_service::epoch::stake_prefix(&start.profile.network),
        ),
        Some(contradiction(target, false)),
    )
    .await;
    let mut blocks = served(&start.g, target + K + 1);
    let keep = blocks.len() - 2;
    let other = generate::branch(&blocks[keep..], 99);
    blocks.truncate(keep);
    blocks.extend(other);
    twice.serve(blocks);
    let error = follow_to(
        &start.profile,
        &twice,
        &start.database.config,
        until(target + 1),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(
            error,
            follow::Error::Check {
                code: "canonical_conflict",
                ..
            }
        ),
        "{}",
        error.report()
    );
    let times = times_of(&twice, &format!("/api/v0/blocks/{target}"));
    assert_eq!(times.len(), 2, "{times:?}");
    assert!(times[1] - times[0] >= 60, "{times:?}");
    let state = start
        .store()
        .await?
        .follow_state(&start.profile)
        .await
        .map_err(named)?;
    assert!(!state.serving);
    assert_eq!(state.reason, "canonical_conflict");
    let client = common::connect(&start.database.config).await?;
    for view in dsa_store::NODE_RELATIONS {
        let refused = client
            .query(&format!("SELECT 1 FROM public.{view} LIMIT 1"), &[])
            .await
            .unwrap_err();
        assert_eq!(
            refused.code(),
            Some(&SqlState::from_code("55000")),
            "{view}"
        );
    }
    let service: (String, String) = client
        .query_one("SELECT state,reason FROM dsa.service_state", &[])
        .await
        .map(|row| (row.get(0), row.get(1)))?;
    assert_eq!(
        service,
        ("stopped".to_owned(), "canonical_conflict".to_owned())
    );
    start.cleanup().await
}

/// Test 3: answers that contradict themselves or the chain and every fault
/// of the fake provider write nothing; the loop retries and continues. A
/// spent allowance is waited out until the next UTC day without an exit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated PostgreSQL 17"]
async fn faulty_answers_write_nothing_and_are_retried() -> Result {
    let mut start = Start::new(13, 70, 15).await?;
    let fake = Fake::start(&start.g, &start.profile).await;
    let stop_at = start.target + 30;
    let busy = (start.target + 1..=stop_at)
        .filter_map(|height| start.g.block(height))
        .filter(|block| !block.transactions.is_empty())
        .cloned()
        .collect::<Vec<_>>();
    assert!(busy.len() >= 8, "{} blocks with transactions", busy.len());
    let page = |block: &Block| format!("/blocks/{}/txs/cbor", block.hash);
    let tip = start.g.block(start.target + K).unwrap().clone();
    let next = format!("/blocks/{}/next", tip.hash);
    {
        let mut chain = fake.chain.lock().unwrap();
        // A transaction whose hash is not its body's.
        let mut wrong = busy[0].page(1);
        wrong[0]["tx_hash"] = json!("cd".repeat(32));
        chain.fault(&page(&busy[0]), Behavior::json(wrong), 1);
        // One transaction fewer than the header states.
        let mut short = busy[1].page(1);
        short.as_array_mut().unwrap().pop();
        chain.fault(&page(&busy[1]), Behavior::json(short), 1);
        // A body cut in half.
        let whole = Behavior::json(busy[2].page(1));
        let half = whole.body.len() / 2;
        chain.fault(&page(&busy[2]), whole.truncated(half), 1);
        chain.fault(&page(&busy[3]), Behavior::status(500), 1);
        chain.fault(&page(&busy[4]), Behavior::dropped(), 1);
        chain.fault(&page(&busy[5]), Behavior::status(429).retry_after("30"), 1);
        chain.fault(&page(&busy[6]), Behavior::status(418), 1);
        chain.fault(
            &page(&busy[7]),
            Behavior::json(busy[7].page(1)).delayed(300),
            1,
        );
        // A header page whose second row does not follow the first.
        let mut rows = (start.target + K + 1..=start.target + K + 100)
            .filter_map(|height| start.g.block(height))
            .map(Block::header)
            .collect::<Vec<_>>();
        rows[1]["previous_block"] = json!("ef".repeat(32));
        chain.fault(&next, Behavior::json(Value::Array(rows)), 1);
        // The key's allowance is spent at the first request.
        chain.fault("/blocks/latest", Behavior::status(402), 1);
    }
    // One block per update, so that each fault is met on its own.
    let settings = Settings {
        limits: Limits {
            blocks: 1,
            ..Limits::default()
        },
        ..until(stop_at)
    };
    let report = follow_to(&start.profile, &fake, &start.database.config, settings)
        .await
        .map_err(|error| error.report().to_string())?;
    assert_eq!(report.position.target.height, stop_at);
    for block in &busy[..7] {
        assert!(
            fake.requests(&page(block)) >= 2,
            "block {} retried",
            block.height
        );
    }
    assert_eq!(fake.requests(&page(&busy[7])), 1, "a slow answer is taken");
    assert!(fake.requests(&next) >= 2, "the header page retried");
    // The spent allowance: nothing is sent before 00:02 UTC of the next day.
    let log = fake.log.lock().unwrap().clone();
    let refused = log
        .iter()
        .position(|(path, _)| path.contains("/blocks/latest"))
        .unwrap();
    let resume = support::START - support::START % 86_400 + 86_400 + 120;
    assert!(
        log[refused + 1..].iter().all(|(_, at)| *at >= resume),
        "{:?}",
        &log[refused..refused + 2]
    );
    start.check("after the faults").await?;
    start.cleanup().await
}

/// Test 4: at an epoch change the parameters are stored before the epoch's
/// first block is served; while the provider does not have them the batch
/// ends before that block. The stake arrives later, and a failing stake read
/// never delays an update.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated PostgreSQL 17"]
async fn epoch_parameters_come_first_and_stake_never_delays_an_update() -> Result {
    let mut start = Start::new(17, 90, 15).await?;
    let g = start.g.known_from_start();
    let stop_at = 90 - K;
    let first = (start.target + 5..=stop_at)
        .find(|height| g.block(*height).unwrap().epoch != g.block(height - 1).unwrap().epoch)
        .expect("an epoch boundary");
    let epoch = g.block(first).unwrap().epoch;
    let fake = Fake::start(&g, &start.profile).await;
    let parameters = fake
        .chain
        .lock()
        .unwrap()
        .parameters
        .remove(&epoch)
        .unwrap();
    fake.serve(served(&g, first + K + 4));
    {
        let mut chain = fake.chain.lock().unwrap();
        let epochs = chain.pools.stake.keys().copied().collect::<Vec<_>>();
        for each in epochs {
            chain.pools.reported_total.insert(each, 1);
        }
    }
    let (_stopper, stop) = Stop::new();
    let (progress, mut view) = watch::channel(Progress::default());
    let run = follow::follow(context(
        &start.profile,
        &fake,
        &start.database.config,
        until(stop_at),
        stop,
        progress,
    ));
    let parameters_path = format!("/epochs/{epoch}/parameters");
    let drive = async {
        progress_until(&mut view, |progress| {
            progress.reason == "epoch_parameters_pending"
        })
        .await;
        assert_eq!(view.borrow().target, Some(first - 1));
        let asked = fake.requests(&parameters_path);
        until_true(|| fake.requests(&parameters_path) >= asked + 2).await;
        assert_eq!(
            view.borrow().target,
            Some(first - 1),
            "no block of the epoch before its parameters"
        );
        fake.chain
            .lock()
            .unwrap()
            .parameters
            .insert(epoch, parameters);
        // At the tip the stake read starts and fails; following goes on.
        until_true(|| fake.requests("/pools") > 0).await;
        let at_failure = view.borrow().target.unwrap();
        fake.serve(served(&g, 90));
        progress_until(&mut view, |progress| progress.target == Some(stop_at)).await;
        fake.chain.lock().unwrap().pools.reported_total.clear();
        at_failure
    };
    let (report, at_failure) = tokio::join!(run, drive);
    let report = report.map_err(|error| error.report().to_string())?;
    assert_eq!(report.position.target.height, stop_at);
    assert!(
        at_failure < stop_at,
        "updates went on after the stake read failed"
    );
    let client = common::connect(&start.database.config).await?;
    let stake_epochs: Vec<i32> = client
        .query(
            "SELECT DISTINCT epoch_no FROM serve.epoch_stake ORDER BY 1",
            &[],
        )
        .await?
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(
        stake_epochs.last().copied(),
        Some(g.block(stop_at).unwrap().epoch as i32)
    );
    start.check("epoch change").await?;
    start.cleanup().await
}

/// Test 6: at the tip each poll costs one request and each block with
/// transactions one page request. DSA_FOLLOW_TIP_BLOCKS sets the number of
/// followed blocks (default 200).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated PostgreSQL 17"]
async fn at_the_tip_one_request_per_poll_and_one_page_per_block() -> Result {
    let blocks = setting("DSA_FOLLOW_TIP_BLOCKS", 200) as u32;
    let profile = profile(19);
    let mut g = chain(&profile, 19, 1, blocks + 40, false).known_from_start();
    g.lengthen_epochs(100);
    let mut start = Start::from_chain(profile, g, 20).await?;
    let stop_at = start.target + blocks;
    // One new block appears with every poll.
    let canonical = start.g.blocks.clone();
    let last = stop_at + K;
    let hook: Hook = Arc::new(move |path, chain: &mut Chain, _| {
        if path.contains("/next") {
            let height = chain.tip().map_or(0, |tip| tip.height) + 1;
            if height <= last
                && let Some(block) = canonical.iter().find(|block| block.height == height)
            {
                chain.blocks_mut().push(block.clone());
            }
        }
        None
    });
    let fake = Fake::with_chain(
        Chain::from_generated(
            &start.g,
            dsa_service::epoch::stake_prefix(&start.profile.network),
        ),
        Some(hook),
    )
    .await;
    fake.serve(served(&start.g, start.target + K));
    let began = Instant::now();
    let report = follow_to(
        &start.profile,
        &fake,
        &start.database.config,
        until(stop_at),
    )
    .await
    .map_err(|error| error.report().to_string())?;
    let busy = (start.target + 1..=stop_at)
        .filter(|height| !start.g.block(*height).unwrap().transactions.is_empty())
        .count();
    let polls = fake.requests("/next");
    let pages = fake.requests("/txs/cbor");
    println!(
        "{}",
        json!({"event":"follow_at_the_tip","blocks":blocks,"blocks_with_transactions":busy,"polls":polls,
            "page_requests":pages,"requests":report.requests,"all_requests":fake.paths().len(),
            "updates":report.updates,"seconds":began.elapsed().as_secs_f64()})
    );
    assert_eq!(polls, blocks as usize, "one poll per block");
    assert_eq!(pages, busy, "one page request per block with transactions");
    assert_eq!(report.updates, u64::from(blocks));
    start.check("at the tip").await?;
    start.cleanup().await
}

/// Test 7: a datum already held is not asked for again, also across a
/// restart; a block of 201 transactions is read in three pages; an output the
/// decoder cannot classify is resolved through the provider's outputs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated PostgreSQL 17"]
async fn held_datums_three_pages_and_unclassified_outputs() -> Result {
    let profile = profile(23);
    let g = chain(&profile, 23, 1, 30, false).known_from_start();
    let start = Start::from_chain(profile, g, 10).await?;
    let mapping = start.profile.addresses.mapping.clone();
    let at_mapping = dsa_chain::transaction::address_bytes(&mapping).unwrap();
    let datum = tx::int_datum(424_242);
    let datum_hash = generate::datum_hash(&datum);
    let first = Tx::new()
        .input(&tx::hash(b"first"), 0)
        .output(Output::new(at_mapping.clone(), 2_000_000).datum_hash(&datum_hash))
        .fee(170_000)
        .build();
    let mut many: Vec<Built> = (0..200).map(|n| scripted::filler(9_000_000 + n)).collect();
    let second = Tx::new()
        .input(&tx::hash(b"second"), 0)
        .output(Output::new(at_mapping.clone(), 3_000_000).datum_hash(&datum_hash))
        .fee(170_000)
        .build();
    many.push(second.clone());
    // Header type 9 is not an address form the decoder knows.
    let mut unknown = vec![0x90_u8];
    unknown.extend([5_u8; 28]);
    let odd = Tx::new()
        .input(&tx::hash(b"odd"), 0)
        .output(Output::new(unknown, 4_000_000))
        .fee(170_000)
        .build();
    let mut blocks = start.g.blocks.clone();
    let x1 = Block::after(blocks.last().unwrap(), vec![first.clone()]);
    let x2 = Block::after(&x1, many);
    let x3 = Block::after(&x2, vec![odd.clone()]);
    blocks.extend([x1.clone(), x2.clone(), x3.clone()]);
    for _ in 0..K {
        let next = Block::after(blocks.last().unwrap(), Vec::new());
        blocks.push(next);
    }
    let fake = Fake::start(&start.g, &start.profile).await;
    {
        let mut chain = fake.chain.lock().unwrap();
        chain.datums.insert(datum_hash.clone(), datum.clone());
        chain.outputs.insert(
            odd.hash.clone(),
            json!({"hash": odd.hash, "inputs": [], "outputs": [{"address": mapping, "amount":
                [{"unit": "lovelace", "quantity": "4000000"}], "output_index": 0, "data_hash": null,
                "inline_datum": null, "collateral": false, "reference_script_hash": null}]}),
        );
    }
    fake.serve(blocks);
    let datum_path = format!("/scripts/datum/{datum_hash}/cbor");
    follow_to(
        &start.profile,
        &fake,
        &start.database.config,
        until(x1.height),
    )
    .await
    .map_err(|error| error.report().to_string())?;
    assert_eq!(fake.requests(&datum_path), 1);
    // A new run: the datum is found in the store.
    follow_to(
        &start.profile,
        &fake,
        &start.database.config,
        until(x3.height),
    )
    .await
    .map_err(|error| error.report().to_string())?;
    assert_eq!(
        fake.requests(&datum_path),
        1,
        "a held datum is not asked for again"
    );
    assert_eq!(fake.requests(&format!("/blocks/{}/txs/cbor", x2.hash)), 3);
    assert_eq!(fake.requests(&format!("/txs/{}/utxos", odd.hash)), 1);
    let client = common::connect(&start.database.config).await?;
    let outputs: Vec<(i64, i32, String, Option<String>)> = client
        .query(
            "SELECT t.block_id::bigint,t.block_index::integer,o.address,encode(o.data_hash,'hex')
             FROM serve.tx t JOIN serve.tx_out o ON o.tx_id=t.id WHERE t.hash=ANY($1) ORDER BY 1",
            &[&[&first.hash, &second.hash, &odd.hash]
                .iter()
                .map(|hash| hex::decode(hash).unwrap())
                .collect::<Vec<_>>()],
        )
        .await?
        .iter()
        .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3)))
        .collect();
    assert_eq!(
        outputs,
        vec![
            (
                i64::from(x1.height),
                0,
                mapping.clone(),
                Some(datum_hash.clone())
            ),
            (
                i64::from(x2.height),
                200,
                mapping.clone(),
                Some(datum_hash.clone())
            ),
            (i64::from(x3.height), 0, mapping.clone(), None),
        ]
    );
    let datums: i64 = client
        .query_one(
            "SELECT count(*) FROM serve.datum WHERE hash=$1",
            &[&hex::decode(&datum_hash).unwrap()],
        )
        .await?
        .get(0);
    assert_eq!(datums, 1);
    start
        .store()
        .await?
        .verify(&start.profile)
        .await
        .map_err(named)?;
    start.cleanup().await
}
