//! The follower's writes against PostgreSQL 17 in DSA_TEST_DATABASE. Updates
//! applied to a converted store equal a conversion of the full history at the
//! same horizon, target and tip. DSA_FOLLOW_CASES sets the number of
//! generated chains (default 64).

#[path = "common/legacy.rs"]
#[allow(dead_code)]
mod common;
#[path = "follow/harness.rs"]
#[allow(dead_code)]
mod harness;

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Instant,
};

use common::{Database, connect};
use dsa_chain::{
    block::{Outcome, Selection, process_block},
    keep::TABLES,
    transaction::decode_all,
};
use dsa_store::{
    Store,
    follow::{EpochParamRow, HeaderRef, Update},
};
use harness::{
    Follower, K, Plan, Random, Result, Setup, Source, WEEK, advance, build, chain, convert, derive,
    diff, dump, epoch_params, header, logical, named, plan, profile, replace, same,
};
use serde_json::{Value, json};

fn cases() -> u64 {
    std::env::var("DSA_FOLLOW_CASES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(64)
}

#[derive(Default)]
struct Totals {
    chains: u64,
    checkpoints: u64,
    updates: u64,
    final_blocks: u64,
    pruned_chains: u64,
    replaced_chains: u64,
    restarted_chains: u64,
    quiet_windows: u64,
    spent_governance_chains: u64,
    largest_statement_count: usize,
}

/// Test 1: following equals a conversion of the full history.
#[tokio::test]
#[ignore = "requires isolated PostgreSQL 17"]
async fn following_equals_a_conversion_of_the_full_history() -> Result {
    let cases = cases();
    let start: u64 = std::env::var("DSA_FOLLOW_SEED_START")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    let mut source = Source::new().await?;
    let pruned = Database::new().await?;
    let unpruned = Database::new().await?;
    let converted = Database::new().await?;
    let began = Instant::now();
    let mut totals = Totals::default();
    for seed in start..start + cases {
        let profile = profile(seed);
        let mut random = Random::new(seed);
        let blocks = 50 + random.below(25) as u32;
        let first = 1;
        let g = chain(&profile, seed, first, blocks, seed.is_multiple_of(4));
        let history = derive(&profile, &g).await?;
        let last_target = first + blocks - 1 - K;
        let t0 = first + 8 + random.below(9) as u32;
        let h0 = t0 - random.below(6) as u32;
        let headers = (first..=t0 + K)
            .map(|height| header(g.block(height).unwrap()))
            .collect::<Vec<_>>();
        source.start(&g, &history, t0, &headers).await?;
        let start_a = convert(&pruned, &source.database, &profile, h0, WEEK).await?;
        let start_b = convert(&unpruned, &source.database, &profile, h0, WEEK).await?;
        same(&start_a, &start_b, "start")?;
        let staked: BTreeSet<u32> = (first..=t0)
            .map(|height| g.block(height).unwrap().epoch)
            .collect();
        let mut a = Follower::open(&pruned.config, &profile, true, staked.clone()).await?;
        let mut b = Follower::open(&unpruned.config, &profile, false, staked).await?;
        let (mut pruning, mut replacing, mut restarting, mut spent_governance) =
            (false, false, false, false);
        let mut salt = 0;
        while a.target.height < last_target {
            salt += 1;
            let step = plan(&mut random, &g, &a, last_target, salt);
            replacing |= step.fork_point < a.tip.height;
            let report = a
                .update(&profile, &g, &step)
                .await
                .map_err(|error| format!("seed {seed}: {error}"))?;
            b.update(&profile, &g, &step)
                .await
                .map_err(|error| format!("seed {seed} unpruned: {error}"))?;
            totals.updates += 1;
            totals.final_blocks += u64::from(step.blocks);
            totals.largest_statement_count = totals.largest_statement_count.max(report.statements);
            pruning |= step.prune_to.is_some();
            if random.chance(6) {
                a.restart(&profile).await?;
                restarting = true;
            }
            if a.target.height == last_target || random.chance(6) {
                totals.checkpoints += 1;
                let headers = a.headers_from(&g, source.through);
                source
                    .extend(&g, &history, a.target.height, &headers)
                    .await?;
                let conversion =
                    convert(&converted, &source.database, &profile, a.horizon, WEEK).await?;
                let followed = match a.store.verify(&profile).await {
                    Ok(followed) => followed,
                    Err(error) => {
                        let differences = diff(&pruned.config, &converted.config).await?;
                        return Err(format!(
                            "seed {seed} at H {} T {} P {}: verify {}; rows: {differences}",
                            a.horizon,
                            a.target.height,
                            a.tip.height,
                            error.report()
                        )
                        .into());
                    }
                };
                if let Err(error) = same(
                    &followed,
                    &conversion,
                    &format!(
                        "seed {seed} at H {} T {} P {}",
                        a.horizon, a.target.height, a.tip.height
                    ),
                ) {
                    let differences = diff(&pruned.config, &converted.config).await?;
                    return Err(format!("{error}; rows: {differences}").into());
                }
                let client = connect(&pruned.config).await?;
                let quiet: bool = client
                    .query_one(
                        "SELECT NOT EXISTS(SELECT 1 FROM serve.tx_in WHERE id BETWEEN $1::bigint*16777216 AND $2::bigint*16777216+16777215)",
                        &[&i64::from(a.horizon), &i64::from(a.target.height)],
                    )
                    .await?
                    .get(0);
                totals.quiet_windows += u64::from(quiet);
                let bodies = vec![
                    profile.addresses["council"].clone(),
                    profile.addresses["technical_committee"].clone(),
                ];
                spent_governance |= client
                    .query_one(
                        "SELECT EXISTS(SELECT 1 FROM dsa.consumed_before_horizon c JOIN serve.tx_out o ON o.id=c.tx_out_id WHERE o.address=ANY($1))",
                        &[&bodies],
                    )
                    .await?
                    .get::<_, bool>(0);
            }
        }
        // The same chain followed without pruning, then advanced once.
        if a.horizon > b.horizon {
            b.store
                .advance_horizon(&profile, a.horizon)
                .await
                .map_err(named)?;
        }
        let once = b.store.verify(&profile).await.map_err(named)?;
        let stepwise = a.store.verify(&profile).await.map_err(named)?;
        same(&stepwise, &once, &format!("seed {seed}: advanced once"))?;
        // The first observation of each shape is the first one handed over.
        let client = connect(&pruned.config).await?;
        let stored: BTreeMap<String, u32> = client
            .query(
                "SELECT shape,block_height::bigint FROM dsa.first_observation",
                &[],
            )
            .await?
            .iter()
            .map(|row| (row.get(0), row.get::<_, i64>(1) as u32))
            .collect();
        assert_eq!(stored, a.seen, "seed {seed}: first observations");
        totals.chains += 1;
        totals.pruned_chains += u64::from(pruning);
        totals.replaced_chains += u64::from(replacing);
        totals.restarted_chains += u64::from(restarting);
        totals.spent_governance_chains += u64::from(spent_governance);
        if totals.chains.is_multiple_of(50) {
            println!(
                "{}",
                json!({"chains":totals.chains,"last_seed":seed,"seconds":began.elapsed().as_secs_f64()})
            );
        }
    }
    println!(
        "{}",
        json!({"event":"follow_equivalence","cases":cases,"chains_checked":totals.chains,"checkpoints":totals.checkpoints,
            "updates":totals.updates,"final_blocks":totals.final_blocks,"chains_with_pruning":totals.pruned_chains,
            "chains_with_header_replacement":totals.replaced_chains,"chains_with_restarts":totals.restarted_chains,
            "quiet_windows_at_checkpoints":totals.quiet_windows,"chains_keeping_a_spent_governance_record":totals.spent_governance_chains,
            "largest_statement_count":totals.largest_statement_count,"seconds":began.elapsed().as_secs_f64()})
    );
    assert_eq!(totals.chains, cases);
    if cases >= 64 {
        assert!(
            totals.pruned_chains > 0 && totals.replaced_chains > 0 && totals.restarted_chains > 0
        );
    }
    if cases >= 500 {
        assert!(totals.quiet_windows > 0, "no quiet window");
        assert!(
            totals.spent_governance_chains > 0,
            "no spent governance record kept"
        );
    }
    source.database.cleanup().await?;
    for database in [pruned, unpruned, converted] {
        database.cleanup().await?;
    }
    Ok(())
}

/// Advances one block at a time until the next `blocks` blocks start a new
/// epoch and hold rows, so that one update exercises every statement.
async fn rich_position(setup: &mut Setup, blocks: u32) -> Result {
    loop {
        let f = &setup.follower;
        let next = f.target.height + 1;
        let last = f.target.height + blocks;
        let epochs = (next..=last)
            .map(|height| setup.g.block(height).unwrap().epoch)
            .collect::<BTreeSet<_>>();
        let busy = (next..=last)
            .filter(|height| !setup.g.block(*height).unwrap().transactions.is_empty())
            .count();
        if epochs.len() > 1 && busy as u32 >= blocks - 1 {
            return Ok(());
        }
        if last >= setup.last_target {
            return Err("no epoch boundary with busy blocks".into());
        }
        let plan = advance(&setup.g, f, 1, None);
        setup.step(&plan).await?;
    }
}

/// The parts of an update that depend on the store: final blocks, supply
/// change, observations and epoch parameters.
async fn parts(
    f: &Follower,
    setup: &Setup,
    plan: &Plan,
) -> Result<(harness::Built, Vec<EpochParamRow>)> {
    let heights = f.target.height + 1..=f.target.height + plan.blocks;
    let built = build(&f.store, &setup.profile, &setup.g, heights.clone()).await?;
    Ok((built, epoch_params(&setup.g, heights)))
}

/// Replaces the top three headers and makes four blocks final, with a
/// horizon advance.
fn rich_plan(setup: &Setup) -> Plan {
    let f = &setup.follower;
    let fork_point = f.tip.height - 3;
    Plan {
        fork_point,
        headers: (fork_point + 1..=f.tip.height + 4)
            .map(|height| header(setup.g.block(height).unwrap()))
            .collect(),
        blocks: 4,
        prune_to: Some(f.target.height + 2),
    }
}

/// Test 2: a failure after any statement leaves the previous state, and the
/// retried update equals an uninterrupted one.
#[tokio::test]
#[ignore = "requires isolated PostgreSQL 17"]
async fn an_update_is_atomic_after_every_statement() -> Result {
    let seed = 7;
    let mut setup = Setup::new(seed, 1, 60, true, WEEK).await?;
    rich_position(&mut setup, 4).await?;
    // The twin follows the same chain and gets the same update uninterrupted.
    let mut twin = Setup::new(seed, 1, 60, true, WEEK).await?;
    rich_position(&mut twin, 4).await?;
    let plan = rich_plan(&setup);
    let calls = [(harness::day(), 1_000)];
    let (built, params) = parts(&setup.follower, &setup, &plan).await?;
    let f = &mut setup.follower;
    let update = Update {
        target: &f.target,
        tip: &f.tip,
        fork_point: plan.fork_point,
        headers: &plan.headers,
        blocks: &built.blocks,
        epoch_params: &params,
        mint_delta: &built.mint,
        calls: &calls,
        observations: &built.observations,
        prune_to: plan.prune_to,
    };
    let before = dump(&setup.database.config).await?;
    let mut failures = 0;
    let report = loop {
        f.store.probe().fail_after = Some(failures + 1);
        match f.store.apply_update(&setup.profile, &update).await {
            Ok(report) => break report,
            Err(error) => {
                assert_eq!(
                    error.report()["error"],
                    "injected_failure",
                    "{}",
                    error.report()
                );
                failures += 1;
                assert_eq!(
                    dump(&setup.database.config).await?,
                    before,
                    "state after a failure after statement {failures}"
                );
            }
        }
    };
    f.store.probe().fail_after = None;
    assert!(failures >= 15, "only {failures} statements");
    // Every statement is a failure point, the last one included.
    assert_eq!(report.statements, failures);
    // Rows reached every table (the descriptor statement also ran: its units
    // were stored already, so it inserted none).
    for table in [
        "block",
        "datum",
        "tx",
        "tx_out",
        "ma_tx_out",
        "tx_in",
        "epoch_param",
        "first_served",
    ] {
        assert!(
            report.inserted.contains_key(table),
            "{table} not exercised: {:?}",
            report.inserted
        );
    }
    println!(
        "{}",
        json!({"event":"atomicity","failures_injected":failures,"statements":report.statements,"inserted":report.inserted,"deleted":report.deleted})
    );
    let (twin_built, twin_params) = parts(&twin.follower, &twin, &plan).await?;
    let t = &mut twin.follower;
    let twin_update = Update {
        target: &t.target,
        tip: &t.tip,
        fork_point: plan.fork_point,
        headers: &plan.headers,
        blocks: &twin_built.blocks,
        epoch_params: &twin_params,
        mint_delta: &twin_built.mint,
        calls: &calls,
        observations: &twin_built.observations,
        prune_to: plan.prune_to,
    };
    let twin_report = t
        .store
        .apply_update(&twin.profile, &twin_update)
        .await
        .map_err(named)?;
    assert_eq!(twin_report.inserted, report.inserted);
    assert_eq!(twin_report.deleted, report.deleted);
    let retried = logical(&mut f.store, &setup.profile, &setup.database.config).await?;
    let uninterrupted = logical(&mut t.store, &twin.profile, &twin.database.config).await?;
    assert_eq!(retried, uninterrupted);
    twin.cleanup().await?;
    setup.cleanup().await
}

/// Test 3: each refusal names its cause and writes nothing.
#[tokio::test]
#[ignore = "requires isolated PostgreSQL 17"]
async fn each_refusal_has_its_code_and_writes_nothing() -> Result {
    let mut setup = Setup::new(11, 1, 60, false, WEEK).await?;
    rich_position(&mut setup, 3).await?;
    let mut base = advance(&setup.g, &setup.follower, 3, None);
    base.prune_to = Some(setup.follower.target.height + 1);
    let (built, params) = parts(&setup.follower, &setup, &base).await?;
    let (target, tip, horizon) = (
        setup.follower.target.clone(),
        setup.follower.tip.clone(),
        setup.follower.horizon,
    );
    let Setup {
        profile,
        g,
        source,
        database,
        follower,
        ..
    } = setup;
    drop(follower);
    let mut store = Store::connect(database.config.clone(), None).await?;
    let before = dump(&database.config).await?;
    let stored_epoch = g.block(target.height).unwrap().epoch;
    let horizon_epoch = g.block(horizon).unwrap().epoch;
    let wrong = HeaderRef {
        height: target.height,
        hash: "00".repeat(32),
    };
    let negative = BTreeMap::from([(profile.assets["cnight"].unit(), -1_000_000_000_000_i128)]);
    let foreign = BTreeMap::from([(format!("{}00", "ab".repeat(28)), 1_i128)]);
    let mut gap = base.headers.clone();
    gap.remove(1);
    let mut extra = base.headers.clone();
    let mut next = extra[0].clone();
    next.height = tip.height + 4;
    extra.push(next);
    // Final heights above a low fork point are checked against the update's
    // headers; the others against the stored ones.
    let low_fork = target.height + 1;
    let low_headers = (low_fork + 1..=tip.height + 3)
        .map(|height| header(g.block(height).unwrap()))
        .collect::<Vec<_>>();
    let mut altered = built.blocks.clone();
    altered[2].hash = "11".repeat(32);
    let mut counted = built.blocks.clone();
    counted[0].transaction_count += 1;
    let mut moved = built.blocks.clone();
    let row = moved
        .iter_mut()
        .find(|block| !block.rows.tx.is_empty())
        .ok_or("no transaction rows")?;
    row.rows.tx[0].block_id += 1;
    let mut conflict = params.clone();
    conflict.push(EpochParamRow {
        epoch: stored_epoch,
        nonce: "22".repeat(32),
        max_tx_size: 1,
    });
    let below = vec![EpochParamRow {
        epoch: horizon_epoch.saturating_sub(3),
        nonce: "33".repeat(32),
        max_tx_size: 1,
    }];
    let shifted = &built.blocks[1..];
    let regress = vec![header(g.block(tip.height - 1).unwrap())];
    let no_mint = BTreeMap::new();
    let negative_calls = [(harness::day(), -1)];
    let valid = Update {
        target: &target,
        tip: &tip,
        fork_point: base.fork_point,
        headers: &base.headers,
        blocks: &built.blocks,
        epoch_params: &params,
        mint_delta: &built.mint,
        calls: &[(harness::day(), 5)],
        observations: &built.observations,
        prune_to: base.prune_to,
    };
    let cases: Vec<(&str, Update<'_>)> = vec![
        (
            "update_base_mismatch",
            Update {
                target: &wrong,
                ..valid
            },
        ),
        (
            "update_headers_not_consecutive",
            Update {
                headers: &gap,
                ..valid
            },
        ),
        (
            "update_fork_point_out_of_range",
            Update {
                fork_point: target.height - 1,
                ..valid
            },
        ),
        (
            "update_fork_point_out_of_range",
            Update {
                fork_point: tip.height + 1,
                ..valid
            },
        ),
        (
            "update_tip_regressed",
            Update {
                fork_point: tip.height - 2,
                headers: &regress,
                blocks: &[],
                epoch_params: &[],
                mint_delta: &no_mint,
                observations: &[],
                prune_to: None,
                ..valid
            },
        ),
        (
            "update_blocks_not_consecutive",
            Update {
                blocks: shifted,
                ..valid
            },
        ),
        (
            "update_target_not_tip_minus_k",
            Update {
                headers: &extra,
                ..valid
            },
        ),
        (
            "update_block_header_mismatch",
            Update {
                fork_point: low_fork,
                headers: &low_headers,
                blocks: &altered,
                ..valid
            },
        ),
        (
            "update_block_header_mismatch",
            Update {
                blocks: &altered,
                ..valid
            },
        ),
        (
            "update_block_header_mismatch",
            Update {
                blocks: &counted,
                ..valid
            },
        ),
        (
            "update_epoch_param_missing",
            Update {
                epoch_params: &[],
                ..valid
            },
        ),
        (
            "update_prune_out_of_range",
            Update {
                prune_to: Some(horizon),
                ..valid
            },
        ),
        (
            "update_prune_out_of_range",
            Update {
                prune_to: Some(target.height + 4),
                ..valid
            },
        ),
        (
            "update_rows_outside_block",
            Update {
                blocks: &moved,
                ..valid
            },
        ),
        (
            "update_mint_unit_not_fixed",
            Update {
                mint_delta: &foreign,
                ..valid
            },
        ),
        (
            "update_supply_negative",
            Update {
                mint_delta: &negative,
                ..valid
            },
        ),
        (
            "update_epoch_param_conflict",
            Update {
                epoch_params: &conflict,
                ..valid
            },
        ),
        (
            "update_epoch_param_below_window",
            Update {
                epoch_params: &below,
                ..valid
            },
        ),
        (
            "update_calls_negative",
            Update {
                calls: &negative_calls,
                ..valid
            },
        ),
    ];
    for (code, update) in cases {
        let error = store
            .apply_update(&profile, &update)
            .await
            .map(|_| ())
            .unwrap_err();
        assert_eq!(error.report()["error"], code, "{}", error.report());
        assert_eq!(dump(&database.config).await?, before, "{code} wrote");
    }
    // Not serving.
    store
        .close_gate("test_closed", &json!({}))
        .await
        .map_err(named)?;
    let closed = dump(&database.config).await?;
    let error = store
        .apply_update(&profile, &valid)
        .await
        .map(|_| ())
        .unwrap_err();
    assert_eq!(error.report()["error"], "update_base_mismatch");
    assert_eq!(dump(&database.config).await?, closed);
    drop(store);
    database.cleanup().await?;
    source.database.cleanup().await
}

/// Test 4: a header replacement changes only the headers above the fork point.
#[tokio::test]
#[ignore = "requires isolated PostgreSQL 17"]
async fn header_replacement_changes_only_headers_above_the_fork_point() -> Result {
    let mut setup = Setup::new(13, 1, 60, false, WEEK).await?;
    let plan = advance(&setup.g, &setup.follower, 3, None);
    setup.step(&plan).await?;
    let content = |dump: &Value| {
        let mut dump = dump.clone();
        let object = dump.as_object_mut().unwrap();
        object.remove("serve.block");
        for table in ["dsa.position", "dsa.service_state", "dsa.calls"] {
            object.remove(table);
        }
        dump
    };
    for (salt, depth) in [(1, 1), (2, 10), (3, K - 1)] {
        let before = dump(&setup.database.config).await?;
        let plan = replace(&setup.g, &setup.follower, depth, salt);
        let fork_point = plan.fork_point;
        setup.step(&plan).await?;
        let after = dump(&setup.database.config).await?;
        assert_eq!(content(&before), content(&after), "depth {depth}");
        let blocks = |dump: &Value, low: bool| {
            dump["serve.block"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|row| (row["id"].as_u64().unwrap() <= u64::from(fork_point)) == low)
                .cloned()
                .collect::<Vec<_>>()
        };
        assert_eq!(blocks(&before, true), blocks(&after, true), "depth {depth}");
        let replaced = blocks(&after, false)
            .iter()
            .map(|row| {
                row["hash"]
                    .as_str()
                    .unwrap()
                    .trim_start_matches("\\x")
                    .to_owned()
            })
            .collect::<Vec<_>>();
        let expected = plan
            .headers
            .iter()
            .map(|row| row.hash.clone())
            .collect::<Vec<_>>();
        assert_eq!(replaced, expected, "depth {depth}");
        assert_eq!(replaced.len() as u32, depth);
        assert_ne!(blocks(&before, false), blocks(&after, false));
    }
    let state = setup
        .follower
        .store
        .follow_state(&setup.profile)
        .await
        .map_err(named)?;
    assert_eq!(state.tip.hash, setup.follower.tip.hash);
    setup.cleanup().await
}

async fn first_served(config: &tokio_postgres::Config) -> Result<Vec<(i64, String)>> {
    Ok(connect(config)
        .await?
        .query(
            "SELECT height::bigint,first_served_at::text FROM dsa.first_served ORDER BY height",
            &[],
        )
        .await?
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect())
}

fn heights(rows: &[(i64, String)]) -> Vec<i64> {
    rows.iter().map(|(height, _)| *height).collect()
}

/// Test 5: the first-served rows are H…T; a catch-up update's blocks share
/// one time; with times moved back the candidate moves and never passes T.
#[tokio::test]
#[ignore = "requires isolated PostgreSQL 17"]
async fn first_served_rows_follow_the_window() -> Result {
    let mut setup = Setup::new(17, 1, 70, false, 86_400).await?;
    let config = setup.database.config.clone();
    let expect = |setup: &Setup| {
        (i64::from(setup.follower.horizon)..=i64::from(setup.follower.target.height))
            .collect::<Vec<_>>()
    };
    assert_eq!(heights(&first_served(&config).await?), expect(&setup));
    let before = first_served(&config).await?;
    let plan = advance(&setup.g, &setup.follower, 5, None);
    setup.step(&plan).await?;
    let rows = first_served(&config).await?;
    assert_eq!(heights(&rows), expect(&setup));
    let new = rows[before.len()..]
        .iter()
        .map(|(_, at)| at.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(new.len(), 1, "one time for the blocks of one update");
    assert!(!before.iter().any(|(_, at)| new.contains(at)));
    // Everything is recent: the candidate stays at the horizon.
    let store = &setup.follower.store;
    assert_eq!(
        store.horizon_candidate().await.map_err(named)?,
        setup.follower.horizon
    );
    for step in 0..6 {
        let f = &setup.follower;
        let prune_to = (step % 2 == 1).then_some(f.horizon + 1);
        let plan = advance(&setup.g, f, 2, prune_to);
        setup.step(&plan).await?;
        assert_eq!(
            heights(&first_served(&config).await?),
            expect(&setup),
            "step {step}"
        );
    }
    // Heights below X first served three days ago: the candidate is X.
    let x = setup.follower.horizon + 3;
    let client = connect(&config).await?;
    client
        .execute(
            "UPDATE dsa.first_served SET first_served_at=first_served_at-interval '3 days' WHERE height<$1",
            &[&(x as i32)],
        )
        .await?;
    let store = &setup.follower.store;
    assert_eq!(store.horizon_candidate().await.map_err(named)?, x);
    let plan = advance(&setup.g, &setup.follower, 1, Some(x));
    setup.step(&plan).await?;
    assert_eq!(setup.follower.horizon, x);
    assert_eq!(heights(&first_served(&config).await?), expect(&setup));
    // All of them: the candidate is T and never above it.
    client
        .batch_execute(
            "UPDATE dsa.first_served SET first_served_at=first_served_at-interval '3 days'",
        )
        .await?;
    let target = setup.follower.target.height;
    let store = &setup.follower.store;
    assert_eq!(store.horizon_candidate().await.map_err(named)?, target);
    let plan = advance(&setup.g, &setup.follower, 2, Some(target));
    setup.step(&plan).await?;
    assert_eq!(heights(&first_served(&config).await?), expect(&setup));
    // The two new heights are recent: the candidate is the first of them.
    let store = &setup.follower.store;
    let candidate = store.horizon_candidate().await.map_err(named)?;
    assert_eq!(candidate, target + 1);
    assert!(candidate <= setup.follower.target.height);
    setup.cleanup().await
}

/// Rows outside every update's range that a store keeps: old unspent outputs
/// with their transactions and cNIGHT rows (every tenth at the mapping
/// address, with its datum), and every fourth of them spent by transactions
/// at the `window` height.
async fn fill(
    config: &tokio_postgres::Config,
    profile: &dsa_chain::profiles::Profile,
    first: i32,
    blocks: i32,
    window: u32,
) -> Result {
    let cnight = &profile.assets["cnight"];
    let last = first + blocks - 1;
    let client = connect(config).await?;
    client
        .execute(
            "INSERT INTO serve.tx(id,hash,block_id,block_index,valid_contract)
             SELECT b::bigint*4096+i,decode(md5('f'||b||'/'||i)||md5('g'||b||'/'||i),'hex'),b,i,true
             FROM generate_series($1::integer,$2::integer) b,generate_series(0,99) i",
            &[&first, &last],
        )
        .await?;
    client
        .execute(
            "INSERT INTO serve.tx_out(id,tx_id,index,address,value,data_hash,consumed_by_tx_id)
             SELECT t.id*4096+j,t.id,j,CASE WHEN t.id%10=0 THEN $3 ELSE 'addr_filler_'||(t.id%997) END,1000000,
              CASE WHEN t.id%10=0 THEN decode(md5('d'||t.id||'/'||j)||md5('e'||t.id||'/'||j),'hex') END,NULL
             FROM serve.tx t,generate_series(0,1) j WHERE t.block_id BETWEEN $1 AND $2 AND (j=0 OR t.id%2=0)",
            &[&i64::from(first), &i64::from(last), &profile.addresses["mapping"]],
        )
        .await?;
    client
        .execute(
            "INSERT INTO serve.datum(id,hash,value)
             SELECT nextval('dsa.datum_id_seq'),o.data_hash,jsonb_build_object('int',o.id)
             FROM serve.tx_out o WHERE o.tx_id BETWEEN $1::bigint*4096 AND $2::bigint*4096+4095 AND o.data_hash IS NOT NULL",
            &[&i64::from(first), &i64::from(last)],
        )
        .await?;
    client
        .execute(
            "INSERT INTO serve.ma_tx_out(id,quantity,tx_out_id,ident)
             SELECT o.id*4096,5,o.id,m.id FROM serve.tx_out o
             JOIN serve.multi_asset m ON m.policy=decode($3,'hex') AND m.name=decode($4,'hex')
             WHERE o.tx_id BETWEEN $1::bigint*4096 AND $2::bigint*4096+4095",
            &[
                &i64::from(first),
                &i64::from(last),
                &cnight.policy,
                &cnight.name,
            ],
        )
        .await?;
    // Spending transactions at the window height, 4,000 inputs each.
    let base = i64::from(window) * 4096 + 1000 + i64::from(first);
    client
        .execute(
            "WITH spent AS (SELECT row_number() OVER (ORDER BY o.id)-1 AS n,o.tx_id,o.index FROM serve.tx_out o
              WHERE o.tx_id BETWEEN $1::bigint*4096 AND $2::bigint*4096+4095 AND o.index=0 AND o.tx_id%4=1),
             spenders AS (INSERT INTO serve.tx(id,hash,block_id,block_index,valid_contract)
              SELECT DISTINCT $3::bigint+n/4000,decode(md5('s'||($3::bigint+n/4000))||md5('t'||($3::bigint+n/4000)),'hex'),
               $4::bigint,$3::bigint+n/4000-$4::bigint*4096,true FROM spent)
             INSERT INTO serve.tx_in(id,tx_in_id,tx_out_id,tx_out_index)
             SELECT ($3::bigint+n/4000)*4096+n%4000,$3::bigint+n/4000,tx_id,index FROM spent",
            &[&i64::from(first), &i64::from(last), &base, &i64::from(window)],
        )
        .await?;
    client.batch_execute("ANALYZE").await?;
    Ok(())
}

fn buffers(plan: &Value) -> u64 {
    let node = &plan[0]["Plan"];
    [
        "Shared Hit Blocks",
        "Shared Read Blocks",
        "Local Hit Blocks",
        "Local Read Blocks",
    ]
    .iter()
    .map(|key| node[*key].as_u64().unwrap_or(0))
    .sum()
}

/// Relations that grow with the store. The others hold a handful of rows by
/// construction (descriptors, epochs, the state rows), where a sequential
/// scan of their one page is the cheapest plan.
const GROWING: [&str; 10] = [
    "serve.block",
    "serve.tx",
    "serve.tx_out",
    "serve.tx_in",
    "serve.ma_tx_out",
    "serve.datum",
    "serve.tx_metadata",
    "serve.epoch_stake",
    "dsa.consumed_before_horizon",
    "dsa.first_served",
];

fn sequential_scans(node: &Value, found: &mut Vec<String>) {
    let relation = format!(
        "{}.{}",
        node["Schema"].as_str().unwrap_or(""),
        node["Relation Name"].as_str().unwrap_or("")
    );
    if node["Node Type"] == "Seq Scan" && GROWING.contains(&relation.as_str()) {
        found.push(relation);
    }
    for child in node["Plans"].as_array().into_iter().flatten() {
        sequential_scans(child, found);
    }
}

type Costs = BTreeMap<String, (u64, Vec<String>, Vec<Value>)>;

async fn costed(setup: &mut Setup, plan: &Plan) -> Result<Costs> {
    setup.follower.store.probe().explain = true;
    setup.step(plan).await?;
    let mut probe = setup.follower.store.probe();
    probe.explain = false;
    let mut costs = BTreeMap::new();
    for (label, plan) in probe.plans.drain(..) {
        let mut scans = Vec::new();
        sequential_scans(&plan[0]["Plan"], &mut scans);
        let entry = costs.entry(label).or_insert((0, Vec::new(), Vec::new()));
        entry.0 += buffers(&plan);
        entry.1.extend(scans);
        entry.2.push(plan);
    }
    Ok(costs)
}

/// Test 6: every statement of an update and the held-output lookup use
/// indexes and read no more buffers when the store holds ten times more rows
/// outside the update; the horizon advance is timed on a Mainnet-size store.
#[tokio::test]
#[ignore = "requires isolated PostgreSQL 17"]
async fn update_cost_is_bounded_by_the_update() -> Result {
    let first = 1_000_000;
    let mut small = Setup::new(19, first, 60, true, WEEK).await?;
    let mut large = Setup::new(19, first, 60, true, WEEK).await?;
    let window = small.follower.target.height;
    fill(&small.database.config, &small.profile, 1_000, 50, window).await?;
    fill(&large.database.config, &large.profile, 1_000, 500, window).await?;
    let mut results = Vec::new();
    for step in 0..3u64 {
        let plans = [&small, &large].map(|setup| {
            let f = &setup.follower;
            let mut plan = advance(&setup.g, f, 4, None);
            if step == 1 {
                plan.fork_point = f.tip.height - 2;
                plan.headers = (plan.fork_point + 1..=f.tip.height + 4)
                    .map(|height| header(setup.g.block(height).unwrap()))
                    .collect();
            }
            plan
        });
        let [small_plan, large_plan] = plans;
        let small_cost = costed(&mut small, &small_plan).await?;
        let large_cost = costed(&mut large, &large_plan).await?;
        for (label, (large_buffers, scans, large_plans)) in &large_cost {
            let (small_buffers, small_plans) = small_cost
                .get(label)
                .map_or((0, Vec::new()), |cost| (cost.0, cost.2.clone()));
            assert!(
                scans.is_empty(),
                "step {step} {label}: sequential scans {scans:?}"
            );
            assert!(
                *large_buffers <= small_buffers * 3 / 2 + 16,
                "step {step} {label}: {small_buffers} buffers grew to {large_buffers}: small {} large {}",
                serde_json::to_string(&small_plans)?,
                serde_json::to_string(large_plans)?
            );
            results.push(json!({"step":step,"statement":label,"buffers_small":small_buffers,"buffers_large":large_buffers}));
        }
    }
    let client = connect(&large.database.config).await?;
    let counts: Value = client
        .query_one(
            "SELECT jsonb_build_object('tx',(SELECT count(*) FROM serve.tx),'tx_out',(SELECT count(*) FROM serve.tx_out),
              'ma_tx_out',(SELECT count(*) FROM serve.ma_tx_out),'tx_in',(SELECT count(*) FROM serve.tx_in),'block',(SELECT count(*) FROM serve.block),
              'datum',(SELECT count(*) FROM serve.datum))",
            &[],
        )
        .await?
        .get(0);
    println!(
        "{}",
        json!({"event":"update_cost","large_store_rows":counts,"statements":results})
    );
    // A Mainnet-size store: about 300,000 outputs, 200,000 transactions.
    fill(&large.database.config, &large.profile, 2_000, 1_500, window).await?;
    let mainnet: Value = client
        .query_one(
            "SELECT jsonb_build_object('tx',(SELECT count(*) FROM serve.tx),'tx_out',(SELECT count(*) FROM serve.tx_out),
              'ma_tx_out',(SELECT count(*) FROM serve.ma_tx_out),'tx_in',(SELECT count(*) FROM serve.tx_in),'block',(SELECT count(*) FROM serve.block),
              'database_bytes',pg_database_size(current_database()))",
            &[],
        )
        .await?
        .get(0);
    let f = &mut large.follower;
    let target = f.horizon + 3;
    let began = Instant::now();
    let advanced = f
        .store
        .advance_horizon(&large.profile, target)
        .await
        .map_err(named)?;
    let seconds = began.elapsed().as_secs_f64();
    println!(
        "{}",
        json!({"event":"horizon_advance_mainnet_size","rows":mainnet,"from":target-3,"to":target,"deleted":advanced.deleted,"seconds":seconds})
    );
    small.cleanup().await?;
    large.cleanup().await
}

/// Test 7: a block that spends a lost cNIGHT output fails with
/// `asset_conservation`; after `close_gate` every view raises 55000.
#[tokio::test]
#[ignore = "requires isolated PostgreSQL 17"]
async fn a_lost_output_stops_the_block_and_close_gate_stops_serving() -> Result {
    let mut setup = Setup::new(23, 1, 70, false, WEEK).await?;
    let cnight = setup.profile.assets["cnight"].unit();
    let selection_profile = setup.profile.clone();
    let selection = Selection::new(&selection_profile);
    // The next block that spends a stored output holding cNIGHT.
    let (height, lost) = loop {
        let f = &setup.follower;
        let height = f.target.height + 1;
        if height > setup.last_target {
            return Err("no spend of a stored cNIGHT output".into());
        }
        let block = setup.g.block(height).unwrap();
        let transactions = decode_all(
            block
                .transactions
                .iter()
                .map(|tx| (tx.hash.as_str(), tx.cbor.as_slice())),
        )?;
        let held = f
            .store
            .held_outputs(&dsa_chain::block::inputs(&transactions))
            .await
            .map_err(named)?;
        if let Some((point, _)) = held
            .iter()
            .find(|(_, held)| held.assets.iter().any(|asset| asset.unit() == cnight))
        {
            break (height, point.clone());
        }
        let plan = advance(&setup.g, f, 1, None);
        setup.step(&plan).await?;
    };
    let client = connect(&setup.database.config).await?;
    let deleted = client
        .execute(
            "WITH o AS (SELECT o.id FROM serve.tx_out o JOIN serve.tx t ON t.id=o.tx_id WHERE t.hash=decode($1,'hex') AND o.index=$2::integer),
             a AS (DELETE FROM serve.ma_tx_out WHERE tx_out_id IN (SELECT id FROM o))
             DELETE FROM serve.tx_out WHERE id IN (SELECT id FROM o)",
            &[&lost.hash, &i32::from(lost.index)],
        )
        .await?;
    assert_eq!(deleted, 1);
    let f = &setup.follower;
    let block = setup.g.block(height).unwrap();
    let transactions = decode_all(
        block
            .transactions
            .iter()
            .map(|tx| (tx.hash.as_str(), tx.cbor.as_slice())),
    )?;
    let held = f
        .store
        .held_outputs(&dsa_chain::block::inputs(&transactions))
        .await
        .map_err(named)?;
    let header = dsa_chain::block::Header {
        height,
        hash: block.hash.clone(),
        previous_hash: block.previous.clone(),
        transaction_count: transactions.len(),
    };
    let mut supplements = dsa_chain::block::Supplements::default();
    let error = loop {
        match process_block(&selection, &header, &transactions, &held, &supplements) {
            Err(error) => break error,
            Ok(Outcome::Ready(_)) => return Err("the block passed without its input".into()),
            Ok(Outcome::Needs(requests)) => {
                for request in requests {
                    match request {
                        dsa_chain::block::SupplementRequest::Datum(hash) => {
                            supplements
                                .datums
                                .insert(hash, dsa_chain::block::DatumAvailability::NotFound);
                        }
                        dsa_chain::block::SupplementRequest::Metadata(tx) => {
                            supplements
                                .metadata
                                .insert(tx.clone(), setup.g.metadata.get(&tx).cloned().flatten());
                        }
                        dsa_chain::block::SupplementRequest::Output { .. } => {
                            return Err("output supplement".into());
                        }
                    }
                }
            }
        }
    };
    assert_eq!(error.code(), "asset_conservation", "{error}");
    let mut store = Store::connect(setup.database.config.clone(), None).await?;
    drop(setup.follower);
    store
        .grant_reader(&setup.profile, &setup.database.reader)
        .await
        .map_err(named)?;
    store
        .close_gate(
            "asset_conservation",
            &json!({"transaction": error.to_string()}),
        )
        .await
        .map_err(named)?;
    let reader = connect(&setup.database.reader_config()).await?;
    for table in TABLES {
        let failure = reader
            .query(&format!("SELECT * FROM public.{table} LIMIT 1"), &[])
            .await
            .unwrap_err();
        assert_eq!(
            failure.code().map(|code| code.code()),
            Some("55000"),
            "{table}"
        );
    }
    let state = store.follow_state(&setup.profile).await.map_err(named)?;
    assert!(!state.serving);
    assert_eq!(state.reason, "asset_conservation");
    let status = store.status(&setup.profile).await.map_err(named)?;
    assert_eq!(status["service_state"]["state"], "stopped");
    assert_eq!(status["service_state"]["reason"], "asset_conservation");
    drop(reader);
    drop(store);
    setup.database.cleanup().await?;
    setup.source.database.cleanup().await
}

async fn observed(client: &tokio_postgres::Client) -> Result<BTreeMap<String, (i64, String)>> {
    Ok(client
        .query(
            "SELECT shape,block_height::bigint,encode(transaction_hash,'hex') FROM dsa.first_observation",
            &[],
        )
        .await?
        .iter()
        .map(|row| {
            (
                row.get(0),
                (row.get(1), row.get::<_, Option<String>>(2).unwrap_or_default()),
            )
        })
        .collect())
}

/// Test 8: verify finds a lost token-less output at a profile address; the
/// first observation of each shape is kept once; a failing insert does not
/// fail the update.
#[tokio::test]
#[ignore = "requires isolated PostgreSQL 17"]
async fn verify_counts_unspent_outputs_and_first_observations_are_kept_once() -> Result {
    let mut setup = Setup::new(29, 1, 70, true, WEEK).await?;
    let config = setup.database.config.clone();
    let client = connect(&config).await?;
    let mut recorded: BTreeMap<String, (i64, String)> = BTreeMap::new();
    let mut handed = 0;
    while setup.follower.target.height < setup.last_target - 20 {
        let plan = advance(&setup.g, &setup.follower, 1, None);
        let report = setup.step(&plan).await?;
        assert!(
            report.observation_failures.is_empty(),
            "{:?}",
            report.observation_failures
        );
        let now = observed(&client).await?;
        for (shape, row) in &recorded {
            assert_eq!(now.get(shape), Some(row), "{shape} replaced");
        }
        recorded = now;
        handed = setup.follower.seen.len();
    }
    let seen = setup.follower.seen.clone();
    assert!(handed >= 2, "only {handed} shapes observed");
    assert_eq!(
        recorded
            .iter()
            .map(|(shape, (height, _))| (shape.clone(), *height as u32))
            .collect::<BTreeMap<_, _>>(),
        seen
    );
    // A failing insert: the update commits and reports it.
    client
        .batch_execute("DELETE FROM dsa.first_observation;CREATE FUNCTION dsa.fail_observation() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'generated observation failure';END$$;CREATE TRIGGER fail_observation BEFORE INSERT ON dsa.first_observation FOR EACH ROW EXECUTE FUNCTION dsa.fail_observation()")
        .await?;
    let failed_at = loop {
        let plan = advance(&setup.g, &setup.follower, 1, None);
        let report = setup.step(&plan).await?;
        if !report.observation_failures.is_empty() {
            assert_eq!(
                report.observation_failures[0]["message"],
                "generated observation failure"
            );
            assert!(report.observations_stored.is_empty());
            break setup.follower.target.height;
        }
        if setup.follower.target.height >= setup.last_target {
            return Err("no candidate after the trigger".into());
        }
    };
    assert!(observed(&client).await?.is_empty());
    assert_eq!(setup.follower.target.height, failed_at);
    client
        .batch_execute("DROP TRIGGER fail_observation ON dsa.first_observation;DROP FUNCTION dsa.fail_observation()")
        .await?;
    let stored_later = loop {
        let plan = advance(&setup.g, &setup.follower, 1, None);
        let report = setup.step(&plan).await?;
        if let Some(shape) = report.observations_stored.first() {
            break (shape.clone(), setup.follower.target.height);
        }
        if setup.follower.target.height >= setup.last_target {
            return Err("no candidate after the failure".into());
        }
    };
    assert!(stored_later.1 > failed_at);
    assert_eq!(
        observed(&client).await?[&stored_later.0].0,
        i64::from(stored_later.1)
    );
    // A token-less unspent output at a profile address, lost.
    setup
        .follower
        .store
        .verify(&setup.profile)
        .await
        .map_err(named)?;
    let addresses = setup
        .profile
        .addresses
        .values()
        .cloned()
        .collect::<Vec<_>>();
    let lost: i64 = client
        .query_opt(
            "SELECT o.id FROM serve.tx_out o WHERE o.address=ANY($1) AND o.data_hash IS NULL
             AND NOT EXISTS(SELECT 1 FROM serve.ma_tx_out a WHERE a.tx_out_id=o.id)
             AND NOT EXISTS(SELECT 1 FROM serve.tx_in i WHERE i.tx_out_id=o.tx_id AND i.tx_out_index=o.index)
             ORDER BY o.id DESC LIMIT 1",
            &[&addresses],
        )
        .await?
        .ok_or("no token-less unspent output at a profile address")?
        .get(0);
    client
        .execute("DELETE FROM serve.tx_out WHERE id=$1", &[&lost])
        .await?;
    let error = setup
        .follower
        .store
        .verify(&setup.profile)
        .await
        .map(|_| ())
        .unwrap_err();
    let report = error.report();
    assert_eq!(report["error"], "unspent_count_mismatch", "{report}");
    assert_eq!(
        report["expected"].as_str().unwrap().parse::<i64>()? - 1,
        report["actual"].as_str().unwrap().parse::<i64>()?
    );
    setup.cleanup().await
}
