//! The status document: one golden document per state, every item the
//! status must hold, and the rule of `/health`. DSA_UPDATE_GOLDEN=1 writes
//! the golden documents from the code instead of comparing with them.

use std::{collections::BTreeMap, path::PathBuf};

use chrono::NaiveDate;
use dsa_provider::Refusal;
use dsa_service::{
    follow::ProviderTip,
    status::{FollowerView, Settings, State, Status, compose, health},
};
use dsa_store::{
    service_view::{
        Answer, DatabaseView, DayCalls, FirstObservation, Identity, Point, ReaderCheck, Recorded,
        StoredPosition, StoredState, Upkeep,
    },
    upkeep::{Rebuilt, SnapshotFile, SupplyCheck},
};
use serde_json::{Value, json};

/// 2026-11-18 12:00:00 UTC.
const NOW: u64 = 1_795_003_200;
const K: u32 = 2160;
const TIP: u32 = 14_210_000;

fn point(height: u32, fill: char, timestamp: i64) -> Point {
    Point {
        height,
        hash: fill.to_string().repeat(64),
        timestamp,
    }
}

fn now() -> i64 {
    NOW as i64
}

/// A serving Mainnet database at the tip, read 5 seconds ago.
fn serving_view() -> DatabaseView {
    let target = point(TIP - K, '2', now() - 43_230);
    let tip = point(TIP, '1', now() - 30);
    let horizon = point(TIP - K - 30_000, '3', now() - 43_230 - 600_000);
    let mut calls = Vec::new();
    for days_ago in 0..8_u64 {
        calls.push(DayCalls {
            day: NaiveDate::from_ymd_opt(2026, 11, 18)
                .unwrap()
                .checked_sub_days(chrono::Days::new(7 - days_ago))
                .unwrap(),
            requests: 9_000 + days_ago as i64,
        });
    }
    DatabaseView {
        observed_at: now() - 5,
        identity: Identity {
            database_format: 1,
            network: "mainnet".to_owned(),
            profile_hash: "a".repeat(64),
            keep_rule_version: 2,
            tool_version: "0.5.0".to_owned(),
            node_release: "2.1.101".to_owned(),
            origin: json!({"kind":"restore","content_digest":"b".repeat(64)}),
            created_at: "2026-11-10T00:00:00.000000+00:00".to_owned(),
        },
        position: StoredPosition {
            serving: true,
            reason: "serving".to_owned(),
            target_height: Some(i64::from(target.height)),
            target_hash: Some(target.hash.clone()),
            target_time: Some("2026-11-18T00:00:00+00:00".to_owned()),
            tip_height: Some(i64::from(tip.height)),
            tip_hash: Some(tip.hash.clone()),
            tip_time: Some("2026-11-18T11:59:30+00:00".to_owned()),
            horizon_height: Some(i64::from(horizon.height)),
            horizon_hash: Some(horizon.hash.clone()),
            horizon_time: Some("2026-11-11T07:20:00+00:00".to_owned()),
            retention_seconds: 604_800,
            updated_at: "2026-11-18T11:59:50.000000+00:00".to_owned(),
        },
        target: Some(target),
        tip: Some(tip),
        horizon: Some(horizon),
        horizon_first_served_at: Some(now() - 600_000),
        current_epoch: Some(667),
        newest_parameters_epoch: Some(667),
        newest_stake_epoch: Some(666),
        service_state: Some(StoredState {
            state: "following".to_owned(),
            reason: "at_tip".to_owned(),
            detail: json!({"target": TIP - K, "wait_until": null}),
            last_update: Some(json!({
                "target": TIP - K, "tip": TIP, "horizon": TIP - K - 30_000, "blocks": 1, "headers": 1,
                "inserted": {"block": 1, "tx": 2, "tx_out": 3, "first_served": 1},
                "deleted": {"tx": 1, "tx_out": 2}, "insert_seconds": 0.25,
                "advance_seconds": 0.5, "keep_reapplied_seconds": null,
                "observations_stored": [], "observation_failures": [], "statements": 21,
                "at": "2026-11-18T11:59:50.000000+00:00"
            })),
            last_error: Some(
                json!({"error": "provider_rate_limited", "at": "2026-11-18T09:00:00+00:00"}),
            ),
            updated_at: now() - 10,
            last_update_at: Some(now() - 10),
            last_error_at: Some(now() - 10_800),
        }),
        calls,
        unspent: BTreeMap::from([("addr_test".to_owned(), 12), ("addr_ics".to_owned(), 40)]),
        first_observations: vec![FirstObservation {
            shape: "stored_large_datum".to_owned(),
            block_height: 14_100_000,
            transaction: Some("4".repeat(64)),
            output_index: Some(0),
            datum_hash: Some("5".repeat(64)),
            raw_datum_bytes: Some(10_240),
            observed_at: now() - 900_000,
            answers: vec![Answer {
                source: "block_page:1".to_owned(),
                bytes: 30_000,
                sha256: "6".repeat(64),
            }],
        }],
        upkeep: Upkeep {
            supply: Some(Recorded {
                at: now() - 3_600,
                record: SupplyCheck {
                    passed: true,
                    supply: BTreeMap::from([("c".repeat(56), "24000000000000000".to_owned())]),
                    unspent_outputs: 250_000,
                    failure: None,
                },
            }),
            snapshot: Some(Recorded {
                at: now() - 40_000,
                record: SnapshotFile {
                    file: format!("mainnet-14200000-{}.snapshot", "b".repeat(12)),
                    content_digest: "b".repeat(64),
                    target: 14_200_000,
                    bytes: 80_000_000,
                },
            }),
            indexes: BTreeMap::from([(
                "idx_tx_out_address".to_owned(),
                Rebuilt {
                    at: now() - 86_400,
                    bytes: 8_388_608,
                },
            )]),
        },
        size_bytes: 250_000_000,
        tls_available: true,
        reader_check: Some(ReaderCheck {
            role: "db_sync_alt_reader".to_owned(),
            passed: true,
            error: None,
            violations: Vec::new(),
            warnings: Vec::new(),
        }),
    }
}

fn following() -> FollowerView {
    FollowerView {
        state: State::Following,
        reason: "at_tip".to_owned(),
        wait_until: None,
        provider_tip: Some(ProviderTip {
            height: TIP + 1,
            hash: "7".repeat(64),
            time: NOW - 10,
            observed_at: NOW - 2,
        }),
        requests_per_second: 8.0,
        requests_in_flight: 0,
        last_refusal: None,
        blocks_remaining: Some(1),
        blocks_per_hour: Some(180.0),
    }
}

fn settings() -> Settings {
    Settings::new("mainnet")
}

/// The inputs of each golden document.
fn states() -> Vec<(&'static str, Option<DatabaseView>, Option<FollowerView>)> {
    let mut empty = serving_view();
    empty.position = StoredPosition {
        serving: false,
        reason: "restore_failed".to_owned(),
        target_height: None,
        target_hash: None,
        target_time: None,
        tip_height: None,
        tip_hash: None,
        tip_time: None,
        horizon_height: None,
        horizon_hash: None,
        horizon_time: None,
        retention_seconds: 604_800,
        updated_at: "2026-11-18T11:00:00.000000+00:00".to_owned(),
    };
    empty.identity.origin = json!({"kind":"restore_failed"});
    (empty.target, empty.tip, empty.horizon) = (None, None, None);
    (empty.horizon_first_served_at, empty.current_epoch) = (None, None);
    (empty.newest_parameters_epoch, empty.newest_stake_epoch) = (None, None);
    empty.service_state = None;
    empty.unspent = BTreeMap::new();
    empty.first_observations = Vec::new();
    empty.upkeep = Upkeep::default();
    let restoring = FollowerView {
        state: State::Restoring,
        reason: "snapshot_file".to_owned(),
        provider_tip: None,
        blocks_remaining: None,
        blocks_per_hour: None,
        ..following()
    };

    let mut behind = serving_view();
    let catching_up = FollowerView {
        state: State::CatchingUp,
        reason: "behind_tip".to_owned(),
        provider_tip: Some(ProviderTip {
            height: TIP + 5_000,
            ..following().provider_tip.unwrap()
        }),
        requests_in_flight: 4,
        blocks_remaining: Some(5_000),
        blocks_per_hour: Some(4_500.0),
        ..following()
    };
    behind.service_state.as_mut().unwrap().state = "catching_up".to_owned();
    // 5,000 blocks behind: the stored positions are about 100,000 s old.
    behind.tip.as_mut().unwrap().timestamp = now() - 100_010;
    behind.target.as_mut().unwrap().timestamp = now() - 143_210;

    let waiting = FollowerView {
        state: State::Waiting,
        reason: "quota_exhausted".to_owned(),
        wait_until: Some(NOW + 43_320),
        provider_tip: Some(ProviderTip {
            observed_at: NOW - 600,
            ..following().provider_tip.unwrap()
        }),
        last_refusal: Some(Refusal {
            http_status: 402,
            code: "quota_exhausted",
            at: NOW - 600,
            until: NOW + 43_320,
        }),
        blocks_remaining: Some(30),
        blocks_per_hour: Some(0.0),
        ..following()
    };

    let mut revoked = serving_view();
    revoked.position.serving = false;
    revoked.position.reason = "supply_mismatch".to_owned();
    let stored = revoked.service_state.as_mut().unwrap();
    stored.state = "stopped".to_owned();
    stored.reason = "supply_mismatch".to_owned();
    stored.detail = json!({"unit": "c".repeat(56)});
    let supply = revoked.upkeep.supply.as_mut().unwrap();
    supply.record.passed = false;
    supply.record.failure = Some(
        json!({"error":"supply_mismatch","unit":"c".repeat(56),"recorded":"24000000000000000","unspent":"23999999999999999"}),
    );
    let stopped = FollowerView {
        state: State::Stopped,
        reason: "supply_mismatch".to_owned(),
        ..following()
    };

    let mut not_running = serving_view();
    not_running.reader_check = None;
    vec![
        ("restoring", Some(empty), Some(restoring)),
        ("catching_up", Some(behind), Some(catching_up)),
        ("following", Some(serving_view()), Some(following())),
        ("waiting", Some(serving_view()), Some(waiting)),
        ("stopped_by_check", Some(revoked), Some(stopped)),
        ("service_not_running", Some(not_running), None),
    ]
}

fn golden_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/status")
        .join(format!("{name}.json"))
}

fn document(status: &Status) -> Value {
    serde_json::to_value(status).unwrap()
}

/// The JSON pointers of the string values that start with `\x`, the bytea
/// text of PostgreSQL: every hash of the document is plain hexadecimal.
fn bytea_text(value: &Value, at: &str, found: &mut Vec<String>) {
    match value {
        Value::String(text) if text.starts_with("\\x") => found.push(at.to_owned()),
        Value::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                bytea_text(item, &format!("{at}/{index}"), found);
            }
        }
        Value::Object(fields) => {
            for (key, item) in fields {
                bytea_text(item, &format!("{at}/{key}"), found);
            }
        }
        _ => {}
    }
}

#[test]
fn every_state_has_its_golden_document() {
    let update = std::env::var("DSA_UPDATE_GOLDEN").is_ok_and(|value| value == "1");
    for (name, view, follower) in states() {
        let actual = document(&compose(view.as_ref(), follower.as_ref(), &settings(), NOW));
        let path = golden_path(name);
        if update {
            std::fs::write(&path, serde_json::to_string_pretty(&actual).unwrap() + "\n").unwrap();
            continue;
        }
        let mut bytea = Vec::new();
        bytea_text(&actual, "", &mut bytea);
        assert!(bytea.is_empty(), "{name}: bytea text at {bytea:?}");
        let expected: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap())
            .unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(
            actual, expected,
            "{name} differs from its golden document; run with DSA_UPDATE_GOLDEN=1 and review the diff"
        );
    }
}

/// Every item the status must hold (owner, 2026-11-17), as the JSON pointer
/// of the field that holds it, plus the fields the monitoring of earlier
/// releases reads.
const ITEMS: &[&str] = &[
    // identity
    "/service_version",
    "/network",
    "/node_release",
    "/database_format",
    "/keep_rule_version",
    // the registered-candidate capability and the limitations
    "/capabilities/registered_candidates/operator_rpc_cli",
    "/limitations",
    "/unsupported_node_statements",
    // the first stored observation of each shape
    "/first_observations/stored_pointer_address",
    "/first_observations/stored_large_datum",
    "/first_observations/stored_bridge_metadata",
    // state, reason, the time the wait ends
    "/state",
    "/state_reason",
    "/wait_until",
    // positions and lag
    "/provider_head",
    "/stored_tip",
    "/served_target",
    "/horizon",
    "/lag/blocks",
    "/lag/seconds",
    "/tip_lag/blocks",
    "/tip_lag/seconds",
    // progress
    "/progress/blocks_remaining",
    "/progress/blocks_per_hour",
    "/progress/estimated_tip_at",
    // last update and last error
    "/last_update",
    "/last_error",
    // retention and the next horizon move
    "/retention/seconds",
    "/retention/next_horizon_move_at",
    // provider
    "/provider/calls_today",
    "/provider/calls_previous_days",
    "/provider/pace_requests_per_second",
    "/provider/requests_in_flight",
    "/provider/last_refusal",
    // epochs
    "/epochs/current_epoch",
    "/epochs/newest_parameters_epoch",
    "/epochs/newest_stake_epoch",
    // integrity
    "/integrity/unspent_outputs",
    "/integrity/supply_check",
    "/integrity/snapshot",
    // database
    "/database/size_bytes",
    "/database/reader_check",
    "/database/tls_available",
    // warnings
    "/warnings",
    // the fields earlier monitoring reads
    "/healthy",
    "/reason_codes",
    "/serving",
    "/sync",
    "/error_code",
    "/poll_error_code",
    "/freshness/provider_observation_age_seconds",
    // the stored rows earlier commands printed
    "/identity",
    "/position",
    "/service_state",
    "/provider_observed_at",
    "/generated_at",
];

#[test]
fn every_status_item_is_in_every_state() {
    for (name, view, follower) in states() {
        let value = document(&compose(view.as_ref(), follower.as_ref(), &settings(), NOW));
        for item in ITEMS {
            assert!(value.pointer(item).is_some(), "{name}: {item} is missing");
        }
        assert_eq!(
            value["provider"]["calls_previous_days"]
                .as_array()
                .unwrap()
                .len(),
            7,
            "{name}"
        );
        for position in ["/served_target", "/stored_tip", "/horizon"] {
            if let Some(point) = value.pointer(position).filter(|point| !point.is_null()) {
                for key in ["height", "hash", "timestamp"] {
                    assert!(point.get(key).is_some(), "{name}: {position}/{key}");
                }
            }
        }
    }
    // Without a database view the document still has every field.
    let value = document(&compose(None, Some(&following()), &settings(), NOW));
    for item in ITEMS {
        assert!(value.pointer(item).is_some(), "no view: {item} is missing");
    }
}

#[test]
fn the_document_names_its_state_and_never_claims_following_without_a_follower() {
    let expected = [
        ("restoring", "restoring", Some("snapshot_file")),
        ("catching_up", "catching_up", Some("behind_tip")),
        ("following", "following", Some("at_tip")),
        ("waiting", "waiting", Some("quota_exhausted")),
        ("stopped_by_check", "stopped", Some("supply_mismatch")),
        (
            "service_not_running",
            "stopped",
            Some("service_not_running"),
        ),
    ];
    for ((name, view, follower), (expected_name, state, reason)) in
        states().into_iter().zip(expected)
    {
        assert_eq!(name, expected_name);
        let status = compose(view.as_ref(), follower.as_ref(), &settings(), NOW);
        let value = document(&status);
        assert_eq!(value["state"], state, "{name}");
        assert_eq!(value["state_reason"].as_str(), reason, "{name}");
        if name != "following" {
            assert!(!status.healthy, "{name}");
        }
    }
    let waiting = &states()[3];
    let status = compose(waiting.1.as_ref(), waiting.2.as_ref(), &settings(), NOW);
    assert_eq!(status.wait_until, Some(NOW + 43_320));
    assert_eq!(status.error_code.as_deref(), Some("quota_exhausted"));
    assert_eq!(status.poll_error_code, Some("quota_exhausted"));
    let stopped = &states()[4];
    let status = compose(stopped.1.as_ref(), stopped.2.as_ref(), &settings(), NOW);
    assert_eq!(document(&status)["serving"], "revoked");
    assert_eq!(status.error_code.as_deref(), Some("supply_mismatch"));
    assert_eq!(
        status
            .integrity
            .supply_check
            .unwrap()
            .record
            .failure
            .unwrap()["error"],
        "supply_mismatch"
    );
    // A stored "following" without a running follower is not claimed.
    let mut view = serving_view();
    for stored in ["following", "catching_up", "waiting", "published"] {
        view.service_state.as_mut().unwrap().state = stored.to_owned();
        let status = compose(Some(&view), None, &settings(), NOW);
        assert_eq!(status.state, State::Stopped, "{stored}");
        assert!(status.reason_codes.contains(&"state_stopped"), "{stored}");
    }
}

#[test]
fn health_is_200_only_when_every_condition_holds() {
    let healthy = compose(Some(&serving_view()), Some(&following()), &settings(), NOW);
    assert_eq!(health(&healthy), (true, Vec::new()));
    assert!(healthy.healthy && healthy.reason_codes.is_empty());
    assert_eq!(healthy.lag.blocks, Some(1));
    // Each failing condition alone gives its own reason code.
    let mut closed = serving_view();
    closed.position.serving = false;
    let mut behind = following();
    behind.provider_tip.as_mut().unwrap().height = TIP + 10;
    let mut at_limit = following();
    at_limit.provider_tip.as_mut().unwrap().height = TIP + 9;
    let mut stale = following();
    stale.provider_tip.as_mut().unwrap().observed_at = NOW - 121;
    let mut fresh = following();
    fresh.provider_tip.as_mut().unwrap().observed_at = NOW - 120;
    let mut unread = following();
    unread.provider_tip = None;
    let mut refused = serving_view();
    refused.reader_check = Some(ReaderCheck {
        passed: false,
        error: Some("reader_privileges_not_exact".to_owned()),
        violations: vec!["relation:dsa.position".to_owned()],
        ..refused.reader_check.unwrap()
    });
    let mut unchecked = serving_view();
    unchecked.reader_check = None;
    let mut old_view = serving_view();
    old_view.observed_at = now() - 61;
    let catching_up = FollowerView {
        state: State::CatchingUp,
        ..following()
    };
    let cases: Vec<(&str, DatabaseView, FollowerView, Vec<&str>)> = vec![
        ("gate closed", closed, following(), vec!["serving_revoked"]),
        (
            "not following",
            serving_view(),
            catching_up,
            vec!["state_catching_up"],
        ),
        (
            "lag above the limit",
            serving_view(),
            behind,
            vec!["serving_block_lag_exceeded"],
        ),
        ("lag at the limit", serving_view(), at_limit, vec![]),
        (
            "stale provider observation",
            serving_view(),
            stale,
            vec!["provider_observation_stale"],
        ),
        ("observation 120 s old", serving_view(), fresh, vec![]),
        (
            "no provider observation",
            serving_view(),
            unread,
            vec!["provider_observation_unknown", "serving_block_lag_unknown"],
        ),
        (
            "reader check failed",
            refused,
            following(),
            vec!["reader_check_failed"],
        ),
        (
            "reader not checked",
            unchecked,
            following(),
            vec!["reader_check_unknown"],
        ),
        (
            "stale database view",
            old_view,
            following(),
            vec!["database_observation_stale"],
        ),
    ];
    for (name, view, follower, expected) in cases {
        let status = compose(Some(&view), Some(&follower), &settings(), NOW);
        let (healthy, mut reasons) = health(&status);
        reasons.sort_unstable();
        let mut expected = expected;
        expected.sort_unstable();
        assert_eq!(healthy, expected.is_empty(), "{name}");
        assert_eq!(reasons, expected, "{name}");
        assert_eq!(status.reason_codes.len(), expected.len(), "{name}");
    }
    // Without a database view the gate is unknown.
    let status = compose(None, Some(&following()), &settings(), NOW);
    assert!(!status.healthy);
    assert!(status.reason_codes.contains(&"serving_unknown"));
    // The limit is a setting.
    let mut wide = settings();
    wide.max_lag_blocks = 20;
    let mut behind = following();
    behind.provider_tip.as_mut().unwrap().height = TIP + 15;
    assert!(compose(Some(&serving_view()), Some(&behind), &wide, NOW).healthy);
    assert!(!compose(Some(&serving_view()), Some(&behind), &settings(), NOW).healthy);
}

#[test]
fn lag_and_progress_are_derived_from_the_provider_tip_and_the_stored_positions() {
    let status = compose(Some(&serving_view()), Some(&following()), &settings(), NOW);
    // The stored tip is the target plus k: the lag is the provider's tip
    // ahead of the stored tip, the tip lag its distance to the target.
    assert_eq!(status.lag.blocks, Some(1));
    assert_eq!(status.lag.seconds, Some(20));
    assert_eq!(status.tip_lag.blocks, Some(u64::from(K) + 1));
    assert_eq!(status.tip_lag.seconds, Some(43_220));
    assert_eq!(status.freshness.provider_observation_age_seconds, Some(2));
    let catching_up = &states()[1];
    let status = compose(
        catching_up.1.as_ref(),
        catching_up.2.as_ref(),
        &settings(),
        NOW,
    );
    // 5,000 blocks at 4,500 an hour: 4,000 seconds.
    assert_eq!(status.progress.estimated_tip_at, Some(NOW + 4_000));
    assert_eq!(status.provider.calls_today, Some(9_007));
    assert_eq!(status.provider.calls_previous_days[0].requests, 9_006);
    assert_eq!(status.provider.calls_previous_days[6].requests, 9_000);
    assert_eq!(status.integrity.unspent_outputs, Some(250_000));
    assert_eq!(
        status.retention.next_horizon_move_at,
        Some(now() - 600_000 + 604_800)
    );
    let update = status.last_update.unwrap();
    assert_eq!((update.events_written, update.rows_pruned), (5, 3));
    assert_eq!(update.duration_seconds, 0.75);
}
