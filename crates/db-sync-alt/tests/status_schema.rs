use std::collections::BTreeSet;

use serde_json::{Value, json};

fn contract() -> (jsonschema::Validator, Value) {
    let schema: Value = serde_json::from_str(include_str!("../schema/status.schema.json")).unwrap();
    jsonschema::meta::validate(&schema).unwrap();
    let validator = jsonschema::options()
        .should_validate_formats(true)
        .build(&schema)
        .unwrap();
    let examples = serde_json::from_str(include_str!("../schema/status.examples.json")).unwrap();
    (validator, examples)
}

#[test]
fn every_service_state_validates() {
    let (validator, examples) = contract();
    let expected = BTreeSet::from([
        "restoring",
        "catching_up",
        "following",
        "waiting",
        "stopped_by_check",
    ]);
    let mut actual = BTreeSet::new();
    for (name, document) in examples.as_object().unwrap() {
        validator
            .validate(document)
            .unwrap_or_else(|error| panic!("{name} failed at {}: {error}", error.instance_path()));
        actual.insert(document["state"].as_str().unwrap());
    }
    assert_eq!(actual, expected);
}

#[test]
fn required_sync_information_is_enforced() {
    let (validator, examples) = contract();
    for path in [
        "/service_version",
        "/node_release",
        "/database_format",
        "/state",
        "/state_reason",
        "/retry_at",
        "/provider_head",
        "/stored_tip",
        "/confirmed_target",
        "/served_target",
        "/horizon",
        "/lag/blocks",
        "/tip_lag/seconds",
        "/progress/blocks_remaining",
        "/progress/blocks_per_hour",
        "/progress/estimated_tip_at",
        "/last_update",
        "/last_error",
        "/retention/seconds",
        "/retention/next_horizon_move_at",
        "/provider/calls_today",
        "/provider/calls_previous_days",
        "/provider/pace_requests_per_second",
        "/provider/requests_in_flight",
        "/provider/last_refusal",
        "/epochs/current_epoch",
        "/epochs/newest_parameters_epoch",
        "/epochs/newest_stake_epoch",
        "/integrity/unspent_outputs",
        "/integrity/supply_check",
        "/integrity/snapshot",
        "/database/size_bytes",
        "/database/reader_check",
        "/database/tls_available",
        "/warnings",
    ] {
        let mut document = examples["following"].clone();
        let (parent, key) = path.rsplit_once('/').unwrap();
        document
            .pointer_mut(parent)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove(key);
        assert!(
            !validator.is_valid(&document),
            "missing required field {path} was accepted"
        );
    }
}

#[test]
fn inconsistent_health_and_invalid_values_are_refused() {
    let (validator, examples) = contract();
    for (path, value) in [
        ("/reader", Value::Null),
        ("/healthy", json!(false)),
        ("/database/reader_check/passed", json!(false)),
        ("/freshness/provider_observation_age_seconds", Value::Null),
        ("/provider/calls_today", json!(-1)),
        ("/provider/pace_requests_per_second", json!(11)),
        ("/provider/utc_day", json!("2026-02-30")),
        ("/horizon/hash", json!("bad hash")),
        ("/state", json!("unknown state")),
        ("/retention/seconds", json!(86399)),
    ] {
        let mut document = examples["following"].clone();
        *document.pointer_mut(path).unwrap() = value;
        assert!(
            !validator.is_valid(&document),
            "invalid value at {path} was accepted"
        );
    }
    let mut waiting = examples["waiting"].clone();
    waiting["retry_at"] = Value::Null;
    assert!(!validator.is_valid(&waiting));
    let mut stopped = examples["stopped_by_check"].clone();
    stopped["last_error"] = Value::Null;
    assert!(!validator.is_valid(&stopped));
}

#[test]
fn consumed_monitoring_fields_keep_their_shapes_and_readiness_meaning() {
    let (_, examples) = contract();
    let following = &examples["following"];
    assert_eq!(following["serving"], "ready");
    assert!(
        following["freshness"]["provider_observation_age_seconds"]
            .as_f64()
            .unwrap()
            <= 120.0
    );
    assert!(following["healthy"].as_bool().unwrap());
    assert!(following["reason_codes"].as_array().unwrap().is_empty());
    assert_eq!(following["lag"]["blocks"], 0);
    assert_eq!(following["tip_lag"]["blocks"], 2160);
    assert!(
        following["provider_head"]["height"].as_u64().unwrap()
            > following["served_target"]["height"].as_u64().unwrap()
    );
    for document in examples.as_object().unwrap().values() {
        for field in ["serving", "sync", "service_version"] {
            assert!(document[field].is_string());
        }
        for field in ["error_code", "poll_error_code"] {
            assert!(document[field].is_null() || document[field].is_string());
        }
        assert!(document["healthy"].is_boolean());
        assert!(document["reason_codes"].is_array());
    }
}
