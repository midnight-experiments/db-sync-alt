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
        "/capabilities",
        "/real_data_evidence",
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

#[test]
fn capability_scope_and_historical_gaps_cannot_be_silently_promoted() {
    let (validator, examples) = contract();
    let document = &examples["following"];
    assert_eq!(
        document["capabilities"],
        serde_json::to_value(dsa_chain::capabilities::NodeCapabilities::pinned()).unwrap()
    );
    for (path, value) in [
        (
            "/capabilities/registered_candidates/enabled_in_block_production_and_import",
            json!(true),
        ),
        (
            "/capabilities/registered_candidates/operator_rpc_cli",
            json!("supported"),
        ),
        (
            "/real_data_evidence/stored_large_datum/historical_validation",
            json!("proven"),
        ),
    ] {
        let mut changed = document.clone();
        *changed.pointer_mut(path).unwrap() = value;
        assert!(!validator.is_valid(&changed), "accepted {path}");
    }
    for shape in [
        "stored_pointer_address",
        "stored_large_datum",
        "stored_bridge_metadata",
    ] {
        assert_eq!(
            document["real_data_evidence"][shape]["historical_validation"],
            "not_proven_on_real_data"
        );
        assert!(document["real_data_evidence"][shape]["first_stored_observation"].is_null());
    }
}

#[test]
fn committed_shape_requires_its_actual_dimension_and_excludes_credentials() {
    let (validator, examples) = contract();
    let observation = json!({"block_height":10,"block_hash":"01".repeat(32),"transaction_hash":"02".repeat(32),"transaction_index":0,"observed_at":1,"output_index":0,"datum_hash":"03".repeat(32),"raw_datum_bytes":10001,"metadata_key":6500973,"sources":[{"source":"block_cbor","endpoint":format!("/blocks/{}/txs/cbor?count=100&page=1&order=asc","01".repeat(32)),"response_sha256":"04".repeat(32)}]});
    for (shape, dimension) in [
        ("stored_pointer_address", "output_index"),
        ("stored_large_datum", "raw_datum_bytes"),
        ("stored_bridge_metadata", "metadata_key"),
    ] {
        let mut document = examples["following"].clone();
        document["real_data_evidence"][shape]["first_stored_observation"] = observation.clone();
        assert!(validator.is_valid(&document));
        document["real_data_evidence"][shape]["first_stored_observation"][dimension] = Value::Null;
        assert!(!validator.is_valid(&document));
    }
    let mut document = examples["following"].clone();
    let mut observation = observation;
    observation["project_id"] = json!("synthetic-key");
    document["real_data_evidence"]["stored_large_datum"]["first_stored_observation"] = observation;
    assert!(!validator.is_valid(&document));
}
