//! Real finalized provider fallback and native adapter-model refusals.
use dsa_chain::transaction;
use dsa_provider::{supplements::resolve_outputs, types::TransactionOutputs};
use serde::Deserialize;

#[derive(Deserialize)]
struct Record {
    tx_hash: String,
    cbor: String,
}

#[test]
fn real_output_fallback_preserves_body_coordinates_value_and_collateral_role() {
    let record: Record =
        serde_json::from_str(include_str!("fixtures/collateral-omission-cbor.json")).unwrap();
    let raw = hex::decode(record.cbor).unwrap();
    let tx = transaction::decode(&raw, &record.tx_hash).unwrap();
    let reply: TransactionOutputs =
        serde_json::from_str(include_str!("fixtures/collateral-omission-utxos.json")).unwrap();
    let resolved = resolve_outputs(&tx, &reply).unwrap();
    assert_eq!(resolved.len(), tx.outputs().len());
    for (index, output) in tx.outputs().iter().enumerate() {
        let output = output.as_ref().unwrap();
        assert_eq!(resolved[&(index as u16)].address_raw, output.address_raw);
        assert_eq!(resolved[&(index as u16)].assets, output.assets);
    }
    assert!(
        !resolved.contains_key(&(tx.outputs().len() as u16)),
        "valid collateral return is outside the transaction's effects"
    );
    let mut bad = reply.clone();
    bad.outputs[0].address = "invalid".to_owned();
    assert_eq!(
        resolve_outputs(&tx, &bad).unwrap_err(),
        "provider_output_address_mismatch"
    );
    let mut bad = reply.clone();
    bad.outputs[0].inline_datum = Some(serde_json::from_value(serde_json::json!("00")).unwrap());
    assert_eq!(
        resolve_outputs(&tx, &bad).unwrap_err(),
        "provider_inline_datum_hash_mismatch"
    );
    let mut bad = reply.clone();
    bad.outputs[0]
        .amount
        .retain(|amount| amount.unit != "lovelace");
    assert_eq!(
        resolve_outputs(&tx, &bad).unwrap_err(),
        "provider_invalid_response"
    );
    let mut bad = reply.clone();
    bad.outputs[1].output_index = bad.outputs[0].output_index;
    assert_eq!(
        resolve_outputs(&tx, &bad).unwrap_err(),
        "provider_invalid_response"
    );
    let mut bad = reply.clone();
    bad.outputs.pop();
    assert_eq!(
        resolve_outputs(&tx, &bad).unwrap_err(),
        "provider_output_count_mismatch"
    );
    let scratch = tempfile::tempdir().unwrap();
    std::fs::create_dir(scratch.path().join("outputs")).unwrap();
    std::fs::write(
        scratch.path().join(format!("outputs/{}.json", tx.hash())),
        serde_json::to_vec(&bad).unwrap(),
    )
    .unwrap();
    let request = dsa_chain::block::SupplementRequest::Output {
        transaction: tx.hash().to_owned(),
        index: 0,
    };
    assert_eq!(
        recorded::load(
            scratch.path(),
            &request,
            1,
            std::slice::from_ref(&tx),
            &mut dsa_chain::block::Supplements::default()
        )
        .unwrap_err()
        .to_string(),
        "provider_output_count_mismatch"
    );
    let mut bad = reply.clone();
    bad.outputs[0].amount[0].unit = "short".to_owned();
    assert_eq!(
        resolve_outputs(&tx, &bad).unwrap_err(),
        "provider_invalid_response"
    );
    let mut bad = reply.clone();
    bad.outputs[0].collateral = true;
    assert_eq!(
        resolve_outputs(&tx, &bad).unwrap_err(),
        "provider_output_role_mismatch"
    );
    let mut bad = reply;
    bad.outputs[0]
        .amount
        .iter_mut()
        .find(|a| a.unit == "lovelace")
        .unwrap()
        .quantity
        .0 += 1;
    assert_eq!(
        resolve_outputs(&tx, &bad).unwrap_err(),
        "provider_output_value_mismatch"
    );
}

#[test]
fn an_invalid_transaction_resolves_only_its_return_at_the_body_output_count() {
    use dsa_testkit::tx::{Output, Tx, hash};
    let address = dsa_chain::profiles::Profile::load("mainnet")
        .unwrap()
        .addresses["mapping"]
        .clone();
    let built = Tx::new()
        .collateral(&hash(b"collateral"), 0)
        .output(Output::new(Vec::new(), 1))
        .output(Output::new(Vec::new(), 1))
        .output(Output::new(Vec::new(), 1))
        .collateral_return(Output::to(&address, 100))
        .invalid()
        .build();
    let tx = transaction::decode(&built.cbor, &built.hash).unwrap();
    let mut reply: TransactionOutputs = serde_json::from_value(serde_json::json!({"hash":built.hash,"outputs":[{"address":address,"output_index":3,"amount":[{"unit":"lovelace","quantity":"100"}],"data_hash":null,"inline_datum":null,"collateral":false}]})).unwrap();
    assert_eq!(
        &resolve_outputs(&tx, &reply).unwrap()[&3],
        tx.collateral_return().unwrap()
    );
    reply.outputs[0].collateral = true;
    assert_eq!(
        resolve_outputs(&tx, &reply).unwrap_err(),
        "provider_output_role_mismatch"
    );
    reply.outputs[0].collateral = false;
    reply.outputs[0].output_index = 0;
    assert_eq!(
        resolve_outputs(&tx, &reply).unwrap_err(),
        "provider_output_count_mismatch"
    );
}

#[path = "support/recorded.rs"]
mod recorded;

#[test]
fn recorded_404_is_scoped_to_the_requesting_block() {
    use dsa_chain::block::{DatumAvailability, SupplementRequest, Supplements};
    let scratch = tempfile::tempdir().unwrap();
    std::fs::create_dir(scratch.path().join("datums")).unwrap();
    let hash = transaction::hash(&[0]);
    let request = SupplementRequest::Datum(hash.clone());
    // A legacy unscoped negative response cannot establish current absence.
    std::fs::write(scratch.path().join(format!("datums/{hash}.json")), "null").unwrap();
    let mut supplements = Supplements::default();
    assert!(
        !recorded::load(scratch.path(), &request, 10, &[], &mut supplements)
            .unwrap()
            .is_some()
    );
    std::fs::write(
        scratch.path().join(format!("datums/10-{hash}.json")),
        "null",
    )
    .unwrap();
    assert!(
        recorded::load(scratch.path(), &request, 10, &[], &mut supplements)
            .unwrap()
            .is_some()
    );
    assert_eq!(supplements.datums[&hash], DatumAvailability::NotFound);
    let mut later = Supplements::default();
    assert!(
        !recorded::load(scratch.path(), &request, 11, &[], &mut later)
            .unwrap()
            .is_some()
    );
    std::fs::write(
        scratch.path().join(format!("datums/11-{hash}.json")),
        "\"00\"",
    )
    .unwrap();
    assert!(
        recorded::load(scratch.path(), &request, 11, &[], &mut later)
            .unwrap()
            .is_some()
    );
    assert_eq!(later.datums[&hash], DatumAvailability::Present(vec![0]));
    std::fs::write(
        scratch.path().join(format!("datums/12-{hash}.json")),
        "\"01\"",
    )
    .unwrap();
    assert_eq!(
        recorded::load(
            scratch.path(),
            &request,
            12,
            &[],
            &mut Supplements::default()
        )
        .unwrap_err()
        .to_string(),
        "provider_datum_hash_mismatch"
    );
}

#[test]
fn shared_positive_datum_requires_bound_capture_provenance() {
    use dsa_chain::block::{DatumAvailability, SupplementRequest, Supplements};
    use sha2::{Digest, Sha256};
    let scratch = tempfile::tempdir().unwrap();
    std::fs::create_dir(scratch.path().join("datums")).unwrap();
    let hash = transaction::hash(&[0]);
    let request = SupplementRequest::Datum(hash.clone());
    let file = scratch.path().join(format!("datums/{hash}.json"));
    std::fs::write(&file, "\"00\"").unwrap();
    assert!(
        !recorded::load(
            scratch.path(),
            &request,
            10,
            &[],
            &mut Supplements::default()
        )
        .unwrap()
        .is_some(),
        "an unproven shared200 must not silently supply a historical request"
    );
    // Native capture-metadata model. Actual corpus provenance records its real
    // API recorder source and a measured completion timestamp or upper bound.
    let mut capture = serde_json::json!({"format":"blockfrost-datum-capture-v1","datum_hash":hash,"response_sha256":format!("{:x}",Sha256::digest(std::fs::read(&file).unwrap())),"response_observed_no_later_than_unix":1,"time_basis":"recorder_completion_bound","capture_source_commit":"ae3853d62dec00c1094080099e8809bde9bd9a33","capture_working_tree":"clean","capture_code_blob":"ca1c98759c9e5fb01768481107b624cdfc2934f7","capture_code_basis":"exact_source_commit"});
    let provenance = file.with_extension("provenance.json");
    std::fs::write(&provenance, serde_json::to_vec(&capture).unwrap()).unwrap();
    let mut known = Supplements::default();
    assert!(
        recorded::load(scratch.path(), &request, 10, &[], &mut known)
            .unwrap()
            .is_some()
    );
    assert_eq!(known.datums[&hash], DatumAvailability::Present(vec![0]));
    capture["response_observed_no_later_than_unix"] = serde_json::json!(u64::MAX);
    std::fs::write(&provenance, serde_json::to_vec(&capture).unwrap()).unwrap();
    assert_eq!(
        recorded::load(
            scratch.path(),
            &request,
            11,
            &[],
            &mut Supplements::default()
        )
        .unwrap_err()
        .to_string(),
        "invalid_shared_datum_capture_provenance"
    );
    capture["response_observed_no_later_than_unix"] = serde_json::json!(1);
    capture["response_sha256"] = serde_json::json!("00".repeat(32));
    std::fs::write(&provenance, serde_json::to_vec(&capture).unwrap()).unwrap();
    assert!(
        recorded::load(
            scratch.path(),
            &request,
            11,
            &[],
            &mut Supplements::default()
        )
        .is_err()
    );
}
