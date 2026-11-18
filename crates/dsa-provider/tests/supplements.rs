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
    let record: Record = serde_json::from_str(include_str!(
        "../../../tests/fixtures/data/blockfrost/collateral-omission-cbor.json"
    ))
    .unwrap();
    let raw = hex::decode(record.cbor).unwrap();
    let tx = transaction::decode(&raw, &record.tx_hash).unwrap();
    let reply: TransactionOutputs = serde_json::from_str(include_str!(
        "../../../tests/fixtures/data/blockfrost/collateral-omission-utxos.json"
    ))
    .unwrap();
    let resolved = resolve_outputs(&tx, &reply).unwrap();
    assert_eq!(resolved.len(), tx.outputs.len() + 1);
    for (index, output) in tx.outputs.iter().enumerate() {
        let output = output.as_ref().unwrap();
        assert_eq!(resolved[&(index as u16)].address_raw, output.address_raw);
        assert_eq!(resolved[&(index as u16)].assets, output.assets);
    }
    assert_eq!(
        resolved[&(tx.outputs.len() as u16)],
        *tx.collateral_output.as_ref().unwrap()
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
fn invalid_native_model_requires_only_the_return_at_body_output_count() {
    // Native projection model only. No serialized transaction or external decoder.
    let address = dsa_chain::profiles::Profile::load("mainnet")
        .unwrap()
        .addresses["mapping"]
        .clone();
    let output = transaction::Output {
        address_raw: transaction::address_bytes(&address).unwrap(),
        address: Some(address.clone()),
        coin: 100,
        assets: Vec::new(),
        data_hash: None,
        inline_datum: None,
        classification_safe: true,
    };
    let tx = transaction::Transaction {
        hash: "03".repeat(32),
        valid: false,
        ordinary: Vec::new(),
        collateral: Vec::new(),
        reference: Vec::new(),
        outputs: vec![None, None, None],
        collateral_output: Some(output.clone()),
        has_collateral_return: true,
        mint: Vec::new(),
        datums: Default::default(),
        body: &[],
        auxiliary: None,
        auxiliary_hash: None,
    };
    let mut reply:TransactionOutputs=serde_json::from_value(serde_json::json!({"hash":tx.hash,"outputs":[{"address":address,"output_index":3,"amount":[{"unit":"lovelace","quantity":"100"}],"data_hash":null,"inline_datum":null,"collateral":false,"consumed_by_tx":null}]})).unwrap();
    assert_eq!(resolve_outputs(&tx, &reply).unwrap()[&3], output);
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
