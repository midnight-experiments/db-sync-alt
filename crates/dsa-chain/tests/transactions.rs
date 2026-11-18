use dsa_chain::transaction::{self, Asset, Outpoint};
use pallas_traverse::{MultiEraAsset, MultiEraInput, MultiEraTx};
use serde::Deserialize;

#[derive(Deserialize)]
struct Record {
    tx_hash: String,
    cbor: String,
}
fn records() -> Vec<Record> {
    serde_json::from_str(include_str!("fixtures/finalized-transactions.json")).unwrap()
}
fn inputs(values: Vec<MultiEraInput<'_>>) -> Vec<Outpoint> {
    let mut result: Vec<_> = values
        .iter()
        .map(|p| Outpoint {
            hash: p.hash().to_string(),
            index: u16::try_from(p.index()).unwrap(),
        })
        .collect();
    result.sort();
    result
}
fn assets(values: Vec<MultiEraAsset<'_>>) -> Vec<Asset> {
    let mut result: Vec<_> = values
        .iter()
        .map(|a| Asset {
            policy: a.policy().to_string(),
            name: hex::encode(a.name()),
            quantity: a.any_coin(),
        })
        .collect();
    result.sort_by(|a, b| (&a.policy, &a.name).cmp(&(&b.policy, &b.name)));
    result
}

#[test]
fn ra_14_a_and_c_real_original_bytes_inputs_outputs_mint_and_validity() {
    let mut compared = 0;
    for record in records() {
        let raw = hex::decode(record.cbor).unwrap();
        let tx = transaction::decode(&raw, &record.tx_hash).unwrap();
        assert_eq!(tx.hash, record.tx_hash);
        if let Ok(other) = MultiEraTx::decode(&raw) {
            compared += 1;
            assert_eq!(tx.hash, other.hash().to_string());
            assert_eq!(tx.valid, other.is_valid());
            assert_eq!(tx.ordinary, inputs(other.inputs()));
            assert_eq!(tx.collateral, inputs(other.collateral()));
            assert_eq!(tx.reference, inputs(other.reference_inputs()));
            assert_eq!(
                tx.mint,
                assets(other.mints().iter().flat_map(|p| p.assets()).collect())
            );
            let outputs = other.outputs();
            assert_eq!(tx.outputs.len(), outputs.len());
            for (ours, theirs) in tx.outputs.iter().zip(outputs) {
                let ours = ours.as_ref().expect("classified recorded output");
                assert_eq!(ours.coin, theirs.value().coin());
                assert_eq!(ours.address_raw, theirs.address().unwrap().to_vec());
                assert_eq!(
                    ours.assets,
                    assets(
                        theirs
                            .value()
                            .assets()
                            .iter()
                            .flat_map(|p| p.assets())
                            .collect()
                    )
                );
            }
            assert_eq!(
                tx.has_collateral_return,
                other.collateral_return().is_some()
            );
        }
    }
    assert!(compared > 0);
    println!("real_finalized_transactions_compared={compared}");
}

#[test]
fn ra_14_a_hash_and_complete_envelope_are_checked() {
    let record = records().remove(0);
    let mut raw = hex::decode(record.cbor).unwrap();
    assert_eq!(
        transaction::decode(&raw, &"00".repeat(32))
            .unwrap_err()
            .code,
        "cbor_body_hash_mismatch"
    );
    raw.push(0);
    assert_eq!(
        transaction::decode(&raw, &record.tx_hash).unwrap_err().code,
        "cbor_trailing_bytes"
    );
}

#[test]
fn ra_14_a_7_and_c_auxiliary_original_hash_binding() {
    let mut checked = 0;
    for record in records() {
        let raw = hex::decode(record.cbor).unwrap();
        let mut tx = transaction::decode(&raw, &record.tx_hash).unwrap();
        if tx.auxiliary.is_some() {
            dsa_chain::metadata::checked(&tx, None).unwrap();
            checked += 1;
            tx.auxiliary_hash = Some("00".repeat(32));
            assert_eq!(
                dsa_chain::metadata::checked(&tx, None).unwrap_err().code,
                "provider_metadata_conflict"
            );
        }
    }
    assert!(checked > 0);
}
