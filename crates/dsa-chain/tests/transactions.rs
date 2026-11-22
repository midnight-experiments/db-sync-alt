use dsa_chain::transaction::{self, Asset, Outpoint};
use dsa_testkit::tx::{Output, Tx, enterprise_address, hash, large_datum};
use pallas_primitives::conway::MintedDatumOption;
use pallas_traverse::{MultiEraAsset, MultiEraInput, MultiEraOutput, MultiEraTx};
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

fn output(ours: &transaction::Output, theirs: &MultiEraOutput<'_>) {
    assert_eq!(ours.address_raw, theirs.address().unwrap().to_vec());
    assert_eq!(ours.coin, theirs.value().coin());
    assert_eq!(
        ours.address,
        transaction::address_text(&theirs.address().unwrap().to_vec())
    );
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
    let (hash, inline) = match theirs.datum() {
        None => (None, None),
        Some(MintedDatumOption::Hash(hash)) => (Some(hash.to_string()), None),
        Some(MintedDatumOption::Data(data)) => {
            let raw = data.0.raw_cbor();
            (Some(transaction::hash(raw)), Some(raw.to_vec()))
        }
    };
    assert_eq!(ours.data_hash, hash);
    assert_eq!(ours.inline_datum, inline);
}

#[test]
fn ra_14_a_and_c_real_original_bytes_inputs_outputs_mint_and_validity() {
    let mut compared = 0;
    let mut collateral_compared = 0;
    for record in records() {
        let raw = hex::decode(record.cbor).unwrap();
        let tx = transaction::decode(&raw, &record.tx_hash).unwrap();
        assert_eq!(tx.hash(), record.tx_hash);
        if let Ok(other) = MultiEraTx::decode(&raw) {
            compared += 1;
            assert_eq!(tx.hash(), other.hash().to_string());
            assert_eq!(tx.is_valid(), other.is_valid());
            assert_eq!(tx.inputs(), inputs(other.inputs()));
            assert_eq!(tx.collateral(), inputs(other.collateral()));
            let witness_datums: std::collections::BTreeMap<_, _> = other
                .plutus_data()
                .iter()
                .map(|datum| {
                    let bytes = datum.raw_cbor();
                    (transaction::hash(bytes), bytes.to_vec())
                })
                .collect();
            let datums: std::collections::BTreeMap<_, _> = tx
                .datums()
                .iter()
                .map(|(hash, bytes)| (hash.clone(), bytes.to_vec()))
                .collect();
            assert_eq!(datums, witness_datums);
            assert_eq!(
                tx.mint(),
                assets(other.mints().iter().flat_map(|p| p.assets()).collect())
            );
            let outputs = other.outputs();
            assert_eq!(tx.outputs().len(), outputs.len());
            for (ours, theirs) in tx.outputs().iter().zip(outputs) {
                let ours = ours.as_ref().expect("classified recorded output");
                output(ours, &theirs);
            }
            assert_eq!(
                tx.has_collateral_return(),
                other.collateral_return().is_some()
            );
            if let Some(theirs) = other.collateral_return() {
                collateral_compared += 1;
                output(
                    tx.collateral_return()
                        .expect("classified real collateral return"),
                    &theirs,
                );
            }
        }
    }
    assert_eq!(
        compared,
        records().len(),
        "every finalized record must compare"
    );
    println!(
        "real_finalized_transactions_compared={compared} collateral_returns_compared={collateral_compared}"
    );
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
        let mut raw = hex::decode(record.cbor).unwrap();
        let tx = transaction::decode(&raw, &record.tx_hash).unwrap();
        let Some(auxiliary) = tx.auxiliary() else {
            continue;
        };
        dsa_chain::metadata::checked(&tx, None).unwrap();
        checked += 1;
        // Other auxiliary data under the same body: the body hash still holds
        // and the auxiliary hash in the body no longer matches.
        let start = raw.len() - auxiliary.len();
        raw.truncate(start);
        raw.extend([0xa1, 0x01, 0x01]);
        let changed = transaction::decode(&raw, &record.tx_hash).unwrap();
        assert_eq!(
            dsa_chain::metadata::checked(&changed, None)
                .unwrap_err()
                .code,
            "provider_metadata_conflict"
        );
    }
    assert!(checked > 0);
}

/// Insert `key: 0` into a built transaction's body and return the CBOR and
/// the new body hash.
fn with_body_key(key: u8, remove_fee: bool) -> (Vec<u8>, String) {
    let tx = Tx::new()
        .input(&hash(b"input"), 0)
        .output(Output::new(enterprise_address(1, [1; 28]), 2_000_000));
    let body = tx.body();
    let built = tx.build();
    let rest = &built.cbor[1 + body.len()..];
    let count = body[0] & 0x1f;
    let mut fields = body[1..].to_vec();
    let mut header = 0xa0 | (count + 1);
    if remove_fee {
        let fee = fields
            .windows(5)
            .position(|window| window == [0x02, 0x1a, 0x00, 0x02, 0x98])
            .unwrap();
        fields.drain(fee..fee + 6);
        header = 0xa0 | count;
    }
    fields.extend([key, 0x00]);
    let mut new_body = vec![header];
    new_body.extend(fields);
    let mut cbor = vec![0x84];
    cbor.extend(&new_body);
    cbor.extend(rest);
    (cbor, hash(&new_body))
}

#[test]
fn a_later_body_field_and_an_oversized_transaction_are_named() {
    assert_eq!(transaction::LAST_BODY_KEY, 22);
    for key in [0x17, 0x0a, 0x0c] {
        let (cbor, body_hash) = with_body_key(key, false);
        assert_eq!(
            transaction::decode(&cbor, &body_hash).unwrap_err().code,
            "unsupported_transaction_body_field",
            "body key {key}"
        );
    }
    let (cbor, body_hash) = with_body_key(0x16, false);
    transaction::decode(&cbor, &body_hash).unwrap();
    let (cbor, body_hash) = with_body_key(0x16, true);
    assert_eq!(
        transaction::decode(&cbor, &body_hash).unwrap_err().code,
        "cbor_transaction_body_shape"
    );
    let oversized = Tx::new()
        .input(&hash(b"input"), 0)
        .output(Output::new(enterprise_address(1, [1; 28]), 2_000_000))
        .witness_datum(large_datum(transaction::TRANSACTION_SIZE_LIMIT))
        .build();
    assert!(oversized.cbor.len() > transaction::TRANSACTION_SIZE_LIMIT);
    assert_eq!(
        transaction::decode(&oversized.cbor, &oversized.hash)
            .unwrap_err()
            .code,
        "transaction_size_limit"
    );
}
