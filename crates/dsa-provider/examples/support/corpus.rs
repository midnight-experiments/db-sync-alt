//! Offline comparison of finalized provider recordings and restored SQL rows.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    path::Path,
};

use dsa_chain::{
    profiles::Profile,
    rows::{self, HeldOutput, Rows, Supplements},
    transaction::{self, Asset, Outpoint, Output},
};
use dsa_provider::types::{CborHex, Metadata, TransactionCbor, TransactionOutputs};
use pallas_primitives::conway::MintedDatumOption;
use pallas_traverse::{MultiEraAsset, MultiEraInput, MultiEraOutput, MultiEraTx};
use serde::{Deserialize, de::DeserializeOwned};

#[derive(Deserialize)]
struct Plan {
    network: String,
    blocks: Vec<Block>,
    datum_hashes: Vec<String>,
}
#[derive(Deserialize)]
struct Block {
    height: u32,
    tx_count: usize,
}
#[derive(Deserialize)]
struct Origins {
    outputs: Vec<Origin>,
    assets: Vec<OriginAsset>,
}
#[derive(Deserialize)]
struct Origin {
    tx_id: i64,
    index: u16,
    hash: String,
    address: String,
    value: String,
    data_hash: Option<String>,
}
#[derive(Deserialize)]
struct OriginAsset {
    tx_out_id: i64,
    policy: String,
    name: String,
    quantity: String,
}

fn read<T: DeserializeOwned>(path: impl AsRef<Path>) -> T {
    serde_json::from_reader(File::open(path).expect("corpus file")).expect("corpus JSON")
}

fn points(inputs: Vec<MultiEraInput<'_>>) -> Vec<Outpoint> {
    let mut result: Vec<_> = inputs
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

fn resolved_output<'a>(
    output: Option<&'a Output>,
    index: usize,
    hash: &str,
    supplements: &'a Supplements,
) -> &'a Output {
    output
        .filter(|o| o.classification_safe)
        .or_else(|| {
            supplements
                .outputs
                .get(hash)
                .and_then(|outputs| outputs.get(&u16::try_from(index).unwrap()))
        })
        .expect("recorded output fallback required for comparison")
}

fn compare_output(ours: &Output, theirs: &MultiEraOutput<'_>) {
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

fn normalize_json(text: &str) -> serde_json::Value {
    let mut decoder = serde_json::Deserializer::from_str(text);
    decoder.disable_recursion_limit();
    let value = serde_json::Value::deserialize(&mut decoder).expect("recorded SQL jsonb");
    decoder.end().unwrap();
    value
}

fn large_datum_observation(bytes: &[u8], source: &str, height: Option<u32>) -> serde_json::Value {
    let rendered = dsa_chain::datum::render(bytes);
    serde_json::json!({
        "raw_byte_length": bytes.len(),
        "source": source,
        "block": height,
        "local_detailed_schema_json_text": rendered.as_ref().ok(),
        "local_render_error": rendered.as_ref().err().map(|e| e.code),
    })
}

fn compare(path: &Path) {
    let plan: Plan = read(path.join("recording-plan.json"));
    let profile = Profile::load(&plan.network).unwrap();
    let provider_path = path.join("provider");
    let mut expected: Rows = read(path.join("expected.json"));
    let origins: Origins = read(path.join("origins.json"));
    let creating: BTreeSet<_> = expected.tx.iter().map(|t| t.id).collect();
    let mut held = BTreeMap::new();
    for origin in origins.outputs {
        if creating.contains(&origin.tx_id) {
            continue;
        }
        let output_id = (origin.tx_id << 12) | i64::from(origin.index);
        let assets = origins
            .assets
            .iter()
            .filter(|a| a.tx_out_id == output_id)
            .map(|a| Asset {
                policy: a.policy.clone(),
                name: a.name.clone(),
                quantity: a.quantity.parse().unwrap(),
            })
            .collect();
        let address_raw = Vec::new();
        held.insert(
            Outpoint {
                hash: origin.hash,
                index: origin.index,
            },
            HeldOutput {
                transaction_id: origin.tx_id,
                output: Output {
                    address_raw,
                    address: Some(origin.address),
                    coin: origin.value.parse().unwrap(),
                    assets,
                    data_hash: origin.data_hash,
                    inline_datum: None,
                    classification_safe: true,
                },
            },
        );
    }
    let eligible: BTreeSet<_> = plan.datum_hashes.iter().cloned().collect();
    let mut preimage_sources: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut large_datums = BTreeMap::new();
    let mut supplements = Supplements::default();
    for entry in std::fs::read_dir(provider_path.join("datums")).unwrap() {
        let entry = entry.unwrap();
        let bytes: Option<CborHex> = read(entry.path());
        if let Some(bytes) = bytes {
            let raw = hex::decode(bytes.as_str()).unwrap();
            let hash = transaction::hash(&raw);
            assert_eq!(entry.path().file_stem().unwrap().to_str().unwrap(), hash);
            if raw.len() > 10000 {
                large_datums.insert(
                    hash.clone(),
                    large_datum_observation(&raw, "datum_api", None),
                );
            }
            preimage_sources
                .entry(hash.clone())
                .or_default()
                .insert("datum_api".to_owned());
            supplements.datums.insert(hash, raw);
        }
    }
    for entry in std::fs::read_dir(provider_path.join("metadata")).unwrap() {
        let entry = entry.unwrap();
        let values: Vec<Metadata> = read(entry.path());
        let matches: Vec<_> = values
            .into_iter()
            .filter(|m| m.label.0 == 6500973)
            .collect();
        assert!(matches.len() <= 1, "duplicate bridge metadata label");
        supplements.metadata.insert(
            entry
                .path()
                .file_stem()
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned(),
            matches.first().map(|m| m.json_metadata.get().to_owned()),
        );
    }
    for entry in std::fs::read_dir(provider_path.join("outputs")).unwrap() {
        let entry = entry.unwrap();
        let values: TransactionOutputs = read(entry.path());
        let mut outputs = BTreeMap::new();
        for output in values.outputs {
            let mut coin = None;
            let mut assets = Vec::new();
            for amount in output.amount {
                if amount.unit == "lovelace" {
                    coin = Some(amount.quantity.0);
                } else {
                    assets.push(Asset {
                        policy: amount.unit[..56].to_owned(),
                        name: amount.unit[56..].to_owned(),
                        quantity: i128::from(amount.quantity.0),
                    });
                }
            }
            assets.sort_by(|a, b| (&a.policy, &a.name).cmp(&(&b.policy, &b.name)));
            outputs.insert(
                u16::try_from(output.output_index).unwrap(),
                Output {
                    address_raw: Vec::new(),
                    address: Some(output.address),
                    coin: coin.expect("lovelace"),
                    assets,
                    data_hash: output.data_hash.map(|h| h.to_string()),
                    inline_datum: output
                        .inline_datum
                        .map(|b| hex::decode(b.as_str()).unwrap()),
                    classification_safe: true,
                },
            );
        }
        supplements.outputs.insert(values.hash.to_string(), outputs);
    }
    // A local corpus is already complete. Inline/witness bytes from its real
    // finalized transactions can supply the same preimages as the datum API.
    let mut support_paths: Vec<_> = std::fs::read_dir(provider_path.join("transactions"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    support_paths.sort();
    for support_path in support_paths {
        let support_height: u32 = support_path
            .file_stem()
            .unwrap()
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        let records: Vec<TransactionCbor> = read(&support_path);
        for record in records {
            let raw = hex::decode(record.cbor.as_str()).unwrap();
            let tx = transaction::decode(&raw, record.tx_hash.as_str()).unwrap();
            for (hash, bytes) in tx.datums {
                if bytes.len() > 10000 {
                    large_datums.insert(
                        hash.clone(),
                        large_datum_observation(&bytes, "witness", Some(support_height)),
                    );
                }
                if eligible.contains(&hash) {
                    assert_eq!(transaction::hash(&bytes), hash);
                    preimage_sources
                        .entry(hash.clone())
                        .or_default()
                        .insert("witness".to_owned());
                    supplements.datums.insert(hash, bytes);
                }
            }
            for output in tx.outputs.into_iter().flatten().chain(tx.collateral_output) {
                if let (Some(hash), Some(bytes)) = (output.data_hash, output.inline_datum) {
                    if bytes.len() > 10000 {
                        large_datums.insert(
                            hash.clone(),
                            large_datum_observation(&bytes, "inline", Some(support_height)),
                        );
                    }
                    if eligible.contains(&hash) {
                        assert_eq!(transaction::hash(&bytes), hash);
                        preimage_sources
                            .entry(hash.clone())
                            .or_default()
                            .insert("inline".to_owned());
                        supplements.datums.insert(hash, bytes);
                    }
                }
            }
        }
    }
    let mut coverage = Vec::new();
    let mut source_counts = BTreeMap::<String, usize>::new();
    for hash in &eligible {
        let bytes = supplements.datums.get(hash);
        let sources = preimage_sources.get(hash).cloned().unwrap_or_default();
        let chosen = ["inline", "witness", "datum_api"]
            .into_iter()
            .find(|s| sources.contains(*s))
            .unwrap_or("missing");
        *source_counts.entry(chosen.to_owned()).or_default() += 1;
        if let Some(bytes) = bytes {
            assert_eq!(
                transaction::hash(bytes),
                *hash,
                "raw preimage versus legacy SQL hash"
            );
            if let Some(sql) = expected.datum.iter().find(|d| d.hash == *hash)
                && sql.value.as_ref().is_some_and(|value| value.len() > 10000)
            {
                large_datums.entry(hash.clone()).or_insert_with(|| {
                    serde_json::json!({
                        "raw_byte_length":bytes.len(),
                        "source":chosen,
                        "sql_jsonb_candidate":true,
                    })
                });
            }
        }
        coverage.push(serde_json::json!({"legacy_sql_hash":hash,"raw_byte_length":bytes.map(Vec::len),"hash_verified":bytes.is_some(),"available_sources":sources,"chosen_source":chosen}));
    }
    for (hash, value) in &mut large_datums {
        if let Some(sql) = expected.datum.iter().find(|d| d.hash == *hash) {
            value["legacy_jsonb_text"] = serde_json::json!(sql.value);
            value["jsonb_oracle"] = serde_json::json!("restored SQL");
        } else {
            value["jsonb_oracle"] = serde_json::json!(
                "unavailable: not an eligible stored datum in this SQL projection"
            );
        }
    }
    serde_json::to_writer_pretty(File::create(path.join("datum-byte-coverage.json")).unwrap(),&serde_json::json!({"eligible_unique_hashes":eligible.len(),"chosen_source_counts":source_counts,"per_hash":coverage,"large_datums":large_datums})).unwrap();
    let mut actual = Rows::default();
    let mut shape_witnesses = BTreeMap::new();
    let (mut invalid_transactions, mut byron_outputs, mut pointer_outputs) =
        (0usize, 0usize, 0usize);
    let (mut decoded, mut compared, mut pallas_unavailable) = (0usize, 0usize, 0usize);
    let (mut collateral_outputs_compared, mut fallback_outputs_compared) = (0usize, 0usize);
    for block in &plan.blocks {
        let records: Vec<TransactionCbor> =
            read(provider_path.join(format!("transactions/{}.json", block.height)));
        assert_eq!(records.len(), block.tx_count);
        let raw: Vec<_> = records
            .iter()
            .map(|r| hex::decode(r.cbor.as_str()).unwrap())
            .collect();
        let transactions: Vec<_> = records
            .iter()
            .zip(&raw)
            .map(|(r, b)| transaction::decode(b, r.tx_hash.as_str()).unwrap())
            .collect();
        for (tx, raw) in transactions.iter().zip(&raw) {
            decoded += 1;
            if !tx.valid {
                invalid_transactions += 1;
                shape_witnesses.entry("phase2_invalid").or_insert_with(
                    || serde_json::json!({"block":block.height,"transaction":tx.hash}),
                );
            }
            for output in tx
                .outputs
                .iter()
                .flatten()
                .chain(tx.collateral_output.iter())
            {
                match output.address_raw.first().map(|h| h >> 4) {
                    Some(8) => {
                        byron_outputs += 1;
                        shape_witnesses.entry("byron_output").or_insert_with(
                            || serde_json::json!({"block":block.height,"transaction":tx.hash}),
                        );
                    }
                    Some(4 | 5) => {
                        pointer_outputs += 1;
                        shape_witnesses.entry("pointer_output").or_insert_with(
                            || serde_json::json!({"block":block.height,"transaction":tx.hash}),
                        );
                    }
                    _ => {}
                }
            }
            match MultiEraTx::decode(raw) {
                Ok(other) => {
                    compared += 1;
                    assert_eq!(tx.hash, other.hash().to_string());
                    assert_eq!(tx.valid, other.is_valid());
                    assert_eq!(tx.ordinary, points(other.inputs()));
                    assert_eq!(tx.collateral, points(other.collateral()));
                    assert_eq!(tx.reference, points(other.reference_inputs()));
                    assert_eq!(
                        tx.mint,
                        assets(other.mints().iter().flat_map(|p| p.assets()).collect())
                    );
                    let outputs = other.outputs();
                    assert_eq!(tx.outputs.len(), outputs.len());
                    for (index, (ours, theirs)) in tx.outputs.iter().zip(outputs).enumerate() {
                        fallback_outputs_compared +=
                            usize::from(ours.as_ref().is_none_or(|o| !o.classification_safe));
                        compare_output(
                            resolved_output(ours.as_ref(), index, &tx.hash, &supplements),
                            &theirs,
                        );
                    }
                    assert_eq!(
                        tx.has_collateral_return,
                        other.collateral_return().is_some()
                    );
                    if let Some(theirs) = other.collateral_return() {
                        collateral_outputs_compared += 1;
                        fallback_outputs_compared += usize::from(
                            tx.collateral_output
                                .as_ref()
                                .is_none_or(|o| !o.classification_safe),
                        );
                        compare_output(
                            resolved_output(
                                tx.collateral_output.as_ref(),
                                tx.outputs.len(),
                                &tx.hash,
                                &supplements,
                            ),
                            &theirs,
                        );
                        shape_witnesses.entry("collateral_return").or_insert_with(||serde_json::json!({"block":block.height,"transaction":tx.hash,"valid":tx.valid}));
                    }
                }
                Err(_) => pallas_unavailable += 1,
            }
        }
        let rows = rows::build_block(
            &profile,
            block.height,
            &transactions,
            &mut held,
            &supplements,
        )
        .unwrap();
        actual.tx.extend(rows.tx);
        actual.tx_out.extend(rows.tx_out);
        actual.ma_tx_out.extend(rows.ma_tx_out);
        actual.tx_in.extend(rows.tx_in);
        actual.datum.extend(rows.datum);
        actual.tx_metadata.extend(rows.tx_metadata);
        actual.mint.extend(rows.mint);
    }
    actual.tx.sort_by_key(|r| r.id);
    actual.tx_out.sort_by_key(|r| r.id);
    actual.ma_tx_out.sort_by_key(|r| r.id);
    actual.tx_in.sort_by_key(|r| r.id);
    actual.tx_metadata.sort_by_key(|r| r.id);
    actual
        .mint
        .sort_by(|a, b| (&a.tx_hash, &a.policy, &a.name).cmp(&(&b.tx_hash, &b.policy, &b.name)));
    actual.datum.sort_by(|a, b| a.hash.cmp(&b.hash));
    actual.datum.dedup_by(|a, b| {
        assert!(a.hash != b.hash || a.value == b.value);
        a.hash == b.hash
    });
    let mut differences = Vec::new();
    for (table, ours, theirs) in [
        (
            "tx",
            serde_json::to_value(&actual.tx).unwrap(),
            serde_json::to_value(&expected.tx).unwrap(),
        ),
        (
            "tx_out",
            serde_json::to_value(&actual.tx_out).unwrap(),
            serde_json::to_value(&expected.tx_out).unwrap(),
        ),
        (
            "ma_tx_out",
            serde_json::to_value(&actual.ma_tx_out).unwrap(),
            serde_json::to_value(&expected.ma_tx_out).unwrap(),
        ),
        (
            "tx_in",
            serde_json::to_value(&actual.tx_in).unwrap(),
            serde_json::to_value(&expected.tx_in).unwrap(),
        ),
        (
            "mint",
            serde_json::to_value(&actual.mint).unwrap(),
            serde_json::to_value(&expected.mint).unwrap(),
        ),
    ] {
        if ours != theirs {
            serde_json::to_writer(
                File::create(path.join(format!("difference-{table}.json"))).unwrap(),
                &serde_json::json!({"actual":ours,"expected":theirs}),
            )
            .unwrap();
            differences.push(table.to_owned());
        }
    }
    expected.datum.sort_by(|a, b| a.hash.cmp(&b.hash));
    assert_eq!(actual.datum.len(), expected.datum.len(), "datum row count");
    for (a, b) in actual.datum.iter().zip(&expected.datum) {
        assert_eq!(a.hash, b.hash);
        assert_eq!(
            a.value.as_deref().map(normalize_json),
            b.value.as_deref().map(normalize_json),
            "datum jsonb {}",
            a.hash
        );
    }
    assert_eq!(actual.tx_metadata.len(), expected.tx_metadata.len());
    for (a, b) in actual.tx_metadata.iter().zip(&expected.tx_metadata) {
        assert_eq!((a.id, a.tx_id, a.key), (b.id, b.tx_id, b.key));
        assert_eq!(
            a.json.as_deref().map(normalize_json),
            b.json.as_deref().map(normalize_json)
        );
    }
    serde_json::to_writer_pretty(
        File::create(path.join("shape-witnesses.json")).unwrap(),
        &shape_witnesses,
    )
    .unwrap();
    let summary = serde_json::json!({"network":plan.network,"blocks":plan.blocks.len(),"decoded_transactions":decoded,"pallas_compared":compared,"pallas_unavailable":pallas_unavailable,"collateral_outputs_compared":collateral_outputs_compared,"fallback_outputs_compared":fallback_outputs_compared,"invalid_transactions":invalid_transactions,"byron_outputs":byron_outputs,"pointer_outputs":pointer_outputs,"tx":actual.tx.len(),"outputs":actual.tx_out.len(),"assets":actual.ma_tx_out.len(),"spends":actual.tx_in.len(),"datums":actual.datum.len(),"metadata":actual.tx_metadata.len(),"mints":actual.mint.len(),"differing_tables":differences,"result":if differences.is_empty(){"equal"}else{"different"}});
    serde_json::to_writer_pretty(
        File::create(path.join("comparison-result.json")).unwrap(),
        &summary,
    )
    .unwrap();
    println!("{summary}");
    assert!(
        differences.is_empty(),
        "restored SQL differences remain: {differences:?}"
    );
}

pub fn run(path: &Path) {
    let path = path.to_path_buf();
    std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(move || compare(&path))
        .unwrap()
        .join()
        .unwrap();
}
