//! Offline comparison of finalized provider recordings and restored SQL rows.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{File, OpenOptions},
    io::Write,
    path::Path,
};

use dsa_chain::{
    block::{
        DatumAvailability, Held, Outcome, Rows, Selection, SupplementRequest, Supplements,
        process_block,
    },
    profiles::Profile,
    transaction::{self, Asset, Outpoint, Output},
};
use dsa_provider::types::TransactionCbor;
use serde::Serialize;

#[path = "origins.rs"]
pub(crate) mod origins;
#[path = "recorded.rs"]
mod recorded;
use pallas_primitives::conway::MintedDatumOption;
use pallas_traverse::{MultiEraAsset, MultiEraInput, MultiEraOutput, MultiEraTx};
use serde::{Deserialize, de::DeserializeOwned};

#[derive(Deserialize)]
struct Plan {
    network: String,
    blocks: Vec<Block>,
}
#[derive(Deserialize)]
struct Block {
    height: u32,
    tx_count: usize,
}

/// A mint row as the legacy tables export it; the store keeps no mint rows,
/// so the replay derives them from the decoded transactions.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MintRow {
    pub tx_hash: String,
    pub policy: String,
    pub name: String,
    pub quantity: String,
}

#[derive(Deserialize)]
struct ExpectedMints {
    mint: Vec<MintRow>,
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
    effective: bool,
) -> Result<Option<&'a Output>, &'static str> {
    let resolved = output.filter(|o| o.address.is_some()).or_else(|| {
        supplements
            .outputs
            .get(hash)
            .and_then(|outputs| outputs.get(&u16::try_from(index).unwrap()))
    });
    if resolved.is_none() && effective {
        return Err("effective_output_comparison_unavailable");
    }
    Ok(resolved)
}

fn compare_output(ours: &Output, theirs: &MultiEraOutput<'_>) {
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

/// Fixed-unit mint effects must equal the independently exported mint rows.
fn check_mint_delta(
    profile: &Profile,
    hashes: &BTreeSet<&str>,
    actual: &BTreeMap<String, i128>,
    expected: &[MintRow],
) {
    let fixed = dsa_chain::assets::fixed_units(profile);
    let mut delta = BTreeMap::new();
    for mint in expected
        .iter()
        .filter(|mint| hashes.contains(mint.tx_hash.as_str()))
    {
        let unit = format!("{}{}", mint.policy, mint.name);
        if fixed.contains(&unit) {
            *delta.entry(unit).or_insert(0i128) += mint.quantity.parse::<i128>().unwrap();
        }
    }
    assert_eq!(
        actual, &delta,
        "fixed-unit effects vs independently exported SQL"
    );
}

fn compare(path: &Path, planning: bool) {
    let plan: Plan = read(path.join("recording-plan.json"));
    let profile = Profile::load(&plan.network).unwrap();
    let selection = Selection::new(&profile);
    let provider_path = path.join("provider");
    let mut expected: Rows = read(path.join("expected.json"));
    let mut expected_mints = read::<ExpectedMints>(path.join("expected.json")).mint;
    let mut actual_mints = Vec::new();
    let projected = plan.blocks.iter().map(|block| block.height).collect();
    let mut held: BTreeMap<Outpoint, Held> =
        origins::load(&path.join("origins.json"), &projected).unwrap();
    let eligible: BTreeSet<_> = expected
        .datum
        .iter()
        .map(|datum| datum.hash.clone())
        .collect();
    let mut known_datums = BTreeSet::<String>::new();
    let mut preimage_sources = BTreeMap::<String, BTreeSet<String>>::new();
    let mut datum_bytes = BTreeMap::<String, usize>::new();
    let mut large_datums = BTreeMap::new();
    let mut missing = BTreeSet::new();
    let mut request_trace = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path.join("supplement-requests.jsonl"))
        .unwrap();
    let mut actual = Rows::default();
    let mut shape_witnesses = BTreeMap::new();
    let mut multi_input_transactions = 0usize;
    let mut skipped_non_effective_collateral_outputs = 0usize;
    let mut skipped_non_effective_ordinary_outputs = 0usize;
    let mut decoder_eras = BTreeMap::<String, usize>::new();
    let (mut invalid_transactions, mut byron_outputs, mut pointer_outputs) =
        (0usize, 0usize, 0usize);
    let (mut decoded, mut compared, mut pallas_unavailable) = (0usize, 0usize, 0usize);
    let (mut collateral_outputs_compared, mut fallback_outputs_compared) = (0usize, 0usize);
    for block in &plan.blocks {
        let records: Vec<TransactionCbor> =
            read(provider_path.join(format!("transactions/{}.json", block.height)));
        assert_eq!(records.len(), block.tx_count);
        let raw: Vec<&[u8]> = records.iter().map(|r| r.cbor.0.as_slice()).collect();
        let transactions = transaction::decode_all(
            records
                .iter()
                .zip(&raw)
                .map(|(record, bytes)| (record.tx_hash.as_str(), *bytes)),
        )
        .unwrap();
        let transactions = transactions.as_slice();
        let provider: dsa_provider::types::Header =
            read(provider_path.join(format!("headers/{}.json", block.height)));
        let header = dsa_chain::block::Header::from(&provider);
        assert_eq!(header.height, block.height);
        let mut supplements = Supplements::default();
        supplements.datums.extend(
            known_datums
                .iter()
                .map(|hash| (hash.clone(), DatumAvailability::Stored)),
        );
        let mut requests = BTreeSet::new();
        let output = loop {
            let outcome = process_block(&selection, &header, transactions, &held, &supplements)
                .unwrap_or_else(|error| panic!("block failed at {}: {error}", block.height));
            let needs = match outcome {
                Outcome::Ready(output) => break output,
                Outcome::Needs(needs) => needs,
            };
            for request in needs {
                assert!(
                    requests.insert(serde_json::to_string(&request).unwrap()),
                    "cached supplement did not satisfy repeated request at block {}: {request:?}",
                    block.height
                );
                if let Some(source) = recorded::load(
                    &provider_path,
                    &request,
                    block.height,
                    transactions,
                    &mut supplements,
                )
                .unwrap()
                {
                    serde_json::to_writer(&mut request_trace,&serde_json::json!({"block":block.height,"request":request,"recording":source})).unwrap();
                    request_trace.write_all(b"\n").unwrap();
                    request_trace.flush().unwrap();
                } else {
                    missing.insert(
                        serde_json::to_string(
                            &serde_json::json!({"block":block.height,"request":request}),
                        )
                        .unwrap(),
                    );
                    serde_json::to_writer_pretty(
                        File::create(path.join("missing-supplements.json")).unwrap(),
                        &missing,
                    )
                    .unwrap();
                    match request {
                        SupplementRequest::Datum(hash) if planning => {
                            supplements.datums.insert(hash, DatumAvailability::NotFound);
                        }
                        _ => panic!("required supplement is unrecorded: {request:?}"),
                    }
                }
            }
        };
        let hashes: BTreeSet<_> = transactions.iter().map(|tx| tx.hash()).collect();
        check_mint_delta(&profile, &hashes, &output.mint_delta, &expected_mints);
        for (point, entry) in &output.created {
            held.insert(point.clone(), entry.clone());
        }
        for point in &output.spent {
            held.remove(point);
        }
        for tx in transactions.iter().filter(|tx| tx.is_valid()) {
            for asset in tx.mint().iter().filter(|asset| selection.required(asset)) {
                actual_mints.push(MintRow {
                    tx_hash: tx.hash().to_owned(),
                    policy: asset.policy.clone(),
                    name: asset.name.clone(),
                    quantity: asset.quantity.to_string(),
                });
            }
        }
        let rows = output.rows;
        for tx in transactions {
            for (hash, bytes) in tx.datums() {
                preimage_sources
                    .entry(hash.clone())
                    .or_default()
                    .insert("witness".to_owned());
                datum_bytes.insert(hash.clone(), bytes.len());
                if bytes.len() > 10000 {
                    large_datums.insert(
                        hash.clone(),
                        large_datum_observation(bytes, "witness", Some(block.height)),
                    );
                }
            }
            for output in tx.outputs().iter().flatten().chain(tx.collateral_return()) {
                if let (Some(hash), Some(bytes)) = (&output.data_hash, &output.inline_datum) {
                    preimage_sources
                        .entry(hash.clone())
                        .or_default()
                        .insert("inline".to_owned());
                    datum_bytes.insert(hash.clone(), bytes.len());
                    if bytes.len() > 10000 {
                        large_datums.insert(
                            hash.clone(),
                            large_datum_observation(bytes, "inline", Some(block.height)),
                        );
                    }
                }
            }
        }
        for (hash, availability) in &supplements.datums {
            if let DatumAvailability::Present(bytes) = availability {
                preimage_sources
                    .entry(hash.clone())
                    .or_default()
                    .insert("datum_api".to_owned());
                datum_bytes.insert(hash.clone(), bytes.len());
                if bytes.len() > 10000 {
                    large_datums.insert(
                        hash.clone(),
                        large_datum_observation(bytes, "datum_api", Some(block.height)),
                    );
                }
            }
        }
        known_datums.extend(rows.datum.iter().map(|datum| datum.hash.clone()));
        for (tx, raw) in transactions.iter().zip(&raw) {
            decoded += 1;
            multi_input_transactions += usize::from(tx.inputs().len() > 1);
            if !tx.is_valid() {
                invalid_transactions += 1;
                shape_witnesses.entry("phase2_invalid").or_insert_with(
                    || serde_json::json!({"block":block.height,"transaction":tx.hash()}),
                );
            }
            for output in tx.outputs().iter().flatten().chain(tx.collateral_return()) {
                match output.address_raw.first().map(|h| h >> 4) {
                    Some(8) => {
                        byron_outputs += 1;
                        shape_witnesses.entry("byron_output").or_insert_with(
                            || serde_json::json!({"block":block.height,"transaction":tx.hash()}),
                        );
                    }
                    Some(4 | 5) => {
                        pointer_outputs += 1;
                        shape_witnesses.entry("pointer_output").or_insert_with(
                            || serde_json::json!({"block":block.height,"transaction":tx.hash()}),
                        );
                    }
                    _ => {}
                }
            }
            match MultiEraTx::decode(raw) {
                Ok(other) => {
                    compared += 1;
                    *decoder_eras
                        .entry(format!("{:?}", other.era()))
                        .or_default() += 1;
                    assert_eq!(tx.hash(), other.hash().to_string());
                    assert_eq!(tx.is_valid(), other.is_valid());
                    assert_eq!(tx.inputs(), points(other.inputs()));
                    assert_eq!(tx.collateral(), points(other.collateral()));
                    let witness_datums: BTreeMap<_, _> = other
                        .plutus_data()
                        .iter()
                        .map(|datum| {
                            let bytes = datum.raw_cbor();
                            (transaction::hash(bytes), bytes.to_vec())
                        })
                        .collect();
                    let datums: BTreeMap<_, _> = tx
                        .datums()
                        .iter()
                        .map(|(hash, bytes)| (hash.clone(), bytes.to_vec()))
                        .collect();
                    assert_eq!(datums, witness_datums, "original witness datum bytes");
                    assert_eq!(
                        tx.mint(),
                        assets(other.mints().iter().flat_map(|p| p.assets()).collect())
                    );
                    let outputs = other.outputs();
                    assert_eq!(tx.outputs().len(), outputs.len());
                    for (index, (ours, theirs)) in tx.outputs().iter().zip(outputs).enumerate() {
                        match resolved_output(
                            ours.as_ref(),
                            index,
                            tx.hash(),
                            &supplements,
                            tx.is_valid(),
                        )
                        .expect("effective ordinary output must be independently compared")
                        {
                            Some(ours) => {
                                fallback_outputs_compared += usize::from(
                                    tx.outputs()[index]
                                        .as_ref()
                                        .is_none_or(|o| o.address.is_none()),
                                );
                                compare_output(ours, &theirs);
                            }
                            None => {
                                skipped_non_effective_ordinary_outputs += 1;
                                shape_witnesses.entry("skipped_invalid_ordinary_output").or_insert_with(||serde_json::json!({"block":block.height,"transaction":tx.hash(),"index":index,"reason":"unresolved output has no chain effect"}));
                            }
                        }
                    }
                    assert_eq!(
                        tx.has_collateral_return(),
                        other.collateral_return().is_some()
                    );
                    if let Some(theirs) = other.collateral_return() {
                        match resolved_output(
                            tx.collateral_return(),
                            tx.outputs().len(),
                            tx.hash(),
                            &supplements,
                            !tx.is_valid(),
                        )
                        .expect("effective collateral return must be independently compared")
                        {
                            Some(ours) => {
                                collateral_outputs_compared += 1;
                                fallback_outputs_compared += usize::from(
                                    tx.collateral_return().is_none_or(|o| o.address.is_none()),
                                );
                                compare_output(ours, &theirs);
                            }
                            None => {
                                skipped_non_effective_collateral_outputs += 1;
                                shape_witnesses.entry("skipped_valid_collateral_return").or_insert_with(||serde_json::json!({"block":block.height,"transaction":tx.hash(),"index":tx.outputs().len(),"reason":"unresolved return has no chain effect"}));
                            }
                        }
                        shape_witnesses.entry("collateral_return").or_insert_with(||serde_json::json!({"block":block.height,"transaction":tx.hash(),"valid":tx.is_valid()}));
                    }
                }
                Err(_) => pallas_unavailable += 1,
            }
        }
        actual.tx.extend(rows.tx);
        actual.tx_out.extend(rows.tx_out);
        actual.ma_tx_out.extend(rows.ma_tx_out);
        actual.tx_in.extend(rows.tx_in);
        actual.datum.extend(rows.datum);
        actual.tx_metadata.extend(rows.tx_metadata);
    }
    actual.tx.sort_by_key(|r| r.id);
    actual.tx_out.sort_by_key(|r| r.id);
    actual.ma_tx_out.sort_by_key(|r| r.id);
    actual.tx_in.sort_by_key(|r| r.id);
    actual.tx_metadata.sort_by_key(|r| r.id);
    for mints in [&mut actual_mints, &mut expected_mints] {
        mints.sort_by(|a, b| {
            (&a.tx_hash, &a.policy, &a.name).cmp(&(&b.tx_hash, &b.policy, &b.name))
        });
    }
    actual.datum.sort_by(|a, b| a.hash.cmp(&b.hash));
    actual.datum.dedup_by(|a, b| {
        assert!(a.hash != b.hash || a.value == b.value);
        a.hash == b.hash
    });
    let mut source_counts = BTreeMap::<String, usize>::new();
    let coverage: Vec<_> = eligible
        .iter()
        .map(|hash| {
            let sources = preimage_sources.get(hash).cloned().unwrap_or_default();
            for source in &sources {
                *source_counts.entry(source.clone()).or_default() += 1;
            }
            serde_json::json!({
                "legacy_sql_hash": hash,
                "raw_byte_length": datum_bytes.get(hash),
                "hash_verified": datum_bytes.contains_key(hash),
                "observed_sources": sources,
            })
        })
        .collect();
    serde_json::to_writer_pretty(
        File::create(path.join("datum-byte-coverage.json")).unwrap(),
        &serde_json::json!({
            "eligible_unique_hashes": eligible.len(),
            "observed_source_hash_counts": source_counts,
            "source_counts_overlap": true,
            "per_hash": coverage,
            "large_datums": large_datums,
            "availability": "current transaction, explicit API response, or earlier successfully stored datum",
            "request_trace": "supplement-requests.jsonl",
        }),
    ).unwrap();
    if planning {
        serde_json::to_writer_pretty(
            File::create(path.join("missing-supplements.json")).unwrap(),
            &missing,
        )
        .unwrap();
        println!(
            "{}",
            serde_json::json!({"result":"planning_only","missing_unique":missing.len(),"missing":missing})
        );
        return;
    }
    assert!(
        decoded > 0,
        "recorded corpus must exercise transaction decoding"
    );
    assert_eq!(
        pallas_unavailable, 0,
        "every recorded transaction must be compared or explicitly qualified"
    );
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
            serde_json::to_value(&actual_mints).unwrap(),
            serde_json::to_value(&expected_mints).unwrap(),
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
    let summary = serde_json::json!({
        "network": plan.network,
        "asset_rule_version": dsa_chain::assets::RULE_VERSION,
        "blocks": plan.blocks.len(),
        "decoded_transactions": decoded,
        "pallas_compared": compared,
        "pallas_unavailable": pallas_unavailable,
        "comparison_decoder_eras": decoder_eras,
        "skipped_non_effective_collateral_outputs": skipped_non_effective_collateral_outputs,
        "skipped_non_effective_ordinary_outputs": skipped_non_effective_ordinary_outputs,
        "collateral_outputs_compared": collateral_outputs_compared,
        "fallback_outputs_compared": fallback_outputs_compared,
        "invalid_transactions": invalid_transactions,
        "multi_input_transactions": multi_input_transactions,
        "byron_outputs": byron_outputs,
        "pointer_outputs": pointer_outputs,
        "tx": actual.tx.len(),
        "outputs": actual.tx_out.len(),
        "assets": actual.ma_tx_out.len(),
        "spends": actual.tx_in.len(),
        "datums": actual.datum.len(),
        "metadata": actual.tx_metadata.len(),
        "mints": actual_mints.len(),
        "differing_tables": differences,
        "result": if differences.is_empty() { "equal" } else { "different" },
    });
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

pub fn run_mode(path: &Path, planning: bool) {
    let path = path.to_path_buf();
    std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(move || compare(&path, planning))
        .unwrap()
        .join()
        .unwrap();
}

pub fn run(path: &Path) {
    run_mode(path, false);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unresolved_non_effective_return_is_reportable_while_effective_output_is_required() {
        let supplements = Supplements::default();
        assert!(
            resolved_output(None, 3, "unused", &supplements, false)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            resolved_output(None, 3, "effective", &supplements, true).unwrap_err(),
            "effective_output_comparison_unavailable"
        );
    }
}
