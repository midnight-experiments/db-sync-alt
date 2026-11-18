//! Offline comparison of finalized provider recordings and restored SQL rows.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{File, OpenOptions},
    io::Write,
    path::Path,
};

use dsa_chain::{
    profiles::Profile,
    rows::{self, BuildError, DatumAvailability, Rows, SupplementRequest, Supplements},
    transaction::{self, Asset, Outpoint, Output},
};
use dsa_provider::types::TransactionCbor;

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
    let resolved = output.filter(|o| o.classification_safe).or_else(|| {
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

fn compare(path: &Path, planning: bool) {
    let plan: Plan = read(path.join("recording-plan.json"));
    let profile = Profile::load(&plan.network).unwrap();
    let provider_path = path.join("provider");
    let mut expected: Rows = read(path.join("expected.json"));
    let projected = plan.blocks.iter().map(|block| block.height).collect();
    let mut held = origins::load(&path.join("origins.json"), &projected).unwrap();
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
        let raw: Vec<_> = records
            .iter()
            .map(|r| hex::decode(r.cbor.as_str()).unwrap())
            .collect();
        let transactions: Vec<_> = records
            .iter()
            .zip(&raw)
            .map(|(r, b)| transaction::decode(b, r.tx_hash.as_str()).unwrap())
            .collect();
        let mut supplements = Supplements::default();
        supplements.datums.extend(
            known_datums
                .iter()
                .map(|hash| (hash.clone(), DatumAvailability::Stored)),
        );
        let mut requests = BTreeSet::new();
        let rows = loop {
            match rows::build_block(
                &profile,
                block.height,
                &transactions,
                &mut held,
                &supplements,
            ) {
                Ok(rows) => break rows,
                Err(BuildError::Required(request)) => {
                    assert!(
                        requests.insert(serde_json::to_string(&request).unwrap()),
                        "cached supplement did not satisfy repeated request at block {}: {request:?}",
                        block.height
                    );
                    if let Some(source) = recorded::load(
                        &provider_path,
                        &request,
                        block.height,
                        &transactions,
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
                        if planning && let SupplementRequest::Datum(hash) = request {
                            supplements.datums.insert(hash, DatumAvailability::NotFound);
                            continue;
                        }
                        panic!("required supplement is unrecorded: {request:?}");
                    }
                }
                Err(error) => panic!("row building failed at {}: {error}", block.height),
            }
        };
        for tx in &transactions {
            for (hash, bytes) in &tx.datums {
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
            for output in tx
                .outputs
                .iter()
                .flatten()
                .chain(tx.collateral_output.iter())
            {
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
            multi_input_transactions += usize::from(tx.ordinary.len() > 1);
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
                    *decoder_eras
                        .entry(format!("{:?}", other.era()))
                        .or_default() += 1;
                    assert_eq!(tx.hash, other.hash().to_string());
                    assert_eq!(tx.valid, other.is_valid());
                    assert_eq!(tx.ordinary, points(other.inputs()));
                    assert_eq!(tx.collateral, points(other.collateral()));
                    assert_eq!(tx.reference, points(other.reference_inputs()));
                    let witness_datums: BTreeMap<_, _> = other
                        .plutus_data()
                        .iter()
                        .map(|datum| {
                            let bytes = datum.raw_cbor();
                            (transaction::hash(bytes), bytes.to_vec())
                        })
                        .collect();
                    assert_eq!(tx.datums, witness_datums, "original witness datum bytes");
                    assert_eq!(
                        tx.mint,
                        assets(other.mints().iter().flat_map(|p| p.assets()).collect())
                    );
                    let outputs = other.outputs();
                    assert_eq!(tx.outputs.len(), outputs.len());
                    for (index, (ours, theirs)) in tx.outputs.iter().zip(outputs).enumerate() {
                        match resolved_output(
                            ours.as_ref(),
                            index,
                            &tx.hash,
                            &supplements,
                            tx.valid,
                        )
                        .expect("effective ordinary output must be independently compared")
                        {
                            Some(ours) => {
                                fallback_outputs_compared += usize::from(
                                    tx.outputs[index]
                                        .as_ref()
                                        .is_none_or(|o| !o.classification_safe),
                                );
                                compare_output(ours, &theirs);
                            }
                            None => {
                                skipped_non_effective_ordinary_outputs += 1;
                                shape_witnesses.entry("skipped_invalid_ordinary_output").or_insert_with(||serde_json::json!({"block":block.height,"transaction":tx.hash,"index":index,"reason":"unresolved output has no chain effect"}));
                            }
                        }
                    }
                    assert_eq!(
                        tx.has_collateral_return,
                        other.collateral_return().is_some()
                    );
                    if let Some(theirs) = other.collateral_return() {
                        match resolved_output(
                            tx.collateral_output.as_ref(),
                            tx.outputs.len(),
                            &tx.hash,
                            &supplements,
                            !tx.valid,
                        )
                        .expect("effective collateral return must be independently compared")
                        {
                            Some(ours) => {
                                collateral_outputs_compared += 1;
                                fallback_outputs_compared += usize::from(
                                    tx.collateral_output
                                        .as_ref()
                                        .is_none_or(|o| !o.classification_safe),
                                );
                                compare_output(ours, &theirs);
                            }
                            None => {
                                skipped_non_effective_collateral_outputs += 1;
                                shape_witnesses.entry("skipped_valid_collateral_return").or_insert_with(||serde_json::json!({"block":block.height,"transaction":tx.hash,"index":tx.outputs.len(),"reason":"unresolved return has no chain effect"}));
                            }
                        }
                        shape_witnesses.entry("collateral_return").or_insert_with(||serde_json::json!({"block":block.height,"transaction":tx.hash,"valid":tx.valid}));
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
        "mints": actual.mint.len(),
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
