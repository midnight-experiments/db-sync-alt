//! Replay the public ledger subset against its independently exported SQL rows.

#[path = "support/corpus.rs"]
mod corpus;

use std::{
    collections::BTreeMap,
    io::Read,
    path::{Component, Path},
};

use flate2::read::GzDecoder;
use serde::Deserialize;
use sha2::{Digest, Sha256};

#[derive(Deserialize)]
struct Bundle {
    format: String,
    network: String,
    files: BTreeMap<String, String>,
    sha256: BTreeMap<String, String>,
}

fn replay(network: &str) -> (tempfile::TempDir, serde_json::Value) {
    assert!(["mainnet", "preprod"].contains(&network));
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(format!("tests/fixtures/{network}-corpus.json.gz"));
    let compressed = std::fs::read(fixture).expect("committed recorded subset");
    let manifest: serde_json::Value = serde_json::from_slice(
        &std::fs::read(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join(format!("tests/fixtures/{network}-corpus.manifest.json")),
        )
        .unwrap(),
    )
    .unwrap();
    let directory = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let combined = std::fs::metadata(directory.join("preprod-corpus.json.gz"))
        .unwrap()
        .len()
        + std::fs::metadata(directory.join("mainnet-corpus.json.gz"))
            .unwrap()
            .len()
        + std::fs::metadata(directory.join("pagination-finalized.json"))
            .unwrap()
            .len();
    assert!(
        combined <= 5 * 1024 * 1024,
        "combined recorded subset budget, conservatively including uncompressed pagination input"
    );
    assert_eq!(manifest["network"].as_str().unwrap(), network);
    assert_eq!(
        format!("{:x}", Sha256::digest(&compressed)),
        manifest["compressed_sha256"].as_str().unwrap()
    );
    assert_eq!(
        compressed.len() as u64,
        manifest["compressed_bytes"].as_u64().unwrap()
    );
    assert!(
        compressed.len() <= 5 * 1024 * 1024,
        "compressed corpus budget"
    );
    let mut raw = Vec::new();
    GzDecoder::new(compressed.as_slice())
        .take(64 * 1024 * 1024 + 1)
        .read_to_end(&mut raw)
        .unwrap();
    assert!(raw.len() <= 64 * 1024 * 1024, "fixture expansion budget");
    assert_eq!(
        raw.len() as u64,
        manifest["raw_bundle_bytes"].as_u64().unwrap()
    );
    let bundle: Bundle = serde_json::from_slice(&raw).unwrap();
    assert_eq!(
        bundle.files.len() as u64,
        manifest["files"].as_u64().unwrap()
    );
    assert_eq!(bundle.format, "recorded-corpus-v1");
    assert_eq!(bundle.network, network);
    assert_eq!(bundle.files.len(), bundle.sha256.len());
    let scratch = tempfile::tempdir().unwrap();
    for (name, content) in bundle.files {
        let path = Path::new(&name);
        assert!(path.components().all(|p| matches!(p, Component::Normal(_))));
        let allowed = ["recording-plan.json", "expected.json", "origins.json"]
            .contains(&name.as_str())
            || [
                "provider/headers/",
                "provider/transactions/",
                "provider/datums/",
                "provider/metadata/",
                "provider/outputs/",
            ]
            .iter()
            .any(|p| name.starts_with(p));
        assert!(
            allowed,
            "only ledger recordings and node-facing rows belong in the fixture"
        );
        assert_eq!(
            format!("{:x}", Sha256::digest(content.as_bytes())),
            bundle.sha256[&name]
        );
        let target = scratch.path().join(path);
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::write(target, content).unwrap();
    }
    for directory in ["headers", "transactions", "datums", "metadata", "outputs"] {
        std::fs::create_dir_all(scratch.path().join("provider").join(directory)).unwrap();
    }
    let plan: serde_json::Value =
        serde_json::from_slice(&std::fs::read(scratch.path().join("recording-plan.json")).unwrap())
            .unwrap();
    let heights: Vec<_> = plan["blocks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|block| block["height"].clone())
        .collect();
    assert_eq!(
        serde_json::Value::Array(heights.clone()),
        manifest["projection_blocks"],
        "only projection blocks may enter replay counts"
    );
    for height in manifest["datum_support_blocks"].as_array().unwrap() {
        assert!(
            !heights.contains(height),
            "support-only block must not enter row or shape counts"
        );
        assert!(
            scratch
                .path()
                .join(format!(
                    "provider/transactions/{}.json",
                    height.as_u64().unwrap()
                ))
                .exists()
        );
    }
    if network == "preprod" {
        // These real bootstrap outpoints exercise token selection separately from
        // projection-row and shape counts. Their original block remains support-only.
        let profile = dsa_chain::profiles::Profile::load("preprod").unwrap();
        let selection = dsa_chain::block::Selection::new(&profile);
        let bootstrap: Vec<dsa_provider::types::TransactionCbor> = serde_json::from_slice(
            &std::fs::read(scratch.path().join("provider/transactions/4318350.json")).unwrap(),
        )
        .unwrap();
        for (hash, policy) in [
            (
                "ed8a1f6afb4cb2b10e37e1092601a20a021da7231faafe10816f3c56bc3532d0",
                "fb964a2751bb9cb9c7a6d1475d1ea4a3960820a0f318883647502426",
            ),
            (
                "725c21c20c9193fe813d98cf886cd9e5e40187ac18cfc3be6e30bb349cbc90b4",
                "475db387ba92282a387e9ddfb7f8d2fd297f9b660450005f3ed37550",
            ),
        ] {
            let record = bootstrap
                .iter()
                .find(|record| record.tx_hash.as_str() == hash)
                .unwrap();
            let tx = dsa_chain::transaction::decode(&record.cbor.0, hash).unwrap();
            let supplements = dsa_chain::block::Supplements::default();
            let outputs = dsa_chain::block::effective_outputs(&tx, &supplements).unwrap();
            let output = outputs
                .iter()
                .find(|(index, _)| *index == 2)
                .unwrap()
                .1
                .unwrap();
            assert!(
                selection.relevant(output),
                "the address retains the original output"
            );
            let foreign = output
                .assets
                .iter()
                .find(|asset| asset.policy == policy && asset.name.is_empty())
                .unwrap();
            assert_eq!(foreign.quantity, 1);
            assert!(
                !selection.required(foreign),
                "v2 omits the observed foreign bootstrap token at its original outpoint"
            );
        }
    }
    // The replay's test boundary checks and builds every block from the same
    // decoded capture, with independent SQL origins and fixed mint effects.
    corpus::run_mode(scratch.path(), true);
    let missing: Vec<String> = serde_json::from_slice(
        &std::fs::read(scratch.path().join("missing-supplements.json")).unwrap(),
    )
    .unwrap();
    assert!(missing.is_empty(), "required API recordings: {missing:?}");
    corpus::run(scratch.path());
    for line in std::fs::read_to_string(scratch.path().join("supplement-requests.jsonl"))
        .unwrap()
        .lines()
    {
        let entry: serde_json::Value = serde_json::from_str(line).unwrap();
        assert!(
            heights.contains(&entry["block"]),
            "successful requests belong to a projection block"
        );
        let source = &entry["recording"];
        let bytes = std::fs::read(
            scratch
                .path()
                .join("provider")
                .join(source["response_path"].as_str().unwrap()),
        )
        .unwrap();
        assert_eq!(
            format!("{:x}", Sha256::digest(&bytes)),
            source["response_sha256"].as_str().unwrap()
        );
        if source["capture"]["format"] == "blockfrost-datum-capture-v1" {
            assert_eq!(entry["request"]["Datum"], source["capture"]["datum_hash"]);
        }
        if source["capture"]["format"] == "context-datum-capture-v1" {
            let capture = &source["capture"];
            assert_eq!(capture["block"], entry["block"]);
            assert_eq!(capture["datum_hash"], entry["request"]["Datum"]);
            assert_eq!(capture["reply_sha256"], source["response_sha256"]);
            let started = capture["request_started_unix"].as_u64().unwrap();
            let completed = capture["response_completed_unix"].as_u64().unwrap();
            assert!(0 < started && started <= completed);
            assert!(
                completed
                    <= std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs()
            );
        }
    }
    let summary: serde_json::Value = serde_json::from_slice(
        &std::fs::read(scratch.path().join("comparison-result.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(summary["asset_rule_version"], 2);
    for (expected, actual) in [
        ("tx", "tx"),
        ("tx_out", "outputs"),
        ("ma_tx_out", "assets"),
        ("tx_in", "spends"),
        ("datum", "datums"),
        ("tx_metadata", "metadata"),
        ("mint", "mints"),
    ] {
        assert_eq!(
            summary[actual], manifest["expected"][expected],
            "manifest {expected} count"
        );
    }
    for (field, count) in manifest["coverage"].as_object().unwrap() {
        assert_eq!(&summary[field], count, "manifest shape coverage {field}");
    }
    assert!(summary["decoded_transactions"].as_u64().unwrap() > 0);
    assert_eq!(summary["pallas_unavailable"], 0);
    println!("public_subset_{network} {summary}");
    (scratch, manifest)
}

#[test]
fn recorded_corpus_subset_matches_sql_and_comparison_decoder() {
    replay("preprod");
}

#[test]
fn recorded_mainnet_subset_matches_sql_and_real_invalid_byron_ordinals() {
    let (scratch, manifest) = replay("mainnet");
    let expected_json = std::fs::read(scratch.path().join("expected.json")).unwrap();
    let expected: dsa_chain::block::Rows = serde_json::from_slice(&expected_json).unwrap();
    let mints: Vec<corpus::MintRow> = serde_json::from_value(
        serde_json::from_slice::<serde_json::Value>(&expected_json).unwrap()["mint"].clone(),
    )
    .unwrap();
    let records = |height: u64| -> Vec<dsa_provider::types::TransactionCbor> {
        serde_json::from_slice(
            &std::fs::read(
                scratch
                    .path()
                    .join(format!("provider/transactions/{height}.json")),
            )
            .unwrap(),
        )
        .unwrap()
    };
    for witness in manifest["witnesses"]["invalid_transactions"]
        .as_array()
        .unwrap()
    {
        let hash = witness["hash"].as_str().unwrap();
        let block = witness["block"].as_u64().unwrap();
        let list = records(block);
        let record = list
            .iter()
            .find(|record| record.tx_hash.as_str() == hash)
            .unwrap();
        let tx = dsa_chain::transaction::decode(&record.cbor.0, hash).unwrap();
        assert!(!tx.is_valid() && tx.has_collateral_return());
        assert_eq!(tx.effective_inputs(), tx.collateral());
        let row = expected.tx.iter().find(|row| row.hash == hash).unwrap();
        assert!(!row.valid_contract);
        let outputs: Vec<_> = expected
            .tx_out
            .iter()
            .filter(|output| output.tx_id == row.id)
            .collect();
        assert_eq!(outputs.len(), 1);
        assert_eq!(
            usize::from(outputs[0].index),
            tx.outputs().len(),
            "invalid effective output retains collateral-return index"
        );
        assert_eq!(
            expected
                .tx_in
                .iter()
                .filter(|input| input.tx_in_id == row.id)
                .count(),
            1
        );
        assert!(!mints.iter().any(|mint| mint.tx_hash == hash));
    }
    let byron_block = manifest["witnesses"]["stored_byron_block"]
        .as_u64()
        .unwrap();
    let mut stored_byron = 0;
    for (index, record) in records(byron_block).iter().enumerate() {
        let tx = dsa_chain::transaction::decode(&record.cbor.0, record.tx_hash.as_str()).unwrap();
        for (output_index, output) in
            dsa_chain::block::effective_outputs(&tx, &dsa_chain::block::Supplements::default())
                .unwrap()
        {
            let output = output.unwrap();
            if output.address_raw.first().map(|byte| byte >> 4) == Some(8) {
                let tx_id = ((byron_block as i64) << 12) | index as i64;
                if let Some(row) = expected
                    .tx_out
                    .iter()
                    .find(|row| row.tx_id == tx_id && row.index == output_index)
                {
                    assert_eq!(Some(row.address.as_str()), output.address.as_deref());
                    stored_byron += 1;
                }
            }
        }
    }
    assert!(
        stored_byron > 0,
        "real Byron output is independently stored, not only an ordinary full-block fact"
    );
    let witness = &manifest["witnesses"]["asset_ordinal"];
    let asset: dsa_chain::block::AssetRow = serde_json::from_value(witness["row"].clone()).unwrap();
    assert!(expected.ma_tx_out.contains(&asset));
    assert_eq!(asset.id % 4096, 15);
    let output = expected
        .tx_out
        .iter()
        .find(|row| row.id == asset.tx_out_id)
        .unwrap();
    let tx = expected
        .tx
        .iter()
        .find(|row| row.id == output.tx_id)
        .unwrap();
    let list = records(witness["block"].as_u64().unwrap());
    let record = list
        .iter()
        .find(|record| record.tx_hash.as_str() == tx.hash)
        .unwrap();
    let decoded = dsa_chain::transaction::decode(&record.cbor.0, &tx.hash).unwrap();
    let supplements = dsa_chain::block::Supplements::default();
    let effective = dsa_chain::block::effective_outputs(&decoded, &supplements).unwrap();
    let original = effective
        .iter()
        .find(|(index, _)| *index == output.index)
        .unwrap()
        .1
        .unwrap();
    assert_eq!(
        (
            original.assets[15].policy.as_str(),
            original.assets[15].name.as_str(),
            original.assets[15].quantity.to_string()
        ),
        (
            asset.policy.as_str(),
            asset.name.as_str(),
            asset.quantity.clone()
        )
    );
    let metadata_hash = manifest["witnesses"]["other_metadata_transaction"]
        .as_str()
        .unwrap();
    let metadata: Vec<dsa_provider::types::MetadataEntry> = serde_json::from_slice(
        &std::fs::read(
            scratch
                .path()
                .join(format!("provider/metadata/{metadata_hash}.json")),
        )
        .unwrap(),
    )
    .unwrap();
    assert!(metadata.iter().any(|item| item.label.0 == 2001));
    assert!(!metadata.iter().any(|item| item.label.0 == 6500973));
    assert!(
        expected.tx_metadata.is_empty(),
        "real other-label metadata is excluded from bridge rows"
    );
    let stored_byron_outputs = expected
        .tx_out
        .iter()
        .filter(|row| {
            dsa_chain::transaction::address_bytes(&row.address)
                .unwrap()
                .first()
                .map(|byte| byte >> 4)
                == Some(8)
        })
        .count();
    let nonzero_asset_ordinals = expected
        .ma_tx_out
        .iter()
        .filter(|row| row.id % 4096 != 0)
        .count();
    let trace: Vec<serde_json::Value> =
        std::fs::read_to_string(scratch.path().join("supplement-requests.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
    let contextual_datum_replies = trace
        .iter()
        .filter(|entry| entry["recording"]["capture"]["format"] == "context-datum-capture-v1")
        .count();
    let other_label_metadata_replies = trace
        .iter()
        .filter(|entry| entry["request"]["Metadata"] == metadata_hash)
        .count();
    assert_eq!(contextual_datum_replies, 4);
    assert_eq!(other_label_metadata_replies, 1);
    let measured = serde_json::json!({"stored_byron_outputs":stored_byron_outputs,"nonzero_asset_ordinals":nonzero_asset_ordinals,"max_asset_ordinal":15,"contextual_datum_replies":contextual_datum_replies,"other_label_metadata_replies":other_label_metadata_replies});
    if let Some(expected) = manifest.get("row_witness_coverage") {
        assert_eq!(expected, &measured, "measured public row witnesses");
    }
    println!("public_mainnet_row_witnesses {measured}");
}

/// The recorded transactions the follower's ledger-content tests embed are
/// members of this corpus, byte for byte.
#[test]
fn recorded_transactions_of_the_follower_tests_are_corpus_members() {
    let (scratch, _) = replay("mainnet");
    let fixture: serde_json::Value = serde_json::from_slice(
        &std::fs::read(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../db-sync-alt/tests/fixtures/recorded-transactions.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        fixture["source"],
        "crates/dsa-provider/tests/fixtures/mainnet-corpus.json.gz"
    );
    for (member, sha256) in fixture["members"].as_object().unwrap() {
        let content = std::fs::read(scratch.path().join(member)).unwrap();
        assert_eq!(
            &format!("{:x}", Sha256::digest(&content)),
            sha256.as_str().unwrap()
        );
    }
    let transactions = fixture["transactions"].as_array().unwrap();
    assert_eq!(transactions.len(), 5);
    for transaction in transactions {
        let block = transaction["block"].as_u64().unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_slice(
            &std::fs::read(
                scratch
                    .path()
                    .join(format!("provider/transactions/{block}.json")),
            )
            .unwrap(),
        )
        .unwrap();
        let row = &rows[transaction["position"].as_u64().unwrap() as usize];
        assert_eq!(row["tx_hash"], transaction["tx_hash"]);
        assert_eq!(row["cbor"], transaction["cbor"]);
    }
}
