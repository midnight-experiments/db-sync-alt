//! Replay the public ledger subset against its independently exported SQL rows.

#[path = "../examples/support/corpus.rs"]
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

#[test]
fn recorded_corpus_subset_matches_sql_and_comparison_decoder() {
    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/preprod-corpus.json.gz");
    let compressed = std::fs::read(fixture).expect("committed recorded subset");
    let manifest: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/preprod-corpus.manifest.json")).unwrap();
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
    assert_eq!(bundle.network, "preprod");
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
    // These real bootstrap outpoints exercise token selection separately from
    // projection-row and shape counts. Their original block remains support-only.
    let profile = dsa_chain::profiles::Profile::load("preprod").unwrap();
    let selection = dsa_chain::rows::Selection::new(&profile);
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
        let bytes = hex::decode(record.cbor.as_str()).unwrap();
        let tx = dsa_chain::transaction::decode(&bytes, hash).unwrap();
        let supplements = dsa_chain::rows::Supplements::default();
        let outputs = dsa_chain::rows::effective_outputs(&tx, &supplements).unwrap();
        let output = outputs.iter().find(|(index, _)| *index == 2).unwrap().1;
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
}
