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
    let bundle: Bundle = serde_json::from_slice(&raw).unwrap();
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
    corpus::run(scratch.path());
}
