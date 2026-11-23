//! The committed `catalogue.json` and `bridge.json` equal a fresh extraction
//! from the vendored node sources, and every SQL-looking literal there is
//! catalogued or listed in `sql-outside-catalogue.json`. After a change of the
//! vendored sources, `DSA_WRITE_CATALOGUE=1 cargo test -p dsa-store --test
//! catalogue` rewrites the two generated files.
mod extract;

use dsa_store::catalogue::{self, Class, OutsideCatalogue};
use std::{collections::BTreeSet, fs, path::PathBuf};

fn crate_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn reference() -> PathBuf {
    crate_dir().join("../../reference")
}

#[test]
fn committed_catalogue_equals_extraction() {
    let extraction = extract::extract(&reference(), catalogue::outside()).unwrap();
    let catalogue_text = serde_json::to_string_pretty(&extraction.catalogue).unwrap();
    let bridge_text = serde_json::to_string_pretty(&extraction.bridge).unwrap() + "\n";
    if std::env::var_os("DSA_WRITE_CATALOGUE").is_some() {
        fs::write(crate_dir().join("catalogue.json"), &catalogue_text).unwrap();
        fs::write(crate_dir().join("bridge.json"), &bridge_text).unwrap();
    }
    assert_eq!(catalogue::TEXT, catalogue_text);
    assert_eq!(catalogue::BRIDGE_TEXT, bridge_text);
    let reads = catalogue::catalogue()
        .entries
        .iter()
        .filter(|e| e.kind == "read")
        .count();
    assert_eq!(reads, 29);
    // Source captures record this hash; recorded captures stay readable only
    // while the catalogue text is unchanged.
    assert_eq!(
        catalogue::sha256(),
        "0b0e9b5d722ea2cef8f44c954a3cf729cd3d328cfeca949264394e7e34c5376c"
    );
    assert_eq!(
        catalogue::outside().node_commit,
        catalogue::catalogue().node_commit
    );
}

/// The statement text recorded by captures for the bridge query at checkpoint
/// blocks 5, 6 and 7, and the node's own assembly for any block.
#[test]
fn bridge_statement_is_the_node_assembly() {
    for (block, sha256) in [
        (
            5,
            "3e18168b6047f808dee59e2bb6038af55838be1ea7fa2ff2ab2790f91aa6daf8",
        ),
        (
            6,
            "8ee3a8e04fc9fd076d67da187b98f154140d082a808eb5efe3574712c2eb5802",
        ),
        (
            7,
            "e931b5adbbcf7c60969aa3ac1f30793c970026a397a67069afd8f8dad05f9550",
        ),
    ] {
        assert_eq!(
            extract::hash(catalogue::bridge_sql(block).as_bytes()),
            sha256
        );
    }
    let source = fs::read_to_string(
        reference().join("partner-chains/toolkit/data-sources/db-sync/src/db_model.rs"),
    )
    .unwrap();
    for block in [0, 6, 4_500_196, i32::MAX] {
        let sql = catalogue::bridge_sql(block);
        assert!(sql.contains(&format!("block.block_no > {block}")));
        assert!(!sql.contains(['{', '}']));
        assert!(sql.contains("LIMIT null;"));
        assert!(sql.contains("tx_in.tx_in_id     AS consuming_tx_id"));
        assert_eq!(
            sql,
            extract::assemble_bridge(&source)
                .unwrap()
                .sql
                .replacen("{}", &block.to_string(), 1)
        );
    }
}

/// A copy of the vendored tree with `extra` appended to one vendored file and
/// the manifest updated for it.
fn tree_with(extra: &str) -> (tempfile::TempDir, String, usize) {
    let dir = tempfile::tempdir().unwrap();
    copy(&reference(), dir.path());
    let file = "primitives/mainchain-follower/src/db/multi_asset_cache.rs";
    let path = dir.path().join(file);
    let mut text = fs::read_to_string(&path).unwrap();
    let line = text.lines().count() + 1;
    text.push_str(extra);
    fs::write(&path, &text).unwrap();
    let manifest_path = dir.path().join("source-manifest.json");
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    manifest["files"][file]["sha256"] = extract::hash(text.as_bytes()).into();
    fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    (dir, file.to_owned(), line)
}

fn copy(from: &std::path::Path, to: &std::path::Path) {
    for entry in fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.path().is_dir() {
            fs::create_dir_all(&target).unwrap();
            copy(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), target).unwrap();
        }
    }
}

#[test]
fn an_uncatalogued_sql_literal_fails_extraction() {
    let (dir, file, line) = tree_with(
        "fn added() {\n    let _ = \"\nWITH\n  recent AS (SELECT id FROM block)\nSELECT * FROM recent\";\n}\n",
    );
    let error = extract::extract(dir.path(), catalogue::outside())
        .err()
        .expect("an uncatalogued WITH statement must fail the extraction");
    assert_eq!(
        error,
        format!("uncatalogued SQL literal: {file}:{}", line + 1)
    );
    // Lower-case SQL is recognised too; prose is not.
    let (dir, _, _) = tree_with("fn added() { let _ = \"insert into block values (1)\"; }\n");
    assert!(extract::extract(dir.path(), catalogue::outside()).is_err());
    let (dir, _, _) = tree_with("fn added() { let _ = \"Insert key into keystore\"; }\n");
    assert!(extract::extract(dir.path(), catalogue::outside()).is_ok());
}

#[test]
fn the_outside_list_is_exact_for_vendored_files() {
    let mut without_bridge: OutsideCatalogue = catalogue::outside().clone();
    without_bridge
        .statements
        .retain(|s| s.class != Class::Assembled);
    assert_eq!(
        extract::extract(&reference(), &without_bridge)
            .err()
            .unwrap(),
        "uncatalogued SQL literal: partner-chains/toolkit/data-sources/db-sync/src/db_model.rs:854"
    );
    let mut stale = catalogue::outside().clone();
    let mut moved = stale
        .statements
        .iter()
        .find(|s| s.class == Class::Assembled)
        .unwrap()
        .clone();
    moved.line += 1;
    stale.statements.push(moved);
    assert!(
        extract::extract(&reference(), &stale)
            .err()
            .unwrap()
            .starts_with("stale SQL exclusion:")
    );
}

#[test]
fn seven_genesis_statements_are_unsupported_with_their_subcommands() {
    let unsupported = catalogue::unsupported().collect::<Vec<_>>();
    assert_eq!(unsupported.len(), 7);
    let subcommands = unsupported
        .iter()
        .flat_map(|s| s.subcommands.iter().map(String::as_str))
        .collect::<BTreeSet<_>>();
    assert_eq!(
        subcommands,
        BTreeSet::from([
            "generate-genesis-config",
            "generate-ics-genesis",
            "generate-reserve-genesis",
            "verify-auth-script",
            "verify-cardano-tip-finalized",
            "verify-federated-authority-auth-script",
            "verify-ics-auth-script",
            "verify-permissioned-candidates-auth-script",
            "verify-reserve-auth-script",
        ])
    );
    for statement in unsupported {
        assert!(statement.file.starts_with("node/src/"));
        assert!(!statement.reason.is_empty());
    }
}

#[test]
fn literal_scanner_and_sql_rule() {
    let values = extract::literals("// \"no\"\n/* \"no\" /* nested */ */ r##\"SELECT \"yes\"\"##")
        .unwrap()
        .into_iter()
        .map(|(_, v)| v)
        .collect::<Vec<_>>();
    assert_eq!(values, vec!["SELECT \"yes\"".to_owned()]);
    for value in [
        "SELECT 1",
        "select count(*) from information_schema.tables",
        "\n\t\tWITH\n\t\t\ta AS (SELECT 1) SELECT * FROM a;",
        "with a as (select 1) select * from a",
        "INSERT INTO t VALUES (1)",
        "CREATE INDEX i ON t(c)",
    ] {
        assert!(extract::sql_looking(value), "{value}");
    }
    for value in [
        "Insert key into keystore (unsafe)",
        "Select the genesis UTXO",
        "update the chain spec",
        "selected",
        "",
        "Delete from list",
    ] {
        assert!(!extract::sql_looking(value), "{value}");
    }
}
