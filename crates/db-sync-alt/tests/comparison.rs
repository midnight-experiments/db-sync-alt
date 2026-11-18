//! Actual CLI comparisons use the same genuine legacy DDL as conversion.
#[path = "common/legacy.rs"]
#[allow(dead_code)]
mod common;
use common::{Database, fixture};
use serde_json::{Value, json};
use std::error::Error as StdError;
use tokio::process::Command;
type Result<T = ()> = std::result::Result<T, Box<dyn StdError + Send + Sync>>;
#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn compare_cli_legacy_conversion_complete_catalogue() -> Result {
    let f = fixture(301);
    let source = Database::new().await?;
    source.legacy(&f).await?;
    let target = Database::new().await?;
    target
        .cli(
            "convert",
            "mainnet",
            &["--source", &source.dsn(true), "--horizon-block", "6"],
        )
        .await?;
    target
        .cli("grant-reader", "mainnet", &["--role", &target.reader])
        .await?;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("positions.json");
    std::fs::write(
        &path,
        serde_json::to_vec(
            &json!({"horizon":6,"target":8,"pairs":[{"cursor":{"block":6,"tx_index":0},"reference":{"block":8,"tx_index":4096}}]}),
        )?,
    )?;
    let output = Command::new(env!("CARGO_BIN_EXE_db-sync-alt"))
        .args([
            "compare",
            "--a",
            &source.dsn(true),
            "--b",
            &target.dsn(true),
            "--network",
            "mainnet",
            "--positions",
            path.to_str().unwrap(),
        ])
        .output()
        .await?;
    assert!(
        output.status.success(),
        "compare: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(report["matched"], true);
    assert!(report["executions"].as_u64().unwrap() >= 58);
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}
