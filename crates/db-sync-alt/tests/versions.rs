//! A snapshot file moves between PostgreSQL major versions. Runs when a second
//! disposable server of another major version is given in
//! DSA_TEST_DATABASE_OTHER, in the same form as DSA_TEST_DATABASE; the test
//! creates and drops databases on both. Without it (unset or empty) the test
//! prints that and returns.
#[path = "common/legacy.rs"]
#[allow(dead_code)]
mod common;
use common::{Database, file_args, fixture};
use serde_json::{Value, json};
use std::error::Error as StdError;
use tokio::process::Command;
type Result<T = ()> = std::result::Result<T, Box<dyn StdError + Send + Sync>>;

const FIRST: &str = "DSA_TEST_DATABASE";
const OTHER: &str = "DSA_TEST_DATABASE_OTHER";
const REQUIRE_OTHER: &str = "DSA_TEST_REQUIRE_OTHER";

async fn major(database: &Database) -> Result<i32> {
    Ok(database
        .admin
        .query_one(
            "SELECT current_setting('server_version_num')::int/10000",
            &[],
        )
        .await?
        .get(0))
}

/// A database written on the server of `from` (converted from a generated
/// source, as in the comparison tests) is snapshotted and its file restored
/// on the server of `to`. The restored database has the content the file
/// states, and the node's queries return the same rows, with the same column
/// names and types, on both servers.
async fn file_moves(from: &str, to: &str, seed: u64) -> Result {
    let mut f = fixture(seed);
    f.reindex_ordinals();
    let network = f.profile.network.clone();
    let source = Database::on(from).await?;
    source.legacy(&f).await?;
    let written = Database::on(from).await?;
    written
        .cli(
            "convert",
            &network,
            &["--source", &source.dsn(true), "--horizon-block", "6"],
        )
        .await?;
    let restored = Database::on(to).await?;
    let (written_major, restored_major) = (major(&written).await?, major(&restored).await?);
    assert_ne!(
        written_major, restored_major,
        "{from} and {to} name servers of the same major version"
    );
    let before = written.cli("verify", &network, &[]).await?;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("moved.dsa");
    written.cli("snapshot", &network, &file_args(&path)).await?;
    let report = restored.cli("restore", &network, &file_args(&path)).await?;
    let after = restored.cli("verify", &network, &[]).await?;
    for key in [
        "content_digest",
        "state_digest",
        "families",
        "protected_state",
        "state",
        "supply",
    ] {
        assert_eq!(before[key], report[key], "restore {key}");
        assert_eq!(before[key], after[key], "verify {key}");
    }
    assert_eq!(written.rows().await?, restored.rows().await?);
    for database in [&written, &restored] {
        database
            .cli("grant-reader", &network, &["--role", &database.reader])
            .await?;
    }
    let output = Command::new(env!("CARGO_BIN_EXE_db-sync-alt"))
        .args([
            "compare",
            "--a",
            &written.dsn(true),
            "--b",
            &restored.dsn(true),
            "--network",
            &network,
            "--positions",
            "auto",
            "--horizon-block",
            "6",
            "--a-verify-unbounded",
        ])
        .output()
        .await?;
    assert!(
        output.status.success(),
        "compare: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let compared: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(compared["matched"], true, "{compared}");
    assert_eq!(compared["supported_difference_count"], 0, "{compared}");
    assert_eq!(compared["unsupported_difference_count"], 0, "{compared}");
    assert_eq!(compared["columns_compared"], true, "{compared}");
    assert_eq!(compared["column_differences"], json!([]), "{compared}");
    println!(
        "{}",
        json!({"event":"snapshot_file_moved","seed":seed,"written_on":written_major,
            "restored_on":restored_major,"content_digest":after["content_digest"],
            "executions":compared["executions"]})
    );
    source.cleanup().await?;
    written.cleanup().await?;
    restored.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires an isolated PostgreSQL server; runs with a second server of another major version in DSA_TEST_DATABASE_OTHER"]
async fn snapshot_file_moves_between_major_versions() -> Result {
    if std::env::var(OTHER).unwrap_or_default().is_empty() {
        // CI sets this in the jobs that have a second server.
        assert!(
            std::env::var(REQUIRE_OTHER).unwrap_or_default().is_empty(),
            "{REQUIRE_OTHER} is set and {OTHER} is not: this run must move files"
        );
        println!("{OTHER} is not set: no second server, no file is moved");
        return Ok(());
    }
    file_moves(FIRST, OTHER, 303).await?;
    file_moves(OTHER, FIRST, 304).await
}
