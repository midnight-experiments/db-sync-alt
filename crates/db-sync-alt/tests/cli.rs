//! The command wrapper: exit statuses per class, start and end lines, and a
//! stop that cancels the running statement and removes temporary files.
#[path = "common/legacy.rs"]
#[allow(dead_code)]
mod common;

use common::{Database, connect, fixture};
use serde_json::Value;
use std::{
    error::Error as StdError,
    process::Stdio,
    time::{Duration, Instant},
};
use tokio::process::Command;

type Result<T = ()> = std::result::Result<T, Box<dyn StdError + Send + Sync>>;

fn binary() -> Command {
    Command::new(env!("CARGO_BIN_EXE_db-sync-alt"))
}

fn lines(output: &std::process::Output) -> Vec<Value> {
    String::from_utf8_lossy(&output.stderr)
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}

fn end_line(output: &std::process::Output) -> Value {
    lines(output)
        .into_iter()
        .find(|line| line["event"] == "finished")
        .expect("one end line")
}

#[tokio::test]
async fn usage_errors_exit_2_and_help_documents_the_statuses() -> Result {
    let help = binary().arg("--help").output().await?;
    assert_eq!(help.status.code(), Some(0));
    let text = String::from_utf8_lossy(&help.stdout);
    for status in ["0", "1", "2", "3", "75"] {
        assert!(text.contains(&format!("  {status} ")), "{text}");
    }
    let missing = binary()
        .args(["verify", "--network", "mainnet"])
        .output()
        .await?;
    assert_eq!(missing.status.code(), Some(2));
    for (args, error, flag) in [
        (
            vec![
                "verify",
                "--network",
                "mainnet",
                "--database",
                "host=h user=u password=p",
            ],
            "password_requires_file",
            "--database",
        ),
        (
            vec![
                "verify",
                "--network",
                "preview",
                "--database",
                "host=h user=u",
            ],
            "unsupported_network",
            "--network",
        ),
        (
            vec![
                "compare",
                "--network",
                "mainnet",
                "--a",
                "host=h",
                "--b",
                "host=h",
                "--positions",
                "/nonexistent",
            ],
            "invalid_comparison_positions_file",
            "--positions",
        ),
    ] {
        let output = binary().args(&args).output().await?;
        assert_eq!(output.status.code(), Some(2), "{args:?}");
        let report = lines(&output).into_iter().next().unwrap();
        assert_eq!(
            (report["error"].as_str(), report["flag"].as_str()),
            (Some(error), Some(flag))
        );
        // Nothing started: no start or end line.
        assert!(
            lines(&output)
                .iter()
                .all(|line| line.get("event").is_none())
        );
    }
    Ok(())
}

#[tokio::test]
async fn an_unreachable_database_exits_75() -> Result {
    let output = binary()
        .args([
            "verify",
            "--network",
            "mainnet",
            "--database",
            "host=127.0.0.1 port=1 user=u dbname=d sslmode=disable connect_timeout=5",
        ])
        .output()
        .await?;
    assert_eq!(output.status.code(), Some(75));
    let end = end_line(&output);
    assert_eq!(end["outcome"], "temporary_failure");
    assert_eq!(end["exit"], 75);
    assert_eq!(end["operation"], "verify");
    assert!(end["duration_seconds"].as_f64().is_some());
    Ok(())
}

#[tokio::test]
#[ignore = "requires an isolated PostgreSQL server"]
async fn failures_exit_1_and_check_refusals_exit_3() -> Result {
    let db = Database::new().await?;
    // A check refuses a database that is not a db-sync-alt database.
    let output = db
        .start_cli("verify", "mainnet", &[])?
        .wait_with_output()
        .await?;
    assert_eq!(output.status.code(), Some(3));
    assert_eq!(end_line(&output)["outcome"], "difference");
    db.cli("create", "mainnet", &[]).await?;
    let output = db
        .start_cli(
            "grant-reader",
            "mainnet",
            &["--role", &db.reader, "--check"],
        )?
        .wait_with_output()
        .await?;
    assert_eq!(output.status.code(), Some(3));
    // An operation that cannot be done fails.
    let output = db
        .start_cli("restore", "mainnet", &["--file", "/nonexistent/snapshot"])?
        .wait_with_output()
        .await?;
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(end_line(&output)["outcome"], "failed");
    let output = db
        .start_cli("status", "mainnet", &[])?
        .wait_with_output()
        .await?;
    assert_eq!(output.status.code(), Some(0));
    let started = lines(&output)
        .into_iter()
        .filter(|line| line["event"] == "started")
        .count();
    assert_eq!(started, 1);
    assert_eq!(end_line(&output)["outcome"], "success");
    db.cleanup().await?;
    Ok(())
}

const SERVED: &str = "serve.block, serve.tx, serve.tx_out, serve.tx_in, serve.ma_tx_out, \
    serve.multi_asset, serve.datum, serve.epoch_param, serve.pool_hash, serve.epoch_stake, \
    serve.tx_metadata";

/// SIGTERM while the command waits on a lock: the statement is cancelled at
/// once (no session of the command remains while the lock is still held),
/// temporary files are removed and nothing is reported as published.
#[tokio::test]
#[ignore = "requires an isolated PostgreSQL server"]
async fn sigterm_during_verify_and_snapshot_cancels_the_statement() -> Result {
    let mut f = fixture(353);
    f.reindex_ordinals();
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
    let dir = tempfile::tempdir()?;
    let file = dir.path().join("stopped.snapshot");
    for command in ["verify", "snapshot"] {
        let lock = connect(&target.config).await?;
        lock.batch_execute(&format!(
            "BEGIN; LOCK TABLE {SERVED} IN ACCESS EXCLUSIVE MODE"
        ))
        .await?;
        let args: Vec<&str> = if command == "snapshot" {
            vec!["--file", file.to_str().unwrap()]
        } else {
            vec![]
        };
        let child = binary()
            .args([
                command,
                "--database",
                &target.dsn(false),
                "--network",
                "mainnet",
            ])
            .args(&args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let pid = child.id().unwrap();
        let waiting = "SELECT count(*) FROM pg_stat_activity
            WHERE datname = $1 AND application_name = 'db-sync-alt'";
        let began = Instant::now();
        loop {
            let blocked: i64 = target
                .admin
                .query_one(
                    &format!("{waiting} AND wait_event_type = 'Lock'"),
                    &[&target.name],
                )
                .await?
                .get(0);
            if blocked > 0 {
                break;
            }
            assert!(
                began.elapsed() < Duration::from_secs(20),
                "{command} never waited on the lock"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        if command == "snapshot" {
            assert!(
                std::fs::read_dir(dir.path())?.next().is_some(),
                "staging directory exists"
            );
        }
        assert!(
            Command::new("kill")
                .args(["-TERM", &pid.to_string()])
                .status()
                .await?
                .success()
        );
        let output =
            tokio::time::timeout(Duration::from_secs(10), child.wait_with_output()).await??;
        assert_eq!(output.status.code(), Some(1), "{command}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("operation_interrupted"), "{stderr}");
        assert!(stderr.contains("\"cancel_request\""), "{stderr}");
        assert_eq!(end_line(&output)["outcome"], "interrupted");
        if command == "snapshot" {
            let report = lines(&output)
                .into_iter()
                .find(|line| line.get("error").is_some())
                .unwrap();
            assert_eq!(report["published"], false);
            assert_eq!(report["operation"], "snapshot");
            assert_eq!(
                std::fs::read_dir(dir.path())?.count(),
                0,
                "temporary files removed"
            );
        }
        // The lock is still held: only a cancelled statement can end.
        let stopped = Instant::now();
        loop {
            let remaining: i64 = target
                .admin
                .query_one(waiting, &[&target.name])
                .await?
                .get(0);
            if remaining == 0 {
                break;
            }
            assert!(
                stopped.elapsed() < Duration::from_secs(5),
                "{command} left its statement running"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        lock.batch_execute("ROLLBACK").await?;
    }
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}
