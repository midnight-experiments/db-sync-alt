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

use common::connect;
use dsa_chain::profiles::Profile;
use dsa_store::{
    Store,
    compare::{self, Bind, IdBounds, Options, Positions, Reader},
    convert::{ConvertOptions, LegacySource},
};

async fn setup(seed: u64) -> Result<(Database, Database, common::fixtures::Fixture)> {
    let mut f = fixture(seed);
    f.reindex_ordinals();
    let source = Database::new().await?;
    source.legacy(&f).await?;
    let target = Database::new().await?;
    target
        .cli(
            "convert",
            &f.profile.network,
            &["--source", &source.dsn(true), "--horizon-block", "6"],
        )
        .await?;
    target
        .cli(
            "grant-reader",
            &f.profile.network,
            &["--role", &target.reader],
        )
        .await?;
    Ok((source, target, f))
}
async fn cli(
    a: &Database,
    b: &Database,
    profile: &Profile,
    extra: &[&str],
) -> Result<std::process::Output> {
    Ok(Command::new(env!("CARGO_BIN_EXE_db-sync-alt"))
        .args([
            "compare",
            "--a",
            &a.dsn(true),
            "--b",
            &b.dsn(true),
            "--network",
            &profile.network,
            "--positions",
            "auto",
            "--horizon-block",
            "6",
            "--verify-unbounded",
        ])
        .args(extra)
        .output()
        .await?)
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn compare_both_networks_all_events_units_pages_and_numeric_payloads() -> Result {
    for seed in [303, 304] {
        let (source, target, f) = setup(seed).await?;
        // The source reports the same integral numeric values with trailing scales.
        target
            .write("UPDATE serve.epoch_stake SET amount=(amount::text||'.0000')::numeric")
            .await?;
        let mut a = Reader::connect(source.reader_config(), None).await?;
        let mut b = Reader::connect(target.reader_config(), None).await?;
        let ta = a.snapshot().await?;
        let positions = Positions::generate(&ta, 6, 8, 500).await?;
        assert!(
            positions
                .pairs
                .iter()
                .any(|p| p.cursor.block == 6 && p.reference.block == 8)
        );
        let missing = compare::choose_missing_policy(&[&ta]).await?;
        assert_eq!(missing.len(), 28);
        assert_ne!(missing, vec![0; 28], "fixture must exercise miss collision");
        let collision = compare::capture(
            &ta,
            &f.profile,
            &positions,
            &Options {
                missing_policy: Some(vec![0; 28]),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(
            collision,
            dsa_store::Error::Refusal("comparison_probe_policy_present")
        ));
        let options = Options {
            missing_policy: Some(missing),
            verify_unbounded: true,
            page_rows: 1,
            ..Default::default()
        };
        let left = compare::capture(&ta, &f.profile, &positions, &options).await?;
        ta.commit().await?;
        let tb = b.snapshot().await?;
        let right = compare::capture(
            &tb,
            &f.profile,
            &positions,
            &Options {
                check_chain_order: true,
                ..options
            },
        )
        .await?;
        tb.commit().await?;
        let report = compare::compare(&left, &right)?;
        assert!(report.matched, "{}", serde_json::to_string(&report)?);
        assert_eq!(report.probe_rows.len(), 58);
        assert!(!report.chain_order_checked_a);
        assert!(report.chain_order_checked_b);
        assert!(report.unbounded_checked_a && report.unbounded_checked_b);
        let mut unchecked = right.clone();
        unchecked.chain_order_checked = false;
        assert!(matches!(
            compare::compare(&left, &unchecked),
            Err(dsa_store::Error::Refusal(
                "comparison_destination_order_not_checked"
            ))
        ));
        for (key, counts) in &report.probe_rows {
            if key.starts_with("resolve_ident:") && key.ends_with("/miss") {
                assert_eq!(*counts, [0, 0]);
            }
        }
        let mut wrong_context = right.clone();
        wrong_context.missing_policy = "fe".repeat(28);
        assert!(matches!(
            compare::compare(&left, &wrong_context),
            Err(dsa_store::Error::Refusal(
                "comparison_capture_context_mismatch"
            ))
        ));
        assert_eq!(report.units["bridge"], f.profile.assets["bridge"].unit());
        assert_eq!(report.units["ics"], f.profile.assets["ics"].unit());
        let seen = left
            .cases
            .values()
            .map(|c| c.query.clone())
            .collect::<std::collections::BTreeSet<_>>();
        for s in dsa_store::catalogue::catalogue()
            .entries
            .iter()
            .filter(|s| s.kind == "read")
        {
            assert!(seen.contains(&s.id), "unexecuted {}", s.id);
        }
        assert!(
            left.cases
                .values()
                .any(|c| c.query.starts_with("get_asset_creates") && !c.rows.is_empty())
        );
        println!(
            "{}",
            json!({"event":"compare_network","network":f.profile.network,"executions":report.executions,"cases":report.compared_cases,"helper_id_differences":report.helper_id_differences,"units":report.units})
        );
        drop(a);
        drop(b);
        source.cleanup().await?;
        target.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn compare_detects_removed_unspent_governance_changed_header_and_ident() -> Result {
    for mutation in ["unspent", "governance", "tx_count", "ident"] {
        let (source, target, f) = setup(305).await?;
        match mutation{
   "unspent"=>{let out=f.chain.inputs.values().find(|i|f.chain.transactions[&i.transaction].block>=6&&f.chain.transactions[&f.chain.outputs[&i.output].transaction].block<6&&f.chain.output_assets.values().any(|a|a.output==i.output&&f.chain.assets[&a.asset]==f.profile.assets["cnight"])).unwrap().output;target.write(&format!("DELETE FROM serve.tx_out WHERE id={out}")).await?;},
   "governance"=>{let policy=hex::decode(&f.profile.assets["council"].policy)?;let c=connect(&target.config).await?;c.execute("DELETE FROM serve.tx_out WHERE id=(SELECT o.id FROM serve.tx_out o JOIN serve.ma_tx_out a ON a.tx_out_id=o.id JOIN serve.multi_asset m ON m.id=a.ident JOIN serve.tx t ON t.id=o.tx_id WHERE m.policy=$1 ORDER BY t.id DESC LIMIT 1)",&[&policy]).await?;},
   "tx_count"=>target.write("UPDATE serve.block SET tx_count=tx_count+1 WHERE id=8").await?,
   "ident"=>target.write("UPDATE serve.multi_asset SET id=id+50000;UPDATE serve.ma_tx_out SET ident=ident+50000").await?,
   _=>unreachable!()
  }
        let out = cli(&source, &target, &f.profile, &[]).await?;
        assert_eq!(
            out.status.code(),
            Some(2),
            "{mutation}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let report: Value = serde_json::from_slice(&out.stdout)?;
        assert_eq!(report["matched"], false);
        assert!(
            report["differences"]
                .as_array()
                .unwrap()
                .iter()
                .any(|d| d["informational"] == false)
        );
        if mutation == "ident" {
            assert!(report["differences"].as_array().unwrap().iter().any(|d| {
                d["query"].as_str().unwrap().starts_with("resolve_ident")
                    && d["informational"] == false
            }));
        }
        source.cleanup().await?;
        target.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn compare_bounds_sentinels_broken_relationship_and_ordinal_order() -> Result {
    let (source, target, f) = setup(307).await?;
    let mut reader = Reader::connect(target.reader_config(), None).await?;
    let tx = reader.snapshot().await?;
    assert!(
        compare::validate_bounds(
            &tx,
            6,
            IdBounds {
                tx: i64::MAX,
                output: 0,
                asset: 0,
                input: 0
            },
            false
        )
        .await
        .is_err()
    );
    compare::validate_chain_order(&tx).await?;
    tx.commit().await?;
    drop(reader);
    let c = connect(&target.config).await?;
    let ids=c.query("SELECT a.id FROM serve.ma_tx_out a WHERE a.tx_out_id=(SELECT tx_out_id FROM serve.ma_tx_out GROUP BY tx_out_id HAVING count(*)>1 LIMIT 1) ORDER BY id",&[]).await?;
    let first: i64 = ids[0].get(0);
    let second: i64 = ids[1].get(0);
    let temp = first / 4096 * 4096 + 4095;
    c.batch_execute(&format!("UPDATE serve.ma_tx_out SET id={temp} WHERE id={first};UPDATE serve.ma_tx_out SET id={first} WHERE id={second};UPDATE serve.ma_tx_out SET id={second} WHERE id={temp}")).await?;
    let out = cli(&source, &target, &f.profile, &[]).await?;
    assert_eq!(out.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&out.stderr).contains("comparison_chain_order_invalid"));
    drop(c);
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn compare_source_snapshot_matches_newest_query_while_legacy_advances() -> Result {
    let mut f = fixture(309);
    f.reindex_ordinals();
    let source = Database::new().await?;
    source.legacy(&f).await?;
    let target = Database::new().await?;
    let mut source_session = LegacySource::connect(source.reader_config(), None).await?;
    let tx = source_session.snapshot().await?;
    let positions = Positions::generate(&tx, 6, 8, 500).await?;
    let mut store = Store::connect(target.config.clone(), None).await?;
    let staged = store
        .stage_conversion(
            &f.profile,
            &tx,
            &ConvertOptions {
                horizon_block: Some(6),
                ..Default::default()
            },
        )
        .await?;
    source.write(&format!("INSERT INTO chain.generations VALUES(2,1,'ready','generated',0,0,{}, {},'{{}}');INSERT INTO serve.block VALUES({},decode('{}','hex'),1084,999999,{},timestamp '2026-01-02',0);UPDATE chain.integrity_state SET projection_generation=2,serve_generation=2;UPDATE chain.service_state SET canonical_ready=2,publication_serial=publication_serial+1",f.bounds.target+1,f.bounds.tip+1,f.bounds.tip+1,"cd".repeat(32),f.bounds.tip+1)).await?;
    let options = Options {
        missing_policy: Some(compare::choose_missing_policy(&[&tx]).await?),
        ..Default::default()
    };
    let left = compare::capture(&tx, &f.profile, &positions, &options).await?;
    assert_eq!(left.newest_height, f.bounds.tip);
    tx.commit().await?;
    staged.finish(&f.profile).await?;
    drop(store);
    target
        .cli("grant-reader", "mainnet", &["--role", &target.reader])
        .await?;
    let mut reader = Reader::connect(target.reader_config(), None).await?;
    let tx = reader.snapshot().await?;
    let right = compare::capture(
        &tx,
        &f.profile,
        &positions,
        &Options {
            check_chain_order: true,
            ..options
        },
    )
    .await?;
    tx.commit().await?;
    assert!(compare::compare(&left, &right)?.matched);
    // A fresh source snapshot must expose the unbounded #4 difference; no hidden
    // cursor/target rewrite is allowed to make the later CLI invocation match.
    let dir = tempfile::tempdir()?;
    let file = dir.path().join("pinned-positions.json");
    std::fs::write(&file, serde_json::to_vec(&positions)?)?;
    let out = Command::new(env!("CARGO_BIN_EXE_db-sync-alt"))
        .args([
            "compare",
            "--a",
            &source.dsn(true),
            "--b",
            &target.dsn(true),
            "--network",
            "mainnet",
            "--positions",
            file.to_str().unwrap(),
        ])
        .output()
        .await?;
    assert_eq!(out.status.code(), Some(2));
    let report: Value = serde_json::from_slice(&out.stdout)?;
    assert!(report["differences"].as_array().unwrap().iter().any(|d| {
        d["query"]
            .as_str()
            .unwrap()
            .starts_with("get_latest_block_info")
            && d["informational"] == false
    }));
    drop(source_session);
    drop(reader);
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn compare_refusals_readonly_credentials_and_no_data_mutation() -> Result {
    let (source, target, f) = setup(311).await?;
    let before_source = source.catalog().await?;
    let before_target = target.catalog().await?;
    let before_rows = target.rows().await?;
    source
        .write("UPDATE chain.service_state SET revoked=true")
        .await?;
    let out = cli(&source, &target, &f.profile, &[]).await?;
    assert_eq!(out.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&out.stderr).contains("comparison_not_serving"));
    source
        .write("UPDATE chain.service_state SET revoked=false")
        .await?;
    let dir = tempfile::tempdir()?;
    let password = dir.path().join("reader.password");
    std::fs::write(&password, "generated-reader-password")?;
    let out = cli(
        &source,
        &target,
        &f.profile,
        &[
            "--a-password-file",
            password.to_str().unwrap(),
            "--b-password-file",
            password.to_str().unwrap(),
        ],
    )
    .await?;
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!String::from_utf8_lossy(&out.stderr).contains("generated-reader-password"));
    let out = Command::new(env!("CARGO_BIN_EXE_db-sync-alt"))
        .args([
            "compare",
            "--a",
            &(source.dsn(true) + " password='generated-inline-secret'"),
            "--b",
            &target.dsn(true),
            "--network",
            "mainnet",
            "--positions",
            "auto",
            "--horizon-block",
            "6",
        ])
        .output()
        .await?;
    assert!(!out.status.success());
    assert!(!String::from_utf8_lossy(&out.stderr).contains("generated-inline-secret"));
    assert_eq!(source.catalog().await?, before_source);
    assert_eq!(target.catalog().await?, before_target);
    assert_eq!(target.rows().await?, before_rows);
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn compare_catalogue_sqlx08_native_types_empty_serving_29x2() -> Result {
    use sqlx::{Connection, Row};
    let target = Database::new().await?;
    target.cli("create", "mainnet", &[]).await?;
    target.write("UPDATE dsa.position SET serving=true,reason='empty-query-qualification',target_height=8,target_hash=decode(repeat('00',32),'hex'),target_time=now(),tip_height=2168,tip_hash=decode(repeat('00',32),'hex'),tip_time=now(),horizon_height=6,horizon_hash=decode(repeat('00',32),'hex'),horizon_time=now()").await?;
    target
        .cli("grant-reader", "mainnet", &["--role", &target.reader])
        .await?;
    let host = match &target.config.get_hosts()[0] {
        tokio_postgres::config::Host::Tcp(s) => s.as_str(),
        _ => return Err("TCP required".into()),
    };
    let opts = sqlx::postgres::PgConnectOptions::new()
        .host(host)
        .port(target.config.get_ports().first().copied().unwrap_or(5432))
        .username(&target.reader)
        .database(&target.name)
        .ssl_mode(sqlx::postgres::PgSslMode::Disable);
    let mut sqlx = sqlx::PgConnection::connect_with(&opts).await?;
    let mut reader = Reader::connect(target.reader_config(), None).await?;
    let tx = reader.snapshot().await?;
    let cat = dsa_store::catalogue::catalogue();
    let mut gated = 0;
    let mut executions = 0;
    for s in cat.entries.iter().filter(|s| s.kind == "read") {
        if !s.sql.contains("pg_indexes") && !s.sql.contains("information_schema.tables") {
            gated += 1;
        }
        for hit in [true, false] {
            let values = s
                .parameters
                .iter()
                .map(|kind| match kind.as_str() {
                    "int4" => Bind::I32(if hit { 8 } else { i32::MAX }),
                    "int8" => Bind::I64(if hit { 1 } else { i64::MAX }),
                    "bytea" => Bind::Bytes(vec![if hit { 1 } else { 0 }; 32]),
                    "text" => Bind::Text(
                        if hit {
                            "idx_tx_in_tx_in_id"
                        } else {
                            "__missing__"
                        }
                        .to_owned(),
                    ),
                    "timestamp" => Bind::Time(
                        chrono::NaiveDateTime::parse_from_str(
                            "2026-01-01 00:00:00",
                            "%Y-%m-%d %H:%M:%S",
                        )
                        .unwrap(),
                    ),
                    _ => panic!("unhandled native type"),
                })
                .collect::<Vec<_>>();
            let expected = compare::rows(&tx, s, &values).await?;
            let sql = format!(
                "SELECT to_jsonb(observation)::text FROM ({}) observation",
                s.sql.trim().trim_end_matches(';')
            );
            let mut query = sqlx::query(&sql);
            for v in &values {
                query = match v {
                    Bind::I32(v) => query.bind(*v),
                    Bind::I64(v) => query.bind(*v),
                    Bind::Text(v) => query.bind(v.as_str()),
                    Bind::Bytes(v) => query.bind(v.as_slice()),
                    Bind::Time(v) => query.bind(*v),
                };
            }
            let mut actual = query
                .fetch_all(&mut sqlx)
                .await?
                .into_iter()
                .map(|r| {
                    let text: String = r.try_get(0)?;
                    Ok(compare::canonical_row(&text)?)
                })
                .collect::<Result<Vec<String>>>()?;
            // All empty data statements have no ordering ambiguity; index catalog rows
            // also have one hit. Sorting here compares driver decoding independently.
            actual.sort();
            let mut expected = expected;
            expected.sort();
            assert_eq!(actual, expected, "{} hit={hit}", s.id);
            executions += 1;
        }
    }
    assert_eq!(gated, 26);
    assert_eq!(executions, 58);
    println!(
        "{}",
        json!({"event":"native_sqlx_qualification","sqlx":"0.8.6","standalone":29,"gated":26,"gated_hit_miss":52,"total_hit_miss":58})
    );
    tx.commit().await?;
    drop(reader);
    drop(sqlx);
    target.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn compare_refuses_rule_one_and_declared_stored_mismatch_even_empty() -> Result {
    let (source, target, f) = setup(319).await?;
    let mut reader = Reader::connect(source.reader_config(), None).await?;
    let tx = reader.snapshot().await?;
    let positions = Positions::generate(&tx, 6, 8, 500).await?;
    let mut good = compare::capture(&tx, &f.profile, &positions, &Options::default()).await?;
    tx.commit().await?;
    let original = good.keep_rule_version;
    good.keep_rule_version = 1;
    assert!(matches!(
        compare::compare(&good, &good),
        Err(dsa_store::Error::Refusal("comparison_keep_rule_mismatch"))
    ));
    good.keep_rule_version = original;
    drop(reader);
    // No row may be required to force the actual identity gate. The compare
    // preflight's independent EXISTS probe must execute on this empty view.
    target.write("TRUNCATE serve.block").await?;
    let c = connect(&target.config).await?;
    for (stored, declared, expected) in [
        (1, 1, "comparison_keep_rule_mismatch"),
        (2, 1, "comparison_keep_rule_mismatch"),
        (1, 2, "comparison_not_serving"),
    ] {
        c.batch_execute(&format!("UPDATE dsa.identity SET keep_rule_version={stored};COMMENT ON FUNCTION dsa.gate() IS 'db-sync-alt keep-rule-version={declared}'")).await?;
        let mut r = Reader::connect(target.reader_config(), None).await?;
        let snapshot = r.snapshot().await?;
        let error = compare::capture(&snapshot, &f.profile, &positions, &Options::default())
            .await
            .unwrap_err();
        assert!(
            matches!(error,dsa_store::Error::Refusal(code) if code==expected),
            "{error:?}"
        );
        snapshot.rollback().await?;
    }
    drop(c);
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}

/// Dedicated generated TLS/SCRAM Docker server: the normal service has no test
/// certificate. On-demand validation must provide both public root paths.
#[tokio::test]
#[ignore = "requires generated TLS/SCRAM PostgreSQL17 and DSA_TLS roots"]
async fn compare_cli_tls_reader_files_and_certificate_identity() -> Result {
    let root = std::env::var("DSA_TLS_ROOT_CERT")?;
    let wrong_root = std::env::var("DSA_TLS_WRONG_ROOT_CERT")?;
    let f = fixture(321);
    let source = Database::new().await?;
    source.legacy(&f).await?;
    let target = Database::new().await?;
    let dir = tempfile::tempdir()?;
    let password = dir.path().join("reader.password");
    let wrong_password = dir.path().join("wrong.password");
    std::fs::write(&password, "generated-tls-reader-password")?;
    std::fs::write(&wrong_password, "generated-wrong-password")?;
    source
        .admin
        .batch_execute(&format!(
            "ALTER ROLE {} PASSWORD 'generated-tls-reader-password'",
            source.reader
        ))
        .await?;
    let a = source.dsn(true).replace(" sslmode=disable", "");
    let captured = dir.path().join("tls-source.capture.json");
    let converted = target
        .cli(
            "convert",
            "mainnet",
            &[
                "--source",
                &a,
                "--source-password-file",
                password.to_str().unwrap(),
                "--source-tls-root-cert",
                &root,
                "--horizon-block",
                "6",
                "--capture-source",
                captured.to_str().unwrap(),
            ],
        )
        .await?;
    assert_eq!(converted["source"]["settings"]["ssl"], true);
    target
        .cli("grant-reader", "mainnet", &["--role", &target.reader])
        .await?;
    target
        .admin
        .batch_execute(&format!(
            "ALTER ROLE {} PASSWORD 'generated-tls-reader-password'",
            target.reader
        ))
        .await?;
    let b = target.dsn(true).replace(" sslmode=disable", "");
    let before = target.rows().await?;
    for case in ["verified", "wrong_ca", "wrong_host", "wrong_password"] {
        let a = if case == "wrong_host" {
            a.replace("host='postgres'", "host='wrong-host'")
        } else {
            a.clone()
        };
        let ca = if case == "wrong_ca" {
            wrong_root.as_str()
        } else {
            root.as_str()
        };
        let pw = if case == "wrong_password" {
            &wrong_password
        } else {
            &password
        };
        let out = Command::new(env!("CARGO_BIN_EXE_db-sync-alt"))
            .args([
                "compare",
                "--a",
                &a,
                "--b",
                &b,
                "--network",
                "mainnet",
                "--positions",
                "auto",
                "--horizon-block",
                "6",
                "--a-tls-root-cert",
                ca,
                "--b-tls-root-cert",
                &root,
                "--a-password-file",
                pw.to_str().unwrap(),
                "--b-password-file",
                password.to_str().unwrap(),
            ])
            .output()
            .await?;
        if case == "verified" {
            assert!(
                out.status.success(),
                "{} {}",
                String::from_utf8_lossy(&out.stderr),
                String::from_utf8_lossy(&out.stdout)
            );
            let report: Value = serde_json::from_slice(&out.stdout)?;
            assert_eq!(report["matched"], true);
            assert_eq!(report["settings_a"]["ssl"], true);
            assert_eq!(report["settings_b"]["ssl"], true);
        } else {
            assert_eq!(out.status.code(), Some(1), "accepted {case}");
        }
        assert!(!String::from_utf8_lossy(&out.stderr).contains("generated-tls-reader-password"));
        println!(
            "{}",
            json!({"event":"tls_case","case":case,"success":out.status.success()})
        );
    }
    let from_capture = Command::new(env!("CARGO_BIN_EXE_db-sync-alt"))
        .args([
            "compare",
            "--a-capture",
            captured.to_str().unwrap(),
            "--b",
            &b,
            "--network",
            "mainnet",
            "--positions",
            "auto",
            "--b-tls-root-cert",
            &root,
            "--b-password-file",
            password.to_str().unwrap(),
        ])
        .output()
        .await?;
    assert!(
        from_capture.status.success(),
        "{}",
        String::from_utf8_lossy(&from_capture.stderr)
    );
    let report: Value = serde_json::from_slice(&from_capture.stdout)?;
    assert_eq!(report["settings_a"]["ssl"], true);
    assert_eq!(report["settings_b"]["ssl"], true);
    assert_eq!(report["matched"], true);
    println!("{}", json!({"event":"tls_source_capture","success":true}));
    assert_eq!(target.rows().await?, before);
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn compare_q19_incidental_candidate_literal_counterexamples() -> Result {
    for scenario in [
        "window_datum",
        "epoch_minus_two_spent",
        "old_origin_header",
        "funding_inputs",
    ] {
        let mut f = fixture(331);
        let address = dsa_store::catalogue::catalogue().candidate_addresses["mainnet"].clone();
        let height = match scenario {
            "window_datum" => 7,
            "epoch_minus_two_spent" => 1,
            "old_origin_header" => 0,
            _ => 3,
        };
        let out = f.output(
            height,
            12,
            20,
            "other",
            "cnight",
            if scenario == "window_datum" {
                Some(99011)
            } else {
                None
            },
        );
        f.chain.outputs.get_mut(&out).unwrap().address = address.clone();
        if scenario == "epoch_minus_two_spent" {
            f.spend(4, 30, 0, out);
        }
        if scenario == "funding_inputs" {
            f.spend(3, 12, 0, f.tags["ordinary_0_0"]);
        }
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
        target
            .cli("grant-reader", "mainnet", &["--role", &target.reader])
            .await?;
        let mut a = Reader::connect(source.reader_config(), None).await?;
        let mut b = Reader::connect(target.reader_config(), None).await?;
        let ta = a.snapshot().await?;
        let tb = b.snapshot().await?;
        let reference = if scenario == "epoch_minus_two_spent" {
            3
        } else {
            8
        };
        let catalogue = dsa_store::catalogue::catalogue();
        for name in [
            "get_utxos_for_address_tx_in_enabled",
            "get_utxos_for_address_tx_in_consumed",
        ] {
            let sql = catalogue
                .entries
                .iter()
                .find(|e| e.kind == "read" && e.name == name)
                .unwrap();
            let values = [Bind::Text(address.clone()), Bind::I32(reference)];
            let left = compare::rows(&ta, sql, &values).await?;
            let right = compare::rows(&tb, sql, &values).await?;
            assert_eq!(left.len(), 1, "{scenario}/{name}");
            if scenario == "funding_inputs" && name == "get_utxos_for_address_tx_in_consumed" {
                // The supported source keeps consumed_by_tx_id NULL, so this
                // inactive variant already has no funding detail on either DB.
                assert_eq!(left, right);
            } else {
                assert_ne!(
                    left, right,
                    "Q19 counterexample {scenario}/{name} disappeared"
                );
            }
            println!(
                "{}",
                json!({"event":"q19_literal_counterexample","scenario":scenario,"query":name,"sql_sha256":sql.sha256,"address":address,"reference_block":reference,"horizon":6,"source_rows":left,"converted_rows":right})
            );
        }
        ta.commit().await?;
        tb.commit().await?;
        let actual = cli(&source, &target, &f.profile, &[]).await?;
        assert_eq!(
            actual.status.code(),
            Some(2),
            "{scenario}: {}",
            String::from_utf8_lossy(&actual.stderr)
        );
        let report: Value = serde_json::from_slice(&actual.stdout)?;
        assert!(report["differences"].as_array().unwrap().iter().any(|d| {
            d["query"]
                .as_str()
                .unwrap()
                .starts_with("get_utxos_for_address_")
                && d["informational"] == false
        }));
        println!(
            "{}",
            json!({"event":"q19_cli_counterexample","scenario":scenario,"exit":actual.status.code()})
        );
        source.cleanup().await?;
        target.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn compare_optional_governance_descriptors_both_absent_and_one_sided() -> Result {
    let mut f = fixture(333);
    let roles = ["council", "technical_committee"];
    let fixed = roles.map(|role| f.asset_id(role));
    // Alternate-name tokens still answer the policy query. The fixed empty-name
    // descriptors are genuinely absent from this otherwise supported source.
    for (index, role) in roles.into_iter().enumerate() {
        let alternate = 9200 + index as i64;
        f.chain.assets.insert(
            alternate,
            dsa_chain::profiles::Asset {
                policy: f.profile.assets[role].policy.clone(),
                name: "fe".to_owned(),
            },
        );
        for row in f.chain.output_assets.values_mut() {
            if row.asset == fixed[index] {
                row.asset = alternate;
            }
        }
        f.chain.assets.remove(&fixed[index]);
    }
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
    target
        .cli("grant-reader", "mainnet", &["--role", &target.reader])
        .await?;
    let out = cli(&source, &target, &f.profile, &[]).await?;
    assert!(
        out.status.success(),
        "{} {}",
        String::from_utf8_lossy(&out.stderr),
        String::from_utf8_lossy(&out.stdout)
    );
    let policy = &f.profile.assets["council"].policy;
    target
        .write(&format!(
            "INSERT INTO serve.multi_asset VALUES(9800,decode('{policy}','hex'),''::bytea)"
        ))
        .await?;
    let out = cli(&source, &target, &f.profile, &[]).await?;
    assert_eq!(out.status.code(), Some(2));
    let report: Value = serde_json::from_slice(&out.stdout)?;
    assert!(report["differences"].as_array().unwrap().iter().any(|v| {
        v["query"].as_str().unwrap().starts_with("resolve_ident:") && v["informational"] == false
    }));
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn compare_capture_row_and_byte_caps_refuse_before_accumulation() -> Result {
    let (source, target, f) = setup(339).await?;
    let mut r = Reader::connect(source.reader_config(), None).await?;
    for (rows, bytes) in [(0, 1024), (1024, 1)] {
        let tx = r.snapshot().await?;
        let positions = Positions::generate(&tx, 6, 8, 500).await?;
        let error = compare::capture(
            &tx,
            &f.profile,
            &positions,
            &Options {
                max_rows: rows,
                max_bytes: bytes,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            dsa_store::Error::Refusal("comparison_capture_limit")
        ));
        tx.rollback().await?;
    }
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn compare_cli_capture_from_conversion_survives_source_advancement_and_integrity() -> Result {
    let mut f = fixture(345);
    f.reindex_ordinals();
    let source = Database::new().await?;
    source.legacy(&f).await?;
    let target = Database::new().await?;
    let dir = tempfile::tempdir()?;
    let file = dir.path().join("source.capture.json");
    let converted = target
        .cli(
            "convert",
            "mainnet",
            &[
                "--source",
                &source.dsn(true),
                "--horizon-block",
                "6",
                "--capture-source",
                file.to_str().unwrap(),
            ],
        )
        .await?;
    assert!(file.is_file());
    assert!(converted.get("source_capture").is_some());
    target
        .cli("grant-reader", "mainnet", &["--role", &target.reader])
        .await?;
    source.write(&format!("INSERT INTO chain.generations VALUES(2,1,'ready','generated',0,0,{}, {},'{{}}');INSERT INTO serve.block VALUES({},decode('{}','hex'),1084,999999,{},timestamp '2026-01-02',0);UPDATE chain.integrity_state SET projection_generation=2,serve_generation=2;UPDATE chain.service_state SET canonical_ready=2,publication_serial=publication_serial+1",f.bounds.target+1,f.bounds.tip+1,f.bounds.tip+1,"cd".repeat(32),f.bounds.tip+1)).await?;
    let output = Command::new(env!("CARGO_BIN_EXE_db-sync-alt"))
        .args([
            "compare",
            "--a-capture",
            file.to_str().unwrap(),
            "--b",
            &target.dsn(true),
            "--network",
            "mainnet",
            "--positions",
            "auto",
            "--verify-unbounded",
        ])
        .output()
        .await?;
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
    let report: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(report["matched"], true);
    assert_eq!(report["newest_a"], report["newest_b"]);
    assert_eq!(report["chain_order_checked_b"], true);
    let mut capture: Value = serde_json::from_slice(&std::fs::read(&file)?)?;
    let positions = dir.path().join("positions.json");
    std::fs::write(
        &positions,
        serde_json::to_vec(&capture["capture"]["positions"])?,
    )?;
    let fresh = Command::new(env!("CARGO_BIN_EXE_db-sync-alt"))
        .args([
            "compare",
            "--a",
            &source.dsn(true),
            "--b",
            &target.dsn(true),
            "--network",
            "mainnet",
            "--positions",
            positions.to_str().unwrap(),
        ])
        .output()
        .await?;
    assert_eq!(fresh.status.code(), Some(2));
    let cases = capture["capture"]["cases"].as_object_mut().unwrap();
    let case = cases
        .values_mut()
        .find(|v| !v["rows"].as_array().unwrap().is_empty())
        .unwrap();
    case["rows"][0] = json!("{\"tampered\":true}");
    let tampered = dir.path().join("tampered.capture.json");
    std::fs::write(&tampered, serde_json::to_vec(&capture)?)?;
    let output = Command::new(env!("CARGO_BIN_EXE_db-sync-alt"))
        .args([
            "compare",
            "--a-capture",
            tampered.to_str().unwrap(),
            "--b",
            &target.dsn(true),
            "--network",
            "mainnet",
            "--positions",
            "auto",
        ])
        .output()
        .await?;
    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&output.stderr).contains("source_capture_hash_mismatch"));
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn compare_preserves_json_numeric_spelling_but_normalizes_scalar_amounts() -> Result {
    let (source, target, f) = setup(347).await?;
    let mut r = Reader::connect(source.reader_config(), None).await?;
    let tx = r.snapshot().await?;
    let make = |sql: &str| dsa_store::catalogue::Statement {
        id: "own-json-scalar-regression".to_owned(),
        name: "own_json_scalar_regression".to_owned(),
        sha256: "own".to_owned(),
        sql: sql.to_owned(),
        kind: "read".to_owned(),
        group: "test".to_owned(),
        parameters: vec![],
        sources: vec![],
    };
    let integer=compare::rows(&tx,&make("SELECT 1::numeric AS amount, '{\"int\":1}'::jsonb AS datum, '{\"value\":1}'::jsonb AS metadata"),&[]).await?;
    let scaled_scalar=compare::rows(&tx,&make("SELECT 1.00::numeric AS amount, '{\"int\":1}'::jsonb AS datum, '{\"value\":1}'::jsonb AS metadata"),&[]).await?;
    assert_eq!(integer, scaled_scalar);
    let scaled_datum=compare::rows(&tx,&make("SELECT 1::numeric AS amount, '{\"int\":1.0}'::jsonb AS datum, '{\"value\":1}'::jsonb AS metadata"),&[]).await?;
    assert_ne!(integer, scaled_datum);
    let scaled_metadata=compare::rows(&tx,&make("SELECT 1::numeric AS amount, '{\"int\":1}'::jsonb AS datum, '{\"value\":1.0}'::jsonb AS metadata"),&[]).await?;
    assert_ne!(integer, scaled_metadata);
    let json_scalar_integer = compare::rows(&tx, &make("SELECT '1'::jsonb AS datum"), &[]).await?;
    let json_scalar_decimal =
        compare::rows(&tx, &make("SELECT '1.0'::jsonb AS datum"), &[]).await?;
    assert_ne!(json_scalar_integer, json_scalar_decimal);
    tx.commit().await?;
    let output = f.tags["council_8"];
    let hash = f.chain.outputs[&output].datum_hash.as_ref().unwrap();
    target.write(&format!("UPDATE serve.datum SET value=jsonb_build_object('constructor',0,'fields',jsonb_build_array(jsonb_build_object('int',108.0::numeric))) WHERE hash=decode('{hash}','hex')")).await?;
    let actual = cli(&source, &target, &f.profile, &[]).await?;
    assert_eq!(actual.status.code(), Some(2));
    let report: Value = serde_json::from_slice(&actual.stdout)?;
    assert!(report["differences"].as_array().unwrap().iter().any(|v| {
        v["query"]
            .as_str()
            .unwrap()
            .starts_with("get_governance_body_utxo:")
            && v["informational"] == false
    }));
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn compare_interrupt_and_deadline_cancel_own_queries_and_release_snapshots() -> Result {
    use std::time::{Duration, Instant};
    let (source, target, f) = setup(349).await?;
    for signal in ["TERM", "INT", "deadline"] {
        let barrier = source.barrier().await?;
        let mut command = Command::new(env!("CARGO_BIN_EXE_db-sync-alt"));
        command.args([
            "compare",
            "--a",
            &source.dsn(true),
            "--b",
            &target.dsn(true),
            "--network",
            "mainnet",
            "--positions",
            "auto",
            "--horizon-block",
            "6",
        ]);
        if signal == "deadline" {
            command.args(["--overall-timeout-seconds", "1"]);
        }
        let child = command
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()?;
        let pid = child.id().unwrap();
        let began = Instant::now();
        loop {
            let waiting:bool=source.admin.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=$1 AND application_name='db-sync-alt-compare' AND wait_event='advisory')",&[&source.name]).await?.get(0);
            if waiting {
                break;
            }
            if began.elapsed() > Duration::from_secs(10) {
                return Err("compare did not reach own read barrier".into());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let stop = Instant::now();
        if signal != "deadline" {
            assert!(
                Command::new("kill")
                    .args([&format!("-{signal}"), &pid.to_string()])
                    .status()
                    .await?
                    .success()
            );
        }
        let output =
            tokio::time::timeout(Duration::from_secs(10), child.wait_with_output()).await??;
        assert_eq!(output.status.code(), Some(1));
        let text = String::from_utf8_lossy(&output.stderr);
        assert!(
            text.contains(if signal == "deadline" {
                "operation_deadline_exceeded"
            } else {
                "operation_interrupted"
            }),
            "{text}"
        );
        assert!(text.contains("cancel_request"), "{text}");
        loop {
            let remaining:i64=source.admin.query_one("SELECT count(*) FROM pg_stat_activity WHERE datname=ANY($1::text[]) AND application_name='db-sync-alt-compare'",&[&vec![source.name.clone(),target.name.clone()]]).await?.get(0);
            if remaining == 0 {
                break;
            }
            if stop.elapsed() > Duration::from_secs(5) {
                return Err("interrupted comparison left a backend/snapshot".into());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        barrier
            .query_one("SELECT pg_advisory_unlock(726,1)", &[])
            .await?;
        drop(barrier);
        println!(
            "{}",
            json!({"event":"comparison_shutdown","signal":signal,"release_seconds":stop.elapsed().as_secs_f64()})
        );
    }
    let output = cli(&source, &target, &f.profile, &[]).await?;
    assert!(output.status.success());
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn compare_event_anchors_real_transaction_witness_and_full_window_coverage() -> Result {
    let mut f = fixture(352);
    f.reindex_ordinals();
    let source = Database::new().await?;
    source.legacy(&f).await?;
    // A valid32-byte all-zero fixture hash collides with the first miss probe.
    source.write("UPDATE serve.tx SET hash=decode(repeat('00',32),'hex') WHERE id=(SELECT min(id) FROM serve.tx WHERE id BETWEEN 6::bigint*4096 AND 8::bigint*4096+4095)").await?;
    let target = Database::new().await?;
    target
        .cli(
            "convert",
            "preprod",
            &["--source", &source.dsn(true), "--horizon-block", "6"],
        )
        .await?;
    target
        .cli("grant-reader", "preprod", &["--role", &target.reader])
        .await?;
    let output = cli(&source, &target, &f.profile, &[]).await?;
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
    let report: Value = serde_json::from_slice(&output.stdout)?;
    let positions = &report["positions"];
    assert!(
        positions["pairs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["cursor"]["block"] == 6
                && p["cursor"]["tx_index"] == 0
                && p["reference"]["block"] == 8)
    );
    assert!(positions["event_hint_queries"].as_u64().unwrap() > 0);
    assert!(positions["witness_tx_hash"].as_str().unwrap().len() == 64);
    let anchors = positions["event_anchors"].as_array().unwrap();
    for unit in [
        f.profile.assets["cnight"].unit(),
        f.profile.assets["bridge"].unit(),
    ] {
        assert!(
            anchors
                .iter()
                .any(|v| v["kind"] == "asset_create" && v["unit"] == unit)
        );
        assert!(
            report["coverage_a"]
                .as_object()
                .unwrap()
                .iter()
                .any(|(query, v)| query.starts_with("get_asset_creates:")
                    && v["unit_rows"][&unit].as_u64().is_some_and(|n| n > 0))
        );
    }
    assert!(report["transaction_hit_available_a"].as_bool().unwrap());
    assert!(report["transaction_hit_available_b"].as_bool().unwrap());
    assert_eq!(report["bridge_probe"]["metadata_key"], 6500973);
    assert_ne!(report["missing_tx_hash"], "00".repeat(32));
    assert_eq!(positions["witness_tx_hash"], "00".repeat(32));
    let witness = positions["witness_tx_hash"].as_str().unwrap();
    let row=connect(&source.config).await?.query_one("SELECT EXISTS(SELECT 1 FROM serve.tx t JOIN serve.block b ON b.id=t.block_id WHERE t.hash=decode($1,'hex'))",&[&witness]).await?;
    assert!(row.get::<_, bool>(0));
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn compare_more_than_five_hundred_pairs_and_quiet_event_classes() -> Result {
    let mut f = fixture(353);
    f.bounds.target = 610;
    f.bounds.tip = 610 + f.profile.cardano.security_parameter;
    for height in 0..=f.bounds.tip {
        f.chain
            .blocks
            .entry(i64::from(height))
            .or_insert(dsa_chain::keep::Header {
                epoch: Some((height / 2) as i32),
                slot: Some(i64::from(height) * 100 + 1),
            });
    }
    f.chain.epochs.extend(0..=305);
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
    target
        .cli("grant-reader", "mainnet", &["--role", &target.reader])
        .await?;
    let began = std::time::Instant::now();
    let output = cli(&source, &target, &f.profile, &[]).await?;
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
    let report: Value = serde_json::from_slice(&output.stdout)?;
    assert!(report["positions"]["pairs"].as_array().unwrap().len() >= 500);
    assert!(
        report["coverage_a"]
            .as_object()
            .unwrap()
            .iter()
            .any(|(query, v)| query.starts_with("get_asset_creates:")
                && v["paired_nonempty_cases"].as_u64().is_some_and(|v| v > 0))
    );
    // Most of this generated window is quiet. It remains a valid comparison;
    // absence of events in a class/interval is not fabricated into a hit.
    assert_eq!(report["matched"], true);
    println!(
        "{}",
        json!({"event":"generated_500_pair_comparison","pairs":report["positions"]["pairs"].as_array().unwrap().len(),"executions":report["executions"],"wall_seconds":began.elapsed().as_secs_f64(),"event_anchors":report["positions"]["event_anchors"].as_array().unwrap().len()})
    );
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn compare_cli_refuses_helper_sentinel_that_violates_the_native_guarantee() -> Result {
    let (source, target, f) = setup(355).await?;
    let c = connect(&source.config).await?;
    let original: String = c
        .query_one(
            "SELECT pg_get_functiondef('chain.gate()'::regprocedure)",
            &[],
        )
        .await?
        .get(0);
    // A deliberately faulty source produces empty helper subqueries only. The
    // original node SQL is unchanged; the independent relationship read sees
    // the real preceding facts and must reject the fabricated zero sentinel.
    let altered = original.replace(
        " RETURN true;",
        " IF position('low_tx.tx_id' in current_query())>0 THEN RETURN false; END IF; RETURN true;",
    );
    assert_ne!(altered, original);
    c.batch_execute(&altered).await?;
    let output = cli(&source, &target, &f.profile, &[]).await?;
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("comparison_invalid_bounds"));
    c.batch_execute(&original).await?;
    assert!(
        cli(&source, &target, &f.profile, &[])
            .await?
            .status
            .success()
    );
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn compare_reader_limits_fill_zero_and_preserve_positive_owner_settings() -> Result {
    let (source, target, _) = setup(357).await?;
    for (statement, idle, expected_statement, expected_idle) in
        [(0, 0, 60000i64, 60000i64), (1234, 2345, 1234, 2345)]
    {
        source.admin.batch_execute(&format!("ALTER ROLE {} SET statement_timeout='{statement}ms';ALTER ROLE {} SET idle_in_transaction_session_timeout='{idle}ms'",source.reader,source.reader)).await?;
        let mut reader = Reader::connect(source.reader_config(), None).await?;
        let tx = reader.snapshot().await?;
        let row = tx.query_one("SELECT (extract(epoch FROM current_setting('statement_timeout')::interval)*1000)::bigint,(extract(epoch FROM current_setting('idle_in_transaction_session_timeout')::interval)*1000)::bigint", &[]).await?;
        assert_eq!(row.get::<_, i64>(0), expected_statement);
        assert_eq!(row.get::<_, i64>(1), expected_idle);
        tx.commit().await?;
        let legacy = LegacySource::connect(source.reader_config(), None).await?;
        let row = legacy.client().query_one("SELECT (extract(epoch FROM current_setting('statement_timeout')::interval)*1000)::bigint,(extract(epoch FROM current_setting('idle_in_transaction_session_timeout')::interval)*1000)::bigint", &[]).await?;
        assert_eq!(row.get::<_, i64>(0), expected_statement);
        assert_eq!(row.get::<_, i64>(1), expected_idle);
    }
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn compare_peer_backend_failure_cancels_the_other_active_query() -> Result {
    use std::time::{Duration, Instant};
    let (source, target, f) = setup(359).await?;
    let left_barrier = source.barrier().await?;
    let right_barrier = connect(&target.config).await?;
    let original: String = right_barrier
        .query_one("SELECT pg_get_functiondef('dsa.gate()'::regprocedure)", &[])
        .await?
        .get(0);
    let altered = original.replace(" RETURN true;", " IF position('to_jsonb' in current_query())>0 THEN PERFORM pg_advisory_xact_lock(726,1); END IF; RETURN true;");
    assert_ne!(altered, original);
    right_barrier.batch_execute(&altered).await?;
    right_barrier
        .query_one("SELECT pg_advisory_lock(726,1)", &[])
        .await?;
    let child = Command::new(env!("CARGO_BIN_EXE_db-sync-alt"))
        .args([
            "compare",
            "--a",
            &source.dsn(true),
            "--b",
            &target.dsn(true),
            "--network",
            &f.profile.network,
            "--positions",
            "auto",
            "--horizon-block",
            "6",
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let began = Instant::now();
    let right_pid = loop {
        let waiting=source.admin.query("SELECT datname,pid FROM pg_stat_activity WHERE datname=ANY($1::text[]) AND application_name='db-sync-alt-compare' AND wait_event='advisory'",&[&vec![source.name.clone(),target.name.clone()]]).await?;
        if waiting.len() == 2 {
            break waiting
                .iter()
                .find(|r| r.get::<_, String>(0) == target.name)
                .unwrap()
                .get::<_, i32>(1);
        }
        if began.elapsed() > Duration::from_secs(10) {
            return Err("both comparison queries did not reach their owned barriers".into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let stop = Instant::now();
    assert!(
        source
            .admin
            .query_one("SELECT pg_terminate_backend($1)", &[&right_pid])
            .await?
            .get::<_, bool>(0)
    );
    let output = tokio::time::timeout(Duration::from_secs(10), child.wait_with_output()).await??;
    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&output.stderr).contains("cancel_request"));
    loop {
        let left:i64=source.admin.query_one("SELECT count(*) FROM pg_stat_activity WHERE datname=ANY($1::text[]) AND application_name='db-sync-alt-compare'",&[&vec![source.name.clone(),target.name.clone()]]).await?.get(0);
        if left == 0 {
            break;
        }
        if stop.elapsed() > Duration::from_secs(5) {
            return Err("peer backend failure left a comparison backend/snapshot".into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    println!(
        "{}",
        json!({"event":"comparison_peer_backend_failure","release_seconds":stop.elapsed().as_secs_f64()})
    );
    left_barrier
        .query_one("SELECT pg_advisory_unlock(726,1)", &[])
        .await?;
    right_barrier
        .query_one("SELECT pg_advisory_unlock(726,1)", &[])
        .await?;
    drop(left_barrier);
    drop(right_barrier);
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn compare_source_capture_path_preflight_preserves_pristine_destination() -> Result {
    let mut f = fixture(361);
    f.reindex_ordinals();
    let source = Database::new().await?;
    source.legacy(&f).await?;
    let target = Database::new().await?;
    let before = target.catalog().await?;
    let dir = tempfile::tempdir()?;
    let existing = dir.path().join("existing.capture");
    std::fs::write(&existing, b"preserved")?;
    let missing = dir.path().join("absent/observations.json");
    for (file, error) in [
        (&existing, "source_capture_exists"),
        (&missing, "invalid_source_capture_path"),
    ] {
        target
            .refusal(
                &f.profile.network,
                &source.dsn(true),
                &[
                    "--horizon-block",
                    "6",
                    "--capture-source",
                    file.to_str().unwrap(),
                ],
                error,
            )
            .await?;
        assert_eq!(before, target.catalog().await?);
        assert_eq!(std::fs::read(&existing)?, b"preserved");
    }
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}
