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
    assert_eq!(target.rows().await?, before);
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}
