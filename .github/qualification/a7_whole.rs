//! Hosted generated performance qualification; not historical/field evidence.
#[path = "common/legacy.rs"]
#[allow(dead_code)]
mod common;
use common::{Database, connect, fixture};
use dsa_chain::{
    assets::fixed_units,
    keep::{Bounds, TABLES},
};
use dsa_store::{convert::LegacySource, keep::Selection};
use futures_util::{StreamExt, pin_mut};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    error::Error,
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::process::Command;
use tokio_postgres::{Config, config::SslMode};
type Result<T = ()> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
#[test]
fn qualification_dispatch_smoke() {
    assert_eq!(env!("CARGO_PKG_NAME"), "db-sync-alt");
    assert!(env!("CARGO_BIN_EXE_db-sync-alt").ends_with("/db-sync-alt"));
    println!("qualification_dispatch_smoke_entered_no_database");
}
fn directory() -> PathBuf {
    PathBuf::from(std::env::var("DSA_QUALIFICATION_OUT").unwrap())
}
fn save(name: &str, value: &Value) -> Result {
    std::fs::write(directory().join(name), serde_json::to_vec_pretty(value)?)?;
    Ok(())
}
fn event(value: Value) {
    println!("{value}");
}
async fn at_root(variable: &str) -> Result<Database> {
    let mut root: Config = std::env::var(variable)?.parse()?;
    let admin = connect(&root).await?;
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let owner = format!("whole_owner_{nonce:x}");
    let reader = format!("whole_reader_{nonce:x}");
    let name = format!("whole_{nonce:x}");
    admin.batch_execute(&format!("CREATE ROLE {owner} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;CREATE ROLE {reader} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 36;ALTER ROLE {reader} SET default_transaction_read_only=on;ALTER ROLE {reader} SET search_path=public;ALTER ROLE {reader} SET statement_timeout='60s';ALTER ROLE {reader} SET idle_in_transaction_session_timeout='60s'")).await?;
    admin
        .batch_execute(&format!("CREATE DATABASE {name} OWNER {owner}"))
        .await?;
    root.user(&owner).dbname(&name).ssl_mode(SslMode::Disable);
    Ok(Database {
        admin,
        config: root,
        owner,
        reader,
        name,
    })
}
async fn command(
    database: &Database,
    name: &str,
    verb: &str,
    args: &[&str],
) -> Result<(bool, Value)> {
    let begin = Instant::now();
    let start = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs_f64();
    let restore_tcp = if verb == "restore" {
        std::env::var("DSA_RESTORE_TCP_USER_TIMEOUT_SECONDS")
            .ok()
            .map(|value| value.parse::<u32>())
            .transpose()?
    } else {
        None
    };
    if let Some(seconds) = restore_tcp {
        assert!((1..=86400).contains(&seconds));
    }
    let mut dsn = database.dsn(false);
    if let Some(seconds) = restore_tcp {
        dsn.push_str(&format!(" tcp_user_timeout={seconds}"));
    }
    let mut child = Command::new(env!("CARGO_BIN_EXE_db-sync-alt"));
    child.args([verb, "--database", &dsn, "--network", "mainnet"]);
    child
        .args(args)
        .stdout(Stdio::from(std::fs::File::create(
            directory().join(format!("{name}.stdout")),
        )?))
        .stderr(Stdio::from(std::fs::File::create(
            directory().join(format!("{name}.stderr")),
        )?))
        .kill_on_drop(true);
    save(
        "active-command.json",
        &json!({"name":name,"started_unix":start,"database":database.name,"restore_only_tcp_user_timeout_seconds":restore_tcp}),
    )?;
    let status = child.spawn()?.wait().await?;
    let elapsed = begin.elapsed().as_secs_f64();
    let raw = std::fs::read(directory().join(format!("{name}.stdout")))?;
    let result = serde_json::from_slice(&raw).unwrap_or(Value::Null);
    save(
        &format!("{name}.measurement.json"),
        &json!({"name":name,"exit_code":status.code(),"success":status.success(),"wall_seconds":elapsed,"started_unix":start,"finished_unix":SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs_f64(),"restore_only_tcp_user_timeout_seconds":restore_tcp}),
    )?;
    event(
        json!({"event":"qualification_command","name":name,"success":status.success(),"seconds":elapsed}),
    );
    Ok((status.success(), result))
}
async fn compare(
    capture: &Path,
    source: Option<&Database>,
    database: &Database,
    name: &str,
) -> Result<Value> {
    let started = Instant::now();
    let mut child = Command::new(env!("CARGO_BIN_EXE_db-sync-alt"));
    child.arg("compare");
    if let Some(source) = source {
        child.args([
            "--a",
            &source.dsn(true),
            "--positions",
            directory()
                .join("captured-positions.json")
                .to_str()
                .unwrap(),
        ]);
    } else {
        child.args([
            "--a-capture",
            capture.to_str().unwrap(),
            "--positions",
            "auto",
        ]);
    }
    let output = child
        .args([
            "--b",
            &database.dsn(true),
            "--network",
            "mainnet",
            "--statement-timeout-seconds",
            "60",
            "--idle-transaction-timeout-seconds",
            "60",
            "--overall-timeout-seconds",
            "7200",
            "--verify-unbounded",
        ])
        .output()
        .await?;
    std::fs::write(directory().join(format!("{name}.stdout")), &output.stdout)?;
    std::fs::write(directory().join(format!("{name}.stderr")), &output.stderr)?;
    save(
        &format!("{name}.measurement.json"),
        &json!({"wall_seconds":started.elapsed().as_secs_f64(),"exit_code":output.status.code()}),
    )?;
    assert!(
        output.status.success(),
        "{name} failed ({:?}); stdout={} stderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(value["matched"], true);
    assert_eq!(value["chain_order_checked_b"], true);
    assert_eq!(value["unbounded_checked_b"], true);
    assert_eq!(value["unbounded_checked_a"], source.is_some());
    assert!(value["positions"]["pairs"].as_array().unwrap().len() >= 500);
    Ok(value)
}
async fn fingerprint(database: &Database) -> Result<Value> {
    let c = connect(&database.config).await?;
    let mut counts = BTreeMap::new();
    let mut sizes = BTreeMap::new();
    for table in TABLES {
        counts.insert(
            table,
            c.query_one(&format!("SELECT count(*) FROM serve.{table}"), &[])
                .await?
                .get::<_, i64>(0),
        );
        sizes.insert(
            table,
            c.query_one(
                "SELECT pg_total_relation_size($1::text::regclass)",
                &[&format!("serve.{table}")],
            )
            .await?
            .get::<_, i64>(0),
        );
    }
    let newest: Value = c
        .query_one(
            "SELECT to_jsonb(b) FROM serve.block b ORDER BY id DESC LIMIT 1",
            &[],
        )
        .await?
        .get(0);
    let bytes: i64 = c
        .query_one("SELECT pg_database_size(current_database())", &[])
        .await?
        .get(0);
    Ok(json!({"counts":counts,"relations_bytes":sizes,"database_bytes":bytes,"newest":newest}))
}
#[tokio::test]
#[ignore = "hosted generated whole qualification only"]
async fn whole_conversion_qualification() -> Result {
    std::fs::create_dir_all(directory())?;
    let divisor: i64 = std::env::var("DSA_WHOLE_DIVISOR")
        .unwrap_or_else(|_| "1".into())
        .parse()?;
    assert!([1, 100].contains(&divisor));
    let headers = 1_202_855 / divisor;
    let wanted_outputs = 2_630_986 / divisor;
    let wanted_assets = 2_630_984 / divisor;
    let wanted_spends = 2_386_344 / divisor;
    let wanted_datums = 1_036_071 / divisor;
    let offset = 13_200_000i64;
    let tip = offset + headers - 1;
    let target = tip - 2160;
    let horizon = target - 30_009.min((target - offset) / 2);
    let span = target - offset - 17;
    let mut f = fixture(401);
    // Translation already re-ranks the full legacy ordinal set using translated hashes.
    f.translate_heights(offset as u32);
    f.bounds = Bounds {
        horizon: horizon as u32,
        target: target as u32,
        tip: tip as u32,
    };
    let source = Database::new().await?;
    source.legacy(&f).await?;
    let target_db = at_root("DSA_DEST_DATABASE").await?;
    let restored_db = at_root("DSA_RESTORED_DATABASE").await?;
    save(
        "databases.json",
        &json!({"source":source.name,"destination":target_db.name,"restored":restored_db.name,"source_owner":source.owner,"source_reader":source.reader,"destination_owner":target_db.owner,"destination_reader":target_db.reader}),
    )?;
    let initial = connect(&source.config).await?;
    let mut expected_supply: BTreeMap<String, String> = fixed_units(&f.profile)
        .into_iter()
        .map(|u| (u, "0".into()))
        .collect();
    for row in initial.query("SELECT encode(m.policy,'hex')||encode(m.name,'hex'),coalesce(sum(a.quantity),0)::text FROM serve.multi_asset m LEFT JOIN serve.ma_tx_out a ON a.ident=m.id LEFT JOIN serve.tx_out o ON o.id=a.tx_out_id WHERE NOT EXISTS(SELECT 1 FROM serve.tx_in i WHERE i.tx_out_id=o.tx_id AND i.tx_out_index=o.index) GROUP BY m.id,m.policy,m.name",&[]).await?{let unit:String=row.get(0);if expected_supply.contains_key(&unit){expected_supply.insert(unit,row.get(1));}}
    let original = fingerprint(&source).await?;
    let datum_id_base: i64 = initial
        .query_one("SELECT coalesce(max(id),0)+10000 FROM serve.datum", &[])
        .await?
        .get(0);
    let n_outputs = wanted_outputs - original["counts"]["tx_out"].as_i64().unwrap();
    let n_assets = wanted_assets - original["counts"]["ma_tx_out"].as_i64().unwrap();
    let n_spends = wanted_spends - original["counts"]["tx_in"].as_i64().unwrap();
    let n_datums = wanted_datums - original["counts"]["datum"].as_i64().unwrap();
    assert!(n_assets <= n_outputs);
    let cn_unit = f.profile.assets["cnight"].unit();
    expected_supply.insert(
        cn_unit.clone(),
        (expected_supply[&cn_unit].parse::<i128>()?
            + i128::from(n_assets - n_assets * n_spends / n_outputs))
        .to_string(),
    );
    let unit = f.asset_id("cnight");
    let mapping = &f.profile.addresses["mapping"];
    let began = Instant::now();
    source.write(&format!("UPDATE serve.block SET epoch_no=600+(id-{offset})/25000,slot_no=id*20,time=timestamp '2026-01-01'+(id-{offset})*interval '20seconds'")).await?;
    for low in (offset..=tip).step_by(25_000) {
        let high = (low + 24999).min(tip);
        source.write(&format!("INSERT INTO serve.block SELECT n,decode(md5('whole-block/'||n::text)||md5('whole-block2/'||n::text),'hex'),600+(n-{offset})/25000,n*20,n,timestamp '2026-01-01'+(n-{offset})*interval '20seconds',102 FROM generate_series({low}::bigint,{high}::bigint)n ON CONFLICT DO NOTHING")).await?;
    }
    for low in (0..n_datums).step_by(10_000) {
        let high = (low + 9999).min(n_datums - 1);
        source.write(&format!("INSERT INTO serve.datum SELECT {datum_id_base}+n,decode(md5('whole-datum/'||n::text)||md5('whole-datum2/'||n::text),'hex'),CASE WHEN n%1000=0 THEN NULL WHEN n%1000=10 THEN 'null'::jsonb ELSE jsonb_build_object('constructor',0,'fields',jsonb_build_array(jsonb_build_object('int',n),jsonb_build_object('bytes',(SELECT string_agg(md5(n::text||'/'||j::text),'') FROM generate_series(0,CASE WHEN n%1000=2 THEN 1023 WHEN n%100=3 THEN 255 WHEN n%25=4 THEN 63 ELSE 15 END)j)))) END FROM generate_series({low}::bigint,{high}::bigint)n")).await?;
        if low % 100000 == 0 {
            event(
                json!({"event":"datum_population","rows":high+1,"seconds":began.elapsed().as_secs_f64()}),
            );
        }
    }
    for low in (0..n_outputs).step_by(25_000) {
        let high = (low + 24999).min(n_outputs - 1);
        let h = format!("({offset}+13+n*{span}/{n_outputs})");
        let spent = format!("((n+1)*{n_spends}/{n_outputs}>n*{n_spends}/{n_outputs})");
        source.write(&format!("WITH coordinates AS(SELECT DISTINCT {h} h FROM generate_series({low}::bigint,{high}::bigint)n) INSERT INTO serve.tx SELECT h*4096+100,decode(lpad(to_hex(h),16,'0')||repeat('ab',24),'hex'),h,100,true FROM coordinates ON CONFLICT DO NOTHING;
 INSERT INTO serve.tx_out SELECT {h}*16777216+100*4096+n%4,{h}*4096+100,(n%4)::smallint,CASE WHEN n%100=0 OR n%1000=10 OR n>={n_assets} THEN '{mapping}' ELSE 'holder-'||(n%10007)::text END,1,decode(md5('whole-datum/'||(n%{n_datums})::text)||md5('whole-datum2/'||(n%{n_datums})::text),'hex'),NULL FROM generate_series({low}::bigint,{high}::bigint)n;
 INSERT INTO serve.ma_tx_out SELECT ({h}*16777216+100*4096+n%4)*4096,1,{h}*16777216+100*4096+n%4,{unit} FROM generate_series({low}::bigint,least({high}::bigint,{n_assets}::bigint-1))n;
 WITH coordinates AS(SELECT DISTINCT {h}+1 h FROM generate_series({low}::bigint,{high}::bigint)n WHERE {spent}) INSERT INTO serve.tx SELECT h*4096+101,decode(lpad(to_hex(h),16,'0')||repeat('cd',24),'hex'),h,101,true FROM coordinates ON CONFLICT DO NOTHING;
 INSERT INTO serve.tx_in SELECT ({h}+1)*16777216+101*4096+n%4,({h}+1)*4096+101,{h}*4096+100,(n%4)::smallint FROM generate_series({low}::bigint,{high}::bigint)n WHERE {spent}")).await?;
        if low % 100000 == 0 {
            event(
                json!({"event":"output_population","rows":high+1,"seconds":began.elapsed().as_secs_f64()}),
            );
        }
    }
    let pools = (2802 / divisor).max(6);
    let stakes = 132350 / divisor;
    let owner = connect(&source.config).await?;
    source.write(&format!("DELETE FROM serve.epoch_stake;DELETE FROM serve.pool_hash;DELETE FROM serve.epoch_param;
 INSERT INTO serve.epoch_param SELECT 600+e,decode(md5('whole-epoch/'||e::text)||md5('whole-epoch2/'||e::text),'hex'),16384 FROM generate_series(0,((({tip}-{offset})/25000))::integer)e;
 INSERT INTO serve.pool_hash SELECT 1000+n,substring(decode(md5('whole-pool/'||n::text)||md5('whole-pool2/'||n::text),'hex') FROM 1 FOR 28) FROM generate_series(0,{pools}-1)n;
 INSERT INTO serve.epoch_stake SELECT 10000+n,1000+(n%{pools}),100000+n,(600+(n/{pools})%49)::integer FROM generate_series(0,{stakes}-1)n;
 UPDATE chain.generations SET target_height={target},witness_height={tip} WHERE id=1")).await?;
    // Epoch stake rows must be unique on (epoch,pool); smoke adapts its epoch count.
    let duplicate:bool=owner.query_one("SELECT EXISTS(SELECT 1 FROM serve.epoch_stake GROUP BY epoch_no,pool_id HAVING count(*)>1)",&[]).await?.get(0);
    assert!(!duplicate);
    for table in TABLES {
        owner
            .batch_execute(&format!("ANALYZE serve.{table}"))
            .await?;
    }
    let ready = fingerprint(&source).await?;
    assert_eq!(ready["counts"]["block"], headers);
    assert_eq!(ready["counts"]["tx_out"], wanted_outputs);
    assert_eq!(ready["counts"]["ma_tx_out"], wanted_assets);
    assert_eq!(ready["counts"]["tx_in"], wanted_spends);
    assert_eq!(ready["counts"]["datum"], wanted_datums);
    assert_eq!(
        ready["counts"]["tx"].as_i64().unwrap(),
        original["counts"]["tx"].as_i64().unwrap() + 2 * span,
        "independent generated creation/spend transaction formula"
    );
    let mut expected_raw = ready["counts"].clone();
    // All controlled initial metadata belongs to old pre-window transactions.
    expected_raw["tx_metadata"] = json!(0);
    save(
        "expected-raw-families.json",
        &json!({"rows":expected_raw,"scope":"same-snapshot transport bounds: block through P, tx/output/asset/spend through T, metadata H..T; catalogue/datum/epoch families complete","independent_formula_counts":{"block":headers,"tx_out":wanted_outputs,"ma_tx_out":wanted_assets,"tx_in":wanted_spends,"datum":wanted_datums,"tx":original["counts"]["tx"].as_i64().unwrap()+2*span}}),
    )?;
    let datum_sizes:Value=owner.query_one("SELECT jsonb_build_object('rows',count(*),'sql_null',count(*) FILTER(WHERE value IS NULL),'json_null',count(*) FILTER(WHERE value='null'::jsonb),'json_utf8_bytes',sum(octet_length(value::text)),'column_bytes',sum(pg_column_size(value)),'maximum_json_utf8_bytes',max(octet_length(value::text))) FROM serve.datum",&[]).await?.get(0);
    let window:Value=owner.query_one(&format!("SELECT jsonb_build_object('outputs',(SELECT count(*) FROM serve.tx_out WHERE id>={horizon}::bigint*16777216),'spends',(SELECT count(*) FROM serve.tx_in WHERE id>={horizon}::bigint*16777216),'old_unspent',(SELECT count(*) FROM serve.tx_out o WHERE id<{horizon}::bigint*16777216 AND NOT EXISTS(SELECT 1 FROM serve.tx_in i WHERE i.tx_out_id=o.tx_id AND i.tx_out_index=o.index)))"),&[]).await?.get(0);
    assert!(window["outputs"].as_i64().unwrap() > 1000);
    assert!(window["spends"].as_i64().unwrap() > 1000);
    save(
        "fixture.json",
        &json!({"kind":"generated_not_field_evidence","divisor":divisor,"fingerprint":ready,"datum_sizes":datum_sizes,"expected_supply":expected_supply,"window":window,"H":horizon,"T":target,"P":tip,"setup_seconds":began.elapsed().as_secs_f64(),"generated_datum_id_base":datum_id_base,"generated_spends_exact":n_spends,"generated_unspent_asset_formula":n_assets-(n_assets*n_spends/n_outputs),"source_chain_tables":"legacy DDL present but no full chain-copy population; served view cardinalities only","datum_distribution":"deterministic high-entropy 512/2048/8192/32768hex-character bands; controlled SQL/JSON nulls; synthetic not empirical"}),
    )?;
    save(
        "fixture-ready.json",
        &json!({"ready":true,"source_database":source.name}),
    )?;
    let wait = Instant::now();
    while !directory().join("io-ready.json").is_file() {
        if wait.elapsed() > Duration::from_secs(1200) {
            return Err("hosted IO admission did not complete".into());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let settings:Value=connect(&source.reader_config()).await?.query_one("SELECT jsonb_build_object('statement_timeout',current_setting('statement_timeout'),'idle',current_setting('idle_in_transaction_session_timeout'),'read_only',current_setting('default_transaction_read_only'))",&[]).await?.get(0);
    assert_eq!(settings["statement_timeout"], "1min");
    assert_eq!(settings["idle"], "1min");
    save("reader-settings.json", &settings)?;
    let mode = std::env::var("DSA_WHOLE_MODE").unwrap_or_else(|_| "qualify".into());
    let snapshot = directory().join("converted.snapshot");
    let capture = directory().join("source.capture.json");
    let h = horizon.to_string();
    let source_dsn = source.dsn(true);
    let base = [
        "--source",
        source_dsn.as_str(),
        "--horizon-block",
        h.as_str(),
        "--source-statement-timeout-seconds",
        "60",
        "--source-idle-transaction-timeout-seconds",
        "60",
        "--statement-timeout-seconds",
        "600",
        "--idle-transaction-timeout-seconds",
        "7200",
        "--overall-timeout-seconds",
        "7200",
    ];
    let mut args = base.to_vec();
    if mode != "diagnose" {
        args.extend([
            "--capture-source",
            capture.to_str().unwrap(),
            "--snapshot",
            snapshot.to_str().unwrap(),
        ]);
    }
    let (ok, converted) = command(&target_db, "convert", "convert", &args).await?;
    if mode == "diagnose" {
        if !ok {
            target_db.closed_empty().await?;
        }
        let selection = Selection::new(&f.profile, f.bounds)?;
        let sql = selection.conversion_rows_sql("tx")?;
        let mut legacy = LegacySource::connect(source.reader_config(), None).await?;
        let tx = legacy.snapshot().await?;
        let plan: Value = tx
            .query_one(
                &format!("EXPLAIN(SETTINGS,FORMAT JSON) {sql}"),
                &selection.parameters(),
            )
            .await?
            .get(0);
        save("predecessor-tx-plan.json", &plan)?;
        let began = Instant::now();
        let stream = tx.query_raw(&sql, selection.parameters()).await?;
        pin_mut!(stream);
        let mut rows = 0u64;
        let mut bytes = 0u64;
        let mut first = None;
        let mut failed = None;
        while let Some(row) = stream.next().await {
            match row {
                Ok(row) => {
                    if first.is_none() {
                        first = Some(began.elapsed().as_secs_f64());
                    }
                    let raw: &str = row.get(0);
                    bytes += raw.len() as u64;
                    rows += 1;
                }
                Err(error) => {
                    failed = Some(
                        error
                            .code()
                            .map(|c| c.code())
                            .unwrap_or("transport")
                            .to_owned(),
                    );
                    break;
                }
            }
        }
        save(
            "predecessor-tx-delivery.json",
            &json!({"source_timeout_seconds":60,"first_row_seconds":first,"delivered_rows":rows,"delivered_json_utf8_bytes":bytes,"elapsed_seconds":began.elapsed().as_secs_f64(),"sqlstate":failed,"sink":"drain immediately, no destination backpressure","actual_archive_tx_delivery":"UNKNOWN; this is a separate generated execution"}),
        )?;
        drop(stream);
        drop(tx);
        drop(legacy);
    } else {
        assert!(
            ok,
            "whole conversion failed, exact logs and generated DBs retained until hosted teardown"
        );
        save("conversion.json", &converted)?;
        for table in TABLES {
            assert_eq!(
                converted["source"]["raw_families"][table]["rows"], expected_raw[table],
                "independent raw source cardinality {table}"
            );
        }
        let capture_value: Value = serde_json::from_slice(&std::fs::read(&capture)?)?;
        save(
            "captured-positions.json",
            &capture_value["capture"]["positions"],
        )?;
        assert_eq!(
            converted["verified"]["supply"],
            json!(expected_supply),
            "independent generated formula supply"
        );
        let (ok, verified) = command(
            &target_db,
            "verify",
            "verify",
            &[
                "--statement-timeout-seconds",
                "600",
                "--idle-transaction-timeout-seconds",
                "7200",
            ],
        )
        .await?;
        assert!(ok);
        assert_eq!(
            verified["content_digest"],
            converted["verified"]["content_digest"]
        );
        let (ok, snapshot_result) = command(
            &target_db,
            "snapshot",
            "snapshot",
            &[
                "--file",
                directory().join("explicit.snapshot").to_str().unwrap(),
                "--statement-timeout-seconds",
                "600",
                "--idle-transaction-timeout-seconds",
                "7200",
            ],
        )
        .await?;
        assert!(ok);
        save("snapshot.json", &snapshot_result)?;
        // Independent converted proof is earned even if a later restore fails.
        target_db
            .cli("grant-reader", "mainnet", &["--role", &target_db.reader])
            .await?;
        let converted_comparison = compare(&capture, None, &target_db, "compare-converted").await?;
        save("compare-converted.json", &converted_comparison)?;
        let converted_wide = compare(
            &capture,
            Some(&source),
            &target_db,
            "compare-converted-frozen-source-wide",
        )
        .await?;
        save("compare-converted-frozen-source-wide.json", &converted_wide)?;
        save(
            "converted-fingerprint.json",
            &fingerprint(&target_db).await?,
        )?;
        let (ok, restored) = command(
            &restored_db,
            "restore",
            "restore",
            &[
                "--file",
                snapshot.to_str().unwrap(),
                "--statement-timeout-seconds",
                "600",
                "--idle-transaction-timeout-seconds",
                "7200",
            ],
        )
        .await?;
        assert!(ok);
        assert_eq!(restored["content_digest"], verified["content_digest"]);
        assert_eq!(restored["state_digest"], verified["state_digest"]);
        let (ok, restored_verified) = command(
            &restored_db,
            "verify-restored",
            "verify",
            &[
                "--statement-timeout-seconds",
                "600",
                "--idle-transaction-timeout-seconds",
                "7200",
            ],
        )
        .await?;
        assert!(ok);
        assert_eq!(
            restored_verified["content_digest"],
            restored["content_digest"]
        );
        assert_eq!(restored_verified["state_digest"], restored["state_digest"]);
        save("restored-verification.json", &restored_verified)?;
        for (db, name) in [(&restored_db, "compare-restored")] {
            db.cli("grant-reader", "mainnet", &["--role", &db.reader])
                .await?;
            let comparison = compare(&capture, None, db, name).await?;
            save(&format!("{name}.json"), &comparison)?;
            let wide_name = format!("{name}-frozen-source-wide");
            let wide = compare(&capture, Some(&source), db, &wide_name).await?;
            save(&format!("{wide_name}.json"), &wide)?;
        }
        save(
            "restored-fingerprint.json",
            &fingerprint(&restored_db).await?,
        )?;
        let after = fingerprint(&source).await?;
        save("source-after.json", &after)?;
        let unchanged = after["counts"] == ready["counts"] && after["newest"] == ready["newest"];
        assert!(unchanged, "source logical fingerprint changed");
        save(
            "result.json",
            &json!({"status":"PASS_GENERATED_ONLY","source_unchanged":unchanged,"snapshot_bytes":std::fs::metadata(&snapshot)?.len(),"capture_bytes":std::fs::metadata(&capture)?.len(),"units":fixed_units(&f.profile),"historical_acceptance":false}),
        )?;
    }
    source.cleanup().await?;
    target_db.cleanup().await?;
    restored_db.cleanup().await?;
    Ok(())
}
