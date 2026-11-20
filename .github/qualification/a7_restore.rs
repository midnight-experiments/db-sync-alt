//! Standalone generated selected-sized snapshot/restore diagnostics.
#[path = "common/legacy.rs"]
#[allow(dead_code)]
mod common;
use common::{Database, connect, fixture};
use dsa_chain::keep::TABLES;
use serde_json::{Value, json};
use std::{
    error::Error,
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::process::Command;
use tokio_postgres::{Config, config::SslMode};
type Result<T = ()> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
fn directory() -> PathBuf {
    PathBuf::from(std::env::var("DSA_QUALIFICATION_OUT").unwrap())
}
fn unix() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}
fn save(name: &str, value: &Value) -> Result {
    let p = directory().join(name);
    let t = p.with_extension("tmp");
    std::fs::write(&t, serde_json::to_vec_pretty(value)?)?;
    std::fs::rename(t, p)?;
    Ok(())
}
async fn handshake(name: &str, value: Value) -> Result<Value> {
    let nonce = format!("{}-{name}", unix());
    let mut request = value;
    request["nonce"] = json!(nonce);
    save(&format!("{name}-request.json"), &request)?;
    let start = Instant::now();
    loop {
        if let Ok(raw) = std::fs::read(directory().join(format!("{name}-ready.json"))) {
            if let Ok(v) = serde_json::from_slice::<Value>(&raw) {
                if v["nonce"] == nonce {
                    return Ok(v);
                }
            }
        }
        assert!(
            start.elapsed() < Duration::from_secs(1200),
            "host handshake timeout {name}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
async fn exec(
    binary: &str,
    db: &Database,
    label: &str,
    verb: &str,
    suffix: &str,
    args: &[&str],
) -> Result<(bool, Value)> {
    let start = Instant::now();
    let at = unix();
    save(
        "active-command.json",
        &json!({"name":label,"started_unix":at}),
    )?;
    let dsn = format!("{}{}", db.dsn(false), suffix);
    let mut command = Command::new(binary);
    command.args([
        verb,
        "--database",
        &dsn,
        "--network",
        "mainnet",
        "--statement-timeout-seconds",
        "600",
        "--idle-transaction-timeout-seconds",
        "7200",
    ]);
    if verb == "convert" {
        command.args(["--overall-timeout-seconds", "7200"]);
    }
    let status = command
        .args(args)
        .stdout(Stdio::from(std::fs::File::create(
            directory().join(format!("{label}.stdout")),
        )?))
        .stderr(Stdio::from(std::fs::File::create(
            directory().join(format!("{label}.stderr")),
        )?))
        .kill_on_drop(true)
        .spawn()?
        .wait()
        .await?;
    save(
        &format!("{label}.measurement.json"),
        &json!({"name":label,"started_unix":at,"finished_unix":unix(),"seconds":start.elapsed().as_secs_f64(),"exit_code":status.code(),"success":status.success(),"owner_statement_seconds":600,"owner_idle_seconds":7200,"owner_tcp_override_seconds":if suffix.is_empty(){Value::Null}else{json!(600)}}),
    )?;
    let raw = std::fs::read(directory().join(format!("{label}.stdout")))?;
    let value = serde_json::from_slice(&raw).unwrap_or(Value::Null);
    println!(
        "{}",
        json!({"event":"restore_trial_command","name":label,"success":status.success(),"seconds":start.elapsed().as_secs_f64()})
    );
    Ok((status.success(), value))
}
async fn compare(
    binary: &str,
    source: &Database,
    target: &Database,
    capture: &Path,
    label: &str,
    frozen: bool,
) -> Result<bool> {
    let start = Instant::now();
    let mut c = Command::new(binary);
    c.arg("compare");
    if frozen {
        c.args([
            "--a",
            &source.dsn(true),
            "--positions",
            directory().join("mini-positions.json").to_str().unwrap(),
        ]);
    } else {
        c.args([
            "--a-capture",
            capture.to_str().unwrap(),
            "--positions",
            "auto",
        ]);
    }
    let o = c
        .args([
            "--b",
            &target.dsn(true),
            "--network",
            "mainnet",
            "--verify-unbounded",
            "--statement-timeout-seconds",
            "60",
            "--idle-transaction-timeout-seconds",
            "60",
            "--overall-timeout-seconds",
            "7200",
        ])
        .output()
        .await?;
    std::fs::write(directory().join(format!("{label}.stdout")), &o.stdout)?;
    std::fs::write(directory().join(format!("{label}.stderr")), &o.stderr)?;
    let v: Value = serde_json::from_slice(&o.stdout).unwrap_or(Value::Null);
    save(
        &format!("{label}.measurement.json"),
        &json!({"exit_code":o.status.code(),"seconds":start.elapsed().as_secs_f64(),"matched":v["matched"],"positions":v["positions"]["pairs"].as_array().map(Vec::len),"source_wide":v["unbounded_checked_a"],"destination_wide":v["unbounded_checked_b"]}),
    )?;
    Ok(o.status.success() && v["matched"] == true)
}
fn timedout(value: &Value) -> bool {
    match value {
        Value::Object(o) => {
            o.get("os_error") == Some(&json!(110))
                || o.get("kind") == Some(&json!("TimedOut"))
                || o.values().any(timedout)
        }
        Value::Array(a) => a.iter().any(timedout),
        _ => false,
    }
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
#[tokio::test]
#[ignore = "standalone hosted restore diagnostics"]
async fn restore_focus() -> Result {
    tokio::time::timeout(Duration::from_secs(7200), restore_focus_inner()).await??;
    Ok(())
}
async fn restore_focus_inner() -> Result {
    let predecessor = std::env::var("DSA_PREDECESSOR_CLI")?;
    let diagnostic = env!("CARGO_BIN_EXE_db-sync-alt");
    let began = Instant::now();
    let offset = 13_200_000i64;
    let horizon = offset + 95_000;
    let target = horizon + 30_009;
    let tip = target + 2160;
    let mut f = fixture(401);
    f.translate_heights(offset as u32);
    f.bounds = dsa_chain::keep::Bounds {
        horizon: horizon as u32,
        target: target as u32,
        tip: tip as u32,
    };
    let source = Database::new().await?;
    source.legacy(&f).await?;
    let converted = at_root("DSA_DEST_DATABASE").await?;
    save(
        "databases.json",
        &json!({"source":source.name,"destination":converted.name}),
    )?;
    let mapping = &f.profile.addresses["mapping"];
    let unit = f.asset_id("cnight");
    let new_datums = 3374i64;
    let datum_base = f.chain.datums.keys().max().copied().unwrap_or(0) + 10000;
    source.write(&format!("UPDATE serve.block SET epoch_no=600+(id-{offset})/25000,slot_no=id*20,time=timestamp '2026-01-01'+(id-{offset})*interval '20seconds' ")).await?;
    for low in (offset..=tip).step_by(25000) {
        let high = (low + 24999).min(tip);
        source.write(&format!("INSERT INTO serve.block SELECT h,decode(md5('mini-block/'||h::text)||md5('mini-block2/'||h::text),'hex'),600+(h-{offset})/25000,h*20,h,timestamp '2026-01-01'+(h-{offset})*interval '20seconds',104 FROM generate_series({low}::bigint,{high}::bigint)h ON CONFLICT DO NOTHING")).await?;
    }
    source.write(&format!("INSERT INTO serve.datum SELECT {datum_base}+n,decode(md5('mini-datum/'||n::text)||md5('mini-datum2/'||n::text),'hex'),CASE WHEN n%1000=0 THEN NULL WHEN n%1000=10 THEN 'null'::jsonb ELSE jsonb_build_object('constructor',0,'fields',jsonb_build_array(jsonb_build_object('int',n),jsonb_build_object('bytes',(SELECT string_agg(md5(n::text||'/'||j::text),'') FROM generate_series(0,CASE WHEN n%1000=2 THEN 1023 WHEN n%100=3 THEN 255 WHEN n%25=4 THEN 63 ELSE 15 END)j)))) END FROM generate_series(0,{new_datums}-1)n")).await?;
    let old = 245000i64;
    let old_span = horizon - offset - 3000;
    for low in (0..old).step_by(25000) {
        let high = (low + 24999).min(old - 1);
        let h = format!("({offset}+3000+n%{old_span})");
        let t = format!("({h}*4096+100+n/{old_span})");
        source.write(&format!("INSERT INTO serve.tx SELECT {t},decode(lpad(to_hex({t}),16,'0')||repeat('ab',24),'hex'),{h},(100+n/{old_span})::integer,true FROM generate_series({low}::bigint,{high}::bigint)n;INSERT INTO serve.tx_out SELECT {t}*4096,{t},0,CASE WHEN n<{new_datums} OR n%100=0 THEN '{mapping}' ELSE 'holder-'||(n%10007)::text END,1,decode(md5('mini-datum/'||(n%{new_datums})::text)||md5('mini-datum2/'||(n%{new_datums})::text),'hex'),NULL FROM generate_series({low}::bigint,{high}::bigint)n;INSERT INTO serve.ma_tx_out SELECT {t}*16777216,1,{t}*4096,{unit} FROM generate_series({low}::bigint,{high}::bigint)n")).await?;
    }
    let window = 65000i64;
    let spends = 60000i64;
    let span = target - horizon - 5;
    for low in (0..window).step_by(25000) {
        let high = (low + 24999).min(window - 1);
        let h = format!("({horizon}+n*{span}/{window})");
        let spent = format!("((n+1)*{spends}/{window}>n*{spends}/{window})");
        source.write(&format!("WITH heights AS(SELECT DISTINCT {h} h FROM generate_series({low}::bigint,{high}::bigint)n)INSERT INTO serve.tx SELECT h*4096+100,decode(lpad(to_hex(h*4096+100),16,'0')||repeat('cd',24),'hex'),h,100,true FROM heights ON CONFLICT DO NOTHING;INSERT INTO serve.tx_out SELECT {h}*16777216+100*4096+n%4,{h}*4096+100,(n%4)::smallint,CASE WHEN n%100=0 THEN '{mapping}' ELSE 'holder-'||(n%10007)::text END,1,decode(md5('mini-datum/'||((n+{old})%{new_datums})::text)||md5('mini-datum2/'||((n+{old})%{new_datums})::text),'hex'),NULL FROM generate_series({low}::bigint,{high}::bigint)n;INSERT INTO serve.ma_tx_out SELECT ({h}*16777216+100*4096+n%4)*4096,1,{h}*16777216+100*4096+n%4,{unit} FROM generate_series({low}::bigint,{high}::bigint)n;WITH heights AS(SELECT DISTINCT {h}+1 h FROM generate_series({low}::bigint,{high}::bigint)n WHERE {spent})INSERT INTO serve.tx SELECT h*4096+101,decode(lpad(to_hex(h*4096+101),16,'0')||repeat('ef',24),'hex'),h,101,true FROM heights ON CONFLICT DO NOTHING;INSERT INTO serve.tx_in SELECT ({h}+1)*16777216+101*4096+n%4,({h}+1)*4096+101,{h}*4096+100,(n%4)::smallint FROM generate_series({low}::bigint,{high}::bigint)n WHERE {spent}")).await?;
    }
    source.write(&format!("DELETE FROM serve.epoch_stake;DELETE FROM serve.pool_hash;DELETE FROM serve.epoch_param;INSERT INTO serve.epoch_param SELECT 600+e,decode(md5('mini-epoch/'||e::text)||md5('mini-epoch2/'||e::text),'hex'),16384 FROM generate_series(0,(({tip}-{offset})/25000)::integer)e;INSERT INTO serve.pool_hash SELECT 1000+n,substring(decode(md5('mini-pool/'||n::text)||md5('mini-pool2/'||n::text),'hex') FROM 1 FOR 28)FROM generate_series(0,2801)n;INSERT INTO serve.epoch_stake SELECT 10000+n,1000+n%2802,100000+n,(601+n/2802)::integer FROM generate_series(0,9061)n;UPDATE chain.generations SET target_height={target},witness_height={tip} WHERE id=1")).await?;
    let owner = connect(&source.config).await?;
    let mut counts = std::collections::BTreeMap::new();
    for table in TABLES {
        owner
            .batch_execute(&format!("ANALYZE serve.{table}"))
            .await?;
        let n: i64 = owner
            .query_one(&format!("SELECT count(*) FROM serve.{table}"), &[])
            .await?
            .get(0);
        counts.insert(table, n);
    }
    save(
        "restore-fixture.json",
        &json!({"kind":"generated_selected_sized_not_full_source","source_counts":counts,"H":horizon,"T":target,"P":tip,"old_unspent_generated":old,"window_outputs_generated":window,"window_spends_generated":spends,"datum_payload_bands":"same512/2048/8192/32768hex,SQLNULL/JSONnull asfullrecipe;3374newdatums","setup_seconds":began.elapsed().as_secs_f64()}),
    )?;
    save("fixture-ready.json", &json!({"ready":true}))?;
    let wait = Instant::now();
    while !directory().join("io-ready.json").exists() {
        assert!(wait.elapsed() < Duration::from_secs(1200));
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let snapshot = directory().join("representative.snapshot");
    let capture = directory().join("source.capture.json");
    let h = horizon.to_string();
    let src = source.dsn(true);
    let reuse = std::env::var("DSA_RESTORE_REUSE").is_ok();
    let (a, b, verified) = if reuse {
        let prior_fixture: Value =
            serde_json::from_slice(&std::fs::read(directory().join("reused-fixture.json"))?)?;
        let new_fixture: Value =
            serde_json::from_slice(&std::fs::read(directory().join("restore-fixture.json"))?)?;
        assert_eq!(prior_fixture["source_counts"], new_fixture["source_counts"]);
        for key in ["H", "T", "P"] {
            assert_eq!(prior_fixture[key], new_fixture[key]);
        }
        let value: Value = serde_json::from_slice(&std::fs::read(
            directory().join("reused-mini-conversion.json"),
        )?)?;
        save(
            "mini-converted-parity.json",
            &json!({"skipped":"original converted database was not retained; retained normal snapshot reused, no source reconversion"}),
        )?;
        (Value::Null, Value::Null, value["verified"].clone())
    } else {
        let (ok, converted_value) = exec(
            &predecessor,
            &converted,
            "mini-convert",
            "convert",
            "",
            &[
                "--source",
                &src,
                "--source-statement-timeout-seconds",
                "60",
                "--source-idle-transaction-timeout-seconds",
                "60",
                "--horizon-block",
                &h,
                "--snapshot",
                snapshot.to_str().unwrap(),
                "--capture-source",
                capture.to_str().unwrap(),
            ],
        )
        .await?;
        assert!(ok, "normal predecessor fixtureconversion failed");
        save("mini-conversion.json", &converted_value)?;
        let (ok, verified) =
            exec(&predecessor, &converted, "mini-verify", "verify", "", &[]).await?;
        assert!(ok);
        assert_eq!(
            verified["content_digest"],
            converted_value["verified"]["content_digest"]
        );
        let file: Value = serde_json::from_slice(&std::fs::read(&capture)?)?;
        save("mini-positions.json", &file["capture"]["positions"])?;
        converted
            .cli("grant-reader", "mainnet", &["--role", &converted.reader])
            .await?;
        let a = compare(
            &predecessor,
            &source,
            &converted,
            &capture,
            "mini-converted-native",
            false,
        )
        .await?;
        let b = compare(
            &predecessor,
            &source,
            &converted,
            &capture,
            "mini-converted-frozen-wide",
            true,
        )
        .await?;
        save(
            "mini-converted-parity.json",
            &json!({"native":a,"frozen_source_wide":b}),
        )?;
        (json!(a), json!(b), verified)
    };
    let file: Value = serde_json::from_slice(&std::fs::read(&capture)?)?;
    save("mini-positions.json", &file["capture"]["positions"])?;
    let mut variants = Vec::new();
    let mut timeout_supported = false;
    for (label, binary, suffix) in [
        ("predecessor-default30", predecessor.as_str(), ""),
        ("diagnostic-default30", diagnostic, ""),
        (
            "predecessor-owner600",
            predecessor.as_str(),
            " tcp_user_timeout=600",
        ),
    ] {
        if label == "predecessor-owner600" && !timeout_supported {
            variants.push(json!({"label":label,"skipped":"no measured timeout cause/kernel evidence in earlierdefault30attempts"}));
            continue;
        }
        let reset = handshake("target-reset", json!({"label":label,"role":"restored"})).await?;
        assert_eq!(reset["ready"], true);
        let dest = at_root("DSA_RESTORED_DATABASE").await?;
        save(
            "databases.json",
            &json!({"source":source.name,"destination":converted.name,"restored":dest.name}),
        )?;
        let trace = handshake(
            "socket-trace",
            json!({"action":"start","label":label,"role":"restored"}),
        )
        .await?;
        save(&format!("{label}.trace-start.json"), &trace)?;
        let name = format!("restore-{label}");
        let (ok, value) = exec(
            binary,
            &dest,
            &name,
            "restore",
            suffix,
            &["--file", snapshot.to_str().unwrap()],
        )
        .await?;
        let trace = handshake(
            "socket-trace",
            json!({"action":"stop","label":label,"role":"restored"}),
        )
        .await?;
        save(&format!("{label}.trace-stop.json"), &trace)?;
        let stderr = std::fs::read_to_string(directory().join(format!("{name}.stderr")))?;
        let safe: Vec<Value> = stderr
            .lines()
            .filter_map(|l| serde_json::from_str(l).ok())
            .collect();
        let driver_timeout = safe.iter().any(timedout);
        let kernel_timeout = trace["tcp_abort_on_timeout_delta"].as_u64().unwrap_or(0) > 0;
        timeout_supported |= !ok && (driver_timeout || kernel_timeout);
        let mut item = json!({"label":label,"success":ok,"value":value,"driver_timeout_errno110":driver_timeout,"kernel_timeout":kernel_timeout,"trace":trace,"diagnostic_events":safe});
        if ok {
            assert_eq!(value["content_digest"], verified["content_digest"]);
            assert_eq!(value["state_digest"], verified["state_digest"]);
            let (checked, independently_verified) = exec(
                binary,
                &dest,
                &format!("{label}-verify"),
                "verify",
                suffix,
                &[],
            )
            .await?;
            assert!(checked);
            assert_eq!(
                independently_verified["content_digest"],
                verified["content_digest"]
            );
            assert_eq!(
                independently_verified["state_digest"],
                verified["state_digest"]
            );
            dest.cli("grant-reader", "mainnet", &["--role", &dest.reader])
                .await?;
            let n = compare(
                &predecessor,
                &source,
                &dest,
                &capture,
                &format!("{label}-native"),
                false,
            )
            .await?;
            let w = compare(
                &predecessor,
                &source,
                &dest,
                &capture,
                &format!("{label}-frozen-wide"),
                true,
            )
            .await?;
            item["parity"] = json!({"native":n,"frozen_source_wide":w});
        } else {
            let client = connect(&dest.config).await?;
            let state: Value = client
                .query_one("SELECT to_jsonb(p)FROM dsa.position p", &[])
                .await?
                .get(0);
            item["failed_state"] = state;
            let mut rows = std::collections::BTreeMap::new();
            for t in TABLES {
                rows.insert(
                    t,
                    client
                        .query_one(&format!("SELECT count(*)FROM serve.{t}"), &[])
                        .await?
                        .get::<_, i64>(0),
                );
            }
            item["failed_rows"] = json!(rows);
        }
        save(&format!("{label}.result.json"), &item)?;
        variants.push(item);
        drop(dest);
    }
    save(
        "restore-trial-result.json",
        &json!({"status":"EXPERIMENT_COMPLETED_NOT_FULL_QUALIFICATION","converted_parity":{"native":a,"wide":b},"reused_portable_snapshot":reuse,"variants":variants,"snapshot_bytes":std::fs::metadata(&snapshot)?.len(),"capture_bytes":std::fs::metadata(&capture)?.len(),"snapshot_producer":"immutable63normalCLI","all_data_generated":true,"source_after_counts_not_re_read":true}),
    )?;
    source.cleanup().await?;
    converted.cleanup().await?;
    Ok(())
}
