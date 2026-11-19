//! Hosted controlled index experiment; never a product qualification verdict.
#[path = "common/legacy.rs"]
#[allow(dead_code)]
mod common;
use common::{Database, connect, fixture};
use serde_json::{Value, json};
use std::{
    error::Error,
    path::PathBuf,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
type Result<T = ()> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
fn out() -> PathBuf {
    PathBuf::from(std::env::var("DSA_QUALIFICATION_OUT").unwrap())
}
fn save(name: &str, value: &Value) -> Result {
    let path = out().join(name);
    let temporary = path.with_extension("json.tmp");
    std::fs::write(&temporary, serde_json::to_vec_pretty(value)?)?;
    std::fs::rename(temporary, path)?;
    Ok(())
}
fn unix() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}
async fn settings(c: &tokio_postgres::Client) -> Result<Value> {
    Ok(c.query_one("SELECT jsonb_build_object('statement_timeout',current_setting('statement_timeout'),'idle_in_transaction_session_timeout',current_setting('idle_in_transaction_session_timeout'),'work_mem',current_setting('work_mem'),'maintenance_work_mem',current_setting('maintenance_work_mem'),'jit',current_setting('jit'),'temp_buffers',current_setting('temp_buffers'),'shared_buffers',current_setting('shared_buffers'),'server_version',current_setting('server_version'))",&[]).await?.get(0))
}
async fn reclaim(label: &str) -> Result<Value> {
    let nonce = format!("{}-{label}", unix());
    save(
        "reclaim-request.json",
        &json!({"nonce":nonce,"bytes":536870912,"label":label}),
    )?;
    let began = Instant::now();
    loop {
        if let Ok(bytes) = std::fs::read(out().join("reclaim-result.json")) {
            if let Ok(v) = serde_json::from_slice::<Value>(&bytes) {
                if v["nonce"] == nonce {
                    assert_eq!(
                        v["matched_cold_admitted"], true,
                        "cold conditions not established: {v}"
                    );
                    return Ok(v);
                }
            }
        }
        assert!(
            began.elapsed() < Duration::from_secs(300),
            "scoped reclaim did not complete"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
#[tokio::test]
#[ignore = "hosted controlled private index experiment"]
async fn private_index_focus() -> Result {
    let began = Instant::now();
    let mut f = fixture(401);
    f.translate_heights(13_200_000);
    let rows = f.rows();
    let database = Database::new().await?;
    save("databases.json", &json!({"destination":database.name}))?;
    let c = connect(&database.config).await?;
    c.batch_execute("SET application_name='dsa-index-focus';SET statement_timeout='600s';SET idle_in_transaction_session_timeout='7200s'").await?;
    c.batch_execute(include_str!("../../dsa-store/src/layout.sql"))
        .await?;
    c.batch_execute(
        "BEGIN;CREATE TEMP TABLE tx_out(LIKE serve.tx_out INCLUDING ALL) ON COMMIT DROP",
    )
    .await?;
    c.execute(
        "INSERT INTO pg_temp.tx_out SELECT * FROM jsonb_populate_recordset(NULL::serve.tx_out,$1)",
        &[&json!(rows["tx_out"])],
    )
    .await?;
    let count = 2_630_986i64;
    let n_outputs = count - f.chain.outputs.len() as i64;
    let n_assets = 2_630_984i64 - f.chain.output_assets.len() as i64;
    let n_datums = 1_036_071i64 - f.chain.datums.len() as i64;
    let offset = 13_200_000i64;
    let tip = offset + 1_202_855 - 1;
    let target = tip - 2160;
    let span = target - offset - 17;
    let mapping = &f.profile.addresses["mapping"];
    for low in (0..n_outputs).step_by(25000) {
        let high = (low + 24999).min(n_outputs - 1);
        let h = format!("({offset}+13+n*{span}/{n_outputs})");
        c.batch_execute(&format!("INSERT INTO pg_temp.tx_out SELECT {h}*16777216+100*4096+n%4,{h}*4096+100,(n%4)::smallint,CASE WHEN n%100=0 OR n%1000=10 OR n>={n_assets} THEN '{mapping}' ELSE 'holder-'||(n%10007)::text END,1,decode(md5('whole-datum/'||(n%{n_datums})::text)||md5('whole-datum2/'||(n%{n_datums})::text),'hex'),NULL FROM generate_series({low}::bigint,{high}::bigint)n")).await?;
    }
    // Reproduce the two successful output lookup indexes preceding address in the shipped loop.
    c.batch_execute("CREATE UNIQUE INDEX conversion_serve_tx_out_tx_index ON pg_temp.tx_out(tx_id,index);CREATE INDEX conversion_idx_tx_out_tx_id ON pg_temp.tx_out(tx_id)").await?;
    let actual: i64 = c
        .query_one("SELECT count(*) FROM pg_temp.tx_out", &[])
        .await?
        .get(0);
    assert_eq!(actual, count);
    let distribution:Value=c.query_one("SELECT jsonb_build_object('rows',count(*),'distinct_addresses',count(DISTINCT address),'min_address_bytes',min(octet_length(address)),'max_address_bytes',max(octet_length(address)),'mapping_rows',count(*)FILTER(WHERE address=$1),'relation_bytes',pg_total_relation_size('pg_temp.tx_out')) FROM pg_temp.tx_out",&[mapping]).await?.get(0);
    let histogram:Value=c.query_one("SELECT jsonb_agg(to_jsonb(h)) FROM(SELECT address,count(*) rows FROM pg_temp.tx_out GROUP BY address ORDER BY rows DESC,address LIMIT 20)h",&[]).await?.get(0);
    save(
        "index-fixture.json",
        &json!({"kind":"generated_index_only_not_whole_qualification","rows":actual,"seed":401,"translated_offset":offset,"n_outputs":n_outputs,"n_assets":n_assets,"n_datums":n_datums,"address_distribution":distribution,"top20":histogram,"settings":settings(&c).await?,"setup_seconds":began.elapsed().as_secs_f64(),"cache_scope":"tx_out plus PK and preceding2lookupindexes only; cold matched by exact owned-cgroup reclaim, not claimed identical to original full working set"}),
    )?;
    save("fixture-ready.json", &json!({"ready":true}))?;
    let wait = Instant::now();
    while !out().join("io-ready.json").exists() {
        assert!(wait.elapsed() < Duration::from_secs(300));
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let mut trials = Vec::new();
    let mut hash120_ok = false;
    let mut hash_failed_120 = false;
    let mut btree120_ok = false;
    for (label, method, analyze, cold, timeout) in [
        ("hash-warm-120", "hash", false, false, 120),
        ("hash-cold-120", "hash", false, true, 120),
        ("hash-cold-600", "hash", false, true, 600),
        ("btree-cold-120", "btree", false, true, 120),
        ("btree-cold-600", "btree", false, true, 600),
        ("hash-analyzed-cold-120", "hash", true, true, 120),
        ("btree-analyzed-cold-120", "btree", true, true, 120),
    ] {
        if (label == "hash-cold-600" && hash120_ok && !hash_failed_120)
            || (label == "btree-cold-600" && btree120_ok)
        {
            trials.push(json!({"label":label,"skipped":"120s trial already completed; no longer limit needed"}));
            continue;
        }
        c.batch_execute("SAVEPOINT focus_trial;SET LOCAL statement_timeout='600s'")
            .await?;
        let analyze_began = Instant::now();
        if analyze {
            c.batch_execute("ANALYZE pg_temp.tx_out").await?;
        }
        let analyze_seconds = if analyze {
            analyze_began.elapsed().as_secs_f64()
        } else {
            0.0
        };
        let cold_evidence = if cold {
            reclaim(label).await?
        } else {
            json!({"scope":"warm/prior-index state; not cold and not exact original working set"})
        };
        c.batch_execute(&format!("SET LOCAL statement_timeout='{timeout}s'"))
            .await?;
        let stats:Value=c.query_one("SELECT jsonb_build_object('reltuples',reltuples,'relpages',relpages,'relallvisible',relallvisible,'heap_bytes',pg_relation_size(oid),'total_bytes',pg_total_relation_size(oid)) FROM pg_class WHERE oid='pg_temp.tx_out'::regclass",&[]).await?.get(0);
        let settings = settings(&c).await?;
        let sql = format!("CREATE INDEX focus_address ON pg_temp.tx_out USING {method}(address)");
        let started = unix();
        save(
            "active-command.json",
            &json!({"name":label,"started_unix":started,"sql":sql}),
        )?;
        let clock = Instant::now();
        let result = c.batch_execute(&sql).await;
        let seconds = clock.elapsed().as_secs_f64();
        let error = result
            .as_ref()
            .err()
            .map(|e| json!({"text":e.to_string(),"sqlstate":e.code().map(|s|s.code())}));
        let mut measurement = json!({"label":label,"method":method,"analyze":analyze,"analyze_seconds":analyze_seconds,"cold":cold,"cold_evidence":cold_evidence,"timeout_seconds":timeout,"settings":settings,"before_stats":stats,"started_unix":started,"finished_unix":unix(),"wall_seconds":seconds,"success":result.is_ok(),"error":error});
        if result.is_ok() {
            let index_bytes: i64 = c
                .query_one("SELECT pg_relation_size('pg_temp.focus_address')", &[])
                .await?
                .get(0);
            measurement["index_bytes"] = json!(index_bytes);
            for (address, key) in [
                (mapping.as_str(), "matching"),
                ("index-focus-absent-controlled", "miss"),
            ] {
                let plan:Value=c.query_one("EXPLAIN(ANALYZE,BUFFERS,SETTINGS,FORMAT JSON)SELECT count(*) FROM pg_temp.tx_out WHERE address=$1",&[&address]).await?.get(0);
                let count: i64 = c
                    .query_one(
                        "SELECT count(*) FROM pg_temp.tx_out WHERE address=$1",
                        &[&address],
                    )
                    .await?
                    .get(0);
                if key == "matching" {
                    assert_eq!(count, distribution["mapping_rows"].as_i64().unwrap())
                } else {
                    assert_eq!(count, 0)
                }
                measurement[key] = json!({"rows":count,"plan":plan});
            }
        }
        c.batch_execute("ROLLBACK TO SAVEPOINT focus_trial;RELEASE SAVEPOINT focus_trial")
            .await?;
        if method == "hash" && timeout == 120 && result.is_err() {
            hash_failed_120 = true;
        }
        if label == "hash-cold-120" {
            hash120_ok = result.is_ok();
        }
        if label == "btree-cold-120" {
            btree120_ok = result.is_ok();
        }
        save(&format!("{label}.json"), &measurement)?;
        println!(
            "{}",
            json!({"event":"private_index_trial","measurement":measurement})
        );
        trials.push(measurement);
    }
    save(
        "index-result.json",
        &json!({"status":"EXPERIMENT_COMPLETED_NOT_PRODUCT_QUALIFICATION","trials":trials,"elapsed_seconds":began.elapsed().as_secs_f64(),"full_conversion_proven":false}),
    )?;
    c.batch_execute("ROLLBACK").await?;
    drop(c);
    database.cleanup().await?;
    Ok(())
}
