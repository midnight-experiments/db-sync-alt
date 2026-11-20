//! Small generated whole-key cost comparison, not independent-model or archive proof.
#[path = "../../dsa-store/src/canonical.rs"]
#[allow(dead_code)]
mod canonical;
#[path = "common/legacy.rs"]
#[allow(dead_code)]
mod common;
#[path = "../../dsa-store/src/keep.rs"]
#[allow(dead_code)]
mod keep;
use dsa_store::Error;
const NODE_RELATIONS: [&str; 11] = dsa_store::NODE_RELATIONS;
use common::{Database, connect, fixture};
use dsa_chain::{
    digest::FamilyHasher,
    keep::{Bounds, TABLES},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    error::Error as StdError,
    path::PathBuf,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
type Result<T = ()> = std::result::Result<T, Box<dyn StdError + Send + Sync>>;
fn out() -> PathBuf {
    PathBuf::from(std::env::var("DSA_QUALIFICATION_OUT").unwrap())
}
fn save(name: &str, value: &Value) -> Result {
    std::fs::write(out().join(name), serde_json::to_vec_pretty(value)?)?;
    Ok(())
}
fn unix() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}
#[test]
fn keys_dispatch_smoke() {
    assert_eq!(env!("CARGO_PKG_NAME"), "db-sync-alt");
    assert!(env!("CARGO_BIN_EXE_db-sync-alt").ends_with("/db-sync-alt"));
    println!("keys_dispatch_smoke_entered_no_database");
}
#[tokio::test]
#[ignore = "manual bounded generated whole-key comparison"]
async fn keys_focus() -> Result {
    tokio::time::timeout(Duration::from_secs(900), keys_inner()).await??;
    Ok(())
}
async fn keys_inner() -> Result {
    let began = Instant::now();
    let offset = 13_200_000i64;
    let h = offset + 1000;
    let t = offset + 2000;
    let p = t + 2160;
    let mut f = fixture(401);
    f.translate_heights(offset as u32);
    f.bounds = Bounds {
        horizon: h as u32,
        target: t as u32,
        tip: p as u32,
    };
    let profile_json = serde_json::to_vec(&f.profile)?;
    std::fs::write(out().join("keys-profile.json"), &profile_json)?;
    let rows = f.rows();
    let db = Database::new().await?;
    save("databases.json", &json!({"destination":db.name}))?;
    let c = connect(&db.config).await?;
    c.batch_execute("SET application_name='dsa-small-whole-keys';SET statement_timeout='60s';SET idle_in_transaction_session_timeout='900s'").await?;
    c.batch_execute(include_str!("../../dsa-store/src/layout.sql"))
        .await?;
    c.batch_execute("BEGIN;CREATE TABLE IF NOT EXISTS dsa.consumed_before_horizon(tx_out_id bigint PRIMARY KEY)").await?;
    for table in TABLES {
        c.batch_execute(&format!(
            "CREATE TEMP TABLE {table}(LIKE serve.{table} INCLUDING ALL) ON COMMIT DROP"
        ))
        .await?;
        c.execute(&format!("INSERT INTO pg_temp.{table} SELECT * FROM jsonb_populate_recordset(NULL::serve.{table},$1)"),&[&json!(rows[table])]).await?;
    }
    let n = 25000 - rows["tx_out"].len() as i64;
    let old = n / 2;
    let window_spends = 15000 - rows["tx_in"].len() as i64 - 10000;
    assert!((0..=n - old).contains(&window_spends));
    let cn = f.asset_id("cnight");
    let pc = f.asset_id("permissioned_candidates");
    let max_unit: i64 = c
        .query_one("SELECT max(id)FROM pg_temp.multi_asset", &[])
        .await?
        .get(0);
    let foreign = max_unit + 100;
    let wrong = foreign + 1;
    let datum_base: i64 = c
        .query_one("SELECT max(id)FROM pg_temp.datum", &[])
        .await?
        .get::<_, i64>(0)
        + 10000;
    let base_null: i64 = c
        .query_one(
            "SELECT count(*)FROM pg_temp.tx_out WHERE data_hash IS NULL",
            &[],
        )
        .await?
        .get(0);
    let null_rest = 18750 - base_null - 2;
    assert!((0..=n - 5).contains(&null_rest));
    let mapping = &f.profile.addresses["mapping"];
    let ics = &f.profile.addresses["ics"];
    c.batch_execute(&format!("INSERT INTO pg_temp.multi_asset VALUES({foreign},decode(repeat('fe',28),'hex'),decode('01','hex'));INSERT INTO pg_temp.multi_asset SELECT {wrong},policy,name||decode('ff','hex')FROM pg_temp.multi_asset WHERE id={pc};INSERT INTO pg_temp.datum SELECT {datum_base}+i,decode(md5('small-datum/'||i::text)||md5('small-datum2/'||i::text),'hex'),jsonb_build_object('int',i)FROM generate_series(0,299)i;
     UPDATE pg_temp.block SET epoch_no=600+(id-{offset})/500,slot_no=id*20,time=timestamp'2026-01-01'+(id-{offset})*interval'20seconds';
     INSERT INTO pg_temp.block SELECT b,decode(md5('small-block/'||b::text)||md5('small-block2/'||b::text),'hex'),600+(b-{offset})/500,b*20,b,timestamp'2026-01-01'+(b-{offset})*interval'20seconds',100 FROM generate_series({offset}::bigint,{p}::bigint)b ON CONFLICT DO NOTHING;
     CREATE TEMP TABLE small_coordinates AS SELECT n,CASE WHEN n<{old} THEN {offset}+100+n%600 ELSE {h}+1+(n-{old})%995 END bh,CASE WHEN n<{old} THEN 100+n/600 ELSE 100+(n-{old})/995 END ti FROM generate_series(0::bigint,{n}-1)n;
     ALTER TABLE small_coordinates ADD COLUMN txid bigint;UPDATE small_coordinates SET txid=bh*4096+ti;
     INSERT INTO pg_temp.tx SELECT txid,decode(md5('small-create/'||txid::text)||md5('small-create2/'||txid::text),'hex'),bh,ti::integer,true FROM small_coordinates;
     INSERT INTO pg_temp.tx_out SELECT txid*4096,txid,0,CASE WHEN n={old}+5 THEN '{ics}' WHEN n>=5 AND mod((n-5)*7919,{n}-5)<250 THEN '{mapping}' ELSE 'holder-'||(n%1000)::text END,1,CASE WHEN n IN(1,4)OR(n>=5 AND (n-4)*{null_rest}/({n}-5)>(n-5)*{null_rest}/({n}-5))THEN NULL WHEN n=2 THEN decode(md5('missing-preimage')||md5('missing-preimage2'),'hex') ELSE decode(md5('small-datum/'||(CASE WHEN n=3 THEN 299 ELSE n%299 END)::text)||md5('small-datum2/'||(CASE WHEN n=3 THEN 299 ELSE n%299 END)::text),'hex')END,NULL FROM small_coordinates;
     INSERT INTO pg_temp.ma_tx_out SELECT o.id*4096+row_number()OVER(PARTITION BY o.id ORDER BY a.policy,a.name)-1,1,o.id,a.id FROM small_coordinates s JOIN pg_temp.tx_out o ON o.tx_id=s.txid JOIN pg_temp.multi_asset a ON a.id={cn} OR a.id=CASE WHEN s.n<3 THEN {pc} WHEN s.n<5 THEN {wrong} ELSE {foreign} END;
     CREATE TEMP TABLE small_spends AS SELECT *,CASE WHEN n<{old} THEN bh+200 ELSE bh+1 END sh,CASE WHEN n<{old} THEN 500+n/600 ELSE 500+(n-{old})/995 END si FROM small_coordinates WHERE n BETWEEN 5 AND 10004 OR(n>={old} AND n<{old}+{window_spends});
     INSERT INTO pg_temp.tx SELECT sh*4096+si,decode(md5('small-spend/'||(sh*4096+si)::text)||md5('small-spend2/'||(sh*4096+si)::text),'hex'),sh,si::integer,true FROM small_spends;
     INSERT INTO pg_temp.tx_in SELECT (sh*4096+si)*4096,sh*4096+si,txid,0 FROM small_spends;
     INSERT INTO pg_temp.tx_metadata(id,key,json,tx_id)SELECT txid,6500973,'{{"bytes":"00"}}'::jsonb,txid FROM small_coordinates WHERE n={old}+5;
     DELETE FROM pg_temp.epoch_stake;DELETE FROM pg_temp.pool_hash;DELETE FROM pg_temp.epoch_param;
     INSERT INTO pg_temp.epoch_param SELECT 600+i,decode(md5('small-epoch/'||i::text)||md5('small-epoch2/'||i::text),'hex'),16384 FROM generate_series(0,8)i;
     INSERT INTO pg_temp.pool_hash SELECT 1000+i,substring(decode(md5('small-pool/'||i::text)||md5('small-pool2/'||i::text),'hex')FROM 1 FOR 28)FROM generate_series(0,5)i;
     INSERT INTO pg_temp.epoch_stake SELECT 10000+i,1000+i%6,100000+i,600+i/6 FROM generate_series(0,53)i;
     CREATE TEMP TABLE saved_keys(trial integer,family text,id bigint)ON COMMIT DROP;CREATE INDEX saved_trial ON saved_keys(trial,family,id)")).await?;
    let mut counts = BTreeMap::new();
    for table in TABLES {
        let count: i64 = c
            .query_one(&format!("SELECT count(*)FROM pg_temp.{table}"), &[])
            .await?
            .get(0);
        counts.insert(table, count);
    }
    assert_eq!(counts["tx_out"], 25000);
    assert!(counts["ma_tx_out"] <= 50000);
    assert_eq!(counts["tx_in"], 15000);
    let shape:Value=c.query_one(&format!("SELECT jsonb_build_object('null_hashes',(SELECT count(*)FROM pg_temp.tx_out WHERE data_hash IS NULL),'nonnull_hashes',(SELECT count(*)FROM pg_temp.tx_out WHERE data_hash IS NOT NULL),'exact_PC_outputs',(SELECT count(*)FROM pg_temp.ma_tx_out WHERE ident={pc}),'wrong_name_PC_outputs',(SELECT count(*)FROM pg_temp.ma_tx_out WHERE ident={wrong}),'mapping_outputs',(SELECT count(*)FROM pg_temp.tx_out WHERE address='{mapping}'),'eligible_ICS_outputs',(SELECT count(*)FROM pg_temp.tx_out WHERE address='{ics}'),'outputs_missing_preimage',(SELECT count(*)FROM pg_temp.tx_out o WHERE o.data_hash IS NOT NULL AND NOT EXISTS(SELECT 1 FROM pg_temp.datum d WHERE d.hash=o.data_hash)),'shared_hash_groups',(SELECT count(*)FROM(SELECT data_hash FROM pg_temp.tx_out WHERE data_hash IS NOT NULL GROUP BY data_hash HAVING count(*)>1)s),'data_hash_ndv',(SELECT count(DISTINCT data_hash)FROM pg_temp.tx_out),'address_ndv',(SELECT count(DISTINCT address)FROM pg_temp.tx_out))"),&[]).await?.get(0);
    assert_eq!(shape["null_hashes"], 18750);
    save(
        "keys-fixture.json",
        &json!({"kind":"small_generated_private_TEMP_no_source_or_conversion","counts":counts,"shape":shape,"H":h,"T":t,"P":p,"seed":401,"profile_json_sha256":hex::encode(Sha256::digest(&profile_json)),"max_generated_DATUMs":300,"setup_seconds":began.elapsed().as_secs_f64(),"equality_scope":"predecessor/product comparison; independent46case pure-model proof is separate normal CI"}),
    )?;
    save("fixture-ready.json", &json!({"ready":true}))?;
    let wait = Instant::now();
    while !out().join("io-ready.json").exists() {
        assert!(wait.elapsed() < Duration::from_secs(120));
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let mut ddls = Vec::new();
    for line in include_str!("../../dsa-store/src/indexes.sql")
        .lines()
        .filter(|s| s.starts_with("CREATE"))
    {
        let sql = line
            .replace("CREATE UNIQUE INDEX ", "CREATE UNIQUE INDEX conversion_")
            .replace("CREATE INDEX ", "CREATE INDEX conversion_")
            .replace("ON serve.", "ON pg_temp.");
        c.batch_execute(&sql).await?;
        ddls.push(sql);
    }
    assert_eq!(ddls.len(), 20);
    for table in TABLES {
        c.batch_execute(&format!("ANALYZE pg_temp.{table}")).await?;
    }
    save("keys-indexes.json", &json!(ddls))?;
    let settings:Value=c.query_one("SELECT jsonb_build_object('statement_timeout',current_setting('statement_timeout'),'idle_in_transaction_session_timeout',current_setting('idle_in_transaction_session_timeout'),'work_mem',current_setting('work_mem'),'maintenance_work_mem',current_setting('maintenance_work_mem'),'jit',current_setting('jit'),'temp_buffers',current_setting('temp_buffers'),'shared_buffers',current_setting('shared_buffers'))",&[]).await?.get(0);
    assert_eq!(settings["statement_timeout"], "1min");
    save("keys-settings.json", &settings)?;
    let input_stats:Value=c.query_one("SELECT jsonb_agg(to_jsonb(s))FROM pg_stats s WHERE s.schemaname LIKE'pg_temp_%'AND(tablename='tx_out'AND attname IN('data_hash','address')OR tablename='ma_tx_out'AND attname='ident')",&[]).await?.get(0);
    save("keys-input-stats.json", &input_stats)?;
    let new = canonical::staging_keys_sql();
    let needle = "o.data_hash IS NOT NULL AND ";
    assert_eq!(new.matches(needle).count(), 1);
    let old_sql = new.replacen(needle, "", 1);
    assert_eq!(old_sql.len() + needle.len(), new.len());
    std::fs::write(out().join("keys-product.sql"), &new)?;
    std::fs::write(out().join("keys-predecessor-form.sql"), &old_sql)?;
    save(
        "keys-sql-provenance.json",
        &json!({"product_sha256":hex::encode(Sha256::digest(new.as_bytes())),"predecessor_form_sha256":hex::encode(Sha256::digest(old_sql.as_bytes())),"removed_only":needle,"removal_count":1,"input_schema":"pg_temp","no_cold_cache_claim":true}),
    )?;
    let selection = keep::Selection::new(&f.profile, f.bounds)?;
    let mut reports = Vec::new();
    for (trial, kind) in ["old", "new", "new", "old"].iter().enumerate() {
        let sql = if *kind == "old" { &old_sql } else { &new };
        let started = Instant::now();
        save(
            "active-command.json",
            &json!({"name":format!("keys-{trial}-{kind}"),"started_unix":unix()}),
        )?;
        let plan: Value = c
            .query_one(
                &format!("EXPLAIN(ANALYZE,BUFFERS,WAL,SETTINGS,FORMAT JSON) {sql}"),
                &selection.parameters(),
            )
            .await?
            .get(0);
        let elapsed = started.elapsed().as_secs_f64();
        save(&format!("keys-{trial}-{kind}-plan.json"), &plan)?;
        c.batch_execute(&format!(
            "INSERT INTO saved_keys SELECT {trial},family,id FROM conversion_keep_keys"
        ))
        .await?;
        let differences:i64=c.query_one(&format!("SELECT count(*)FROM((SELECT family,id FROM saved_keys WHERE trial=0 EXCEPT ALL SELECT family,id FROM conversion_keep_keys)UNION ALL(SELECT family,id FROM conversion_keep_keys EXCEPT ALL SELECT family,id FROM saved_keys WHERE trial=0))d"),&[]).await?.get(0);
        assert_eq!(differences, 0);
        let mut digesters: BTreeMap<String, FamilyHasher> = TABLES
            .iter()
            .map(|t| (t.to_string(), FamilyHasher::new(t)))
            .collect();
        for row in c
            .query(
                "SELECT family,id FROM conversion_keep_keys ORDER BY family,id",
                &[],
            )
            .await?
        {
            let family: String = row.get(0);
            let id: i64 = row.get(1);
            digesters.get_mut(&family).unwrap().row(&id.to_be_bytes());
        }
        let digests: BTreeMap<_, _> = digesters
            .into_iter()
            .map(|(k, v)| (k, v.finish()))
            .collect();
        let pc_datum:bool=c.query_one(&format!("SELECT EXISTS(SELECT 1 FROM conversion_keep_keys WHERE family='datum'AND id={datum_base})"),&[]).await?.get(0);
        let wrong_datum:bool=c.query_one(&format!("SELECT EXISTS(SELECT 1 FROM conversion_keep_keys WHERE family='datum'AND id={datum_base}+299)"),&[]).await?.get(0);
        assert!(pc_datum);
        assert!(!wrong_datum);
        reports.push(json!({"trial":trial,"variant":kind,"whole_CTAS_explain_wall_seconds":elapsed,"both_ways_except_all_difference_rows":differences,"family_key_digests":digests,"PC_available_datum_kept":pc_datum,"exclusive_wrong_name_datum_kept":wrong_datum,"cache_scope":"natural warm/order-balanced; fresh output relation, unchanged input stats; no cold claim"}));
        save("keys-trials.json", &json!(reports))?;
        c.batch_execute("DROP TABLE conversion_keep_keys").await?;
    }
    save(
        "keys-result.json",
        &json!({"status":"PASS_PREDECESSOR_PRODUCT_KEYS_ONLY","trials":reports,"order":["old","new","new","old"],"settings":settings,"source_connection":false,"conversion":false,"archive_timing_claim":false,"independent_model_proof":"separate89793eb normal CI46cases/506familycomparisons"}),
    )?;
    c.batch_execute("ROLLBACK").await?;
    drop(c);
    db.cleanup().await?;
    Ok(())
}
