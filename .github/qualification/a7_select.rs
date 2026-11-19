//! Hosted local-TEMP selection diagnostic; not a converter/field qualification.
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
use futures_util::{StreamExt, pin_mut};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error as StdError,
    path::PathBuf,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
type Result<T = ()> = std::result::Result<T, Box<dyn StdError + Send + Sync>>;
fn out() -> PathBuf {
    PathBuf::from(std::env::var("DSA_QUALIFICATION_OUT").unwrap())
}
fn save(name: &str, value: &Value) -> Result {
    let p = out().join(name);
    let t = p.with_extension("tmp");
    std::fs::write(&t, serde_json::to_vec_pretty(value)?)?;
    std::fs::rename(t, p)?;
    Ok(())
}
fn event(value: Value) {
    println!("{value}");
}
fn unix() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}
async fn settings(c: &tokio_postgres::Client) -> Result<Value> {
    Ok(c.query_one("SELECT jsonb_build_object('statement_timeout',current_setting('statement_timeout'),'idle_in_transaction_session_timeout',current_setting('idle_in_transaction_session_timeout'),'work_mem',current_setting('work_mem'),'maintenance_work_mem',current_setting('maintenance_work_mem'),'jit',current_setting('jit'),'temp_buffers',current_setting('temp_buffers'),'shared_buffers',current_setting('shared_buffers'))",&[]).await?.get(0))
}
#[tokio::test]
#[ignore = "hosted local selection diagnostic"]
async fn selection_focus() -> Result {
    tokio::time::timeout(Duration::from_secs(7200), selection_focus_inner()).await??;
    Ok(())
}
async fn selection_focus_inner() -> Result {
    let began = Instant::now();
    let divisor = 1i64;
    let headers = 1_202_855;
    let wanted_outputs = 2_630_986;
    let wanted_assets = 2_630_984;
    let wanted_spends = 2_386_344;
    let wanted_datums = 1_036_071;
    let offset = 13_200_000i64;
    let tip = offset + headers - 1;
    let target = tip - 2160;
    let horizon = target - 30_009;
    let span = target - offset - 17;
    let mut f = fixture(401);
    f.translate_heights(offset as u32);
    f.profile.cardano.security_parameter = 2160;
    f.bounds = Bounds {
        horizon: horizon as u32,
        target: target as u32,
        tip: tip as u32,
    };
    let rows = f.rows();
    let db = Database::new().await?;
    save("databases.json", &json!({"destination":db.name}))?;
    let c = connect(&db.config).await?;
    c.batch_execute("SET application_name='dsa-selection-focus';SET statement_timeout='600s';SET idle_in_transaction_session_timeout='7200s'").await?;
    c.batch_execute(include_str!("../../dsa-store/src/layout.sql"))
        .await?;
    c.batch_execute("BEGIN;CREATE TABLE dsa.consumed_before_horizon(tx_out_id bigint PRIMARY KEY)")
        .await?;
    for table in TABLES {
        c.batch_execute(&format!(
            "CREATE TEMP TABLE {table}(LIKE serve.{table} INCLUDING ALL) ON COMMIT DROP"
        ))
        .await?;
        if table != "tx_metadata" {
            c.execute(&format!("INSERT INTO pg_temp.{table} SELECT * FROM jsonb_populate_recordset(NULL::serve.{table},$1)"),&[&json!(rows[table])]).await?;
        }
    }
    let n_outputs = wanted_outputs - rows["tx_out"].len() as i64;
    let n_assets = wanted_assets - rows["ma_tx_out"].len() as i64;
    let n_spends = wanted_spends - rows["tx_in"].len() as i64;
    let n_datums = wanted_datums - rows["datum"].len() as i64;
    let datum_id_base = f.chain.datums.keys().max().copied().unwrap_or(0) + 10000;
    let unit = f.asset_id("cnight");
    let mapping = &f.profile.addresses["mapping"];
    let mut expected_supply: BTreeMap<String, i128> = dsa_chain::assets::fixed_units(&f.profile)
        .into_iter()
        .map(|u| (u, 0))
        .collect();
    let descriptors: BTreeMap<i64, String> = rows["multi_asset"]
        .iter()
        .map(|r| {
            (
                r["id"].as_i64().unwrap(),
                format!(
                    "{}{}",
                    r["policy"].as_str().unwrap().trim_start_matches("\\x"),
                    r["name"].as_str().unwrap().trim_start_matches("\\x")
                ),
            )
        })
        .collect();
    let spent: BTreeSet<i64> = rows["tx_in"]
        .iter()
        .map(|r| r["tx_out_id"].as_i64().unwrap() * 4096 + r["tx_out_index"].as_i64().unwrap())
        .collect();
    for asset in &rows["ma_tx_out"] {
        if !spent.contains(&asset["tx_out_id"].as_i64().unwrap()) {
            if let Some(sum) =
                expected_supply.get_mut(&descriptors[&asset["ident"].as_i64().unwrap()])
            {
                *sum += asset["quantity"].to_string().parse::<i128>()?;
            }
        }
    }
    *expected_supply
        .get_mut(&f.profile.assets["cnight"].unit())
        .unwrap() += i128::from(n_assets - n_assets * n_spends / n_outputs);

    c.batch_execute(&format!("UPDATE pg_temp.block SET epoch_no=600+(id-{offset})/25000,slot_no=id*20,time=timestamp '2026-01-01'+(id-{offset})*interval '20seconds'")).await?;
    for low in (offset..=tip).step_by(25_000) {
        let high = (low + 24999).min(tip);
        c.batch_execute(&format!("INSERT INTO pg_temp.block SELECT n,decode(md5('whole-block/'||n::text)||md5('whole-block2/'||n::text),'hex'),600+(n-{offset})/25000,n*20,n,timestamp '2026-01-01'+(n-{offset})*interval '20seconds',102 FROM generate_series({low}::bigint,{high}::bigint)n ON CONFLICT DO NOTHING")).await?;
    }
    for low in (0..n_datums).step_by(10_000) {
        let high = (low + 9999).min(n_datums - 1);
        c.batch_execute(&format!("INSERT INTO pg_temp.datum SELECT {datum_id_base}+n,decode(md5('whole-datum/'||n::text)||md5('whole-datum2/'||n::text),'hex'),CASE WHEN n%1000=0 THEN NULL WHEN n%1000=10 THEN 'null'::jsonb ELSE jsonb_build_object('constructor',0,'fields',jsonb_build_array(jsonb_build_object('int',n),jsonb_build_object('bytes',(SELECT string_agg(md5(n::text||'/'||j::text),'') FROM generate_series(0,CASE WHEN n%1000=2 THEN 1023 WHEN n%100=3 THEN 255 WHEN n%25=4 THEN 63 ELSE 15 END)j)))) END FROM generate_series({low}::bigint,{high}::bigint)n")).await?;
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
        c.batch_execute(&format!("WITH coordinates AS(SELECT DISTINCT {h} h FROM generate_series({low}::bigint,{high}::bigint)n) INSERT INTO pg_temp.tx SELECT h*4096+100,decode(lpad(to_hex(h),16,'0')||repeat('ab',24),'hex'),h,100,true FROM coordinates ON CONFLICT DO NOTHING;
 INSERT INTO pg_temp.tx_out SELECT {h}*16777216+100*4096+n%4,{h}*4096+100,(n%4)::smallint,CASE WHEN n%100=0 OR n%1000=10 OR n>={n_assets} THEN '{mapping}' ELSE 'holder-'||(n%10007)::text END,1,decode(md5('whole-datum/'||(n%{n_datums})::text)||md5('whole-datum2/'||(n%{n_datums})::text),'hex'),NULL FROM generate_series({low}::bigint,{high}::bigint)n;
 INSERT INTO pg_temp.ma_tx_out SELECT ({h}*16777216+100*4096+n%4)*4096,1,{h}*16777216+100*4096+n%4,{unit} FROM generate_series({low}::bigint,least({high}::bigint,{n_assets}::bigint-1))n;
 WITH coordinates AS(SELECT DISTINCT {h}+1 h FROM generate_series({low}::bigint,{high}::bigint)n WHERE {spent}) INSERT INTO pg_temp.tx SELECT h*4096+101,decode(lpad(to_hex(h),16,'0')||repeat('cd',24),'hex'),h,101,true FROM coordinates ON CONFLICT DO NOTHING;
 INSERT INTO pg_temp.tx_in SELECT ({h}+1)*16777216+101*4096+n%4,({h}+1)*4096+101,{h}*4096+100,(n%4)::smallint FROM generate_series({low}::bigint,{high}::bigint)n WHERE {spent}")).await?;
        if low % 100000 == 0 {
            event(
                json!({"event":"output_population","rows":high+1,"seconds":began.elapsed().as_secs_f64()}),
            );
        }
    }
    let pools = (2802 / divisor).max(6);
    let stakes = 132350 / divisor;
    let owner = &c;
    c.batch_execute(&format!("DELETE FROM pg_temp.epoch_stake;DELETE FROM pg_temp.pool_hash;DELETE FROM pg_temp.epoch_param;
 INSERT INTO pg_temp.epoch_param SELECT 600+e,decode(md5('whole-epoch/'||e::text)||md5('whole-epoch2/'||e::text),'hex'),16384 FROM generate_series(0,((({tip}-{offset})/25000))::integer)e;
 INSERT INTO pg_temp.pool_hash SELECT 1000+n,substring(decode(md5('whole-pool/'||n::text)||md5('whole-pool2/'||n::text),'hex') FROM 1 FOR 28) FROM generate_series(0,{pools}-1)n;
 INSERT INTO pg_temp.epoch_stake SELECT 10000+n,1000+(n%{pools}),100000+n,(600+(n/{pools})%49)::integer FROM generate_series(0,{stakes}-1)n")).await?;
    // Epoch stake rows must be unique on (epoch,pool); smoke adapts its epoch count.
    let duplicate:bool=owner.query_one("SELECT EXISTS(SELECT 1 FROM pg_temp.epoch_stake GROUP BY epoch_no,pool_id HAVING count(*)>1)",&[]).await?.get(0);
    assert!(!duplicate);

    // Preflight owned synthetic TEMP rows before installing physical caps.
    let mut counts = BTreeMap::new();
    let mut sizes = BTreeMap::new();
    for table in TABLES {
        let n: i64 = c
            .query_one(&format!("SELECT count(*) FROM pg_temp.{table}"), &[])
            .await?
            .get(0);
        counts.insert(table, n);
        let size: i64 = c
            .query_one(
                "SELECT pg_total_relation_size($1::text::regclass)",
                &[&format!("pg_temp.{table}")],
            )
            .await?
            .get(0);
        sizes.insert(table, size);
    }
    assert_eq!(counts["block"], headers);
    assert_eq!(counts["tx_out"], wanted_outputs);
    assert_eq!(counts["ma_tx_out"], wanted_assets);
    assert_eq!(counts["tx_in"], wanted_spends);
    assert_eq!(counts["datum"], wanted_datums);
    assert_eq!(counts["tx_metadata"], 0);
    save(
        "selection-fixture.json",
        &json!({"kind":"generated_local_TEMP_not_source_conversion","counts":counts,"raw_sizes":sizes,"H":horizon,"T":target,"P":tip,"setup_seconds":began.elapsed().as_secs_f64(),"settings":settings(&c).await?,"recipe":"identical seed401/fullrecipe from acd4d10; direct typedTEMP population instead of sourceCOPY; source/capturecost excluded"}),
    )?;
    save("fixture-ready.json", &json!({"ready":true}))?;
    let wait = Instant::now();
    while !out().join("io-ready.json").exists() {
        assert!(wait.elapsed() < Duration::from_secs(1200));
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // Candidate policy must be selected only after phase1 measurements.
    let policy =
        std::env::var("DSA_SELECTION_INDEX_POLICY").unwrap_or_else(|_| "original20".into());
    let ddl = if policy == "candidate7" {
        include_str!("a7_candidate_indexes.sql")
    } else {
        include_str!("../../dsa-store/src/indexes.sql")
    };
    let mut index_costs = Vec::new();
    for line in ddl.lines().filter(|line| line.starts_with("CREATE")) {
        let sql = if policy == "candidate7" {
            line.to_owned()
        } else {
            line.replace("CREATE UNIQUE INDEX ", "CREATE UNIQUE INDEX conversion_")
                .replace("CREATE INDEX ", "CREATE INDEX conversion_")
                .replace("ON serve.", "ON pg_temp.")
        };
        let start = Instant::now();
        save(
            "active-command.json",
            &json!({"name":"private_index","sql":sql,"started_unix":unix()}),
        )?;
        c.batch_execute(&sql).await?;
        let seconds = start.elapsed().as_secs_f64();
        let parts: Vec<_> = sql.split_whitespace().collect();
        let index = parts[if parts[1] == "UNIQUE" { 3 } else { 2 }];
        let bytes: i64 = c
            .query_one(
                "SELECT pg_relation_size($1::text::regclass)",
                &[&format!("pg_temp.{index}")],
            )
            .await?
            .get(0);
        index_costs.push(json!({"sql":sql,"seconds":seconds,"index_bytes":bytes,"completion":"SQL completed; TEMP writes may remain buffered; physicalIO captured separately"}));
        save("selection-index-costs.json", &json!(index_costs))?;
    }
    let analyze = Instant::now();
    for table in TABLES {
        c.batch_execute(&format!("ANALYZE pg_temp.{table}")).await?;
    }
    save(
        "selection-analyze.json",
        &json!({"seconds":analyze.elapsed().as_secs_f64(),"settings":settings(&c).await?}),
    )?;
    let selection = keep::Selection::new(&f.profile, f.bounds)?;
    let keys_sql = canonical::staging_keys_sql();
    save(
        "selection-keys-sql.json",
        &json!({"sql":keys_sql,"settings":settings(&c).await?}),
    )?;
    let plan_sql = keys_sql
        .strip_prefix("CREATE TEMP TABLE conversion_keep_keys ON COMMIT DROP AS ")
        .unwrap();
    let plan: Value = c
        .query_one(
            &format!("EXPLAIN(SETTINGS,FORMAT JSON) {plan_sql}"),
            &selection.parameters(),
        )
        .await?
        .get(0);
    save("selection-keys-estimated-plan.json", &plan)?;
    let start = Instant::now();
    save(
        "active-command.json",
        &json!({"name":"staging_keys","started_unix":unix()}),
    )?;
    let keys = c.execute(&keys_sql, &selection.parameters()).await?;
    let ctasseconds = start.elapsed().as_secs_f64();
    c.batch_execute("ALTER TABLE pg_temp.conversion_keep_keys ADD PRIMARY KEY(family,id);ANALYZE pg_temp.conversion_keep_keys").await?;
    let key_counts:Value=c.query_one("SELECT jsonb_object_agg(family,rows) FROM(SELECT family,count(*) rows FROM pg_temp.conversion_keep_keys GROUP BY family)s",&[]).await?.get(0);
    save(
        "selection-keys-result.json",
        &json!({"command_rows":keys,"counts_by_family":key_counts,"ctas_seconds":ctasseconds,"with_index_analyze_seconds":start.elapsed().as_secs_f64()}),
    )?;
    let mut digests = BTreeMap::new();
    let mut measurements = Vec::new();
    for table in TABLES {
        let sql = canonical::selected_staging(table)?;
        let plan: Value = c
            .query_one(
                &format!("EXPLAIN(SETTINGS,FORMAT JSON) {sql}"),
                &selection.parameters(),
            )
            .await?
            .get(0);
        save(&format!("selection-{table}-estimated-plan.json"), &plan)?;
        let began = Instant::now();
        save(
            "active-command.json",
            &json!({"name":format!("canonical_stream_{table}"),"started_unix":unix()}),
        )?;
        let stream = c.query_raw(&sql, selection.parameters()).await?;
        pin_mut!(stream);
        let mut first = None;
        let mut count = 0u64;
        let mut original_bytes = 0u64;
        let mut digest = FamilyHasher::new(table);
        while let Some(row) = stream.next().await {
            let row = row?;
            if first.is_none() {
                first = Some(began.elapsed().as_secs_f64());
            }
            let original: &str = row.try_get(0)?;
            let canonical: &str = row.try_get(1)?;
            digest.row(canonical.as_bytes());
            count += 1;
            original_bytes += original.len() as u64;
        }
        drop(stream);
        let stream_seconds = began.elapsed().as_secs_f64();
        let family = digest.finish();
        let columns:Vec<String>=c.query("SELECT attname FROM pg_attribute WHERE attrelid=$1::text::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum",&[&format!("pg_temp.{table}")]).await?.iter().map(|r|format!("s.\"{}\"",r.get::<_,String>(0))).collect();
        let key = if table == "epoch_param" {
            "epoch_no"
        } else {
            "id"
        };
        let insert = format!(
            "INSERT INTO serve.{table} SELECT {} FROM pg_temp.{table} s JOIN pg_temp.conversion_keep_keys k ON k.id=s.{key} WHERE k.family='{table}'",
            columns.join(",")
        );
        let started = Instant::now();
        save(
            "active-command.json",
            &json!({"name":format!("typed_insert_{table}"),"started_unix":unix()}),
        )?;
        let inserted = c.execute(&insert, &[]).await?;
        assert_eq!(inserted, count);
        let measurement = json!({"family":table,"rows":count,"original_bytes":original_bytes,"first_row_seconds":first,"stream_seconds":stream_seconds,"insert_seconds":started.elapsed().as_secs_f64(),"canonical_digest":family,"stream_sql":sql,"typed_insert_sql":insert});
        save(&format!("selection-{table}-result.json"), &measurement)?;
        measurements.push(measurement);
        digests.insert(table, family);
        if table == "ma_tx_out" {
            save(
                "active-command.json",
                &json!({"name":"ma_warm_diagnostic_explain_analyze","started_unix":unix()}),
            )?;
            let plan: Value = c
                .query_one(
                    &format!("EXPLAIN(ANALYZE,BUFFERS,SETTINGS,FORMAT JSON) {sql}"),
                    &selection.parameters(),
                )
                .await?
                .get(0);
            save("selection-ma-warm-diagnostic-actual-plan.json", &plan)?;
        }
    }
    c.execute("INSERT INTO dsa.consumed_before_horizon SELECT o.id FROM pg_temp.tx_out o JOIN pg_temp.conversion_keep_keys k ON k.id=o.id AND k.family='tx_out' WHERE o.id<$1::bigint*16777216 AND EXISTS(SELECT 1 FROM pg_temp.tx_in i WHERE i.tx_out_id=o.tx_id AND i.tx_out_index=o.index AND i.id BETWEEN 0 AND $1::bigint*16777216-1)",&[&horizon]).await?;
    let final_index_began = Instant::now();
    for line in include_str!("../../dsa-store/src/indexes.sql")
        .lines()
        .filter(|line| line.starts_with("CREATE"))
    {
        c.batch_execute(line).await?;
    }
    for table in TABLES {
        c.batch_execute(&format!("ANALYZE serve.{table}")).await?;
    }
    save(
        "selection-final-serve-index-analyze.json",
        &json!({"seconds":final_index_began.elapsed().as_secs_f64(),"scope":"normal finalserve lookup shape before independent supply/closure/canonical validation; no gate/publication"}),
    )?;
    let mut supplies = BTreeMap::new();
    for unit in dsa_chain::assets::fixed_units(&f.profile) {
        let policy = hex::decode(&unit[..56])?;
        let name = hex::decode(&unit[56..])?;
        let before = expected_supply[&unit].to_string();
        let after:String=c.query_one("SELECT coalesce(sum(a.quantity),0)::text FROM serve.ma_tx_out a JOIN serve.multi_asset m ON m.id=a.ident JOIN serve.tx_out o ON o.id=a.tx_out_id WHERE m.policy=$1 AND m.name=$2 AND NOT EXISTS(SELECT 1 FROM serve.tx_in i WHERE i.tx_out_id=o.tx_id AND i.tx_out_index=o.index) AND NOT EXISTS(SELECT 1 FROM dsa.consumed_before_horizon c WHERE c.tx_out_id=o.id)",&[&policy,&name]).await?.get(0);
        assert_eq!(before, after, "fixed supply after local projection {unit}");
        supplies.insert(
            unit,
            json!({"independent_generated_formula":before,"selected":after}),
        );
    }
    save("selection-supply.json", &json!(supplies))?;
    let closure_violation: bool = c
        .query_one(include_str!("a7_closure.sql"), &[])
        .await?
        .get(0);
    assert!(!closure_violation);
    save(
        "selection-closure.json",
        &json!({"referential_closure_violation":closure_violation,"sql_origin":"verbatim referential_closure_failed check at productdaf5963 verify.rs"}),
    )?;
    // Independently rehash published typed rows using the pinned canonical catalogue.
    for table in TABLES {
        let sql = canonical::query(table)?;
        let stream = c.query_raw(&sql, [&horizon]).await?;
        pin_mut!(stream);
        let mut h = FamilyHasher::new(table);
        while let Some(row) = stream.next().await {
            let row = row?;
            let value: &str = row.try_get(0)?;
            h.row(value.as_bytes());
        }
        assert_eq!(
            h.finish(),
            digests[table],
            "typed insert canonical equivalence {table}"
        );
    }
    save(
        "selection-result.json",
        &json!({"status":"PASS_LOCAL_SELECTION_DIAGNOSTIC_ONLY","index_policy":policy,"families":measurements,"full_converter_proven":false,"source_or_capture_tested":false}),
    )?;
    c.batch_execute("ROLLBACK").await?;
    drop(c);
    db.cleanup().await?;
    Ok(())
}
