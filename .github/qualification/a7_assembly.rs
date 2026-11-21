//! Generated destination-only stage qualification. No source/converter/publication path.
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
use sha2::{Digest, Sha256};
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
    Ok(c.query_one("SELECT jsonb_build_object('statement_timeout',current_setting('statement_timeout'),'idle_in_transaction_session_timeout',current_setting('idle_in_transaction_session_timeout'),'work_mem',current_setting('work_mem'),'maintenance_work_mem',current_setting('maintenance_work_mem'),'jit',current_setting('jit'),'temp_buffers',current_setting('temp_buffers'),'shared_buffers',current_setting('shared_buffers'),'track_io_timing',current_setting('track_io_timing'))",&[]).await?.get(0))
}
#[test]
fn assembly_dispatch_smoke() {
    assert_eq!(env!("CARGO_PKG_NAME"), "db-sync-alt");
    let plan = dsa_store::keep::Selection::staging_plan().unwrap();
    assert_eq!(plan.len(), 20);
    assert_eq!(plan.last().unwrap().table, "conversion_keep_keys");
    println!("assembly_dispatch_smoke_entered_no_database");
}
#[tokio::test]
#[ignore = "hosted local selection diagnostic"]
async fn assembly_focus() -> Result {
    tokio::time::timeout(Duration::from_secs(7200), assembly_focus_inner()).await??;
    Ok(())
}
async fn assembly_focus_inner() -> Result {
    let began = Instant::now();
    let divisor: i64 = std::env::var("DSA_WHOLE_DIVISOR")?.parse()?;
    assert!(divisor == 1 || divisor == 100);
    let headers = 1_202_855 / divisor;
    let wanted_outputs = 2_630_986 / divisor;
    let wanted_assets = 2_630_984 / divisor;
    let wanted_spends = 2_386_344 / divisor;
    let wanted_datums = 1_036_071 / divisor;
    let offset = 13_200_000i64;
    let tip = offset + headers - 1;
    let target = tip - 2160;
    let horizon = target - (30_009 / divisor).max(300);
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
    c.batch_execute("SET application_name='dsa-assembly-focus';SET statement_timeout='600s';SET idle_in_transaction_session_timeout='7200s'").await?;
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
    let n_outputs = wanted_outputs - rows["tx_out"].len() as i64 - 8;
    let n_assets = wanted_assets - rows["ma_tx_out"].len() as i64 - 8;
    let n_spends = wanted_spends - rows["tx_in"].len() as i64;
    let n_datums = wanted_datums - rows["datum"].len() as i64 - 8;
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
        c.batch_execute(&format!("INSERT INTO pg_temp.datum SELECT {datum_id_base}+n,decode(md5('whole-datum/'||n::text)||md5('whole-datum2/'||n::text),'hex'),CASE WHEN n%1000=0 THEN NULL WHEN n%1000=10 THEN 'null'::jsonb ELSE jsonb_build_object('constructor',0,'fields',jsonb_build_array(jsonb_build_object('int',n),jsonb_build_object('bytes',(SELECT string_agg(md5(n::text||'/'||j::text),'') FROM generate_series(0,CASE WHEN n%1000=2 THEN 1023 WHEN n%100=3 THEN 255 WHEN n%25=4 THEN 63 ELSE 39 END)j)))) END FROM generate_series({low}::bigint,{high}::bigint)n")).await?;
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
 INSERT INTO pg_temp.tx_out SELECT {h}*16777216+100*4096+n%4,{h}*4096+100,(n%4)::smallint,CASE WHEN n%100=0 OR n%1000=10 OR n>={n_assets} THEN '{mapping}' ELSE rpad('holder-'||(n%10007)::text,100,'x') END,1,CASE WHEN n%1000<427 OR n%100=0 OR n%1000=10 OR n>={n_assets} THEN decode(md5('whole-datum/'||(n%{n_datums})::text)||md5('whole-datum2/'||(n%{n_datums})::text),'hex') ELSE NULL END,NULL FROM generate_series({low}::bigint,{high}::bigint)n;
 INSERT INTO pg_temp.ma_tx_out SELECT ({h}*16777216+100*4096+n%4)*4096,1,{h}*16777216+100*4096+n%4,{unit} FROM generate_series({low}::bigint,least({high}::bigint,{n_assets}::bigint-1))n;
 WITH coordinates AS(SELECT DISTINCT {h}+1 h FROM generate_series({low}::bigint,{high}::bigint)n WHERE {spent}) INSERT INTO pg_temp.tx SELECT h*4096+101,decode(lpad(to_hex(h),16,'0')||repeat('cd',24),'hex'),h,101,true FROM coordinates ON CONFLICT DO NOTHING;
 INSERT INTO pg_temp.tx_in SELECT ({h}+1)*16777216+101*4096+n%4,({h}+1)*4096+101,{h}*4096+100,(n%4)::smallint FROM generate_series({low}::bigint,{high}::bigint)n WHERE {spent}")).await?;
        if low % 100000 == 0 {
            event(
                json!({"event":"output_population","rows":high+1,"seconds":began.elapsed().as_secs_f64()}),
            );
        }
    }
    let pc = f.asset_id("permissioned_candidates");
    let control_tx = (horizon + 10) * 4096 + 200;
    let control_datum_base = datum_id_base + n_datums;
    let wrong: i64 = c
        .query_one("SELECT max(id)::bigint+100 FROM pg_temp.multi_asset", &[])
        .await?
        .get(0);
    c.batch_execute(&format!("INSERT INTO pg_temp.multi_asset SELECT {wrong},policy,name||decode('ff','hex') FROM pg_temp.multi_asset WHERE id={pc};
     INSERT INTO pg_temp.tx VALUES({control_tx},decode(md5('stage-control')||md5('stage-control2'),'hex'),{horizon}+10,200,true);
     INSERT INTO pg_temp.datum SELECT {control_datum_base}+n,decode(md5('stage-control-datum/'||n::text)||md5('stage-control-datum2/'||n::text),'hex'),jsonb_build_object('int',n) FROM generate_series(0,7)n;
     INSERT INTO pg_temp.tx_out SELECT {control_tx}*4096+n,{control_tx},n::smallint,rpad('stage-control-holder',100,'x'),1,decode(md5('stage-control-datum/'||n::text)||md5('stage-control-datum2/'||n::text),'hex'),NULL FROM generate_series(0,7)n;
     INSERT INTO pg_temp.ma_tx_out SELECT ({control_tx}*4096+n)*4096,1,{control_tx}*4096+n,CASE WHEN n<6 THEN {pc} ELSE {wrong} END FROM generate_series(0,7)n")).await?;
    let pools = (2802 / divisor).max(6);
    let stakes = (132350 / divisor).min(pools * ((tip - offset) / 25000 + 1));
    let owner = &c;
    c.batch_execute(&format!("DELETE FROM pg_temp.epoch_stake;DELETE FROM pg_temp.pool_hash;DELETE FROM pg_temp.epoch_param;
 INSERT INTO pg_temp.epoch_param SELECT 600+e,decode(md5('whole-epoch/'||e::text)||md5('whole-epoch2/'||e::text),'hex'),16384 FROM generate_series(0,((({tip}-{offset})/25000))::integer)e;
 INSERT INTO pg_temp.pool_hash SELECT 1000+n,substring(decode(md5('whole-pool/'||n::text)||md5('whole-pool2/'||n::text),'hex') FROM 1 FOR 28) FROM generate_series(0,{pools}-1)n;
 INSERT INTO pg_temp.epoch_stake SELECT 10000+n,1000+(n%{pools}),100000+n,(600+(n/{pools})%(({tip}-{offset})/25000+1))::integer FROM generate_series(0,{stakes}-1)n")).await?;
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
        "stage-fixture.json",
        &json!({"kind":"generated_destination_TEMP_structural_stages","counts":counts,"raw_sizes":sizes,"H":horizon,"T":target,"P":tip,"setup_seconds":began.elapsed().as_secs_f64(),"settings":settings(&c).await?,"recipe":"seed401/all11 direct TEMP recipe; synthetic ordinary holder bodies padded100bytes, profile addresses unchanged; bulk56.8percentNULLhash; common datum bytes1280/rare2048-8192-32768; eight reserved window outputs/six exactPC/two wrongname; original datum payload bands; no binaryCOPY/source/capture", "divisor":divisor}),
    )?;
    save(
        "stage-pre-index-layout.json",
        &relation_sizes(&c, false).await?,
    )?;
    save("fixture-ready.json", &json!({"ready":true}))?;
    let wait = Instant::now();
    while !out().join("io-ready.json").exists() {
        assert!(wait.elapsed() < Duration::from_secs(1200));
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let ddl = include_str!("../../dsa-store/src/indexes.sql");
    let policy = "original20";
    let mut index_costs = Vec::new();
    for line in ddl.lines().filter(|line| line.starts_with("CREATE")) {
        let sql = line
            .replace("CREATE UNIQUE INDEX ", "CREATE UNIQUE INDEX conversion_")
            .replace("CREATE INDEX ", "CREATE INDEX conversion_")
            .replace("ON serve.", "ON pg_temp.");
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
        save("stage-raw-index-costs.json", &json!(index_costs))?;
    }
    let analyze = Instant::now();
    let mut analyze_costs = Vec::new();
    for table in TABLES {
        let start = Instant::now();
        c.batch_execute(&format!("ANALYZE pg_temp.{table}")).await?;
        analyze_costs.push(json!({"table":table,"seconds":start.elapsed().as_secs_f64()}));
        save("stage-raw-analyze-details.json", &json!(analyze_costs))?;
    }
    save(
        "stage-raw-analyze.json",
        &json!({"seconds":analyze.elapsed().as_secs_f64(),"settings":settings(&c).await?}),
    )?;
    let profile_json = serde_json::to_vec_pretty(&f.profile)?;
    std::fs::write(out().join("stage-profile.json"), &profile_json)?;
    save("stage-input-distribution.json", &c.query_one("SELECT jsonb_build_object('outputs',count(*),'null_hashes',count(*)FILTER(WHERE data_hash IS NULL),'nonnull_hashes',count(*)FILTER(WHERE data_hash IS NOT NULL),'address_ndv',count(DISTINCT address),'hash_ndv',count(DISTINCT data_hash),'address_min_bytes',min(octet_length(address)),'address_max_bytes',max(octet_length(address)),'address_average_bytes',avg(octet_length(address)),'synthetic_holder_min_bytes',min(octet_length(address))FILTER(WHERE address LIKE 'holder-%'),'synthetic_holder_max_bytes',max(octet_length(address))FILTER(WHERE address LIKE 'holder-%'),'six_added_exact_pc_outputs',6,'two_added_wrong_name_outputs',2,'control_transaction',$1::bigint,'actual_exact_pc_asset_rows',(SELECT count(*)FROM pg_temp.ma_tx_out WHERE ident=$2::bigint),'actual_exact_pc_outputs',(SELECT count(DISTINCT tx_out_id)FROM pg_temp.ma_tx_out WHERE ident=$2::bigint),'actual_wrong_name_asset_rows',(SELECT count(*)FROM pg_temp.ma_tx_out WHERE ident=$3::bigint))FROM pg_temp.tx_out",&[&control_tx,&pc,&wrong]).await?.get(0))?;
    save("stage-settings.json", &settings(&c).await?)?;
    save(
        "stage-post-index-layout.json",
        &relation_sizes(&c, false).await?,
    )?;
    let selection = dsa_store::keep::Selection::new(&f.profile, f.bounds)?;
    let stages = dsa_store::keep::Selection::staging_plan()?;
    assert_eq!(stages.len(), 20);
    let historical: Value = serde_json::from_str(include_str!("a7_d038_stage_reference.json"))?;
    assert_eq!(
        historical["product_pin"],
        "d038f5e8bf52d90c66e601e2496d4945b9528215"
    );
    let old = historical["stages"].as_array().unwrap();
    assert_eq!(old.len(), 6);
    let mut first_five = Vec::new();
    for (stage, reference) in stages.iter().take(5).zip(old) {
        let same = stage.table == reference["table"].as_str().unwrap()
            && stage.create_sql == reference["create_sql"].as_str().unwrap()
            && stage.prepare_sql == reference["prepare_sql"].as_str().unwrap();
        first_five.push(json!({"table":stage.table,"byte_identical":same}));
        assert!(same, "first five must match compiled d038 API");
    }
    save("assembly-first-five-parity.json", &json!(first_five))?;
    save("stage-actual-product-plan.json",&json!(stages.iter().map(|s|json!({"table":s.table,"create_sql":s.create_sql,"prepare_sql":s.prepare_sql,"create_sha256":sha(&s.create_sql),"prepare_sha256":sha(&s.prepare_sql)})).collect::<Vec<_>>()))?;
    let mut costs = Vec::new();
    for (i, stage) in stages.iter().take(5).enumerate() {
        run_stage(&c, &selection, i, stage, &mut costs).await?;
    }
    let reference_limit: u32 = std::env::var("DSA_REFERENCE_STATEMENT_SECONDS")
        .unwrap_or_else(|_| "600".into())
        .parse()?;
    assert!(matches!(reference_limit, 600 | 900));
    let reference_sql = old[5]["create_sql"].as_str().unwrap().replacen(
        "CREATE TEMP TABLE conversion_keep_keys ",
        "CREATE TEMP TABLE comparison_d038_keys ",
        1,
    );
    save(
        "assembly-d038-reference-sql.json",
        &json!({"historical_product":historical["product_pin"],"original_sql":old[5]["create_sql"],"executed_sql":reference_sql,"executed_sha256":sha(&reference_sql),"only_change":"destination reference TEMP table name","statement_limit_seconds":reference_limit,"candidate_statement_limit_seconds":600,"source_connection_exists":false}),
    )?;
    c.batch_execute("SAVEPOINT d038_reference").await?;
    c.batch_execute(&format!("SET LOCAL statement_timeout='{reference_limit}s'"))
        .await?;
    let start = Instant::now();
    let reference_started = unix();
    save(
        "active-command.json",
        &json!({"name":"d038_final_reference","started_unix":reference_started,"reference_only_statement_seconds":reference_limit}),
    )?;
    let result = c
        .query_one(
            &format!("EXPLAIN(ANALYZE,BUFFERS,WAL,SETTINGS,FORMAT JSON) {reference_sql}"),
            &selection.parameters(),
        )
        .await;
    let reference_ok = match result {
        Ok(row) => {
            let plan: Value = row.get(0);
            save("assembly-d038-reference-plan.json", &plan)?;
            save(
                "assembly-d038-reference-result.json",
                &json!({"status":"COMPLETED","seconds":start.elapsed().as_secs_f64(),"started_unix":reference_started,"finished_unix":unix(),"statement_limit_seconds":reference_limit}),
            )?;
            c.batch_execute("RELEASE SAVEPOINT d038_reference;SET LOCAL statement_timeout='600s';ALTER TABLE pg_temp.comparison_d038_keys ADD PRIMARY KEY(family,id);ANALYZE pg_temp.comparison_d038_keys").await?;
            save(
                "assembly-d038-reference-families.json",
                &key_hashes(&c, "comparison_d038_keys").await?,
            )?;
            true
        }
        Err(error) => {
            let state = error.code().map(|c| c.code().to_owned());
            save(
                "assembly-d038-reference-result.json",
                &json!({"status":"FAILED","sqlstate":state,"seconds":start.elapsed().as_secs_f64(),"started_unix":reference_started,"finished_unix":unix(),"statement_limit_seconds":reference_limit,"parity_unavailable":true}),
            )?;
            c.batch_execute("ROLLBACK TO SAVEPOINT d038_reference;RELEASE SAVEPOINT d038_reference;SET LOCAL statement_timeout='600s'").await?;
            if state.as_deref() != Some("57014") {
                return Err(error.into());
            }
            false
        }
    };
    save(
        "assembly-candidate-settings-after-reference.json",
        &settings(&c).await?,
    )?;
    for (i, stage) in stages.iter().enumerate().skip(5) {
        run_stage(&c, &selection, i, stage, &mut costs).await?;
    }
    let hashes = key_hashes(&c, "conversion_keep_keys").await?;
    save("stage-final-families.json", &hashes)?;
    let duplicate:bool=c.query_one("SELECT EXISTS(SELECT 1 FROM pg_temp.conversion_keep_keys GROUP BY family,id HAVING count(*)<>1)",&[]).await?.get(0);
    assert!(!duplicate);
    let parity = if reference_ok {
        let difference:i64=c.query_one("SELECT count(*)FROM((SELECT family,id FROM pg_temp.comparison_d038_keys EXCEPT ALL SELECT family,id FROM pg_temp.conversion_keep_keys)UNION ALL(SELECT family,id FROM pg_temp.conversion_keep_keys EXCEPT ALL SELECT family,id FROM pg_temp.comparison_d038_keys))d",&[]).await?.get(0);
        assert_eq!(difference, 0);
        let old: Value = serde_json::from_slice(&std::fs::read(
            out().join("assembly-d038-reference-families.json"),
        )?)?;
        assert_eq!(old, hashes);
        json!({"complete11_except_all_difference":0,"all11_family_id_hashes_equal":true})
    } else {
        json!({"complete11_except_all_difference":null,"all11_family_id_hashes_equal":null,"reason":"Finite d038 final reference timed out; no parity claim"})
    };
    save("stage-parity.json", &parity)?;
    let controls:Value=c.query_one("SELECT jsonb_build_object('kept_control_outputs',(SELECT count(*)FROM pg_temp.conversion_keep_keys k JOIN pg_temp.tx_out o ON o.id=k.id AND k.family='tx_out' WHERE o.tx_id=$1),'kept_exact_pc_datums',(SELECT count(*)FROM pg_temp.conversion_keep_keys WHERE family='datum'AND id BETWEEN $2 AND $2+5),'kept_wrong_name_only_datums',(SELECT count(*)FROM pg_temp.conversion_keep_keys WHERE family='datum'AND id BETWEEN $2+6 AND $2+7),'kept_wrong_name_asset_rows',(SELECT count(*)FROM pg_temp.conversion_keep_keys k JOIN pg_temp.ma_tx_out a ON a.id=k.id AND k.family='ma_tx_out' WHERE a.ident=$3))",&[&control_tx,&control_datum_base,&wrong]).await?.get(0);
    assert_eq!(controls["kept_control_outputs"], 8);
    assert_eq!(controls["kept_exact_pc_datums"], 6);
    assert_eq!(controls["kept_wrong_name_only_datums"], 0);
    assert_eq!(controls["kept_wrong_name_asset_rows"], 0);
    save("stage-controls.json", &controls)?;
    save(
        "assembly-result.json",
        &json!({"status":if reference_ok{"PASS_GENERATED_ASSEMBLY_EQUALITY"}else{"ASSEMBLY_COMPLETED_REFERENCE_TIMEOUT_PARITY_UNPROVED"},"actual_product_api":"dsa_store::keep::Selection::staging_plan","stages":stages.len(),"first_five_byte_identical_to_d038":true,"reference_completed":reference_ok,"reference_only_statement_seconds":reference_limit,"candidate_statement_seconds":600,"overall_seconds":7200,"parity":parity,"controls":controls,"final_keys_unique":!duplicate,"divisor":divisor,"elapsed_seconds":began.elapsed().as_secs_f64(),"source_or_capture_tested":false,"full_converter_proven":false,"order":"Commonfirst5, historical d038 final reference, revised actual remaining stages; warm-order, profiling-overhead and layout limits apply"}),
    )?;
    c.batch_execute("ROLLBACK").await?;
    assert!(
        c.query_one(
            "SELECT to_regclass('pg_temp.conversion_keep_keys')IS NULL",
            &[]
        )
        .await?
        .get::<_, bool>(0)
    );
    drop(c);
    db.cleanup().await?;
    Ok(())
}
async fn run_stage(
    c: &tokio_postgres::Client,
    selection: &dsa_store::keep::Selection,
    i: usize,
    stage: &dsa_store::keep::KeyStage,
    costs: &mut Vec<Value>,
) -> Result {
    let start = Instant::now();
    let started = unix();
    save(
        "active-command.json",
        &json!({"name":format!("stage_{i}_{}",stage.table),"started_unix":started,"candidate_statement_seconds":600}),
    )?;
    let plan: Value = c
        .query_one(
            &format!(
                "EXPLAIN(ANALYZE,BUFFERS,WAL,SETTINGS,FORMAT JSON) {}",
                stage.create_sql
            ),
            &selection.parameters(),
        )
        .await?
        .get(0);
    let seconds = start.elapsed().as_secs_f64();
    save(&format!("stage-{i}-{}-plan.json", stage.table), &plan)?;
    let mut prepare = Vec::new();
    for (n, sql) in stage
        .prepare_sql
        .split(';')
        .filter(|s| !s.trim().is_empty())
        .enumerate()
    {
        let started = unix();
        let start = Instant::now();
        save(
            "active-command.json",
            &json!({"name":format!("prepare_{i}_{n}_{}",stage.table),"started_unix":started}),
        )?;
        c.batch_execute(sql).await?;
        prepare.push(json!({"sql":sql,"seconds":start.elapsed().as_secs_f64(),"started_unix":started,"finished_unix":unix()}));
    }
    let rows: i64 = c
        .query_one(&format!("SELECT count(*)FROM pg_temp.{}", stage.table), &[])
        .await?
        .get(0);
    let layout = relation_sizes(c, true).await?;
    costs.push(json!({"table":stage.table,"rows":rows,"create_seconds":seconds,"prepare":prepare,"started_unix":started,"finished_unix":unix(),"footprint":layout}));
    save("stage-costs.json", &json!(costs))?;
    Ok(())
}
fn sha(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}
async fn key_hashes(c: &tokio_postgres::Client, table: &str) -> Result<Value> {
    assert!(matches!(
        table,
        "comparison_d038_keys" | "conversion_keep_keys"
    ));
    let mut hashers: BTreeMap<&str, FamilyHasher> = TABLES
        .into_iter()
        .map(|t| (t, FamilyHasher::new(t)))
        .collect();
    let sql = format!("SELECT family,id::text FROM pg_temp.{table} ORDER BY family,id");
    let stream = c
        .query_raw(
            &sql,
            std::iter::empty::<&(dyn tokio_postgres::types::ToSql + Sync)>(),
        )
        .await?;
    pin_mut!(stream);
    while let Some(row) = stream.next().await {
        let row = row?;
        let family: String = row.get(0);
        let id: String = row.get(1);
        hashers
            .get_mut(family.as_str())
            .expect("known family")
            .row(id.as_bytes());
    }
    Ok(json!(
        hashers
            .into_iter()
            .map(|(t, h)| (t, h.finish()))
            .collect::<BTreeMap<_, _>>()
    ))
}
async fn relation_sizes(c: &tokio_postgres::Client, _reference: bool) -> Result<Value> {
    let values=c.query("SELECT jsonb_build_object('table',relname,'heap_main_bytes',pg_relation_size(oid),'index_bytes',pg_indexes_size(oid),'total_bytes',pg_total_relation_size(oid),'toast_total_bytes',CASE WHEN reltoastrelid=0 THEN 0 ELSE pg_total_relation_size(reltoastrelid)END,'reltuples',reltuples)FROM pg_class WHERE relnamespace=pg_my_temp_schema()AND relkind='r'ORDER BY relname",&[]).await?;
    let mut rows = Vec::new();
    let mut product = 0i64;
    let mut reference = 0i64;
    for row in values {
        let v: Value = row.get(0);
        let bytes = v["total_bytes"].as_i64().unwrap();
        if v["table"] == "comparison_d038_keys" {
            reference += bytes;
        } else {
            product += bytes;
        }
        rows.push(v);
    }
    Ok(
        json!({"relations":rows,"product_temp_total_bytes":product,"additional_reference_temp_bytes":reference,"scope":"Own generated TEMP relations only; historical reference excluded from product total but present in physical PGDATA telemetry; direct typed layout is not binaryCOPY physical identity"}),
    )
}
