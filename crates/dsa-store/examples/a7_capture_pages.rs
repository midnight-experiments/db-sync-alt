//! Isolated archived/generated-fixture measurement using the unchanged native row path.
//! No network access occurs without an explicit configuration, loopback fixture and reader.
use dsa_store::{
    catalogue::catalogue,
    compare::{Bind, Reader, rows},
};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    error::Error,
    fs::{self, File},
    io::{BufWriter, Write},
    path::PathBuf,
    str::FromStr,
    time::{Duration, Instant},
};
use tokio_postgres::{Config, config::Host};
type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
#[derive(Deserialize)]
struct Job {
    database: String,
    manifest: PathBuf,
    manifest_sha256: String,
    output: PathBuf,
    page_sequence: Vec<i32>,
}
fn check(ok: bool, reason: &str) -> Result<()> {
    if !ok {
        return Err(reason.to_owned().into());
    }
    Ok(())
}
fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn config(text: &str) -> Result<Config> {
    let mut c = Config::from_str(text)?;
    check(
        matches!(c.get_hosts(),[Host::Tcp(h)] if h=="127.0.0.1"),
        "only explicit loopback fixture allowed",
    )?;
    check(
        c.get_ports() == [5432]
            && c.get_user() == Some("db_sync_alt_reader")
            && c.get_dbname() == Some("fixture"),
        "not the isolated fixture reader",
    )?;
    check(
        c.get_password().is_none() && c.get_options().is_none(),
        "no password or arbitrary startup options",
    )?;
    c.connect_timeout(Duration::from_secs(10));
    Ok(c)
}
fn binds(types: &[Value], values: &[Value]) -> Result<Vec<Bind>> {
    check(types.len() == values.len(), "parameter count")?;
    types
        .iter()
        .zip(values)
        .map(|(t, v)| {
            let n = v.as_i64().ok_or("integer parameter required")?;
            match t.as_str() {
                Some("int4") => Ok(Bind::I32(i32::try_from(n)?)),
                Some("int8") => Ok(Bind::I64(n)),
                _ => Err("unclassified parameter type".into()),
            }
        })
        .collect()
}
async fn experiment(reader: &mut Reader, job: &Job, manifest: &Value) -> Result<Value> {
    let began = Instant::now();
    let tx = reader.snapshot().await?;
    tx.batch_execute("SET LOCAL search_path=public; SET LOCAL bytea_output=hex; SET LOCAL DateStyle=ISO; SET LOCAL TimeZone=UTC").await?;
    let settings=tx.query_one("SELECT current_user,current_database(),current_setting('transaction_read_only'),current_setting('transaction_isolation'),current_setting('statement_timeout'),current_setting('idle_in_transaction_session_timeout')",&[]).await?;
    check(
        settings.get::<_, String>(0) == "db_sync_alt_reader"
            && settings.get::<_, String>(1) == "fixture"
            && settings.get::<_, String>(2) == "on"
            && settings.get::<_, String>(3) == "repeatable read"
            && settings.get::<_, String>(4) == "1min"
            && settings.get::<_, String>(5) == "1min",
        "reader session differs",
    )?;
    let cat = catalogue();
    let mut cases = Vec::new();
    let mut normal_queries = 0;
    for (run_index, &size) in job.page_sequence.iter().enumerate() {
        for name in ["creates", "spends"] {
            let q = &manifest["queries"][name];
            let id = q["query"].as_str().ok_or("query id")?;
            check(
                id == if name == "creates" {
                    "get_asset_creates:5918f8e1d2"
                } else {
                    "get_asset_spends:bc73c8ec39"
                },
                "only the two pinned event statements",
            )?;
            let s = cat
                .entries
                .iter()
                .find(|e| e.id == id)
                .ok_or("catalogue statement absent")?;
            check(
                hash(s.sql.as_bytes()) == q["literal_sql_sha256"].as_str().ok_or("SQL hash")?,
                "literal SQL changed",
            )?;
            let types = q["parameter_types"].as_array().ok_or("types")?;
            check(
                s.parameters
                    .iter()
                    .map(String::as_str)
                    .eq(types.iter().map(|v| v.as_str().unwrap_or(""))),
                "native types differ",
            )?;
            let mut values = q["base_parameter_values"]
                .as_array()
                .ok_or("values")?
                .clone();
            let file_name = format!("{run_index}-{size}-{name}.pages.jsonl");
            let path = job.output.join(&file_name);
            let mut output =
                BufWriter::new(File::options().write(true).create_new(true).open(&path)?);
            let start = Instant::now();
            let mut total = 0usize;
            let mut offset = 0i32;
            let mut times = Vec::new();
            loop {
                check(times.len() < 10000 && total <= 1_000_000, "case bound")?;
                values[5] = json!(size);
                values[6] = json!(offset);
                eprintln!(
                    "{}",
                    json!({"event":"native_page_started","run_index":run_index,"size":size,"name":name,"offset":offset})
                );
                let args = binds(types, &values)?;
                let page_started = Instant::now();
                let page = rows(&tx, s, &args).await?;
                let elapsed = page_started.elapsed().as_secs_f64();
                check(
                    page.len() <= usize::try_from(size)? && elapsed < 60.0,
                    "native page bound",
                )?;
                let count = page.len();
                total += count;
                normal_queries += 1;
                times.push(elapsed);
                serde_json::to_writer(
                    &mut output,
                    &json!({"query":id,"sql_sha256":q["literal_sql_sha256"],"page_size":size,"offset":offset,"parameter_types":types,"parameter_values":values,"parameter_OIDs":q["parameter_OIDs"],"rows":page,"elapsed_seconds":elapsed,"run_index":run_index}),
                )?;
                output.write_all(b"\n")?;
                output.flush()?;
                if count < usize::try_from(size)? {
                    break;
                }
                offset = offset.checked_add(size).ok_or("offset overflow")?;
            }
            output.flush()?;
            output.get_ref().sync_all()?;
            let whole = start.elapsed().as_secs_f64();
            check(
                Some(total as u64) == q["expected_rows"].as_u64(),
                "actual count differs from sealed archived oracle",
            )?;
            // This extra empty observation is separately timed, not silently charged as a normal short-page loop.
            values[6] = json!(i32::try_from(total)?);
            let args = binds(types, &values)?;
            let empty_started = Instant::now();
            let empty = rows(&tx, s, &args).await?;
            let empty_seconds = empty_started.elapsed().as_secs_f64();
            check(
                empty.is_empty() && empty_seconds < 60.0,
                "post-end empty probe",
            )?;
            let item = json!({"run_index":run_index,"query":id,"name":name,"size":size,"rows":total,"pages":times.len(),"last_offset":offset,"last_count":total-usize::try_from(offset)?,"sum_native_page_seconds":times.iter().sum::<f64>(),"max_native_page_seconds":times.iter().copied().fold(0.0,f64::max),"whole_case_seconds_including_output_fsync":whole,"auxiliary_empty_probe_seconds":empty_seconds,"auxiliary_empty_probe_count":empty.len(),"auxiliary_empty_probe_offset":total,"output":file_name,"output_sha256":hash(&fs::read(&path)?),"normal_case_queries":times.len()});
            eprintln!("{}", json!({"event":"native_case_finished","case":item}));
            cases.push(item);
            fs::write(
                job.output.join("checkpoint.json"),
                serde_json::to_vec_pretty(&json!({"cases":cases}))?,
            )?;
        }
    }
    tx.commit().await?;
    Ok(
        json!({"status":"NATIVE_ROW_PATH_COMPLETE_RR_COMMITTED","cases":cases,"normal_case_queries":normal_queries,"whole_experiment_seconds":began.elapsed().as_secs_f64(),"source_transaction_committed":true,"reader_dropped":false,"interpretation":"native rows_limited prepare/query/canonical path; file output is extra measured overhead; no full convert/capture or live fit assertion"}),
    )
}
#[tokio::main]
async fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.get(1).is_some_and(|s| s == "--self-test") {
        check(
            config(
                "host=127.0.0.1 port=5432 user=db_sync_alt_reader dbname=fixture sslmode=disable",
            )
            .is_ok(),
            "config positive",
        )?;
        for text in [
            "host=remote.invalid port=5432 user=db_sync_alt_reader dbname=db_sync_alt",
            "host=127.0.0.1 port=5432 user=postgres dbname=fixture",
        ] {
            check(config(text).is_err(), "config negative")?;
        }
        println!(
            "{}",
            json!({"self_test":"PASS_NO_CONNECTION","source":option_env!("DSA_BENCH_SOURCE_SHA"),"native_compare_source_sha256":hash(include_bytes!("../src/compare.rs"))})
        );
        return Ok(());
    }
    check(
        args.len() == 2,
        "usage: a7_capture_pages CONFIG or --self-test",
    )?;
    let job: Job = serde_json::from_slice(&fs::read(&args[1])?)?;
    check(
        job.page_sequence == [256, 1024, 4096, 4096, 1024, 256],
        "only fixed symmetric256/1024/4096 sequence",
    )?;
    check(
        job.output.is_dir() && fs::read_dir(&job.output)?.next().is_none(),
        "fresh allocated output directory required",
    )?;
    let bytes = fs::read(&job.manifest)?;
    check(hash(&bytes) == job.manifest_sha256, "manifest hash")?;
    let manifest: Value = serde_json::from_slice(&bytes)?;
    let connect_started = Instant::now();
    let mut reader = Reader::connect(config(&job.database)?, None).await?;
    reader.limits(Some(60), Some(60)).await?;
    let cancel = reader.cancellation();
    #[cfg(unix)]
    let mut signal = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let work = experiment(&mut reader, &job, &manifest);
    tokio::pin!(work);
    let mut result = tokio::select! { r=&mut work=>r?, _=tokio::time::sleep(Duration::from_secs(1800))=>{let _=tokio::time::timeout(Duration::from_secs(5),cancel.cancel()).await;return Err("finite archive benchmark deadline".into());}, _=async { #[cfg(unix)] { signal.recv().await; } #[cfg(not(unix))] { let _=tokio::signal::ctrl_c().await; }}=>{let _=tokio::time::timeout(Duration::from_secs(5),cancel.cancel()).await;return Err("benchmark interrupted".into());} };
    // experiment's transaction is committed; the caller still verifies backend/resource termination.
    result["connect_to_result_seconds"] = json!(connect_started.elapsed().as_secs_f64());
    fs::write(
        job.output.join("result.json"),
        serde_json::to_vec_pretty(&result)?,
    )?;
    println!("{}", result);
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn target_refusals() {
        assert!(
            config(
                "host=127.0.0.1 port=5432 user=db_sync_alt_reader dbname=fixture sslmode=disable"
            )
            .is_ok()
        );
        assert!(
            config("host=127.0.0.1 port=5432 user=db_sync_alt_reader dbname=db_sync_alt").is_err()
        );
        assert!(config("host=127.0.0.1 port=5432 user=db_sync_alt_reader dbname=fixture options='-c work_mem=1GB'").is_err());
    }
    #[test]
    fn typed_bigints_and_narrowing() {
        assert!(matches!(
            binds(&[json!("int8")], &[json!(i64::MAX)]).unwrap()[0],
            Bind::I64(i64::MAX)
        ));
        assert!(binds(&[json!("int4")], &[json!(i64::MAX)]).is_err());
        assert!(binds(&[json!("int8")], &[json!(1.0)]).is_err());
    }
}
