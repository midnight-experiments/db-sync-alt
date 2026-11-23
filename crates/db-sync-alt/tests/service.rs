//! The service's parts besides following: the `health` command against a
//! running HTTP server (no database), and against PostgreSQL 17 in
//! DSA_TEST_DATABASE the status document of `db-sync-alt status`, the HTTP
//! answers from a refreshed database view, index upkeep, a month of churn,
//! the daily snapshot files and the supply check. DSA_HTTP_REQUESTS,
//! DSA_INDEX_BLOAT_ROWS and DSA_CHURN_{DAYS,BLOCKS_PER_DAY,STEPS_PER_DAY}
//! set the sizes of the longer tests.

#[path = "common/legacy.rs"]
#[allow(dead_code)]
mod common;
#[path = "follow/harness.rs"]
#[allow(dead_code)]
mod harness;

use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    process::Output,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use chrono::NaiveTime;
use common::{Database, connect};
use dsa_chain::profiles::Profile;
use dsa_service::{
    config::Snapshots,
    follow::ProviderTip,
    http::serve,
    status::{self, Documents, FollowerView, State},
    upkeep::{self, IndexPolicy, Job, Schedule},
};
use dsa_store::Store;
use harness::{K, Result, Setup, WEEK, advance, named};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{oneshot, watch},
};

/// An HTTP server of the service in this process, answering `healthy`.
async fn health_server(
    healthy: bool,
) -> (SocketAddr, watch::Sender<Documents>, oneshot::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (documents, receiver) = watch::channel(Documents {
        healthy,
        body: "{}".into(),
    });
    let (stop, stopped) = oneshot::channel::<()>();
    tokio::spawn(serve(listener, receiver, async {
        let _ = stopped.await;
    }));
    (address, documents, stop)
}

async fn health(arguments: &[&str], environment: &[(&str, &str)]) -> (Output, Duration) {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_db-sync-alt"));
    command
        .arg("health")
        .args(arguments)
        .env_remove("DSA_HEALTH_URL");
    for (name, value) in environment {
        command.env(name, value);
    }
    let started = Instant::now();
    let output = command.output().await.unwrap();
    (output, started.elapsed())
}

/// The `health` command exits 0 on 200 and 1 otherwise within 3 seconds,
/// reads `DSA_HEALTH_URL`, and the flag wins.
#[tokio::test]
async fn the_health_command_exits_0_on_200_and_1_otherwise() -> Result {
    let (address, documents, stop) = health_server(true).await;
    let url = format!("http://{address}/health");
    let (output, took) = health(&["--url", &url], &[]).await;
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert!(took < Duration::from_secs(3));
    let printed: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(printed["http_status"], 200);
    // From the environment, and the flag wins over it.
    let (output, _) = health(&[], &[("DSA_HEALTH_URL", &url)]).await;
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let (output, _) = health(
        &["--url", &url],
        &[("DSA_HEALTH_URL", "http://127.0.0.1:1/health")],
    )
    .await;
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    // Unhealthy: 503 is exit 1.
    documents.send(Documents {
        healthy: false,
        body: "{}".into(),
    })?;
    let (output, took) = health(&["--url", &url], &[]).await;
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert!(took < Duration::from_secs(3));
    assert!(String::from_utf8_lossy(&output.stderr).contains("service_unhealthy"));
    // Nothing listens.
    let _ = stop.send(());
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (output, took) = health(&["--url", &url], &[]).await;
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert!(took < Duration::from_secs(3));
    // A listener that never answers.
    let silent = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/health", silent.local_addr()?);
    let (output, took) = health(&["--url", &url], &[]).await;
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert!(took < Duration::from_millis(3500), "{took:?}");
    // An invalid address is a usage error, found before any connection.
    for invalid in ["https://127.0.0.1/health", "127.0.0.1:18080", "http://h:0/"] {
        let (output, _) = health(&["--url", invalid], &[]).await;
        assert_eq!(output.status.code(), Some(2), "{invalid}");
        assert!(String::from_utf8_lossy(&output.stderr).contains("\"flag\":\"--url\""));
    }
    let (output, _) = health(&[], &[("DSA_HEALTH_URL", "ftp://x")]).await;
    assert_eq!(output.status.code(), Some(2));
    drop(silent);
    Ok(())
}

fn setting(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs())
}

/// The keys of the document and of its fixed sections.
fn shape(document: &Value) -> BTreeSet<String> {
    let mut keys = BTreeSet::new();
    for (key, value) in document.as_object().unwrap() {
        keys.insert(key.clone());
        if matches!(
            key.as_str(),
            "lag"
                | "tip_lag"
                | "progress"
                | "retention"
                | "provider"
                | "epochs"
                | "integrity"
                | "database"
                | "freshness"
                | "health"
                | "capabilities"
                | "first_observations"
        ) {
            for inner in value.as_object().unwrap().keys() {
                keys.insert(format!("{key}/{inner}"));
            }
        }
    }
    keys
}

/// A follower at the stored tip whose provider tip was read `now`.
fn at_tip(view: &dsa_store::service_view::DatabaseView, now: u64) -> FollowerView {
    let tip = view.tip.as_ref().unwrap();
    FollowerView {
        state: State::Following,
        reason: "at_tip".to_owned(),
        wait_until: None,
        provider_tip: Some(ProviderTip {
            height: tip.height,
            hash: tip.hash.clone(),
            time: u64::try_from(tip.timestamp).unwrap(),
            observed_at: now,
        }),
        requests_per_second: 8.0,
        requests_in_flight: 0,
        last_refusal: None,
        blocks_remaining: Some(0),
        blocks_per_hour: None,
    }
}

/// One GET; the status code and the body.
async fn http_get(address: SocketAddr, path: &str) -> Result<(u16, String)> {
    let mut stream = TcpStream::connect(address).await?;
    stream
        .write_all(format!("GET {path} HTTP/1.1\r\nHost: test\r\n\r\n").as_bytes())
        .await?;
    let mut answer = String::new();
    stream.read_to_string(&mut answer).await?;
    let (head, body) = answer.split_once("\r\n\r\n").ok_or("no head")?;
    Ok((head[9..12].parse()?, body.to_owned()))
}

/// Test 1 (database part): `db-sync-alt status` prints the document of a
/// stopped service from the database alone, with the shape of the golden
/// document; a stored `following` is not claimed; a closed gate is named.
#[tokio::test]
#[ignore = "requires isolated PostgreSQL 17"]
async fn the_status_command_prints_the_document_of_a_stopped_service() -> Result {
    let f = common::fixture(201);
    let network = f.profile.network.clone();
    let source = Database::new().await?;
    source.legacy(&f).await?;
    let target = Database::new().await?;
    target
        .cli(
            "convert",
            &network,
            &[
                "--source",
                &source.dsn(true),
                "--horizon-block",
                &f.bounds.horizon.to_string(),
            ],
        )
        .await?;
    target
        .cli("grant-reader", &network, &["--role", &target.reader])
        .await?;
    // A process recorded `following` and ended without a stop.
    let mut store = Store::connect(target.config.clone(), None).await?;
    store
        .record_state("following", "at_tip", &json!({}), None)
        .await
        .map_err(named)?;
    drop(store);
    let document = target
        .cli("status", &network, &["--reader", &target.reader])
        .await?;
    assert_eq!(document["state"], "stopped", "{document}");
    assert_eq!(document["state_reason"], "service_not_running");
    assert_eq!(document["service_state"]["state"], "following");
    assert_eq!(document["serving"], "ready");
    assert_eq!(document["sync"], "unknown");
    assert_eq!(document["healthy"], false);
    let reasons = document["reason_codes"].as_array().unwrap();
    assert!(reasons.contains(&json!("state_stopped")), "{reasons:?}");
    assert!(
        !reasons
            .iter()
            .any(|reason| reason.as_str().unwrap().starts_with("serving_u")
                || reason.as_str().unwrap().starts_with("reader")),
        "{reasons:?}"
    );
    assert_eq!(document["database"]["reader_check"]["passed"], true);
    assert_eq!(document["served_target"]["height"], f.bounds.target);
    assert_eq!(document["horizon"]["height"], f.bounds.horizon);
    assert!(document["provider_head"].is_null() && document["lag"]["blocks"].is_null());
    let golden: Value = serde_json::from_str(include_str!(
        "../../dsa-service/tests/status/service_not_running.json"
    ))?;
    assert_eq!(shape(&document), shape(&golden));
    // The reader comes from DSA_READER too.
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_db-sync-alt"))
        .args([
            "status",
            "--database",
            &target.dsn(false),
            "--network",
            &network,
        ])
        .env("DSA_READER", &target.reader)
        .output()
        .await?;
    let from_environment: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(
        from_environment["database"]["reader_check"]["role"],
        target.reader.as_str()
    );
    // A check closes the gate: the document names it.
    let mut store = Store::connect(target.config.clone(), None).await?;
    store
        .close_gate("supply_mismatch", &json!({"unit": "planted"}))
        .await
        .map_err(named)?;
    drop(store);
    let document = target.cli("status", &network, &[]).await?;
    assert_eq!(
        (
            &document["state"],
            &document["state_reason"],
            &document["error_code"],
            &document["serving"]
        ),
        (
            &json!("stopped"),
            &json!("supply_mismatch"),
            &json!("supply_mismatch"),
            &json!("revoked")
        )
    );
    assert!(
        document["reason_codes"]
            .as_array()
            .unwrap()
            .contains(&json!("serving_revoked"))
    );
    let _ = Profile::load(&network)?;
    source.cleanup().await?;
    target.cleanup().await
}

async fn sessions(admin: &tokio_postgres::Client, name: &str) -> Result<Vec<(i32, String)>> {
    Ok(admin
        .query(
            "SELECT pid,coalesce(query_start::text,'')||' '||coalesce(state_change::text,'') FROM pg_stat_activity
             WHERE datname=$1 AND backend_type='client backend' ORDER BY pid",
            &[&name],
        )
        .await?
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect())
}

/// Every node of an `EXPLAIN (FORMAT JSON)` plan.
fn plan_nodes(plan: &Value, nodes: &mut Vec<Value>) {
    if let Some(object) = plan.as_object() {
        if object.contains_key("Node Type") {
            let mut node = object.clone();
            node.remove("Plans");
            nodes.push(Value::Object(node));
        }
        for value in object.values() {
            plan_nodes(value, nodes);
        }
    } else if let Some(items) = plan.as_array() {
        for item in items {
            plan_nodes(item, nodes);
        }
    }
}

/// Test 2 (database part): answers come from the last view; no statement
/// runs on any session of the database while 1,000 requests are answered;
/// every served table the view reads has an index path.
#[tokio::test]
#[ignore = "requires isolated PostgreSQL 17"]
async fn http_answers_come_from_the_last_view_and_run_no_statement() -> Result {
    let requests = setting("DSA_HTTP_REQUESTS", 200);
    let mut setup = Setup::new(4101, 1, 60, false, WEEK).await?;
    for _ in 0..3 {
        let plan = advance(&setup.g, &setup.follower, 2, None);
        setup.step(&plan).await?;
    }
    let profile = setup.profile.clone();
    setup
        .follower
        .store
        .grant_reader(&profile, &setup.database.reader)
        .await
        .map_err(named)?;
    // The refresh: one view on a connection of its own.
    let mut refresh = Store::connect(setup.database.config.clone(), None).await?;
    let view = refresh
        .service_view(&profile, Some(&setup.database.reader))
        .await
        .map_err(named)?;
    let settings = status::Settings::new(&profile.network);
    let now = unix_now();
    let (_documents, receiver) = watch::channel(status::render(
        Some(&view),
        Some(&at_tip(&view, now)),
        &settings,
        now,
    ));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (stop, stopped) = oneshot::channel::<()>();
    let server = tokio::spawn(serve(listener, receiver, async {
        let _ = stopped.await;
    }));
    let before = sessions(&setup.database.admin, &setup.database.name).await?;
    assert!(before.len() >= 2, "{before:?}");
    let started = Instant::now();
    for index in 0..requests {
        let path = if index % 2 == 0 { "/status" } else { "/health" };
        let (code, body) = http_get(address, path).await?;
        assert_eq!(code, 200, "{path}: {body}");
        if index == 0 {
            let document: Value = serde_json::from_str(&body)?;
            assert_eq!(document["state"], "following");
            assert_eq!(document["healthy"], true, "{}", document["reason_codes"]);
            assert_eq!(
                document["served_target"]["height"],
                setup.follower.target.height
            );
        }
    }
    let elapsed = started.elapsed();
    let after = sessions(&setup.database.admin, &setup.database.name).await?;
    assert_eq!(before, after, "a session of the database ran a statement");
    eprintln!(
        "{}",
        json!({"event":"http_requests_without_statements","requests":requests,"seconds":elapsed.as_secs_f64(),"sessions":before.len()})
    );
    let _ = stop.send(());
    server.await?;
    // Every served table the view reads has an index path.
    refresh
        .client()
        .batch_execute("BEGIN;SET LOCAL enable_seqscan=off")
        .await?;
    let plan: Value = refresh
        .client()
        .query_one(
            &format!(
                "EXPLAIN (VERBOSE, FORMAT JSON) {}",
                dsa_store::service_view::STATEMENT
            ),
            &[],
        )
        .await?
        .get(0);
    refresh.client().batch_execute("ROLLBACK").await?;
    let mut nodes = Vec::new();
    plan_nodes(&plan, &mut nodes);
    let served = nodes
        .iter()
        .filter(|node| node["Schema"] == "serve")
        .collect::<Vec<_>>();
    assert!(served.len() >= 3, "{plan}");
    for node in served {
        assert_ne!(node["Node Type"], "Seq Scan", "{node}");
    }
    drop(refresh);
    setup.cleanup().await
}

/// The layout's index names, each with its count in `pg_indexes`.
async fn index_name_counts(
    client: &tokio_postgres::Client,
    names: &[String],
) -> Result<BTreeMap<String, i64>> {
    Ok(client
        .query(
            "SELECT n,(SELECT count(*) FROM pg_indexes WHERE indexname=n) FROM unnest($1::text[]) n",
            &[&names],
        )
        .await?
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect())
}

/// Test 4: a rebuild of bloated indexes runs while a node reader repeats a
/// literal node query: no error, no wait above one second, and every node
/// index name exactly once in `pg_indexes` at every sample. An interrupted
/// rebuild's leftover is dropped at the next start.
#[tokio::test]
#[ignore = "requires isolated PostgreSQL 17"]
async fn an_index_rebuild_keeps_each_index_name_once_while_the_node_reads() -> Result {
    let rows = setting("DSA_INDEX_BLOAT_ROWS", 60_000);
    let mut setup = Setup::new(4301, 1, 60, false, WEEK).await?;
    let profile = setup.profile.clone();
    setup
        .follower
        .store
        .grant_reader(&profile, &setup.database.reader)
        .await
        .map_err(named)?;
    let upkeep_store = Store::connect(setup.database.config.clone(), None).await?;
    let policy = IndexPolicy {
        growth: 2.0,
        min_bytes: 256 * 1024,
    };
    // The first look records every index's size.
    assert!(
        upkeep::rebuild_indexes(&upkeep_store, &policy)
            .await
            .map_err(named)?
            .is_none()
    );
    let recorded = upkeep_store.index_sizes().await.map_err(named)?;
    assert!(recorded.iter().all(|index| index.rebuilt.is_some()));
    let names = recorded
        .iter()
        .map(|index| index.name.clone())
        .collect::<Vec<_>>();
    // Bloat: rows the indexes of tx_out take in, then lose.
    let owner = connect(&setup.database.config).await?;
    owner
        .batch_execute(&format!(
            "INSERT INTO serve.tx_out(id,tx_id,index,address,value)
             SELECT t*4096,t,0,'planted_'||t,1 FROM generate_series(100000000::bigint,{}) t;
             DELETE FROM serve.tx_out WHERE tx_id>=100000000",
            100_000_000 + rows - 1
        ))
        .await?;
    owner.batch_execute("VACUUM serve.tx_out").await?;
    let bloated = upkeep_store.index_sizes().await.map_err(named)?;
    let grown = bloated
        .iter()
        .filter(|index| {
            index.bytes > 2 * index.rebuilt.unwrap().bytes && index.bytes > policy.min_bytes
        })
        .count();
    assert!(grown >= 2, "{bloated:?}");
    // A node reader repeats the node's governance query; a sampler reads
    // pg_indexes.
    let statement = dsa_store::catalogue::catalogue()
        .entries
        .iter()
        .find(|entry| entry.name == "get_governance_body_utxo")
        .ok_or("no governance statement")?
        .sql
        .clone();
    let council_address = profile.addresses.council.clone();
    let council_policy = hex::decode(&profile.assets.council.policy)?;
    let target = i32::try_from(setup.follower.target.height)?;
    let stop = Arc::new(AtomicBool::new(false));
    let reader = tokio::spawn({
        let (stop, config) = (Arc::clone(&stop), setup.database.reader_config());
        async move {
            let client = connect(&config).await?;
            let prepared = client.prepare(&statement).await?;
            let (mut runs, mut rows, mut longest, mut errors) =
                (0_u64, 0_usize, Duration::ZERO, Vec::new());
            while !stop.load(Ordering::SeqCst) {
                let started = Instant::now();
                match client
                    .query(&prepared, &[&council_address, &council_policy, &target])
                    .await
                {
                    Ok(found) => rows += found.len(),
                    Err(error) => errors.push(error.to_string()),
                }
                longest = longest.max(started.elapsed());
                runs += 1;
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((runs, rows, longest, errors))
        }
    });
    let sampler = tokio::spawn({
        let (stop, config, names) = (
            Arc::clone(&stop),
            setup.database.config.clone(),
            names.clone(),
        );
        async move {
            let client = connect(&config).await?;
            let (mut samples, mut violations) = (0_u64, Vec::new());
            while !stop.load(Ordering::SeqCst) {
                let counts = index_name_counts(&client, &names).await?;
                if counts.values().any(|count| *count != 1) {
                    violations.push(counts);
                }
                samples += 1;
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((samples, violations))
        }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut rebuilds = Vec::new();
    while let Some(rebuild) = upkeep::rebuild_indexes(&upkeep_store, &policy)
        .await
        .map_err(named)?
    {
        rebuilds.push(rebuild);
        assert!(rebuilds.len() <= names.len(), "rebuilds do not end");
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    stop.store(true, Ordering::SeqCst);
    let (runs, found, longest, errors) = reader.await??;
    let (samples, violations) = sampler.await??;
    eprintln!(
        "{}",
        json!({"event":"index_rebuilds","planted_rows":rows,"rebuilds":rebuilds,"reader_queries":runs,"reader_rows":found,
            "reader_longest_seconds":longest.as_secs_f64(),"pg_indexes_samples":samples})
    );
    assert_eq!(rebuilds.len(), grown, "{rebuilds:?}");
    assert!(
        rebuilds
            .iter()
            .all(|rebuild| rebuild.after < rebuild.before)
    );
    assert!(errors.is_empty(), "{errors:?}");
    assert!(longest < Duration::from_secs(1), "{longest:?}");
    assert!(runs > 10 && samples > 10, "{runs} {samples}");
    assert!(violations.is_empty(), "{violations:?}");
    let rebuilt = upkeep_store.index_sizes().await.map_err(named)?;
    assert_eq!(upkeep::index_decision(&rebuilt, &policy).rebuild, None);
    // An interrupted rebuild: a writer's open transaction keeps a rebuild
    // waiting after its new index exists; the rebuild is cancelled there.
    let blocker = connect(&setup.database.config).await?;
    blocker
        .batch_execute("BEGIN;DELETE FROM serve.tx_out WHERE false")
        .await?;
    let rebuilding = connect(&setup.database.config).await?;
    let pid: i32 = rebuilding
        .query_one("SELECT pg_backend_pid()", &[])
        .await?
        .get(0);
    let interrupted = tokio::spawn(async move {
        rebuilding
            .batch_execute("REINDEX INDEX CONCURRENTLY serve.idx_tx_out_tx_id")
            .await
    });
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let exists: bool = owner
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_class WHERE relname='idx_tx_out_tx_id_ccnew')",
                &[],
            )
            .await?
            .get(0);
        if exists {
            break;
        }
        assert!(Instant::now() < deadline, "the rebuild never started");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    owner
        .query_one("SELECT pg_cancel_backend($1)", &[&pid])
        .await?;
    assert!(interrupted.await?.is_err());
    blocker.batch_execute("ROLLBACK").await?;
    let leftover: Vec<(String, bool)> = owner
        .query(
            "SELECT c.relname::text,i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
             WHERE c.relnamespace='serve'::regnamespace AND c.relname LIKE '%\\_cc%'",
            &[],
        )
        .await?
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(leftover, vec![("idx_tx_out_tx_id_ccnew".to_owned(), false)]);
    assert!(
        index_name_counts(&owner, &names)
            .await?
            .values()
            .all(|count| *count == 1)
    );
    // The next start drops it.
    let dropped = upkeep::start_indexes(&upkeep_store).await.map_err(named)?;
    assert_eq!(dropped, vec!["idx_tx_out_tx_id_ccnew".to_owned()]);
    let remaining: i64 = owner
        .query_one(
            "SELECT count(*) FROM pg_class WHERE relnamespace='serve'::regnamespace AND relname LIKE '%\\_cc%'",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(remaining, 0);
    assert!(
        upkeep::start_indexes(&upkeep_store)
            .await
            .map_err(named)?
            .is_empty()
    );
    setup.follower.store.verify(&profile).await.map_err(named)?;
    drop((owner, upkeep_store));
    setup.cleanup().await
}

/// Heap (with TOAST) and index bytes of the served and state tables.
async fn relation_bytes(
    client: &tokio_postgres::Client,
) -> Result<(i64, i64, BTreeMap<String, i64>)> {
    let row = client
        .query_one(
            "SELECT coalesce(sum(pg_relation_size(c.oid)+coalesce(pg_relation_size(nullif(c.reltoastrelid,0)),0)) FILTER(WHERE c.relkind='r'),0)::bigint,
             coalesce(sum(pg_relation_size(c.oid)) FILTER(WHERE c.relkind='i'),0)::bigint
             FROM pg_class c WHERE c.relnamespace IN ('serve'::regnamespace,'dsa'::regnamespace)",
            &[],
        )
        .await?;
    let indexes = client
        .query(
            "SELECT relname::text,pg_relation_size(oid) FROM pg_class WHERE relnamespace='serve'::regnamespace AND relkind='i' ORDER BY 1",
            &[],
        )
        .await?
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    Ok((row.get(0), row.get(1), indexes))
}

async fn database_bytes(client: &tokio_postgres::Client) -> Result<i64> {
    Ok(client
        .query_one("SELECT pg_database_size(current_database())", &[])
        .await?
        .get(0))
}

/// The served and state tables; vacuuming them stands in for autovacuum,
/// which works on wall time while the test's days pass in seconds.
const VACUUMED: &str = "VACUUM serve.block,serve.tx,serve.tx_out,serve.tx_in,serve.ma_tx_out,serve.multi_asset,serve.datum,\
    serve.epoch_param,serve.pool_hash,serve.epoch_stake,serve.tx_metadata,dsa.first_served,dsa.consumed_before_horizon,\
    dsa.unspent,dsa.service_state,dsa.calls,dsa.upkeep,dsa.position,dsa.supply";

/// Test 5: thirty simulated days of churn with a retention of one day, the
/// horizon advanced every step and the upkeep on its schedule: the served
/// and state tables stay within 1.3 times a fresh restore of the final
/// content (SC-012).
#[tokio::test]
#[ignore = "requires isolated PostgreSQL 17"]
async fn thirty_days_of_churn_stay_within_a_fresh_restore() -> Result {
    let days = setting("DSA_CHURN_DAYS", 30) as u32;
    let blocks_per_day = setting("DSA_CHURN_BLOCKS_PER_DAY", 48) as u32;
    let steps_per_day = setting("DSA_CHURN_STEPS_PER_DAY", 4).max(1) as u32;
    let blocks_per_step = (blocks_per_day / steps_per_day).max(1);
    let seed = 4502;
    let profile = harness::profile(seed);
    let (first, target) = (1, 11);
    let total = target + days * steps_per_day * blocks_per_step + K + 2;
    let began = Instant::now();
    let mut g = harness::chain(&profile, seed, first, total, false);
    g.lengthen_epochs(blocks_per_day * 5);
    let history = harness::derive(&profile, &g).await?;
    let headers = (first..=target + K)
        .map(|height| harness::header(g.block(height).unwrap()))
        .collect::<Vec<_>>();
    let mut source = harness::Source::new().await?;
    source.start(&g, &history, target, &headers).await?;
    drop(history);
    let database = Database::new().await?;
    harness::convert(&database, &source.database, &profile, target - 2, 86_400).await?;
    source.database.cleanup().await?;
    let staked = (first..=target)
        .map(|height| g.block(height).unwrap().epoch)
        .collect();
    let mut follower = harness::Follower::open(&database.config, &profile, true, staked).await?;
    let owner = connect(&database.config).await?;
    let upkeep_store = Store::connect(database.config.clone(), None).await?;
    let mut snapshot_store = Store::connect(database.config.clone(), None).await?;
    let dir = tempfile::tempdir()?;
    let snapshots = Snapshots {
        dir: dir.path().to_owned(),
        keep: 2,
        at: NaiveTime::from_hms_opt(3, 0, 0).unwrap(),
    };
    let start_bytes = relation_bytes(&owner).await?;
    // 2026-11-18 00:00 UTC; the clock of the upkeep schedule.
    let mut now: u64 = 1_794_960_000;
    let step_seconds = u64::from(86_400 / steps_per_day);
    let mut schedule = Schedule::default();
    let mut last_written = None;
    let (mut supply_checks, mut index_looks, mut files) = (0_u32, 0_u32, 0_u32);
    let mut rebuilds = Vec::new();
    let mut daily = Vec::new();
    for step in 0..days * steps_per_day {
        let new_target = follower.target.height + blocks_per_step;
        let horizon = new_target.saturating_sub(blocks_per_day);
        let prune_to = (horizon > follower.horizon).then_some(horizon);
        let plan = advance(&g, &follower, blocks_per_step, prune_to);
        follower.update(&profile, &g, &plan).await?;
        owner.batch_execute(VACUUMED).await?;
        now += step_seconds;
        for job in schedule.due(Some(&snapshots), last_written, now) {
            match job {
                Job::Supply => {
                    let check = upkeep::check_supply(&mut follower.store, &profile)
                        .await
                        .map_err(named)?;
                    assert!(check.passed, "{check:?}");
                    schedule.supply_checked = Some(now);
                    supply_checks += 1;
                }
                Job::Indexes => {
                    if let Some(rebuild) =
                        upkeep::rebuild_indexes(&upkeep_store, &IndexPolicy::default())
                            .await
                            .map_err(named)?
                    {
                        rebuilds.push(rebuild);
                    }
                    schedule.indexes_checked = Some(now);
                    index_looks += 1;
                }
                Job::Snapshot => {
                    upkeep::write_snapshot(&mut snapshot_store, &profile, &snapshots)
                        .await
                        .map_err(named)?;
                    last_written = Some(now);
                    files += 1;
                }
            }
        }
        if (step + 1) % steps_per_day == 0 {
            daily.push(
                json!({"day":(step + 1) / steps_per_day,"target":follower.target.height,
                "horizon":follower.horizon,"database_bytes":database_bytes(&owner).await?,
                "relations":relation_bytes(&owner).await?.0 + relation_bytes(&owner).await?.1}),
            );
        }
    }
    follower.store.verify(&profile).await.map_err(named)?;
    // The final content, restored into a fresh database.
    let file = dir.path().join("final.snapshot");
    let description = follower
        .store
        .snapshot(&profile, &file)
        .await
        .map_err(named)?;
    let fresh = Database::new().await?;
    let mut restored = Store::connect(fresh.config.clone(), None).await?;
    let loaded = restored
        .restore(&profile, &file, Some(&description.verified.content_digest))
        .await
        .map_err(named)?;
    assert_eq!(loaded.content_digest, description.verified.content_digest);
    let fresh_client = connect(&fresh.config).await?;
    let (churned_heap, churned_indexes, churned_each) = relation_bytes(&owner).await?;
    let (fresh_heap, fresh_indexes, fresh_each) = relation_bytes(&fresh_client).await?;
    let ratio = (churned_heap + churned_indexes) as f64 / (fresh_heap + fresh_indexes) as f64;
    let kept = std::fs::read_dir(dir.path())?
        .filter_map(|entry| entry.ok()?.file_name().into_string().ok())
        .filter(|name| name.starts_with(&profile.network))
        .count();
    let evidence = json!({
        "event": "churn",
        "days": days, "blocks_per_day": blocks_per_day, "steps_per_day": steps_per_day,
        "final_target": follower.target.height, "final_horizon": follower.horizon,
        "start_relation_bytes": start_bytes.0 + start_bytes.1,
        "churned": {"heap": churned_heap, "indexes": churned_indexes, "database": database_bytes(&owner).await?},
        "fresh": {"heap": fresh_heap, "indexes": fresh_indexes, "database": database_bytes(&fresh_client).await?},
        "ratio": ratio,
        "index_bytes": churned_each.iter().map(|(name, bytes)| (name.clone(), json!({"churned": bytes, "fresh": fresh_each.get(name)}))).collect::<serde_json::Map<_, _>>(),
        "supply_checks": supply_checks, "index_looks": index_looks, "rebuilds": rebuilds,
        "snapshot_files_written": files, "snapshot_files_kept": kept,
        "daily": daily, "seconds": began.elapsed().as_secs_f64(),
    });
    eprintln!("{evidence}");
    assert!(ratio <= 1.3, "{evidence}");
    assert_eq!(supply_checks, days * 4);
    assert_eq!(index_looks, days * 24.min(steps_per_day));
    assert_eq!(files, days);
    assert_eq!(kept, 2);
    drop((
        follower,
        owner,
        upkeep_store,
        snapshot_store,
        restored,
        fresh_client,
    ));
    database.cleanup().await?;
    fresh.cleanup().await
}

/// Test 6: the daily snapshot file on an injected clock: a start after the
/// time of day, a day without a run, a failed write tried again without
/// losing the older files, the keep count; the recorded digest equals the
/// one `verify` computes.
#[tokio::test]
#[ignore = "requires isolated PostgreSQL 17"]
async fn daily_snapshot_files_follow_their_schedule_and_keep_the_newest() -> Result {
    use std::os::unix::fs::PermissionsExt;
    const HOUR: u64 = 3600;
    const DAY: u64 = 1_794_960_000;
    let mut setup = Setup::new(4601, 1, 80, false, WEEK).await?;
    let profile = setup.profile.clone();
    let network = profile.network.clone();
    let other = if network == "mainnet" {
        "preprod"
    } else {
        "mainnet"
    };
    let dir = tempfile::tempdir()?;
    let snapshots = Snapshots {
        dir: dir.path().to_owned(),
        keep: 2,
        at: NaiveTime::from_hms_opt(3, 0, 0).unwrap(),
    };
    let foreign = [
        format!("{other}-5-0123456789ab.snapshot"),
        "notes.txt".to_owned(),
    ];
    for name in &foreign {
        std::fs::write(dir.path().join(name), b"not ours")?;
    }
    let stale = dir.path().join(format!(".{network}-snapshot-left"));
    std::fs::create_dir(&stale)?;
    std::fs::write(stale.join("snapshot"), b"partial")?;
    let mut writer = Store::connect(setup.database.config.clone(), None).await?;
    let owner = connect(&setup.database.config).await?;
    // Only the snapshot job is looked at here.
    let mut schedule = Schedule {
        supply_checked: Some(u64::MAX / 2),
        indexes_checked: Some(u64::MAX / 2),
        snapshot_failed: None,
    };
    let ours = |dir: &std::path::Path| -> BTreeSet<String> {
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|entry| entry.ok()?.file_name().into_string().ok())
            .filter(|name| name.starts_with(&format!("{network}-")))
            .collect()
    };
    let due = |schedule: &Schedule, last: Option<u64>, now: u64| {
        schedule.due(Some(&snapshots), last, now) == vec![Job::Snapshot]
    };
    // A start after the time of day without that day's file: written now,
    // under its name, and recorded with the digest `verify` computes.
    let mut last = None;
    let mut now = DAY + 10 * HOUR;
    assert!(due(&schedule, last, now));
    let a = upkeep::write_snapshot(&mut writer, &profile, &snapshots)
        .await
        .map_err(named)?;
    last = Some(now);
    let verified = setup.follower.store.verify(&profile).await.map_err(named)?;
    assert_eq!(a.content_digest, verified.content_digest);
    assert_eq!(a.target, setup.follower.target.height);
    assert_eq!(
        a.file,
        upkeep::snapshot_name(&network, a.target, &a.content_digest)
    );
    assert_eq!(std::fs::metadata(dir.path().join(&a.file))?.len(), a.bytes);
    assert!(!stale.exists(), "a stale temporary directory stays");
    let view = writer.service_view(&profile, None).await.map_err(named)?;
    assert_eq!(view.upkeep.snapshot.as_ref().unwrap().record, a);
    let document = status::compose(Some(&view), None, &status::Settings::new(&network), now);
    assert_eq!(
        document.integrity.snapshot.unwrap().record.content_digest,
        a.content_digest
    );
    // Not again that day.
    now = DAY + 20 * HOUR;
    assert!(!due(&schedule, last, now));
    // The next day at the time of day.
    let plan = advance(&setup.g, &setup.follower, 2, None);
    setup.step(&plan).await?;
    now = DAY + 27 * HOUR;
    assert!(due(&schedule, last, now));
    let b = upkeep::write_snapshot(&mut writer, &profile, &snapshots)
        .await
        .map_err(named)?;
    last = Some(now);
    // A day without a run: one file at the next look.
    let plan = advance(&setup.g, &setup.follower, 2, None);
    setup.step(&plan).await?;
    now = DAY + 3 * 24 * HOUR + 9 * HOUR;
    assert!(due(&schedule, last, now));
    let c = upkeep::write_snapshot(&mut writer, &profile, &snapshots)
        .await
        .map_err(named)?;
    last = Some(now);
    assert!(!due(&schedule, last, now + HOUR));
    assert_eq!(
        ours(dir.path()),
        BTreeSet::from([b.file.clone(), c.file.clone()])
    );
    // A failed write leaves the older files and is tried again after ten
    // minutes: a database that does not serve cannot be written.
    let plan = advance(&setup.g, &setup.follower, 2, None);
    setup.step(&plan).await?;
    let kept_bytes = |names: &[&String]| -> Vec<u64> {
        names
            .iter()
            .map(|name| std::fs::metadata(dir.path().join(name)).unwrap().len())
            .collect()
    };
    let before = kept_bytes(&[&b.file, &c.file]);
    now = DAY + 4 * 24 * HOUR + 3 * HOUR;
    assert!(due(&schedule, last, now));
    owner
        .batch_execute("UPDATE dsa.position SET serving=false WHERE id=1")
        .await?;
    assert!(
        upkeep::write_snapshot(&mut writer, &profile, &snapshots)
            .await
            .is_err()
    );
    owner
        .batch_execute("UPDATE dsa.position SET serving=true WHERE id=1")
        .await?;
    schedule.snapshot_failed = Some(now);
    assert_eq!(
        ours(dir.path()),
        BTreeSet::from([b.file.clone(), c.file.clone()])
    );
    assert_eq!(kept_bytes(&[&b.file, &c.file]), before);
    // A directory that refuses the file, where permissions apply.
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o555))?;
    let refused = tempfile::tempfile_in(dir.path()).is_err();
    if refused {
        assert!(
            upkeep::write_snapshot(&mut writer, &profile, &snapshots)
                .await
                .is_err()
        );
    }
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755))?;
    assert_eq!(
        ours(dir.path()),
        BTreeSet::from([b.file.clone(), c.file.clone()])
    );
    assert!(!due(&schedule, last, now + 599));
    assert!(due(&schedule, last, now + 600));
    let d = upkeep::write_snapshot(&mut writer, &profile, &snapshots)
        .await
        .map_err(named)?;
    assert_eq!(
        ours(dir.path()),
        BTreeSet::from([c.file.clone(), d.file.clone()])
    );
    // The same content again keeps the file it already has.
    let again = upkeep::write_snapshot(&mut writer, &profile, &snapshots)
        .await
        .map_err(named)?;
    assert_eq!(again, d);
    assert_eq!(
        ours(dir.path()),
        BTreeSet::from([c.file.clone(), d.file.clone()])
    );
    for name in &foreign {
        assert!(dir.path().join(name).exists(), "{name} was removed");
    }
    // The file restores to the recorded digest.
    let fresh = Database::new().await?;
    let mut restored = Store::connect(fresh.config.clone(), None).await?;
    let loaded = restored
        .restore(&profile, &dir.path().join(&d.file), Some(&d.content_digest))
        .await
        .map_err(named)?;
    assert_eq!(loaded.content_digest, d.content_digest);
    eprintln!(
        "{}",
        json!({"event":"daily_snapshot_files","written":[a, b, c, d],"failure_with_permissions":refused})
    );
    drop((writer, owner, restored));
    fresh.cleanup().await?;
    setup.cleanup().await
}

/// Test 7: a planted supply difference closes the gate; the node reads
/// SQLSTATE 55000 and `/status` names the check.
#[tokio::test]
#[ignore = "requires isolated PostgreSQL 17"]
async fn a_supply_difference_closes_the_gate_and_the_status_names_it() -> Result {
    let mut setup = Setup::new(4701, 1, 60, false, WEEK).await?;
    let profile = setup.profile.clone();
    setup
        .follower
        .store
        .grant_reader(&profile, &setup.database.reader)
        .await
        .map_err(named)?;
    let check = upkeep::check_supply(&mut setup.follower.store, &profile)
        .await
        .map_err(named)?;
    assert!(check.passed && check.failure.is_none(), "{check:?}");
    let owner = connect(&setup.database.config).await?;
    let unspent: i64 = owner
        .query_one(
            "SELECT count(*) FROM serve.tx_out o WHERE NOT EXISTS(SELECT 1 FROM serve.tx_in i WHERE i.tx_out_id=o.tx_id AND i.tx_out_index=o.index)
             AND NOT EXISTS(SELECT 1 FROM dsa.consumed_before_horizon c WHERE c.tx_out_id=o.id)",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(check.unspent_outputs, unspent);
    let mut refresh = Store::connect(setup.database.config.clone(), None).await?;
    let view = refresh
        .service_view(&profile, Some(&setup.database.reader))
        .await
        .map_err(named)?;
    assert_eq!(view.upkeep.supply.as_ref().unwrap().record, check);
    // Plant a difference in the recorded supply.
    let unit: String = owner
        .query_one("SELECT unit FROM dsa.supply ORDER BY unit LIMIT 1", &[])
        .await?
        .get(0);
    owner
        .execute(
            "UPDATE dsa.supply SET quantity=quantity+1 WHERE unit=$1",
            &[&unit],
        )
        .await?;
    let check = upkeep::check_supply(&mut setup.follower.store, &profile)
        .await
        .map_err(named)?;
    assert!(!check.passed);
    let failure = check.failure.clone().unwrap();
    assert_eq!(failure["error"], "supply_mismatch");
    assert_eq!(failure["unit"], unit.as_str());
    // The gate is closed: every public view raises 55000 for the node.
    let reader = connect(&setup.database.reader_config()).await?;
    for table in dsa_store::NODE_RELATIONS {
        let error = reader
            .query(&format!("SELECT * FROM public.{table} LIMIT 1"), &[])
            .await
            .unwrap_err();
        assert_eq!(
            error.code().map(|code| code.code()),
            Some("55000"),
            "{table}"
        );
    }
    // /status names it.
    let view = refresh
        .service_view(&profile, Some(&setup.database.reader))
        .await
        .map_err(named)?;
    let now = unix_now();
    let follower = FollowerView {
        state: State::Stopped,
        reason: "supply_mismatch".to_owned(),
        ..at_tip(&view, now)
    };
    let (_documents, receiver) = watch::channel(status::render(
        Some(&view),
        Some(&follower),
        &status::Settings::new(&profile.network),
        now,
    ));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (stop, stopped) = oneshot::channel::<()>();
    let server = tokio::spawn(serve(listener, receiver, async {
        let _ = stopped.await;
    }));
    let (code, body) = http_get(address, "/status").await?;
    assert_eq!(code, 200);
    let document: Value = serde_json::from_str(&body)?;
    assert_eq!(document["state"], "stopped");
    assert_eq!(document["state_reason"], "supply_mismatch");
    assert_eq!(document["error_code"], "supply_mismatch");
    assert_eq!(document["serving"], "revoked");
    assert_eq!(document["integrity"]["supply_check"]["passed"], false);
    assert_eq!(
        document["integrity"]["supply_check"]["failure"]["unit"],
        unit.as_str()
    );
    assert_eq!(document["position"]["reason"], "supply_mismatch");
    let (code, body) = http_get(address, "/health").await?;
    assert_eq!(code, 503);
    assert!(body.contains("serving_revoked"));
    let _ = stop.send(());
    server.await?;
    // And from the database alone.
    let stopped_document = status::compose(
        Some(&view),
        None,
        &status::Settings::new(&profile.network),
        now,
    );
    assert_eq!(
        stopped_document.state_reason.as_deref(),
        Some("supply_mismatch")
    );
    drop((refresh, owner, reader));
    setup.cleanup().await
}
