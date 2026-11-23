//! The service against PostgreSQL 17 in DSA_TEST_DATABASE and the fake
//! provider of the testkit: in this process on a clock that runs faster than
//! the wall clock (temporary conditions, conditions that last), and as the
//! `run` command in a child process (first start, restart, exit statuses,
//! memory, stop, the status after a stop). DSA_RUN_CATCH_UP_BLOCKS sets the
//! blocks of the catch-up (default 300) and DSA_RUN_LARGE_BLOCKS the size of
//! the Mainnet-size store of the restart (default 30 filled block ranges of
//! 100 transactions; 2,000 is about 300,000 outputs).

#[path = "follower/support.rs"]
#[allow(dead_code)]
mod chains;
#[path = "common/legacy.rs"]
#[allow(dead_code)]
mod common;
#[path = "follow/harness.rs"]
#[allow(dead_code)]
mod harness;
#[path = "run/support.rs"]
#[allow(dead_code)]
mod support;

use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use chains::{DAY, Start, served};
use common::Database;
use dsa_chain::profiles::Profile;
use dsa_service::{
    config, follow,
    retry::Stop,
    run::{Context, Open, Report, run},
    upkeep::IndexPolicy,
};
use dsa_store::Store;
use dsa_testkit::generate::Generated;
use dsa_testkit::{
    chain::{Block, Chain},
    server::Behavior,
};
use harness::{K, Result, Source, chain, convert, derive, fill, header, named};
use serde_json::{Value, json};
use support::{Child, Fake, Hook, Proxy, Scaled, get, watch_memory};
use tokio::{net::TcpListener, task::JoinHandle};

fn setting(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// 2026-11-19 12:00:00 UTC.
const NOON: u64 = 1_795_046_400 + 43_200;
/// Simulated seconds per real second of the in-process tests.
const RATE: u64 = 50;

/// A converted store on a generated chain (k = 12) behind a proxy, the fake
/// provider and a fast clock.
struct World {
    start: Start,
    clock: Arc<Scaled>,
    fake: Fake,
    proxy: Proxy,
}

impl World {
    async fn new(seed: u64, blocks: u32) -> Result<Self> {
        let start = Start::new(seed, blocks, 20).await?;
        let clock = Scaled::new(NOON, RATE);
        let fake = Fake::start(
            Chain::from_generated(
                &start.g,
                dsa_service::epoch::stake_prefix(&start.profile.network),
            ),
            clock.clone(),
        )
        .await;
        let host = match &start.database.config.get_hosts()[0] {
            tokio_postgres::config::Host::Tcp(host) => host.clone(),
            tokio_postgres::config::Host::Unix(path) => path.display().to_string(),
        };
        let port = start
            .database
            .config
            .get_ports()
            .first()
            .copied()
            .unwrap_or(5432);
        let proxy = Proxy::start(host, port).await;
        Ok(Self {
            start,
            clock,
            fake,
            proxy,
        })
    }

    /// Serves the generated chain through `height`.
    fn serve(&self, height: u32) {
        *self.fake.chain.lock().unwrap().blocks_mut() = served(&self.start.g, height);
    }

    /// Starts the service in this process; its address, its stop and its
    /// task.
    async fn run(
        &self,
        fatal_after: u64,
    ) -> (
        SocketAddr,
        tokio::sync::watch::Sender<bool>,
        JoinHandle<std::result::Result<Report, follow::Error>>,
    ) {
        self.run_with(fatal_after, config::WRITER_BUSY_AFTER_SECONDS)
            .await
    }

    /// [`World::run`] with the time the writer lock is waited for.
    async fn run_with(
        &self,
        fatal_after: u64,
        writer_busy_after: u64,
    ) -> (
        SocketAddr,
        tokio::sync::watch::Sender<bool>,
        JoinHandle<std::result::Result<Report, follow::Error>>,
    ) {
        self.run_changed(|context| {
            context.settings.database_fatal_after = Duration::from_secs(fatal_after);
            context.settings.writer_busy_after = Duration::from_secs(writer_busy_after);
        })
        .await
    }

    /// Starts the service with the world's context after `change`.
    async fn run_changed(
        &self,
        change: impl FnOnce(&mut Context),
    ) -> (
        SocketAddr,
        tokio::sync::watch::Sender<bool>,
        JoinHandle<std::result::Result<Report, follow::Error>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let config = self.proxy.config(&self.start.database.config);
        let open: Open = Arc::new(move |_| {
            let config = config.clone();
            Box::pin(async move { Store::connect(config, None).await })
        });
        let (stopper, stop) = Stop::new();
        let context = Context {
            profile: self.start.profile.clone(),
            provider: self.fake.provider(self.clock.clone()),
            open,
            settings: config::Settings {
                listen: address,
                reader: self.start.database.reader.clone(),
                restore: None,
                retention: None,
                snapshots: None,
                health_max_lag_blocks: 9,
                database_fatal_after: Duration::from_secs(config::MIN_DATABASE_FATAL_AFTER_SECONDS),
                writer_busy_after: Duration::from_secs(config::WRITER_BUSY_AFTER_SECONDS),
            },
            follower: follow::Settings::default(),
            index_policy: IndexPolicy::default(),
            refresh_interval: Duration::from_millis(200),
            upkeep_tick: Duration::from_millis(500),
            listener,
            time: self.clock.clone(),
            stop,
        };
        let mut context = context;
        change(&mut context);
        (address, stopper, tokio::spawn(run(context)))
    }
}

/// The fields of a status document that tell what the service does.
fn summary(document: &Value) -> Value {
    json!({
        "state": document["state"], "reason": document["state_reason"], "wait_until": document["wait_until"],
        "error_code": document["error_code"], "poll_error_code": document["poll_error_code"],
        "target": document["served_target"]["height"], "healthy": document["healthy"],
    })
}

/// Reads `/status` until `done` holds (at most two minutes of real time),
/// keeping a summary of every answer.
async fn status_until(
    address: SocketAddr,
    samples: &mut Vec<Value>,
    what: &str,
    done: impl Fn(&Value) -> bool,
) -> Value {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        if let Some((code, document)) = get(address, "/status").await {
            assert_eq!(code, 200, "{document}");
            samples.push(summary(&document));
            if done(&document) {
                return document;
            }
        }
        assert!(
            Instant::now() < deadline,
            "{what}: never reached; last {:?}",
            samples.last()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn waiting_with_its_end(document: &Value) -> bool {
    document["state"] == "waiting"
        && document["wait_until"].is_u64()
        && document["state_reason"]
            .as_str()
            .is_some_and(|reason| !reason.is_empty())
}

fn target_at(height: u32) -> impl Fn(&Value) -> bool {
    move |document| {
        document["state"] == "following" && document["served_target"]["height"] == json!(height - K)
    }
}

/// While active, every request answers with `behavior` from the first one
/// for `seconds` of the clock; then the chain answers again.
fn fault(behavior: Behavior, seconds: u64) -> Hook {
    let first = AtomicU64::new(0);
    Arc::new(move |_, _, now| {
        let began = match first.compare_exchange(0, now, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => now,
            Err(began) => began,
        };
        (now < began + seconds).then(|| behavior.clone())
    })
}

/// Every page of the headers after a block has its second row linked to
/// another block: an answer the follower refuses.
fn broken_links() -> Hook {
    Arc::new(|path, chain: &mut Chain, _| {
        let route = path.split('?').next()?;
        let hash = route
            .strip_prefix("/api/v0/blocks/")?
            .strip_suffix("/next")?;
        let from = chain.by_hash(hash)?.height;
        let mut rows = (from + 1..=from + 100)
            .filter_map(|height| chain.at(height))
            .map(Block::header)
            .collect::<Vec<_>>();
        if rows.len() < 2 {
            return None;
        }
        rows[1]["previous_block"] = json!("ef".repeat(32));
        Some(Behavior::json(Value::Array(rows)))
    })
}

/// The service starts while PostgreSQL is down and waits; follows
/// once it is up; survives a restart of PostgreSQL while following; waits
/// out a dropped provider connection, 429, 418, 5xx and 402, each for 60
/// simulated seconds, with `/status` naming the state and the time the wait
/// ends; and follows on after each. An answer the follower refuses on every
/// attempt is retried for longer than the fatal time, named in `/status`,
/// `/health` and `dsa.service_state`, and never ends the service.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated PostgreSQL 17"]
async fn temporary_conditions_never_end_the_service() -> Result {
    let world = World::new(51, 160).await?;
    let mut samples = Vec::new();
    let mut height = world.start.target + K;
    world.serve(height);
    // PostgreSQL is down at start.
    world.proxy.refuse();
    let (address, stopper, task) = world.run(600).await;
    let down = status_until(
        address,
        &mut samples,
        "database down at start",
        |document| {
            document["state_reason"] == "database_unreachable" && waiting_with_its_end(document)
        },
    )
    .await;
    let (code, health) = get(address, "/health").await.unwrap();
    assert_eq!(code, 503, "{health}");
    println!(
        "{}",
        json!({"event":"database_down_at_start","status":summary(&down)})
    );
    world.proxy.accept();
    height += 8;
    world.serve(height);
    status_until(
        address,
        &mut samples,
        "following after start",
        target_at(height),
    )
    .await;

    // A restart of PostgreSQL while following: every connection drops and
    // new ones are refused for a while.
    world.proxy.refuse();
    height += 4;
    world.serve(height);
    let restart = status_until(
        address,
        &mut samples,
        "database restart",
        waiting_with_its_end,
    )
    .await;
    println!(
        "{}",
        json!({"event":"database_restart","status":summary(&restart)})
    );
    tokio::time::sleep(world.clock.real(60)).await;
    world.proxy.accept();
    status_until(
        address,
        &mut samples,
        "following after the restart",
        target_at(height),
    )
    .await;

    // Each provider condition for 60 simulated seconds.
    for (name, behavior) in [
        ("dropped", Behavior::dropped()),
        ("429", Behavior::status(429)),
        ("418", Behavior::status(418)),
        ("503", Behavior::status(503)),
        ("402", Behavior::status(402)),
    ] {
        world.fake.set_hook(Some(fault(behavior, 60)));
        height += 3;
        world.serve(height);
        let waiting = status_until(address, &mut samples, name, waiting_with_its_end).await;
        println!(
            "{}",
            json!({"event":"provider_condition","condition":name,"status":summary(&waiting)})
        );
        // A refusal that names a later end than the condition (418: at
        // least an hour, 402: the next UTC day) is waited out: the clock
        // moves there.
        if let Some(until) = waiting["wait_until"].as_u64()
            && until > world.clock.now() + 120
        {
            assert!(
                matches!(name, "418" | "402"),
                "{name}: {}",
                summary(&waiting)
            );
            tokio::time::sleep(world.clock.real(70)).await;
            world.clock.jump_to(until + 1);
        }
        status_until(address, &mut samples, name, target_at(height)).await;
        world.fake.set_hook(None);
        assert!(!task.is_finished(), "{name}: the service ended");
    }

    // An answer refused on every attempt, for longer than the fatal time.
    world.fake.set_hook(Some(broken_links()));
    height += 5;
    world.serve(height);
    let refused = status_until(address, &mut samples, "refused answer", |document| {
        document["error_code"] == "provider_header_link_mismatch" && waiting_with_its_end(document)
    })
    .await;
    let began = world.clock.now();
    tokio::time::sleep(world.clock.real(900)).await;
    assert!(world.clock.now() >= began + 900);
    assert!(!task.is_finished(), "a refused answer ended the service");
    let (code, health) = get(address, "/health").await.unwrap();
    assert_eq!(code, 503);
    assert_eq!(
        health["error_code"], "provider_header_link_mismatch",
        "{health}"
    );
    assert!(
        health["reason_codes"]
            .as_array()
            .unwrap()
            .contains(&json!("state_waiting"))
    );
    let stored: (String, String) = common::connect(&world.start.database.config)
        .await?
        .query_one("SELECT state,reason FROM dsa.service_state", &[])
        .await
        .map(|row| (row.get(0), row.get(1)))?;
    assert_eq!(
        stored,
        (
            "waiting".to_owned(),
            "provider_header_link_mismatch".to_owned()
        )
    );
    println!(
        "{}",
        json!({"event":"refused_answer","status":summary(&refused),"lasted_simulated_seconds":world.clock.now()-began})
    );
    world.fake.set_hook(None);
    status_until(
        address,
        &mut samples,
        "following after the refused answer",
        target_at(height),
    )
    .await;

    // A requested stop ends the service with its report.
    stopper.send(true)?;
    let report = tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("the service ends")?
        .map_err(|error| error.report().to_string())?;
    assert_eq!(report.stopped, "stop_requested");
    assert_eq!(report.status.state, dsa_service::status::State::Stopped);
    let mut store = world.start.store().await?;
    store.verify(&world.start.profile).await.map_err(named)?;
    let states = samples
        .iter()
        .map(|sample| format!("{}/{}", sample["state"], sample["reason"]))
        .collect::<std::collections::BTreeSet<_>>();
    println!(
        "{}",
        json!({"event":"states_seen","states":states,"samples":samples.len()})
    );
    drop(store);
    world.start.cleanup().await
}

/// Serves the generated chain through `height` with recent block times: 20
/// seconds apart, the newest made 20 seconds ago on the world's clock.
fn serve_recent(world: &World, height: u32) {
    let now = world.clock.now();
    let mut blocks = served(&world.start.g, height);
    for block in &mut blocks {
        block.time = now - 20 * u64::from(height - block.height) - 20;
    }
    *world.fake.chain.lock().unwrap().blocks_mut() = blocks;
}

/// A provider whose chain stands still: the polls keep finding nothing new,
/// and once the provider's head block is older than 600 seconds `/health`
/// answers 503 with `provider_head_stale` while `/status` answers 200. A new
/// block makes the service healthy again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated PostgreSQL 17"]
async fn a_provider_whose_chain_stands_still_fails_health() -> Result {
    let world = World::new(63, 80).await?;
    let mut samples = Vec::new();
    let height = world.start.target + K + 2;
    serve_recent(&world, height);
    let (address, stopper, task) = world.run(600).await;
    status_until(address, &mut samples, "healthy at the tip", |document| {
        document["healthy"] == true
    })
    .await;
    let stale = status_until(
        address,
        &mut samples,
        "a head older than 600 s",
        |document| {
            document["reason_codes"]
                .as_array()
                .is_some_and(|codes| codes.contains(&json!("provider_head_stale")))
        },
    )
    .await;
    assert_eq!(stale["state"], "following", "{}", summary(&stale));
    assert_eq!(stale["reason_codes"], json!(["provider_head_stale"]));
    let (code, health) = get(address, "/health").await.unwrap();
    assert_eq!(code, 503, "{health}");
    let (code, _) = get(address, "/status").await.unwrap();
    assert_eq!(code, 200);
    println!(
        "{}",
        json!({"event":"provider_head_stale","provider_head":stale["provider_head"],"generated_at":stale["generated_at"]})
    );
    serve_recent(&world, height + 1);
    status_until(
        address,
        &mut samples,
        "healthy after a new block",
        |document| document["healthy"] == true,
    )
    .await;
    stopper.send(true)?;
    let report = tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("the service ends")?
        .map_err(|error| error.report().to_string())?;
    assert_eq!(report.stopped, "stop_requested");
    world.start.cleanup().await
}

/// A block whose transaction page the provider client refuses on every
/// attempt (not valid JSON) keeps the service waiting with the code in
/// `/status`, past the fatal time; once the page is right it follows on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated PostgreSQL 17"]
async fn an_answer_the_client_refuses_never_ends_the_service() -> Result {
    let world = World::new(57, 80).await?;
    let mut samples = Vec::new();
    let target = world.start.target;
    let busy = (target + 1..=target + 8)
        .filter_map(|height| world.start.g.block(height))
        .find(|block| !block.transactions.is_empty())
        .expect("a block with transactions")
        .clone();
    let height = busy.height + K;
    world.serve(height);
    let page = format!("/blocks/{}/txs/cbor", busy.hash);
    world.fake.set_hook(Some(Arc::new(move |path, _, _| {
        path.contains(&page)
            .then(|| Behavior::raw("not a transaction page".to_owned()))
    })));
    let (address, stopper, task) = world.run(300).await;
    let refused = status_until(address, &mut samples, "refused page", |document| {
        document["error_code"] == "provider_invalid_response" && waiting_with_its_end(document)
    })
    .await;
    let began = world.clock.now();
    tokio::time::sleep(world.clock.real(900)).await;
    assert!(world.clock.now() >= began + 900);
    assert!(!task.is_finished(), "a refused page ended the service");
    let (code, health) = get(address, "/health").await.unwrap();
    assert_eq!(code, 503, "{health}");
    assert_eq!(
        health["error_code"], "provider_invalid_response",
        "{health}"
    );
    println!(
        "{}",
        json!({"event":"client_refused_answer","status":summary(&refused),"lasted_simulated_seconds":world.clock.now()-began})
    );
    world.fake.set_hook(None);
    status_until(
        address,
        &mut samples,
        "following after the page",
        target_at(height),
    )
    .await;
    stopper.send(true)?;
    let report = tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("the service ends")?
        .map_err(|error| error.report().to_string())?;
    assert_eq!(report.stopped, "stop_requested");
    world.start.cleanup().await
}

/// A session that holds the writer lock, as the session of a process that is
/// gone does until the server ends it, keeps the service waiting with
/// `writer_busy`, at start and on a reconnect. Once the server ends that
/// session the service takes the lock and follows, without an exit; a lock
/// held longer than the wait refuses the start. Every session of the store
/// sets the server-side keepalive.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated PostgreSQL 17"]
async fn a_held_writer_lock_is_waited_for_and_taken_once_free() -> Result {
    let world = World::new(59, 80).await?;
    let mut samples = Vec::new();
    let mut height = world.start.target + K;
    world.serve(height);
    let admin = &world.start.database.admin;
    let config = world.start.database.config.clone();
    let hold = || async {
        let mut holder = Store::connect(config.clone(), None).await?;
        holder.lock_writer().await.map_err(named)?;
        let pid: i32 = holder
            .client()
            .query_one("SELECT pg_backend_pid()", &[])
            .await?
            .get(0);
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>((holder, pid))
    };
    let (holder, pid) = hold().await?;
    for (name, value) in [
        ("tcp_keepalives_idle", "30"),
        ("tcp_keepalives_interval", "10"),
        ("tcp_keepalives_count", "3"),
    ] {
        let set: String = holder
            .client()
            .query_one(&format!("SHOW {name}"), &[])
            .await?
            .get(0);
        assert_eq!(set, value, "{name}");
    }
    // At start.
    let (address, stopper, task) = world.run_with(600, 3_600).await;
    status_until(address, &mut samples, "writer busy at start", |document| {
        document["state_reason"] == "writer_busy" && waiting_with_its_end(document)
    })
    .await;
    admin
        .execute("SELECT pg_terminate_backend($1)", &[&pid])
        .await?;
    drop(holder);
    status_until(
        address,
        &mut samples,
        "following once the lock is free",
        target_at(height),
    )
    .await;
    // On a reconnect: the service's sessions end, and another session takes
    // the lock before it is back.
    world.proxy.refuse();
    let (holder, pid) = hold().await?;
    world.proxy.accept();
    height += 3;
    world.serve(height);
    status_until(
        address,
        &mut samples,
        "writer busy on a reconnect",
        |document| document["state_reason"] == "writer_busy" && waiting_with_its_end(document),
    )
    .await;
    admin
        .execute("SELECT pg_terminate_backend($1)", &[&pid])
        .await?;
    drop(holder);
    status_until(
        address,
        &mut samples,
        "following after the reconnect",
        target_at(height),
    )
    .await;
    assert!(!task.is_finished(), "the service ended");
    stopper.send(true)?;
    let report = tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("the service ends")?
        .map_err(|error| error.report().to_string())?;
    assert_eq!(report.stopped, "stop_requested");
    // A lock held for longer than the wait refuses the start.
    let (holder, _) = hold().await?;
    let (_address, _stopper, task) = world.run_with(600, 120).await;
    let error = tokio::time::timeout(Duration::from_secs(60), task)
        .await
        .expect("the service ends")?
        .unwrap_err();
    let report = error.report();
    assert_eq!(report["error"], "writer_busy", "{report}");
    assert!(
        report["detail"]["lasted_seconds"]
            .as_u64()
            .is_some_and(|seconds| seconds > 120),
        "{report}"
    );
    println!("{}", json!({"event":"writer_busy","refused":report}));
    drop(holder);
    world.start.cleanup().await
}

/// Plants a size after the last rebuild of one byte for `index`, so that the
/// index counts as grown past twice that size.
async fn plant_index_record(config: &tokio_postgres::Config, index: &str) -> Result {
    common::connect(config)
        .await?
        .execute(
            "INSERT INTO dsa.upkeep(name,at,detail) VALUES('index:'||$1,clock_timestamp(),jsonb_build_object('bytes',1))
             ON CONFLICT(name) DO UPDATE SET detail=EXCLUDED.detail",
            &[&index],
        )
        .await?;
    Ok(())
}

/// The `dsa.upkeep` rows by name.
async fn upkeep_rows(config: &tokio_postgres::Config) -> Result<BTreeMap<String, Value>> {
    Ok(common::connect(config)
        .await?
        .query("SELECT name,detail FROM dsa.upkeep", &[])
        .await?
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect())
}

/// The snapshot files of `network` in `dir`.
fn daily_files(dir: &std::path::Path, network: &str) -> std::collections::BTreeSet<String> {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|entry| entry.ok()?.file_name().into_string().ok())
        .filter(|name| name.starts_with(&format!("{network}-")) && name.ends_with(".snapshot"))
        .collect()
}

/// The service itself rebuilds an index grown past twice its size after its
/// last rebuild, and writes a snapshot file every day at its time, keeping
/// the newest two: after files of three days the oldest is gone, and
/// `dsa.upkeep` and `/status` name the newest file and the rebuilt index. As
/// the `run` command it logs both operations.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated PostgreSQL 17"]
async fn the_service_rebuilds_an_index_and_keeps_its_daily_files() -> Result {
    let world = World::new(67, 120).await?;
    let mut samples = Vec::new();
    let config = world.start.database.config.clone();
    plant_index_record(&config, "idx_tx_out_tx_id").await?;
    let dir = tempfile::tempdir()?;
    let mut height = world.start.target + K;
    world.serve(height);
    let snapshots = config::Snapshots {
        dir: dir.path().to_owned(),
        keep: 2,
        at: chrono::NaiveTime::from_hms_opt(3, 0, 0).unwrap(),
    };
    // The service's days start after the wall clock's, which the database
    // uses for the time a file was recorded.
    let wall = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let midnight = wall - wall % 86_400 + 2 * 86_400;
    world.clock.jump_to(midnight + 43_200);
    let (address, stopper, task) = world
        .run_changed(|context| {
            context.settings.snapshots = Some(snapshots);
            context.settings.database_fatal_after = Duration::from_secs(600);
            context.index_policy = IndexPolicy {
                growth: 2.0,
                min_bytes: 0,
            };
        })
        .await;
    let network = world.start.profile.network.clone();
    let mut written = Vec::new();
    for day in 0..3_u64 {
        if day > 0 {
            height += 3;
            world.serve(height);
            status_until(address, &mut samples, "following", target_at(height)).await;
            // The next day, a minute after the file's time.
            world
                .clock
                .jump_to(midnight + day * 86_400 + 3 * 3_600 + 60);
        }
        let target = height - K;
        let document = status_until(address, &mut samples, "the day's file", |document| {
            document["integrity"]["snapshot"]["target"] == json!(target)
        })
        .await;
        written.push(
            document["integrity"]["snapshot"]["file"]
                .as_str()
                .unwrap()
                .to_owned(),
        );
    }
    let last = status_until(address, &mut samples, "the rebuilt index", |document| {
        document["database"]["index_rebuilds"]["idx_tx_out_tx_id"]["bytes"]
            .as_i64()
            .is_some_and(|bytes| bytes > 1)
    })
    .await;
    assert!(
        last["warnings"]
            .as_array()
            .unwrap()
            .iter()
            .all(|warning| !warning.as_str().unwrap().starts_with("upkeep_failed")),
        "{}",
        last["warnings"]
    );
    stopper.send(true)?;
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("the service ends")?
        .map_err(|error| error.report().to_string())?;
    assert_eq!(
        daily_files(dir.path(), &network),
        written[1..].iter().cloned().collect(),
        "the newest two of {written:?}"
    );
    let rows = upkeep_rows(&config).await?;
    assert_eq!(rows["snapshot"]["file"], json!(written[2]));
    let rebuilt = &rows["index:idx_tx_out_tx_id"];
    assert!(
        rebuilt.get("before").is_some() && rebuilt.get("baseline").is_none(),
        "{rebuilt}"
    );
    println!(
        "{}",
        json!({"event":"service_upkeep","files":written,"rebuild":rebuilt})
    );
    world.start.cleanup().await?;

    // The command: a bloated index above the floor of 8 MiB and a file at
    // start, each logged at its start and end.
    let p = Preprod::new(89, 1, 0, false).await?;
    let fake = p.fake(p.t0 + p.k()).await;
    let owner = common::connect(&p.converted.config).await?;
    owner
        .batch_execute(
            "INSERT INTO serve.tx_out(id,tx_id,index,address,value)
             SELECT t*4096,t,0,'planted_'||t,1 FROM generate_series(100000000::bigint,100399999) t;
             DELETE FROM serve.tx_out WHERE tx_id>=100000000",
        )
        .await?;
    owner.batch_execute("VACUUM serve.tx_out").await?;
    plant_index_record(&p.converted.config, "idx_tx_out_address").await?;
    let files = tempfile::tempdir()?;
    let extra = [
        "--snapshot-dir",
        &files.path().to_string_lossy(),
        "--snapshot-keep",
        "2",
        "--snapshot-at",
        "00:00",
    ]
    .map(str::to_owned);
    let child = Child::start(
        &run_arguments(&p.converted, "preprod", &fake, &extra),
        &key_environment(&fake),
    );
    let deadline = Instant::now() + Duration::from_secs(120);
    let ended = |events: &[Value], name: &str| {
        events
            .iter()
            .any(|line| line["event"] == format!("{name}_finished"))
    };
    while !(ended(&child.events(), "index_rebuild") && ended(&child.events(), "snapshot_file")) {
        assert!(Instant::now() < deadline, "{:?}", child.events());
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    child.signal("TERM");
    let (code, _, lines) = child.finish().await;
    assert_eq!(code, Some(0), "{lines:?}");
    let events = lines
        .iter()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .collect::<Vec<_>>();
    for name in ["index_rebuild", "snapshot_file"] {
        assert!(
            events
                .iter()
                .any(|line| line["event"] == format!("{name}_started")),
            "{name}: {lines:?}"
        );
        let end = events
            .iter()
            .find(|line| line["event"] == format!("{name}_finished"))
            .unwrap();
        assert_eq!(end["outcome"], "success", "{end}");
    }
    let rebuild = events
        .iter()
        .find(|line| line["event"] == "index_rebuild_finished")
        .unwrap();
    assert_eq!(rebuild["detail"]["name"], "idx_tx_out_address", "{rebuild}");
    let file = events
        .iter()
        .find(|line| line["event"] == "snapshot_file_finished")
        .unwrap()["detail"]["file"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        daily_files(files.path(), "preprod"),
        std::collections::BTreeSet::from([file])
    );
    drop(owner);
    p.cleanup().await
}

/// Review of what ends the service: a database that stays unreachable, and
/// a provider that answers 5xx, end it after the fatal time with the lasting
/// condition, which the command reports with exit status 75.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated PostgreSQL 17"]
async fn lasting_conditions_end_the_service() -> Result {
    let world = World::new(53, 60).await?;
    world.serve(world.start.target + K);
    // The database stays unreachable.
    world.proxy.refuse();
    let (address, _stopper, task) = world.run(300).await;
    let mut samples = Vec::new();
    status_until(address, &mut samples, "database unreachable", |document| {
        document["state_reason"] == "database_unreachable"
    })
    .await;
    let error = tokio::time::timeout(Duration::from_secs(60), task)
        .await
        .expect("the service ends")?
        .unwrap_err();
    let report = error.report();
    assert!(matches!(error, follow::Error::Lasting { .. }), "{report}");
    assert_eq!(report["error"], "database_error", "{report}");
    assert!(report["lasted_seconds"].as_u64().unwrap() > 300, "{report}");
    println!("{}", json!({"event":"lasting_database","report":report}));
    // The provider answers 503 to everything.
    world.proxy.accept();
    world
        .fake
        .set_hook(Some(Arc::new(|_, _, _| Some(Behavior::status(503)))));
    let (_address, _stopper, task) = world.run(300).await;
    let error = tokio::time::timeout(Duration::from_secs(60), task)
        .await
        .expect("the service ends")?
        .unwrap_err();
    let report = error.report();
    assert!(matches!(error, follow::Error::Lasting { .. }), "{report}");
    assert_eq!(report["error"], "provider_unavailable", "{report}");
    println!("{}", json!({"event":"lasting_provider","report":report}));
    let stored: (String, String) = common::connect(&world.start.database.config)
        .await?
        .query_one("SELECT state,reason FROM dsa.service_state", &[])
        .await
        .map(|row| (row.get(0), row.get(1)))?;
    assert_eq!(
        stored,
        ("stopped".to_owned(), "provider_unavailable".to_owned())
    );
    world.start.cleanup().await
}

/// A converted Preprod store (k = 2,160) on a generated chain with `blocks`
/// final blocks after its target, and that store written to a snapshot file.
struct Preprod {
    profile: Profile,
    g: Generated,
    source: Source,
    converted: Database,
    dir: tempfile::TempDir,
    digest: String,
    t0: u32,
}

impl Preprod {
    async fn new(seed: u64, first: u32, blocks: u32, bulky: bool) -> Result<Self> {
        let profile = Profile::load("preprod")?;
        let k = profile.cardano.security_parameter;
        let t0 = first + 19;
        let mut g = chain(
            &profile,
            seed,
            first,
            t0 - first + 1 + blocks + k + 5,
            false,
        )
        .known_from_start();
        g.lengthen_epochs(1_000);
        if bulky {
            // Seven unrelated transactions of about 16.5 KB in each of a
            // window of blocks, so that the CBOR limit ends the batches.
            let full = (blocks / 10).clamp(50, 300);
            for block in g
                .blocks
                .iter_mut()
                .filter(|block| (t0 + 51..=t0 + 50 + full).contains(&block.height))
            {
                let seed = u64::from(block.height) << 8;
                block
                    .transactions
                    .extend((0..7).map(|n| bulky_transaction(seed + n)));
            }
        }
        let history = derive(&profile, &g).await?;
        let mut source = Source::new().await?;
        let headers = (first..=t0 + k)
            .map(|height| header(g.block(height).unwrap()))
            .collect::<Vec<_>>();
        source.start(&g, &history, t0, &headers).await?;
        let converted = Database::new().await?;
        convert(&converted, &source.database, &profile, t0 - 2, DAY).await?;
        let dir = tempfile::tempdir()?;
        let mut store = Store::connect(converted.config.clone(), None).await?;
        let description = store
            .snapshot(&profile, &dir.path().join("preprod.snapshot"))
            .await
            .map_err(named)?;
        Ok(Self {
            profile,
            g,
            source,
            converted,
            dir,
            digest: description.verified.content_digest,
            t0,
        })
    }

    fn file(&self) -> String {
        self.dir
            .path()
            .join("preprod.snapshot")
            .to_string_lossy()
            .into_owned()
    }

    fn k(&self) -> u32 {
        self.profile.cardano.security_parameter
    }

    async fn fake(&self, through: u32) -> Fake {
        let fake = Fake::start(
            Chain::from_generated(&self.g, dsa_service::epoch::stake_prefix("preprod")),
            Arc::new(dsa_provider::SystemClock),
        )
        .await;
        *fake.chain.lock().unwrap().blocks_mut() = served(&self.g, through);
        fake
    }

    async fn cleanup(self) -> Result {
        self.source.database.cleanup().await?;
        self.converted.cleanup().await
    }
}

/// An unrelated transaction of about 16.5 KB: metadata of 250 byte strings
/// of 64 bytes, skipped by the decoder by structure.
fn bulky_transaction(seed: u64) -> dsa_testkit::tx::Built {
    use dsa_testkit::tx::{self, Output, Tx};
    let mut value = vec![0x99, 0x00, 250];
    for index in 0..250_u64 {
        value.extend([0x58, 64]);
        value.extend(
            (0..64_u64).map(|byte| ((seed.wrapping_mul(31) + index * 64 + byte) % 251) as u8),
        );
    }
    Tx::new()
        .input(&tx::hash(&seed.to_be_bytes()), 0)
        .output(Output::new(tx::enterprise_address(0, [9; 28]), 1_000_000))
        .metadata(674, value)
        .fee(500_000)
        .build()
}

/// The arguments of `run` for `database` and the fake provider; the key file
/// comes from `DSA_PROVIDER_KEY_FILE`.
fn run_arguments(database: &Database, network: &str, fake: &Fake, extra: &[String]) -> Vec<String> {
    let mut arguments = [
        "run",
        "--database",
        &database.dsn(false),
        "--network",
        network,
        "--reader",
        &database.reader,
        "--provider-url",
        &fake.server.url,
        "--requests-per-second",
        "10",
        "--max-in-flight",
        "4",
        "--listen",
        "127.0.0.1:0",
    ]
    .map(str::to_owned)
    .to_vec();
    arguments.extend(extra.iter().cloned());
    arguments
}

fn key_environment(fake: &Fake) -> Vec<(&'static str, String)> {
    vec![(
        "DSA_PROVIDER_KEY_FILE",
        fake.key_file().to_string_lossy().into_owned(),
    )]
}

/// Reads `/status` of a child until `done` holds, within `limit`; every body
/// is kept for the check that no secret appears in it.
async fn child_status_until(
    child: &Child,
    address: SocketAddr,
    bodies: &mut Vec<String>,
    states: &mut std::collections::BTreeSet<String>,
    limit: Duration,
    done: impl Fn(&Value) -> bool,
) -> Value {
    let deadline = Instant::now() + limit;
    loop {
        if let Some((_, document)) = get(address, "/status").await {
            states.insert(document["state"].as_str().unwrap_or("").to_owned());
            bodies.push(document.to_string());
            if done(&document) {
                return document;
            }
        }
        assert!(
            Instant::now() < deadline,
            "never reached; last lines {:?}",
            child
                .lines
                .lock()
                .unwrap()
                .iter()
                .rev()
                .take(8)
                .collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// The parts of the status document that come from the database.
fn database_part(document: &Value) -> Value {
    let mut part = serde_json::Map::new();
    for key in [
        "service_version",
        "network",
        "node_release",
        "database_format",
        "keep_rule_version",
        "state",
        "state_reason",
        "error_code",
        "serving",
        "stored_tip",
        "served_target",
        "horizon",
        "last_update",
        "last_error",
        "retention",
        "epochs",
        "integrity",
        "identity",
        "position",
        "service_state",
        "first_observations",
        "capabilities",
        "limitations",
        "unsupported_node_statements",
    ] {
        part.insert(key.to_owned(), document[key].clone());
    }
    for key in [
        "size_bytes",
        "tls_available",
        "reader_check",
        "index_rebuilds",
    ] {
        part.insert(format!("database/{key}"), document["database"][key].clone());
    }
    for key in ["utc_day", "calls_today", "calls_previous_days"] {
        part.insert(format!("provider/{key}"), document["provider"][key].clone());
    }
    Value::Object(part)
}

/// The first start, the stop, the restart, memory and the secrets: the first
/// start with `--snapshot` on an empty database restores (state `restoring`) and
/// catches up; SIGTERM during the catch-up ends the process within 5
/// seconds with exit status 0 and a store that verifies; a restart continues
/// to the tip; resident memory stays under 64 MiB throughout; after the
/// stop, `db-sync-alt status` equals the last document in every
/// database-derived field; no line and no document holds the provider key,
/// the database password or the connection string.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated PostgreSQL 17"]
async fn the_service_restores_catches_up_stops_and_restarts() -> Result {
    let blocks = setting("DSA_RUN_CATCH_UP_BLOCKS", 300) as u32;
    let p = Preprod::new(61, 1, blocks, true).await?;
    let k = p.k();
    let end = p.t0 + blocks;
    let fake = p.fake(end + k).await;
    let database = Database::new().await?;
    let secrets = tempfile::tempdir()?;
    let password = format!("pw-{:x}", std::process::id() as u64 * 0x9e37_79b9);
    let password_file = secrets.path().join("password");
    std::fs::write(&password_file, &password)?;
    let extra = vec![
        "--password-file".to_owned(),
        password_file.to_string_lossy().into_owned(),
        "--snapshot".to_owned(),
        p.file(),
        "--expect-content-digest".to_owned(),
        p.digest.clone(),
    ];
    let arguments = run_arguments(&database, "preprod", &fake, &extra);
    let mut bodies = Vec::new();
    let mut all_lines = Vec::new();
    let mut outputs = Vec::new();

    // First start: restore, then catch up; SIGTERM halfway.
    let began = Instant::now();
    let child = Child::start(&arguments, &key_environment(&fake));
    let peak = watch_memory(child.pid);
    let address = child.address().await;
    let mut states = std::collections::BTreeSet::new();
    let halfway = p.t0 + blocks / 2;
    child_status_until(
        &child,
        address,
        &mut bodies,
        &mut states,
        Duration::from_secs(600),
        |document| {
            document["state"] == "catching_up"
                && document["served_target"]["height"]
                    .as_u64()
                    .is_some_and(|height| height >= u64::from(halfway))
        },
    )
    .await;
    assert!(states.contains("restoring"), "{states:?}");
    let restored_after = began.elapsed().as_secs_f64();
    let signalled = Instant::now();
    child.signal("TERM");
    let (code, stdout, lines) = child.finish().await;
    let stop_seconds = signalled.elapsed().as_secs_f64();
    let first_peak = peak.await?;
    assert_eq!(code, Some(0), "{lines:?}");
    assert!(stop_seconds < 5.0, "stopped after {stop_seconds} s");
    let report: Value = serde_json::from_str(&stdout)?;
    assert_eq!(report["stopped"], "stop_requested");
    assert_eq!(report["restored"]["content_digest"], json!(p.digest));
    let events = lines
        .iter()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .collect::<Vec<_>>();
    for event in [
        "restore_started",
        "restore_finished",
        "supply_check_finished",
        "index_leftovers_finished",
    ] {
        assert!(
            events.iter().any(|line| line["event"] == event),
            "{event}: {lines:?}"
        );
    }
    // At most one update line per 10 seconds.
    let updates = events
        .iter()
        .filter(|line| line["event"] == "update")
        .count();
    assert!(
        updates as f64 <= began.elapsed().as_secs_f64() / 10.0 + 1.0,
        "{updates} update lines"
    );
    all_lines.extend(lines);
    outputs.push(stdout);
    let mut store = Store::connect(database.config.clone(), None).await?;
    let verified = store.verify(&p.profile).await.map_err(named)?;
    let stopped_at = verified.state.target.height;
    assert!(stopped_at >= halfway, "{stopped_at}");

    // Restart: the database is not empty, --snapshot is not used again.
    let began = Instant::now();
    let child = Child::start(&arguments, &key_environment(&fake));
    let peak = watch_memory(child.pid);
    let address = child.address().await;
    let mut states = std::collections::BTreeSet::new();
    child_status_until(
        &child,
        address,
        &mut bodies,
        &mut states,
        Duration::from_secs(60),
        |document| {
            matches!(
                document["state"].as_str(),
                Some("catching_up" | "following")
            )
        },
    )
    .await;
    let restart_seconds = began.elapsed().as_secs_f64();
    let last = child_status_until(
        &child,
        address,
        &mut bodies,
        &mut states,
        Duration::from_secs(3_600),
        |document| {
            document["state"] == "following" && document["served_target"]["height"] == json!(end)
        },
    )
    .await;
    assert!(!states.contains("restoring"), "{states:?}");
    let caught_up_seconds = began.elapsed().as_secs_f64();
    let signalled = Instant::now();
    child.signal("TERM");
    let (code, stdout, lines) = child.finish().await;
    let final_stop_seconds = signalled.elapsed().as_secs_f64();
    let second_peak = peak.await?;
    assert_eq!(code, Some(0), "{lines:?}");
    assert!(
        final_stop_seconds < 5.0,
        "stopped after {final_stop_seconds} s"
    );
    let report: Value = serde_json::from_str(&stdout)?;
    assert!(report["restored"].is_null(), "{report}");
    all_lines.extend(lines);
    outputs.push(stdout);
    let peak = first_peak.max(second_peak);
    assert!(peak < 64 * 1024, "peak resident memory {peak} KiB");
    let verified = store.verify(&p.profile).await.map_err(named)?;
    assert_eq!(verified.state.target.height, end);

    // The status command after the stop equals the last document.
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_db-sync-alt"))
        .args([
            "status",
            "--database",
            &database.dsn(false),
            "--network",
            "preprod",
            "--reader",
            &database.reader,
        ])
        .output()
        .await?;
    assert!(output.status.success());
    let stored: Value = serde_json::from_slice(&output.stdout)?;
    let last_document = &report["status"];
    assert_eq!(
        database_part(&stored),
        database_part(last_document),
        "status command {stored} against the last document {last_document}"
    );
    assert_eq!(stored["state"], "stopped");
    assert_eq!(stored["state_reason"], "stop_requested");
    outputs.push(String::from_utf8_lossy(&output.stdout).into_owned());

    // No secret in any line or document.
    let text = [all_lines.join("\n"), outputs.join("\n"), bodies.join("\n")].join("\n");
    for secret in [
        password.as_str(),
        dsa_testkit::server::KEY,
        database.dsn(false).as_str(),
    ] {
        assert!(!text.contains(secret), "a secret appears in the output");
    }
    println!(
        "{}",
        json!({"event":"run_catch_up","blocks":blocks,"first_start_to_halfway_seconds":restored_after,
            "sigterm_during_catch_up_seconds":stop_seconds,"stopped_at_target":stopped_at,
            "restart_to_catching_up_seconds":restart_seconds,"restart_to_tip_seconds":caught_up_seconds,
            "sigterm_at_tip_seconds":final_stop_seconds,"peak_resident_kib":peak,
            "peak_first_run_kib":first_peak,"peak_second_run_kib":second_peak,
            "requests":fake.requests(),"status_reads":bodies.len(),"last":summary(&last)})
    );
    drop(store);
    database.cleanup().await?;
    p.cleanup().await
}

/// A serving store of Mainnet size follows again within
/// 60 seconds of the process start, also the second time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated PostgreSQL 17"]
async fn a_restart_of_a_mainnet_size_store_follows_within_60_seconds() -> Result {
    let ranges = setting("DSA_RUN_LARGE_BLOCKS", 30) as i32;
    let p = Preprod::new(71, 1_000_000, 0, false).await?;
    let k = p.k();
    let database = &p.converted;
    // Old unspent outputs below the horizon, with their transactions.
    fill(&database.config, &p.profile, 1_000, ranges, p.t0).await?;
    let mut store = Store::connect(database.config.clone(), None).await?;
    let check = store.check_supply(&p.profile).await.map_err(named)?;
    if let Some(failure) = &check.failure {
        common::connect(&database.config)
            .await?
            .execute(
                "UPDATE dsa.supply SET quantity=$2::text::numeric WHERE unit=$1",
                &[
                    &failure["unit"].as_str().unwrap_or_default(),
                    &failure["unspent"].as_str().unwrap_or_default(),
                ],
            )
            .await?;
    }
    let check = store.check_supply(&p.profile).await.map_err(named)?;
    assert!(check.passed, "{check:?}");
    let size: Value = common::connect(&database.config)
        .await?
        .query_one(
            "SELECT jsonb_build_object('tx',(SELECT count(*) FROM serve.tx),'tx_out',(SELECT count(*) FROM serve.tx_out),
              'ma_tx_out',(SELECT count(*) FROM serve.ma_tx_out),'tx_in',(SELECT count(*) FROM serve.tx_in),
              'block',(SELECT count(*) FROM serve.block),'database_bytes',pg_database_size(current_database()))",
            &[],
        )
        .await?
        .get(0);
    drop(store);
    let fake = p.fake(p.t0 + k).await;
    let arguments = run_arguments(database, "preprod", &fake, &[]);
    let mut starts = Vec::new();
    for _ in 0..2 {
        let began = Instant::now();
        let child = Child::start(&arguments, &key_environment(&fake));
        let address = child.address().await;
        let mut bodies = Vec::new();
        let mut states = std::collections::BTreeSet::new();
        child_status_until(
            &child,
            address,
            &mut bodies,
            &mut states,
            Duration::from_secs(120),
            |document| document["state"] == "following",
        )
        .await;
        let to_following = began.elapsed().as_secs_f64();
        // The supply check of the start, on the whole store.
        let deadline = Instant::now() + Duration::from_secs(120);
        while !child
            .events()
            .iter()
            .any(|line| line["event"] == "supply_check_finished")
        {
            assert!(Instant::now() < deadline, "no supply check");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let signalled = Instant::now();
        child.signal("TERM");
        let (code, _, lines) = child.finish().await;
        let stop_seconds = signalled.elapsed().as_secs_f64();
        assert_eq!(code, Some(0), "{lines:?}");
        let supply = lines
            .iter()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .find(|line| line["event"] == "supply_check_finished")
            .map(|line| line["seconds"].clone());
        starts.push(json!({"to_following_seconds":to_following,"sigterm_seconds":stop_seconds,"supply_check_seconds":supply}));
        assert!(to_following < 60.0, "following after {to_following} s");
        assert!(stop_seconds < 5.0, "stopped after {stop_seconds} s");
    }
    println!(
        "{}",
        json!({"event":"run_restart","ranges":ranges,"store":size,"starts":starts})
    );
    p.cleanup().await
}

/// The `--snapshot` file is read only when a restore is needed: a start on a
/// serving database whose file the daily rotation removed follows without a
/// word about it; on an empty database the missing file refuses the start
/// with exit status 1 and an error that names the flag.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated PostgreSQL 17"]
async fn a_missing_snapshot_file_matters_only_for_a_restore() -> Result {
    let p = Preprod::new(87, 1, 0, false).await?;
    let k = p.k();
    let fake = p.fake(p.t0 + k).await;
    let missing = p.dir.path().join("rotated-away.snapshot");
    let extra = vec![
        "--snapshot".to_owned(),
        missing.to_string_lossy().into_owned(),
        "--expect-content-digest".to_owned(),
        p.digest.clone(),
    ];
    let child = Child::start(
        &run_arguments(&p.converted, "preprod", &fake, &extra),
        &key_environment(&fake),
    );
    let address = child.address().await;
    let (mut bodies, mut states) = (Vec::new(), std::collections::BTreeSet::new());
    child_status_until(
        &child,
        address,
        &mut bodies,
        &mut states,
        Duration::from_secs(120),
        |document| document["state"] == "following",
    )
    .await;
    child.signal("TERM");
    let (code, _, lines) = child.finish().await;
    assert_eq!(code, Some(0), "{lines:?}");
    assert!(
        !lines.iter().any(|line| line.contains("snapshot_file")),
        "{lines:?}"
    );
    assert!(
        bodies.iter().all(|body| !body.contains("snapshot_file")),
        "a word about the file in /status"
    );
    let empty = Database::new().await?;
    let child = Child::start(
        &run_arguments(&empty, "preprod", &fake, &extra),
        &key_environment(&fake),
    );
    let (code, _, lines) = tokio::time::timeout(Duration::from_secs(60), child.finish())
        .await
        .expect("the service ends");
    assert_eq!(code, Some(1), "{lines:?}");
    assert!(
        lines.iter().any(|line| {
            line.contains("\"error\":\"snapshot_file_unreadable\"")
                && line.contains("\"flag\":\"--snapshot\"")
        }),
        "{lines:?}"
    );
    empty.cleanup().await?;
    p.cleanup().await
}

/// Runs a one-shot command of the binary on `database` with no `DSA_`
/// variable; its output.
async fn command(database: &Database, arguments: &[&str]) -> Result<std::process::Output> {
    let mut process = tokio::process::Command::new(env!("CARGO_BIN_EXE_db-sync-alt"));
    process
        .args(arguments)
        .args(["--database", &database.dsn(false), "--network", "preprod"]);
    for (name, _) in std::env::vars().filter(|(name, _)| name.starts_with("DSA_")) {
        process.env_remove(name);
    }
    Ok(process.output().await?)
}

/// A first restore stopped after its build committed and before it
/// published leaves a closed database with the build beside it; the next
/// `run --snapshot` restores it and follows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated PostgreSQL 17"]
async fn run_completes_a_restore_interrupted_between_its_commits() -> Result {
    let p = Preprod::new(83, 1, 0, false).await?;
    let k = p.k();
    let fake = p.fake(p.t0 + k).await;
    let database = Database::new().await?;
    for arguments in [
        &["create"][..],
        &["grant-reader", "--role", &database.reader],
    ] {
        let output = command(&database, arguments).await?;
        assert!(output.status.success(), "{arguments:?}: {output:?}");
    }
    // A reader holds a view, so the restore's publication waits; it is
    // stopped once its build has committed.
    let mut reader_config = database.config.clone();
    reader_config.user(&database.reader);
    let holder = common::connect(&reader_config).await?;
    holder
        .batch_execute("BEGIN;DECLARE held CURSOR FOR SELECT * FROM public.block")
        .await?;
    let mut restore = tokio::process::Command::new(env!("CARGO_BIN_EXE_db-sync-alt"));
    restore.args([
        "restore",
        "--database",
        &database.dsn(false),
        "--network",
        "preprod",
        "--file",
        &p.file(),
    ]);
    for (name, _) in std::env::vars().filter(|(name, _)| name.starts_with("DSA_")) {
        restore.env_remove(name);
    }
    let restore = restore
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;
    let pid = restore.id().ok_or("restore ended")?.to_string();
    let owner = common::connect(&database.config).await?;
    let built = "SELECT to_regnamespace('dsa_retired') IS NOT NULL";
    let deadline = Instant::now() + Duration::from_secs(120);
    while !owner.query_one(built, &[]).await?.get::<_, bool>(0) {
        assert!(Instant::now() < deadline, "the build never committed");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        std::process::Command::new("kill")
            .args(["-TERM", &pid])
            .status()?
            .success()
    );
    let output = restore.wait_with_output().await?;
    assert!(!output.status.success(), "{output:?}");
    holder.batch_execute("COMMIT").await?;
    drop(holder);
    assert!(owner.query_one(built, &[]).await?.get::<_, bool>(0));
    // The next start with --snapshot restores again and follows.
    let extra = vec![
        "--snapshot".to_owned(),
        p.file(),
        "--expect-content-digest".to_owned(),
        p.digest.clone(),
    ];
    let child = Child::start(
        &run_arguments(&database, "preprod", &fake, &extra),
        &key_environment(&fake),
    );
    let address = child.address().await;
    let (mut bodies, mut states) = (Vec::new(), std::collections::BTreeSet::new());
    child_status_until(
        &child,
        address,
        &mut bodies,
        &mut states,
        Duration::from_secs(120),
        |document| {
            document["state"] == "following" && document["served_target"]["height"] == json!(p.t0)
        },
    )
    .await;
    child.signal("TERM");
    let (code, stdout, lines) = child.finish().await;
    assert_eq!(code, Some(0), "{lines:?}");
    let report: Value = serde_json::from_str(&stdout)?;
    assert_eq!(
        report["restored"]["content_digest"],
        json!(p.digest),
        "{report}"
    );
    assert!(!owner.query_one(built, &[]).await?.get::<_, bool>(0));
    let mut store = Store::connect(database.config.clone(), None).await?;
    let verified = store.verify(&p.profile).await.map_err(named)?;
    assert_eq!(verified.content_digest, p.digest);
    drop((store, owner));
    database.cleanup().await?;
    p.cleanup().await
}

/// Waits (at most a minute) until a `db-sync-alt` session of `database`
/// waits for a lock.
async fn writer_waits_for_a_lock(database: &Database) -> Result {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let waiting: bool = database
            .admin
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=$1 AND application_name='db-sync-alt' AND wait_event_type='Lock')",
                &[&database.name],
            )
            .await?
            .get(0);
        if waiting {
            return Ok(());
        }
        assert!(Instant::now() < deadline, "no session waited for the lock");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A stop that arrives while an update waits for a lock that another session
/// holds outlasts the grace of 10 seconds: the statements are cancelled and
/// `run` ends with exit status 0 and a report that says the stop was forced,
/// never with an unknown outcome. The update is absent, and a restart
/// follows on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated PostgreSQL 17"]
async fn a_stop_during_a_long_statement_is_forced_after_the_grace() -> Result {
    let p = Preprod::new(79, 1, 0, false).await?;
    let k = p.k();
    let fake = p.fake(p.t0 + k).await;
    let database = &p.converted;
    let extra = ["--poll-interval-seconds", "1"].map(str::to_owned);
    let arguments = run_arguments(database, "preprod", &fake, &extra);
    let (mut bodies, mut states) = (Vec::new(), std::collections::BTreeSet::new());
    let child = Child::start(&arguments, &key_environment(&fake));
    let address = child.address().await;
    child_status_until(
        &child,
        address,
        &mut bodies,
        &mut states,
        Duration::from_secs(120),
        |document| document["state"] == "following",
    )
    .await;
    // Another session holds the stored position: the next update waits.
    let holder = common::connect(&database.config).await?;
    holder
        .batch_execute("BEGIN;SELECT 1 FROM dsa.position FOR UPDATE")
        .await?;
    *fake.chain.lock().unwrap().blocks_mut() = served(&p.g, p.t0 + k + 5);
    writer_waits_for_a_lock(database).await?;
    let signalled = Instant::now();
    child.signal("TERM");
    let (code, stdout, lines) = child.finish().await;
    let stop_seconds = signalled.elapsed().as_secs_f64();
    holder.batch_execute("ROLLBACK").await?;
    assert_eq!(code, Some(0), "{lines:?}");
    assert!(
        (10.0..20.0).contains(&stop_seconds),
        "stopped after {stop_seconds} s"
    );
    let report: Value = serde_json::from_str(&stdout)?;
    assert_eq!(report["stopped"], "stop_forced_after_grace", "{report}");
    assert_eq!(report["partial_update"], false, "{report}");
    assert!(
        !lines
            .iter()
            .any(|line| line.contains("\"outcome\":\"unknown\"")),
        "{lines:?}"
    );
    let mut store = Store::connect(database.config.clone(), None).await?;
    let verified = store.verify(&p.profile).await.map_err(named)?;
    assert_eq!(verified.state.target.height, p.t0, "the update is absent");
    drop(store);
    let child = Child::start(&arguments, &key_environment(&fake));
    let address = child.address().await;
    child_status_until(
        &child,
        address,
        &mut bodies,
        &mut states,
        Duration::from_secs(120),
        |document| {
            document["state"] == "following"
                && document["served_target"]["height"] == json!(p.t0 + 5)
        },
    )
    .await;
    child.signal("TERM");
    let (code, stdout, lines) = child.finish().await;
    assert_eq!(code, Some(0), "{lines:?}");
    let report: Value = serde_json::from_str(&stdout)?;
    assert_eq!(report["stopped"], "stop_requested", "{report}");
    println!(
        "{}",
        json!({"event":"forced_stop","stop_seconds":stop_seconds,"report_stopped":"stop_forced_after_grace"})
    );
    p.cleanup().await
}

/// Each condition that retrying cannot cure ends `run` with exit
/// status 1 and a named error; an invalid setting with 2; a database that
/// stays unreachable and a provider that answers 5xx, each longer than
/// `--database-fatal-after-seconds`, with 75.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated PostgreSQL 17"]
async fn fatal_conditions_end_the_service_with_their_status() -> Result {
    let p = Preprod::new(73, 1, 0, false).await?;
    let k = p.k();
    let fake = p.fake(p.t0 + k).await;
    let database = &p.converted;
    let ended = |code: Option<i32>, lines: &[String], status: i32, error: &str| {
        assert_eq!(code, Some(status), "{error}: {lines:?}");
        let named = lines
            .iter()
            .rev()
            .find(|line| line.contains(&format!("\"error\":\"{error}\"")))
            .unwrap_or_else(|| panic!("{error}: {lines:?}"));
        println!(
            "{}",
            json!({"event":"fatal_case","exit":status,"error":error,"line":named})
        );
    };
    let quick = |arguments: Vec<String>, environment: Vec<(&'static str, String)>| async move {
        let child = Child::start(&arguments, &environment);
        let (code, _, lines) = tokio::time::timeout(Duration::from_secs(60), child.finish())
            .await
            .expect("the service ends");
        (code, lines)
    };

    // Conditions that last: started first, they end after 60 seconds.
    let closed = TcpListener::bind("127.0.0.1:0").await?;
    let unused = closed.local_addr()?.port();
    drop(closed);
    let mut unreachable = run_arguments(database, "preprod", &fake, &[]);
    unreachable[2] =
        format!("host=127.0.0.1 port={unused} user=nobody dbname=nowhere sslmode=disable");
    unreachable.extend(["--database-fatal-after-seconds".to_owned(), "60".to_owned()]);
    let down = Child::start(&unreachable, &key_environment(&fake));
    let down_address = down.address().await;
    let failing = p.fake(p.t0 + k + 5).await;
    failing.set_hook(Some(Arc::new(|_, _, _| Some(Behavior::status(503)))));
    // On a database of its own, restored at its first start: it holds the
    // writer lock while it waits.
    let other = Database::new().await?;
    let mut failing_arguments = run_arguments(&other, "preprod", &failing, &[]);
    failing_arguments.extend(
        [
            "--database-fatal-after-seconds",
            "60",
            "--snapshot",
            &p.file(),
        ]
        .map(str::to_owned),
    );
    let lasting_started = Instant::now();
    let unavailable = Child::start(&failing_arguments, &key_environment(&failing));
    // A second writer: another session holds the writer lock of a database
    // of its own for longer than the 120 seconds the service waits for it.
    let busy = Database::new().await?;
    let mut holder = Store::connect(busy.config.clone(), None).await?;
    holder.lock_writer().await.map_err(named)?;
    let second = Child::start(
        &run_arguments(&busy, "preprod", &fake, &[]),
        &key_environment(&fake),
    );
    let second_address = second.address().await;
    let (mut second_bodies, mut second_states) = (Vec::new(), std::collections::BTreeSet::new());
    child_status_until(
        &second,
        second_address,
        &mut second_bodies,
        &mut second_states,
        Duration::from_secs(30),
        |document| document["state"] == "waiting" && document["state_reason"] == "writer_busy",
    )
    .await;

    let mut bodies = Vec::new();
    let mut states = std::collections::BTreeSet::new();
    let waiting = child_status_until(
        &down,
        down_address,
        &mut bodies,
        &mut states,
        Duration::from_secs(30),
        |document| {
            document["state_reason"] == "database_unreachable" && document["wait_until"].is_u64()
        },
    )
    .await;
    assert_eq!(waiting["state"], "waiting");

    // An invalid setting, and an empty variable, before any connection.
    let (code, lines) = quick(
        run_arguments(
            database,
            "preprod",
            &fake,
            &["--snapshot-keep".to_owned(), "0".to_owned()],
        ),
        key_environment(&fake),
    )
    .await;
    assert_eq!(code, Some(2), "{lines:?}");
    assert!(
        lines
            .iter()
            .any(|line| line.contains("\"flag\":\"--snapshot-keep\"")),
        "{lines:?}"
    );
    let mut environment = key_environment(&fake);
    environment.push(("DSA_MAX_IN_FLIGHT", String::new()));
    let mut without = run_arguments(database, "preprod", &fake, &[]);
    let at = without
        .iter()
        .position(|item| item == "--max-in-flight")
        .unwrap();
    without.drain(at..at + 2);
    let (code, lines) = quick(without, environment).await;
    assert_eq!(code, Some(2), "{lines:?}");
    assert!(
        lines.iter().any(|line| line.contains("--max-in-flight")),
        "{lines:?}"
    );

    // The provider rejects the key.
    let rejecting = p.fake(p.t0 + k).await;
    rejecting.set_hook(Some(Arc::new(|_, _, _| Some(Behavior::status(403)))));
    let (code, lines) = quick(
        run_arguments(database, "preprod", &rejecting, &[]),
        key_environment(&rejecting),
    )
    .await;
    ended(code, &lines, 1, "provider_key_rejected");

    // A retention other than the stored one, then changed on purpose.
    let retention = vec!["--retention-seconds".to_owned(), "172800".to_owned()];
    let (code, lines) = quick(
        run_arguments(database, "preprod", &fake, &retention),
        key_environment(&fake),
    )
    .await;
    ended(code, &lines, 1, "retention_mismatch");
    let stored: (String, String) = common::connect(&database.config)
        .await?
        .query_one("SELECT state,reason FROM dsa.service_state", &[])
        .await
        .map(|row| (row.get(0), row.get(1)))?;
    assert_eq!(
        stored,
        ("stopped".to_owned(), "retention_mismatch".to_owned())
    );
    let mut changed = retention.clone();
    changed.push("--change-retention".to_owned());
    let child = Child::start(
        &run_arguments(database, "preprod", &fake, &changed),
        &key_environment(&fake),
    );
    let address = child.address().await;
    child_status_until(
        &child,
        address,
        &mut bodies,
        &mut states,
        Duration::from_secs(60),
        |document| document["state"] == "following" && document["retention"]["seconds"] == 172_800,
    )
    .await;
    child.signal("INT");
    let (code, _, lines) = child.finish().await;
    assert_eq!(code, Some(0), "{lines:?}");
    assert!(
        lines
            .iter()
            .any(|line| line.contains("\"event\":\"retention_changed\""))
    );

    // The database of another network.
    let (code, lines) = quick(
        run_arguments(database, "mainnet", &fake, &[]),
        key_environment(&fake),
    )
    .await;
    ended(code, &lines, 1, "network_mismatch");

    // A reader that can read more than the node's relations.
    let mut root: tokio_postgres::Config = std::env::var("DSA_TEST_DATABASE")?.parse()?;
    root.dbname(&database.name);
    let admin = common::connect(&root).await?;
    admin
        .batch_execute(&format!(
            "GRANT SELECT ON dsa.position TO {}",
            database.reader
        ))
        .await?;
    let (code, lines) = quick(
        run_arguments(database, "preprod", &fake, &[]),
        key_environment(&fake),
    )
    .await;
    ended(code, &lines, 1, "reader_privileges_not_exact");
    admin
        .batch_execute(&format!(
            "REVOKE SELECT ON dsa.position FROM {}",
            database.reader
        ))
        .await?;

    // An empty database without --snapshot.
    let empty = Database::new().await?;
    let (code, lines) = quick(
        run_arguments(&empty, "preprod", &fake, &[]),
        key_environment(&fake),
    )
    .await;
    ended(code, &lines, 1, "snapshot_required");
    empty.cleanup().await?;

    // A closed gate found at start.
    let mut store = Store::connect(database.config.clone(), None).await?;
    store
        .close_gate("canonical_conflict", &json!({"planted": true}))
        .await
        .map_err(named)?;
    drop(store);
    let (code, lines) = quick(
        run_arguments(database, "preprod", &fake, &[]),
        key_environment(&fake),
    )
    .await;
    ended(code, &lines, 1, "database_not_serving");

    // The second writer waited with `writer_busy`, then was refused.
    let (code, _, lines) = tokio::time::timeout(Duration::from_secs(240), second.finish())
        .await
        .expect("the second writer ends");
    ended(code, &lines, 1, "writer_busy");
    let waited = lasting_started.elapsed().as_secs_f64();
    assert!(waited >= 120.0, "{waited}");
    drop(holder);
    busy.cleanup().await?;

    // The two conditions that lasted.
    let (code, _, lines) = tokio::time::timeout(Duration::from_secs(120), down.finish())
        .await
        .expect("the service ends");
    ended(code, &lines, 75, "database_error");
    let (code, _, lines) = tokio::time::timeout(Duration::from_secs(120), unavailable.finish())
        .await
        .expect("the service ends");
    ended(code, &lines, 75, "provider_unavailable");
    let lasted = lasting_started.elapsed().as_secs_f64();
    assert!(lasted >= 60.0, "{lasted}");
    println!(
        "{}",
        json!({"event":"fatal_conditions","lasting_seconds":lasted})
    );
    other.cleanup().await?;
    p.cleanup().await
}
