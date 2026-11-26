//! The service: one long-running process that serves `/health` and
//! `/status`, prepares the database (writer lock, a first restore, the
//! retention, the node's reader), follows the provider's chain into it,
//! reads the database part of the status every 20 seconds on a connection
//! of its own and runs the upkeep on its schedule.
//!
//! The HTTP listener answers from the first moment, also while PostgreSQL
//! cannot be reached. Every temporary condition is retried: a database or
//! provider condition ends the service only after it has lasted the stated
//! time, an answer the follower refuses never ends it, and a refusal with
//! its own resume time is waited out. Upkeep never holds the writer's
//! transaction: the supply check runs on the writer's connection between
//! two updates, an index rebuild and the daily snapshot file on connections
//! of their own; a failing job is logged, shown in the status and tried at
//! its next slot.

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, PoisonError},
    time::{Duration, Instant},
};

use dsa_chain::profiles::Profile;
use dsa_provider::Provider;
use dsa_store::{Cancellation, Store, service_view::DatabaseView};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::watch, task::JoinHandle};

use crate::{
    config,
    follow::{self, Between, Error, Progress},
    http,
    retry::{Backoff, Stop, Time, pause_until},
    status::{self, Documents, FollowerView, State, Status},
    upkeep::{self, IndexPolicy, Job, Schedule},
};

/// How often the database part of the status is read.
pub const REFRESH_INTERVAL: Duration = Duration::from_secs(20);
/// How often the upkeep schedule is looked at.
pub const UPKEEP_TICK: Duration = Duration::from_secs(60);
/// While nothing changes, the documents are composed again this often, so
/// that their ages stay current.
const RENDER_INTERVAL: Duration = Duration::from_secs(5);
/// The longest one read of the status may take.
const REFRESH_DEADLINE: Duration = Duration::from_secs(15);
/// The longest the last read of the status may take after a stop.
const FINAL_READ_DEADLINE: Duration = Duration::from_millis(1_500);

/// What a connection is for. The caller registers each connection for
/// cancellation under its role, so that a new connection of a role replaces
/// the earlier one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// The writer: the start, the follower and its reconnects.
    Writer,
    /// The read of the status's database part.
    Status,
    /// One upkeep job.
    Upkeep,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Writer => "writer",
            Self::Status => "status",
            Self::Upkeep => "upkeep",
        }
    }
}

/// Opens one new connection for a role, registered for cancellation by the
/// caller.
pub type Open = Arc<
    dyn Fn(Role) -> Pin<Box<dyn Future<Output = Result<Store, dsa_store::Error>> + Send>>
        + Send
        + Sync,
>;

/// The service's outside world and settings.
pub struct Context {
    pub profile: Profile,
    pub provider: Provider,
    pub open: Open,
    pub settings: config::Settings,
    pub follower: follow::Settings,
    pub index_policy: IndexPolicy,
    pub refresh_interval: Duration,
    pub upkeep_tick: Duration,
    /// Bound by the caller to `settings.listen`.
    pub listener: TcpListener,
    pub time: Arc<dyn Time>,
    pub stop: Stop,
}

/// How a service that was asked to stop ended.
#[derive(Debug, Serialize)]
pub struct Report {
    pub stopped: &'static str,
    /// The content digest and target of the snapshot file restored at start.
    pub restored: Option<Value>,
    /// The follower's report, when it ran.
    pub follower: Option<follow::Report>,
    /// The last status document.
    pub status: Status,
}

/// What the parts of the service share to compose the documents.
struct Shared {
    /// What the service says before the follower runs; `None` while the
    /// follower's progress speaks.
    phase: watch::Sender<Option<FollowerView>>,
    progress: watch::Sender<Progress>,
    database: watch::Sender<Option<Arc<DatabaseView>>>,
    /// Failures of the status read and of the upkeep jobs, as warnings.
    notes: Mutex<Notes>,
    /// Requests for a status read now, numbered; and the last request a
    /// finished read served.
    refresh_wanted: watch::Sender<u64>,
    refresh_done: watch::Sender<u64>,
    documents: watch::Sender<Documents>,
    settings: status::Settings,
}

#[derive(Default)]
struct Notes {
    refresh: Option<String>,
    upkeep: std::collections::BTreeMap<&'static str, String>,
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Shared {
    fn compose(&self, now: u64) -> Status {
        let database = self.database.borrow().clone();
        let view = self
            .phase
            .borrow()
            .clone()
            .unwrap_or_else(|| FollowerView::from(&*self.progress.borrow()));
        let mut status = status::compose(database.as_deref(), Some(&view), &self.settings, now);
        let notes = lock(&self.notes);
        if let Some(code) = &notes.refresh {
            status.warnings.push(format!("status_read_failed:{code}"));
        }
        for (job, code) in &notes.upkeep {
            status.warnings.push(format!("upkeep_failed:{job}:{code}"));
        }
        status
    }

    fn render(&self, now: u64) -> Status {
        let status = self.compose(now);
        self.documents.send_replace(Documents::new(&status));
        status
    }

    fn note_upkeep(&self, job: &'static str, failure: Option<String>) {
        let mut notes = lock(&self.notes);
        match failure {
            Some(code) => notes.upkeep.insert(job, code),
            None => notes.upkeep.remove(job),
        };
    }
}

/// A view of a service that is not following yet.
fn phase(provider: &Provider, state: State, reason: &str, wait_until: Option<u64>) -> FollowerView {
    let status = provider.status();
    FollowerView {
        state,
        reason: reason.to_owned(),
        wait_until,
        provider_tip: None,
        requests_per_second: status.requests_per_second,
        requests_in_flight: status.requests_in_flight,
        last_refusal: status.last_refusal,
        blocks_remaining: None,
        blocks_per_hour: None,
        stake_failure: None,
        stake_missing: None,
    }
}

fn code(report: &Value) -> String {
    report["error"]
        .as_str()
        .unwrap_or("service_error")
        .to_owned()
}

/// Logs the start of a job and returns its start.
fn started(event: &str, detail: Value) -> Instant {
    let mut line = json!({"event": format!("{event}_started")});
    if let (Some(line), Some(detail)) = (line.as_object_mut(), detail.as_object()) {
        line.extend(detail.clone());
    }
    eprintln!("{line}");
    Instant::now()
}

/// Logs the end of a job with its outcome and duration.
fn finished(event: &str, began: Instant, outcome: Result<Value, Value>) {
    let (name, detail) = match outcome {
        Ok(detail) => ("success", detail),
        Err(detail) => ("failed", detail),
    };
    eprintln!(
        "{}",
        json!({"event": format!("{event}_finished"), "outcome": name,
            "seconds": began.elapsed().as_secs_f64(), "detail": detail})
    );
}

/// The supply check on the writer's connection between two updates: at
/// start and every six hours. A difference closes the gate; a database error
/// is logged, shown and tried at the next slot.
struct SupplyCheck {
    profile: Profile,
    schedule: Arc<Mutex<Schedule>>,
    shared: Arc<Shared>,
}

impl Between for SupplyCheck {
    fn run<'s>(
        &'s mut self,
        store: &'s mut Store,
        now: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 's>> {
        Box::pin(async move {
            {
                let mut schedule = lock(&self.schedule);
                if !schedule.due(None, None, now).contains(&Job::Supply) {
                    return Ok(());
                }
                schedule.supply_checked = Some(now);
            }
            let began = started("supply_check", json!({}));
            match store.check_supply(&self.profile).await {
                Ok(check) if check.passed => {
                    finished(
                        "supply_check",
                        began,
                        Ok(json!({"unspent_outputs": check.unspent_outputs})),
                    );
                    self.shared.note_upkeep("supply_check", None);
                    Ok(())
                }
                Ok(check) => {
                    let detail =
                        json!({"failure": check.failure, "unspent_outputs": check.unspent_outputs});
                    finished("supply_check", began, Err(detail.clone()));
                    Err(Error::Check {
                        code: upkeep::SUPPLY_MISMATCH,
                        detail,
                    })
                }
                Err(error) => {
                    let report = error.report();
                    finished("supply_check", began, Err(report.clone()));
                    self.shared.note_upkeep("supply_check", Some(code(&report)));
                    Err(Error::Store(error))
                }
            }
        })
    }
}

/// The follower's connections: first the prepared writer's, then new ones.
fn handing(store: Store, open: Open) -> follow::Connect {
    let first = Mutex::new(Some(store));
    Box::new(move || {
        let first = lock(&first).take();
        let open = Arc::clone(&open);
        Box::pin(async move {
            match first {
                Some(store) => Ok(store),
                None => open(Role::Writer).await,
            }
        })
    })
}

/// What the start leaves: the writer's connection with the lock held, and
/// what a restore at start produced.
struct Prepared {
    store: Store,
    restored: Option<Value>,
}

struct Service {
    profile: Profile,
    provider: Provider,
    open: Open,
    settings: config::Settings,
    time: Arc<dyn Time>,
    stop: Stop,
    shared: Arc<Shared>,
}

impl Service {
    fn now(&self) -> u64 {
        self.time.unix_seconds()
    }

    fn show(&self, state: State, reason: &str, wait_until: Option<u64>) {
        self.shared
            .phase
            .send_replace(Some(phase(&self.provider, state, reason, wait_until)));
    }

    /// Prepares the database, retrying while it cannot be reached and while
    /// another session holds the writer lock; a condition that lasts longer
    /// than its stated time ends the service.
    async fn start(&self, backoff: &mut Backoff) -> Result<Prepared, Error> {
        let mut busy = Backoff::default();
        loop {
            match self.prepare().await {
                Ok(prepared) => {
                    if backoff.failures() > 0 {
                        eprintln!(
                            "{}",
                            json!({"event":"database_reachable","failures":backoff.failures(),
                                "lasted_seconds":backoff.lasted(self.now())})
                        );
                    }
                    if busy.failures() > 0 {
                        eprintln!(
                            "{}",
                            json!({"event":"writer_lock_taken","failures":busy.failures(),
                                "lasted_seconds":busy.lasted(self.now())})
                        );
                    }
                    return Ok(prepared);
                }
                Err(Error::Store(error)) if error.temporary() => {
                    self.unreachable(backoff, Error::Store(error)).await?;
                }
                Err(Error::Store(dsa_store::Error::Refusal("writer_busy"))) => {
                    self.writer_busy(&mut busy).await?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Waits after another session was found holding the writer lock, or
    /// refuses the start once it has held it for `writer_busy_after`: a
    /// session whose process is gone keeps the lock until the server ends it.
    async fn writer_busy(&self, busy: &mut Backoff) -> Result<(), Error> {
        let now = self.now();
        let wait = busy.failed(now).as_secs();
        let lasted = busy.lasted(now);
        let limit = self.settings.writer_busy_after.as_secs();
        if lasted >= limit {
            return Err(Error::Refusal {
                code: "writer_busy",
                detail: json!({"lasted_seconds": lasted}),
            });
        }
        // The last attempt falls within the wait.
        let wait = wait.min(limit - lasted);
        if busy.failures() == 1 {
            eprintln!(
                "{}",
                json!({"event":"writer_busy","refused_after_seconds":self.settings.writer_busy_after.as_secs()})
            );
        }
        self.show(State::Waiting, "writer_busy", Some(now + wait));
        if pause_until(&*self.time, &self.stop, now + wait).await {
            Ok(())
        } else {
            Err(Error::Stopped)
        }
    }

    /// Waits after a failed attempt to reach the database, or ends the
    /// service when the condition has lasted too long.
    async fn unreachable(&self, backoff: &mut Backoff, error: Error) -> Result<(), Error> {
        let now = self.now();
        let wait = backoff.failed(now).as_secs();
        let lasted = backoff.lasted(now);
        let report = error.report();
        if lasted > self.settings.database_fatal_after.as_secs() {
            return Err(Error::Lasting {
                seconds: lasted,
                error: Box::new(error),
            });
        }
        if backoff.failures() == 1 {
            eprintln!("{}", json!({"event":"database_unreachable","error":report}));
        }
        self.show(State::Waiting, "database_unreachable", Some(now + wait));
        if pause_until(&*self.time, &self.stop, now + wait).await {
            Ok(())
        } else {
            Err(Error::Stopped)
        }
    }

    /// One attempt: connect, take the writer lock, restore an empty database
    /// or the build an interrupted restore left, check the retention and
    /// grant and check the reader.
    async fn prepare(&self) -> Result<Prepared, Error> {
        self.show(State::Waiting, "database_connecting", None);
        let mut store = (self.open)(Role::Writer).await?;
        store.lock_writer().await?;
        let state = match store.follow_state(&self.profile).await {
            Ok(state) => Some(state),
            Err(dsa_store::Error::Refusal(
                code @ ("missing_serving_position" | "unsupported_database_format"),
            )) => {
                if !store.loadable(&self.profile).await? {
                    // A build holds no position before its rows below the
                    // horizon are written: it is refused as not serving,
                    // as it is later.
                    if let Some(building) = store.building_progress().await? {
                        return Err(Error::Refusal {
                            code: "database_not_serving",
                            detail: json!({"reason": building.reason}),
                        });
                    }
                    return Err(Error::Store(dsa_store::Error::Refusal(code)));
                }
                None
            }
            Err(error) => return Err(error.into()),
        };
        let restored = match state {
            Some(state) if state.serving => None,
            // The build of an interrupted restore, or an empty layout, is
            // restored again; a database that served is refused.
            Some(state) => {
                if self.settings.restore.is_none() || !store.loadable(&self.profile).await? {
                    return Err(Error::Refusal {
                        code: "database_not_serving",
                        detail: json!({"reason": state.reason}),
                    });
                }
                Some(self.restore(&mut store).await?)
            }
            None => Some(self.restore(&mut store).await?),
        };
        let state = store.follow_state(&self.profile).await?;
        if let Some(retention) = self.settings.retention
            && retention.seconds != state.retention_seconds
        {
            if !retention.change {
                return Err(self
                    .refused(
                        &mut store,
                        "retention_mismatch",
                        json!({"stored": state.retention_seconds, "given": retention.seconds,
                            "change_with": "--change-retention"}),
                    )
                    .await);
            }
            store
                .set_retention(&self.profile, retention.seconds)
                .await?;
            eprintln!(
                "{}",
                json!({"event":"retention_changed","from":state.retention_seconds,"to":retention.seconds})
            );
        }
        let reader = &self.settings.reader;
        let checked = match store.grant_reader(&self.profile, reader).await {
            Ok(_) => store.check_reader(&self.profile, reader).await,
            Err(error) => Err(error),
        };
        match checked {
            Ok(report) => {
                eprintln!(
                    "{}",
                    json!({"event":"reader_checked","role":reader,"warnings":report["warnings"]})
                );
            }
            Err(error) if error.temporary() => return Err(error.into()),
            Err(error) => {
                let report = error.report();
                let code = match &error {
                    dsa_store::Error::ReaderPrivileges(_) => "reader_privileges_not_exact",
                    dsa_store::Error::Refusal(code) => code,
                    _ => return Err(error.into()),
                };
                return Err(self.refused(&mut store, code, report).await);
            }
        }
        Ok(Prepared { store, restored })
    }

    /// Records a refusal of a database this service may write as its
    /// stopped state, and returns it.
    async fn refused(&self, store: &mut Store, code: &'static str, detail: Value) -> Error {
        let error = Error::Refusal { code, detail };
        let report = error.report();
        if let Err(failure) = store
            .record_state("stopped", code, &json!({}), Some(&report))
            .await
        {
            eprintln!(
                "{}",
                json!({"event":"state_record_failed","error":failure.report()})
            );
        }
        error
    }

    /// The first start on an empty database, or after an interrupted
    /// restore: restore `--snapshot`. The file is needed only here; a missing
    /// or unreadable one refuses the start and names the flag.
    async fn restore(&self, store: &mut Store) -> Result<Value, Error> {
        let Some(restore) = &self.settings.restore else {
            return Err(Error::Refusal {
                code: "snapshot_required",
                detail: json!({"database": "empty", "flag": "--snapshot"}),
            });
        };
        let unreadable = match std::fs::File::open(&restore.file).and_then(|file| file.metadata()) {
            Ok(metadata) if metadata.is_file() => None,
            Ok(_) => Some("not_a_file".to_owned()),
            Err(error) => Some(format!("{:?}", error.kind())),
        };
        if let Some(kind) = unreadable {
            return Err(Error::Refusal {
                code: "snapshot_file_unreadable",
                detail: json!({"flag": "--snapshot", "kind": kind}),
            });
        }
        let description = dsa_store::snapshot::describe(&restore.file, &self.profile)?;
        let in_file = description.verified.state.retention_seconds;
        if let Some(retention) = self.settings.retention
            && retention.seconds != in_file
            && !retention.change
        {
            return Err(Error::Refusal {
                code: "retention_mismatch",
                detail: json!({"stored": in_file, "given": retention.seconds,
                    "change_with": "--change-retention"}),
            });
        }
        let file = restore
            .file
            .file_name()
            .map(|name| name.to_string_lossy().into_owned());
        self.show(State::Restoring, "restoring_snapshot", None);
        let began = started(
            "restore",
            json!({"file": file, "content_digest": description.verified.content_digest,
                "target": description.verified.state.target.height}),
        );
        let result = tokio::select! {
            result = store.restore(
                &self.profile,
                &restore.file,
                restore.expect_content_digest.as_deref(),
            ) => result,
            () = self.stop.wait() => {
                finished("restore", began, Err(json!({"error": "stop_requested"})));
                return Err(Error::Stopped);
            }
        };
        match result {
            Ok(verified) => {
                let restored = json!({"file": file, "content_digest": verified.content_digest,
                    "target": verified.state.target.height});
                finished("restore", began, Ok(restored.clone()));
                Ok(restored)
            }
            Err(error) => {
                finished("restore", began, Err(error.report()));
                Err(error.into())
            }
        }
    }
}

/// Reads the database part of the status every `interval` on a read-only
/// connection of its own, until `halt`.
async fn refresh(
    shared: Arc<Shared>,
    open: Open,
    profile: Profile,
    reader: String,
    time: Arc<dyn Time>,
    interval: Duration,
    mut halt: watch::Receiver<bool>,
) {
    let mut store: Option<Store> = None;
    let mut failing: Option<Instant> = None;
    let mut wanted = shared.refresh_wanted.subscribe();
    let mut served = 0;
    loop {
        let requested = *wanted.borrow_and_update();
        // A restore builds and publishes the tables: no read holds a lock
        // its publication would wait for. A request comes once it is done.
        let restoring = shared
            .phase
            .borrow()
            .as_ref()
            .is_some_and(|view| view.state == State::Restoring);
        if restoring && requested == served {
            store = None;
            tokio::select! {
                () = tokio::time::sleep(interval) => continue,
                _ = wanted.changed() => continue,
                _ = halt.wait_for(|halt| *halt) => return,
            }
        }
        let read = tokio::time::timeout(
            REFRESH_DEADLINE,
            read_view(&mut store, &open, &profile, &reader, &*time),
        )
        .await;
        let failure = match read {
            Ok(Ok(view)) => {
                shared.database.send_replace(Some(Arc::new(view)));
                None
            }
            Ok(Err(error)) => {
                if error.temporary() {
                    store = None;
                }
                Some(error.report())
            }
            Err(_) => {
                store = None;
                Some(json!({"error": "status_read_timeout"}))
            }
        };
        match failure {
            None => {
                if let Some(since) = failing.take() {
                    eprintln!(
                        "{}",
                        json!({"event":"status_read_recovered","lasted_seconds":since.elapsed().as_secs_f64()})
                    );
                }
                lock(&shared.notes).refresh = None;
            }
            Some(report) => {
                if failing.is_none() {
                    failing = Some(Instant::now());
                    eprintln!("{}", json!({"event":"status_read_failed","error":report}));
                }
                lock(&shared.notes).refresh = Some(code(&report));
            }
        }
        served = requested;
        shared.refresh_done.send_replace(requested);
        tokio::select! {
            () = tokio::time::sleep(interval) => {}
            _ = wanted.changed() => {}
            _ = halt.wait_for(|halt| *halt) => return,
        }
    }
}

/// One read of the status's database part. Its time is the service's clock
/// at the read, so that its age is measured on one clock.
async fn read_view(
    store: &mut Option<Store>,
    open: &Open,
    profile: &Profile,
    reader: &str,
    time: &dyn Time,
) -> Result<DatabaseView, dsa_store::Error> {
    if store.is_none() {
        let opened = open(Role::Status).await?;
        opened
            .client()
            .batch_execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
            .await?;
        *store = Some(opened);
    }
    let store = store.as_mut().expect("connected");
    let mut view = store.service_view(profile, Some(reader)).await?;
    view.observed_at = i64::try_from(time.unix_seconds()).unwrap_or(i64::MAX);
    Ok(view)
}

/// Composes the documents again whenever a part changes, and every few
/// seconds, until `halt`.
async fn render(shared: Arc<Shared>, time: Arc<dyn Time>, mut halt: watch::Receiver<bool>) {
    let mut phase = shared.phase.subscribe();
    let mut progress = shared.progress.subscribe();
    let mut database = shared.database.subscribe();
    loop {
        shared.render(time.unix_seconds());
        tokio::select! {
            _ = phase.changed() => {}
            _ = progress.changed() => {}
            _ = database.changed() => {}
            () = tokio::time::sleep(RENDER_INTERVAL) => {}
            _ = halt.wait_for(|halt| *halt) => return,
        }
    }
}

/// The upkeep on connections of its own: what an interrupted index rebuild
/// left is dropped at start; the indexes are looked at every hour and the
/// daily snapshot file is written at its time. `current` holds the running
/// job's connection, so that a stop can cancel its statement.
struct Upkeep {
    shared: Arc<Shared>,
    open: Open,
    profile: Profile,
    snapshots: Option<config::Snapshots>,
    policy: IndexPolicy,
    schedule: Arc<Mutex<Schedule>>,
    time: Arc<dyn Time>,
    tick: Duration,
    current: Arc<Mutex<Option<Cancellation>>>,
}

impl Upkeep {
    async fn connect(&self) -> Result<Store, dsa_store::Error> {
        let store = (self.open)(Role::Upkeep).await?;
        *lock(&self.current) = Some(store.cancellation());
        Ok(store)
    }

    async fn job<T: Serialize>(
        &self,
        name: &'static str,
        work: impl Future<Output = Result<T, dsa_store::Error>>,
    ) -> bool {
        let began = started(name, json!({}));
        let result = work.await;
        *lock(&self.current) = None;
        match result {
            Ok(value) => {
                finished(
                    name,
                    began,
                    Ok(serde_json::to_value(value).unwrap_or(Value::Null)),
                );
                self.shared.note_upkeep(name, None);
                true
            }
            Err(error) => {
                let report = error.report();
                finished(name, began, Err(report.clone()));
                self.shared.note_upkeep(name, Some(code(&report)));
                false
            }
        }
    }

    /// Looks at the index sizes, records the missing ones and rebuilds the
    /// index the policy names (`upkeep::rebuild_indexes`); only a rebuild is
    /// logged as an operation.
    async fn indexes(&self) {
        let mut began = None;
        let result = async {
            let store = self.connect().await?;
            upkeep::rebuild_indexes(&store, &self.policy, |_| {
                began = Some(started("index_rebuild", json!({})));
            })
            .await
        }
        .await;
        *lock(&self.current) = None;
        match (result, began) {
            (Ok(Some(rebuild)), Some(began)) => {
                finished(
                    "index_rebuild",
                    began,
                    Ok(serde_json::to_value(rebuild).unwrap_or(Value::Null)),
                );
                self.shared.note_upkeep("index_rebuild", None);
            }
            (Ok(_), _) => self.shared.note_upkeep("index_rebuild", None),
            (Err(error), Some(began)) => {
                let report = error.report();
                finished("index_rebuild", began, Err(report.clone()));
                self.shared
                    .note_upkeep("index_rebuild", Some(code(&report)));
            }
            (Err(error), None) => {
                let report = error.report();
                eprintln!("{}", json!({"event":"index_sizes_failed","error":report}));
                self.shared
                    .note_upkeep("index_rebuild", Some(code(&report)));
            }
        }
    }

    async fn run(self, mut halt: watch::Receiver<bool>) {
        self.job("index_leftovers", async {
            let store = self.connect().await?;
            upkeep::start_indexes(&store).await
        })
        .await;
        let mut written: Option<u64> = None;
        loop {
            let now = self.time.unix_seconds();
            let view = self.shared.database.borrow().clone();
            let recorded = view
                .as_ref()
                .and_then(|view| view.upkeep.snapshot.as_ref())
                .and_then(|snapshot| u64::try_from(snapshot.at).ok());
            let last_written = written.max(recorded);
            let due = lock(&self.schedule).due(self.snapshots.as_ref(), last_written, now);
            for job in due {
                match job {
                    // Runs on the writer's connection, between updates.
                    Job::Supply => {}
                    Job::Indexes => {
                        lock(&self.schedule).indexes_checked = Some(now);
                        self.indexes().await;
                    }
                    // The time of the last file comes from the database.
                    Job::Snapshot if view.is_some() => {
                        let Some(snapshots) = &self.snapshots else {
                            continue;
                        };
                        let done = self
                            .job("snapshot_file", async {
                                let mut store = self.connect().await?;
                                upkeep::write_snapshot(&mut store, &self.profile, snapshots).await
                            })
                            .await;
                        let mut schedule = lock(&self.schedule);
                        if done {
                            written = Some(now);
                            schedule.snapshot_failed = None;
                        } else {
                            schedule.snapshot_failed = Some(now);
                        }
                    }
                    Job::Snapshot => {}
                }
            }
            tokio::select! {
                () = tokio::time::sleep(self.tick) => {}
                _ = halt.wait_for(|halt| *halt) => return,
            }
        }
    }
}

/// The tasks besides the follower, ended together.
struct Tasks {
    halt: watch::Sender<bool>,
    handles: Vec<JoinHandle<()>>,
    current: Arc<Mutex<Option<Cancellation>>>,
}

impl Tasks {
    /// Cancels a running upkeep statement and ends every task: each ends at
    /// its next step, and what still runs after half a second is aborted.
    async fn end(self) {
        let _ = self.halt.send(true);
        let running = lock(&self.current).take();
        if let Some(cancellation) = running {
            let _ = tokio::time::timeout(Duration::from_secs(1), cancellation.cancel()).await;
        }
        let aborts = self
            .handles
            .iter()
            .map(JoinHandle::abort_handle)
            .collect::<Vec<_>>();
        let ended = futures_util::future::join_all(self.handles);
        if tokio::time::timeout(Duration::from_millis(500), ended)
            .await
            .is_err()
        {
            for abort in aborts {
                abort.abort();
            }
        }
    }
}

/// Runs the service until a stop request or a condition that ends it.
pub async fn run(context: Context) -> Result<Report, Error> {
    let Context {
        profile,
        provider,
        open,
        settings,
        follower,
        index_policy,
        refresh_interval,
        upkeep_tick,
        listener,
        time,
        stop,
    } = context;
    let status_settings = settings.status(&profile.network);
    let initial = phase(&provider, State::Waiting, "starting", None);
    let shared = Arc::new(Shared {
        phase: watch::channel(Some(initial)).0,
        progress: watch::channel(Progress {
            reason: "starting".to_owned(),
            ..Progress::default()
        })
        .0,
        database: watch::channel(None).0,
        notes: Mutex::new(Notes::default()),
        refresh_wanted: watch::channel(0).0,
        refresh_done: watch::channel(0).0,
        documents: watch::channel(Documents {
            healthy: false,
            body: "{}".into(),
        })
        .0,
        settings: status_settings,
    });
    shared.render(time.unix_seconds());
    let (halt, halted) = watch::channel(false);
    let current = Arc::new(Mutex::new(None));
    let mut tasks = Tasks {
        halt,
        handles: Vec::new(),
        current: Arc::clone(&current),
    };
    let address = listener.local_addr().ok();
    eprintln!(
        "{}",
        json!({"event":"listening","address":address.map(|address| address.to_string())})
    );
    {
        let documents = shared.documents.subscribe();
        let mut halt = halted.clone();
        tasks
            .handles
            .push(tokio::spawn(http::serve(listener, documents, async move {
                let _ = halt.wait_for(|halt| *halt).await;
            })));
    }
    tasks.handles.push(tokio::spawn(render(
        Arc::clone(&shared),
        Arc::clone(&time),
        halted.clone(),
    )));
    tasks.handles.push(tokio::spawn(refresh(
        Arc::clone(&shared),
        Arc::clone(&open),
        profile.clone(),
        settings.reader.clone(),
        Arc::clone(&time),
        refresh_interval,
        halted.clone(),
    )));
    let service = Service {
        profile: profile.clone(),
        provider: provider.clone(),
        open: Arc::clone(&open),
        settings: settings.clone(),
        time: Arc::clone(&time),
        stop: stop.clone(),
        shared: Arc::clone(&shared),
    };
    let schedule = Arc::new(Mutex::new(Schedule::default()));
    let mut follower_settings = follower;
    follower_settings.fatal_after = settings.database_fatal_after;
    follower_settings.refused_answers_end_the_run = false;
    follower_settings.writer_busy_after = Some(settings.writer_busy_after);
    let mut backoff = Backoff::default();
    let mut restored = None;
    let mut upkeep_started = false;
    let outcome = loop {
        let prepared = match service.start(&mut backoff).await {
            Ok(prepared) => prepared,
            Err(error) => break Err(error),
        };
        restored = restored.or(prepared.restored);
        // The database part of the status is read once the database is
        // prepared (restored, reader granted), so that the first document
        // of the follower is current.
        let mut generation = 0;
        shared.refresh_wanted.send_modify(|wanted| {
            *wanted += 1;
            generation = *wanted;
        });
        let mut done = shared.refresh_done.subscribe();
        let read =
            tokio::time::timeout(REFRESH_DEADLINE, done.wait_for(|done| *done >= generation));
        tokio::select! {
            _ = read => {}
            () = stop.wait() => {}
        }
        if !upkeep_started {
            upkeep_started = true;
            let upkeep = Upkeep {
                shared: Arc::clone(&shared),
                open: Arc::clone(&open),
                profile: profile.clone(),
                snapshots: settings.snapshots.clone(),
                policy: index_policy,
                schedule: Arc::clone(&schedule),
                time: Arc::clone(&time),
                tick: upkeep_tick,
                current: Arc::clone(&current),
            };
            tasks.handles.push(tokio::spawn(upkeep.run(halted.clone())));
        }
        shared.phase.send_replace(None);
        let between = SupplyCheck {
            profile: profile.clone(),
            schedule: Arc::clone(&schedule),
            shared: Arc::clone(&shared),
        };
        let followed = follow::follow_with(
            follow::Context {
                profile: &profile,
                provider: provider.clone(),
                connect: handing(prepared.store, Arc::clone(&open)),
                settings: follower_settings.clone(),
                time: Arc::clone(&time),
                stop: stop.clone(),
                progress: shared.progress.clone(),
                requests: None,
            },
            Some(Box::new(between)),
        )
        .await;
        match followed {
            // The follower could not start on the prepared connection:
            // reach the database again.
            Err(Error::Store(error)) if error.temporary() => {
                if let Err(error) = service.unreachable(&mut backoff, Error::Store(error)).await {
                    break Err(error);
                }
            }
            other => break other.map(Some),
        }
    };
    let outcome = match outcome {
        Err(Error::Stopped) => Ok(None),
        other => other,
    };
    if outcome.is_ok() && shared.phase.borrow().is_some() {
        service.show(State::Stopped, "stop_requested", None);
    }
    if let Err(error) = &outcome {
        let report = error.report();
        eprintln!("{}", json!({"event":"service_stopped","error":report}));
        if shared.phase.borrow().is_some() {
            service.show(State::Stopped, &code(&report), None);
        }
    }
    // The last document: read the database once more, briefly.
    let mut store = None;
    if let Ok(Ok(view)) = tokio::time::timeout(
        FINAL_READ_DEADLINE,
        read_view(&mut store, &open, &profile, &settings.reader, &*time),
    )
    .await
    {
        shared.database.send_replace(Some(Arc::new(view)));
    }
    drop(store);
    let status = shared.render(time.unix_seconds());
    tasks.end().await;
    let follower = outcome?;
    Ok(Report {
        stopped: follower
            .as_ref()
            .map_or("stop_requested", |report| report.stopped),
        restored,
        follower,
        status,
    })
}
