//! The follower loop. It asks the provider for the blocks after the stored
//! tip, replaces the headers above the target after a tip fork, makes blocks
//! final in batches, stores each epoch's parameters with the update that
//! serves its first block, reads stake distributions apart from the updates
//! and waits out every temporary condition. The stored position keeps tip
//! height − target height = k at every commit.

use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, PoisonError},
    time::{Duration, Instant},
};

use chrono::{DateTime, Days, NaiveDateTime};
use dsa_chain::{
    block::Selection,
    profiles::{Profile, network_magic},
};
use dsa_provider::{
    Kind, Provider, Refusal,
    pages::PAGE_SIZE,
    types::{Hash32, Header, Page},
};
use dsa_store::{
    Store,
    follow::{BlockRow, EpochParamRow, HeaderRef, Update, UpdateReport},
};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::{sync::watch, task::JoinHandle};

use crate::{
    block::{self, Limits},
    epoch::{self, Distribution, StakeError},
    retry::{Backoff, Stop, Time, pause_until},
};

/// The wait before a contradicted target is read again, and before epoch
/// parameters the provider does not have yet are asked for again.
pub const RECHECK: Duration = Duration::from_secs(60);
/// While no update happens, call counts are stored at least this often
/// (seconds).
pub const COUNTS_INTERVAL: u64 = 600;
/// At the tip, while a poll finds nothing after the stored tip, the
/// provider's head is read this often.
pub const HEAD_INTERVAL: Duration = Duration::from_secs(60);
/// The window of the blocks-per-hour figure (seconds).
const RATE_WINDOW: u64 = 600;
/// While catching up, at most one update line per this many seconds.
const UPDATE_LOG_INTERVAL: Duration = Duration::from_secs(10);
/// During a long wait, the work between steps runs at least this often
/// (seconds).
const BETWEEN_INTERVAL: u64 = 60;

#[derive(Clone, Debug, PartialEq)]
pub struct Settings {
    /// The pause between iterations at the tip.
    pub poll_interval: Duration,
    /// The least time between two horizon advances.
    pub prune_interval: Duration,
    pub limits: Limits,
    /// Transaction pages requested ahead; the provider's in-flight limit.
    pub max_in_flight: usize,
    /// A database or provider condition that lasts longer ends the run.
    pub fatal_after: Duration,
    /// Whether an answer the follower refuses on every attempt (it
    /// contradicts itself or the chain) also ends the run once it has lasted
    /// `fatal_after`. Otherwise it is retried without end and named in the
    /// state.
    pub refused_answers_end_the_run: bool,
    /// How long a reconnect waits for the writer lock that another session
    /// holds before the run ends with `writer_busy`; `None` ends it at once.
    pub writer_busy_after: Option<Duration>,
    /// Make no block above this height final, complete the stake of the
    /// epochs up to it and return once the stored target equals it.
    pub stop_at_target: Option<u32>,
    /// Follow a database that a build fills (not serving, reason
    /// `rebuilding`): its window is made final with the same checks, and
    /// the horizon does not move.
    pub building: bool,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(20),
            prune_interval: Duration::from_secs(3_600),
            limits: Limits::default(),
            max_in_flight: 4,
            fatal_after: Duration::from_secs(1_800),
            refused_answers_end_the_run: true,
            writer_busy_after: None,
            stop_at_target: None,
            building: false,
        }
    }
}

/// Requests the follower made, per endpoint family.
#[derive(Clone, Debug, Default)]
pub struct Requests(Arc<Mutex<BTreeMap<&'static str, u64>>>);

impl Requests {
    pub fn count(&self, family: &'static str) {
        *self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(family)
            .or_default() += 1;
    }

    pub fn get(&self) -> BTreeMap<String, u64> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|(family, count)| ((*family).to_owned(), *count))
            .collect()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    #[default]
    CatchingUp,
    Following,
    Waiting,
    Stopped,
}

impl State {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CatchingUp => "catching_up",
            Self::Following => "following",
            Self::Waiting => "waiting",
            Self::Stopped => "stopped",
        }
    }
}

/// The provider's head as last observed. Times are Unix seconds.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ProviderTip {
    pub height: u32,
    pub hash: String,
    pub time: u64,
    pub observed_at: u64,
}

/// The target's epoch without a stored stake distribution while the
/// follower is at the tip, and since when (Unix seconds).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct StakeMissing {
    pub epoch: u32,
    pub since: u64,
}

/// What only the running loop knows, published after every step.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Progress {
    pub state: State,
    pub reason: String,
    /// When the current wait ends (Unix seconds).
    pub wait_until: Option<u64>,
    pub provider_tip: Option<ProviderTip>,
    pub requests_per_second: f64,
    pub requests_in_flight: usize,
    /// The provider's last refusal of every request, with the time its wait
    /// ends.
    pub last_refusal: Option<Refusal>,
    /// Final blocks the provider has that the database does not.
    pub blocks_remaining: Option<u64>,
    /// Blocks made final per hour over the last ten minutes.
    pub blocks_per_hour: Option<f64>,
    pub last_update: Option<UpdateReport>,
    pub last_error: Option<Value>,
    /// The stored target as the loop knows it.
    pub target: Option<u32>,
    /// The last failed stake read: its epoch, when it failed, when it is
    /// tried again, its failures in a row and the error with its cause.
    pub stake_failure: Option<Value>,
    pub stake_missing: Option<StakeMissing>,
}

/// Where a run left the database.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Position {
    pub target: HeaderRef,
    pub tip: HeaderRef,
    pub horizon: u32,
}

/// The result of a run that ended by a stop request or at its stop target.
#[derive(Clone, Debug, Serialize)]
pub struct Report {
    /// `stop_target` or `stop_requested`.
    pub stopped: &'static str,
    pub position: Position,
    pub updates: u64,
    /// Requests started per endpoint family; a page requested ahead and
    /// dropped when a batch ends at a limit is counted.
    pub requests: BTreeMap<String, u64>,
    /// Requests the provider client sent during the run.
    pub sent: i64,
    pub seconds: f64,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{}", .0.code)]
    Provider(dsa_provider::Error),
    #[error(transparent)]
    Store(#[from] dsa_store::Error),
    /// A provider answer that contradicts itself or the chain; nothing was
    /// written and the step is tried again.
    #[error("{code}")]
    Answer { code: &'static str, detail: Value },
    /// A check that stops serving: the gate is closed.
    #[error("{code}")]
    Check { code: &'static str, detail: Value },
    /// The loop cannot run on this database or with these settings.
    #[error("{code}")]
    Refusal { code: &'static str, detail: Value },
    /// A temporary condition that lasted longer than `fatal_after`.
    #[error("{error}")]
    Lasting { seconds: u64, error: Box<Error> },
    #[error("stop_requested")]
    Stopped,
}

impl Error {
    pub(crate) fn answer(code: &'static str, detail: Value) -> Self {
        Self::Answer { code, detail }
    }

    /// The named cause, without keys, URLs or connection settings.
    pub fn report(&self) -> Value {
        match self {
            Self::Provider(error) => json!({"error":error.code,"provider":error}),
            Self::Store(error) => error.report(),
            Self::Answer { code, detail } => json!({"error":code,"detail":detail}),
            Self::Check { code, detail } => json!({"error":code,"detail":detail,"serving":false}),
            Self::Refusal { code, detail } => json!({"error":code,"detail":detail}),
            Self::Lasting { seconds, error } => {
                let mut report = error.report();
                report["lasted_seconds"] = json!(seconds);
                report
            }
            Self::Stopped => json!({"error":"stop_requested"}),
        }
    }

    fn code(&self) -> String {
        self.report()["error"]
            .as_str()
            .unwrap_or("follower_error")
            .to_owned()
    }
}

/// Runs a provider request unless the stop comes first.
pub(crate) async fn guarded<T>(
    stop: &Stop,
    request: impl Future<Output = Result<T, dsa_provider::Error>>,
) -> Result<T, Error> {
    tokio::select! {
        result = request => result.map_err(Error::Provider),
        () = stop.wait() => Err(Error::Stopped),
    }
}

/// Opens a new connection to the store, registered for cancellation by the
/// caller.
pub type Connect = Box<
    dyn Fn() -> Pin<Box<dyn Future<Output = Result<Store, dsa_store::Error>> + Send>> + Send + Sync,
>;

/// The loop's outside world.
pub struct Context<'a> {
    pub profile: &'a Profile,
    pub provider: Provider,
    pub connect: Connect,
    pub settings: Settings,
    pub time: Arc<dyn Time>,
    pub stop: Stop,
    pub progress: watch::Sender<Progress>,
}

/// Work that runs on the writer's connection between two steps of the loop,
/// never during an update.
pub trait Between: Send {
    /// Runs what is due at `now` (Unix seconds). A `Check` error closes the
    /// gate with its code and ends the run; a temporary store error drops the
    /// connection; other errors are the work's own to report.
    fn run<'s>(
        &'s mut self,
        store: &'s mut Store,
        now: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 's>>;
}

/// Follows the provider's chain into the store until a stop request, the
/// stop target, or a condition that cannot be retried.
pub async fn follow(context: Context<'_>) -> Result<Report, Error> {
    follow_with(context, None).await
}

/// [`follow`], with `between` run on the writer's connection between steps.
pub async fn follow_with<'a>(
    context: Context<'a>,
    between: Option<Box<dyn Between + 'a>>,
) -> Result<Report, Error> {
    let started = Instant::now();
    let mut follower = Follower::start(context).await?;
    follower.between = between;
    let result = follower.run().await;
    follower.finish(result, started).await
}

/// The kind of condition a failed step belongs to: how long one condition
/// has lasted is measured within its kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Condition {
    /// The database or the provider's transport: retried, and ends the run
    /// once it has lasted `fatal_after`.
    Temporary,
    /// An answer the follower refuses.
    Answer,
    /// Another session holds the writer lock at a reconnect: retried, and
    /// ends the run once it has lasted `writer_busy_after`.
    WriterBusy,
}

/// The SQLSTATE of a store error, if the server gave one.
fn sqlstate(error: &dsa_store::Error) -> Option<String> {
    error.report()["sqlstate"].as_str().map(str::to_owned)
}

/// Store errors a retry may cure, and whether the connection is dropped
/// first: a lost connection or a server that is starting or stopping (drop),
/// a stored position that moved, as after a commit whose outcome was not
/// seen (drop, then read it again), and on the service's own rows a lock or
/// statement timeout, a serialization failure, a deadlock or a lack of
/// resources (keep).
fn retried(error: &dsa_store::Error) -> Option<bool> {
    if error.temporary() || matches!(error, dsa_store::Error::Refusal("update_base_mismatch")) {
        return Some(true);
    }
    let code = sqlstate(error)?;
    (code.starts_with("53") || matches!(code.as_str(), "55P03" | "57014" | "40001" | "40P01"))
        .then_some(false)
}

/// The stored position: the target's header, the headers above it and the
/// horizon height.
struct Stored {
    target: BlockRow,
    above: BTreeMap<u32, BlockRow>,
    horizon: u32,
}

impl Stored {
    fn tip(&self) -> &BlockRow {
        self.above
            .last_key_value()
            .map_or(&self.target, |(_, row)| row)
    }
}

async fn load(store: &Store, profile: &Profile, building: bool) -> Result<Stored, Error> {
    let state = store.follow_state(profile).await?;
    let built = building && state.reason == dsa_store::rebuild::REBUILDING;
    if !state.serving && !built {
        return Err(Error::Refusal {
            code: "database_not_serving",
            detail: json!({"reason":state.reason}),
        });
    }
    let (target, tip) = (state.target.height, state.tip.height);
    let mut rows = store.headers_above(target.saturating_sub(1)).await?;
    rows.retain(|row| row.height >= target);
    let incomplete = || Error::Refusal {
        code: "stored_headers_incomplete",
        detail: json!({"target":target,"tip":tip}),
    };
    let mut rows = rows.into_iter();
    let target_row = rows
        .next()
        .filter(|row| row.height == target && row.hash == state.target.hash)
        .ok_or_else(incomplete)?;
    let above: BTreeMap<u32, BlockRow> = rows.map(|row| (row.height, row)).collect();
    let k = profile.cardano.security_parameter;
    if tip != target + k
        || above.len() != k as usize
        || above.keys().copied().ne(target + 1..=tip)
        || above.get(&tip).is_none_or(|row| row.hash != state.tip.hash)
    {
        return Err(incomplete());
    }
    Ok(Stored {
        target: target_row,
        above,
        horizon: state.horizon.height,
    })
}

/// Requests the client has counted, over the days it keeps.
fn sent(provider: &Provider) -> i64 {
    provider.counts().iter().map(|(_, count)| count).sum()
}

fn hash32(hash: &str) -> Result<Hash32, Error> {
    hash.parse()
        .map_err(|_| Error::answer("invalid_block_hash", json!({"hash":hash})))
}

fn block_row(header: &Header) -> BlockRow {
    BlockRow {
        height: header.height,
        hash: header.hash.to_string(),
        epoch: header.epoch,
        slot: header.slot,
        time: i64::try_from(header.time)
            .ok()
            .and_then(|seconds| DateTime::from_timestamp(seconds, 0))
            .map_or(NaiveDateTime::MIN, |time| time.naive_utc()),
        transaction_count: header.tx_count,
    }
}

/// The position of the first header that does not follow the one before it,
/// starting from (`height`, `hash`).
fn first_break(height: u32, hash: &str, headers: &[Header]) -> Option<usize> {
    let (mut height, mut hash) = (height, hash.to_owned());
    for (index, header) in headers.iter().enumerate() {
        if header.height != height + 1
            || header.previous_block.as_ref().map(Hash32::as_str) != Some(hash.as_str())
        {
            return Some(index);
        }
        height = header.height;
        hash = header.hash.to_string();
    }
    None
}

enum Outcome {
    /// More final blocks are available now.
    More,
    /// The database holds everything the provider has.
    AtTip,
    Wait {
        reason: &'static str,
        until: u64,
    },
}

/// The waits after a stake read the provider's answers made fail, for one
/// epoch, in seconds; the last repeats. A transport failure waits by
/// [`crate::retry::BACKOFF`].
pub const STAKE_REFUSED_BACKOFF: [u64; 9] = [1, 2, 4, 8, 16, 30, 60, 300, 600];

/// The next attempt at one epoch's stake.
#[derive(Clone, Copy, Debug, Default)]
struct StakeRetry {
    failures: usize,
    at: u64,
}

/// The background stake read: one epoch at a time, the oldest without stake
/// whose own retry time has passed, so that an epoch that cannot be read
/// does not hold back newer ones.
#[derive(Default)]
struct Stake {
    task: Option<(u32, JoinHandle<Result<Distribution, StakeError>>)>,
    /// Epochs whose read failed.
    retries: BTreeMap<u32, StakeRetry>,
    /// A failed read of the stored stake epochs waits until then.
    window_retry_at: u64,
    window_failures: Backoff,
}

enum StakeStatus {
    /// Every epoch up to the target's has a distribution.
    Done,
    /// A read runs or waits for its next attempt.
    Busy,
}

struct Follower<'a> {
    profile: &'a Profile,
    selection: Selection<'a>,
    provider: Provider,
    requests: Requests,
    connect: Connect,
    settings: Settings,
    time: Arc<dyn Time>,
    stop: Stop,
    progress: watch::Sender<Progress>,
    store: Option<Store>,
    stored: Stored,
    provider_tip: Option<ProviderTip>,
    /// When `/blocks/latest` was last read.
    head_read_at: Option<u64>,
    /// Reads of the head in a row that stood above the stored tip while
    /// nothing followed the stored tip.
    head_above: u32,
    last_prune_check: Option<u64>,
    last_counts: u64,
    last_update_log: Option<Instant>,
    updates: u64,
    sent_before: i64,
    recorded: Option<(State, String)>,
    samples: VecDeque<(u64, u32)>,
    stake: Stake,
    backoff: Backoff,
    /// The condition the current failures belong to.
    condition: Option<Condition>,
    between: Option<Box<dyn Between + 'a>>,
    /// The provider's `/genesis` named the profile's network.
    network_checked: bool,
}

impl<'a> Follower<'a> {
    async fn start(context: Context<'a>) -> Result<Self, Error> {
        let Context {
            profile,
            provider,
            connect,
            settings,
            time,
            stop,
            progress,
        } = context;
        let mut store = connect().await?;
        let stored = load(&store, profile, settings.building).await?;
        if let Some(stop_at) = settings.stop_at_target
            && stop_at < stored.target.height
        {
            return Err(Error::Refusal {
                code: "stop_target_below_stored_target",
                detail: json!({"stop_at_target":stop_at,"target":stored.target.height}),
            });
        }
        let now = time.unix_seconds();
        let today = i64::try_from(now)
            .ok()
            .and_then(|seconds| DateTime::from_timestamp(seconds, 0))
            .map_or(chrono::NaiveDate::MIN, |time| time.date_naive());
        let since = today.checked_sub_days(Days::new(7)).unwrap_or(today);
        provider.seed_counts(&store.calls(since).await?);
        // Takes the writer lock: a second writer is refused here.
        store.record_calls(&provider.counts()).await?;
        let sent_before = sent(&provider);
        let mut follower = Self {
            selection: Selection::new(profile),
            profile,
            provider,
            requests: Requests::default(),
            connect,
            settings,
            stop,
            progress,
            store: Some(store),
            provider_tip: None,
            head_read_at: None,
            head_above: 0,
            last_prune_check: None,
            last_counts: now,
            last_update_log: None,
            updates: 0,
            sent_before,
            recorded: None,
            samples: VecDeque::from([(now, stored.target.height)]),
            stored,
            stake: Stake::default(),
            backoff: Backoff::default(),
            condition: None,
            between: None,
            network_checked: false,
            time,
        };
        follower
            .set_state(State::CatchingUp, "starting", None, None)
            .await;
        Ok(follower)
    }

    fn now(&self) -> u64 {
        self.time.unix_seconds()
    }

    fn k(&self) -> u32 {
        self.profile.cardano.security_parameter
    }

    async fn run(&mut self) -> Result<&'static str, Error> {
        loop {
            if self.stop.requested() {
                return Ok("stop_requested");
            }
            if self.settings.stop_at_target == Some(self.stored.target.height) {
                return match self.complete_stake().await {
                    Ok(()) => Ok("stop_target"),
                    Err(Error::Stopped) => Ok("stop_requested"),
                    Err(error) => Err(error),
                };
            }
            let outcome = self.step().await;
            self.collect_stake().await;
            let wait = match outcome {
                Ok(Outcome::More) => {
                    self.recovered();
                    self.set_state(State::CatchingUp, "catching_up", None, None)
                        .await;
                    None
                }
                Ok(Outcome::AtTip) => {
                    self.recovered();
                    self.set_state(State::Following, "following", None, None)
                        .await;
                    self.start_stake().await;
                    self.persist_counts_if_due().await;
                    Some(self.now() + self.settings.poll_interval.as_secs())
                }
                Ok(Outcome::Wait { reason, until }) => {
                    self.recovered();
                    self.set_state(State::Waiting, reason, Some(until), None)
                        .await;
                    self.persist_counts_if_due().await;
                    Some(until)
                }
                Err(Error::Stopped) => return Ok("stop_requested"),
                Err(error) => Some(self.failed(error).await?),
            };
            self.between_steps().await?;
            if let Some(wait) = wait
                && !self.wait_until(wait).await?
            {
                return Ok("stop_requested");
            }
        }
    }

    /// Ends a run of failures: logs how long the condition lasted.
    fn recovered(&mut self) {
        if let Some(condition) = self.condition.take() {
            let now = self.now();
            eprintln!(
                "{}",
                json!({"event":"follower_recovered","condition":condition,
                    "failures":self.backoff.failures(),"lasted_seconds":self.backoff.lasted(now)})
            );
        }
        self.backoff.reset();
    }

    /// Runs the work between steps on the writer's connection, if any.
    async fn between_steps(&mut self) -> Result<(), Error> {
        let now = self.now();
        let (Some(between), Some(store)) = (self.between.as_mut(), self.store.as_mut()) else {
            return Ok(());
        };
        match between.run(store, now).await {
            Ok(()) => Ok(()),
            Err(error @ Error::Check { .. }) => Err(self.closed_by_check(error).await),
            Err(Error::Store(error)) if error.temporary() => {
                self.store = None;
                Ok(())
            }
            Err(_) => Ok(()),
        }
    }

    /// Waits until `until`; `false` when a stop came first. With work
    /// between steps, the wait is cut so that the work runs at least every
    /// minute.
    async fn wait_until(&mut self, until: u64) -> Result<bool, Error> {
        if self.between.is_none() {
            return Ok(pause_until(&*self.time, &self.stop, until).await);
        }
        loop {
            let now = self.now();
            if now >= until {
                return Ok(!self.stop.requested());
            }
            if !pause_until(&*self.time, &self.stop, until.min(now + BETWEEN_INTERVAL)).await {
                return Ok(false);
            }
            self.between_steps().await?;
        }
    }

    /// Closes the gate with a check's code and returns the error that ends
    /// the run.
    async fn closed_by_check(&mut self, error: Error) -> Error {
        let Error::Check { code, detail } = &error else {
            return self.fatal(error).await;
        };
        let (code, detail) = (*code, detail.clone());
        if let Err(closing) = self.close_gate(code, &detail).await {
            eprintln!(
                "{}",
                json!({"event":"gate_close_failed","check":code,"error":closing.report()})
            );
            return self.fatal(closing).await;
        }
        self.fatal(error).await
    }

    /// Decides what a failed step leads to: the time of the next attempt, or
    /// the error that ends the run.
    async fn failed(&mut self, error: Error) -> Result<u64, Error> {
        let now = self.now();
        let (condition, not_before) = match &error {
            Error::Provider(provider) => match provider.kind {
                Kind::Fatal { .. } => return Err(self.fatal(error).await),
                // A refusal of every request names its own end: waited out,
                // never fatal (a spent allowance resumes the next UTC day).
                Kind::Wait { until } => {
                    // The provider answers: an earlier condition has ended,
                    // and the wait does not count towards the next one.
                    self.recovered();
                    let report = error.report();
                    self.set_state(State::Waiting, provider.code, Some(until), Some(&report))
                        .await;
                    self.persist_counts_if_due().await;
                    return Ok(until);
                }
                // A transport failure, a timeout or a 5xx counts towards the
                // fatal time; an answer the client refused is one the
                // follower refuses.
                Kind::Retry { until } if provider.transport() => (Condition::Temporary, until),
                Kind::Retry { until } => (Condition::Answer, until),
            },
            Error::Store(dsa_store::Error::Refusal("writer_busy"))
                if self.settings.writer_busy_after.is_some() =>
            {
                self.store = None;
                (Condition::WriterBusy, now)
            }
            Error::Store(store) => match retried(store) {
                Some(reconnect) => {
                    if reconnect {
                        self.store = None;
                    }
                    (Condition::Temporary, now)
                }
                None => return Err(self.fatal(error).await),
            },
            Error::Answer { .. } => (Condition::Answer, now),
            Error::Check { .. } => return Err(self.closed_by_check(error).await),
            _ => return Err(self.fatal(error).await),
        };
        // Failures of another kind start a new condition.
        if self.condition != Some(condition) {
            self.backoff.reset();
            self.condition = Some(condition);
        }
        let mut until = not_before.max(now + self.backoff.failed(now).as_secs());
        let lasted = self.backoff.lasted(now);
        let limit = match condition {
            Condition::Temporary => Some(self.settings.fatal_after),
            Condition::Answer => self
                .settings
                .refused_answers_end_the_run
                .then_some(self.settings.fatal_after),
            Condition::WriterBusy => self.settings.writer_busy_after,
        };
        if let (Condition::WriterBusy, Some(limit)) = (condition, limit) {
            // The last attempt falls within the wait.
            until = until.min(now + limit.as_secs().saturating_sub(lasted));
        }
        if limit.is_some_and(|limit| lasted > limit.as_secs()) {
            // A writer lock held too long is refused as at start, not as a
            // temporary condition.
            let ending = match condition {
                Condition::WriterBusy => Error::Refusal {
                    code: "writer_busy",
                    detail: json!({"lasted_seconds": lasted}),
                },
                _ => Error::Lasting {
                    seconds: lasted,
                    error: Box::new(error),
                },
            };
            return Err(self.fatal(ending).await);
        }
        let report = error.report();
        eprintln!(
            "{}",
            json!({"event":"follower_retry","condition":condition,"error":report,"retry_at":until,
                "failures":self.backoff.failures(),"lasted_seconds":lasted})
        );
        let code = error.code();
        self.set_state(State::Waiting, &code, Some(until), Some(&report))
            .await;
        Ok(until)
    }

    async fn close_gate(&mut self, code: &str, detail: &Value) -> Result<(), Error> {
        self.connect_if_needed().await?;
        let store = self.store.as_mut().expect("connected");
        store.close_gate(code, detail).await?;
        eprintln!(
            "{}",
            json!({"event":"gate_closed","check":code,"detail":detail})
        );
        Ok(())
    }

    /// Records the error that ends the run and returns it.
    async fn fatal(&mut self, error: Error) -> Error {
        let report = error.report();
        let code = error.code();
        self.set_state(State::Stopped, &code, None, Some(&report))
            .await;
        self.persist_counts().await;
        eprintln!("{}", json!({"event":"follower_stopped","error":report}));
        error
    }

    async fn finish(
        mut self,
        result: Result<&'static str, Error>,
        started: Instant,
    ) -> Result<Report, Error> {
        if let Some((_, task)) = self.stake.task.take() {
            task.abort();
        }
        let stopped = result?;
        self.persist_counts().await;
        self.set_state(State::Stopped, stopped, None, None).await;
        let tip = self.stored.tip();
        Ok(Report {
            stopped,
            position: Position {
                target: HeaderRef {
                    height: self.stored.target.height,
                    hash: self.stored.target.hash.clone(),
                },
                tip: HeaderRef {
                    height: tip.height,
                    hash: tip.hash.clone(),
                },
                horizon: self.stored.horizon,
            },
            updates: self.updates,
            requests: self.requests.get(),
            sent: sent(&self.provider) - self.sent_before,
            seconds: started.elapsed().as_secs_f64(),
        })
    }

    /// Connects again after a lost connection and reads the stored position,
    /// which a commit whose outcome was not seen may have moved.
    async fn connect_if_needed(&mut self) -> Result<(), Error> {
        if self.store.is_some() {
            return Ok(());
        }
        let mut store = (self.connect)().await?;
        let stored = load(&store, self.profile, self.settings.building).await?;
        store.record_calls(&self.provider.counts()).await?;
        self.stored = stored;
        self.store = Some(store);
        self.recorded = None;
        Ok(())
    }

    fn publish(&self, state: State, reason: &str, until: Option<u64>, error: Option<&Value>) {
        let status = self.provider.status();
        let now = self.now();
        let k = u64::from(self.k());
        let target = self.stored.target.height;
        let remaining = self.provider_tip.as_ref().map(|tip| {
            u64::from(tip.height)
                .saturating_sub(k)
                .saturating_sub(u64::from(target))
        });
        let rate = self.rate(now);
        self.progress.send_modify(|progress| {
            progress.state = state;
            progress.reason = reason.to_owned();
            progress.wait_until = until;
            progress.provider_tip = self.provider_tip.clone();
            progress.requests_per_second = status.requests_per_second;
            progress.requests_in_flight = status.requests_in_flight;
            progress.last_refusal = status.last_refusal.clone();
            progress.blocks_remaining = remaining;
            progress.blocks_per_hour = rate;
            progress.target = Some(target);
            if let Some(error) = error {
                progress.last_error = Some(error.clone());
            }
        });
    }

    /// Publishes the step and records a changed state in the database.
    async fn set_state(
        &mut self,
        state: State,
        reason: &str,
        until: Option<u64>,
        error: Option<&Value>,
    ) {
        self.publish(state, reason, until, error);
        if self.recorded.as_ref() == Some(&(state, reason.to_owned())) {
            return;
        }
        let Some(store) = self.store.as_mut() else {
            return;
        };
        let mut detail = json!({
            "wait_until": until,
            "target": self.stored.target.height,
            "provider_tip": self.provider_tip,
        });
        if self.settings.building {
            detail["build_target"] = json!(self.settings.stop_at_target);
        }
        match store
            .record_state(state.as_str(), reason, &detail, error)
            .await
        {
            Ok(()) => {
                eprintln!(
                    "{}",
                    json!({"event":"follower_state","state":state,"reason":reason,"wait_until":until})
                );
                self.recorded = Some((state, reason.to_owned()));
            }
            Err(failure) => {
                if failure.temporary() {
                    self.store = None;
                }
                eprintln!(
                    "{}",
                    json!({"event":"state_record_failed","error":failure.report()})
                );
            }
        }
    }

    fn rate(&self, now: u64) -> Option<f64> {
        let (first_time, first_target) = *self.samples.front()?;
        let (last_time, last_target) = *self.samples.back()?;
        let span = now.max(last_time).saturating_sub(first_time);
        (span > 0).then(|| f64::from(last_target - first_target) * 3_600.0 / span as f64)
    }

    fn sample(&mut self, now: u64) {
        self.samples.push_back((now, self.stored.target.height));
        while self.samples.len() > 2
            && self
                .samples
                .get(1)
                .is_some_and(|(time, _)| now.saturating_sub(*time) >= RATE_WINDOW)
        {
            self.samples.pop_front();
        }
    }

    async fn persist_counts_if_due(&mut self) {
        if self.now().saturating_sub(self.last_counts) >= COUNTS_INTERVAL {
            self.persist_counts().await;
        }
    }

    async fn persist_counts(&mut self) {
        let counts = self.provider.counts();
        let Some(store) = self.store.as_mut() else {
            return;
        };
        match store.record_calls(&counts).await {
            Ok(()) => self.last_counts = self.now(),
            Err(failure) => {
                if failure.temporary() {
                    self.store = None;
                }
                eprintln!(
                    "{}",
                    json!({"event":"calls_record_failed","error":failure.report()})
                );
            }
        }
    }

    fn observe(&mut self, header: &Header) {
        self.provider_tip = Some(ProviderTip {
            height: header.height,
            hash: header.hash.to_string(),
            time: header.time,
            observed_at: self.now(),
        });
    }

    async fn latest(&mut self) -> Result<Header, Error> {
        self.requests.count("headers");
        let latest = guarded(&self.stop, self.provider.latest_block()).await?;
        self.observe(&latest);
        self.head_read_at = Some(self.now());
        Ok(latest)
    }

    /// At the tip, when nothing follows the stored tip: reads the provider's
    /// own head once a minute, so that its age shows a provider whose chain
    /// stands still. A head above the stored tip on two such reads in a row
    /// means that the stored tip left the provider's chain although its
    /// `/next` stays empty: the head is returned, and the fork search runs.
    async fn head_at_tip(&mut self, tip: u32) -> Result<Option<Header>, Error> {
        let now = self.now();
        if self
            .head_read_at
            .is_some_and(|at| now.saturating_sub(at) < HEAD_INTERVAL.as_secs())
        {
            // The provider answered; its head is the one read last.
            if let Some(head) = self.provider_tip.as_mut() {
                head.observed_at = now;
            }
            return Ok(None);
        }
        let head = self.latest().await?;
        if head.height <= tip {
            self.head_above = 0;
            return Ok(None);
        }
        self.head_above += 1;
        if self.head_above < 2 {
            return Ok(None);
        }
        self.head_above = 0;
        eprintln!(
            "{}",
            json!({"event":"next_empty_below_head","tip":tip,"provider_head":head.height})
        );
        Ok(Some(head))
    }

    /// The hash the provider returns for `height`, if any.
    async fn hash_at(&mut self, height: u32) -> Result<Option<String>, Error> {
        self.requests.count("headers");
        Ok(guarded(&self.stop, self.provider.block_at(height))
            .await?
            .map(|header| header.hash.to_string()))
    }

    /// The provider serves the profile's network: its `/genesis` names the
    /// network's magic. A provider of another network contradicts the stored
    /// chain without anything being wrong with it, so the run ends with
    /// `provider_network_mismatch` and the gate untouched. A `/genesis` that
    /// cannot be read is retried as any other request.
    async fn check_network(&mut self) -> Result<(), Error> {
        let Some(expected) = network_magic(&self.profile.network) else {
            return Ok(());
        };
        self.requests.count("genesis");
        let genesis = guarded(&self.stop, self.provider.genesis()).await?;
        if genesis.network_magic != expected {
            return Err(Error::Refusal {
                code: "provider_network_mismatch",
                detail: json!({"network":self.profile.network,"expected_magic":expected,
                    "received_magic":genesis.network_magic}),
            });
        }
        Ok(())
    }

    /// One iteration: poll, then at most one update. The first one checks
    /// the provider's network.
    async fn step(&mut self) -> Result<Outcome, Error> {
        if !self.network_checked {
            self.check_network().await?;
            self.network_checked = true;
        }
        self.connect_if_needed().await?;
        if self.provider_tip.is_none() {
            self.latest().await?;
        }
        let target = self.stored.target.height;
        let (tip, tip_hash) = {
            let row = self.stored.tip();
            (row.height, row.hash.clone())
        };
        self.requests.count("headers");
        let page = guarded(
            &self.stop,
            self.provider
                .next_blocks(&hash32(&tip_hash)?, Page::first()),
        )
        .await?;
        let linked = page.as_ref().is_some_and(|rows| {
            rows.is_empty() || first_break(tip, &tip_hash, &rows[..1]).is_none()
        });
        let rows = page.filter(|_| linked);
        let (fork_point, fetched) = match rows {
            Some(rows) if !rows.is_empty() => {
                self.head_above = 0;
                if let Some(index) = first_break(tip, &tip_hash, &rows) {
                    return Err(Error::answer(
                        "provider_header_link_mismatch",
                        json!({"height":rows[index].height,"after":tip_hash}),
                    ));
                }
                if rows.len() >= PAGE_SIZE {
                    self.latest().await?;
                } else if let Some(last) = rows.last() {
                    self.observe(last);
                }
                (tip, rows)
            }
            rows => {
                let latest = match rows {
                    // Nothing follows the stored tip.
                    Some(_) => match self.head_at_tip(tip).await? {
                        Some(head) => head,
                        None => return Ok(Outcome::AtTip),
                    },
                    // The stored tip left the provider's chain.
                    None => self.latest().await?,
                };
                if latest.height < tip {
                    return Ok(Outcome::Wait {
                        reason: "provider_chain_shorter",
                        until: self.now() + self.settings.poll_interval.as_secs(),
                    });
                }
                let fork_point = self.find_fork().await?;
                let until = latest
                    .height
                    .min(tip.saturating_add(self.settings.limits.blocks));
                let rows = self.headers_from(fork_point, until).await?;
                (fork_point, rows)
            }
        };
        let new = fetched.iter().map(block_row).collect::<Vec<_>>();
        let last = fork_point + new.len() as u32;
        if last < tip {
            return Ok(Outcome::Wait {
                reason: "provider_chain_shorter",
                until: self.now() + self.settings.poll_interval.as_secs(),
            });
        }
        // The headers after the replacement, above the target.
        let mut chain = self
            .stored
            .above
            .range(..=fork_point)
            .map(|(height, row)| (*height, row.clone()))
            .collect::<BTreeMap<_, _>>();
        chain.extend(new.into_iter().map(|row| (row.height, row)));
        let header_at = |height: u32| -> BlockRow { chain[&height].clone() };
        let mut blocks = (last - tip).min(self.settings.limits.blocks);
        if let Some(stop_at) = self.settings.stop_at_target {
            blocks = blocks.min(stop_at - target);
        }
        let mut finals = (target + 1..=target + blocks)
            .map(header_at)
            .collect::<Vec<_>>();
        let (mut params, pending) = self.epoch_parameters(&mut finals).await?;
        let built = if finals.is_empty() {
            block::Built::default()
        } else {
            let context = block::Context {
                provider: &self.provider,
                requests: &self.requests,
                selection: &self.selection,
                store: self.store.as_ref().expect("connected"),
                stop: &self.stop,
                max_in_flight: self.settings.max_in_flight,
            };
            block::build(&context, &finals, &self.settings.limits).await?
        };
        let blocks = built.blocks.len() as u32;
        params.retain(|param| {
            finals[..blocks as usize]
                .iter()
                .any(|row| row.epoch == param.epoch)
        });
        let wait = pending.then(|| Outcome::Wait {
            reason: "epoch_parameters_pending",
            until: self.now() + RECHECK.as_secs(),
        });
        if blocks == 0 && fork_point == tip {
            return Ok(wait.unwrap_or(Outcome::AtTip));
        }
        let headers = (fork_point + 1..=tip + blocks)
            .map(header_at)
            .collect::<Vec<_>>();
        let new_target = if blocks == 0 {
            self.stored.target.clone()
        } else {
            header_at(target + blocks)
        };
        let above = (target + blocks + 1..=tip + blocks)
            .map(|height| (height, header_at(height)))
            .collect::<BTreeMap<_, _>>();
        let now = self.now();
        let prune_due = !self.settings.building
            && self
                .last_prune_check
                .is_none_or(|at| now.saturating_sub(at) >= self.settings.prune_interval.as_secs());
        let store = self.store.as_mut().expect("connected");
        let prune_to = if prune_due {
            let candidate = store.horizon_candidate().await?;
            (candidate > self.stored.horizon && candidate <= new_target.height).then_some(candidate)
        } else {
            None
        };
        let calls = self.provider.counts();
        let began = Instant::now();
        let report = store
            .apply_update(
                self.profile,
                &Update {
                    target: &HeaderRef {
                        height: target,
                        hash: self.stored.target.hash.clone(),
                    },
                    tip: &HeaderRef {
                        height: tip,
                        hash: tip_hash,
                    },
                    fork_point,
                    headers: &headers,
                    blocks: &built.blocks,
                    epoch_params: &params,
                    mint_delta: &built.mint_delta,
                    calls: &calls,
                    observations: &built.observations,
                    prune_to,
                },
            )
            .await?;
        if prune_due {
            self.last_prune_check = Some(now);
        }
        self.stored = Stored {
            target: new_target,
            above,
            horizon: report.horizon,
        };
        self.last_counts = now;
        self.updates += 1;
        self.sample(now);
        if self
            .last_update_log
            .is_none_or(|at| at.elapsed() >= UPDATE_LOG_INTERVAL)
        {
            self.last_update_log = Some(Instant::now());
            eprintln!(
                "{}",
                json!({"event":"update","target":report.target,"tip":report.tip,"horizon":report.horizon,
                    "blocks":report.blocks,"headers":report.headers,"cbor_bytes":built.cbor_bytes,"rows":built.rows,
                    "seconds":began.elapsed().as_secs_f64()})
            );
        }
        self.progress
            .send_modify(|progress| progress.last_update = Some(report));
        let more = self
            .provider_tip
            .as_ref()
            .is_some_and(|provider| provider.height > tip + blocks);
        Ok(wait.unwrap_or(if more { Outcome::More } else { Outcome::AtTip }))
    }

    /// The parameters of the epochs of `finals` that the store does not hold.
    /// When the provider does not have them yet, `finals` ends before the
    /// first block of that epoch and the second value is `true`.
    async fn epoch_parameters(
        &mut self,
        finals: &mut Vec<BlockRow>,
    ) -> Result<(Vec<EpochParamRow>, bool), Error> {
        let mut epochs = finals.iter().map(|row| row.epoch).collect::<Vec<_>>();
        epochs.dedup();
        if epochs.is_empty() {
            return Ok((Vec::new(), false));
        }
        let stored = self
            .store
            .as_ref()
            .expect("connected")
            .stored_epoch_params(&epochs)
            .await?;
        let mut params = Vec::new();
        for epoch in epochs.into_iter().filter(|epoch| !stored.contains(epoch)) {
            let answer = guarded(
                &self.stop,
                epoch::parameters(&self.provider, &self.requests, epoch),
            )
            .await?;
            match answer {
                Some(row) => params.push(row),
                None => {
                    finals.retain(|row| row.epoch < epoch);
                    return Ok((params, true));
                }
            }
        }
        Ok((params, false))
    }

    /// The highest stored height at or above the target whose hash the
    /// provider still returns. The target itself must be on the provider's
    /// chain: another hash is read again after a minute, and another hash on
    /// both reads closes the gate. A missing answer at the target's height,
    /// while the provider's head is not below the stored tip, is an answer
    /// that contradicts the chain: nothing is written and the step is tried
    /// again.
    async fn find_fork(&mut self) -> Result<u32, Error> {
        let target = self.stored.target.clone();
        let missing = || {
            Error::answer(
                "provider_target_missing",
                json!({"height":target.height,"stored":target.hash}),
            )
        };
        let first = self.hash_at(target.height).await?.ok_or_else(missing)?;
        if first != target.hash {
            let until = self.now() + RECHECK.as_secs();
            self.set_state(State::Waiting, "target_contradicted", Some(until), None)
                .await;
            if !pause_until(&*self.time, &self.stop, until).await {
                return Err(Error::Stopped);
            }
            let second = self.hash_at(target.height).await?.ok_or_else(missing)?;
            if second != target.hash {
                // Only a provider of this network can contradict a final
                // block.
                self.check_network().await?;
                return Err(Error::Check {
                    code: "canonical_conflict",
                    detail: json!({"height":target.height,"stored":target.hash,"provider":[first,second]}),
                });
            }
        }
        let (mut low, mut high) = (target.height, self.stored.tip().height);
        while low < high {
            let middle = low + (high - low).div_ceil(2);
            let found = self.hash_at(middle).await?;
            if found.as_deref() == Some(self.stored.above[&middle].hash.as_str()) {
                low = middle;
            } else {
                high = middle - 1;
            }
        }
        Ok(low)
    }

    /// The provider's headers after the stored height `from`, up to `until`.
    async fn headers_from(&mut self, from: u32, until: u32) -> Result<Vec<Header>, Error> {
        let mut height = from;
        let mut hash = if from == self.stored.target.height {
            self.stored.target.hash.clone()
        } else {
            self.stored.above[&from].hash.clone()
        };
        let mut headers = Vec::new();
        loop {
            self.requests.count("headers");
            let rows = guarded(
                &self.stop,
                self.provider.next_blocks(&hash32(&hash)?, Page::first()),
            )
            .await?
            .ok_or_else(|| {
                Error::answer(
                    "provider_fork_point_unknown",
                    json!({"height":height,"hash":hash}),
                )
            })?;
            if let Some(index) = first_break(height, &hash, &rows) {
                return Err(Error::answer(
                    "provider_header_link_mismatch",
                    json!({"height":rows[index].height,"after":hash}),
                ));
            }
            let full = rows.len() >= PAGE_SIZE;
            for row in rows {
                if row.height > until {
                    break;
                }
                height = row.height;
                hash = row.hash.to_string();
                headers.push(row);
            }
            if !full || height >= until {
                return Ok(headers);
            }
        }
    }

    /// Stores a finished stake read between updates; a failed read is logged
    /// and tried again later.
    async fn collect_stake(&mut self) {
        if !self
            .stake
            .task
            .as_ref()
            .is_some_and(|(_, task)| task.is_finished())
        {
            return;
        }
        let (epoch, task) = self.stake.task.take().expect("a finished task");
        let result = task.await;
        self.stake_result(epoch, result).await;
    }

    async fn stake_result(
        &mut self,
        epoch: u32,
        result: Result<Result<Distribution, StakeError>, tokio::task::JoinError>,
    ) {
        let now = self.now();
        let (failure, refusal) = match result {
            Ok(Ok(distribution)) => {
                let Some(store) = self.store.as_mut() else {
                    // Read again once the connection is back.
                    self.stake.retries.entry(epoch).or_default().at = now;
                    return;
                };
                match store.put_epoch_stake(epoch, &distribution.rows()).await {
                    Ok(_) | Err(dsa_store::Error::Refusal("stake_epoch_below_window")) => {
                        self.stake.retries.remove(&epoch);
                        eprintln!(
                            "{}",
                            json!({"event":"stake_stored","epoch":epoch,"method":distribution.method,
                                "pools":distribution.pools.len(),"active_stake":distribution.active_stake.to_string()})
                        );
                        self.progress.send_modify(|progress| {
                            if progress
                                .stake_failure
                                .as_ref()
                                .is_some_and(|failure| failure["epoch"] == epoch)
                            {
                                progress.stake_failure = None;
                            }
                        });
                        return;
                    }
                    Err(error) => {
                        if error.temporary() {
                            self.store = None;
                        }
                        (error.report(), false)
                    }
                }
            }
            Ok(Err(error)) => (error.report(), error.refusal()),
            Err(_) => (json!({"error":"stake_task_failed"}), false),
        };
        let retry = self.stake.retries.entry(epoch).or_default();
        let wait = if refusal {
            STAKE_REFUSED_BACKOFF[retry.failures.min(STAKE_REFUSED_BACKOFF.len() - 1)]
        } else {
            crate::retry::BACKOFF[retry.failures.min(crate::retry::BACKOFF.len() - 1)]
        };
        retry.failures += 1;
        retry.at = now + wait;
        let report = json!({"epoch":epoch,"at":now,"retry_at":retry.at,"failures":retry.failures,
            "error":failure});
        eprintln!(
            "{}",
            json!({"event":"stake_read_failed","epoch":epoch,"error":report["error"],
                "retry_at":retry.at,"failures":retry.failures})
        );
        self.progress
            .send_modify(|progress| progress.stake_failure = Some(report));
    }

    /// Starts the read of the oldest epoch up to the target's that has no
    /// distribution and whose own retry time has passed, unless a read runs.
    /// At the tip it notes since when the target's epoch has no stake.
    async fn start_stake(&mut self) -> StakeStatus {
        let now = self.now();
        if self.stake.task.is_some() || now < self.stake.window_retry_at {
            return StakeStatus::Busy;
        }
        let Some(store) = self.store.as_ref() else {
            return StakeStatus::Busy;
        };
        let through = self.stored.target.epoch;
        let window = match store.stake_window(through).await {
            Ok(window) => window,
            Err(error) => {
                if error.temporary() {
                    self.store = None;
                }
                self.stake.window_retry_at = now + self.stake.window_failures.failed(now).as_secs();
                return StakeStatus::Busy;
            }
        };
        self.stake.window_failures.reset();
        self.stake
            .retries
            .retain(|epoch, _| window.missing.contains(epoch));
        let missing = window.missing.contains(&through).then(|| {
            self.progress
                .borrow()
                .stake_missing
                .filter(|missing| missing.epoch == through)
                .unwrap_or(StakeMissing {
                    epoch: through,
                    since: now,
                })
        });
        self.progress
            .send_modify(|progress| progress.stake_missing = missing);
        if window.missing.is_empty() {
            return StakeStatus::Done;
        }
        let due = window.missing.iter().copied().find(|epoch| {
            self.stake
                .retries
                .get(epoch)
                .is_none_or(|retry| retry.at <= now)
        });
        let Some(first) = due else {
            return StakeStatus::Busy;
        };
        let provider = self.provider.clone();
        let requests = self.requests.clone();
        let network = self.profile.network.clone();
        let waits = epoch::Waits {
            time: Arc::clone(&self.time),
            stop: self.stop.clone(),
        };
        let task = tokio::spawn(async move {
            epoch::read_stake(&provider, &requests, &waits, first, &network).await
        });
        self.stake.task = Some((first, task));
        StakeStatus::Busy
    }

    /// When the next stake read may start: the window's retry, otherwise the
    /// earliest retry of an epoch.
    fn next_stake_attempt(&self) -> u64 {
        let now = self.now();
        if now < self.stake.window_retry_at {
            return self.stake.window_retry_at;
        }
        self.stake
            .retries
            .values()
            .map(|retry| retry.at)
            .min()
            .unwrap_or(now)
            .max(now)
    }

    /// Reads and stores the stake of every epoch up to the target's.
    async fn complete_stake(&mut self) -> Result<(), Error> {
        loop {
            if self.store.is_none()
                && let Err(error) = self.connect_if_needed().await
            {
                let until = self.failed(error).await?;
                if !pause_until(&*self.time, &self.stop, until).await {
                    return Err(Error::Stopped);
                }
                continue;
            }
            if let StakeStatus::Done = self.start_stake().await {
                return Ok(());
            }
            match self.stake.task.take() {
                Some((epoch, mut task)) => {
                    let result = tokio::select! {
                        result = &mut task => result,
                        () = self.stop.wait() => {
                            task.abort();
                            return Err(Error::Stopped);
                        }
                    };
                    self.stake_result(epoch, result).await;
                }
                None => {
                    let until = self.next_stake_attempt().max(self.now() + 1);
                    self.set_state(State::Waiting, "stake_pending", Some(until), None)
                        .await;
                    if !pause_until(&*self.time, &self.stop, until).await {
                        return Err(Error::Stopped);
                    }
                }
            }
        }
    }
}
