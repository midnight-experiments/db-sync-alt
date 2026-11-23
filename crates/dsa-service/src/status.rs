//! The status document of `/status`, `/health` and `db-sync-alt status`. It
//! is a pure function of what the database says about itself
//! ([`DatabaseView`]), what only a running follower knows ([`FollowerView`])
//! and the settings, at a given time. Without a follower the document never
//! claims `following`, so `/health` is never 200 from the database alone.

use std::{collections::BTreeMap, sync::Arc};

use chrono::{DateTime, Days, NaiveDate};
use dsa_chain::{capabilities::NodeCapabilities, observation::Shape, profiles::NODE_RELEASE};
use dsa_provider::Refusal;
use dsa_store::{
    catalogue::Outside,
    service_view::{
        DatabaseView, DayCalls, FirstObservation, Identity, Point, ReaderCheck, Recorded,
        StoredPosition, StoredState,
    },
    upkeep::{Rebuilt, SnapshotFile, SupplyCheck},
};
use serde::Serialize;
use serde_json::Value;

use crate::follow::ProviderTip;

/// `/health` fails when the provider's head block is older than this
/// (seconds): Cardano makes a block every 20 seconds on average.
pub const PROVIDER_HEAD_MAX_AGE_SECONDS: u64 = 600;

/// The thresholds of `/health`, and the network the service runs for.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Settings {
    #[serde(skip)]
    pub network: String,
    /// The served target may be this many blocks behind the provider's tip
    /// minus the security parameter.
    pub max_lag_blocks: u32,
    /// The provider's tip must have been read this recently (seconds).
    pub provider_observation_max_age_seconds: u64,
    /// The database view must be this recent (seconds).
    pub database_observation_max_age_seconds: u64,
}

impl Settings {
    pub fn new(network: impl Into<String>) -> Self {
        Self {
            network: network.into(),
            max_lag_blocks: 9,
            provider_observation_max_age_seconds: 120,
            database_observation_max_age_seconds: 120,
        }
    }
}

/// What the service is doing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Restoring,
    CatchingUp,
    Following,
    Waiting,
    Stopped,
}

impl State {
    fn reason_code(self) -> &'static str {
        match self {
            Self::Restoring => "state_restoring",
            Self::CatchingUp => "state_catching_up",
            Self::Following => "state_following",
            Self::Waiting => "state_waiting",
            Self::Stopped => "state_stopped",
        }
    }
}

impl From<crate::follow::State> for State {
    fn from(state: crate::follow::State) -> Self {
        use crate::follow::State as Loop;
        match state {
            Loop::CatchingUp => Self::CatchingUp,
            Loop::Following => Self::Following,
            Loop::Waiting => Self::Waiting,
            Loop::Stopped => Self::Stopped,
        }
    }
}

/// What only a running follower knows. The fields carry the names and types
/// of `follow::Progress`, so one is copied into the other field by field.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FollowerView {
    pub state: State,
    pub reason: String,
    /// When the current wait ends (Unix seconds).
    pub wait_until: Option<u64>,
    pub provider_tip: Option<ProviderTip>,
    pub requests_per_second: f64,
    pub requests_in_flight: usize,
    pub last_refusal: Option<Refusal>,
    pub blocks_remaining: Option<u64>,
    pub blocks_per_hour: Option<f64>,
}

impl From<&crate::follow::Progress> for FollowerView {
    fn from(progress: &crate::follow::Progress) -> Self {
        Self {
            state: progress.state.into(),
            reason: progress.reason.clone(),
            wait_until: progress.wait_until,
            provider_tip: progress.provider_tip.clone(),
            requests_per_second: progress.requests_per_second,
            requests_in_flight: progress.requests_in_flight,
            last_refusal: progress.last_refusal.clone(),
            blocks_remaining: progress.blocks_remaining,
            blocks_per_hour: progress.blocks_per_hour,
        }
    }
}

/// Whether the node reads data: the gate of the database.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Serving {
    /// The gate is open.
    Ready,
    /// The database has not served yet.
    Unready,
    /// A check closed the gate of a database that served.
    Revoked,
    /// No database view yet.
    Unknown,
}

/// The follower's activity in the words of earlier releases.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Sync {
    Unknown,
    Idle,
    Syncing,
    Backoff,
    QuotaWait,
    Halted,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Lag {
    pub blocks: Option<u64>,
    pub seconds: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Freshness {
    pub provider_observation_age_seconds: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Progress {
    pub blocks_remaining: Option<u64>,
    pub blocks_per_hour: Option<f64>,
    /// When the database is expected at the tip (Unix seconds).
    pub estimated_tip_at: Option<u64>,
}

/// The last update as recorded with it.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct LastUpdate {
    pub at: Option<i64>,
    pub duration_seconds: f64,
    /// Blocks made final.
    pub blocks: u64,
    /// Headers stored.
    pub headers: u64,
    /// Rows written to the event tables.
    pub events_written: u64,
    /// Rows deleted: the horizon advance and replaced headers.
    pub rows_pruned: u64,
    pub target: Option<u64>,
    pub horizon: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct LastError {
    pub code: Option<String>,
    pub at: Option<i64>,
    pub detail: Value,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Retention {
    pub seconds: Option<i64>,
    pub horizon_first_served_at: Option<i64>,
    /// The earliest time the horizon may move (Unix seconds).
    pub next_horizon_move_at: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Provider {
    pub utc_day: NaiveDate,
    pub calls_today: Option<i64>,
    /// The seven UTC days before today, newest first.
    pub calls_previous_days: Vec<DayCalls>,
    pub pace_requests_per_second: Option<f64>,
    pub requests_in_flight: Option<usize>,
    /// The provider's last refusal of every request (402, 418, 429), with
    /// the time its wait ends.
    pub last_refusal: Option<Refusal>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Epochs {
    pub current_epoch: Option<u32>,
    pub newest_parameters_epoch: Option<u32>,
    pub newest_stake_epoch: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Integrity {
    /// Every stored unspent output, as counted by the last supply check.
    pub unspent_outputs: Option<i64>,
    pub supply_check: Option<Recorded<SupplyCheck>>,
    pub snapshot: Option<Recorded<SnapshotFile>>,
    /// Stored unspent outputs per profile address, kept by every update.
    pub unspent_at_profile_addresses: BTreeMap<String, i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Database {
    pub observed_at: Option<i64>,
    pub observation_age_seconds: Option<u64>,
    pub size_bytes: Option<i64>,
    /// Whether the server has TLS enabled.
    pub tls_available: Option<bool>,
    pub reader_check: Option<ReaderCheck>,
    /// Each index's size after its last rebuild.
    pub index_rebuilds: BTreeMap<String, Rebuilt>,
}

/// A stated limit of what a pruned database answers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Limitation {
    pub code: &'static str,
    pub description: &'static str,
}

pub const LIMITATIONS: [Limitation; 3] = [
    Limitation {
        code: "registered_candidates_unsupported",
        description: "The registered-candidate results of the node's operator RPCs and CLI commands, including the registrations of the Ariadne-parameter calls, are not supported. Block import and production use the permissioned candidates, which are served in full.",
    },
    Limitation {
        code: "genesis_subcommands_incomplete",
        description: "The node's genesis subcommands read history that a pruned database does not keep; their statements are listed in unsupported_node_statements.",
    },
    Limitation {
        code: "history_below_horizon",
        description: "Events below the horizon are not served. A node whose position is below the horizon starts from a newer node snapshot.",
    },
];

/// The status document.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Status {
    pub service_version: &'static str,
    pub network: String,
    /// The node release the keep rule is bound to.
    pub node_release: &'static str,
    pub database_format: i32,
    pub keep_rule_version: i32,
    /// When the document was composed (Unix seconds).
    pub generated_at: u64,
    pub healthy: bool,
    pub reason_codes: Vec<&'static str>,
    pub state: State,
    pub state_reason: Option<String>,
    /// When the current wait or retry ends (Unix seconds).
    pub wait_until: Option<u64>,
    pub serving: Serving,
    pub sync: Sync,
    /// The condition that keeps the service from following.
    pub error_code: Option<String>,
    /// The provider's refusal while its wait lasts.
    pub poll_error_code: Option<&'static str>,
    pub provider_head: Option<Point>,
    pub provider_observed_at: Option<u64>,
    pub freshness: Freshness,
    /// The newest stored header.
    pub stored_tip: Option<Point>,
    /// The newest block whose events are served: the target.
    pub served_target: Option<Point>,
    pub horizon: Option<Point>,
    /// The provider's tip ahead of the stored tip: how far the served target
    /// is behind the provider's tip minus the security parameter.
    pub lag: Lag,
    /// The provider's tip ahead of the served target.
    pub tip_lag: Lag,
    pub progress: Progress,
    pub last_update: Option<LastUpdate>,
    pub last_error: Option<LastError>,
    pub retention: Retention,
    pub provider: Provider,
    pub epochs: Epochs,
    pub integrity: Integrity,
    pub database: Database,
    pub identity: Option<Identity>,
    pub position: Option<StoredPosition>,
    pub service_state: Option<StoredState>,
    /// The first stored observation of each rare shape, `null` until one is
    /// stored.
    pub first_observations: BTreeMap<&'static str, Option<FirstObservation>>,
    pub capabilities: NodeCapabilities,
    pub limitations: Vec<Limitation>,
    pub unsupported_node_statements: Vec<&'static Outside>,
    pub warnings: Vec<String>,
    pub health: Settings,
}

fn age(now: u64, then: i64) -> u64 {
    now.saturating_sub(u64::try_from(then).unwrap_or(0))
}

fn utc_day(now: u64) -> NaiveDate {
    DateTime::from_timestamp(i64::try_from(now).unwrap_or(i64::MAX), 0)
        .unwrap_or_default()
        .date_naive()
}

fn ahead(head: &Point, of: &Point) -> Lag {
    Lag {
        blocks: Some(u64::from(head.height.saturating_sub(of.height))),
        seconds: Some((head.timestamp - of.timestamp).max(0)),
    }
}

fn number(value: &Value, key: &str) -> Option<u64> {
    value.get(key).and_then(Value::as_u64)
}

fn seconds(value: &Value, key: &str) -> f64 {
    value.get(key).and_then(Value::as_f64).unwrap_or(0.0)
}

fn sum(value: &Value, key: &str, skip: &[&str]) -> u64 {
    value
        .get(key)
        .and_then(Value::as_object)
        .map(|rows| {
            rows.iter()
                .filter(|(table, _)| !skip.contains(&table.as_str()))
                .filter_map(|(_, count)| count.as_u64())
                .sum()
        })
        .unwrap_or(0)
}

fn last_update(stored: &StoredState) -> Option<LastUpdate> {
    let report = stored.last_update.as_ref()?;
    Some(LastUpdate {
        at: stored.last_update_at,
        duration_seconds: seconds(report, "insert_seconds")
            + seconds(report, "advance_seconds")
            + seconds(report, "keep_reapplied_seconds"),
        blocks: number(report, "blocks").unwrap_or(0),
        headers: number(report, "headers").unwrap_or(0),
        events_written: sum(report, "inserted", &["block", "first_served"]),
        rows_pruned: sum(report, "deleted", &[]),
        target: number(report, "target"),
        horizon: number(report, "horizon"),
    })
}

fn last_error(stored: &StoredState) -> Option<LastError> {
    let error = stored.last_error.as_ref()?;
    Some(LastError {
        code: error
            .get("error")
            .and_then(Value::as_str)
            .map(str::to_owned),
        at: stored.last_error_at,
        detail: error.clone(),
    })
}

/// The reasons `/health` is not 200, each its own code; none means healthy.
pub fn health(status: &Status) -> (bool, Vec<&'static str>) {
    let settings = &status.health;
    let mut reasons = Vec::new();
    match status.serving {
        Serving::Ready => {}
        Serving::Unready => reasons.push("serving_unready"),
        Serving::Revoked => reasons.push("serving_revoked"),
        Serving::Unknown => reasons.push("serving_unknown"),
    }
    if status
        .database
        .observation_age_seconds
        .is_some_and(|age| age > settings.database_observation_max_age_seconds)
    {
        reasons.push("database_observation_stale");
    }
    if status.state != State::Following {
        reasons.push(status.state.reason_code());
    }
    match status.lag.blocks {
        None => reasons.push("serving_block_lag_unknown"),
        Some(blocks) if blocks > u64::from(settings.max_lag_blocks) => {
            reasons.push("serving_block_lag_exceeded")
        }
        Some(_) => {}
    }
    match status.freshness.provider_observation_age_seconds {
        None => reasons.push("provider_observation_unknown"),
        Some(age) if age > settings.provider_observation_max_age_seconds => {
            reasons.push("provider_observation_stale")
        }
        Some(_) => {}
    }
    if status.provider_head.as_ref().is_some_and(|head| {
        status
            .generated_at
            .saturating_sub(u64::try_from(head.timestamp).unwrap_or(0))
            > PROVIDER_HEAD_MAX_AGE_SECONDS
    }) {
        reasons.push("provider_head_stale");
    }
    match &status.database.reader_check {
        None => reasons.push("reader_check_unknown"),
        Some(check) if !check.passed => reasons.push("reader_check_failed"),
        Some(_) => {}
    }
    (reasons.is_empty(), reasons)
}

/// The document at `now` (Unix seconds). `database` is the last view read,
/// `None` before the first; `follower` is `None` when no follower runs.
pub fn compose(
    database: Option<&DatabaseView>,
    follower: Option<&FollowerView>,
    settings: &Settings,
    now: u64,
) -> Status {
    let stored = database.and_then(|view| view.service_state.as_ref());
    let (state, state_reason, wait_until) = match follower {
        Some(follower) => (
            follower.state,
            Some(follower.reason.clone()).filter(|reason| !reason.is_empty()),
            follower.wait_until,
        ),
        // A stored state other than a stop says what a process was doing
        // when it last wrote; without one running, the service is stopped.
        None => match stored {
            Some(stored) if stored.state == "stopped" => {
                (State::Stopped, Some(stored.reason.clone()), None)
            }
            _ => (State::Stopped, Some("service_not_running".to_owned()), None),
        },
    };
    let error_code = match state {
        State::Waiting => state_reason.clone(),
        State::Stopped => state_reason
            .clone()
            .filter(|reason| !matches!(reason.as_str(), "stop_requested" | "service_not_running")),
        _ => None,
    };
    let serving = match database {
        None => Serving::Unknown,
        Some(view) if view.position.serving => Serving::Ready,
        Some(view) if view.target.is_some() => Serving::Revoked,
        Some(_) => Serving::Unready,
    };
    let sync = match follower.map(|follower| follower.state) {
        None => Sync::Unknown,
        Some(State::Following) => Sync::Idle,
        Some(State::CatchingUp | State::Restoring) => Sync::Syncing,
        Some(State::Waiting) if state_reason.as_deref() == Some("quota_exhausted") => {
            Sync::QuotaWait
        }
        Some(State::Waiting) => Sync::Backoff,
        Some(State::Stopped) => Sync::Halted,
    };
    let tip = follower.and_then(|follower| follower.provider_tip.as_ref());
    let provider_head = tip.map(|tip| Point {
        height: tip.height,
        hash: tip.hash.clone(),
        timestamp: i64::try_from(tip.time).unwrap_or(i64::MAX),
    });
    let stored_tip = database.and_then(|view| view.tip.clone());
    let served_target = database.and_then(|view| view.target.clone());
    let lag = match (&provider_head, &stored_tip) {
        (Some(head), Some(tip)) => ahead(head, tip),
        _ => Lag::default(),
    };
    let tip_lag = match (&provider_head, &served_target) {
        (Some(head), Some(target)) => ahead(head, target),
        _ => Lag::default(),
    };
    let blocks_remaining = follower
        .and_then(|follower| follower.blocks_remaining)
        .or(lag.blocks);
    let blocks_per_hour = follower.and_then(|follower| follower.blocks_per_hour);
    let estimated_tip_at = match (blocks_remaining, blocks_per_hour) {
        (Some(0), _) => Some(now),
        (Some(remaining), Some(rate)) if rate > 0.0 => {
            Some(now + (remaining as f64 / rate * 3600.0).ceil() as u64)
        }
        _ => None,
    };
    let today = utc_day(now);
    let calls = |day: NaiveDate| {
        database.map(|view| {
            view.calls
                .iter()
                .find(|calls| calls.day == day)
                .map_or(0, |calls| calls.requests)
        })
    };
    let calls_previous_days = match database {
        None => Vec::new(),
        Some(_) => (1..=7)
            .filter_map(|days| today.checked_sub_days(Days::new(days)))
            .map(|day| DayCalls {
                day,
                requests: calls(day).unwrap_or(0),
            })
            .collect(),
    };
    let last_refusal = follower.and_then(|follower| follower.last_refusal.clone());
    let poll_error_code = last_refusal
        .as_ref()
        .filter(|refusal| refusal.until > now)
        .map(|refusal| refusal.code);
    let supply_check = database.and_then(|view| view.upkeep.supply.clone());
    let reader_check = database.and_then(|view| view.reader_check.clone());
    let mut warnings = reader_check
        .as_ref()
        .map(|check| check.warnings.clone())
        .unwrap_or_default();
    if database.is_some_and(|view| !view.tls_available) {
        warnings.push("database_tls_unavailable".to_owned());
    }
    let mut status = Status {
        service_version: dsa_chain::VERSION,
        network: settings.network.clone(),
        node_release: NODE_RELEASE,
        database_format: dsa_store::DATABASE_FORMAT,
        keep_rule_version: dsa_store::KEEP_RULE_VERSION,
        generated_at: now,
        healthy: false,
        reason_codes: Vec::new(),
        state,
        state_reason,
        wait_until,
        serving,
        sync,
        error_code,
        poll_error_code,
        provider_observed_at: tip.map(|tip| tip.observed_at),
        freshness: Freshness {
            provider_observation_age_seconds: tip.map(|tip| now.saturating_sub(tip.observed_at)),
        },
        provider_head,
        stored_tip,
        served_target,
        horizon: database.and_then(|view| view.horizon.clone()),
        lag,
        tip_lag,
        progress: Progress {
            blocks_remaining,
            blocks_per_hour,
            estimated_tip_at,
        },
        last_update: stored.and_then(last_update),
        last_error: stored.and_then(last_error),
        retention: Retention {
            seconds: database.map(|view| view.position.retention_seconds),
            horizon_first_served_at: database.and_then(|view| view.horizon_first_served_at),
            next_horizon_move_at: database.and_then(|view| {
                view.horizon_first_served_at
                    .map(|at| at + view.position.retention_seconds)
            }),
        },
        provider: Provider {
            utc_day: today,
            calls_today: calls(today),
            calls_previous_days,
            pace_requests_per_second: follower.map(|follower| follower.requests_per_second),
            requests_in_flight: follower.map(|follower| follower.requests_in_flight),
            last_refusal,
        },
        epochs: Epochs {
            current_epoch: database.and_then(|view| view.current_epoch),
            newest_parameters_epoch: database.and_then(|view| view.newest_parameters_epoch),
            newest_stake_epoch: database.and_then(|view| view.newest_stake_epoch),
        },
        integrity: Integrity {
            unspent_outputs: supply_check
                .as_ref()
                .map(|check| check.record.unspent_outputs),
            supply_check,
            snapshot: database.and_then(|view| view.upkeep.snapshot.clone()),
            unspent_at_profile_addresses: database
                .map(|view| view.unspent.clone())
                .unwrap_or_default(),
        },
        database: Database {
            observed_at: database.map(|view| view.observed_at),
            observation_age_seconds: database.map(|view| age(now, view.observed_at)),
            size_bytes: database.map(|view| view.size_bytes),
            tls_available: database.map(|view| view.tls_available),
            reader_check,
            index_rebuilds: database
                .map(|view| view.upkeep.indexes.clone())
                .unwrap_or_default(),
        },
        identity: database.map(|view| view.identity.clone()),
        position: database.map(|view| view.position.clone()),
        service_state: stored.cloned(),
        first_observations: Shape::ALL
            .into_iter()
            .map(|shape| {
                let name = shape.as_str();
                let first = database.and_then(|view| {
                    view.first_observations
                        .iter()
                        .find(|observation| observation.shape == name)
                        .cloned()
                });
                (name, first)
            })
            .collect(),
        capabilities: NodeCapabilities::pinned(),
        limitations: LIMITATIONS.to_vec(),
        unsupported_node_statements: dsa_store::catalogue::unsupported().collect(),
        warnings,
        health: settings.clone(),
    };
    let (healthy, reasons) = health(&status);
    status.healthy = healthy;
    status.reason_codes = reasons;
    status
}

/// The rendered answers of `/status` and `/health`: one JSON body, and
/// whether `/health` answers 200.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Documents {
    pub healthy: bool,
    pub body: Arc<str>,
}

impl Documents {
    pub fn new(status: &Status) -> Self {
        Self {
            healthy: status.healthy,
            body: serde_json::to_string(status)
                .unwrap_or_else(|_| r#"{"error_code":"status_unavailable"}"#.to_owned())
                .into(),
        }
    }
}

/// Composes the document and renders both answers.
pub fn render(
    database: Option<&DatabaseView>,
    follower: Option<&FollowerView>,
    settings: &Settings,
    now: u64,
) -> Documents {
    Documents::new(&compose(database, follower, settings, now))
}
