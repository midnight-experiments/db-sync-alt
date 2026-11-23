//! The read behind the service's status document: what the database says
//! about itself, in one read-only snapshot with a statement limit of five
//! seconds. Every statement is index-bound, over a single-row table or over a
//! table bounded by the profile or the layout (FR-027); the number of
//! unspent outputs comes from the last supply check, never from a count.

use std::collections::BTreeMap;

use chrono::NaiveDate;
use dsa_chain::profiles::Profile;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_postgres::IsolationLevel;

use crate::{
    Error, Store,
    upkeep::{INDEX_PREFIX, Rebuilt, SNAPSHOT, SUPPLY, SnapshotFile, SupplyCheck},
};

/// The statement limit of the view's transaction.
pub const STATEMENT_LIMIT: &str = "5s";

/// A block by height, hash (lowercase hexadecimal) and time (Unix seconds).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Point {
    pub height: u32,
    pub hash: String,
    pub timestamp: i64,
}

/// The stored identity row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Identity {
    pub database_format: i32,
    pub network: String,
    pub profile_hash: String,
    pub keep_rule_version: i32,
    pub tool_version: String,
    pub node_release: String,
    pub origin: Value,
    pub created_at: String,
}

/// The stored position row, as PostgreSQL renders it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredPosition {
    pub serving: bool,
    pub reason: String,
    pub target_height: Option<i64>,
    pub target_hash: Option<String>,
    pub target_time: Option<String>,
    pub tip_height: Option<i64>,
    pub tip_hash: Option<String>,
    pub tip_time: Option<String>,
    pub horizon_height: Option<i64>,
    pub horizon_hash: Option<String>,
    pub horizon_time: Option<String>,
    pub retention_seconds: i64,
    pub updated_at: String,
}

/// The service's state as last recorded in `dsa.service_state`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredState {
    pub state: String,
    pub reason: String,
    pub detail: Value,
    /// The last update's report, with the time it committed as `at`.
    pub last_update: Option<Value>,
    pub last_error: Option<Value>,
    pub updated_at: i64,
    pub last_update_at: Option<i64>,
    pub last_error_at: Option<i64>,
}

/// The calls sent on one UTC day.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DayCalls {
    pub day: NaiveDate,
    pub requests: i64,
}

/// A provider answer kept with a first observation: its source, size and
/// SHA-256, which refer to the bytes kept in `dsa.first_observation`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Answer {
    pub source: String,
    pub bytes: i64,
    pub sha256: String,
}

/// The first stored observation of a rare shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FirstObservation {
    pub shape: String,
    pub block_height: u32,
    pub transaction: Option<String>,
    pub output_index: Option<i32>,
    pub datum_hash: Option<String>,
    pub raw_datum_bytes: Option<i32>,
    pub observed_at: i64,
    pub answers: Vec<Answer>,
}

/// An upkeep record with the time it was written (Unix seconds).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Recorded<T> {
    pub at: i64,
    #[serde(flatten)]
    pub record: T,
}

/// The records of `dsa.upkeep`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Upkeep {
    pub supply: Option<Recorded<SupplyCheck>>,
    pub snapshot: Option<Recorded<SnapshotFile>>,
    /// Each index's size after its last rebuild, by index name.
    pub indexes: BTreeMap<String, Rebuilt>,
}

/// Whether the node's reader login has exactly its rights.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReaderCheck {
    pub role: String,
    pub passed: bool,
    /// The refusal's code when the check failed.
    pub error: Option<String>,
    /// The rights the reader has beyond the node's relations.
    pub violations: Vec<String>,
    pub warnings: Vec<String>,
}

/// What one read of the database says about itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseView {
    /// When the view was read (Unix seconds, the database's clock).
    pub observed_at: i64,
    pub identity: Identity,
    pub position: StoredPosition,
    /// The newest block whose events are served.
    pub target: Option<Point>,
    /// The newest stored header.
    pub tip: Option<Point>,
    pub horizon: Option<Point>,
    /// When the horizon's block was first served; it may move once the
    /// retention has passed since.
    pub horizon_first_served_at: Option<i64>,
    /// The epoch of the newest stored header.
    pub current_epoch: Option<u32>,
    pub newest_parameters_epoch: Option<u32>,
    pub newest_stake_epoch: Option<u32>,
    pub service_state: Option<StoredState>,
    /// The calls of the last eight UTC days, oldest first.
    pub calls: Vec<DayCalls>,
    /// Stored unspent outputs per profile address.
    pub unspent: BTreeMap<String, i64>,
    pub first_observations: Vec<FirstObservation>,
    pub upkeep: Upkeep,
    pub size_bytes: i64,
    /// Whether the server has TLS enabled.
    pub tls_available: bool,
    /// `None` when no reader was named.
    pub reader_check: Option<ReaderCheck>,
}

#[derive(Deserialize)]
struct RawRecord {
    at: i64,
    detail: Value,
}

#[derive(Deserialize)]
struct Raw {
    observed_at: i64,
    identity: Identity,
    position: StoredPosition,
    target: Option<Point>,
    tip: Option<Point>,
    horizon: Option<Point>,
    horizon_first_served_at: Option<i64>,
    current_epoch: Option<u32>,
    newest_parameters_epoch: Option<u32>,
    newest_stake_epoch: Option<u32>,
    service_state: Option<StoredState>,
    calls: Vec<DayCalls>,
    unspent: BTreeMap<String, i64>,
    first_observations: Vec<FirstObservation>,
    upkeep: BTreeMap<String, RawRecord>,
    size_bytes: i64,
    tls_available: bool,
}

fn upkeep(records: BTreeMap<String, RawRecord>) -> Result<Upkeep, Error> {
    let mut upkeep = Upkeep::default();
    for (name, RawRecord { at, detail }) in records {
        let invalid = |_| Error::Refusal("invalid_upkeep_record");
        if name == SUPPLY {
            upkeep.supply = Some(Recorded {
                at,
                record: serde_json::from_value(detail).map_err(invalid)?,
            });
        } else if name == SNAPSHOT {
            upkeep.snapshot = Some(Recorded {
                at,
                record: serde_json::from_value(detail).map_err(invalid)?,
            });
        } else if let Some(index) = name.strip_prefix(INDEX_PREFIX) {
            let bytes = detail["bytes"]
                .as_i64()
                .ok_or(Error::Refusal("invalid_upkeep_record"))?;
            upkeep
                .indexes
                .insert(index.to_owned(), Rebuilt { at, bytes });
        }
    }
    Ok(upkeep)
}

async fn reader_check<C: tokio_postgres::GenericClient>(
    client: &C,
    role: &str,
) -> Result<ReaderCheck, Error> {
    let refused = |error: Option<String>, violations| ReaderCheck {
        role: role.to_owned(),
        passed: false,
        error,
        violations,
        warnings: Vec::new(),
    };
    match crate::verify_reader(client, role).await {
        Ok(report) => Ok(ReaderCheck {
            role: role.to_owned(),
            passed: true,
            error: None,
            violations: Vec::new(),
            warnings: serde_json::from_value(report["warnings"].clone())
                .map_err(|_| Error::Refusal("invalid_service_view"))?,
        }),
        Err(Error::ReaderPrivileges(violations)) => Ok(refused(
            Some("reader_privileges_not_exact".to_owned()),
            violations,
        )),
        Err(Error::Refusal(code)) => Ok(refused(Some(code.to_owned()), Vec::new())),
        Err(error) => Err(error),
    }
}

impl Store {
    /// The read behind the status document, in one read-only snapshot whose
    /// statements stop after five seconds; with `reader`, the reader check
    /// of `grant-reader --check` as well. Run it on a connection of its own
    /// beside the follower's.
    pub async fn service_view(
        &mut self,
        profile: &Profile,
        reader: Option<&str>,
    ) -> Result<DatabaseView, Error> {
        self.open(profile).await?;
        let tx = self
            .client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await?;
        tx.batch_execute(&format!(
            "SET LOCAL statement_timeout='{STATEMENT_LIMIT}'"
        ))
        .await?;
        let value: Value = tx.query_one(include_str!("status.sql"), &[]).await?.get(0);
        let reader_check = match reader {
            Some(role) => Some(reader_check(&tx, role).await?),
            None => None,
        };
        tx.commit().await?;
        let raw: Raw =
            serde_json::from_value(value).map_err(|_| Error::Refusal("invalid_service_view"))?;
        Ok(DatabaseView {
            observed_at: raw.observed_at,
            identity: raw.identity,
            position: raw.position,
            target: raw.target,
            tip: raw.tip,
            horizon: raw.horizon,
            horizon_first_served_at: raw.horizon_first_served_at,
            current_epoch: raw.current_epoch,
            newest_parameters_epoch: raw.newest_parameters_epoch,
            newest_stake_epoch: raw.newest_stake_epoch,
            service_state: raw.service_state,
            calls: raw.calls,
            unspent: raw.unspent,
            first_observations: raw.first_observations,
            upkeep: upkeep(raw.upkeep)?,
            size_bytes: raw.size_bytes,
            tls_available: raw.tls_available,
            reader_check,
        })
    }
}
