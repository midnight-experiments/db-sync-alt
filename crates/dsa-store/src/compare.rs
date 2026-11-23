//! Exact node-query comparison on two read-only REPEATABLE READ snapshots.
//!
//! `capture` accepts an already-open source transaction, so a conversion
//! coordinator can capture #4's unchanged newest-header query before committing
//! that source. No cursor predicate is added to #4. All reads use exact native
//! types and public reader views. Only #9/#10 may differ after independently
//! checking their documented relationships/sentinels. Ident resolution #16 is
//! an ordinary exact answer, and each endpoint's ident/bounds feed its events.
//!
//! Rows use PostgreSQL17 JSONB canonical text. A nonrecursive lexer normalizes
//! SQL scalar numeric display scale, preserving typed JSON/JSONB columns verbatim
//! because node datum/metadata decoders observe JSON number spelling. NULL, wide
//! integers, bytea and timestamps are preserved without nested Rust JSON parsing. Ordered
//! statements keep result order; statements without a top-level ORDER BY compare
//! duplicate-preserving multisets. No tie exemption changes an ordered result.
use crate::{
    Error, NODE_RELATIONS, Store,
    catalogue::{self, Statement},
};
use chrono::NaiveDateTime;
use dsa_chain::profiles::{AssetRole, Profile};
use futures_util::{StreamExt, pin_mut};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};
use tokio_postgres::{
    Config, IsolationLevel, Transaction,
    types::{ToSql, Type},
};

/// Canonical observation encoding; independent of retained-data rule version.
pub const OBSERVATION_FORMAT: u32 = 2;
/// The probes a capture runs. Plan 0 is the plan of captures that do not
/// record one; plan 1 also probes `get_highest_block`,
/// `get_epoch_for_block_hash` and `get_block_info_for_tx_hash` at every pair.
/// The second side of a comparison runs the plan of the first.
pub const PROBE_PLAN: u32 = 1;
/// Differing rows reported per query, by default.
pub const MAX_DIFFERING_ROWS: usize = 1000;
const REGISTERED_CANDIDATE_NODE: &str = "86774edf8cc76345a5c3a4221f3fe80c094b1c33";

// Event hints: real transactions of each event class inside the window, by
// positional ID range ($1 horizon, $2 target).
const HINT_ASSET_CREATES: &str = "
SELECT DISTINCT t.block_id::bigint, t.block_index::bigint, t.id
FROM public.ma_tx_out a
JOIN public.multi_asset m ON m.id = a.ident
JOIN public.tx_out o ON o.id = a.tx_out_id
JOIN public.tx t ON t.id = o.tx_id
WHERE a.id BETWEEN $1::bigint * 68719476736 AND $2::bigint * 68719476736 + 68719476735
  AND t.id BETWEEN $1::bigint * 4096 AND $2::bigint * 4096 + 4095
  AND m.policy = $3::bytea AND m.name = $4::bytea
ORDER BY t.id LIMIT $5::bigint";
const HINT_ASSET_SPENDS: &str = "
SELECT DISTINCT t.block_id::bigint, t.block_index::bigint, t.id
FROM public.tx_in i
JOIN public.tx t ON t.id = i.tx_in_id
JOIN public.tx_out o ON o.tx_id = i.tx_out_id AND o.index = i.tx_out_index
JOIN public.ma_tx_out a ON a.tx_out_id = o.id
JOIN public.multi_asset m ON m.id = a.ident
WHERE i.id BETWEEN $1::bigint * 16777216 AND $2::bigint * 16777216 + 16777215
  AND t.id BETWEEN $1::bigint * 4096 AND $2::bigint * 4096 + 4095
  AND m.policy = $3::bytea AND m.name = $4::bytea
ORDER BY t.id LIMIT $5::bigint";
const HINT_REGISTRATIONS: &str = "
SELECT DISTINCT t.block_id::bigint, t.block_index::bigint, t.id
FROM public.ma_tx_out a
JOIN public.multi_asset m ON m.id = a.ident
JOIN public.tx_out o ON o.id = a.tx_out_id
JOIN public.datum d ON d.hash = o.data_hash
JOIN public.tx t ON t.id = o.tx_id
WHERE a.id BETWEEN $1::bigint * 68719476736 AND $2::bigint * 68719476736 + 68719476735
  AND t.id BETWEEN $1::bigint * 4096 AND $2::bigint * 4096 + 4095
  AND m.policy = $3::bytea AND m.name = $4::bytea AND o.address = $5::text AND a.quantity = 1
ORDER BY t.id LIMIT $6::bigint";
const HINT_DEREGISTRATIONS: &str = "
SELECT DISTINCT t.block_id::bigint, t.block_index::bigint, t.id
FROM public.tx_in i
JOIN public.tx t ON t.id = i.tx_in_id
JOIN public.tx_out o ON o.tx_id = i.tx_out_id AND o.index = i.tx_out_index
JOIN public.datum d ON d.hash = o.data_hash
WHERE i.id BETWEEN $1::bigint * 16777216 AND $2::bigint * 16777216 + 16777215
  AND t.id BETWEEN $1::bigint * 4096 AND $2::bigint * 4096 + 4095
  AND o.address = $3::text
ORDER BY t.id LIMIT $4::bigint";
const HINT_BRIDGE_OUTPUTS: &str = "
SELECT DISTINCT t.block_id::bigint, t.block_index::bigint, t.id
FROM public.tx_out o
JOIN public.tx t ON t.id = o.tx_id
WHERE o.id BETWEEN $1::bigint * 16777216 AND $2::bigint * 16777216 + 16777215
  AND t.id BETWEEN $1::bigint * 4096 AND $2::bigint * 4096 + 4095
  AND o.address = $3::text
ORDER BY t.id LIMIT $4::bigint";
/// The newest transaction in [cursor, reference) of a pair ($1, $2 cursor
/// block and index; $3, $4 reference block and index).
const PAIR_TRANSACTION: &str = "
SELECT hash FROM public.tx
WHERE (block_id, block_index) >= ($1::bigint, $2::bigint)
  AND (block_id, block_index) < ($3::bigint, $4::bigint)
ORDER BY block_id DESC, block_index DESC LIMIT 1";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct Cursor {
    pub block: u32,
    pub tx_index: u32,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct Pair {
    pub cursor: Cursor,
    pub reference: Cursor,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct EventAnchor {
    pub kind: String,
    pub unit: Option<String>,
    pub block: u32,
    pub tx_index: u32,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Positions {
    #[serde(default)]
    pub event_anchors: Vec<EventAnchor>,
    #[serde(default)]
    pub event_hint_queries: u32,
    #[serde(default)]
    pub witness_tx_hash: Option<String>,
    pub horizon: u32,
    pub target: u32,
    pub pairs: Vec<Pair>,
}
impl Positions {
    pub fn validate(&self) -> Result<(), Error> {
        if self.horizon > self.target
            || self.target >= 134217728
            || self.pairs.is_empty()
            || self.pairs.len() > 10000
        {
            return Err(Error::Refusal("invalid_comparison_positions"));
        }
        if self.event_anchors.len() > 10000
            || self
                .witness_tx_hash
                .as_ref()
                .is_some_and(|h| h.len() != 64 || !h.bytes().all(|b| b.is_ascii_hexdigit()))
        {
            return Err(Error::Refusal("invalid_comparison_positions"));
        }
        for p in &self.pairs {
            if p.cursor.block < self.horizon
                || p.reference.block > self.target
                || p.cursor > p.reference
                || p.cursor.tx_index > i32::MAX as u32
                || p.reference.tx_index > i32::MAX as u32
            {
                return Err(Error::Refusal("invalid_comparison_positions"));
            }
        }
        Ok(())
    }
    /// Consecutive cursor/reference pairs sampled across the entire window, one
    /// whole-window span, and neighbours at every epoch boundary. Small windows
    /// include every block. End indexes match the node's tx_count+1 convention.
    pub async fn generate(
        tx: &Transaction<'_>,
        horizon: u32,
        target: u32,
        max_pairs: usize,
    ) -> Result<Self, Error> {
        if !(1..=5000).contains(&max_pairs) || horizon > target {
            return Err(Error::Refusal("invalid_comparison_positions"));
        }
        let h = i64::from(horizon);
        let t = i64::from(target);
        let n = usize::try_from(u64::from(target) - u64::from(horizon) + 1)
            .map_err(|_| Error::Refusal("invalid_comparison_positions"))?;
        let mut uniform_edges = BTreeSet::new();
        if n > 1 {
            let count = (n - 1).min(max_pairs);
            for i in 0..count {
                uniform_edges.insert(if count == 1 {
                    0
                } else {
                    i * (n - 2) / (count - 1)
                });
            }
        }
        let prepared = tx.prepare("SELECT id,tx_count::bigint,epoch_no::integer FROM public.block WHERE id BETWEEN $1 AND $2 ORDER BY id").await?;
        let parameters: [&(dyn ToSql + Sync); 2] = [&h, &t];
        let stream = tx.query_raw(&prepared, parameters).await?;
        pin_mut!(stream);
        let start = Cursor {
            block: horizon,
            tx_index: 0,
        };
        let mut pairs = BTreeSet::new();
        let mut previous: Option<(Cursor, Option<i32>)> = None;
        let mut seen = 0usize;
        while let Some(row) = stream.next().await {
            let row = row?;
            let block = u32::try_from(row.try_get::<_, i64>(0)?)
                .map_err(|_| Error::Refusal("comparison_height_out_of_range"))?;
            if seen >= n || u64::from(block) != u64::from(horizon) + seen as u64 {
                return Err(Error::Refusal("comparison_header_gap"));
            }
            let end = Cursor {
                block,
                tx_index: u32::try_from(
                    row.try_get::<_, i64>(1)?
                        .checked_add(1)
                        .ok_or(Error::Refusal("comparison_tx_count_out_of_range"))?,
                )
                .map_err(|_| Error::Refusal("comparison_tx_count_out_of_range"))?,
            };
            let epoch: Option<i32> = row.try_get(2)?;
            if seen == 0 {
                pairs.insert(Pair {
                    cursor: start,
                    reference: end,
                });
            }
            if let Some((before, previous_epoch)) = previous
                && (uniform_edges.contains(&(seen - 1)) || previous_epoch != epoch)
            {
                pairs.insert(Pair {
                    cursor: before,
                    reference: end,
                });
            }
            if pairs.len() > 10000 {
                return Err(Error::Refusal("invalid_comparison_positions"));
            }
            previous = Some((end, epoch));
            seen += 1;
        }
        if seen != n {
            return Err(Error::Refusal("comparison_header_gap"));
        }
        pairs.insert(Pair {
            cursor: start,
            reference: previous.ok_or(Error::Refusal("comparison_header_gap"))?.0,
        });
        let witness_tx_hash = tx
            .query_opt(
                "SELECT encode(hash, 'hex') FROM public.tx
                 WHERE id BETWEEN $1::bigint * 4096 AND $2::bigint * 4096 + 4095
                 ORDER BY id LIMIT 1",
                &[&i64::from(horizon), &i64::from(target)],
            )
            .await?
            .map(|r| r.get(0));
        let out = Self {
            event_anchors: vec![],
            event_hint_queries: 1,
            witness_tx_hash,
            horizon,
            target,
            pairs: pairs.into_iter().collect(),
        };
        out.validate()?;
        Ok(out)
    }
    /// Uniform whole-window/epoch pairs plus bounded actual event witnesses.
    /// Limits are per class; missing classes are reported as genuinely quiet.
    pub async fn generate_with_events(
        tx: &Transaction<'_>,
        profile: &Profile,
        horizon: u32,
        target: u32,
        max_pairs: usize,
    ) -> Result<Self, Error> {
        let mut out = Self::generate(tx, horizon, target, max_pairs).await?;
        let mut pairs = out.pairs.iter().cloned().collect::<BTreeSet<_>>();
        let mut anchors = BTreeSet::new();
        let h = i64::from(horizon);
        let t = i64::from(target);
        let limit = max_pairs.min(16) as i64;
        let mut remember = |kind: &str,
                            unit: Option<String>,
                            rows: Vec<tokio_postgres::Row>|
         -> Result<(), Error> {
            for row in rows {
                let block = u32::try_from(row.try_get::<_, i64>(0)?)
                    .map_err(|_| Error::Refusal("comparison_height_out_of_range"))?;
                let index = u32::try_from(row.try_get::<_, i64>(1)?)
                    .map_err(|_| Error::Refusal("comparison_tx_count_out_of_range"))?;
                if block < horizon || block > target || index >= 4096 {
                    return Err(Error::Refusal("comparison_invalid_event_anchor"));
                }
                pairs.insert(Pair {
                    cursor: Cursor {
                        block,
                        tx_index: index,
                    },
                    reference: Cursor {
                        block,
                        tx_index: index + 1,
                    },
                });
                anchors.insert(EventAnchor {
                    kind: kind.to_owned(),
                    unit: unit.clone(),
                    block,
                    tx_index: index,
                });
            }
            Ok(())
        };
        let units = [&profile.assets.cnight, &profile.assets.bridge]
            .into_iter()
            .map(|a| (a.unit(), a.clone()))
            .collect::<BTreeMap<_, _>>();
        for (unit, asset) in units {
            let policy =
                hex::decode(asset.policy).map_err(|_| Error::Refusal("invalid_keep_profile"))?;
            let name =
                hex::decode(asset.name).map_err(|_| Error::Refusal("invalid_keep_profile"))?;
            let creates = tx
                .query(HINT_ASSET_CREATES, &[&h, &t, &policy, &name, &limit])
                .await?;
            out.event_hint_queries += 1;
            remember("asset_create", Some(unit.clone()), creates)?;
            let spends = tx
                .query(HINT_ASSET_SPENDS, &[&h, &t, &policy, &name, &limit])
                .await?;
            out.event_hint_queries += 1;
            remember("asset_spend", Some(unit), spends)?;
        }
        let auth = &profile.assets.auth;
        let policy =
            hex::decode(&auth.policy).map_err(|_| Error::Refusal("invalid_keep_profile"))?;
        let name = hex::decode(&auth.name).map_err(|_| Error::Refusal("invalid_keep_profile"))?;
        let mapping = &profile.addresses.mapping;
        let registrations = tx
            .query(
                HINT_REGISTRATIONS,
                &[&h, &t, &policy, &name, &mapping, &limit],
            )
            .await?;
        out.event_hint_queries += 1;
        remember("registration", Some(auth.unit()), registrations)?;
        let deregistrations = tx
            .query(HINT_DEREGISTRATIONS, &[&h, &t, &mapping, &limit])
            .await?;
        out.event_hint_queries += 1;
        remember("deregistration", None, deregistrations)?;
        let bridge = tx
            .query(
                HINT_BRIDGE_OUTPUTS,
                &[&h, &t, &profile.addresses.ics, &limit],
            )
            .await?;
        out.event_hint_queries += 1;
        remember("bridge_output", Some(profile.assets.bridge.unit()), bridge)?;
        out.pairs = pairs.into_iter().collect();
        out.event_anchors = anchors.into_iter().collect();
        if out.witness_tx_hash.is_none() {
            let tip = target
                .checked_add(profile.cardano.security_parameter)
                .ok_or(Error::Refusal("comparison_target_not_final"))?;
            let selection = crate::keep::Selection::new(
                profile,
                dsa_chain::keep::Bounds {
                    horizon,
                    target,
                    tip,
                },
            )?;
            let sql = format!(
                "{},
                 witness_candidates AS MATERIALIZED (
                     SELECT t.id, t.hash FROM public.tx t
                     JOIN keep_tx k ON k.id = t.id
                     JOIN keep_block b ON b.id = t.block_id
                     WHERE t.id BETWEEN 0 AND $2::bigint * 4096 + 4095)
                 SELECT encode(hash, 'hex') FROM witness_candidates ORDER BY id DESC LIMIT 1",
                crate::keep::Selection::ctes(crate::keep::Form::Source(
                    crate::keep::SourceSchema::Public
                ))
            );
            out.witness_tx_hash = tx
                .query_opt(&sql, &selection.parameters())
                .await?
                .map(|r| r.get(0));
            out.event_hint_queries += 1;
        }
        out.validate()?;
        Ok(out)
    }
}
#[derive(Debug, Clone)]
pub struct Options {
    /// Repeat every event query with bounds 0..i64::MAX and require the same
    /// events.
    pub verify_unbounded: bool,
    pub check_chain_order: bool,
    pub probe_plan: u32,
    pub page_rows: i32,
    pub max_rows: usize,
    pub max_bytes: usize,
    pub candidate_address: String,
    /// Pin one valid absent policy for both independently exported captures.
    pub missing_policy: Option<Vec<u8>>,
    pub missing_tx_hash: Option<Vec<u8>>,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            verify_unbounded: false,
            check_chain_order: false,
            probe_plan: PROBE_PLAN,
            page_rows: 256,
            max_rows: 1_000_000,
            max_bytes: 512 * 1024 * 1024,
            candidate_address: "__untracked_candidates__".to_owned(),
            missing_policy: None,
            missing_tx_hash: None,
        }
    }
}
/// Same TLS/credential handling as Store, default RO forced even if role default
/// is off. Positive timeouts are inherited; an unlimited 0 receives a 60s bound.
pub struct Reader {
    store: Store,
}
impl Reader {
    pub async fn connect(mut config: Config, root: Option<&Path>) -> Result<Self, Error> {
        let options = format!(
            "{} -c default_transaction_read_only=on",
            config.get_options().unwrap_or("")
        );
        config
            .options(&options)
            .application_name("db-sync-alt-compare");
        let store = Store::connect(config, root).await?;
        store.finite_read_limits().await?;
        Ok(Self { store })
    }
    pub async fn limits(&self, statement: Option<u32>, idle: Option<u32>) -> Result<(), Error> {
        for (name, value) in [
            ("statement_timeout", statement),
            ("idle_in_transaction_session_timeout", idle),
        ] {
            if let Some(n) = value {
                if !(1..=86400).contains(&n) {
                    return Err(Error::Refusal("invalid_comparison_timeout"));
                }
                self.store
                    .client()
                    .query_one("SELECT set_config($1,$2,false)", &[&name, &format!("{n}s")])
                    .await?;
            }
        }
        Ok(())
    }
    pub fn cancellation(&self) -> crate::Cancellation {
        self.store.cancellation()
    }
    pub async fn snapshot(&mut self) -> Result<Transaction<'_>, Error> {
        Ok(self
            .store
            .client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await?)
    }
}
#[derive(Debug, Clone)]
pub enum Bind {
    I32(i32),
    I64(i64),
    Text(String),
    Bytes(Vec<u8>),
    Time(NaiveDateTime),
}
impl Bind {
    pub fn sql(&self) -> &(dyn ToSql + Sync) {
        match self {
            Self::I32(v) => v,
            Self::I64(v) => v,
            Self::Text(v) => v,
            Self::Bytes(v) => v,
            Self::Time(v) => v,
        }
    }
}
pub fn parameter_types(s: &Statement) -> Vec<Type> {
    s.parameters
        .iter()
        .map(|t| match t.as_str() {
            "int4" => Type::INT4,
            "int8" => Type::INT8,
            "text" => Type::TEXT,
            "bytea" => Type::BYTEA,
            "timestamp" => Type::TIMESTAMP,
            _ => panic!("unclassified built native type"),
        })
        .collect()
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandidateInput {
    pub address: String,
    pub reference_block: i32,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Case {
    pub query: String,
    pub group: String,
    pub label: String,
    pub sql_sha256: String,
    pub ordered: bool,
    pub rows: Arc<Vec<String>>,
    pub elapsed_seconds: f64,
    // Omitted None preserves the checksum serialization of existing format2 files.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_input: Option<CandidateInput>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Capture {
    #[serde(default)]
    pub observation_format: u32,
    pub keep_rule_version: i32,
    pub missing_policy: String,
    pub missing_tx_hash: String,
    pub candidate_address: String,
    pub transaction_hit_available: bool,
    pub node_commit: String,
    pub catalogue_sha256: String,
    pub profile_hash: String,
    pub network: String,
    pub positions: Positions,
    pub units: BTreeMap<String, String>,
    pub settings: serde_json::Value,
    pub newest_height: u32,
    pub cases: BTreeMap<String, Case>,
    pub chain_order_checked: bool,
    pub unbounded_checked: bool,
    pub executions: usize,
    // The fields below are omitted when empty, so a capture written without
    // them keeps its checksum.
    /// See [`PROBE_PLAN`].
    #[serde(default, skip_serializing_if = "is_zero")]
    pub probe_plan: u32,
    /// The result columns of every executed query, by query ID.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub columns: BTreeMap<String, Vec<Column>>,
    /// Event cases whose answer with bounds 0..i64::MAX differs from the
    /// answer with the database's own bounds.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unbounded_differences: Vec<String>,
}
fn is_zero(value: &u32) -> bool {
    *value == 0
}
/// A result column: its name and its PostgreSQL type, which the node's
/// decoder checks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub type_name: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Difference {
    pub query: String,
    pub group: String,
    pub label: String,
    pub informational: bool,
    pub unsupported: bool,
    pub fatal: bool,
    pub coverage: String,
    pub a_rows: usize,
    pub b_rows: usize,
    /// Positions that differ in an ordered answer; rows of the multiset
    /// difference in an unordered one.
    pub differing_rows: usize,
    /// The differing rows, in order, while the query's bound lasts.
    pub rows: Vec<RowDifference>,
    pub rows_truncated: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowDifference {
    /// The position in an ordered answer; none in an unordered one, where a
    /// row is present on one side only.
    pub index: Option<usize>,
    pub a: Option<String>,
    pub b: Option<String>,
    pub a_sha256: Option<String>,
    pub b_sha256: Option<String>,
    /// The previews hold at most 4,096 characters; the hashes cover the row.
    pub preview_truncated: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDifference {
    pub query: String,
    pub a: Option<Vec<Column>>,
    pub b: Option<Vec<Column>>,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryCoverage {
    pub group: String,
    pub cases: usize,
    pub nonempty_cases: usize,
    pub paired_nonempty_cases: usize,
    pub rows: usize,
    pub unit_rows: BTreeMap<String, usize>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BridgeProbe {
    pub metadata_key: i64,
    pub metadata_key_source: String,
    pub checkpoint_mode: String,
    pub asset_source: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnsupportedObservation {
    pub coverage: String,
    pub query: String,
    pub sql_sha256: String,
    pub label: String,
    pub a_input: CandidateInput,
    pub b_input: CandidateInput,
    pub matched: bool,
    pub a_rows: Arc<Vec<String>>,
    pub b_rows: Arc<Vec<String>>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisteredCandidateCoverage {
    pub supported: bool,
    pub configured_address: Option<String>,
    pub scope: String,
    pub node_source: String,
    pub later_node_requirement: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Report {
    pub observation_format: u32,
    pub keep_rule_version: i32,
    pub missing_policy: String,
    pub missing_tx_hash: String,
    pub candidate_address: String,
    pub transaction_hit_available_a: bool,
    pub transaction_hit_available_b: bool,
    pub matched: bool,
    pub registered_candidates: RegisteredCandidateCoverage,
    pub unsupported_observations: Vec<UnsupportedObservation>,
    pub unsupported_difference_count: usize,
    pub supported_difference_count: usize,
    /// Actual row counts for each catalogue hit/miss label at both endpoints.
    pub probe_rows: BTreeMap<String, [usize; 2]>,
    pub chain_order_checked_a: bool,
    pub chain_order_checked_b: bool,
    pub unbounded_checked_a: bool,
    pub unbounded_checked_b: bool,
    pub unbounded_differences_a: Vec<String>,
    pub unbounded_differences_b: Vec<String>,
    pub probe_plan: u32,
    /// False when either capture holds no result columns (a capture written
    /// before columns were recorded); column types are then not compared.
    pub columns_compared: bool,
    pub column_differences: Vec<ColumnDifference>,
    pub max_differing_rows: usize,
    pub coverage_a: BTreeMap<String, QueryCoverage>,
    pub coverage_b: BTreeMap<String, QueryCoverage>,
    pub bridge_probe: BridgeProbe,
    pub node_commit: String,
    pub catalogue_sha256: String,
    pub network: String,
    pub positions: Positions,
    pub units: BTreeMap<String, String>,
    pub executions: usize,
    pub compared_cases: usize,
    pub helper_id_differences: usize,
    pub differences: Vec<Difference>,
    pub settings_a: serde_json::Value,
    pub settings_b: serde_json::Value,
    pub newest_a: u32,
    pub newest_b: u32,
}
fn hash(s: &str) -> String {
    format!("{:x}", Sha256::digest(s.as_bytes()))
}
/// Numeric normalization is lexical and non-recursive. JSON strings are copied
/// verbatim; no number is converted through a machine integer or float.
pub fn canonical_row(input: &str) -> Result<String, Error> {
    let b = input.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'"' => {
                out.push(b[i]);
                i += 1;
                while i < b.len() {
                    let c = b[i];
                    out.push(c);
                    i += 1;
                    if c == b'\\' {
                        if i == b.len() {
                            return Err(Error::Refusal("invalid_comparison_row"));
                        }
                        out.push(b[i]);
                        i += 1;
                    } else if c == b'"' {
                        break;
                    }
                }
            }
            b'-' | b'0'..=b'9' => {
                let start = i;
                i += 1;
                while i < b.len()
                    && (b[i].is_ascii_digit() || matches!(b[i], b'.' | b'e' | b'E' | b'+' | b'-'))
                {
                    i += 1;
                }
                let token = &input[start..i];
                let negative = token.starts_with('-');
                let token = token.trim_start_matches('-');
                let (parts, exponent) =
                    token.split_once(['e', 'E']).map_or((token, 0), |(d, e)| {
                        (d, e.parse::<i64>().unwrap_or(i64::MAX))
                    });
                let fraction = parts.split_once('.').map_or(0, |(_, f)| f.len() as i64);
                let digits = parts.replace('.', "");
                let digits = digits.trim_start_matches('0');
                if digits.is_empty() {
                    out.push(b'0');
                } else {
                    let trimmed = digits.trim_end_matches('0');
                    let shift = exponent
                        .checked_sub(fraction)
                        .and_then(|v| v.checked_add((digits.len() - trimmed.len()) as i64))
                        .ok_or(Error::Refusal("invalid_comparison_number"))?;
                    if negative {
                        out.push(b'-');
                    }
                    if shift >= 0 {
                        let zeros = usize::try_from(shift)
                            .map_err(|_| Error::Refusal("invalid_comparison_number"))?;
                        if zeros > 1_000_000_000 {
                            return Err(Error::Refusal("invalid_comparison_number"));
                        }
                        out.extend_from_slice(trimmed.as_bytes());
                        out.resize(out.len() + zeros, b'0');
                    } else {
                        let point = trimmed.len() as i64 + shift;
                        if point > 0 {
                            let point = point as usize;
                            out.extend_from_slice(&trimmed.as_bytes()[..point]);
                            out.push(b'.');
                            out.extend_from_slice(&trimmed.as_bytes()[point..]);
                        } else {
                            let zeros = usize::try_from(-point)
                                .map_err(|_| Error::Refusal("invalid_comparison_number"))?;
                            if zeros > 1_000_000_000 {
                                return Err(Error::Refusal("invalid_comparison_number"));
                            }
                            out.extend_from_slice(b"0.");
                            out.resize(out.len() + zeros, b'0');
                            out.extend_from_slice(trimmed.as_bytes());
                        }
                    }
                }
            }
            c if c.is_ascii_whitespace() => {
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8(out).map_err(|_| Error::Refusal("invalid_comparison_row"))
}
/// Numeric SQL scalars compare by value. JSON/JSONB result columns preserve
/// their PostgreSQL text, because the node's datum/metadata decoders can observe
/// the difference between JSON integer1 and decimal1.0. Only root column names
/// are parsed; nested data is scanned iteratively and is never decoded by Rust.
pub fn canonical_row_with_json(row: &str, fields: &BTreeSet<String>) -> Result<String, Error> {
    if fields.is_empty() {
        return canonical_row(row);
    }
    fn quoted(bytes: &[u8], mut i: usize) -> Result<usize, Error> {
        i += 1;
        while i < bytes.len() {
            match bytes[i] {
                b'\\' => i += 2,
                b'"' => return Ok(i + 1),
                _ => i += 1,
            }
        }
        Err(Error::Refusal("invalid_comparison_row"))
    }
    fn end_value(bytes: &[u8], start: usize) -> Result<usize, Error> {
        let mut depth = 0usize;
        let mut i = start;
        while i < bytes.len() {
            match bytes[i] {
                b'"' => {
                    i = quoted(bytes, i)?;
                    if depth == 0 {
                        return Ok(i);
                    }
                    continue;
                }
                b'{' | b'[' => depth += 1,
                b'}' | b']' => {
                    if depth == 0 {
                        return Ok(i);
                    }
                    depth -= 1;
                    if depth == 0 {
                        return Ok(i + 1);
                    }
                }
                b',' if depth == 0 => return Ok(i),
                _ => {}
            }
            i += 1;
        }
        Err(Error::Refusal("invalid_comparison_row"))
    }
    let bytes = row.as_bytes();
    let mut i = 0;
    let mut depth = 0usize;
    let mut previous = 0u8;
    let mut copied = 0usize;
    let mut out = String::new();
    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                let end = quoted(bytes, i)?;
                if depth == 1 && (previous == b'{' || previous == b',') {
                    let key: String = serde_json::from_str(&row[i..end])
                        .map_err(|_| Error::Refusal("invalid_comparison_row"))?;
                    if fields.contains(&key) {
                        let mut value = end;
                        while value < bytes.len() && bytes[value].is_ascii_whitespace() {
                            value += 1;
                        }
                        if bytes.get(value) != Some(&b':') {
                            return Err(Error::Refusal("invalid_comparison_row"));
                        }
                        value += 1;
                        while value < bytes.len() && bytes[value].is_ascii_whitespace() {
                            value += 1;
                        }
                        let last = end_value(bytes, value)?;
                        out.push_str(&canonical_row(&row[copied..value])?);
                        out.push_str(&row[value..last]);
                        copied = last;
                        i = last;
                        previous = b'v';
                        continue;
                    }
                }
                i = end;
                previous = b'v';
                continue;
            }
            b'{' | b'[' => depth += 1,
            b'}' | b']' => {
                depth = depth
                    .checked_sub(1)
                    .ok_or(Error::Refusal("invalid_comparison_row"))?;
            }
            _ => {}
        }
        if !bytes[i].is_ascii_whitespace() {
            previous = bytes[i];
        }
        i += 1;
    }
    out.push_str(&canonical_row(&row[copied..])?);
    Ok(out)
}

fn ordered(sql: &str) -> bool {
    let mut depth = 0i32;
    let mut quote = false;
    let mut last = String::new();
    let mut word = String::new();
    let flush = |w: &mut String, last: &mut String, depth: i32| {
        let found = depth == 0 && last == "ORDER" && w == "BY";
        if !w.is_empty() {
            *last = std::mem::take(w);
        }
        found
    };
    for c in sql.chars() {
        if c == '\'' {
            quote = !quote;
        }
        if quote {
            continue;
        }
        if c.is_ascii_alphanumeric() || c == '_' {
            word.push(c.to_ascii_uppercase());
        } else {
            if flush(&mut word, &mut last, depth) {
                return true;
            }
            if c == '(' {
                depth += 1;
            } else if c == ')' {
                depth -= 1;
            }
        }
    }
    flush(&mut word, &mut last, depth)
}
pub async fn rows(
    tx: &Transaction<'_>,
    statement: &Statement,
    values: &[Bind],
) -> Result<Vec<String>, Error> {
    Ok(rows_limited(tx, statement, values, usize::MAX, usize::MAX)
        .await?
        .1)
}
/// The statement's result columns and its rows as canonical text.
async fn rows_limited(
    tx: &Transaction<'_>,
    statement: &Statement,
    values: &[Bind],
    max_rows: usize,
    max_bytes: usize,
) -> Result<(Vec<Column>, Vec<String>), Error> {
    let native = tx
        .prepare_typed(
            statement.sql.trim().trim_end_matches(';'),
            &parameter_types(statement),
        )
        .await
        .map_err(|error| query_error(statement, error))?;
    let columns = native
        .columns()
        .iter()
        .map(|column| Column {
            name: column.name().to_owned(),
            type_name: column.type_().name().to_owned(),
        })
        .collect();
    let json_fields = native
        .columns()
        .iter()
        .filter(|column| *column.type_() == Type::JSONB || *column.type_() == Type::JSON)
        .map(|column| column.name().to_owned())
        .collect::<BTreeSet<_>>();
    // to_jsonb(record) alone conflates SQL NULL with a non-NULL JSON null.
    // Preserve that type distinction alongside JSON columns; nested values
    // remain PostgreSQL text and are never decoded or recursively normalized.
    let payload = if json_fields.is_empty() {
        "to_jsonb(observation)".to_owned()
    } else {
        let mut flag_key = "__dsa_sql_nulls".to_owned();
        while native.columns().iter().any(|c| c.name() == flag_key) {
            flag_key.push('_');
        }
        let nulls = json_fields
            .iter()
            .map(|field| {
                format!(
                    "'{}',observation.\"{}\" IS NULL",
                    field.replace('\'', "''"),
                    field.replace('\"', "\"\"")
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "to_jsonb(observation) || jsonb_build_object('{flag_key}',jsonb_build_object({nulls}))"
        )
    };
    let sql = format!(
        "SELECT ({payload})::text FROM ({}) observation",
        statement.sql.trim().trim_end_matches(';')
    );
    let prepared = tx
        .prepare_typed(&sql, &parameter_types(statement))
        .await
        .map_err(|error| query_error(statement, error))?;
    let params = values.iter().map(Bind::sql).collect::<Vec<_>>();
    let stream = tx
        .query_raw(&prepared, params)
        .await
        .map_err(|error| query_error(statement, error))?;
    pin_mut!(stream);
    let mut rows = Vec::new();
    let mut bytes = 0usize;
    while let Some(row) = stream.next().await {
        let row = row.map_err(|error| query_error(statement, error))?;
        let raw: &str = row.get(0);
        if rows.len() >= max_rows || raw.len() > max_bytes.saturating_sub(bytes) {
            return Err(Error::Refusal("comparison_capture_limit"));
        }
        let canonical = canonical_row_with_json(raw, &json_fields)?;
        if canonical.len() > max_bytes.saturating_sub(bytes) {
            return Err(Error::Refusal("comparison_capture_limit"));
        }
        bytes += canonical.len();
        rows.push(canonical);
    }
    if !ordered(&statement.sql) {
        rows.sort();
    }
    Ok((columns, rows))
}
fn query_error(statement: &Statement, error: tokio_postgres::Error) -> Error {
    Error::ComparisonQuery {
        query: statement.id.clone(),
        sqlstate: error.code().map(|c| c.code().to_owned()),
    }
}
#[derive(Debug, Clone)]
struct Header {
    height: u32,
    hash: Vec<u8>,
    epoch: i32,
    slot: i64,
    time: NaiveDateTime,
}
async fn header(tx: &Transaction<'_>, height: u32) -> Result<Header, Error> {
    let r = tx
        .query_opt(
            "SELECT hash::bytea, epoch_no::integer, slot_no::bigint, time
             FROM public.block WHERE id = $1",
            &[&i64::from(height)],
        )
        .await?
        .ok_or(Error::Refusal("comparison_missing_header"))?;
    Ok(Header {
        height,
        hash: r.get(0),
        epoch: r
            .get::<_, Option<i32>>(1)
            .ok_or(Error::Refusal("comparison_missing_epoch"))?,
        slot: r.get::<_, Option<i64>>(2).unwrap_or(0),
        time: r.get(3),
    })
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdBounds {
    pub tx: i64,
    pub output: i64,
    pub asset: i64,
    pub input: i64,
}
impl IdBounds {
    fn full(high: bool) -> Self {
        let v = if high { i64::MAX } else { 0 };
        Self {
            tx: v,
            output: v,
            asset: v,
            input: v,
        }
    }
    fn parse(rows: &[String]) -> Result<Self, Error> {
        let value: serde_json::Value = serde_json::from_str(
            rows.first()
                .ok_or(Error::Refusal("comparison_bounds_missing"))?,
        )?;
        let id = |k: &str| {
            value[k]
                .as_i64()
                .ok_or(Error::Refusal("comparison_invalid_bounds"))
        };
        Ok(Self {
            tx: id("tx_id!")?,
            output: id("tx_out_id!")?,
            asset: id("ma_tx_out_id!")?,
            input: id("tx_in_id!")?,
        })
    }
}
/// Check the literal helper's nearest-parent relationships, allowing the node's
/// unspecified ties within one parent. Sentinel0 can also be a real tx/output ID.
pub async fn validate_bounds(
    tx: &Transaction<'_>,
    block: u32,
    b: IdBounds,
    high: bool,
) -> Result<(), Error> {
    let anchor = tx
        .query_opt(
            "SELECT id FROM public.block WHERE block_no=$1",
            &[&(block as i32)],
        )
        .await?
        .map_or(0, |r| r.get::<_, i64>(0));
    for (table, parent, value, threshold, strict) in [
        ("tx", "block_id", b.tx, anchor, true),
        ("tx_out", "tx_id", b.output, b.tx, false),
        ("ma_tx_out", "tx_out_id", b.asset, b.output, false),
        ("tx_in", "tx_in_id", b.input, b.tx, false),
    ] {
        let compare = match (high, strict) {
            (false, true) => "<",
            (false, false) => "<=",
            (true, true) => ">",
            (true, false) => ">=",
        };
        let closer = if high { "<" } else { ">" };
        let sentinel = if high { i64::MAX } else { 0 };
        let sql = format!(
            "SELECT EXISTS(
                 SELECT 1 FROM public.{table} picked
                 WHERE picked.id = $1 AND picked.{parent} {compare} $2
                   AND NOT EXISTS(
                       SELECT 1 FROM public.{table} nearer
                       WHERE nearer.{parent} {closer} picked.{parent}
                         AND nearer.{parent} {compare} $2))
             OR ($1 = $3 AND NOT EXISTS(
                 SELECT 1 FROM public.{table} candidate WHERE candidate.{parent} {compare} $2))"
        );
        if !tx
            .query_one(&sql, &[&value, &threshold, &sentinel])
            .await?
            .get::<_, bool>(0)
        {
            return Err(Error::Refusal("comparison_invalid_bounds"));
        }
    }
    Ok(())
}
pub async fn validate_chain_order(tx: &Transaction<'_>) -> Result<(), Error> {
    for sql in [
        "SELECT id,lag(id) OVER(ORDER BY block_id,block_index) previous FROM public.tx",
        "SELECT o.id,lag(o.id) OVER(ORDER BY t.block_id,t.block_index,o.index) previous FROM public.tx_out o JOIN public.tx t ON t.id=o.tx_id",
        "SELECT i.id,
                lag(i.id) OVER(ORDER BY t.block_id, t.block_index, origin.hash, i.tx_out_index) previous
         FROM public.tx_in i
         JOIN public.tx t ON t.id = i.tx_in_id
         JOIN public.tx origin ON origin.id = i.tx_out_id",
        "SELECT a.id,
                lag(a.id) OVER(ORDER BY t.block_id, t.block_index, o.index, m.policy, m.name) previous
         FROM public.ma_tx_out a
         JOIN public.tx_out o ON o.id = a.tx_out_id
         JOIN public.tx t ON t.id = o.tx_id
         JOIN public.multi_asset m ON m.id = a.ident",
    ] {
        let invalid: bool = tx
            .query_one(
                &format!("SELECT EXISTS(SELECT 1 FROM ({sql}) ordered WHERE id<=previous)"),
                &[],
            )
            .await?
            .get(0);
        if invalid {
            return Err(Error::Refusal("comparison_chain_order_invalid"));
        }
    }
    Ok(())
}

async fn preflight(tx: &Transaction<'_>) -> Result<serde_json::Value, Error> {
    let settings = tx
        .query_one(
            "SELECT jsonb_build_object(
                 'statement_timeout', current_setting('statement_timeout'),
                 'idle_in_transaction_session_timeout',
                     current_setting('idle_in_transaction_session_timeout'),
                 'work_mem', current_setting('work_mem'),
                 'ssl', coalesce((SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()), false),
                 'default_transaction_read_only', current_setting('default_transaction_read_only'),
                 'transaction_read_only', current_setting('transaction_read_only'),
                 'transaction_isolation', current_setting('transaction_isolation'))",
            &[],
        )
        .await?
        .get::<_, serde_json::Value>(0);
    if settings["statement_timeout"] == "0"
        || settings["idle_in_transaction_session_timeout"] == "0"
    {
        return Err(Error::Refusal("comparison_requires_finite_read_limits"));
    }
    if settings["default_transaction_read_only"] != "on"
        || settings["transaction_read_only"] != "on"
        || settings["transaction_isolation"] != "repeatable read"
    {
        return Err(Error::Refusal(
            "comparison_snapshot_not_read_only_repeatable_read",
        ));
    }
    let row = tx
        .query_one(
            "SELECT current_user::text,
                    r.rolcanlogin
                    AND NOT (r.rolsuper OR r.rolcreatedb OR r.rolcreaterole
                             OR r.rolreplication OR r.rolbypassrls)
                    AND r.oid <> d.datdba
             FROM pg_roles r CROSS JOIN pg_database d
             WHERE r.rolname = current_user AND d.datname = current_database()",
            &[],
        )
        .await?;
    if !row.get::<_, bool>(1) {
        return Err(Error::Refusal("comparison_invalid_reader_role"));
    }
    let role: String = row.get(0);
    let gates = tx
        .query(
            "SELECT p.oid, n.nspname::text, obj_description(p.oid, 'pg_proc')
             FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
             WHERE n.nspname IN ('chain', 'dsa') AND p.proname = 'gate' AND p.pronargs = 0
               AND p.prosecdef AND p.provolatile = 's' AND p.prorettype = 16",
            &[],
        )
        .await?;
    if gates.len() != 1 {
        return Err(Error::Refusal("comparison_unsupported_gate"));
    }
    let gate: u32 = gates[0].get(0);
    let schema: String = gates[0].get(1);
    if schema == "dsa" {
        let declared: Option<String> = gates[0].get(2);
        let expected = format!("db-sync-alt keep-rule-version={}", crate::KEEP_RULE_VERSION);
        if declared.as_deref() != Some(expected.as_str()) {
            return Err(Error::Refusal("comparison_keep_rule_mismatch"));
        }
    }

    let sql = include_str!("reader_check.sql")
        .replace("'dsa.gate()'::regprocedure", "$3::oid")
        .replace("'dsa.gate()'", "$3::oid");
    let violations = tx
        .query(&sql, &[&role, &NODE_RELATIONS.to_vec(), &gate])
        .await?
        .into_iter()
        .map(|r| r.get::<_, String>(0))
        .collect::<Vec<_>>();
    if !violations.is_empty() {
        return Err(Error::ReaderPrivileges(violations));
    }
    tx.query_opt("SELECT EXISTS(SELECT 1 FROM public.block)", &[])
        .await
        .map_err(|error| {
            if error.code().is_some_and(|c| c.code() == "55000") {
                Error::Refusal("comparison_not_serving")
            } else {
                Error::Postgres(error)
            }
        })?;
    Ok(settings)
}
/// Deterministic, node-domain policy absent from every anchored reader.
/// A conversion coordinator can choose from its source snapshot, then pass the
/// same bytes in Options to the later converted capture. Independently chosen
/// probes must agree or compare refuses a context mismatch.
pub async fn choose_missing_policy(readers: &[&Transaction<'_>]) -> Result<Vec<u8>, Error> {
    if readers.is_empty() {
        return Err(Error::Refusal("comparison_probe_requires_reader"));
    }
    for (index, tx) in readers.iter().enumerate() {
        preflight(tx)
            .await
            .map_err(|error| Error::ComparisonSource {
                side: if index == 0 { "a" } else { "b" },
                error: Box::new(error),
            })?;
    }
    for counter in 0u32..1024 {
        let policy = if counter == 0 {
            vec![0; 28]
        } else {
            Sha256::digest(format!("db-sync-alt-missing-policy/{counter}")).to_vec()[..28].to_vec()
        };
        let mut absent = true;
        for tx in readers {
            if tx
                .query_one(
                    "SELECT EXISTS(SELECT 1 FROM public.multi_asset WHERE policy=$1::bytea)",
                    &[&policy],
                )
                .await?
                .get::<_, bool>(0)
            {
                absent = false;
                break;
            }
        }
        if absent {
            return Ok(policy);
        }
    }
    Err(Error::Refusal("comparison_no_missing_probe_policy"))
}

pub async fn choose_missing_transaction_hash(
    readers: &[&Transaction<'_>],
) -> Result<Vec<u8>, Error> {
    if readers.is_empty() {
        return Err(Error::Refusal("comparison_probe_requires_reader"));
    }
    for tx in readers {
        preflight(tx).await?;
    }
    for counter in 0u32..1024 {
        let value = if counter == 0 {
            vec![0; 32]
        } else {
            Sha256::digest(format!("db-sync-alt-missing-transaction/{counter}")).to_vec()
        };
        let mut absent = true;
        for tx in readers {
            if tx
                .query_one(
                    "SELECT EXISTS(SELECT 1 FROM public.tx WHERE hash=$1::bytea)",
                    &[&value],
                )
                .await?
                .get::<_, bool>(0)
            {
                absent = false;
                break;
            }
        }
        if absent {
            return Ok(value);
        }
    }
    Err(Error::Refusal("comparison_no_missing_transaction_hash"))
}

struct Context {
    horizon: Header,
    target: Header,
    first_epoch: i32,
    units: BTreeMap<AssetRole, i64>,
    profile: Profile,
    positions: Positions,
    options: Options,
}
impl Context {
    fn id(&self, role: AssetRole) -> Result<i64, Error> {
        self.units
            .get(&role)
            .copied()
            .ok_or(Error::Refusal("comparison_missing_profile_unit"))
    }
    fn values(
        &self,
        s: &Statement,
        hit: bool,
        low: IdBounds,
        high: IdBounds,
        pair: &Pair,
        offset: i32,
    ) -> Result<Vec<Bind>, Error> {
        use Bind::{Bytes, I32, I64, Text, Time};
        let reference = if hit {
            pair.reference.block as i32
        } else {
            i32::MAX
        };
        let epoch = if hit { self.target.epoch } else { i32::MAX };
        let h = if hit {
            self.target.hash.clone()
        } else {
            vec![0; 32]
        };
        let asset = |role: AssetRole| -> Result<i64, Error> {
            Ok(if hit { self.id(role)? } else { i64::MAX })
        };
        Ok(match s.name.as_str() {
            "get_low_bounds" | "get_high_bounds" | "get_max_tx_size" => vec![I32(reference)],
            "get_latest_stable_epoch" => vec![I32(if hit {
                self.profile.cardano.security_parameter as i32
            } else {
                i32::MAX
            })],
            "get_latest_block_for_epoch" | "get_stake_distribution" | "get_epoch_nonce" => {
                vec![I32(epoch)]
            }
            "get_blocks_by_numbers" => {
                if hit {
                    vec![
                        I32(self.positions.horizon as i32),
                        I32(self.positions.target as i32),
                    ]
                } else {
                    vec![I32(i32::MAX), I32(i32::MAX)]
                }
            }
            "get_block_by_hash" | "get_epoch_for_block_hash" => vec![Bytes(h)],
            "get_block_info_for_tx_hash" => vec![Bytes(if hit {
                self.positions
                    .witness_tx_hash
                    .as_ref()
                    .map(|h| {
                        hex::decode(h).map_err(|_| Error::Refusal("invalid_comparison_positions"))
                    })
                    .transpose()?
                    .unwrap_or_else(|| {
                        self.options
                            .missing_tx_hash
                            .clone()
                            .expect("capture pins missing tx hash")
                    })
            } else {
                self.options
                    .missing_tx_hash
                    .clone()
                    .expect("capture pins missing tx hash")
            })],
            "resolve_ident" => {
                let a = &self.profile.assets.cnight;
                vec![
                    Bytes(if hit {
                        hex::decode(&a.policy)
                            .map_err(|_| Error::Refusal("invalid_keep_profile"))?
                    } else {
                        self.options
                            .missing_policy
                            .clone()
                            .expect("capture pins missing policy")
                    }),
                    Bytes(if hit {
                        hex::decode(&a.name).map_err(|_| Error::Refusal("invalid_keep_profile"))?
                    } else {
                        vec![]
                    }),
                ]
            }
            "get_token_utxo_for_epoch" => {
                if s.parameters.len() == 2 {
                    vec![I64(asset(AssetRole::PermissionedCandidates)?), I32(epoch)]
                } else {
                    let a = &self.profile.assets.permissioned_candidates;
                    vec![
                        Bytes(if hit {
                            hex::decode(&a.policy).unwrap()
                        } else {
                            self.options
                                .missing_policy
                                .clone()
                                .expect("capture pins missing policy")
                        }),
                        Bytes(hex::decode(&a.name).unwrap()),
                        I32(epoch),
                    ]
                }
            }
            "get_governance_body_utxo" => vec![
                Text(if hit {
                    self.profile.addresses.council.clone()
                } else {
                    String::new()
                }),
                Bytes(if hit {
                    hex::decode(&self.profile.assets.council.policy).unwrap()
                } else {
                    self.options
                        .missing_policy
                        .clone()
                        .expect("capture pins missing policy")
                }),
                I32(reference),
            ],
            "get_utxos_for_address_tx_in_enabled" | "get_utxos_for_address_tx_in_consumed" => vec![
                Text(if hit {
                    self.options.candidate_address.clone()
                } else {
                    String::new()
                }),
                I32(reference),
            ],
            "index_exists" => vec![Text(if hit {
                "idx_tx_in_tx_out_id_tx_out_index".to_owned()
            } else {
                "__missing_index__".to_owned()
            })],
            "get_highest_block" => vec![
                I32(reference),
                I64(if hit { self.horizon.slot } else { i64::MAX }),
                I64(if hit { self.target.slot } else { i64::MAX }),
                Time(self.horizon.time),
                Time(self.target.time),
            ],
            "get_registrations"
            | "get_deregistrations"
            | "get_asset_creates"
            | "get_asset_spends" => {
                let mut args = match s.name.as_str() {
                    "get_registrations" => vec![
                        Text(if hit {
                            self.profile.addresses.mapping.clone()
                        } else {
                            String::new()
                        }),
                        I64(asset(AssetRole::Auth)?),
                    ],
                    "get_deregistrations" => vec![Text(if hit {
                        self.profile.addresses.mapping.clone()
                    } else {
                        String::new()
                    })],
                    _ => vec![I64(asset(AssetRole::Cnight)?)],
                };
                args.extend([
                    I32(pair.cursor.block as i32),
                    I32(pair.cursor.tx_index as i32),
                    I32(pair.reference.block as i32),
                    I32(pair.reference.tx_index as i32),
                    I32(self.options.page_rows),
                    I32(offset),
                    I64(low.tx),
                    I64(high.tx),
                ]);
                if s.name == "get_registrations" || s.name == "get_asset_creates" {
                    args.extend([
                        I64(low.output),
                        I64(high.output),
                        I64(low.asset),
                        I64(high.asset),
                    ]);
                } else {
                    args.extend([I64(low.input), I64(high.input)]);
                }
                args
            }
            _ if s.parameters.is_empty() => vec![],
            _ => return Err(Error::Refusal("comparison_unclassified_query")),
        })
    }
}
struct Collector {
    capture: Capture,
    rows: usize,
    bytes: usize,
    options: Options,
}
impl Collector {
    async fn add(
        &mut self,
        tx: &Transaction<'_>,
        s: &Statement,
        label: &str,
        values: &[Bind],
    ) -> Result<Arc<Vec<String>>, Error> {
        let started = Instant::now();
        self.capture.executions += 1;
        let (columns, data) = rows_limited(
            tx,
            s,
            values,
            self.options.max_rows.saturating_sub(self.rows),
            self.options.max_bytes.saturating_sub(self.bytes),
        )
        .await?;
        self.columns(s, columns)?;
        let rows = self.record(s, label, data, started.elapsed().as_secs_f64())?;
        if is_registered_query(s) {
            let [Bind::Text(address), Bind::I32(reference_block)] = values else {
                return Err(Error::Refusal("comparison_invalid_candidate_input"));
            };
            self.capture
                .cases
                .get_mut(&format!("{}/{label}", s.id))
                .unwrap()
                .candidate_input = Some(CandidateInput {
                address: address.clone(),
                reference_block: *reference_block,
            });
        }
        Ok(rows)
    }
    /// One statement has one result shape on one database.
    fn columns(&mut self, s: &Statement, columns: Vec<Column>) -> Result<(), Error> {
        match self.capture.columns.get(&s.id) {
            Some(known) if *known != columns => {
                Err(Error::Refusal("comparison_column_shape_changed"))
            }
            Some(_) => Ok(()),
            None => {
                self.capture.columns.insert(s.id.clone(), columns);
                Ok(())
            }
        }
    }
    fn record(
        &mut self,
        s: &Statement,
        label: &str,
        data: Vec<String>,
        elapsed: f64,
    ) -> Result<Arc<Vec<String>>, Error> {
        self.rows += data.len();
        self.bytes += data.iter().map(String::len).sum::<usize>();
        if self.rows > self.options.max_rows || self.bytes > self.options.max_bytes {
            return Err(Error::Refusal("comparison_capture_limit"));
        }
        let data = Arc::new(data);
        let key = format!("{}/{label}", s.id);
        if self
            .capture
            .cases
            .insert(
                key,
                Case {
                    query: s.id.clone(),
                    group: s.group.clone(),
                    label: label.to_owned(),
                    sql_sha256: s.sha256.clone(),
                    ordered: ordered(&s.sql),
                    rows: Arc::clone(&data),
                    elapsed_seconds: elapsed,
                    candidate_input: None,
                },
            )
            .is_some()
        {
            return Err(Error::Refusal("comparison_duplicate_case"));
        }
        Ok(data)
    }
    async fn bounds(
        &mut self,
        tx: &Transaction<'_>,
        entries: &[Statement],
        block: u32,
        high: bool,
        label: &str,
    ) -> Result<IdBounds, Error> {
        let name = if high {
            "get_high_bounds"
        } else {
            "get_low_bounds"
        };
        let s = entries
            .iter()
            .find(|e| e.name == name && e.kind == "read")
            .unwrap();
        let data = self.add(tx, s, label, &[Bind::I32(block as i32)]).await?;
        let b = IdBounds::parse(&data)?;
        validate_bounds(tx, block, b, high).await?;
        Ok(b)
    }
}
/// Capture original node answers while the caller still owns its source snapshot.
/// This performs no DDL, TEMP, mutations, session-limit changes or writer locks.
pub async fn capture(
    tx: &Transaction<'_>,
    profile: &Profile,
    positions: &Positions,
    options: &Options,
) -> Result<Capture, Error> {
    tx.batch_execute("SET LOCAL search_path=public; SET LOCAL bytea_output=hex; SET LOCAL DateStyle=ISO; SET LOCAL TimeZone=UTC").await?;
    positions.validate()?;
    if !(1..=10000).contains(&options.page_rows) {
        return Err(Error::Refusal("invalid_comparison_page_size"));
    }
    if options.probe_plan > PROBE_PLAN {
        return Err(Error::Refusal("comparison_unknown_probe_plan"));
    }
    let settings = preflight(tx).await?;
    let mut effective = options.clone();
    let missing = match &options.missing_policy {
        Some(policy) if policy.len() == 28 => {
            let present: bool = tx
                .query_one(
                    "SELECT EXISTS(SELECT 1 FROM public.multi_asset WHERE policy=$1::bytea)",
                    &[policy],
                )
                .await?
                .get(0);
            if present {
                return Err(Error::Refusal("comparison_probe_policy_present"));
            }
            policy.clone()
        }
        Some(_) => return Err(Error::Refusal("comparison_probe_policy_invalid")),
        None => choose_missing_policy(&[tx]).await?,
    };
    effective.missing_policy = Some(missing.clone());
    let missing_tx = match &options.missing_tx_hash {
        Some(value) if value.len() == 32 => {
            let present: bool = tx
                .query_one(
                    "SELECT EXISTS(SELECT 1 FROM public.tx WHERE hash=$1::bytea)",
                    &[value],
                )
                .await?
                .get(0);
            if present {
                return Err(Error::Refusal("comparison_probe_transaction_present"));
            }
            value.clone()
        }
        Some(_) => return Err(Error::Refusal("comparison_probe_transaction_invalid")),
        None => choose_missing_transaction_hash(&[tx]).await?,
    };
    effective.missing_tx_hash = Some(missing_tx.clone());
    let newest = tx
        .query_opt("SELECT id FROM public.block ORDER BY id DESC LIMIT 1", &[])
        .await?
        .ok_or(Error::Refusal("comparison_empty_headers"))?
        .get::<_, i64>(0);
    let newest =
        u32::try_from(newest).map_err(|_| Error::Refusal("comparison_height_out_of_range"))?;
    let _ = crate::convert::network_evidence(tx, profile, newest).await?;
    let h = header(tx, positions.horizon).await?;
    let target = header(tx, positions.target).await?;
    if newest
        < positions
            .target
            .saturating_add(profile.cardano.security_parameter)
    {
        return Err(Error::Refusal("comparison_target_not_final"));
    }
    let duplicate: bool = tx
        .query_one(
            "WITH observed_outputs AS MATERIALIZED (
                 SELECT id FROM public.tx_out
                 WHERE id BETWEEN $1::bigint * 16777216 AND $2::bigint * 16777216 + 16777215
                 UNION
                 SELECT o.id FROM public.tx_in i
                 JOIN public.tx_out o ON o.tx_id = i.tx_out_id AND o.index = i.tx_out_index
                 WHERE i.id BETWEEN $1::bigint * 16777216 AND $2::bigint * 16777216 + 16777215)
             SELECT EXISTS(
                 SELECT 1 FROM observed_outputs o
                 JOIN public.ma_tx_out a ON a.tx_out_id = o.id
                 GROUP BY a.tx_out_id, a.ident HAVING count(*) > 1)",
            &[&i64::from(positions.horizon), &i64::from(positions.target)],
        )
        .await?
        .get(0);
    if duplicate {
        return Err(Error::Refusal("comparison_duplicate_asset_unit"));
    }
    let cat = catalogue::catalogue();
    let entries = &cat.entries;
    if effective.candidate_address == "__untracked_candidates__" {
        effective.candidate_address = cat
            .candidate_addresses
            .get(&profile.network)
            .ok_or(Error::Refusal("comparison_unknown_network_inputs"))?
            .clone();
    }
    let mut result = Collector {
        capture: Capture {
            observation_format: OBSERVATION_FORMAT,
            keep_rule_version: crate::KEEP_RULE_VERSION,
            missing_policy: hex::encode(&missing),
            missing_tx_hash: hex::encode(&missing_tx),
            candidate_address: effective.candidate_address.clone(),
            transaction_hit_available: false,
            node_commit: cat.node_commit.clone(),
            catalogue_sha256: catalogue::sha256(),
            profile_hash: profile.hash()?,
            network: profile.network.clone(),
            positions: positions.clone(),
            units: profile
                .assets
                .iter()
                .map(|(role, asset)| (role.name().to_owned(), asset.unit()))
                .collect(),
            settings,
            newest_height: newest,
            cases: BTreeMap::new(),
            chain_order_checked: options.check_chain_order,
            unbounded_checked: options.verify_unbounded,
            executions: 0,
            probe_plan: options.probe_plan,
            columns: BTreeMap::new(),
            unbounded_differences: Vec::new(),
        },
        rows: 0,
        bytes: 0,
        options: options.clone(),
    };
    if options.check_chain_order {
        validate_chain_order(tx).await?;
    }
    let resolver = entries
        .iter()
        .find(|e| e.name == "resolve_ident" && e.kind == "read")
        .unwrap();
    let mut units = BTreeMap::new();
    for role in dsa_chain::assets::FIXED_UNIT_ROLES {
        let a = profile.assets.get(role);
        let data = result
            .add(
                tx,
                resolver,
                &format!("unit/{}/{}", role.name(), a.unit()),
                &[
                    Bind::Bytes(hex::decode(&a.policy).unwrap()),
                    Bind::Bytes(hex::decode(&a.name).unwrap()),
                ],
            )
            .await?;
        if data.is_empty() && matches!(role, AssetRole::Council | AssetRole::TechnicalCommittee) {
            // These fixed descriptors are retained when available, but the node
            // governance query resolves a policy directly rather than this ID.
            // The recorded exact lookup still catches one-sided presence.
            continue;
        }
        if data.len() != 1 {
            return Err(Error::Refusal("comparison_missing_profile_unit"));
        }
        let v: serde_json::Value = serde_json::from_str(&data[0])?;
        units.insert(
            role,
            v["id"]
                .as_i64()
                .ok_or(Error::Refusal("comparison_invalid_ident"))?,
        );
    }
    for alias in [AssetRole::Ics, AssetRole::Reserve] {
        if let Some((_, id)) = units
            .iter()
            .find(|(role, _)| profile.assets.get(**role).unit() == profile.assets.get(alias).unit())
        {
            let id = *id;
            units.insert(alias, id);
        }
    }
    let context = Context {
        first_epoch: (h.epoch - 2).max(0),
        horizon: h,
        target,
        units,
        profile: profile.clone(),
        positions: positions.clone(),
        options: effective,
    };
    let first = &positions.pairs[0];
    let low = result
        .bounds(tx, entries, first.cursor.block, false, "initial/low")
        .await?;
    let high = result
        .bounds(tx, entries, first.reference.block, true, "initial/high")
        .await?;
    // Exact29/58 inventory: the original26 gated statements plus3 catalog probes.
    // Parameterless probes repeat; "miss" cannot change an argument they lack.
    for s in entries.iter().filter(|s| s.kind == "read") {
        for hit in [true, false] {
            let values = context.values(s, hit, low, high, first, 0)?;
            let label = if hit
                && s.name == "get_block_info_for_tx_hash"
                && positions.witness_tx_hash.is_none()
            {
                "unavailable"
            } else if hit {
                "hit"
            } else {
                "miss"
            };
            let data = result.add(tx, s, label, &values).await?;
            if hit && s.name == "get_block_info_for_tx_hash" {
                result.capture.transaction_hit_available = !data.is_empty();
            }
            if hit && s.group == "startup" {
                let v: serde_json::Value = serde_json::from_str(
                    data.first()
                        .ok_or(Error::Refusal("comparison_startup_probe_failed"))?,
                )?;
                if s.name == "from_connection" {
                    if v.get("count").is_some() {
                        if v["count"] != 1 {
                            return Err(Error::Refusal("comparison_startup_probe_failed"));
                        }
                    } else if v["exists"] != true {
                        return Err(Error::Refusal("comparison_empty_tx_in"));
                    }
                }
                if s.name == "index_exists" && data.len() != 1 {
                    return Err(Error::Refusal("comparison_startup_probe_failed"));
                }
                if s.name == "create_index_if_not_exists" && v["exists"] != true {
                    return Err(Error::Refusal("comparison_startup_probe_failed"));
                }
            }
            if s.name == "get_low_bounds" || s.name == "get_high_bounds" {
                if !hit && s.name == "get_high_bounds" {
                    if !data.is_empty() {
                        return Err(Error::Refusal("comparison_invalid_bounds"));
                    }
                } else {
                    validate_bounds(
                        tx,
                        if hit {
                            first.reference.block
                        } else {
                            i32::MAX as u32
                        },
                        IdBounds::parse(&data)?,
                        s.name == "get_high_bounds",
                    )
                    .await?;
                }
            }
        }
    }
    // Diagnostic address overrides remain supported/fatal, but cannot suppress
    // the configured registered-candidate observations required by the contract.
    let configured_candidates = &cat.candidate_addresses[&profile.network];
    if &context.options.candidate_address != configured_candidates {
        for s in entries.iter().filter(|s| is_registered_query(s)) {
            result
                .add(
                    tx,
                    s,
                    "configured/hit",
                    &[
                        Bind::Text(configured_candidates.clone()),
                        Bind::I32(first.reference.block as i32),
                    ],
                )
                .await?;
        }
    }
    // All11 actual index names, every epoch the window touches plus2 old epochs.
    let index = entries
        .iter()
        .find(|s| s.name == "index_exists" && s.kind == "read")
        .unwrap();
    for name in [
        "idx_block_block_no",
        "idx_tx_block_id",
        "idx_tx_out_tx_id",
        "idx_tx_out_data_hash",
        "idx_tx_out_address",
        "idx_tx_in_tx_in_id",
        "idx_tx_in_tx_out_id_tx_out_index",
        "idx_multi_asset_policy_name",
        "idx_multi_asset_policy_name_hex",
        "idx_ma_tx_out_ident",
        "idx_ma_tx_out_id_ident",
    ] {
        if result
            .add(
                tx,
                index,
                &format!("required/{name}"),
                &[Bind::Text(name.to_owned())],
            )
            .await?
            .len()
            != 1
        {
            return Err(Error::Refusal("comparison_startup_probe_failed"));
        }
    }
    for epoch in context.first_epoch..=context.target.epoch {
        for s in entries.iter().filter(|s| {
            s.kind == "read"
                && [
                    "get_latest_block_for_epoch",
                    "get_token_utxo_for_epoch",
                    "get_epoch_nonce",
                    "get_stake_distribution",
                ]
                .contains(&s.name.as_str())
        }) {
            let mut values = context.values(s, true, low, high, first, 0)?;
            *values
                .last_mut()
                .ok_or(Error::Refusal("comparison_invalid_epoch_bind"))? = Bind::I32(epoch);
            let data = result
                .add(tx, s, &format!("epoch/{epoch}"), &values)
                .await?;
            if s.name == "get_latest_block_for_epoch"
                && let Some(row) = data.first()
            {
                let row: serde_json::Value = serde_json::from_str(row)?;
                let block = row["block_no"]
                    .as_i64()
                    .ok_or(Error::Refusal("comparison_invalid_epoch_header"))?;
                let block = i32::try_from(block)
                    .map_err(|_| Error::Refusal("comparison_height_out_of_range"))?;
                for candidates in entries.iter().filter(|e| {
                    e.kind == "read"
                        && [
                            "get_utxos_for_address_tx_in_enabled",
                            "get_utxos_for_address_tx_in_consumed",
                        ]
                        .contains(&e.name.as_str())
                }) {
                    result
                        .add(
                            tx,
                            candidates,
                            &format!("epoch/{epoch}/{}/candidates", s.id),
                            &[
                                Bind::Text(context.options.candidate_address.clone()),
                                Bind::I32(block),
                            ],
                        )
                        .await?;
                    if &context.options.candidate_address != configured_candidates {
                        result
                            .add(
                                tx,
                                candidates,
                                &format!("epoch/{epoch}/{}/configured_candidates", s.id),
                                &[Bind::Text(configured_candidates.clone()), Bind::I32(block)],
                            )
                            .await?;
                    }
                }
            }
        }
    }
    for (n, pair) in positions.pairs.iter().enumerate() {
        let cursor = header(tx, pair.cursor.block).await?;
        let reference = header(tx, pair.reference.block).await?;
        let low = result
            .bounds(
                tx,
                entries,
                pair.cursor.block,
                false,
                &format!("pair/{n}/low"),
            )
            .await?;
        let high = result
            .bounds(
                tx,
                entries,
                pair.reference.block,
                true,
                &format!("pair/{n}/high"),
            )
            .await?;
        if [low.tx, low.output, low.asset, low.input]
            .into_iter()
            .zip([high.tx, high.output, high.asset, high.input])
            .any(|(a, b)| a > b)
        {
            return Err(Error::Refusal("comparison_inverted_bounds"));
        }
        for s in entries.iter().filter(|s| {
            s.kind == "read"
                && [
                    "get_registrations",
                    "get_deregistrations",
                    "get_asset_creates",
                    "get_asset_spends",
                ]
                .contains(&s.name.as_str())
        }) {
            let roles = if s.name.starts_with("get_asset_") {
                let mut seen = BTreeSet::new();
                [
                    AssetRole::Cnight,
                    AssetRole::Bridge,
                    AssetRole::Ics,
                    AssetRole::Reserve,
                ]
                .into_iter()
                .filter(|role| {
                    context.units.contains_key(role)
                        && seen.insert(profile.assets.get(*role).unit())
                })
                .collect::<Vec<_>>()
            } else {
                vec![AssetRole::Auth]
            };
            for role in roles {
                let started = Instant::now();
                let mut pages = Vec::new();
                let mut page_bytes = 0usize;
                let mut offset = 0;
                loop {
                    let mut values = context.values(s, true, low, high, pair, offset)?;
                    if s.name.starts_with("get_asset_") {
                        values[0] = Bind::I64(context.id(role)?);
                    }
                    result.capture.executions += 1;
                    let (columns, page) = rows_limited(
                        tx,
                        s,
                        &values,
                        options.max_rows.saturating_sub(result.rows + pages.len()),
                        options.max_bytes.saturating_sub(result.bytes + page_bytes),
                    )
                    .await?;
                    result.columns(s, columns)?;
                    let count = page.len();
                    page_bytes += page.iter().map(String::len).sum::<usize>();
                    pages.extend(page);
                    if count < options.page_rows as usize {
                        break;
                    }
                    offset = offset
                        .checked_add(options.page_rows)
                        .ok_or(Error::Refusal("comparison_page_limit"))?;
                }
                if options.verify_unbounded {
                    let mut compared = 0usize;
                    let mut same = true;
                    let mut offset = 0;
                    loop {
                        let mut values = context.values(
                            s,
                            true,
                            IdBounds::full(false),
                            IdBounds::full(true),
                            pair,
                            offset,
                        )?;
                        if s.name.starts_with("get_asset_") {
                            values[0] = Bind::I64(context.id(role)?);
                        }
                        result.capture.executions += 1;
                        let (_, page) =
                            rows_limited(tx, s, &values, options.max_rows, options.max_bytes)
                                .await?;
                        let count = page.len();
                        if pages.get(compared..compared + count) != Some(page.as_slice()) {
                            same = false;
                            break;
                        }
                        compared += count;
                        if count < options.page_rows as usize {
                            break;
                        }
                        offset = offset
                            .checked_add(options.page_rows)
                            .ok_or(Error::Refusal("comparison_page_limit"))?;
                    }
                    if !same || compared != pages.len() {
                        result.capture.unbounded_differences.push(format!(
                            "{}/pair/{n}/{}",
                            s.id,
                            role.name()
                        ));
                    }
                }
                result.record(
                    s,
                    &format!("pair/{n}/{}", role.name()),
                    pages,
                    started.elapsed().as_secs_f64(),
                )?;
            }
        }
        let governance = entries
            .iter()
            .find(|s| s.name == "get_governance_body_utxo" && s.kind == "read")
            .unwrap();
        for body in profile.governance() {
            result
                .add(
                    tx,
                    governance,
                    &format!("pair/{n}/{}", body.name),
                    &[
                        Bind::Text(body.address.to_owned()),
                        Bind::Bytes(hex::decode(&body.asset.policy).unwrap()),
                        Bind::I32(reference.height as i32),
                    ],
                )
                .await?;
        }
        // The node's two header-by-hash variants retain all their original columns.
        for s in entries
            .iter()
            .filter(|s| s.name == "get_block_by_hash" && s.kind == "read")
        {
            result
                .add(
                    tx,
                    s,
                    &format!("pair/{n}/reference"),
                    &[Bind::Bytes(reference.hash.clone())],
                )
                .await?;
        }
        if options.probe_plan >= 1 {
            let statement = |name: &str| {
                entries
                    .iter()
                    .find(|s| s.name == name && s.kind == "read")
                    .unwrap()
            };
            result
                .add(
                    tx,
                    statement("get_highest_block"),
                    &format!("pair/{n}/range"),
                    &[
                        Bind::I32(reference.height as i32),
                        Bind::I64(cursor.slot),
                        Bind::I64(reference.slot),
                        Bind::Time(cursor.time),
                        Bind::Time(reference.time),
                    ],
                )
                .await?;
            result
                .add(
                    tx,
                    statement("get_epoch_for_block_hash"),
                    &format!("pair/{n}/reference"),
                    &[Bind::Bytes(reference.hash.clone())],
                )
                .await?;
            let transaction: Vec<u8> = tx
                .query_opt(
                    PAIR_TRANSACTION,
                    &[
                        &i64::from(pair.cursor.block),
                        &i64::from(pair.cursor.tx_index),
                        &i64::from(pair.reference.block),
                        &i64::from(pair.reference.tx_index),
                    ],
                )
                .await?
                .map_or_else(|| missing_tx.clone(), |row| row.get(0));
            result
                .add(
                    tx,
                    statement("get_block_info_for_tx_hash"),
                    &format!("pair/{n}/transaction"),
                    &[Bind::Bytes(transaction)],
                )
                .await?;
        }
        let cost = entries
            .iter()
            .find(|s| s.name == "get_max_tx_size")
            .unwrap();
        result
            .add(
                tx,
                cost,
                &format!("pair/{n}/cursor"),
                &[Bind::I32(pair.cursor.block as i32)],
            )
            .await?;
        let bridge_units = [AssetRole::Bridge, AssetRole::Ics, AssetRole::Reserve]
            .into_iter()
            .filter(|role| context.units.contains_key(role))
            .map(|r| (profile.assets.get(r).unit(), r))
            .collect::<BTreeMap<_, _>>();
        for (unit, role) in bridge_units {
            let a = profile.assets.get(role);
            let sql = catalogue::bridge_sql(pair.cursor.block as i32 - 1);
            let statement = Statement {
                id: "get_bridge_txs:enabled".to_owned(),
                name: "get_bridge_txs".to_owned(),
                sha256: hash(&sql),
                sql,
                kind: "read".to_owned(),
                group: "node".to_owned(),
                parameters: ["text", "text", "bytea", "bytea", "int8", "int4"]
                    .map(str::to_owned)
                    .to_vec(),
                sources: vec![],
            };
            result
                .add(
                    tx,
                    &statement,
                    &format!("pair/{n}/unit/{unit}"),
                    &[
                        Bind::Text(profile.addresses.ics.clone()),
                        Bind::Text(profile.addresses.reserve.clone()),
                        Bind::Bytes(hex::decode(&a.policy).unwrap()),
                        Bind::Bytes(hex::decode(&a.name).unwrap()),
                        Bind::I64(6500973),
                        Bind::I32(pair.reference.block as i32),
                    ],
                )
                .await?;
        }
    }
    Ok(result.capture)
}
fn keys_for_probe_rows(a: &Capture, b: &Capture) -> BTreeMap<String, [usize; 2]> {
    a.cases
        .iter()
        .filter(|(_, case)| {
            case.label == "hit" || case.label == "miss" || case.label == "unavailable"
        })
        .map(|(key, case)| {
            (
                key.clone(),
                [
                    case.rows.len(),
                    b.cases.get(key).map_or(0, |v| v.rows.len()),
                ],
            )
        })
        .collect()
}
fn coverage(capture: &Capture) -> BTreeMap<String, QueryCoverage> {
    let mut result: BTreeMap<String, QueryCoverage> = BTreeMap::new();
    for case in capture.cases.values() {
        let stats = result.entry(case.query.clone()).or_default();
        stats.group = case.group.clone();
        stats.cases += 1;
        stats.rows += case.rows.len();
        if !case.rows.is_empty() {
            stats.nonempty_cases += 1;
            if case.label.starts_with("pair/") {
                stats.paired_nonempty_cases += 1;
            }
        }
        let unit = case
            .label
            .split_once("/unit/")
            .map(|(_, unit)| unit.to_owned())
            .or_else(|| {
                case.label
                    .split('/')
                    .nth(2)
                    .and_then(|role| capture.units.get(role).cloned())
            });
        if let Some(unit) = unit {
            *stats.unit_rows.entry(unit).or_default() += case.rows.len();
        }
    }
    result
}
fn is_registered_query(s: &Statement) -> bool {
    s.kind == "read"
        && [
            "get_utxos_for_address_tx_in_enabled",
            "get_utxos_for_address_tx_in_consumed",
        ]
        .contains(&s.name.as_str())
}
/// Format2 observations from b683466 omit input metadata. Their two original
/// call sites are fixed: initial hit/miss and an epoch-end header witness.
/// Recover only those known inputs; an unknown case is never exempted.
fn candidate_input(capture: &Capture, case: &Case) -> Option<CandidateInput> {
    if !case.query.starts_with("get_utxos_for_address_tx_in_") {
        return None;
    }
    let cat = catalogue::catalogue();
    if capture.node_commit != cat.node_commit || capture.catalogue_sha256 != catalogue::sha256() {
        return None;
    }
    if !cat
        .entries
        .iter()
        .any(|s| s.id == case.query && is_registered_query(s) && s.sha256 == case.sql_sha256)
    {
        return None;
    }
    if let Some(input) = &case.candidate_input {
        return Some(input.clone());
    }
    if case.label == "hit" {
        return Some(CandidateInput {
            address: capture.candidate_address.clone(),
            reference_block: i32::try_from(capture.positions.pairs.first()?.reference.block)
                .ok()?,
        });
    }
    if case.label == "miss" {
        return Some(CandidateInput {
            address: String::new(),
            reference_block: i32::MAX,
        });
    }
    let parts = case.label.split('/').collect::<Vec<_>>();
    if parts.len() != 4 || parts[0] != "epoch" || parts[3] != "candidates" {
        return None;
    }
    let epoch = parts[1].parse::<i32>().ok()?;
    let header = cat
        .entries
        .iter()
        .find(|s| s.id == parts[2] && s.name == "get_latest_block_for_epoch" && s.kind == "read")?;
    let witness = capture.cases.get(&format!("{}/epoch/{epoch}", header.id))?;
    if witness.sql_sha256 != header.sha256 {
        return None;
    }
    let row: serde_json::Value = serde_json::from_str(witness.rows.first()?).ok()?;
    Some(CandidateInput {
        address: capture.candidate_address.clone(),
        reference_block: i32::try_from(row["block_no"].as_i64()?).ok()?,
    })
}
/// Side A of a comparison: a database, or a capture file of one.
pub enum SideA {
    Database {
        config: Box<Config>,
        root: Option<PathBuf>,
    },
    Capture(PathBuf),
}

/// One `compare` run.
pub struct Request {
    pub profile: Profile,
    pub a: SideA,
    pub b: Config,
    pub b_root: Option<PathBuf>,
    /// Explicit positions; otherwise generated from side A (`horizon_block`)
    /// or taken from the capture file.
    pub positions: Option<Positions>,
    pub horizon_block: Option<u32>,
    pub max_pairs: usize,
    pub statement_timeout_seconds: Option<u32>,
    pub idle_transaction_timeout_seconds: Option<u32>,
    pub a_verify_unbounded: bool,
    pub candidate_address: Option<String>,
    pub max_differing_rows: usize,
}

fn side(side: &'static str) -> impl Fn(Error) -> Error {
    move |error| Error::ComparisonSource {
        side,
        error: Box::new(error),
    }
}

fn probe_bytes(value: &str, bytes: usize) -> Result<Vec<u8>, Error> {
    match hex::decode(value) {
        Ok(decoded) if decoded.len() == bytes => Ok(decoded),
        _ => Err(Error::Refusal("comparison_probe_policy_invalid")),
    }
}

async fn open_reader(
    config: Config,
    root: Option<&Path>,
    request: &Request,
    watch: &impl Fn(crate::Cancellation),
) -> Result<Reader, Error> {
    let reader = Reader::connect(config, root).await?;
    watch(reader.cancellation());
    reader
        .limits(
            request.statement_timeout_seconds,
            request.idle_transaction_timeout_seconds,
        )
        .await?;
    Ok(reader)
}

/// Captures both sides and compares them. `watch` receives every database
/// session as it opens, so that a stopped run can cancel its statements.
pub async fn compare_databases(
    request: Request,
    watch: impl Fn(crate::Cancellation),
) -> Result<Report, Error> {
    let profile = &request.profile;
    let left_file = match &request.a {
        SideA::Capture(file) => Some(crate::source_capture::read(file, profile)?),
        SideA::Database { .. } => None,
    };
    if let Some(left) = left_file {
        if request
            .candidate_address
            .as_ref()
            .is_some_and(|a| a != &left.candidate_address)
        {
            return Err(Error::Refusal("comparison_capture_context_mismatch"));
        }
        let positions = match &request.positions {
            Some(positions) if *positions != left.positions => {
                return Err(Error::Refusal("comparison_capture_context_mismatch"));
            }
            _ => left.positions.clone(),
        };
        if request
            .horizon_block
            .is_some_and(|h| h != positions.horizon)
        {
            return Err(Error::Refusal("comparison_horizon_mismatch"));
        }
        let mut b = open_reader(
            request.b.clone(),
            request.b_root.as_deref(),
            &request,
            &watch,
        )
        .await
        .map_err(side("b"))?;
        let tx = b.snapshot().await.map_err(side("b"))?;
        let options = Options {
            check_chain_order: true,
            verify_unbounded: true,
            probe_plan: left.probe_plan,
            missing_policy: Some(probe_bytes(&left.missing_policy, 28)?),
            missing_tx_hash: Some(probe_bytes(&left.missing_tx_hash, 32)?),
            candidate_address: request
                .candidate_address
                .clone()
                .unwrap_or_else(|| left.candidate_address.clone()),
            ..Default::default()
        };
        let right = capture(&tx, profile, &positions, &options)
            .await
            .map_err(side("b"))?;
        tx.commit()
            .await
            .map_err(|e| side("b")(Error::Postgres(e)))?;
        return compare(&left, &right, request.max_differing_rows);
    }
    let SideA::Database { config, root } = &request.a else {
        unreachable!("a capture file is compared above")
    };
    let mut a = open_reader((**config).clone(), root.as_deref(), &request, &watch)
        .await
        .map_err(side("a"))?;
    let mut b = open_reader(
        request.b.clone(),
        request.b_root.as_deref(),
        &request,
        &watch,
    )
    .await
    .map_err(side("b"))?;
    let left_tx = a.snapshot().await.map_err(side("a"))?;
    let right_tx = b.snapshot().await.map_err(side("b"))?;
    let missing_policy = choose_missing_policy(&[&left_tx, &right_tx]).await?;
    let missing_tx_hash = choose_missing_transaction_hash(&[&left_tx, &right_tx]).await?;
    let positions = match &request.positions {
        Some(positions) => positions.clone(),
        None => {
            let horizon = request
                .horizon_block
                .ok_or(Error::Refusal("comparison_horizon_required"))?;
            let newest: i64 = left_tx
                .query_one("SELECT id FROM public.block ORDER BY id DESC LIMIT 1", &[])
                .await
                .map_err(|e| {
                    side("a")(if e.code().is_some_and(|c| c.code() == "55000") {
                        Error::Refusal("comparison_not_serving")
                    } else {
                        Error::Postgres(e)
                    })
                })?
                .get(0);
            let target = u32::try_from(newest)
                .ok()
                .and_then(|p| p.checked_sub(profile.cardano.security_parameter))
                .ok_or(Error::Refusal("comparison_target_not_final"))?;
            Positions::generate_with_events(&left_tx, profile, horizon, target, request.max_pairs)
                .await
                .map_err(side("a"))?
        }
    };
    let mut options = Options {
        verify_unbounded: request.a_verify_unbounded,
        missing_policy: Some(missing_policy),
        missing_tx_hash: Some(missing_tx_hash),
        ..Default::default()
    };
    if let Some(address) = &request.candidate_address {
        options.candidate_address = address.clone();
    }
    let mut right_options = options.clone();
    right_options.check_chain_order = true;
    right_options.verify_unbounded = true;
    // Both snapshots pin the same absence probe. They are read concurrently so
    // that one idle snapshot cannot expire while the other is captured.
    let left_read = async {
        let value = capture(&left_tx, profile, &positions, &options)
            .await
            .map_err(side("a"))?;
        left_tx
            .commit()
            .await
            .map_err(|e| side("a")(Error::Postgres(e)))?;
        Ok::<_, Error>(value)
    };
    let right_read = async {
        let value = capture(&right_tx, profile, &positions, &right_options)
            .await
            .map_err(side("b"))?;
        right_tx
            .commit()
            .await
            .map_err(|e| side("b")(Error::Postgres(e)))?;
        Ok::<_, Error>(value)
    };
    let (left, right) = tokio::try_join!(left_read, right_read)?;
    compare(&left, &right, request.max_differing_rows)
}

/// The rows that differ: positions of an ordered answer, or the rows of an
/// unordered answer present on one side only (as multisets).
fn row_differences<'r>(
    x: &'r [String],
    y: &'r [String],
    ordered: bool,
) -> Vec<(Option<usize>, Option<&'r String>, Option<&'r String>)> {
    if ordered {
        return (0..x.len().max(y.len()))
            .filter(|i| x.get(*i) != y.get(*i))
            .map(|i| (Some(i), x.get(i), y.get(i)))
            .collect();
    }
    let mut x = x.iter().collect::<Vec<_>>();
    let mut y = y.iter().collect::<Vec<_>>();
    x.sort();
    y.sort();
    let (mut i, mut j) = (0, 0);
    let mut out = Vec::new();
    while i < x.len() || j < y.len() {
        match (x.get(i), y.get(j)) {
            (Some(a), Some(b)) if a == b => {
                i += 1;
                j += 1;
            }
            (Some(a), Some(b)) if a < b => {
                out.push((None, Some(*a), None));
                i += 1;
            }
            (Some(a), None) => {
                out.push((None, Some(*a), None));
                i += 1;
            }
            (_, b) => {
                out.push((None, None, b.copied()));
                j += 1;
            }
        }
    }
    out
}
fn preview(row: Option<&String>) -> (Option<String>, Option<String>, bool) {
    match row {
        Some(row) => (
            Some(row.chars().take(4096).collect()),
            Some(hash(row)),
            row.chars().count() > 4096,
        ),
        None => (None, None, false),
    }
}
/// Helper IDs are informational. Only the configured-address
/// registered-candidate observations; raw rows and differences remain present.
/// Every other query/input and every identifier retains exact comparison.
/// Result columns are compared when both captures hold them. At most
/// `max_differing_rows` differing rows are reported per query.
pub fn compare(a: &Capture, b: &Capture, max_differing_rows: usize) -> Result<Report, Error> {
    if a.node_commit != REGISTERED_CANDIDATE_NODE || b.node_commit != REGISTERED_CANDIDATE_NODE {
        return Err(Error::Refusal("comparison_node_coverage_contract"));
    }
    if a.observation_format != OBSERVATION_FORMAT || b.observation_format != OBSERVATION_FORMAT {
        return Err(Error::Refusal("comparison_observation_format_mismatch"));
    }
    if a.keep_rule_version != crate::KEEP_RULE_VERSION
        || b.keep_rule_version != crate::KEEP_RULE_VERSION
    {
        return Err(Error::Refusal("comparison_keep_rule_mismatch"));
    }
    if a.network != b.network
        || a.missing_tx_hash != b.missing_tx_hash
        || a.candidate_address != b.candidate_address
        || a.missing_policy != b.missing_policy
        || a.node_commit != b.node_commit
        || a.catalogue_sha256 != b.catalogue_sha256
        || a.profile_hash != b.profile_hash
        || a.positions != b.positions
        || a.units != b.units
        || a.probe_plan != b.probe_plan
    {
        return Err(Error::Refusal("comparison_capture_context_mismatch"));
    }
    if !b.chain_order_checked {
        return Err(Error::Refusal("comparison_destination_order_not_checked"));
    }
    let catalogue = catalogue::catalogue();
    let helpers = catalogue
        .entries
        .iter()
        .filter(|e| e.name == "get_low_bounds" || e.name == "get_high_bounds")
        .map(|e| e.id.clone())
        .collect::<BTreeSet<_>>();
    let mut differences = Vec::new();
    let mut unsupported_observations = Vec::new();
    let configured_address = catalogue.candidate_addresses.get(&a.network).cloned();
    let mut budget: BTreeMap<String, usize> = BTreeMap::new();
    let keys = a
        .cases
        .keys()
        .chain(b.cases.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    for key in keys {
        let left = a.cases.get(&key);
        let right = b.cases.get(&key);
        let mut unsupported = false;
        if let (Some(x), Some(y)) = (left, right) {
            if x.sql_sha256 != y.sql_sha256 || x.ordered != y.ordered {
                return Err(Error::Refusal("comparison_query_context_mismatch"));
            }
            if let (Some(a_input), Some(b_input), Some(address)) = (
                candidate_input(a, x),
                candidate_input(b, y),
                configured_address.as_ref(),
            ) && a_input == b_input
                && &a_input.address == address
            {
                unsupported = true;
                unsupported_observations.push(UnsupportedObservation {
                    coverage: "unsupported_registered_candidates".to_owned(),
                    query: x.query.clone(),
                    sql_sha256: x.sql_sha256.clone(),
                    label: x.label.clone(),
                    a_input,
                    b_input,
                    matched: x.rows == y.rows,
                    a_rows: Arc::clone(&x.rows),
                    b_rows: Arc::clone(&y.rows),
                });
            }
            if x.rows == y.rows {
                continue;
            }
        }
        let source = left.or(right).unwrap();
        let x = left.map_or(&[][..], |c| c.rows.as_slice());
        let y = right.map_or(&[][..], |c| c.rows.as_slice());
        let differing = row_differences(x, y, source.ordered);
        let remaining = budget
            .entry(source.query.clone())
            .or_insert(max_differing_rows);
        let shown = differing.len().min(*remaining);
        *remaining -= shown;
        let rows = differing
            .iter()
            .take(shown)
            .map(|(index, a_row, b_row)| {
                let (a, a_sha256, a_cut) = preview(*a_row);
                let (b, b_sha256, b_cut) = preview(*b_row);
                RowDifference {
                    index: *index,
                    a,
                    b,
                    a_sha256,
                    b_sha256,
                    preview_truncated: a_cut || b_cut,
                }
            })
            .collect();
        let informational = helpers.contains(&source.query) && left.is_some() && right.is_some();
        differences.push(Difference {
            query: source.query.clone(),
            group: source.group.clone(),
            label: source.label.clone(),
            informational,
            unsupported,
            fatal: !informational && !unsupported,
            coverage: if unsupported {
                "unsupported_registered_candidates"
            } else {
                "supported"
            }
            .to_owned(),
            a_rows: x.len(),
            b_rows: y.len(),
            differing_rows: differing.len(),
            rows,
            rows_truncated: shown < differing.len(),
        });
    }
    let columns_compared = !a.columns.is_empty() && !b.columns.is_empty();
    let column_differences = if columns_compared {
        a.columns
            .keys()
            .chain(b.columns.keys())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter(|query| a.columns.get(*query) != b.columns.get(*query))
            .map(|query| ColumnDifference {
                query: query.clone(),
                a: a.columns.get(query).cloned(),
                b: b.columns.get(query).cloned(),
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let info = differences.iter().filter(|d| d.informational).count();
    let probe_rows = keys_for_probe_rows(a, b);
    let matched = differences.iter().all(|d| !d.fatal)
        && column_differences.is_empty()
        && b.unbounded_checked
        && a.unbounded_differences.is_empty()
        && b.unbounded_differences.is_empty();
    Ok(Report {
        observation_format: OBSERVATION_FORMAT,
        missing_policy: a.missing_policy.clone(),
        missing_tx_hash: a.missing_tx_hash.clone(),
        candidate_address: a.candidate_address.clone(),
        transaction_hit_available_a: a.transaction_hit_available,
        transaction_hit_available_b: b.transaction_hit_available,
        probe_rows,
        chain_order_checked_a: a.chain_order_checked,
        chain_order_checked_b: b.chain_order_checked,
        unbounded_checked_a: a.unbounded_checked,
        unbounded_checked_b: b.unbounded_checked,
        unbounded_differences_a: a.unbounded_differences.clone(),
        unbounded_differences_b: b.unbounded_differences.clone(),
        probe_plan: a.probe_plan,
        columns_compared,
        column_differences,
        max_differing_rows,
        coverage_a: coverage(a),
        coverage_b: coverage(b),
        bridge_probe: BridgeProbe {
            metadata_key: 6500973,
            metadata_key_source: "node 86774edf: partner-chains/toolkit/smart-contracts/\
                plutus-data/src/bridge.rs:6 TOKEN_TRANSFER_METADATUM_KEY; db_model.rs:889 binding"
                .to_owned(),
            checkpoint_mode: "synthetic Block(cursor.block-1), unlimited raw SQL range".to_owned(),
            asset_source: "pinned profile shipped bridge genesis plus separate ICS/reserve \
                inputs; current runtime checkpoint/asset not measured"
                .to_owned(),
        },
        keep_rule_version: crate::KEEP_RULE_VERSION,
        matched,
        registered_candidates: RegisteredCandidateCoverage {
            supported: false,
            configured_address,
            scope: "Only literal #22 variants at the configured registered-candidates address; \
                raw observations retained. Operator RPC/CLI registered-candidate results are \
                unsupported."
                .to_owned(),
            node_source: "node 2.1.101 authority_selection_inputs.rs:142-153: \
                registered_candidates = Vec::new()"
                .to_owned(),
            later_node_requirement: "Full registered-address coverage and a new keep-rule \
                version before enabling registered candidates."
                .to_owned(),
        },
        unsupported_difference_count: differences.iter().filter(|d| d.unsupported).count(),
        supported_difference_count: differences.iter().filter(|d| d.fatal).count(),
        unsupported_observations,
        node_commit: a.node_commit.clone(),
        catalogue_sha256: a.catalogue_sha256.clone(),
        network: a.network.clone(),
        positions: a.positions.clone(),
        units: a.units.clone(),
        executions: a.executions + b.executions,
        compared_cases: a.cases.len().max(b.cases.len()),
        helper_id_differences: info,
        differences,
        settings_a: a.settings.clone(),
        settings_b: b.settings.clone(),
        newest_a: a.newest_height,
        newest_b: b.newest_height,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn lossless_rows_numeric_json_null_and_deep() {
        assert_eq!(
            canonical_row(
                r#"{"n":340282366920938463463374607431768211456.000,"s":"1.00","null":null}"#
            )
            .unwrap(),
            r#"{"n":340282366920938463463374607431768211456,"s":"1.00","null":null}"#
        );
        assert_eq!(
            canonical_row("[1.20,120e-2,-0.0,0.00100]").unwrap(),
            "[1.2,1.2,0,0.001]"
        );
        let deep = format!("{}1.000{}", "[".repeat(2000), "]".repeat(2000));
        assert_eq!(
            canonical_row(&deep).unwrap(),
            format!("{}1{}", "[".repeat(2000), "]".repeat(2000))
        );
    }
    fn registered_capture() -> Capture {
        let cat = catalogue::catalogue();
        let statement = cat.entries.iter().find(|s| is_registered_query(s)).unwrap();
        let input = CandidateInput {
            address: cat.candidate_addresses["mainnet"].clone(),
            reference_block: 8,
        };
        Capture {
            observation_format: OBSERVATION_FORMAT,
            keep_rule_version: crate::KEEP_RULE_VERSION,
            missing_policy: "00".repeat(28),
            missing_tx_hash: "00".repeat(32),
            candidate_address: input.address.clone(),
            transaction_hit_available: false,
            node_commit: cat.node_commit.clone(),
            catalogue_sha256: catalogue::sha256(),
            profile_hash: "fixture".to_owned(),
            network: "mainnet".to_owned(),
            positions: Positions {
                horizon: 6,
                target: 8,
                event_anchors: vec![],
                event_hint_queries: 0,
                witness_tx_hash: None,
                pairs: vec![Pair {
                    cursor: Cursor {
                        block: 6,
                        tx_index: 0,
                    },
                    reference: Cursor {
                        block: 8,
                        tx_index: 0,
                    },
                }],
            },
            units: BTreeMap::new(),
            settings: serde_json::json!({}),
            newest_height: 8,
            cases: BTreeMap::from([(
                format!("{}/hit", statement.id),
                Case {
                    query: statement.id.clone(),
                    group: statement.group.clone(),
                    label: "hit".to_owned(),
                    sql_sha256: statement.sha256.clone(),
                    ordered: false,
                    rows: Arc::new(vec!["source".to_owned()]),
                    elapsed_seconds: 0.0,
                    candidate_input: Some(input),
                },
            )]),
            chain_order_checked: true,
            unbounded_checked: true,
            executions: 1,
            probe_plan: PROBE_PLAN,
            columns: BTreeMap::from([(
                statement.id.clone(),
                vec![Column {
                    name: "utxo_id".to_owned(),
                    type_name: "int8".to_owned(),
                }],
            )]),
            unbounded_differences: Vec::new(),
        }
    }
    fn case(capture: &Capture, label: &str, ordered: bool, rows: &[&str]) -> Case {
        let mut case = capture.cases.values().next().unwrap().clone();
        case.label = label.to_owned();
        case.ordered = ordered;
        case.candidate_input = None;
        case.rows = Arc::new(rows.iter().map(|r| (*r).to_owned()).collect());
        case
    }
    #[test]
    fn a_type_only_difference_fails_and_columns_absent_on_one_side_are_not_compared() {
        let a = registered_capture();
        let mut b = a.clone();
        b.columns.values_mut().next().unwrap()[0].type_name = "int4".to_owned();
        let report = compare(&a, &b, MAX_DIFFERING_ROWS).unwrap();
        assert!(report.columns_compared);
        assert!(!report.matched);
        assert_eq!(report.supported_difference_count, 0);
        assert_eq!(report.column_differences.len(), 1);
        assert_eq!(
            report.column_differences[0].b.as_ref().unwrap()[0].type_name,
            "int4"
        );
        // A capture written before columns were recorded compares rows only.
        let mut old = a.clone();
        old.columns.clear();
        let report = compare(&old, &b, MAX_DIFFERING_ROWS).unwrap();
        assert!(!report.columns_compared);
        assert!(report.matched);
    }
    #[test]
    fn every_differing_row_is_reported_up_to_the_bound_per_query() {
        let mut a = registered_capture();
        a.cases.clear();
        let mut b = a.clone();
        let template = registered_capture();
        a.cases.insert(
            "q/ordered".to_owned(),
            case(&template, "ordered", true, &["1", "2", "3", "4"]),
        );
        b.cases.insert(
            "q/ordered".to_owned(),
            case(&template, "ordered", true, &["1", "x", "3"]),
        );
        a.cases.insert(
            "q/set".to_owned(),
            case(&template, "set", false, &["a", "b", "b", "c"]),
        );
        b.cases.insert(
            "q/set".to_owned(),
            case(&template, "set", false, &["b", "c", "d"]),
        );
        let report = compare(&a, &b, MAX_DIFFERING_ROWS).unwrap();
        assert!(!report.matched);
        let ordered = report
            .differences
            .iter()
            .find(|d| d.label == "ordered")
            .unwrap();
        assert_eq!(ordered.differing_rows, 2);
        assert_eq!(
            ordered
                .rows
                .iter()
                .map(|r| (r.index, r.a.clone(), r.b.clone()))
                .collect::<Vec<_>>(),
            vec![
                (Some(1), Some("2".to_owned()), Some("x".to_owned())),
                (Some(3), Some("4".to_owned()), None)
            ]
        );
        let set = report
            .differences
            .iter()
            .find(|d| d.label == "set")
            .unwrap();
        assert_eq!(
            set.rows
                .iter()
                .map(|r| (r.index, r.a.clone(), r.b.clone()))
                .collect::<Vec<_>>(),
            vec![
                (None, Some("a".to_owned()), None),
                (None, Some("b".to_owned()), None),
                (None, None, Some("d".to_owned()))
            ]
        );
        // One bound per query ID; both cases run the same query.
        let report = compare(&a, &b, 3).unwrap();
        let shown = report
            .differences
            .iter()
            .map(|d| d.rows.len())
            .sum::<usize>();
        assert_eq!(shown, 3);
        assert!(report.differences.iter().any(|d| d.rows_truncated));
        assert_eq!(
            report
                .differences
                .iter()
                .map(|d| d.differing_rows)
                .sum::<usize>(),
            5
        );
    }
    #[test]
    fn the_expanded_bounds_check_of_the_second_side_counts_towards_matched() {
        let a = registered_capture();
        let mut b = a.clone();
        assert!(compare(&a, &b, MAX_DIFFERING_ROWS).unwrap().matched);
        b.unbounded_checked = false;
        assert!(!compare(&a, &b, MAX_DIFFERING_ROWS).unwrap().matched);
        b.unbounded_checked = true;
        b.unbounded_differences = vec!["get_asset_creates:x/pair/0/cnight".to_owned()];
        let report = compare(&a, &b, MAX_DIFFERING_ROWS).unwrap();
        assert!(!report.matched);
        assert_eq!(report.unbounded_differences_b.len(), 1);
        let mut other_plan = a.clone();
        other_plan.probe_plan = 0;
        assert!(compare(&other_plan, &a, MAX_DIFFERING_ROWS).is_err());
    }
    #[test]
    fn registered_coverage_requires_equal_bindings_and_known_legacy_calls() {
        let a = registered_capture();
        let mut b = a.clone();
        b.cases.values_mut().next().unwrap().rows = Arc::new(vec!["converted".to_owned()]);
        let report = compare(&a, &b, MAX_DIFFERING_ROWS).unwrap();
        assert!(report.matched);
        assert_eq!(report.unsupported_difference_count, 1);
        b.cases
            .values_mut()
            .next()
            .unwrap()
            .candidate_input
            .as_mut()
            .unwrap()
            .reference_block = 7;
        let report = compare(&a, &b, MAX_DIFFERING_ROWS).unwrap();
        assert!(!report.matched);
        assert_eq!(report.supported_difference_count, 1);
        assert_eq!(report.unsupported_difference_count, 0);
        let mut a = a;
        a.cases.values_mut().next().unwrap().candidate_input = None;
        b.cases.values_mut().next().unwrap().candidate_input = None;
        assert!(compare(&a, &b, MAX_DIFFERING_ROWS).unwrap().matched); // exact historical hit inference
        for malformed in [
            "unknown_label",
            "wrong_sql",
            "missing_epoch",
            "malformed_epoch",
            "wrong_epoch_sql",
        ] {
            let mut a = a.clone();
            let mut b = b.clone();
            if malformed == "wrong_sql" {
                for capture in [&mut a, &mut b] {
                    capture.cases.values_mut().next().unwrap().sql_sha256 = "bad".to_owned();
                }
            } else if malformed == "unknown_label" {
                for capture in [&mut a, &mut b] {
                    capture.cases.values_mut().next().unwrap().label = "unknown".to_owned();
                }
            } else {
                let cat = catalogue::catalogue();
                let header = cat
                    .entries
                    .iter()
                    .find(|s| s.name == "get_latest_block_for_epoch" && s.kind == "read")
                    .unwrap();
                for capture in [&mut a, &mut b] {
                    capture.cases.values_mut().next().unwrap().label =
                        format!("epoch/4/{}/candidates", header.id);
                    if malformed != "missing_epoch" {
                        capture.cases.insert(
                            format!("{}/epoch/4", header.id),
                            Case {
                                query: header.id.clone(),
                                group: header.group.clone(),
                                label: "epoch/4".to_owned(),
                                sql_sha256: if malformed == "wrong_epoch_sql" {
                                    "bad".to_owned()
                                } else {
                                    header.sha256.clone()
                                },
                                ordered: false,
                                rows: Arc::new(vec![if malformed == "malformed_epoch" {
                                    r#"{"block_no":"eight"}"#.to_owned()
                                } else {
                                    r#"{"block_no":8}"#.to_owned()
                                }]),
                                elapsed_seconds: 0.0,
                                candidate_input: None,
                            },
                        );
                    }
                }
            }
            let report = compare(&a, &b, MAX_DIFFERING_ROWS).unwrap();
            assert!(!report.matched, "{malformed}");
            assert_eq!(report.unsupported_difference_count, 0, "{malformed}");
        }
        let cat = catalogue::catalogue();
        let header = cat
            .entries
            .iter()
            .find(|s| s.name == "get_latest_block_for_epoch" && s.kind == "read")
            .unwrap();
        for capture in [&mut a, &mut b] {
            capture.cases.values_mut().next().unwrap().label =
                format!("epoch/4/{}/candidates", header.id);
            capture.cases.insert(
                format!("{}/epoch/4", header.id),
                Case {
                    query: header.id.clone(),
                    group: header.group.clone(),
                    label: "epoch/4".to_owned(),
                    sql_sha256: header.sha256.clone(),
                    ordered: false,
                    rows: Arc::new(vec![r#"{"block_no":8}"#.to_owned()]),
                    elapsed_seconds: 0.0,
                    candidate_input: None,
                },
            );
        }
        assert!(compare(&a, &b, MAX_DIFFERING_ROWS).unwrap().matched); // exact historical epoch inference
        let equal = compare(&a, &a, MAX_DIFFERING_ROWS).unwrap();
        assert!(equal.matched);
        assert_eq!(equal.unsupported_observations.len(), 1);
        assert!(equal.unsupported_observations[0].matched);
    }
    #[test]
    fn outer_order_only() {
        assert!(ordered(
            "WITH q AS(SELECT 1 ORDER BY x) SELECT * FROM q ORDER BY y"
        ));
        assert!(!ordered("SELECT array_agg(x ORDER BY y) FROM x"));
        assert!(!ordered("SELECT 'ORDER BY' FROM x"));
    }
}
