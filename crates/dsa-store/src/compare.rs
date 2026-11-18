//! Exact node-query comparison on two read-only REPEATABLE READ snapshots.
//!
//! `capture` accepts an already-open source transaction, so a conversion
//! coordinator can capture #4's unchanged newest-header query before committing
//! that source. No cursor predicate is added to #4. All reads use exact native
//! types and public reader views. Only #9/#10 may differ after independently
//! checking their documented relationships/sentinels. Ident resolution #16 is
//! an ordinary exact answer, and each endpoint's ident/bounds feed its events.
//!
//! Rows are PostgreSQL17 JSONB canonical text, normalized with a non-recursive
//! numeric lexer (no f64/JSON parser/depth limit). This preserves NULL, wide
//! integers, JSON, bytea and timestamps; numeric scale is insignificant. Ordered
//! statements keep result order; statements without a top-level ORDER BY compare
//! duplicate-preserving multisets. No tie exemption changes an ordered result.
use crate::{
    Error, NODE_RELATIONS, Store,
    catalogue::{self, Checkpoint, Statement},
};
use chrono::NaiveDateTime;
use dsa_chain::profiles::Profile;
use futures_util::{StreamExt, pin_mut};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Arc,
    time::Instant,
};
use tokio_postgres::{
    Config, IsolationLevel, Transaction,
    types::{ToSql, Type},
};

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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Positions {
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
        let rows=tx.query("SELECT id,tx_count::bigint,epoch_no::integer FROM public.block WHERE id BETWEEN $1 AND $2 ORDER BY id",&[&i64::from(horizon),&i64::from(target)]).await?;
        if rows.len() as u64 != u64::from(target) - u64::from(horizon) + 1 {
            return Err(Error::Refusal("comparison_header_gap"));
        }
        let end = |i: usize| -> Result<Cursor, Error> {
            Ok(Cursor {
                block: u32::try_from(rows[i].get::<_, i64>(0))
                    .map_err(|_| Error::Refusal("comparison_height_out_of_range"))?,
                tx_index: u32::try_from(
                    rows[i]
                        .get::<_, i64>(1)
                        .checked_add(1)
                        .ok_or(Error::Refusal("comparison_tx_count_out_of_range"))?,
                )
                .map_err(|_| Error::Refusal("comparison_tx_count_out_of_range"))?,
            })
        };
        let mut pairs = BTreeSet::new();
        let n = rows.len();
        pairs.insert(Pair {
            cursor: Cursor {
                block: horizon,
                tx_index: 0,
            },
            reference: end(0)?,
        });
        pairs.insert(Pair {
            cursor: Cursor {
                block: horizon,
                tx_index: 0,
            },
            reference: end(n - 1)?,
        });
        if n > 1 {
            let count = (n - 1).min(max_pairs);
            for i in 0..count {
                let a = if count == 1 {
                    0
                } else {
                    i * (n - 2) / (count - 1)
                };
                pairs.insert(Pair {
                    cursor: end(a)?,
                    reference: end(a + 1)?,
                });
            }
        }
        for i in 1..n {
            if rows[i].get::<_, Option<i32>>(2) != rows[i - 1].get::<_, Option<i32>>(2) {
                pairs.insert(Pair {
                    cursor: end(i - 1)?,
                    reference: end(i)?,
                });
            }
        }
        let out = Self {
            horizon,
            target,
            pairs: pairs.into_iter().collect(),
        };
        out.validate()?;
        Ok(out)
    }
}
#[derive(Debug, Clone)]
pub struct Options {
    pub verify_unbounded: bool,
    pub check_chain_order: bool,
    pub page_rows: i32,
    pub max_rows: usize,
    pub max_bytes: usize,
    pub candidate_address: String,
    /// Pin one valid absent policy for both independently exported captures.
    pub missing_policy: Option<Vec<u8>>,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            verify_unbounded: false,
            check_chain_order: false,
            page_rows: 256,
            max_rows: 1_000_000,
            max_bytes: 512 * 1024 * 1024,
            candidate_address: "__untracked_candidates__".to_owned(),
            missing_policy: None,
        }
    }
}
/// Same TLS/credential handling as Store, default RO forced even if role default
/// is off. Session timeouts are inherited unless explicit positive limits set.
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
        Ok(Self {
            store: Store::connect(config, root).await?,
        })
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Case {
    pub query: String,
    pub group: String,
    pub label: String,
    pub sql_sha256: String,
    pub ordered: bool,
    pub rows: Arc<Vec<String>>,
    pub elapsed_seconds: f64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Capture {
    pub keep_rule_version: i32,
    pub missing_policy: String,
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
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Difference {
    pub query: String,
    pub group: String,
    pub label: String,
    pub informational: bool,
    pub a_rows: usize,
    pub b_rows: usize,
    pub first_index: usize,
    pub a: Option<String>,
    pub b: Option<String>,
    pub a_row_sha256: Option<String>,
    pub b_row_sha256: Option<String>,
    pub preview_truncated: bool,
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
pub struct Report {
    pub keep_rule_version: i32,
    pub missing_policy: String,
    pub matched: bool,
    /// Actual row counts for each catalogue hit/miss label at both endpoints.
    pub probe_rows: BTreeMap<String, [usize; 2]>,
    pub chain_order_checked_a: bool,
    pub chain_order_checked_b: bool,
    pub unbounded_checked_a: bool,
    pub unbounded_checked_b: bool,
    pub coverage_a: BTreeMap<String, QueryCoverage>,
    pub coverage_b: BTreeMap<String, QueryCoverage>,
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
    rows_limited(tx, statement, values, usize::MAX, usize::MAX).await
}
async fn rows_limited(
    tx: &Transaction<'_>,
    statement: &Statement,
    values: &[Bind],
    max_rows: usize,
    max_bytes: usize,
) -> Result<Vec<String>, Error> {
    let sql = format!(
        "SELECT to_jsonb(observation)::text FROM ({}) observation",
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
        let canonical = canonical_row(raw)?;
        if canonical.len() > max_bytes.saturating_sub(bytes) {
            return Err(Error::Refusal("comparison_capture_limit"));
        }
        bytes += canonical.len();
        rows.push(canonical);
    }
    if !ordered(&statement.sql) {
        rows.sort();
    }
    Ok(rows)
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
    let r=tx.query_opt("SELECT hash::bytea,epoch_no::integer,slot_no::bigint,time FROM public.block WHERE id=$1",&[&i64::from(height)]).await?.ok_or(Error::Refusal("comparison_missing_header"))?;
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
            "SELECT EXISTS(SELECT 1 FROM public.{table} picked WHERE picked.id=$1 AND picked.{parent}{compare}$2 AND NOT EXISTS(SELECT 1 FROM public.{table} nearer WHERE nearer.{parent}{closer}picked.{parent} AND nearer.{parent}{compare}$2)) OR ($1=$3 AND NOT EXISTS(SELECT 1 FROM public.{table} candidate WHERE candidate.{parent}{compare}$2))"
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
        "SELECT i.id,lag(i.id) OVER(ORDER BY t.block_id,t.block_index,origin.hash,i.tx_out_index) previous FROM public.tx_in i JOIN public.tx t ON t.id=i.tx_in_id JOIN public.tx origin ON origin.id=i.tx_out_id",
        "SELECT a.id,lag(a.id) OVER(ORDER BY t.block_id,t.block_index,o.index,m.policy,m.name) previous FROM public.ma_tx_out a JOIN public.tx_out o ON o.id=a.tx_out_id JOIN public.tx t ON t.id=o.tx_id JOIN public.multi_asset m ON m.id=a.ident",
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
    let settings=tx.query_one("SELECT jsonb_build_object('statement_timeout',current_setting('statement_timeout'),'idle_in_transaction_session_timeout',current_setting('idle_in_transaction_session_timeout'),'work_mem',current_setting('work_mem'),'ssl',coalesce((SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()),false),'default_transaction_read_only',current_setting('default_transaction_read_only'),'transaction_read_only',current_setting('transaction_read_only'),'transaction_isolation',current_setting('transaction_isolation'))",&[]).await?.get::<_,serde_json::Value>(0);
    if settings["default_transaction_read_only"] != "on"
        || settings["transaction_read_only"] != "on"
        || settings["transaction_isolation"] != "repeatable read"
    {
        return Err(Error::Refusal(
            "comparison_snapshot_not_read_only_repeatable_read",
        ));
    }
    let row=tx.query_one("SELECT current_user::text,r.rolcanlogin AND NOT(r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls) AND r.oid<>d.datdba FROM pg_roles r CROSS JOIN pg_database d WHERE r.rolname=current_user AND d.datname=current_database()",&[]).await?;
    if !row.get::<_, bool>(1) {
        return Err(Error::Refusal("comparison_invalid_reader_role"));
    }
    let role: String = row.get(0);
    let gates=tx.query("SELECT p.oid,n.nspname::text,obj_description(p.oid,'pg_proc') FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname IN ('chain','dsa') AND p.proname='gate' AND p.pronargs=0 AND p.prosecdef AND p.provolatile='s' AND p.prorettype=16",&[]).await?;
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

struct Context {
    horizon: Header,
    target: Header,
    first_epoch: i32,
    units: BTreeMap<String, i64>,
    profile: Profile,
    positions: Positions,
    options: Options,
}
impl Context {
    fn id(&self, role: &str) -> Result<i64, Error> {
        self.units
            .get(role)
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
        let asset =
            |role: &str| -> Result<i64, Error> { Ok(if hit { self.id(role)? } else { i64::MAX }) };
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
            "get_block_info_for_tx_hash" => vec![Bytes(vec![0; 32])], // Meaningful tx hits are added from actual window transactions.
            "resolve_ident" => {
                let a = &self.profile.assets["cnight"];
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
                    vec![I64(asset("permissioned_candidates")?), I32(epoch)]
                } else {
                    let a = &self.profile.assets["permissioned_candidates"];
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
                    self.profile.addresses["council"].clone()
                } else {
                    String::new()
                }),
                Bytes(if hit {
                    hex::decode(&self.profile.assets["council"].policy).unwrap()
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
                            self.profile.addresses["mapping"].clone()
                        } else {
                            String::new()
                        }),
                        I64(asset("auth")?),
                    ],
                    "get_deregistrations" => vec![Text(if hit {
                        self.profile.addresses["mapping"].clone()
                    } else {
                        String::new()
                    })],
                    _ => vec![I64(asset("cnight")?)],
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
        let data = rows_limited(
            tx,
            s,
            values,
            self.options.max_rows.saturating_sub(self.rows),
            self.options.max_bytes.saturating_sub(self.bytes),
        )
        .await?;
        self.record(s, label, data, started.elapsed().as_secs_f64())
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
    let duplicate:bool=tx.query_one("SELECT EXISTS(SELECT 1 FROM public.ma_tx_out WHERE id BETWEEN $1::bigint*68719476736 AND $2::bigint*68719476736+68719476735 GROUP BY tx_out_id,ident HAVING count(*)>1)",&[&i64::from(positions.horizon),&i64::from(positions.target)]).await?.get(0);
    if duplicate {
        return Err(Error::Refusal("comparison_duplicate_asset_unit"));
    }
    let cat = catalogue::catalogue();
    let entries = &cat.entries;
    let mut result = Collector {
        capture: Capture {
            keep_rule_version: crate::KEEP_RULE_VERSION,
            missing_policy: hex::encode(&missing),
            node_commit: cat.node_commit.clone(),
            catalogue_sha256: hash(catalogue::GENERATED),
            profile_hash: profile.hash()?,
            network: profile.network.clone(),
            positions: positions.clone(),
            units: profile
                .assets
                .iter()
                .map(|(k, v)| (k.clone(), v.unit()))
                .collect(),
            settings,
            newest_height: newest,
            cases: BTreeMap::new(),
            chain_order_checked: options.check_chain_order,
            unbounded_checked: options.verify_unbounded,
            executions: 0,
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
        let a = &profile.assets[role];
        let data = result
            .add(
                tx,
                resolver,
                &format!("unit/{role}/{}", a.unit()),
                &[
                    Bind::Bytes(hex::decode(&a.policy).unwrap()),
                    Bind::Bytes(hex::decode(&a.name).unwrap()),
                ],
            )
            .await?;
        if data.is_empty() && ["council", "technical_committee"].contains(&role) {
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
            role.to_owned(),
            v["id"]
                .as_i64()
                .ok_or(Error::Refusal("comparison_invalid_ident"))?,
        );
    }
    for alias in ["ics", "reserve"] {
        if let Some((_, id)) = units
            .iter()
            .find(|(role, _)| profile.assets[*role].unit() == profile.assets[alias].unit())
        {
            let id = *id;
            units.insert(alias.to_owned(), id);
        }
    }
    if effective.candidate_address == "__untracked_candidates__" {
        effective.candidate_address = cat
            .candidate_addresses
            .get(&profile.network)
            .ok_or(Error::Refusal("comparison_unknown_network_inputs"))?
            .clone();
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
            let data = result
                .add(tx, s, if hit { "hit" } else { "miss" }, &values)
                .await?;
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
            result
                .add(tx, s, &format!("epoch/{epoch}"), &values)
                .await?;
        }
    }
    for (n, pair) in positions.pairs.iter().enumerate() {
        let _ = header(tx, pair.cursor.block).await?;
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
                ["cnight", "bridge", "ics", "reserve"]
                    .into_iter()
                    .filter(|role| {
                        context.units.contains_key(*role)
                            && seen.insert(profile.assets[*role].unit())
                    })
                    .collect::<Vec<_>>()
            } else {
                vec!["auth"]
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
                    let page = rows_limited(
                        tx,
                        s,
                        &values,
                        options.max_rows.saturating_sub(result.rows + pages.len()),
                        options.max_bytes.saturating_sub(result.bytes + page_bytes),
                    )
                    .await?;
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
                        let page =
                            rows_limited(tx, s, &values, options.max_rows, options.max_bytes)
                                .await?;
                        let count = page.len();
                        if pages.get(compared..compared + count) != Some(page.as_slice()) {
                            return Err(Error::Refusal("comparison_bounds_exclude_events"));
                        }
                        compared += count;
                        if count < options.page_rows as usize {
                            break;
                        }
                        offset = offset
                            .checked_add(options.page_rows)
                            .ok_or(Error::Refusal("comparison_page_limit"))?;
                    }
                    if compared != pages.len() {
                        return Err(Error::Refusal("comparison_bounds_exclude_events"));
                    }
                }
                result.record(
                    s,
                    &format!("pair/{n}/{role}"),
                    pages,
                    started.elapsed().as_secs_f64(),
                )?;
            }
        }
        let governance = entries
            .iter()
            .find(|s| s.name == "get_governance_body_utxo" && s.kind == "read")
            .unwrap();
        for body in ["council", "technical_committee"] {
            result
                .add(
                    tx,
                    governance,
                    &format!("pair/{n}/{body}"),
                    &[
                        Bind::Text(profile.addresses[body].clone()),
                        Bind::Bytes(hex::decode(&profile.assets[body].policy).unwrap()),
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
        let bridge_units = ["bridge", "ics", "reserve"]
            .into_iter()
            .filter(|role| context.units.contains_key(*role))
            .map(|r| (profile.assets[r].unit(), r))
            .collect::<BTreeMap<_, _>>();
        for (unit, role) in bridge_units {
            let a = &profile.assets[role];
            let sql = catalogue::bridge_sql(Checkpoint::Block(pair.cursor.block as i32 - 1), None);
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
                        Bind::Text(profile.addresses["ics"].clone()),
                        Bind::Text(profile.addresses["reserve"].clone()),
                        Bind::Bytes(hex::decode(&a.policy).unwrap()),
                        Bind::Bytes(hex::decode(&a.name).unwrap()),
                        Bind::I64(6500973),
                        Bind::I32(pair.reference.block as i32),
                    ],
                )
                .await?;
        }
    }
    // Resolve an actually present window transaction for the bridge checkpoint helper.
    if let Some(tx_hash)=tx.query_opt("SELECT hash::bytea FROM public.tx WHERE id BETWEEN $1::bigint*4096 AND $2::bigint*4096+4095 ORDER BY id LIMIT 1",&[&i64::from(positions.horizon),&i64::from(positions.target)]).await?{let s=entries.iter().find(|s|s.name=="get_block_info_for_tx_hash").unwrap();result.add(tx,s,"window_transaction",&[Bind::Bytes(tx_hash.get(0))]).await?;}
    Ok(result.capture)
}
fn keys_for_probe_rows(a: &Capture, b: &Capture) -> BTreeMap<String, [usize; 2]> {
    a.cases
        .iter()
        .filter(|(_, case)| case.label == "hit" || case.label == "miss")
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
/// Only verified helper ID differences are informational. Case provenance,
/// profile/positions and every other answer (including resolve_ident) are exact.
pub fn compare(a: &Capture, b: &Capture) -> Result<Report, Error> {
    if a.keep_rule_version != crate::KEEP_RULE_VERSION
        || b.keep_rule_version != crate::KEEP_RULE_VERSION
    {
        return Err(Error::Refusal("comparison_keep_rule_mismatch"));
    }
    if a.missing_policy != b.missing_policy
        || a.node_commit != b.node_commit
        || a.catalogue_sha256 != b.catalogue_sha256
        || a.profile_hash != b.profile_hash
        || a.positions != b.positions
        || a.units != b.units
    {
        return Err(Error::Refusal("comparison_capture_context_mismatch"));
    }
    if !b.chain_order_checked {
        return Err(Error::Refusal("comparison_destination_order_not_checked"));
    }
    let helpers = catalogue::catalogue()
        .entries
        .into_iter()
        .filter(|e| e.name == "get_low_bounds" || e.name == "get_high_bounds")
        .map(|e| e.id)
        .collect::<BTreeSet<_>>();
    let mut differences = Vec::new();
    let keys = a
        .cases
        .keys()
        .chain(b.cases.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    for key in keys {
        let left = a.cases.get(&key);
        let right = b.cases.get(&key);
        if let (Some(x), Some(y)) = (left, right) {
            if x.sql_sha256 != y.sql_sha256 || x.ordered != y.ordered {
                return Err(Error::Refusal("comparison_query_context_mismatch"));
            }
            if x.rows == y.rows {
                continue;
            }
        }
        let source = left.or(right).unwrap();
        let x = left.map_or(&[][..], |c| c.rows.as_slice());
        let y = right.map_or(&[][..], |c| c.rows.as_slice());
        let first = (0..x.len().max(y.len()))
            .find(|i| x.get(*i) != y.get(*i))
            .unwrap_or(0);
        differences.push(Difference {
            query: source.query.clone(),
            group: source.group.clone(),
            label: source.label.clone(),
            informational: helpers.contains(&source.query) && left.is_some() && right.is_some(),
            a_rows: x.len(),
            b_rows: y.len(),
            first_index: first,
            a: x.get(first).map(|row| row.chars().take(4096).collect()),
            b: y.get(first).map(|row| row.chars().take(4096).collect()),
            a_row_sha256: x.get(first).map(|row| hash(row)),
            b_row_sha256: y.get(first).map(|row| hash(row)),
            preview_truncated: x.get(first).is_some_and(|row| row.chars().count() > 4096)
                || y.get(first).is_some_and(|row| row.chars().count() > 4096),
        });
    }
    let info = differences.iter().filter(|d| d.informational).count();
    let probe_rows = keys_for_probe_rows(a, b);
    Ok(Report {
        missing_policy: a.missing_policy.clone(),
        probe_rows,
        chain_order_checked_a: a.chain_order_checked,
        chain_order_checked_b: b.chain_order_checked,
        unbounded_checked_a: a.unbounded_checked,
        unbounded_checked_b: b.unbounded_checked,
        coverage_a: coverage(a),
        coverage_b: coverage(b),
        keep_rule_version: crate::KEEP_RULE_VERSION,
        matched: differences.iter().all(|d| d.informational),
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
    #[test]
    fn outer_order_only() {
        assert!(ordered(
            "WITH q AS(SELECT 1 ORDER BY x) SELECT * FROM q ORDER BY y"
        ));
        assert!(!ordered("SELECT array_agg(x ORDER BY y) FROM x"));
        assert!(!ordered("SELECT 'ORDER BY' FROM x"));
    }
}
