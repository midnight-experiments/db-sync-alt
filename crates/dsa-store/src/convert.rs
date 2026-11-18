//! Convert an unchanged serving schema5 legacy public reader snapshot.
//!
//! `LegacySource` always sets default_transaction_read_only=on. Its snapshot is
//! REPEATABLE READ READ ONLY; all source facts, sparse old-spent bits, supply and
//! anchors come from that one transaction. Catalog checks require schema5's
//! eleven public gated views and the legacy gate, with the exact reader grant:
//! no TEMP, private facts, writes, ownership, privileged role or membership.
//! Network evidence is the fixed cNIGHT unit and the profile start hash when the
//! source covers that header. No provider or private mint table is consulted.
//!
//! The newest source header P determines target T=P-k, independently of the last
//! event. A bounded materialized ID-bounds/count aggregate checks complete source
//! headers. Default H is the newest header at/before target.time-retention, found
//! by indexed height bisection on monotone Cardano header times, without a time
//! index or a block-rate estimate. Shorter source history clamps H to its first
//! header. Report chosen times, elapsed target-H seconds, clamp and read cost.
//! Explicit --horizon-block takes precedence. Retention is still a duration:
//! every retained window header gets first_served=publication time, preserving
//! the full configured period after conversion, as after restore.
//!
//! One source stream per family returns original-row JSONB text, natural-key
//! canonical text and the old-spent marker in natural-key order. Rust never
//! parses arbitrary datum/metadata JSON. Destination batches are bounded by rows
//! and bytes; one supported row larger than the byte target travels alone.
//! Every original ID/ordinal and full supplied asset payload survives. Q18 is
//! deliberately upstream; the converter neither filters nor invents assets.
//! Source and destination family/state digests and inserted counts must agree.
//!
//! stage_conversion accepts the caller's already-open readonly transaction. An
//! A6/A7 coordinator can capture original node statements, including the newest
//! header, before closing it. The convenience CLI commits the source after
//! staging, then finishes destination indexes/verification/publication. Failures
//! roll back loaded rows and leave only a closed, owned convert_failed layout;
//! serving/nonempty destinations are preserved. No source DSN, password, user,
//! hostname, certificate path or other connection secret enters provenance.
//! Source limits/work_mem and family wall times are reported explicitly; default
//! owner limits are preserved unless the operator supplies positive overrides.

use crate::{
    Error, NODE_RELATIONS, Store,
    keep::Selection,
    snapshot::Description,
    verify::{self, MARKERS, Position, ServingState, Verification},
};
use dsa_chain::{
    VERSION,
    digest::{FamilyDigest, FamilyHasher, root},
    keep::Bounds,
    profiles::Profile,
};
use futures_util::{StreamExt, pin_mut};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Instant,
};
use tokio_postgres::{Config, IsolationLevel, Transaction};

#[derive(Debug, Clone)]
pub struct ConvertOptions {
    pub horizon_block: Option<u32>,
    pub retention_seconds: i64,
    pub batch_rows: usize,
    pub batch_bytes: usize,
    pub snapshot: Option<PathBuf>,
}
impl Default for ConvertOptions {
    fn default() -> Self {
        Self {
            horizon_block: None,
            retention_seconds: 604800,
            batch_rows: 4096,
            batch_bytes: 4 * 1024 * 1024,
            snapshot: None,
        }
    }
}
impl ConvertOptions {
    fn validate(&self) -> Result<(), Error> {
        if self.retention_seconds < 86400 {
            return Err(Error::Refusal("retention_below_minimum"));
        }
        if !(1..=65536).contains(&self.batch_rows) || !(1024..=67108864).contains(&self.batch_bytes)
        {
            return Err(Error::Refusal("invalid_conversion_batch_limit"));
        }
        if self.snapshot.as_ref().is_some_and(|p| p.exists()) {
            return Err(Error::Refusal("snapshot_already_exists"));
        }
        Ok(())
    }
}

pub struct LegacySource {
    store: Store,
}
impl LegacySource {
    pub async fn connect(
        mut config: Config,
        root_certificate: Option<&Path>,
    ) -> Result<Self, Error> {
        let options = format!(
            "{} -c default_transaction_read_only=on",
            config.get_options().unwrap_or("")
        );
        config
            .options(&options)
            .application_name("db-sync-alt-convert-source");
        Ok(Self {
            store: Store::connect(config, root_certificate).await?,
        })
    }
    pub fn client(&self) -> &tokio_postgres::Client {
        self.store.client()
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
    pub async fn limits(
        &self,
        statement_seconds: Option<u32>,
        idle_seconds: Option<u32>,
    ) -> Result<(), Error> {
        for (name, seconds) in [
            ("statement_timeout", statement_seconds),
            ("idle_in_transaction_session_timeout", idle_seconds),
        ] {
            if let Some(seconds) = seconds {
                if !(1..=86400).contains(&seconds) {
                    return Err(Error::Refusal("invalid_source_timeout"));
                }
                self.store
                    .client
                    .query_one(
                        "SELECT set_config($1,$2,false)",
                        &[&name, &format!("{seconds}s")],
                    )
                    .await?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceSettings {
    pub server_version_num: i32,
    pub work_mem: String,
    pub statement_timeout: String,
    pub idle_in_transaction_session_timeout: String,
    pub default_transaction_read_only: String,
    pub transaction_read_only: String,
    pub transaction_isolation: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FamilyCost {
    pub rows: u64,
    pub jsonb_utf8_bytes: u64,
    pub batches: u64,
    pub elapsed_seconds: f64,
    pub sql_sha256: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceReport {
    pub network: String,
    pub state: ServingState,
    pub settings: SourceSettings,
    pub source_header_count: i64,
    pub bounds_read_seconds: f64,
    pub horizon_read_seconds: f64,
    pub horizon_point_queries: u32,
    pub covered_seconds: String,
    pub coverage_shorter_than_retention: bool,
    pub explicit_horizon: bool,
    pub horizon_clamped_to_source_start: bool,
    pub converted_at: String,
    pub families: BTreeMap<String, FamilyCost>,
    pub longest_family_seconds: f64,
}
#[derive(Debug, Serialize)]
pub struct Conversion {
    pub verified: Verification,
    pub source: SourceReport,
    pub snapshot: Option<Description>,
}

pub struct StagedConversion<'a> {
    transaction: Transaction<'a>,
    profile_hash: String,
    source: SourceReport,
    families: BTreeMap<String, FamilyDigest>,
    protected_state: FamilyDigest,
    supply: BTreeMap<String, String>,
}
impl StagedConversion<'_> {
    pub fn source(&self) -> &SourceReport {
        &self.source
    }
    pub async fn finish(self, profile: &Profile) -> Result<Conversion, Error> {
        if self.profile_hash != profile.hash()? {
            return Err(Error::Refusal("profile_hash_mismatch"));
        }
        let tx = &self.transaction;
        let state = &self.source.state;
        write_state(tx, state).await?;
        for (unit, quantity) in &self.supply {
            tx.execute(
                "INSERT INTO dsa.supply(unit,quantity) VALUES($1,$2::text::numeric)",
                &[unit, quantity],
            )
            .await?;
        }
        tx.batch_execute(include_str!("indexes.sql")).await?;
        for table in NODE_RELATIONS.into_iter().chain([MARKERS]) {
            let schema = if table == MARKERS { "dsa" } else { "serve" };
            tx.batch_execute(&format!("ANALYZE {schema}.{table}"))
                .await?;
        }
        for table in ["multi_asset", "datum", "pool_hash", "epoch_stake"] {
            tx.query_one(&format!("SELECT setval('dsa.{table}_id_seq',greatest(coalesce(max(id),0)+1,1),false) FROM serve.{table}"),&[]).await?;
        }
        let verified = verify::verify(tx, profile, false).await?;
        if verified.families != self.families
            || verified.content_digest != root(&self.families)
            || verified.protected_state != self.protected_state
            || verified.state_digest
                != root(&BTreeMap::from([(
                    MARKERS.to_owned(),
                    self.protected_state.clone(),
                )]))
            || verified.supply != self.supply
        {
            return Err(Error::Refusal("conversion_source_digest_mismatch"));
        }
        tx.execute("INSERT INTO dsa.first_served SELECT id,clock_timestamp() FROM serve.block WHERE id BETWEEN $1 AND $2",&[&i64::from(state.horizon.height),&i64::from(state.target.height)]).await?;
        let origin = json!({"kind":"converted","source_kind":"legacy_schema5","source_network":self.source.network,"source_target_height":state.target.height,"source_target_hash":state.target.hash,"source_target_time":state.target.time,"source_tip_height":state.tip.height,"source_tip_hash":state.tip.hash,"source_tip_time":state.tip.time,"source_created_at":self.source.converted_at,"source_horizon_height":state.horizon.height,"source_horizon_hash":state.horizon.hash,"source_horizon_time":state.horizon.time,"initial_covered_seconds":self.source.covered_seconds,"initial_coverage_shorter_than_retention":self.source.coverage_shorter_than_retention});
        tx.execute(
            "UPDATE dsa.identity SET origin=$1,tool_version=$2 WHERE id=1",
            &[&origin, &VERSION],
        )
        .await?;
        tx.batch_execute(
            "UPDATE dsa.position SET serving=true,reason='serving',updated_at=now() WHERE id=1",
        )
        .await?;
        self.transaction.commit().await?;
        Ok(Conversion {
            verified,
            source: self.source,
            snapshot: None,
        })
    }
}

async fn write_state(tx: &Transaction<'_>, state: &ServingState) -> Result<(), Error> {
    tx.execute("UPDATE dsa.position SET horizon_height=$1::integer,horizon_hash=decode($2,'hex'),horizon_time=$3::text::timestamptz,target_height=$4::integer,target_hash=decode($5,'hex'),target_time=$6::text::timestamptz,tip_height=$7::integer,tip_hash=decode($8,'hex'),tip_time=$9::text::timestamptz,retention_seconds=$10,updated_at=now() WHERE id=1",&[&(state.horizon.height as i32),&state.horizon.hash,&state.horizon.time,&(state.target.height as i32),&state.target.hash,&state.target.time,&(state.tip.height as i32),&state.tip.hash,&state.tip.time,&state.retention_seconds]).await?;
    Ok(())
}

async fn settings(source: &Transaction<'_>) -> Result<SourceSettings, Error> {
    let row=source.query_one("SELECT current_setting('server_version_num')::integer,current_setting('work_mem'),current_setting('statement_timeout'),current_setting('idle_in_transaction_session_timeout'),current_setting('default_transaction_read_only'),current_setting('transaction_read_only'),current_setting('transaction_isolation')",&[]).await?;
    let settings = SourceSettings {
        server_version_num: row.get(0),
        work_mem: row.get(1),
        statement_timeout: row.get(2),
        idle_in_transaction_session_timeout: row.get(3),
        default_transaction_read_only: row.get(4),
        transaction_read_only: row.get(5),
        transaction_isolation: row.get(6),
    };
    if !(170000..180000).contains(&settings.server_version_num) {
        return Err(Error::Refusal("postgresql_17_required"));
    }
    if settings.default_transaction_read_only != "on"
        || settings.transaction_read_only != "on"
        || settings.transaction_isolation != "repeatable read"
    {
        return Err(Error::Refusal(
            "source_snapshot_not_read_only_repeatable_read",
        ));
    }
    Ok(settings)
}

async fn validate_reader(source: &Transaction<'_>) -> Result<u32, Error> {
    let row=source.query_one("SELECT current_user::text,r.rolcanlogin AND NOT (r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls) AND r.oid<>d.datdba FROM pg_roles r CROSS JOIN pg_database d WHERE r.rolname=current_user AND d.datname=current_database()",&[]).await?;
    if !row.get::<_, bool>(1) {
        return Err(Error::Refusal("invalid_source_reader_role"));
    }
    let role: String = row.get(0);
    let gate=source.query_opt("SELECT p.oid,p.prosecdef AND p.provolatile='s' AND p.prorettype=16 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='chain' AND p.proname='gate' AND p.pronargs=0 AND p.prokind='f'",&[]).await?.ok_or(Error::Refusal("unsupported_legacy_schema"))?;
    if !gate.get::<_, bool>(1) {
        return Err(Error::Refusal("unsupported_legacy_schema"));
    }
    let oid: u32 = gate.get(0);
    let tables = NODE_RELATIONS.to_vec();
    let sql = include_str!("reader_check.sql")
        .replace("'dsa.gate()'::regprocedure", "$3::oid")
        .replace("'dsa.gate()'", "$3::oid");
    let violations = source
        .query(&sql, &[&role, &tables, &oid])
        .await?
        .into_iter()
        .map(|r| r.get::<_, String>(0))
        .collect::<Vec<_>>();
    if !violations.is_empty() {
        return Err(Error::ReaderPrivileges(violations));
    }
    Ok(oid)
}

fn columns(table: &str) -> Vec<(&'static str, &'static str, &'static str)> {
    let values = match table {
        "block" => {
            "id:int8,hash:hash32type,epoch_no:word31type,slot_no:word63type,block_no:word31type,time:timestamp,tx_count:word63type"
        }
        "tx" => "id:int8,hash:hash32type,block_id:int8,block_index:word31type,valid_contract:bool",
        "tx_out" => {
            "id:int8,tx_id:int8,index:txindex,address:text,value:lovelace,data_hash:hash32type,consumed_by_tx_id:int8"
        }
        "tx_in" => "id:int8,tx_in_id:int8,tx_out_id:int8,tx_out_index:txindex",
        "ma_tx_out" => "id:int8,quantity:word64type,tx_out_id:int8,ident:int8",
        "multi_asset" => "id:int8,policy:hash28type,name:asset32type",
        "datum" => "id:int8,hash:hash32type,value:jsonb",
        "epoch_param" => "epoch_no:word31type,nonce:hash32type,max_tx_size:word31type",
        "pool_hash" => "id:int8,hash_raw:hash28type",
        "epoch_stake" => "id:int8,pool_id:int8,amount:lovelace,epoch_no:word31type",
        "tx_metadata" => "id:int8,key:word64type,tx_id:int8,json:jsonb",
        _ => "",
    };
    values
        .split(',')
        .filter_map(|s| s.split_once(':'))
        .map(|(name, kind)| {
            (
                name,
                kind,
                if ["int8", "timestamp", "bool", "text", "jsonb"].contains(&kind) {
                    "pg_catalog"
                } else {
                    "public"
                },
            )
        })
        .collect()
}

async fn validate_schema(source: &Transaction<'_>, gate: u32) -> Result<(), Error> {
    for table in NODE_RELATIONS {
        let actual=source.query("SELECT a.attname::text,t.typname::text,n.nspname::text FROM pg_class c JOIN pg_namespace relation_schema ON relation_schema.oid=c.relnamespace JOIN pg_attribute a ON a.attrelid=c.oid JOIN pg_type t ON t.oid=a.atttypid JOIN pg_namespace n ON n.oid=t.typnamespace WHERE relation_schema.nspname='public' AND c.relname=$1 AND c.relkind='v' AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum",&[&table]).await?;
        let expected = columns(table);
        if actual.len() != expected.len()
            || actual.iter().zip(expected).any(|(r, e)| {
                r.get::<_, String>(0) != e.0
                    || r.get::<_, String>(1) != e.1
                    || r.get::<_, String>(2) != e.2
            })
        {
            return Err(Error::Refusal("unsupported_legacy_schema"));
        }
    }
    let tables = NODE_RELATIONS.to_vec();
    let count:i64=source.query_one("SELECT count(DISTINCT r.ev_class) FROM pg_rewrite r JOIN pg_depend d ON d.classid='pg_rewrite'::regclass AND d.objid=r.oid AND d.refclassid='pg_proc'::regclass AND d.refobjid=$1 JOIN pg_class c ON c.oid=r.ev_class JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname=ANY($2::text[])",&[&gate,&tables]).await?.get(0);
    if count != 11 {
        return Err(Error::Refusal("unsupported_legacy_schema"));
    }
    // Calling the actual view evaluates its security-definer gate, while the
    // reader remains unable to resolve/read private chain facts directly.
    source
        .query_opt("SELECT id FROM public.block LIMIT 1", &[])
        .await
        .map_err(|error| {
            if error.code().is_some_and(|c| c.code() == "55000") {
                Error::Refusal("source_not_serving")
            } else {
                Error::Postgres(error)
            }
        })?;
    Ok(())
}

async fn header(source: &Transaction<'_>, height: u32) -> Result<Position, Error> {
    let row=source.query_opt("SELECT id,encode(hash,'hex'),to_char(time,'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') FROM public.block WHERE id=$1",&[&i64::from(height)]).await?.ok_or(Error::Refusal("missing_source_header"))?;
    Ok(Position {
        height: u32::try_from(row.get::<_, i64>(0))
            .map_err(|_| Error::Refusal("source_height_out_of_range"))?,
        hash: row.get(1),
        time: row.get(2),
    })
}

async fn old_enough(
    source: &Transaction<'_>,
    height: u32,
    target: &Position,
    retention: i64,
) -> Result<bool, Error> {
    Ok(source.query_one("SELECT extract(epoch FROM (($2::text::timestamptz AT TIME ZONE 'UTC')-time)) >= $3::bigint FROM public.block WHERE id=$1",&[&i64::from(height),&target.time,&retention]).await?.get(0))
}

async fn source_report(
    source: &Transaction<'_>,
    profile: &Profile,
    options: &ConvertOptions,
) -> Result<SourceReport, Error> {
    let settings = settings(source).await?;
    let gate = validate_reader(source).await?;
    validate_schema(source, gate).await?;
    let start = Instant::now();
    // The materialized bounded input prevents a planner min/max index shortcut.
    // Only this one bounds pass scans history; time navigation uses point reads.
    let row=source.query_one("WITH ids AS MATERIALIZED(SELECT id FROM public.block WHERE id BETWEEN 0 AND 134217727) SELECT min(id),max(id),count(*) FROM ids",&[]).await?;
    let first = row
        .get::<_, Option<i64>>(0)
        .ok_or(Error::Refusal("source_has_no_headers"))?;
    let newest = row
        .get::<_, Option<i64>>(1)
        .ok_or(Error::Refusal("source_has_no_headers"))?;
    let count: i64 = row.get(2);
    if count != newest - first + 1 {
        return Err(Error::Refusal("source_header_gap"));
    }
    let first = u32::try_from(first).map_err(|_| Error::Refusal("source_height_out_of_range"))?;
    let newest = u32::try_from(newest).map_err(|_| Error::Refusal("source_height_out_of_range"))?;
    let tip = header(source, newest).await?;
    let target_height = newest
        .checked_sub(profile.cardano.security_parameter)
        .ok_or(Error::Refusal("source_tip_below_security_parameter"))?;
    if target_height < first {
        return Err(Error::Refusal("source_target_before_history"));
    }
    let target = header(source, target_height).await?;
    let bounds_read_seconds = start.elapsed().as_secs_f64();
    let cn = profile
        .assets
        .get("cnight")
        .ok_or(Error::Refusal("invalid_keep_profile"))?;
    let policy = hex::decode(&cn.policy).map_err(|_| Error::Refusal("invalid_keep_profile"))?;
    let name = hex::decode(&cn.name).map_err(|_| Error::Refusal("invalid_keep_profile"))?;
    let network:bool=source.query_one("SELECT EXISTS(SELECT 1 FROM public.multi_asset WHERE policy=$1::bytea AND name=$2::bytea)",&[&policy,&name]).await?.get(0);
    if !network {
        return Err(Error::Refusal("network_mismatch"));
    }
    if profile.start.block_number >= first
        && profile.start.block_number <= newest
        && header(source, profile.start.block_number).await?.hash != profile.start.block_hash
    {
        return Err(Error::Refusal("source_start_anchor_mismatch"));
    }
    let horizon_started = Instant::now();
    let mut point_queries = 0;
    let mut clamped = false;
    let horizon = if let Some(h) = options.horizon_block {
        if h > target_height {
            return Err(Error::Refusal("horizon_above_source_target"));
        }
        if h < first {
            return Err(Error::Refusal("horizon_before_source_history"));
        }
        h
    } else {
        point_queries += 1;
        if !old_enough(source, first, &target, options.retention_seconds).await? {
            clamped = true;
            first
        } else {
            let (mut low, mut high) = (first, target_height);
            while low < high {
                let mid = low + (high - low).div_ceil(2);
                point_queries += 1;
                if old_enough(source, mid, &target, options.retention_seconds).await? {
                    low = mid;
                } else {
                    high = mid - 1;
                }
            }
            low
        }
    };
    let horizon = header(source, horizon).await?;
    point_queries += 1;
    let covered=source.query_one("SELECT extract(epoch FROM ($1::text::timestamptz-$2::text::timestamptz))::text,extract(epoch FROM ($1::text::timestamptz-$2::text::timestamptz))<$3::bigint,to_char(transaction_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')",&[&target.time,&horizon.time,&options.retention_seconds]).await?;
    let selection = Selection::new(
        profile,
        Bounds {
            horizon: horizon.height,
            target: target.height,
            tip: tip.height,
        },
    )?;
    selection
        .validate_source(source, crate::keep::SourceSchema::Public)
        .await?;
    Ok(SourceReport {
        network: profile.network.clone(),
        state: ServingState {
            horizon,
            target,
            tip,
            retention_seconds: options.retention_seconds,
        },
        settings,
        source_header_count: count,
        bounds_read_seconds,
        horizon_read_seconds: horizon_started.elapsed().as_secs_f64(),
        horizon_point_queries: point_queries,
        covered_seconds: covered.get(0),
        coverage_shorter_than_retention: covered.get(1),
        explicit_horizon: options.horizon_block.is_some(),
        horizon_clamped_to_source_start: clamped,
        converted_at: covered.get(2),
        families: BTreeMap::new(),
        longest_family_seconds: 0.0,
    })
}

async fn supply(
    source: &Transaction<'_>,
    profile: &Profile,
    target: u32,
) -> Result<BTreeMap<String, String>, Error> {
    let units = profile
        .assets
        .values()
        .map(|asset| (asset.unit(), asset))
        .collect::<BTreeMap<_, _>>();
    let policies = units
        .values()
        .map(|a| hex::decode(&a.policy).map_err(|_| Error::Refusal("invalid_keep_profile")))
        .collect::<Result<Vec<_>, _>>()?;
    let names = units
        .values()
        .map(|a| hex::decode(&a.name).map_err(|_| Error::Refusal("invalid_keep_profile")))
        .collect::<Result<Vec<_>, _>>()?;
    let rows=source.query("WITH fixed AS(SELECT * FROM unnest($1::bytea[],$2::bytea[]) f(policy,name)) SELECT encode(f.policy,'hex')||encode(f.name,'hex'),trunc(coalesce(sum(a.quantity) FILTER(WHERE o.id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM public.tx_in i WHERE i.tx_out_id=o.tx_id AND i.tx_out_index=o.index AND i.id BETWEEN 0 AND $3::bigint*16777216+16777215)),0))::text FROM fixed f LEFT JOIN public.multi_asset m ON m.policy=f.policy AND m.name=f.name LEFT JOIN public.ma_tx_out a ON a.ident=m.id AND a.id BETWEEN 0 AND $3::bigint*68719476736+68719476735 LEFT JOIN public.tx_out o ON o.id=a.tx_out_id AND o.id BETWEEN 0 AND $3::bigint*16777216+16777215 GROUP BY f.policy,f.name ORDER BY f.policy,f.name",&[&policies,&names,&i64::from(target)]).await?;
    Ok(rows.into_iter().map(|r| (r.get(0), r.get(1))).collect())
}

struct Batch {
    json: String,
    rows: usize,
    markers: Vec<i64>,
    batches: u64,
}
impl Batch {
    fn new() -> Self {
        Self {
            json: "[".to_owned(),
            rows: 0,
            markers: Vec::new(),
            batches: 0,
        }
    }
    async fn flush(&mut self, tx: &Transaction<'_>, table: &str) -> Result<(), Error> {
        if self.rows == 0 {
            return Ok(());
        }
        self.json.push(']');
        let count=tx.execute(&format!("INSERT INTO serve.{table} SELECT * FROM jsonb_populate_recordset(NULL::serve.{table},$1::text::jsonb)"),&[&self.json]).await?;
        if count != self.rows as u64 {
            return Err(Error::Refusal("conversion_batch_row_mismatch"));
        }
        if !self.markers.is_empty() {
            tx.execute(
                "INSERT INTO dsa.consumed_before_horizon SELECT unnest($1::bigint[])",
                &[&self.markers],
            )
            .await?;
        }
        self.json.clear();
        self.json.push('[');
        self.rows = 0;
        self.markers.clear();
        self.batches += 1;
        Ok(())
    }
}

impl Store {
    /// The caller retains the source transaction for reference-answer capture.
    /// Finish/commit source reads before `StagedConversion::finish` if possible.
    pub async fn stage_conversion<'a>(
        &'a mut self,
        profile: &Profile,
        source: &Transaction<'_>,
        options: &ConvertOptions,
    ) -> Result<StagedConversion<'a>, Error> {
        options.validate()?;
        self.prepare_destination(profile, "convert_failed").await?;
        let mut report = source_report(source, profile, options).await?;
        let amounts = supply(source, profile, report.state.target.height).await?;
        let selection = Selection::new(
            profile,
            Bounds {
                horizon: report.state.horizon.height,
                target: report.state.target.height,
                tip: report.state.tip.height,
            },
        )?;
        let tx = self.writer_transaction().await?;
        for line in include_str!("indexes.sql")
            .lines()
            .filter(|s| s.starts_with("CREATE"))
        {
            let name = line
                .split("INDEX ")
                .nth(1)
                .and_then(|s| s.split_whitespace().next())
                .ok_or(Error::Refusal("invalid_index_catalogue"))?;
            tx.batch_execute(&format!("DROP INDEX IF EXISTS serve.{name}"))
                .await?;
        }
        let mut families = BTreeMap::new();
        let mut markers = FamilyHasher::new(MARKERS);
        for table in NODE_RELATIONS {
            let sql = selection.conversion_rows_sql(table)?;
            let started = Instant::now();
            let rows = source.query_raw(&sql, selection.parameters()).await?;
            pin_mut!(rows);
            let mut digest = FamilyHasher::new(table);
            let mut batch = Batch::new();
            let (mut count, mut bytes) = (0, 0);
            while let Some(row) = rows.next().await {
                let row = row?;
                let original: &str = row.try_get(0)?;
                let canonical: &str = row.try_get(1)?;
                let key: i64 = row.try_get(2)?;
                let marker: Option<&str> = row.try_get(3)?;
                if batch.rows > 0
                    && (batch.rows >= options.batch_rows
                        || batch.json.len() + original.len() + 2 > options.batch_bytes)
                {
                    batch.flush(&tx, table).await?;
                }
                digest.row(canonical.as_bytes());
                count += 1;
                bytes += original.len() as u64;
                if batch.rows > 0 {
                    batch.json.push(',');
                }
                batch.json.push_str(original);
                batch.rows += 1;
                if let Some(marker) = marker {
                    markers.row(marker.as_bytes());
                    batch.markers.push(key);
                }
                if batch.rows >= options.batch_rows || batch.json.len() + 1 >= options.batch_bytes {
                    batch.flush(&tx, table).await?;
                }
            }
            batch.flush(&tx, table).await?;
            let elapsed = started.elapsed().as_secs_f64();
            report.longest_family_seconds = report.longest_family_seconds.max(elapsed);
            report.families.insert(
                table.to_owned(),
                FamilyCost {
                    rows: count,
                    jsonb_utf8_bytes: bytes,
                    batches: batch.batches,
                    elapsed_seconds: elapsed,
                    sql_sha256: format!("{:x}", Sha256::digest(sql.as_bytes())),
                },
            );
            families.insert(table.to_owned(), digest.finish());
        }
        Ok(StagedConversion {
            transaction: tx,
            profile_hash: profile.hash()?,
            source: report,
            families,
            protected_state: markers.finish(),
            supply: amounts,
        })
    }

    pub async fn convert(
        &mut self,
        profile: &Profile,
        source: &mut LegacySource,
        options: &ConvertOptions,
    ) -> Result<Conversion, Error> {
        let snapshot = source.snapshot().await?;
        let stage = self.stage_conversion(profile, &snapshot, options).await?;
        snapshot.commit().await?;
        let mut converted = stage.finish(profile).await?;
        if let Some(path) = &options.snapshot {
            converted.snapshot = Some(
                self.snapshot(profile, path)
                    .await
                    .map_err(|error| Error::ConvertedSnapshot(Box::new(error)))?,
            );
        }
        Ok(converted)
    }
}
