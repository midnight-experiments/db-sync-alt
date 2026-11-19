//! Convert an unchanged serving schema5 legacy public reader snapshot.
//!
//! `LegacySource` always sets default_transaction_read_only=on. Its snapshot is
//! REPEATABLE READ READ ONLY; all source facts, sparse old-spent bits, supply and
//! anchors come from that one transaction. Catalog checks require schema5's
//! eleven public gated views and the legacy gate, with the exact reader grant:
//! no TEMP, private facts, writes, ownership, privileged role or membership.
//! Network evidence is a positive non-placeholder pinned start hash or a held
//! positive asset from a policy exclusive to this network's generated profiles.
//! Shared Preprod/Preview units alone are ambiguous. No private facts are read.
//!
//! The newest source header P determines target T=P-k, independently of the last
//! event. Indexed endpoint seeks and a bounded count verify the promised suffix,
//! accepting isolated older origin/epoch headers below a verified start anchor.
//! Default H is the newest header at/before target.time-retention, found
//! by indexed height bisection on monotone Cardano header times, without a time
//! index or a block-rate estimate. Shorter source history clamps H to its verified
//! continuous beginning, ignoring older isolated origin/epoch headers. Report chosen times, elapsed target-H seconds, clamp and read cost.
//! Explicit --horizon-block takes precedence. Retention is still a duration:
//! every retained window header gets first_served=publication time, preserving
//! the full configured period after conversion, as after restore.
//!
//! One source stream per family returns original-row JSONB text, natural-key
//! canonical text and the old-spent marker in natural-key order. Rust never
//! parses arbitrary datum/metadata JSON. Destination batches are bounded by rows
//! and bytes; one supported row larger than the byte target travels alone.
//! Rule v2 filters asset rows at every address before loading, preserving the
//! original IDs/ordinals and every retained payload. Output shells remain.
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
        if let Some(path) = &self.snapshot {
            let parent = path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            // Probe only a disposable file; never create the requested output.
            // A later filesystem failure is still possible and has a distinct
            // completed-conversion report with recovery instructions.
            tempfile::NamedTempFile::new_in(parent)
                .map_err(|_| Error::Refusal("invalid_snapshot_destination"))?;
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
        let store = Store::connect(config, root_certificate).await?;
        store.finite_read_limits().await?;
        Ok(Self { store })
    }
    pub fn client(&self) -> &tokio_postgres::Client {
        self.store.client()
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
    pub ssl: bool,
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
    /// Count only [H,P]; earlier source headers can be sparse.
    pub window_header_count: i64,
    pub source_first_header: u32,
    pub horizon_search_start_height: u32,
    pub network_evidence: String,
    pub preflight_seconds: f64,
    pub network_read_seconds: f64,
    pub supply_read_seconds: f64,
    pub supply_batches: u64,
    /// Raw asset rows paged before filtering; includes foreign units.
    pub supply_rows: u64,
    pub longest_supply_page_seconds: f64,
    pub bounds_read_seconds: f64,
    pub horizon_read_seconds: f64,
    pub horizon_point_queries: u32,
    pub horizon_prefix_chunks: u32,
    pub covered_seconds: String,
    pub coverage_shorter_than_retention: bool,
    pub explicit_horizon: bool,
    pub horizon_clamped_to_source_start: bool,
    pub converted_at: String,
    pub families: BTreeMap<String, FamilyCost>,
    pub longest_family_seconds: f64,
}
#[derive(Debug, Default, Serialize)]
pub struct DestinationCost {
    pub prepare_seconds: f64,
    pub index_analyze_sequence_seconds: f64,
    pub verify_seconds: f64,
    pub commit_seconds: f64,
    pub finish_seconds: f64,
}
#[derive(Debug, Serialize)]
pub struct Conversion {
    pub verified: Verification,
    pub source: SourceReport,
    pub snapshot: Option<Description>,
    pub destination: DestinationCost,
}

pub struct StagedConversion<'a> {
    transaction: Transaction<'a>,
    profile_hash: String,
    source: SourceReport,
    families: BTreeMap<String, FamilyDigest>,
    protected_state: FamilyDigest,
    supply: BTreeMap<String, String>,
    destination: DestinationCost,
}
impl StagedConversion<'_> {
    pub fn source(&self) -> &SourceReport {
        &self.source
    }
    pub async fn finish(mut self, profile: &Profile) -> Result<Conversion, Error> {
        if self.profile_hash != profile.hash()? {
            return Err(Error::Refusal("profile_hash_mismatch"));
        }
        let finished = Instant::now();
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
        let index_started = Instant::now();
        tx.batch_execute(include_str!("indexes.sql")).await?;
        for table in NODE_RELATIONS.into_iter().chain([MARKERS]) {
            let schema = if table == MARKERS { "dsa" } else { "serve" };
            tx.batch_execute(&format!("ANALYZE {schema}.{table}"))
                .await?;
        }
        for table in ["multi_asset", "datum", "pool_hash", "epoch_stake"] {
            tx.query_one(&format!("SELECT setval('dsa.{table}_id_seq',greatest(coalesce(max(id),0)+1,1),false) FROM serve.{table}"),&[]).await?;
        }
        self.destination.index_analyze_sequence_seconds = index_started.elapsed().as_secs_f64();
        let verify_started = Instant::now();
        let verified = verify::verify(tx, profile, false).await?;
        self.destination.verify_seconds = verify_started.elapsed().as_secs_f64();
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
        let commit_started = Instant::now();
        self.transaction.commit().await?;
        self.destination.commit_seconds = commit_started.elapsed().as_secs_f64();
        self.destination.finish_seconds = finished.elapsed().as_secs_f64();
        Ok(Conversion {
            verified,
            source: self.source,
            snapshot: None,
            destination: self.destination,
        })
    }
}

async fn write_state(tx: &Transaction<'_>, state: &ServingState) -> Result<(), Error> {
    tx.execute("UPDATE dsa.position SET horizon_height=$1::integer,horizon_hash=decode($2,'hex'),horizon_time=$3::text::timestamptz,target_height=$4::integer,target_hash=decode($5,'hex'),target_time=$6::text::timestamptz,tip_height=$7::integer,tip_hash=decode($8,'hex'),tip_time=$9::text::timestamptz,retention_seconds=$10,updated_at=now() WHERE id=1",&[&(state.horizon.height as i32),&state.horizon.hash,&state.horizon.time,&(state.target.height as i32),&state.target.hash,&state.target.time,&(state.tip.height as i32),&state.tip.hash,&state.tip.time,&state.retention_seconds]).await?;
    Ok(())
}

async fn settings(source: &Transaction<'_>) -> Result<SourceSettings, Error> {
    let row=source.query_one("SELECT current_setting('server_version_num')::integer,current_setting('work_mem'),current_setting('statement_timeout'),current_setting('idle_in_transaction_session_timeout'),current_setting('default_transaction_read_only'),current_setting('transaction_read_only'),current_setting('transaction_isolation'),coalesce((SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()),false)",&[]).await?;
    let settings = SourceSettings {
        server_version_num: row.get(0),
        work_mem: row.get(1),
        statement_timeout: row.get(2),
        idle_in_transaction_session_timeout: row.get(3),
        default_transaction_read_only: row.get(4),
        transaction_read_only: row.get(5),
        transaction_isolation: row.get(6),
        ssl: row.get(7),
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

// Both endpoint seeks have only an ID range predicate: the index returns its
// first qualifying tuple, without a filtered backward walk over event tables.
async fn endpoint(
    source: &Transaction<'_>,
    low: u32,
    high: u32,
    newest: bool,
) -> Result<Option<u32>, Error> {
    let direction = if newest { "DESC" } else { "ASC" };
    let sql = format!(
        "SELECT id FROM public.block WHERE id BETWEEN $1 AND $2 ORDER BY id {direction} LIMIT 1"
    );
    source
        .query_opt(&sql, &[&i64::from(low), &i64::from(high)])
        .await?
        .map(|r| {
            u32::try_from(r.get::<_, i64>(0))
                .map_err(|_| Error::Refusal("source_height_out_of_range"))
        })
        .transpose()
}

async fn old_enough(
    source: &Transaction<'_>,
    height: u32,
    target: &Position,
    retention: i64,
) -> Result<bool, Error> {
    Ok(source.query_one("SELECT extract(epoch FROM (($2::text::timestamptz AT TIME ZONE 'UTC')-time)) >= $3::bigint FROM public.block WHERE id=$1", &[&i64::from(height), &target.time, &retention]).await?.get(0))
}

pub(crate) async fn network_evidence(
    source: &Transaction<'_>,
    profile: &Profile,
    tip: u32,
) -> Result<(String, Option<u32>), Error> {
    if profile.start.block_hash.bytes().all(|b| b == b'0') {
        return Err(Error::Refusal("source_profile_has_no_positive_anchor"));
    }
    let anchor = profile.start.block_number;
    if anchor <= tip
        && source
            .query_opt(
                "SELECT encode(hash,'hex') FROM public.block WHERE id=$1",
                &[&i64::from(anchor)],
            )
            .await?
            .is_some_and(|r| r.get::<_, String>(0) != profile.start.block_hash)
    {
        return Err(Error::Refusal("source_start_anchor_mismatch"));
    }
    if anchor <= tip
        && source
            .query_opt(
                "SELECT id FROM public.block WHERE id=$1",
                &[&i64::from(anchor)],
            )
            .await?
            .is_some()
    {
        return Ok(("pinned_start_header".to_owned(), Some(anchor)));
    }
    // Compare policy IDs, not names: Preprod's bridge NIGHT name does not
    // distinguish it from Preview, whose cNIGHT policy is the same.
    let known: Vec<Profile> = [
        include_str!("../../dsa-chain/profiles/mainnet.json"),
        include_str!("../../dsa-chain/profiles/preprod.json"),
        include_str!("../../dsa-chain/profiles/preview.json"),
    ]
    .into_iter()
    .map(serde_json::from_str)
    .collect::<Result<_, _>>()?;
    let policies = profile
        .assets
        .values()
        .filter(|a| {
            !known
                .iter()
                .filter(|p| p.network != profile.network)
                .any(|p| p.assets.values().any(|other| other.policy == a.policy))
        })
        .map(|a| hex::decode(&a.policy).map_err(|_| Error::Refusal("invalid_keep_profile")))
        .collect::<Result<Vec<_>, _>>()?;
    let positive: bool = source.query_one("SELECT EXISTS(SELECT 1 FROM public.multi_asset m JOIN public.ma_tx_out a ON a.ident=m.id JOIN public.tx_out o ON o.id=a.tx_out_id JOIN public.tx t ON t.id=o.tx_id WHERE m.policy=ANY($1::bytea[]) AND a.quantity>0 AND a.id BETWEEN 0 AND $2::bigint*68719476736+68719476735 AND o.id BETWEEN 0 AND $2::bigint*16777216+16777215 AND t.id BETWEEN 0 AND $2::bigint*4096+4095)", &[&policies, &i64::from(tip)]).await?.get(0);
    if !positive {
        return Err(Error::Refusal("source_network_ambiguous"));
    }
    Ok(("positive_network_exclusive_policy".to_owned(), None))
}

async fn source_report(
    source: &Transaction<'_>,
    profile: &Profile,
    options: &ConvertOptions,
) -> Result<SourceReport, Error> {
    let preflight_started = Instant::now();
    let settings = settings(source).await?;
    let gate = validate_reader(source).await?;
    validate_schema(source, gate).await?;
    let preflight_seconds = preflight_started.elapsed().as_secs_f64();
    let start = Instant::now();
    let newest = endpoint(source, 0, 134217727, true)
        .await?
        .ok_or(Error::Refusal("source_has_no_headers"))?;
    let oldest = endpoint(source, 0, newest, false)
        .await?
        .ok_or(Error::Refusal("source_has_no_headers"))?;
    let tip = header(source, newest).await?;
    let target_height = newest
        .checked_sub(profile.cardano.security_parameter)
        .ok_or(Error::Refusal("source_tip_below_security_parameter"))?;
    let target = header(source, target_height).await?;
    let network_started = Instant::now();
    let (network_evidence, start_anchor) = network_evidence(source, profile, newest).await?;
    let network_read_seconds = network_started.elapsed().as_secs_f64();
    // A positive start anchor establishes a known suffix. An adjacent earlier
    // prefix may also be continuous; discover it only when target-time retention
    // needs it. Without that anchor never classify an arbitrary gap as a start.
    let mut first = start_anchor
        .filter(|h| *h <= target_height)
        .unwrap_or(oldest);
    if first > target_height {
        return Err(Error::Refusal("source_target_before_history"));
    }
    let bounds_read_seconds = start.elapsed().as_secs_f64();
    let horizon_started = Instant::now();
    let mut point_queries = 0;
    let mut prefix_chunks = 0;
    let mut clamped = false;
    if options.horizon_block.is_none() && start_anchor.is_some() {
        let mut span = 1024;
        while first > oldest {
            point_queries += 1;
            if old_enough(source, first, &target, options.retention_seconds).await? {
                break;
            }
            let low = first.saturating_sub(span).max(oldest);
            // The last consecutive group ends at first, a known present row.
            // A gap below the verified anchor is a boundary of earlier legacy
            // coverage, not a hole inside the promised suffix. Rows on the
            // other side can still be retained origin/epoch facts.
            let begin:i64=source.query_one("WITH ordered AS(SELECT id,lag(id) OVER(ORDER BY id) AS previous FROM public.block WHERE id BETWEEN $1 AND $2) SELECT max(id) FILTER(WHERE previous IS NULL OR id<>previous+1) FROM ordered", &[&i64::from(low),&i64::from(first)]).await?.get(0);
            prefix_chunks += 1;
            let begin =
                u32::try_from(begin).map_err(|_| Error::Refusal("source_height_out_of_range"))?;
            first = begin;
            if first > low {
                break;
            }
            span = span.saturating_mul(2);
        }
    }
    let horizon = if let Some(h) = options.horizon_block {
        if h > target_height {
            return Err(Error::Refusal("horizon_above_source_target"));
        }
        if h < oldest {
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
                let preceding = endpoint(source, first, mid, true)
                    .await?
                    .ok_or(Error::Refusal("source_header_gap"))?;
                if old_enough(source, preceding, &target, options.retention_seconds).await? {
                    low = mid;
                } else {
                    high = mid - 1;
                }
            }
            endpoint(source, first, low, true)
                .await?
                .ok_or(Error::Refusal("source_header_gap"))?
        }
    };
    // Combined count/min/max consumes only the requested suffix. count(*)
    // prevents PostgreSQL's MinMaxAgg shortcut without a materialized spool.
    let range = source
        .query_one(
            "SELECT count(*),min(id),max(id) FROM public.block WHERE id BETWEEN $1 AND $2",
            &[&i64::from(horizon), &i64::from(newest)],
        )
        .await?;
    let count: i64 = range.get(0);
    if count != i64::from(newest) - i64::from(horizon) + 1
        || range.get::<_, Option<i64>>(1) != Some(i64::from(horizon))
        || range.get::<_, Option<i64>>(2) != Some(i64::from(newest))
    {
        return Err(Error::Refusal("source_header_gap"));
    }
    let has_spend: bool = source.query_one("SELECT EXISTS(SELECT 1 FROM public.tx_in WHERE id BETWEEN 0 AND $1::bigint*16777216+16777215)", &[&i64::from(target_height)]).await?.get(0);
    if !has_spend {
        return Err(Error::Refusal("source_has_no_spends"));
    }
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
        window_header_count: count,
        source_first_header: oldest,
        horizon_search_start_height: first,
        network_evidence,
        preflight_seconds,
        network_read_seconds,
        supply_read_seconds: 0.0,
        supply_batches: 0,
        supply_rows: 0,
        longest_supply_page_seconds: 0.0,
        bounds_read_seconds,
        horizon_read_seconds: horizon_started.elapsed().as_secs_f64(),
        horizon_point_queries: point_queries,
        horizon_prefix_chunks: prefix_chunks,
        covered_seconds: covered.get(0),
        coverage_shorter_than_retention: covered.get(1),
        explicit_horizon: options.horizon_block.is_some(),
        horizon_clamped_to_source_start: clamped,
        converted_at: covered.get(2),
        families: BTreeMap::new(),
        longest_family_seconds: 0.0,
    })
}

#[derive(Default)]
struct SupplyCost {
    batches: u64,
    rows: u64,
    longest_seconds: f64,
}

async fn supply(
    source: &Transaction<'_>,
    profile: &Profile,
    target: u32,
) -> Result<(BTreeMap<String, String>, SupplyCost), Error> {
    let units = dsa_chain::assets::fixed_units(profile);
    let policies = units
        .iter()
        .map(|u| hex::decode(&u[..56]).map_err(|_| Error::Refusal("invalid_keep_profile")))
        .collect::<Result<Vec<_>, _>>()?;
    let names = units
        .iter()
        .map(|u| hex::decode(&u[56..]).map_err(|_| Error::Refusal("invalid_keep_profile")))
        .collect::<Result<Vec<_>, _>>()?;
    let mut amounts = units
        .iter()
        .map(|u| (u.clone(), "0".to_owned()))
        .collect::<BTreeMap<_, _>>();
    let sql = include_str!("conversion_supply.sql");
    let sql_sha256 = format!("{:x}", Sha256::digest(sql.as_bytes()));
    let started = Instant::now();
    let mut after_id = -1_i64;
    let mut cost = SupplyCost::default();
    eprintln!(
        "{}",
        json!({"event":"source_supply_started","operation":"convert","target":target,"page_rows":4096,"sql_sha256":sql_sha256})
    );
    loop {
        let previous = units.iter().map(|u| amounts[u].clone()).collect::<Vec<_>>();
        let page_started = Instant::now();
        eprintln!(
            "{}",
            json!({"event":"source_supply_page_started","operation":"convert","page":cost.batches,"after_asset_id":after_id,"target":target,"sql_sha256":sql_sha256})
        );
        let rows = source
            .query(
                sql,
                &[&policies, &names, &i64::from(target), &after_id, &previous],
            )
            .await
            .map_err(|error| Error::SourceRead {
                family: "supply",
                elapsed_seconds: page_started.elapsed().as_secs_f64(),
                error,
            })?;
        let elapsed = page_started.elapsed().as_secs_f64();
        let first = rows.first().ok_or(Error::Refusal("invalid_supply_page"))?;
        let last: Option<i64> = first.get(2);
        let count: i64 = first.get(3);
        if !(0..=4096).contains(&count)
            || (count == 0) != last.is_none()
            || last.is_some_and(|id| id <= after_id)
        {
            return Err(Error::Refusal("invalid_supply_page"));
        }
        amounts = rows.into_iter().map(|r| (r.get(0), r.get(1))).collect();
        if amounts.keys().ne(units.iter()) {
            return Err(Error::Refusal("invalid_supply_page"));
        }
        cost.batches += 1;
        cost.rows += count as u64;
        cost.longest_seconds = cost.longest_seconds.max(elapsed);
        eprintln!(
            "{}",
            json!({"event":"source_supply_page_finished","operation":"convert","page":cost.batches-1,"after_asset_id":after_id,"last_asset_id":last,"rows":count,"elapsed_seconds":elapsed,"sql_sha256":sql_sha256})
        );
        match last {
            Some(id) => after_id = id,
            None => break,
        }
    }
    eprintln!(
        "{}",
        json!({"event":"source_supply_finished","operation":"convert","batches":cost.batches,"rows":cost.rows,"elapsed_seconds":started.elapsed().as_secs_f64(),"longest_page_seconds":cost.longest_seconds,"sql_sha256":sql_sha256})
    );
    Ok((amounts, cost))
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
    /// Read-only refusal check. prepare_destination repeats its authoritative
    /// checks under the writer lock after source preflight, closing the race.
    async fn conversion_destination_preflight(&self, profile: &Profile) -> Result<(), Error> {
        let identity: bool = self
            .client
            .query_one("SELECT to_regclass('dsa.identity') IS NOT NULL", &[])
            .await?
            .get(0);
        if !identity {
            let occupied: bool = self.client.query_one("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname NOT LIKE 'pg\\_%' AND nspname NOT IN ('public','information_schema')) OR EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public') OR EXISTS(SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public') OR EXISTS(SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace WHERE n.nspname='public') OR EXISTS(SELECT 1 FROM pg_extension WHERE extname<>'plpgsql')", &[]).await?.get(0);
            return if occupied {
                Err(Error::Refusal("destination_not_empty"))
            } else {
                Ok(())
            };
        }
        self.open(profile).await?;
        let row = self.client.query_one("SELECT p.serving,i.origin->>'kind' FROM dsa.position p CROSS JOIN dsa.identity i WHERE p.id=1 AND i.id=1", &[]).await?;
        if row.get::<_, bool>(0) {
            return Err(Error::Refusal("destination_not_empty"));
        }
        let origin: Option<String> = row.get(1);
        if origin.as_deref() == Some("restore_failed") {
            return Err(Error::Refusal("destination_owned_by_failed_restore"));
        }
        if !matches!(origin.as_deref(), Some("empty" | "convert_failed")) {
            return Err(Error::Refusal("destination_not_empty"));
        }
        let mut counts = NODE_RELATIONS
            .iter()
            .map(|t| format!("(SELECT count(*) FROM serve.{t})"))
            .collect::<Vec<_>>();
        counts.extend(
            [
                "dsa.consumed_before_horizon",
                "dsa.supply",
                "dsa.first_served",
                "dsa.calls",
            ]
            .map(|t| format!("(SELECT count(*) FROM {t})")),
        );
        let occupied: i64 = self
            .client
            .query_one(&format!("SELECT {}", counts.join("+")), &[])
            .await?
            .get(0);
        if occupied != 0 {
            return Err(Error::Refusal("destination_not_empty"));
        }
        Ok(())
    }

    /// The caller retains the source transaction for reference-answer capture.
    /// Finish/commit source reads before `StagedConversion::finish` if possible.
    pub async fn stage_conversion<'a>(
        &'a mut self,
        profile: &Profile,
        source: &Transaction<'_>,
        options: &ConvertOptions,
    ) -> Result<StagedConversion<'a>, Error> {
        options.validate()?;
        self.conversion_destination_preflight(profile).await?;
        let mut report = source_report(source, profile, options).await?;
        let supply_started = Instant::now();
        let (amounts, supply_cost) = supply(source, profile, report.state.target.height).await?;
        report.supply_read_seconds = supply_started.elapsed().as_secs_f64();
        report.supply_batches = supply_cost.batches;
        report.supply_rows = supply_cost.rows;
        report.longest_supply_page_seconds = supply_cost.longest_seconds;
        let prepare_started = Instant::now();
        self.prepare_destination(profile, "convert_failed").await?;
        let destination = DestinationCost {
            prepare_seconds: prepare_started.elapsed().as_secs_f64(),
            ..Default::default()
        };
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
            let sql_sha256 = format!("{:x}", Sha256::digest(sql.as_bytes()));
            eprintln!(
                "{}",
                json!({"operation":"convert","event":"source_family_started","family":table,"sql_sha256":sql_sha256})
            );
            let rows = source
                .query_raw(&sql, selection.parameters())
                .await
                .map_err(|error| Error::SourceRead {
                    family: table,
                    elapsed_seconds: started.elapsed().as_secs_f64(),
                    error,
                })?;
            pin_mut!(rows);
            let mut digest = FamilyHasher::new(table);
            let mut batch = Batch::new();
            let (mut count, mut bytes) = (0, 0);
            while let Some(row) = rows.next().await {
                let row = row.map_err(|error| Error::SourceRead {
                    family: table,
                    elapsed_seconds: started.elapsed().as_secs_f64(),
                    error,
                })?;
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
            eprintln!(
                "{}",
                json!({"operation":"convert","event":"source_family_finished","family":table,"rows":count,"jsonb_utf8_bytes":bytes,"batches":batch.batches,"elapsed_seconds":elapsed,"sql_sha256":sql_sha256})
            );
            report.families.insert(
                table.to_owned(),
                FamilyCost {
                    rows: count,
                    jsonb_utf8_bytes: bytes,
                    batches: batch.batches,
                    elapsed_seconds: elapsed,
                    sql_sha256,
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
            destination,
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
            converted.snapshot = Some(self.snapshot(profile, path).await.map_err(|error| {
                Error::ConvertedSnapshot {
                    error: Box::new(error),
                    content_digest: converted.verified.content_digest.clone(),
                    state_digest: converted.verified.state_digest.clone(),
                }
            })?);
        }
        Ok(converted)
    }
}
