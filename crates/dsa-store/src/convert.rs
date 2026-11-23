//! Convert a serving 0.4.x database (schema 5) through its public reader.
//!
//! The source session is read-only (`default_transaction_read_only=on`) and
//! all reads (facts, spent-before-H state, supply, anchors) come from one
//! REPEATABLE READ READ ONLY transaction. The reader must hold exactly the
//! legacy grant: the eleven gated views and the legacy gate, nothing else.
//! The network is proven by the profile's pinned start header or by a held
//! asset of a policy only this network's profile uses.
//!
//! The newest source header P sets the target T = P - k. The default horizon
//! is the newest header at or before target time minus the retention, found
//! by bisection on header times; a shorter source history clamps it to its
//! continuous beginning, and `--horizon-block` overrides it. Every height H…T
//! counts as first served at publication, so the full retention follows.
//!
//! Bounded binary COPY pages fill a private temporary copy in the destination
//! transaction; the source digests are checked again on the copy. The keep
//! rule is then evaluated on the copy in stages, and the kept rows are
//! inserted with their original IDs and payloads. Digests, the spent-before-H
//! state, closure and supply are verified before the gate opens. The whole
//! load is one destination transaction that holds the served tables, so a
//! reader granted before a conversion waits for it. A failure rolls back and
//! leaves a closed `convert_failed` layout that a new load accepts.

use crate::{
    Error, NODE_RELATIONS, Store,
    keep::Selection,
    load,
    snapshot::Description,
    verify::{self, Checks, MARKERS, Position, ServingState, Verification},
};
use dsa_chain::{digest::root, keep::Bounds, profiles::Profile};
use serde::{Deserialize, Serialize};
use serde_json::json;

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
    /// Source transport costs, before local retention; all reads use one RR snapshot.
    pub raw_families: BTreeMap<String, crate::conversion_stage::RawFamilyCost>,
    pub staging_peak_bytes: i64,
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
    pub local_selection_seconds: f64,
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
    raw: crate::conversion_stage::RawState,
    selection: Selection,
    options: ConvertOptions,
    supply: BTreeMap<String, String>,
    destination: DestinationCost,
    commit_sent: std::sync::Arc<std::sync::atomic::AtomicBool>,
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
        let selected = crate::conversion_stage::select(
            tx,
            &self.source.state,
            &self.options,
            &self.raw,
            &self.selection,
        )
        .await?;
        self.destination.local_selection_seconds = finished.elapsed().as_secs_f64();
        self.source.staging_peak_bytes =
            self.source.staging_peak_bytes.max(selected.temporary_bytes);
        self.source.families = selected.costs;
        self.source.longest_family_seconds = self
            .source
            .families
            .values()
            .map(|c| c.elapsed_seconds)
            .fold(0.0, f64::max);
        let families = selected.families;
        let protected_state = selected.markers;
        let state = &self.source.state;
        load::write_position(tx, state).await?;
        load::write_supply(tx, &self.supply).await?;
        let index_started = Instant::now();
        tx.batch_execute(include_str!("indexes.sql")).await?;
        load::analyze_and_sequences(tx).await?;
        self.destination.index_analyze_sequence_seconds = index_started.elapsed().as_secs_f64();
        let verify_started = Instant::now();
        let verified = verify::verify(tx, profile, Checks::LOAD).await?;
        self.destination.verify_seconds = verify_started.elapsed().as_secs_f64();
        let selected = Verification {
            families: families.clone(),
            content_digest: root(&families),
            state_digest: root(&BTreeMap::from([(
                MARKERS.to_owned(),
                protected_state.clone(),
            )])),
            protected_state,
            supply: self.supply.clone(),
            ..verified.clone()
        };
        verify::same(&selected, &verified, "conversion_source_digest_mismatch")?;
        let origin = json!({"kind":"converted","source_kind":"legacy_schema5","source_network":self.source.network,"source_target_height":state.target.height,"source_target_hash":state.target.hash,"source_target_time":state.target.time,"source_tip_height":state.tip.height,"source_tip_hash":state.tip.hash,"source_tip_time":state.tip.time,"source_created_at":self.source.converted_at,"source_horizon_height":state.horizon.height,"source_horizon_hash":state.horizon.hash,"source_horizon_time":state.horizon.time,"initial_covered_seconds":self.source.covered_seconds,"initial_coverage_shorter_than_retention":self.source.coverage_shorter_than_retention});
        load::publish_state(tx, profile, state, &origin).await?;
        let commit_started = Instant::now();
        self.commit_sent
            .store(true, std::sync::atomic::Ordering::SeqCst);
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

async fn settings(source: &Transaction<'_>) -> Result<SourceSettings, Error> {
    let row = source
        .query_one(
            "SELECT current_setting('server_version_num')::integer,current_setting('work_mem'),
             current_setting('statement_timeout'),current_setting('idle_in_transaction_session_timeout'),
             current_setting('default_transaction_read_only'),current_setting('transaction_read_only'),
             current_setting('transaction_isolation'),
             coalesce((SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()),false)",
            &[],
        )
        .await?;
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
    let row=source.query_one("SELECT current_user::text,r.rolcanlogin AND NOT (r.rolsuper OR r.rolcreatedb OR r.rolcreaterole \
        OR r.rolreplication OR r.rolbypassrls) AND r.oid<>d.datdba FROM pg_roles r CROSS \
        JOIN pg_database d WHERE r.rolname=current_user AND d.datname=current_database()",&[]).await?;
    if !row.get::<_, bool>(1) {
        return Err(Error::Refusal("invalid_source_reader_role"));
    }
    let role: String = row.get(0);
    let gate = source
        .query_opt(
            "SELECT p.oid,p.prosecdef AND p.provolatile='s' AND p.prorettype=16 FROM pg_proc p \
        JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='chain' AND p.proname='gate' \
        AND p.pronargs=0 AND p.prokind='f'",
            &[],
        )
        .await?
        .ok_or(Error::Refusal("unsupported_legacy_schema"))?;
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

pub(crate) fn columns(table: &str) -> Vec<(&'static str, &'static str, &'static str)> {
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
        let actual=source.query("SELECT a.attname::text,t.typname::text,n.nspname::text FROM pg_class c \
            JOIN pg_namespace relation_schema ON relation_schema.oid=c.relnamespace \
            JOIN pg_attribute a ON a.attrelid=c.oid JOIN pg_type t ON t.oid=a.atttypid \
            JOIN pg_namespace n ON n.oid=t.typnamespace WHERE relation_schema.nspname='public' \
            AND c.relname=$1 AND c.relkind='v' AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum",&[&table]).await?;
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
    let count: i64 = source
        .query_one(
            "SELECT count(DISTINCT r.ev_class) FROM pg_rewrite r \
        JOIN pg_depend d ON d.classid='pg_rewrite'::regclass AND d.objid=r.oid \
        AND d.refclassid='pg_proc'::regclass AND d.refobjid=$1 JOIN pg_class c ON c.oid=r.ev_class \
        JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' \
        AND c.relname=ANY($2::text[])",
            &[&gate, &tables],
        )
        .await?
        .get(0);
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
        .iter()
        .map(|(_, asset)| asset)
        .filter(|a| {
            !known
                .iter()
                .filter(|p| p.network != profile.network)
                .any(|p| p.assets.iter().any(|(_, other)| other.policy == a.policy))
        })
        .map(|a| hex::decode(&a.policy).map_err(|_| Error::Refusal("invalid_keep_profile")))
        .collect::<Result<Vec<_>, _>>()?;
    let positive: bool = source.query_one("SELECT EXISTS(SELECT 1 FROM public.multi_asset m JOIN public.ma_tx_out a ON a.ident=m.id \
        JOIN public.tx_out o ON o.id=a.tx_out_id JOIN public.tx t ON t.id=o.tx_id \
        WHERE m.policy=ANY($1::bytea[]) AND a.quantity>0 AND a.id BETWEEN 0 \
        AND $2::bigint*68719476736+68719476735 AND o.id BETWEEN 0 AND $2::bigint*16777216+16777215 \
        AND t.id BETWEEN 0 AND $2::bigint*4096+4095)", &[&policies, &i64::from(tip)]).await?.get(0);
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
    let covered=source.query_one("SELECT extract(epoch FROM ($1::text::timestamptz-$2::text::timestamptz))::text,extract(epoch \
        FROM ($1::text::timestamptz-$2::text::timestamptz))<$3::bigint,to_char(transaction_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')",&[&target.time,&horizon.time,&options.retention_seconds]).await?;
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
        raw_families: BTreeMap::new(),
        staging_peak_bytes: 0,
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
    let started = Instant::now();
    // A LIMIT can otherwise sort an unbounded bitmap/heap scan under stale
    // statistics. Scope ordered-index paths to this project-owned operation;
    // restore the caller's flags before family or native node statements.
    let planner=source.query_one("SELECT current_setting('enable_seqscan'),current_setting('enable_bitmapscan'),current_setting('enable_indexscan'),current_setting('enable_indexonlyscan')",&[]).await?;
    let planner_before = (0..4)
        .map(|n| planner.get::<_, String>(n))
        .collect::<Vec<_>>();
    let set_planner = "SELECT set_config('enable_seqscan',$1,true),set_config('enable_bitmapscan',$2,true),set_config('enable_indexscan',$3,true),set_config('enable_indexonlyscan',$4,true)";
    source
        .query_one(set_planner, &[&"off", &"off", &"on", &"on"])
        .await?;
    let mut after_id = -1_i64;
    let mut cost = SupplyCost::default();
    eprintln!(
        "{}",
        json!({"event":"source_supply_started","operation":"convert","target":target,"planner_before":planner_before})
    );
    loop {
        let previous = units.iter().map(|u| amounts[u].clone()).collect::<Vec<_>>();
        let page_started = Instant::now();
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
        match last {
            Some(id) => after_id = id,
            None => break,
        }
    }
    let restored = source
        .query_one(
            set_planner,
            &[
                &planner_before[0],
                &planner_before[1],
                &planner_before[2],
                &planner_before[3],
            ],
        )
        .await?;
    if (0..4).any(|n| restored.get::<_, String>(n) != planner_before[n]) {
        return Err(Error::Refusal("source_supply_planner_restore_failed"));
    }
    eprintln!(
        "{}",
        json!({"event":"source_supply_finished","operation":"convert","batches":cost.batches,"rows":cost.rows,"elapsed_seconds":started.elapsed().as_secs_f64(),"longest_page_seconds":cost.longest_seconds,"planner_restored":planner_before})
    );
    Ok((amounts, cost))
}

impl Store {
    /// The caller retains the source transaction for reference-answer capture.
    /// Capture and commit source reads before `StagedConversion::finish`, whose
    /// local keep/digest/index work uses only the verified private TEMP copy.
    pub async fn stage_conversion<'a>(
        &'a mut self,
        profile: &Profile,
        source: &Transaction<'_>,
        options: &ConvertOptions,
    ) -> Result<StagedConversion<'a>, Error> {
        options.validate()?;
        // Refuse an occupied destination before reading the source; the load
        // checks it again under its locks.
        self.destination(profile).await?;
        let mut report = source_report(source, profile, options).await?;
        let supply_started = Instant::now();
        let (amounts, supply_cost) = supply(source, profile, report.state.target.height).await?;
        report.supply_read_seconds = supply_started.elapsed().as_secs_f64();
        report.supply_batches = supply_cost.batches;
        report.supply_rows = supply_cost.rows;
        report.longest_supply_page_seconds = supply_cost.longest_seconds;
        let prepare_started = Instant::now();
        self.prepare_load(profile, "convert_failed").await?;
        let selection = Selection::new(
            profile,
            Bounds {
                horizon: report.state.horizon.height,
                target: report.state.target.height,
                tip: report.state.tip.height,
            },
        )?;
        let commit_sent = self.cancellation.commit_sent.clone();
        let tx = self.writer_transaction().await?;
        load::lock_empty(&tx, "ACCESS EXCLUSIVE", "convert_failed").await?;
        for name in load::secondary_index_names() {
            tx.batch_execute(&format!("DROP INDEX IF EXISTS serve.{name}"))
                .await?;
        }
        let destination = DestinationCost {
            prepare_seconds: prepare_started.elapsed().as_secs_f64(),
            ..Default::default()
        };
        let raw = crate::conversion_stage::stage(source, &tx, &report.state, options).await?;
        report.raw_families = raw.costs.clone();
        report.staging_peak_bytes = raw.temporary_bytes;
        Ok(StagedConversion {
            transaction: tx,
            profile_hash: profile.hash()?,
            source: report,
            raw,
            selection,
            options: options.clone(),
            supply: amounts,
            destination,
            commit_sent,
        })
    }

    pub async fn convert(
        &mut self,
        profile: &Profile,
        source: &mut LegacySource,
        options: &ConvertOptions,
    ) -> Result<Conversion, Error> {
        let snapshot = source.snapshot().await?;
        let result = async {
            let staged = self.stage_conversion(profile, &snapshot, options).await?;
            snapshot.commit().await?;
            staged.finish(profile).await
        }
        .await;
        let mut converted = match result {
            Ok(converted) => converted,
            Err(error) => {
                self.release_writer().await;
                return Err(error);
            }
        };
        self.snapshot_converted(profile, &mut converted, options)
            .await?;
        Ok(converted)
    }

    /// Writes the snapshot file a conversion asked for. The database already
    /// serves; a failure names the converted digests.
    pub(crate) async fn snapshot_converted(
        &mut self,
        profile: &Profile,
        converted: &mut Conversion,
        options: &ConvertOptions,
    ) -> Result<(), Error> {
        if let Some(path) = &options.snapshot {
            converted.snapshot = Some(self.snapshot(profile, path).await.map_err(|error| {
                Error::ConvertedSnapshot {
                    error: Box::new(error),
                    content_digest: converted.verified.content_digest.clone(),
                    state_digest: converted.verified.state_digest.clone(),
                }
            })?);
        }
        Ok(())
    }
}
