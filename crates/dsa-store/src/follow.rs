//! The follower's writes. One update is one transaction under the writer
//! lock: it checks that it was built on the stored target and tip, replaces
//! the headers above its fork point, inserts the rows of the blocks that
//! became final, records their first-served time, the supply change, the
//! unspent outputs at the profile addresses, the call counts and the first
//! observations, moves the position and, when asked, advances the horizon.
//! Rows travel as arrays, so the number of statements of an update does not
//! grow with its size.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Instant,
};

use chrono::{NaiveDate, NaiveDateTime};
use dsa_chain::{
    block::{Held, Rows},
    ids,
    observation::{Candidate, Source},
    profiles::Profile,
    transaction::{Asset, Outpoint},
};
use serde::Serialize;
use serde_json::{Value, json};
use tokio_postgres::{Row, Transaction, types::ToSql};

use crate::{
    Error, Store,
    horizon::{self, Advance},
    verify::Position,
};

/// The stored unspent outputs at each of the addresses in `$1`: no spend
/// names them and they are not recorded as spent before the horizon.
pub(crate) const UNSPENT_COUNTS: &str = "SELECT a.address,(SELECT count(*) FROM serve.tx_out o
 WHERE o.address=a.address
 AND NOT EXISTS(SELECT 1 FROM serve.tx_in i WHERE i.tx_out_id=o.tx_id AND i.tx_out_index=o.index)
 AND NOT EXISTS(SELECT 1 FROM dsa.consumed_before_horizon c WHERE c.tx_out_id=o.id))
 FROM (SELECT DISTINCT unnest($1::text[]) AS address) a";

/// The distinct addresses of the profile, sorted.
pub(crate) fn profile_addresses(profile: &Profile) -> Vec<String> {
    profile
        .addresses
        .iter()
        .map(|(_, address)| address.to_owned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// The stored position as the follower starts from it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FollowState {
    pub target: Position,
    pub tip: Position,
    pub horizon: Position,
    pub retention_seconds: i64,
    pub serving: bool,
    pub reason: String,
}

/// A block by height and hash (lowercase hexadecimal).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HeaderRef {
    pub height: u32,
    pub hash: String,
}

/// A header as `serve.block` stores it. `time` is UTC.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BlockRow {
    pub height: u32,
    pub hash: String,
    pub epoch: u32,
    pub slot: u64,
    pub time: NaiveDateTime,
    pub transaction_count: u32,
}

/// A block that becomes final with this update, and the rows
/// [`dsa_chain::block::process_block`] built for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalBlock {
    pub height: u32,
    pub hash: String,
    pub transaction_count: u32,
    pub rows: Rows,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EpochParamRow {
    pub epoch: u32,
    /// Lowercase hexadecimal, 32 bytes.
    pub nonce: String,
    pub max_tx_size: u32,
}

/// An observation candidate of a final block of the update, with the
/// provider answers its sources name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Observation {
    pub height: u32,
    pub candidate: Candidate,
    pub answers: BTreeMap<Source, Vec<u8>>,
}

/// One step of the follower. `target` and `tip` are the stored positions the
/// update was built on; `fork_point` is between them, and the stored headers
/// above it are replaced by `headers`, which run from `fork_point + 1` to the
/// new tip. `blocks` run from the stored target plus one to the new target,
/// and the new tip is the new target plus the security parameter.
#[derive(Debug, Clone, Copy)]
pub struct Update<'a> {
    pub target: &'a HeaderRef,
    pub tip: &'a HeaderRef,
    pub fork_point: u32,
    pub headers: &'a [BlockRow],
    pub blocks: &'a [FinalBlock],
    /// Parameters of epochs that are not stored yet.
    pub epoch_params: &'a [EpochParamRow],
    /// Net mint of the blocks per fixed unit.
    pub mint_delta: &'a BTreeMap<String, i128>,
    /// Calls sent per UTC day, as absolute counts.
    pub calls: &'a [(NaiveDate, i64)],
    pub observations: &'a [Observation],
    /// Advance the horizon to this height after the update.
    pub prune_to: Option<u32>,
}

/// What one update wrote.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct UpdateReport {
    pub target: u32,
    pub tip: u32,
    pub horizon: u32,
    pub blocks: u32,
    pub headers: u32,
    /// Rows inserted per table.
    pub inserted: BTreeMap<String, u64>,
    /// Rows deleted per table: replaced headers, the horizon advance and the
    /// keep rule applied again at the same horizon.
    pub deleted: BTreeMap<String, u64>,
    pub insert_seconds: f64,
    pub advance_seconds: Option<f64>,
    /// The update brought the window's first spend, so the keep rule was
    /// applied again at the same horizon: the newest spend below the horizon
    /// is kept only while the window holds none.
    pub keep_reapplied_seconds: Option<f64>,
    /// Shapes recorded as a first observation by this update.
    pub observations_stored: Vec<String>,
    /// Candidates that could not be recorded; the update committed anyway.
    pub observation_failures: Vec<Value>,
    /// Statements the update ran, the horizon advance counted as one.
    pub statements: usize,
}

/// Test instrumentation for [`Store::apply_update`] and
/// [`Store::held_outputs`]: fail after a given number of statements, or record
/// the `EXPLAIN (ANALYZE, BUFFERS)` plan of every statement. Not a setting.
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct Probe {
    pub fail_after: Option<usize>,
    pub explain: bool,
    pub plans: Vec<(String, Value)>,
}

fn bytes(text: &str, length: Option<usize>) -> Result<Vec<u8>, Error> {
    hex::decode(text)
        .ok()
        .filter(|value| length.is_none_or(|length| value.len() == length))
        .ok_or(Error::Refusal("update_invalid_row"))
}

fn height(value: i64) -> Result<u32, Error> {
    u32::try_from(value).map_err(|_| Error::Refusal("invalid_serving_position"))
}

/// Counts statements, applies the probe and records plans.
struct Steps {
    count: usize,
    fail_after: Option<usize>,
    explain: bool,
    plans: Vec<(String, Value)>,
}

impl Steps {
    fn step(&mut self) -> Result<(), Error> {
        self.count += 1;
        if self.fail_after == Some(self.count) {
            return Err(Error::Refusal("injected_failure"));
        }
        Ok(())
    }

    async fn explain(
        &mut self,
        transaction: &Transaction<'_>,
        label: &str,
        sql: &str,
        parameters: &[&(dyn ToSql + Sync)],
    ) -> Result<(), Error> {
        if !self.explain {
            return Ok(());
        }
        transaction.batch_execute("SAVEPOINT explain").await?;
        let plan: Value = transaction
            .query_one(
                &format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}"),
                parameters,
            )
            .await?
            .get(0);
        transaction
            .batch_execute("ROLLBACK TO SAVEPOINT explain")
            .await?;
        self.plans.push((label.to_owned(), plan));
        Ok(())
    }

    async fn query(
        &mut self,
        transaction: &Transaction<'_>,
        label: &str,
        sql: &str,
        parameters: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, Error> {
        self.explain(transaction, label, sql, parameters).await?;
        let rows = transaction.query(sql, parameters).await?;
        self.step()?;
        Ok(rows)
    }

    async fn execute(
        &mut self,
        transaction: &Transaction<'_>,
        label: &str,
        sql: &str,
        parameters: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, Error> {
        self.explain(transaction, label, sql, parameters).await?;
        let count = transaction.execute(sql, parameters).await?;
        self.step()?;
        Ok(count)
    }
}

/// The new position an update leads to, checked without the database.
struct Shape {
    target: u32,
    tip: u32,
    /// Final heights whose header is the stored one: (stored target, min(new
    /// target, fork point)].
    stored_final: Option<(u32, u32)>,
}

fn check_shape(update: &Update<'_>, k: u32, fixed: &BTreeSet<String>) -> Result<Shape, Error> {
    let (target, tip) = (update.target.height, update.tip.height);
    if !(target..=tip).contains(&update.fork_point) {
        return Err(Error::Refusal("update_fork_point_out_of_range"));
    }
    for (offset, header) in update.headers.iter().enumerate() {
        if u64::from(header.height) != u64::from(update.fork_point) + 1 + offset as u64 {
            return Err(Error::Refusal("update_headers_not_consecutive"));
        }
        if ids::block_id(header.height).is_err() {
            return Err(Error::Refusal("update_height_out_of_range"));
        }
        bytes(&header.hash, Some(32))?;
    }
    let new_tip = update.fork_point + update.headers.len() as u32;
    if new_tip < tip {
        return Err(Error::Refusal("update_tip_regressed"));
    }
    for (offset, block) in update.blocks.iter().enumerate() {
        if u64::from(block.height) != u64::from(target) + 1 + offset as u64 {
            return Err(Error::Refusal("update_blocks_not_consecutive"));
        }
    }
    let new_target = target + update.blocks.len() as u32;
    if new_target > new_tip || new_tip - new_target != k {
        return Err(Error::Refusal("update_target_not_tip_minus_k"));
    }
    if update.prune_to.is_some_and(|height| height > new_target) {
        return Err(Error::Refusal("update_prune_out_of_range"));
    }
    for block in update.blocks {
        if block.height > update.fork_point {
            let header = &update.headers[(block.height - update.fork_point - 1) as usize];
            if header.hash != block.hash || header.transaction_count != block.transaction_count {
                return Err(Error::Refusal("update_block_header_mismatch"));
            }
        }
        if !rows_inside(&block.rows, block.height) {
            return Err(Error::Refusal("update_rows_outside_block"));
        }
    }
    if update.mint_delta.keys().any(|unit| !fixed.contains(unit)) {
        return Err(Error::Refusal("update_mint_unit_not_fixed"));
    }
    if update.calls.iter().any(|(_, count)| *count < 0) {
        return Err(Error::Refusal("update_calls_negative"));
    }
    let mut params = BTreeMap::new();
    for param in update.epoch_params {
        bytes(&param.nonce, Some(32))?;
        if params
            .insert(param.epoch, (&param.nonce, param.max_tx_size))
            .is_some_and(|other| other != (&param.nonce, param.max_tx_size))
        {
            return Err(Error::Refusal("update_epoch_param_conflict"));
        }
    }
    let last_stored = new_target.min(update.fork_point);
    Ok(Shape {
        target: new_target,
        tip: new_tip,
        stored_final: (last_stored > target).then_some((target + 1, last_stored)),
    })
}

/// Every row of a final block lies in that block.
fn rows_inside(rows: &Rows, height: u32) -> bool {
    let transaction = |id: i64| ids::transaction_position(id).is_ok_and(|(h, _)| h == height);
    let output = |id: i64| ids::output_position(id).is_ok_and(|(h, _, _)| h == height);
    rows.tx
        .iter()
        .all(|row| transaction(row.id) && row.block_id == i64::from(height))
        && rows
            .tx_out
            .iter()
            .all(|row| output(row.id) && transaction(row.tx_id))
        && rows
            .tx_in
            .iter()
            .all(|row| ids::input_position(row.id).is_ok_and(|(h, _, _)| h == height))
        && rows
            .ma_tx_out
            .iter()
            .all(|row| ids::asset_position(row.id).is_ok_and(|(h, ..)| h == height))
        && rows.tx_metadata.iter().all(|row| transaction(row.tx_id))
}

fn source_text(source: &Source) -> String {
    match source {
        Source::BlockPage(page) => format!("block_page:{page}"),
        Source::Datum(hash) => format!("datum:{hash}"),
        Source::TransactionOutputs(hash) => format!("transaction_outputs:{hash}"),
        Source::TransactionMetadata(hash) => format!("transaction_metadata:{hash}"),
    }
}

#[derive(Default)]
struct TxColumns {
    id: Vec<i64>,
    hash: Vec<Vec<u8>>,
    block: Vec<i64>,
    index: Vec<i32>,
    valid: Vec<bool>,
}

#[derive(Default)]
struct OutputColumns {
    id: Vec<i64>,
    tx: Vec<i64>,
    index: Vec<i32>,
    address: Vec<String>,
    value: Vec<String>,
    data_hash: Vec<Option<Vec<u8>>>,
}

#[derive(Default)]
struct AssetColumns {
    id: Vec<i64>,
    quantity: Vec<String>,
    output: Vec<i64>,
    policy: Vec<Vec<u8>>,
    name: Vec<Vec<u8>>,
}

#[derive(Default)]
struct InputColumns {
    id: Vec<i64>,
    tx: Vec<i64>,
    output_tx: Vec<i64>,
    output_index: Vec<i32>,
}

#[derive(Default)]
struct DatumColumns {
    hash: Vec<Vec<u8>>,
    value: Vec<Option<String>>,
}

#[derive(Default)]
struct MetadataColumns {
    id: Vec<i64>,
    key: Vec<i64>,
    tx: Vec<i64>,
    json: Vec<Option<String>>,
}

/// The rows of all final blocks as statement parameters, one array per
/// column.
#[derive(Default)]
struct Columns {
    tx: TxColumns,
    out: OutputColumns,
    asset: AssetColumns,
    input: InputColumns,
    datum: DatumColumns,
    metadata: MetadataColumns,
}

impl Columns {
    fn new(blocks: &[FinalBlock]) -> Result<Self, Error> {
        let mut c = Self::default();
        for rows in blocks.iter().map(|block| &block.rows) {
            for row in &rows.tx {
                c.tx.id.push(row.id);
                c.tx.hash.push(bytes(&row.hash, Some(32))?);
                c.tx.block.push(row.block_id);
                c.tx.index.push(i32::from(row.block_index));
                c.tx.valid.push(row.valid_contract);
            }
            for row in &rows.tx_out {
                c.out.id.push(row.id);
                c.out.tx.push(row.tx_id);
                c.out.index.push(i32::from(row.index));
                c.out.address.push(row.address.clone());
                c.out.value.push(row.value.clone());
                c.out.data_hash.push(
                    row.data_hash
                        .as_deref()
                        .map(|hash| bytes(hash, Some(32)))
                        .transpose()?,
                );
            }
            for row in &rows.ma_tx_out {
                c.asset.id.push(row.id);
                c.asset.quantity.push(row.quantity.clone());
                c.asset.output.push(row.tx_out_id);
                c.asset.policy.push(bytes(&row.policy, Some(28))?);
                c.asset.name.push(bytes(&row.name, None)?);
            }
            for row in &rows.tx_in {
                c.input.id.push(row.id);
                c.input.tx.push(row.tx_in_id);
                c.input.output_tx.push(row.tx_out_id);
                c.input.output_index.push(i32::from(row.tx_out_index));
            }
            for row in &rows.datum {
                c.datum.hash.push(bytes(&row.hash, Some(32))?);
                c.datum.value.push(row.value.clone());
            }
            for row in &rows.tx_metadata {
                c.metadata.id.push(row.id);
                c.metadata.key.push(
                    i64::try_from(row.key).map_err(|_| Error::Refusal("update_invalid_row"))?,
                );
                c.metadata.tx.push(row.tx_id);
                c.metadata.json.push(row.json.clone());
            }
        }
        Ok(c)
    }
}

const INSERT_HEADERS: &str = "INSERT INTO serve.block(id,hash,epoch_no,slot_no,block_no,time,tx_count)
 SELECT h,hash,epoch,slot,h,time,txs
 FROM unnest($1::bigint[],$2::bytea[],$3::integer[],$4::bigint[],$5::timestamp[],$6::bigint[]) u(h,hash,epoch,slot,time,txs)";
const INSERT_ASSETS: &str = "INSERT INTO serve.multi_asset(id,policy,name)
 SELECT nextval('dsa.multi_asset_id_seq'),u.policy,u.name
 FROM (SELECT DISTINCT policy,name FROM unnest($1::bytea[],$2::bytea[]) u(policy,name)) u
 WHERE NOT EXISTS(SELECT 1 FROM serve.multi_asset m WHERE m.policy=u.policy AND m.name=u.name)";
const INSERT_DATUMS: &str = "INSERT INTO serve.datum(id,hash,value)
 SELECT nextval('dsa.datum_id_seq'),u.hash,u.value::jsonb
 FROM (SELECT DISTINCT ON (hash) hash,value FROM unnest($1::bytea[],$2::text[]) u(hash,value) ORDER BY hash) u
 WHERE NOT EXISTS(SELECT 1 FROM serve.datum d WHERE d.hash=u.hash)";
const INSERT_TRANSACTIONS: &str =
    "INSERT INTO serve.tx(id,hash,block_id,block_index,valid_contract)
 SELECT * FROM unnest($1::bigint[],$2::bytea[],$3::bigint[],$4::integer[],$5::boolean[])";
const INSERT_OUTPUTS: &str = "INSERT INTO serve.tx_out(id,tx_id,index,address,value,data_hash,consumed_by_tx_id)
 SELECT id,tx_id,index::smallint,address,value::numeric,data_hash,NULL
 FROM unnest($1::bigint[],$2::bigint[],$3::integer[],$4::text[],$5::text[],$6::bytea[]) u(id,tx_id,index,address,value,data_hash)";
const INSERT_ASSET_ROWS: &str = "INSERT INTO serve.ma_tx_out(id,quantity,tx_out_id,ident)
 SELECT u.id,u.quantity::numeric,u.tx_out_id,m.id
 FROM unnest($1::bigint[],$2::text[],$3::bigint[],$4::bytea[],$5::bytea[]) u(id,quantity,tx_out_id,policy,name)
 JOIN serve.multi_asset m ON m.policy=u.policy AND m.name=u.name";
const INSERT_INPUTS: &str = "INSERT INTO serve.tx_in(id,tx_in_id,tx_out_id,tx_out_index)
 SELECT id,tx_in_id,tx_out_id,tx_out_index::smallint
 FROM unnest($1::bigint[],$2::bigint[],$3::bigint[],$4::integer[]) u(id,tx_in_id,tx_out_id,tx_out_index)";
const INSERT_METADATA: &str = "INSERT INTO serve.tx_metadata(id,key,tx_id,json)
 SELECT id,key::numeric,tx_id,json::jsonb
 FROM unnest($1::bigint[],$2::bigint[],$3::bigint[],$4::text[]) u(id,key,tx_id,json)";
const INSERT_EPOCH_PARAMS: &str = "INSERT INTO serve.epoch_param(epoch_no,nonce,max_tx_size)
 SELECT * FROM unnest($1::integer[],$2::bytea[],$3::integer[])";
const INSERT_FIRST_SERVED: &str = "INSERT INTO dsa.first_served(height,first_served_at)
 SELECT h,n.at FROM generate_series($1::integer,$2::integer) h CROSS JOIN (SELECT clock_timestamp() AS at) n";
const UPDATE_SUPPLY: &str = "WITH d AS (SELECT * FROM unnest($1::text[],$2::text[]) d(unit,delta))
 UPDATE dsa.supply s SET quantity=s.quantity+d.delta::numeric FROM d
 WHERE s.unit=d.unit AND s.quantity+d.delta::numeric>=0";
// The spent outputs are found from the update's inputs first: a filter on
// the profile addresses must not turn into a scan of every output there.
const UPDATE_UNSPENT: &str = "WITH spent AS MATERIALIZED (
  SELECT o.address FROM unnest($2::bigint[],$3::integer[]) i(tx_id,index)
  JOIN serve.tx_out o ON o.tx_id=i.tx_id AND o.index=i.index
 ), changes AS (
  SELECT address,1 AS delta FROM unnest($1::text[]) address
  UNION ALL
  SELECT address,-1 FROM spent
 ), totals AS (
  SELECT address,sum(delta)::bigint AS delta FROM changes WHERE address=ANY($4::text[]) GROUP BY address
 )
 UPDATE dsa.unspent u SET outputs=u.outputs+t.delta FROM totals t WHERE u.address=t.address AND t.delta<>0";
const UPSERT_CALLS: &str =
    "INSERT INTO dsa.calls(day,requests) SELECT * FROM unnest($1::date[],$2::bigint[])
 ON CONFLICT(day) DO UPDATE SET requests=greatest(dsa.calls.requests,EXCLUDED.requests)";
const INSERT_OBSERVATION: &str = "INSERT INTO dsa.first_observation(shape,block_height,transaction_hash,output_index,datum_hash,raw_datum_bytes,sources,answers)
 VALUES($1::text,$2::integer,$3::bytea,$4::integer::smallint,$5::bytea,$6::integer,$7::text[],$8::bytea[])
 ON CONFLICT(shape) DO NOTHING";
const UPDATE_POSITION: &str = "UPDATE dsa.position p SET target_height=t.id,target_hash=t.hash,target_time=t.time AT TIME ZONE 'UTC',
 tip_height=e.id,tip_hash=e.hash,tip_time=e.time AT TIME ZONE 'UTC',updated_at=clock_timestamp()
 FROM serve.block t,serve.block e WHERE p.id=1 AND t.id=$1::bigint AND e.id=$2::bigint";
const RECORD_UPDATE: &str = "INSERT INTO dsa.service_state(id,state,reason,last_update,updated_at)
 VALUES(1,'published','update',$1::jsonb||jsonb_build_object('at',clock_timestamp()),clock_timestamp())
 ON CONFLICT(id) DO UPDATE SET last_update=EXCLUDED.last_update,updated_at=EXCLUDED.updated_at";
const HELD_OUTPUTS: &str = "SELECT encode(t.hash,'hex'),o.index::integer,o.tx_id,o.address,encode(o.data_hash,'hex'),
 coalesce(a.policies,'{}'),coalesce(a.names,'{}'),coalesce(a.quantities,'{}')
 FROM unnest($1::bytea[],$2::integer[]) p(hash,index)
 JOIN serve.tx t ON t.hash=p.hash
 JOIN serve.tx_out o ON o.tx_id=t.id AND o.index=p.index
 LEFT JOIN LATERAL (SELECT array_agg(encode(m.policy,'hex') ORDER BY x.id) AS policies,
  array_agg(encode(m.name,'hex') ORDER BY x.id) AS names,array_agg(trunc(x.quantity)::text ORDER BY x.id) AS quantities
  FROM serve.ma_tx_out x JOIN serve.multi_asset m ON m.id=x.ident WHERE x.tx_out_id=o.id) a ON true
 WHERE NOT EXISTS(SELECT 1 FROM serve.tx_in i WHERE i.tx_out_id=o.tx_id AND i.tx_out_index=o.index)
 AND NOT EXISTS(SELECT 1 FROM dsa.consumed_before_horizon c WHERE c.tx_out_id=o.id)";

impl Store {
    #[doc(hidden)]
    pub fn probe(&self) -> std::sync::MutexGuard<'_, Probe> {
        lock(&self.probe)
    }

    /// The stored target, tip and horizon, the retention and whether the
    /// database serves. `missing_serving_position` means it holds no data.
    pub async fn follow_state(&self, profile: &Profile) -> Result<FollowState, Error> {
        self.open(profile).await?;
        let state = crate::verify::state(&self.client).await?;
        let row = self
            .client
            .query_one("SELECT serving,reason FROM dsa.position WHERE id=1", &[])
            .await?;
        Ok(FollowState {
            target: state.target,
            tip: state.tip,
            horizon: state.horizon,
            retention_seconds: state.retention_seconds,
            serving: row.get(0),
            reason: row.get(1),
        })
    }

    /// The stored headers above `height`, in height order.
    pub async fn headers_above(&self, height: u32) -> Result<Vec<BlockRow>, Error> {
        self.client
            .query(
                "SELECT id::bigint,encode(hash,'hex'),epoch_no::bigint,slot_no::bigint,time,tx_count::bigint
                 FROM serve.block WHERE id>$1::bigint ORDER BY id",
                &[&i64::from(height)],
            )
            .await?
            .iter()
            .map(|row| {
                let number = |index: usize| {
                    row.get::<_, Option<i64>>(index)
                        .ok_or(Error::Refusal("stored_header_incomplete"))
                };
                Ok(BlockRow {
                    height: height_of(number(0)?)?,
                    hash: row.get(1),
                    epoch: height_of(number(2)?)?,
                    slot: u64::try_from(number(3)?)
                        .map_err(|_| Error::Refusal("stored_header_incomplete"))?,
                    time: row.get(4),
                    transaction_count: height_of(number(5)?)?,
                })
            })
            .collect()
    }

    /// The stored, unspent outputs among `points`, with their eligible tokens
    /// and datum hash: one statement for all points.
    pub async fn held_outputs(
        &self,
        points: &[Outpoint],
    ) -> Result<BTreeMap<Outpoint, Held>, Error> {
        let hashes = points
            .iter()
            .map(|point| bytes(&point.hash, Some(32)))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| Error::Refusal("invalid_outpoint"))?;
        let indexes = points
            .iter()
            .map(|point| i32::from(point.index))
            .collect::<Vec<_>>();
        let parameters: [&(dyn ToSql + Sync); 2] = [&hashes, &indexes];
        if self.probe().explain {
            let plan: Value = self
                .client
                .query_one(
                    &format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {HELD_OUTPUTS}"),
                    &parameters,
                )
                .await?
                .get(0);
            self.probe().plans.push(("held_outputs".to_owned(), plan));
        }
        let mut held = BTreeMap::new();
        for row in self.client.query(HELD_OUTPUTS, &parameters).await? {
            let (policies, names, quantities): (Vec<String>, Vec<String>, Vec<String>) =
                (row.get(5), row.get(6), row.get(7));
            let assets = policies
                .into_iter()
                .zip(names)
                .zip(quantities)
                .map(|((policy, name), quantity)| {
                    Ok(Asset {
                        policy,
                        name,
                        quantity: quantity
                            .parse()
                            .map_err(|_| Error::Refusal("stored_quantity_invalid"))?,
                    })
                })
                .collect::<Result<Vec<_>, Error>>()?;
            let index: i32 = row.get(1);
            held.insert(
                Outpoint {
                    hash: row.get(0),
                    index: u16::try_from(index)
                        .map_err(|_| Error::Refusal("stored_output_invalid"))?,
                },
                Held {
                    transaction_id: row.get(2),
                    address: row.get(3),
                    assets,
                    data_hash: row.get(4),
                },
            );
        }
        Ok(held)
    }

    /// Which of `hashes` have a datum row.
    pub async fn stored_datums(&self, hashes: &[String]) -> Result<BTreeSet<String>, Error> {
        let values = hashes
            .iter()
            .map(|hash| bytes(hash, Some(32)))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| Error::Refusal("invalid_datum_hash"))?;
        Ok(self
            .client
            .query(
                "SELECT encode(hash,'hex') FROM serve.datum WHERE hash=ANY($1::bytea[])",
                &[&values],
            )
            .await?
            .iter()
            .map(|row| row.get(0))
            .collect())
    }

    /// Applies one update in one transaction; see [`Update`] for what it must
    /// hold. A refusal writes nothing.
    pub async fn apply_update(
        &mut self,
        profile: &Profile,
        update: &Update<'_>,
    ) -> Result<UpdateReport, Error> {
        let fixed = dsa_chain::assets::fixed_units(profile);
        let shape = check_shape(update, profile.cardano.security_parameter, &fixed)?;
        let profile_hash = profile.hash()?;
        let columns = Columns::new(update.blocks)?;
        self.lock_writer().await?;
        let (fail_after, explain) = {
            let probe = lock(&self.probe);
            (probe.fail_after, probe.explain)
        };
        let mut steps = Steps {
            count: 0,
            fail_after,
            explain,
            plans: Vec::new(),
        };
        let transaction = self.client.transaction().await?;
        let result = apply(
            &transaction,
            &mut steps,
            profile,
            &profile_hash,
            update,
            &shape,
            &columns,
        )
        .await;
        let result = match result {
            Ok(report) => transaction
                .commit()
                .await
                .map(|()| report)
                .map_err(Error::from),
            Err(error) => {
                drop(transaction);
                Err(error)
            }
        };
        lock(&self.probe).plans.extend(steps.plans);
        result
    }

    /// Stops serving: every public view raises SQLSTATE 55000 until a load.
    pub async fn close_gate(&mut self, reason: &str, detail: &Value) -> Result<(), Error> {
        let transaction = self.writer_transaction().await?;
        transaction
            .execute(
                "UPDATE dsa.position SET serving=false,reason=$1,updated_at=clock_timestamp() WHERE id=1",
                &[&reason],
            )
            .await?;
        transaction
            .execute(
                "INSERT INTO dsa.service_state(id,state,reason,detail,updated_at) VALUES(1,'stopped',$1,$2,clock_timestamp())
                 ON CONFLICT(id) DO UPDATE SET state=EXCLUDED.state,reason=EXCLUDED.reason,detail=EXCLUDED.detail,updated_at=EXCLUDED.updated_at",
                &[&reason, detail],
            )
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Records the service's state, and its last error when `error` is given.
    pub async fn record_state(
        &mut self,
        state: &str,
        reason: &str,
        detail: &Value,
        error: Option<&Value>,
    ) -> Result<(), Error> {
        self.lock_writer().await?;
        self.client
            .execute(
                "INSERT INTO dsa.service_state(id,state,reason,detail,last_error,updated_at)
                 VALUES(1,$1,$2,$3,$4::jsonb||jsonb_build_object('at',clock_timestamp()),clock_timestamp())
                 ON CONFLICT(id) DO UPDATE SET state=EXCLUDED.state,reason=EXCLUDED.reason,detail=EXCLUDED.detail,
                 last_error=coalesce(EXCLUDED.last_error,dsa.service_state.last_error),updated_at=EXCLUDED.updated_at",
                &[&state, &reason, detail, &error],
            )
            .await?;
        Ok(())
    }

    /// Stores the stake distribution of `epoch` (pool hash, amount in
    /// lovelace) when the epoch has none yet. `false`: it already had one.
    pub async fn put_epoch_stake(
        &mut self,
        epoch: u32,
        pools: &[(Vec<u8>, String)],
    ) -> Result<bool, Error> {
        let hashes = pools.iter().map(|(hash, _)| hash).collect::<Vec<_>>();
        if pools.is_empty() || hashes.iter().any(|hash| hash.len() != 28) {
            return Err(Error::Refusal("invalid_epoch_stake"));
        }
        if hashes.iter().collect::<BTreeSet<_>>().len() != hashes.len() {
            return Err(Error::Refusal("duplicate_stake_pool"));
        }
        let amounts = pools
            .iter()
            .map(|(_, amount)| amount.as_str())
            .collect::<Vec<_>>();
        if amounts
            .iter()
            .any(|amount| amount.is_empty() || !amount.bytes().all(|b| b.is_ascii_digit()))
        {
            return Err(Error::Refusal("invalid_epoch_stake"));
        }
        let epoch = i32::try_from(epoch).map_err(|_| Error::Refusal("invalid_epoch_stake"))?;
        let transaction = self.writer_transaction().await?;
        let row = transaction
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM serve.epoch_stake WHERE epoch_no=$1::integer),
                 (SELECT b.epoch_no::integer FROM dsa.position p JOIN serve.block b ON b.id=p.horizon_height WHERE p.id=1)",
                &[&epoch],
            )
            .await?;
        if row.get::<_, bool>(0) {
            return Ok(false);
        }
        let horizon_epoch: Option<i32> = row.get(1);
        if horizon_epoch.is_none_or(|horizon| epoch < horizon - 2) {
            return Err(Error::Refusal("stake_epoch_below_window"));
        }
        let hashes = hashes.into_iter().cloned().collect::<Vec<_>>();
        transaction
            .execute(
                "INSERT INTO serve.pool_hash(id,hash_raw) SELECT nextval('dsa.pool_hash_id_seq'),u.hash
                 FROM unnest($1::bytea[]) u(hash) WHERE NOT EXISTS(SELECT 1 FROM serve.pool_hash p WHERE p.hash_raw=u.hash)",
                &[&hashes],
            )
            .await?;
        transaction
            .execute(
                "INSERT INTO serve.epoch_stake(id,pool_id,amount,epoch_no)
                 SELECT nextval('dsa.epoch_stake_id_seq'),p.id,u.amount::numeric,$3::integer
                 FROM unnest($1::bytea[],$2::text[]) u(hash,amount) JOIN serve.pool_hash p ON p.hash_raw=u.hash",
                &[&hashes, &amounts, &epoch],
            )
            .await?;
        transaction.commit().await?;
        Ok(true)
    }
}

/// The epochs whose stake distribution the window keeps: from `first` (the
/// horizon's epoch minus two) on; `newest` is the newest one stored, and
/// `missing` the epochs without stake from the oldest one stored in the
/// window (`first` when none is) through the asked one, oldest first.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StakeWindow {
    pub first: u32,
    pub newest: Option<u32>,
    pub missing: Vec<u32>,
}

impl Store {
    /// The stored call counts per UTC day from `since` on.
    pub async fn calls(&self, since: NaiveDate) -> Result<Vec<(NaiveDate, i64)>, Error> {
        Ok(self
            .client
            .query(
                "SELECT day,requests FROM dsa.calls WHERE day>=$1 ORDER BY day",
                &[&since],
            )
            .await?
            .iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect())
    }

    /// Stores call counts outside an update: for each day the larger of the
    /// stored and the given count stays.
    pub async fn record_calls(&mut self, calls: &[(NaiveDate, i64)]) -> Result<(), Error> {
        if calls.iter().any(|(_, count)| *count < 0) {
            return Err(Error::Refusal("update_calls_negative"));
        }
        let (days, requests): (Vec<NaiveDate>, Vec<i64>) = calls.iter().copied().unzip();
        self.lock_writer().await?;
        self.client
            .execute(UPSERT_CALLS, &[&days, &requests])
            .await?;
        Ok(())
    }

    /// Which of `epochs` have stored parameters.
    pub async fn stored_epoch_params(&self, epochs: &[u32]) -> Result<BTreeSet<u32>, Error> {
        let wanted = epochs
            .iter()
            .map(|epoch| i32::try_from(*epoch).map_err(|_| Error::Refusal("invalid_epoch")))
            .collect::<Result<Vec<_>, _>>()?;
        self.client
            .query(
                "SELECT epoch_no::bigint FROM serve.epoch_param WHERE epoch_no=ANY($1::integer[])",
                &[&wanted],
            )
            .await?
            .iter()
            .map(|row| height_of(row.get(0)))
            .collect()
    }

    /// The epochs the window keeps stake for, the newest one stored, and the
    /// ones without stake after the oldest one stored in the window through
    /// `through`: one index probe per epoch.
    pub async fn stake_window(&self, through: u32) -> Result<StakeWindow, Error> {
        let row = self
            .client
            .query_one(
                "WITH h AS (SELECT (SELECT greatest(b.epoch_no::bigint-2,0) FROM dsa.position p JOIN serve.block b ON b.id=p.horizon_height WHERE p.id=1) AS first)
                 SELECT h.first,(SELECT max(epoch_no)::bigint FROM serve.epoch_stake),
                 ARRAY(SELECT e FROM generate_series(coalesce((SELECT min(epoch_no)::bigint FROM serve.epoch_stake WHERE epoch_no>=h.first),h.first),$1::bigint) e
                       WHERE NOT EXISTS(SELECT 1 FROM serve.epoch_stake s WHERE s.epoch_no=e) ORDER BY e)
                 FROM h",
                &[&i64::from(through)],
            )
            .await?;
        let first: i64 = row
            .get::<_, Option<i64>>(0)
            .ok_or(Error::Refusal("missing_horizon_epoch"))?;
        let missing = row
            .get::<_, Vec<i64>>(2)
            .into_iter()
            .map(height_of)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(StakeWindow {
            first: height_of(first)?,
            newest: row.get::<_, Option<i64>>(1).map(height_of).transpose()?,
            missing,
        })
    }
}

fn lock(probe: &std::sync::Mutex<Probe>) -> std::sync::MutexGuard<'_, Probe> {
    probe
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn height_of(value: i64) -> Result<u32, Error> {
    u32::try_from(value).map_err(|_| Error::Refusal("stored_header_incomplete"))
}

fn count(inserted: &mut BTreeMap<String, u64>, table: &str, rows: u64) {
    if rows > 0 {
        *inserted.entry(table.to_owned()).or_default() += rows;
    }
}

async fn apply(
    transaction: &Transaction<'_>,
    steps: &mut Steps,
    profile: &Profile,
    profile_hash: &str,
    update: &Update<'_>,
    shape: &Shape,
    columns: &Columns,
) -> Result<UpdateReport, Error> {
    let started = Instant::now();
    let position = steps
        .query(
            transaction,
            "position",
            "SELECT p.serving,p.target_height::bigint,encode(p.target_hash,'hex'),p.tip_height::bigint,
             encode(p.tip_hash,'hex'),p.horizon_height::bigint,h.epoch_no::bigint,i.profile_hash
             FROM dsa.position p CROSS JOIN dsa.identity i LEFT JOIN serve.block h ON h.id=p.horizon_height
             WHERE p.id=1 AND i.id=1 FOR UPDATE OF p",
            &[],
        )
        .await?
        .pop()
        .ok_or(Error::Refusal("missing_serving_position"))?;
    if position.get::<_, String>(7) != profile_hash {
        return Err(Error::Refusal("profile_hash_mismatch"));
    }
    let stored = |index: usize| position.get::<_, Option<i64>>(index);
    let hash = |index: usize| position.get::<_, Option<String>>(index);
    if !position.get::<_, bool>(0)
        || stored(1) != Some(i64::from(update.target.height))
        || hash(2).as_deref() != Some(update.target.hash.as_str())
        || stored(3) != Some(i64::from(update.tip.height))
        || hash(4).as_deref() != Some(update.tip.hash.as_str())
    {
        return Err(Error::Refusal("update_base_mismatch"));
    }
    let horizon = height(stored(5).ok_or(Error::Refusal("missing_serving_position"))?)?;
    let horizon_epoch = stored(6).ok_or(Error::Refusal("missing_horizon_epoch"))?;
    if update.prune_to.is_some_and(|height| height <= horizon) {
        return Err(Error::Refusal("update_prune_out_of_range"));
    }
    // The epoch of every final block, from its header after the replacement.
    let mut epochs = BTreeSet::new();
    if let Some((first, last)) = shape.stored_final {
        let headers = steps
            .query(
                transaction,
                "stored_final_headers",
                "SELECT id::bigint,encode(hash,'hex'),tx_count::bigint,epoch_no::bigint FROM serve.block
                 WHERE id BETWEEN $1::bigint AND $2::bigint ORDER BY id",
                &[&i64::from(first), &i64::from(last)],
            )
            .await?;
        let blocks = &update.blocks[..(last - first + 1) as usize];
        if headers.len() != blocks.len() {
            return Err(Error::Refusal("update_block_header_mismatch"));
        }
        for (block, header) in blocks.iter().zip(&headers) {
            if header.get::<_, i64>(0) != i64::from(block.height)
                || header.get::<_, String>(1) != block.hash
                || header.get::<_, i64>(2) != i64::from(block.transaction_count)
            {
                return Err(Error::Refusal("update_block_header_mismatch"));
            }
            epochs.insert(
                header
                    .get::<_, Option<i64>>(3)
                    .ok_or(Error::Refusal("stored_header_incomplete"))?,
            );
        }
    }
    for block in update
        .blocks
        .iter()
        .filter(|block| block.height > update.fork_point)
    {
        let header = &update.headers[(block.height - update.fork_point - 1) as usize];
        epochs.insert(i64::from(header.epoch));
    }
    let given: BTreeMap<i64, &EpochParamRow> = update
        .epoch_params
        .iter()
        .map(|param| (i64::from(param.epoch), param))
        .collect();
    if given.keys().any(|epoch| *epoch < horizon_epoch - 2) {
        return Err(Error::Refusal("update_epoch_param_below_window"));
    }
    let mut new_params = given.clone();
    if !epochs.is_empty() || !given.is_empty() {
        let wanted = epochs
            .iter()
            .chain(given.keys())
            .map(|epoch| i32::try_from(*epoch).map_err(|_| Error::Refusal("update_invalid_row")))
            .collect::<Result<Vec<_>, _>>()?;
        let rows = steps
            .query(
                transaction,
                "stored_epoch_params",
                "SELECT epoch_no::bigint,encode(nonce,'hex'),max_tx_size::bigint FROM serve.epoch_param
                 WHERE epoch_no=ANY($1::integer[])",
                &[&wanted],
            )
            .await?;
        let mut present = BTreeSet::new();
        for row in rows {
            let epoch: i64 = row.get(0);
            present.insert(epoch);
            if let Some(param) = new_params.remove(&epoch)
                && (row.get::<_, String>(1) != param.nonce
                    || row.get::<_, i64>(2) != i64::from(param.max_tx_size))
            {
                return Err(Error::Refusal("update_epoch_param_conflict"));
            }
        }
        if epochs
            .iter()
            .any(|epoch| !present.contains(epoch) && !given.contains_key(epoch))
        {
            return Err(Error::Refusal("update_epoch_param_missing"));
        }
    }

    // Whether the window already holds a spend; see `keep_reapplied_seconds`.
    let window_spent = if columns.input.id.is_empty() || update.prune_to.is_some() {
        true
    } else {
        steps
            .query(
                transaction,
                "window_spend",
                "SELECT EXISTS(SELECT 1 FROM serve.tx_in WHERE id BETWEEN $1::bigint*16777216 AND $2::bigint*16777216+16777215)",
                &[&i64::from(horizon), &i64::from(update.target.height)],
            )
            .await?[0]
            .get(0)
    };
    let mut inserted = BTreeMap::new();
    let mut deleted = BTreeMap::new();
    if update.fork_point < update.tip.height {
        let rows = steps
            .execute(
                transaction,
                "delete_headers",
                "DELETE FROM serve.block WHERE id>$1::bigint",
                &[&i64::from(update.fork_point)],
            )
            .await?;
        count(&mut deleted, "block", rows);
    }
    if !update.headers.is_empty() {
        let h = update.headers;
        let heights = h.iter().map(|x| i64::from(x.height)).collect::<Vec<_>>();
        let hashes = h
            .iter()
            .map(|x| bytes(&x.hash, Some(32)))
            .collect::<Result<Vec<_>, _>>()?;
        let epochs = h
            .iter()
            .map(|x| i32::try_from(x.epoch).map_err(|_| Error::Refusal("update_invalid_row")))
            .collect::<Result<Vec<_>, _>>()?;
        let slots = h
            .iter()
            .map(|x| i64::try_from(x.slot).map_err(|_| Error::Refusal("update_invalid_row")))
            .collect::<Result<Vec<_>, _>>()?;
        let times = h.iter().map(|x| x.time).collect::<Vec<_>>();
        let counts = h
            .iter()
            .map(|x| i64::from(x.transaction_count))
            .collect::<Vec<_>>();
        let rows = steps
            .execute(
                transaction,
                "insert_headers",
                INSERT_HEADERS,
                &[&heights, &hashes, &epochs, &slots, &times, &counts],
            )
            .await?;
        count(&mut inserted, "block", rows);
    }
    let c = columns;
    if !c.asset.id.is_empty() {
        let rows = steps
            .execute(
                transaction,
                "insert_multi_asset",
                INSERT_ASSETS,
                &[&c.asset.policy, &c.asset.name],
            )
            .await?;
        count(&mut inserted, "multi_asset", rows);
    }
    if !c.datum.hash.is_empty() {
        let rows = steps
            .execute(
                transaction,
                "insert_datum",
                INSERT_DATUMS,
                &[&c.datum.hash, &c.datum.value],
            )
            .await?;
        count(&mut inserted, "datum", rows);
    }
    if !c.tx.id.is_empty() {
        let rows = steps
            .execute(
                transaction,
                "insert_tx",
                INSERT_TRANSACTIONS,
                &[&c.tx.id, &c.tx.hash, &c.tx.block, &c.tx.index, &c.tx.valid],
            )
            .await?;
        count(&mut inserted, "tx", rows);
    }
    if !c.out.id.is_empty() {
        let rows = steps
            .execute(
                transaction,
                "insert_tx_out",
                INSERT_OUTPUTS,
                &[
                    &c.out.id,
                    &c.out.tx,
                    &c.out.index,
                    &c.out.address,
                    &c.out.value,
                    &c.out.data_hash,
                ],
            )
            .await?;
        count(&mut inserted, "tx_out", rows);
    }
    if !c.asset.id.is_empty() {
        let rows = steps
            .execute(
                transaction,
                "insert_ma_tx_out",
                INSERT_ASSET_ROWS,
                &[
                    &c.asset.id,
                    &c.asset.quantity,
                    &c.asset.output,
                    &c.asset.policy,
                    &c.asset.name,
                ],
            )
            .await?;
        if rows != c.asset.id.len() as u64 {
            return Err(Error::Refusal("update_asset_descriptor_missing"));
        }
        count(&mut inserted, "ma_tx_out", rows);
    }
    if !c.input.id.is_empty() {
        let rows = steps
            .execute(
                transaction,
                "insert_tx_in",
                INSERT_INPUTS,
                &[
                    &c.input.id,
                    &c.input.tx,
                    &c.input.output_tx,
                    &c.input.output_index,
                ],
            )
            .await?;
        count(&mut inserted, "tx_in", rows);
    }
    if !c.metadata.id.is_empty() {
        let rows = steps
            .execute(
                transaction,
                "insert_tx_metadata",
                INSERT_METADATA,
                &[
                    &c.metadata.id,
                    &c.metadata.key,
                    &c.metadata.tx,
                    &c.metadata.json,
                ],
            )
            .await?;
        count(&mut inserted, "tx_metadata", rows);
    }
    if !new_params.is_empty() {
        let epochs = new_params
            .values()
            .map(|p| i32::try_from(p.epoch).map_err(|_| Error::Refusal("update_invalid_row")))
            .collect::<Result<Vec<_>, _>>()?;
        let nonces = new_params
            .values()
            .map(|p| bytes(&p.nonce, Some(32)))
            .collect::<Result<Vec<_>, _>>()?;
        let sizes = new_params
            .values()
            .map(|p| i32::try_from(p.max_tx_size).map_err(|_| Error::Refusal("update_invalid_row")))
            .collect::<Result<Vec<_>, _>>()?;
        let rows = steps
            .execute(
                transaction,
                "insert_epoch_param",
                INSERT_EPOCH_PARAMS,
                &[&epochs, &nonces, &sizes],
            )
            .await?;
        count(&mut inserted, "epoch_param", rows);
    }
    if !update.blocks.is_empty() {
        let first = (update.target.height + 1) as i32;
        let last = shape.target as i32;
        let rows = steps
            .execute(
                transaction,
                "insert_first_served",
                INSERT_FIRST_SERVED,
                &[&first, &last],
            )
            .await?;
        count(&mut inserted, "first_served", rows);
    }
    let deltas = update
        .mint_delta
        .iter()
        .filter(|(_, delta)| **delta != 0)
        .collect::<Vec<_>>();
    if !deltas.is_empty() {
        let units = deltas
            .iter()
            .map(|(unit, _)| unit.as_str())
            .collect::<Vec<_>>();
        let amounts = deltas
            .iter()
            .map(|(_, delta)| delta.to_string())
            .collect::<Vec<_>>();
        let rows = steps
            .execute(
                transaction,
                "update_supply",
                UPDATE_SUPPLY,
                &[&units, &amounts],
            )
            .await?;
        if rows != deltas.len() as u64 {
            return Err(Error::Refusal("update_supply_negative"));
        }
    }
    if !c.out.id.is_empty() || !c.input.id.is_empty() {
        steps
            .execute(
                transaction,
                "update_unspent",
                UPDATE_UNSPENT,
                &[
                    &c.out.address,
                    &c.input.output_tx,
                    &c.input.output_index,
                    &profile_addresses(profile),
                ],
            )
            .await?;
    }
    if !update.calls.is_empty() {
        let mut days = BTreeMap::new();
        for (day, requests) in update.calls {
            let entry = days.entry(*day).or_insert(*requests);
            *entry = (*entry).max(*requests);
        }
        let (days, requests): (Vec<NaiveDate>, Vec<i64>) = days.into_iter().unzip();
        steps
            .execute(
                transaction,
                "upsert_calls",
                UPSERT_CALLS,
                &[&days, &requests],
            )
            .await?;
    }
    let (stored, failures) = observe(transaction, steps, update.observations).await?;
    let rows = steps
        .execute(
            transaction,
            "update_position",
            UPDATE_POSITION,
            &[&i64::from(shape.target), &i64::from(shape.tip)],
        )
        .await?;
    if rows != 1 {
        return Err(Error::Refusal("update_position_header_missing"));
    }
    let insert_seconds = started.elapsed().as_secs_f64();
    let mut advance_seconds = None;
    let mut keep_reapplied_seconds = None;
    let mut new_horizon = horizon;
    if !window_spent {
        let began = Instant::now();
        let bounds = dsa_chain::keep::Bounds {
            horizon,
            target: shape.target,
            tip: shape.tip,
        };
        for (table, rows) in horizon::apply_keep(transaction, profile, bounds).await? {
            count(&mut deleted, &table, rows);
        }
        steps.step()?;
        keep_reapplied_seconds = Some(began.elapsed().as_secs_f64());
    }
    if let Some(target) = update.prune_to {
        let began = Instant::now();
        let Advance {
            horizon,
            deleted: removed,
            markers_added,
        } = horizon::advance_horizon(transaction, profile, target).await?;
        steps.step()?;
        advance_seconds = Some(began.elapsed().as_secs_f64());
        new_horizon = horizon;
        for (table, rows) in removed {
            count(&mut deleted, &table, rows);
        }
        count(&mut inserted, crate::verify::MARKERS, markers_added);
    }
    let mut report = UpdateReport {
        target: shape.target,
        tip: shape.tip,
        horizon: new_horizon,
        blocks: update.blocks.len() as u32,
        headers: update.headers.len() as u32,
        inserted,
        deleted,
        insert_seconds,
        advance_seconds,
        keep_reapplied_seconds,
        observations_stored: stored,
        observation_failures: failures,
        statements: 0,
    };
    report.statements = steps.count + 1;
    steps
        .execute(
            transaction,
            "record_update",
            RECORD_UPDATE,
            &[&serde_json::to_value(&report)?],
        )
        .await?;
    Ok(report)
}

/// Records the first candidate of each shape that is not stored yet, each
/// under its own savepoint. A failure is returned, never raised.
async fn observe(
    transaction: &Transaction<'_>,
    steps: &mut Steps,
    observations: &[Observation],
) -> Result<(Vec<String>, Vec<Value>), Error> {
    let mut first = BTreeMap::new();
    for observation in observations {
        first
            .entry(observation.candidate.shape)
            .or_insert(observation);
    }
    let mut stored = Vec::new();
    let mut failures = Vec::new();
    for (shape, observation) in first {
        let candidate = &observation.candidate;
        let shape = shape.as_str();
        let answers = candidate
            .sources
            .iter()
            .map(|source| observation.answers.get(source).cloned())
            .collect::<Option<Vec<_>>>();
        let Some(answers) = answers else {
            failures.push(json!({"shape":shape,"error":"observation_answer_missing"}));
            continue;
        };
        let hash = |value: &Option<String>| {
            value
                .as_deref()
                .map(|text| bytes(text, Some(32)))
                .transpose()
        };
        let (Ok(transaction_hash), Ok(datum_hash)) =
            (hash(&candidate.transaction), hash(&candidate.datum_hash))
        else {
            failures.push(json!({"shape":shape,"error":"observation_invalid"}));
            continue;
        };
        let sources = candidate
            .sources
            .iter()
            .map(source_text)
            .collect::<Vec<_>>();
        let height = observation.height as i32;
        let index = candidate.output_index.map(i32::from);
        let length = candidate
            .raw_datum_bytes
            .map(|length| i32::try_from(length).unwrap_or(i32::MAX));
        transaction.batch_execute("SAVEPOINT observation").await?;
        steps.step()?;
        let parameters: [&(dyn ToSql + Sync); 8] = [
            &shape,
            &height,
            &transaction_hash,
            &index,
            &datum_hash,
            &length,
            &sources,
            &answers,
        ];
        steps
            .explain(
                transaction,
                "insert_first_observation",
                INSERT_OBSERVATION,
                &parameters,
            )
            .await?;
        match transaction.execute(INSERT_OBSERVATION, &parameters).await {
            Ok(rows) => {
                transaction
                    .batch_execute("RELEASE SAVEPOINT observation")
                    .await?;
                if rows == 1 {
                    stored.push(shape.to_owned());
                }
            }
            Err(error) => {
                transaction
                    .batch_execute("ROLLBACK TO SAVEPOINT observation")
                    .await?;
                let mut failure = crate::postgres_report(&error);
                failure["shape"] = json!(shape);
                failures.push(failure);
            }
        }
        steps.step()?;
    }
    Ok((stored, failures))
}
