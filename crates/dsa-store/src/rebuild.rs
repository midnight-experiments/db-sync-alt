//! The database side of a build from the provider alone (`rebuild`).
//!
//! A build works in the database it fills. Its origin kind is `rebuilding`
//! and its position names the reason `rebuilding` while the gate is closed;
//! the schema `dsa_rebuild` holds the build's plan, phase and call count
//! (`progress`) and every provider result the build has parsed (`item`), so a
//! build that stops continues where it stopped and asks the provider only for
//! what it does not hold yet. A load (`restore`, `convert`) refuses such a
//! database; dropping the database discards the build.
//!
//! The rows below the horizon are written once, with the position target
//! H − 1, tip H − 1 + k and horizon H; the follower then makes the window
//! final through its ordinary updates, which this module lets run on a
//! database that builds. Publication is one transaction: the newest spend
//! below H when the window holds none (K10), the pruned form of the keep rule
//! applied once, the follower's state rows, the first-served times, `verify`
//! with every check, the open gate and the end of `dsa_rebuild`.

use std::collections::BTreeMap;

use dsa_chain::{block::Rows, keep::Bounds, profiles::Profile};
use serde_json::Value;
use tokio_postgres::Transaction;

use crate::{
    Error, Store,
    follow::{
        BlockRow, Columns, EpochParamRow, FinalBlock, INSERT_ASSET_ROWS, INSERT_ASSETS,
        INSERT_DATUMS, INSERT_EPOCH_PARAMS, INSERT_HEADERS, INSERT_INPUTS, INSERT_OUTPUTS,
        INSERT_TRANSACTIONS,
    },
    horizon, load,
    verify::{self, Checks, Position, ServingState, Verification},
};

/// The origin kind and the position's reason while a build runs.
pub const REBUILDING: &str = "rebuilding";

const SCHEMA: &str = "CREATE SCHEMA dsa_rebuild;
REVOKE ALL ON SCHEMA dsa_rebuild FROM PUBLIC;
CREATE TABLE dsa_rebuild.progress (
 id smallint PRIMARY KEY CHECK(id=1),
 plan jsonb NOT NULL,
 phase text NOT NULL,
 detail jsonb NOT NULL DEFAULT '{}'::jsonb,
 calls bigint NOT NULL DEFAULT 0 CHECK(calls>=0),
 started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE dsa_rebuild.item (
 kind text NOT NULL,
 key text NOT NULL,
 value jsonb NOT NULL,
 PRIMARY KEY(kind,key)
);";

/// What the database holds for a build.
#[derive(Debug, Clone, PartialEq)]
pub enum Building {
    /// Nothing yet: a build can start here.
    Empty,
    /// A build that stopped: its plan, phase, detail and calls so far.
    Stopped {
        plan: Value,
        phase: String,
        detail: Value,
        calls: i64,
    },
}

/// The progress record a status document shows.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct Progress {
    pub phase: String,
    pub reason: String,
    pub plan: Value,
    pub detail: Value,
    pub calls: i64,
    pub started_at: String,
    pub updated_at: String,
}

/// Rows a build writes outside the follower's updates: the rows below the
/// horizon, or the newest spend below it (K10). Descriptors carry their IDs.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BuiltRows {
    pub headers: Vec<BlockRow>,
    pub rows: Rows,
    /// (ID, policy, name), lowercase hexadecimal.
    pub descriptors: Vec<(i64, String, String)>,
    pub epoch_params: Vec<EpochParamRow>,
    /// Retained outputs spent before the horizon.
    pub markers: Vec<i64>,
}

/// The base of a build: its rows below H, the supply at H and the position.
#[derive(Debug, Clone, PartialEq)]
pub struct Base {
    pub built: BuiltRows,
    pub supply: BTreeMap<String, String>,
    pub state: ServingState,
}

fn position_of(row: &BlockRow) -> Position {
    Position {
        height: row.height,
        hash: row.hash.clone(),
        time: row.time.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string(),
    }
}

impl ServingState {
    /// The state of headers given as stored rows.
    pub fn of_headers(
        horizon: &BlockRow,
        target: &BlockRow,
        tip: &BlockRow,
        retention_seconds: i64,
    ) -> Self {
        Self {
            horizon: position_of(horizon),
            target: position_of(target),
            tip: position_of(tip),
            retention_seconds,
        }
    }
}

/// Inserts rows; with `existing` a row whose key is stored already is kept.
async fn insert(
    transaction: &Transaction<'_>,
    built: &BuiltRows,
    existing: bool,
) -> Result<(), Error> {
    let conflict = if existing {
        " ON CONFLICT DO NOTHING"
    } else {
        ""
    };
    if !built.descriptors.is_empty() {
        let ids = built.descriptors.iter().map(|d| d.0).collect::<Vec<_>>();
        let policies = built
            .descriptors
            .iter()
            .map(|d| hex::decode(&d.1).map_err(|_| Error::Refusal("rebuild_invalid_row")))
            .collect::<Result<Vec<_>, _>>()?;
        let names = built
            .descriptors
            .iter()
            .map(|d| hex::decode(&d.2).map_err(|_| Error::Refusal("rebuild_invalid_row")))
            .collect::<Result<Vec<_>, _>>()?;
        transaction
            .execute(
                "INSERT INTO serve.multi_asset(id,policy,name)
                 SELECT u.id,u.policy,u.name FROM unnest($1::bigint[],$2::bytea[],$3::bytea[]) u(id,policy,name)
                 WHERE NOT EXISTS(SELECT 1 FROM serve.multi_asset m WHERE m.policy=u.policy AND m.name=u.name)",
                &[&ids, &policies, &names],
            )
            .await?;
        transaction
            .query_one(
                "SELECT setval('dsa.multi_asset_id_seq',greatest(coalesce(max(id),0)+1,1),false) FROM serve.multi_asset",
                &[],
            )
            .await?;
    }
    if !built.headers.is_empty() {
        let h = &built.headers;
        let heights = h.iter().map(|x| i64::from(x.height)).collect::<Vec<_>>();
        let hashes = h
            .iter()
            .map(|x| hex::decode(&x.hash).map_err(|_| Error::Refusal("rebuild_invalid_row")))
            .collect::<Result<Vec<_>, _>>()?;
        let epochs = h
            .iter()
            .map(|x| i32::try_from(x.epoch).map_err(|_| Error::Refusal("rebuild_invalid_row")))
            .collect::<Result<Vec<_>, _>>()?;
        let slots = h
            .iter()
            .map(|x| i64::try_from(x.slot).map_err(|_| Error::Refusal("rebuild_invalid_row")))
            .collect::<Result<Vec<_>, _>>()?;
        let times = h.iter().map(|x| x.time).collect::<Vec<_>>();
        let counts = h
            .iter()
            .map(|x| i64::from(x.transaction_count))
            .collect::<Vec<_>>();
        transaction
            .execute(
                &format!("{INSERT_HEADERS}{conflict}"),
                &[&heights, &hashes, &epochs, &slots, &times, &counts],
            )
            .await?;
    }
    let columns = Columns::new(&[FinalBlock {
        height: 0,
        hash: String::new(),
        transaction_count: 0,
        rows: built.rows.clone(),
    }])?;
    let c = &columns;
    if !c.datum.hash.is_empty() {
        transaction
            .execute(INSERT_DATUMS, &[&c.datum.hash, &c.datum.value])
            .await?;
    }
    if !c.tx.id.is_empty() {
        transaction
            .execute(
                &format!("{INSERT_TRANSACTIONS}{conflict}"),
                &[&c.tx.id, &c.tx.hash, &c.tx.block, &c.tx.index, &c.tx.valid],
            )
            .await?;
    }
    if !c.out.id.is_empty() {
        transaction
            .execute(
                &format!("{INSERT_OUTPUTS}{conflict}"),
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
    }
    if !c.asset.id.is_empty() {
        if existing {
            // Rows added to stored ones (the newest spend below H) bring the
            // descriptors they need, numbered as the follower numbers new
            // ones.
            transaction
                .execute(INSERT_ASSETS, &[&c.asset.policy, &c.asset.name])
                .await?;
        }
        let rows = transaction
            .execute(
                &format!("{INSERT_ASSET_ROWS}{conflict}"),
                &[
                    &c.asset.id,
                    &c.asset.quantity,
                    &c.asset.output,
                    &c.asset.policy,
                    &c.asset.name,
                ],
            )
            .await?;
        let stored: i64 = transaction
            .query_one(
                "SELECT count(*) FROM serve.ma_tx_out WHERE id=ANY($1)",
                &[&c.asset.id],
            )
            .await?
            .get(0);
        if (!existing && rows != c.asset.id.len() as u64) || stored != c.asset.id.len() as i64 {
            return Err(Error::Refusal("rebuild_asset_descriptor_missing"));
        }
    }
    if !c.input.id.is_empty() {
        transaction
            .execute(
                &format!("{INSERT_INPUTS}{conflict}"),
                &[
                    &c.input.id,
                    &c.input.tx,
                    &c.input.output_tx,
                    &c.input.output_index,
                ],
            )
            .await?;
    }
    if !built.epoch_params.is_empty() {
        let p = &built.epoch_params;
        let epochs = p
            .iter()
            .map(|x| i32::try_from(x.epoch).map_err(|_| Error::Refusal("rebuild_invalid_row")))
            .collect::<Result<Vec<_>, _>>()?;
        let nonces = p
            .iter()
            .map(|x| hex::decode(&x.nonce).map_err(|_| Error::Refusal("rebuild_invalid_row")))
            .collect::<Result<Vec<_>, _>>()?;
        let sizes = p
            .iter()
            .map(|x| {
                i32::try_from(x.max_tx_size).map_err(|_| Error::Refusal("rebuild_invalid_row"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        transaction
            .execute(
                &format!("{INSERT_EPOCH_PARAMS}{conflict}"),
                &[&epochs, &nonces, &sizes],
            )
            .await?;
    }
    if !built.markers.is_empty() {
        transaction
            .execute(
                "INSERT INTO dsa.consumed_before_horizon(tx_out_id) SELECT unnest($1::bigint[]) ON CONFLICT DO NOTHING",
                &[&built.markers],
            )
            .await?;
    }
    Ok(())
}

impl Store {
    /// What the database holds for a build. A database that serves or holds
    /// anything else, or a build a check closed, is refused.
    pub async fn building(&mut self, profile: &Profile) -> Result<Building, Error> {
        let identity: bool = self
            .client
            .query_one("SELECT to_regclass('dsa.identity') IS NOT NULL", &[])
            .await?
            .get(0);
        if identity {
            self.open(profile).await?;
            let row = self
                .client
                .query_one(
                    "SELECT i.origin->>'kind',p.serving,p.reason,to_regclass('dsa_rebuild.progress') IS NOT NULL
                     FROM dsa.identity i CROSS JOIN dsa.position p WHERE i.id=1 AND p.id=1",
                    &[],
                )
                .await?;
            let (kind, serving, reason, progress): (Option<String>, bool, String, bool) =
                (row.get(0), row.get(1), row.get(2), row.get(3));
            if kind.as_deref() == Some(REBUILDING) && progress && !serving {
                if reason != REBUILDING {
                    return Err(Error::Mismatch {
                        error: "rebuild_refused",
                        item: "reason".to_owned(),
                        expected: REBUILDING.to_owned(),
                        actual: reason,
                    });
                }
                let row = self
                    .client
                    .query_one(
                        "SELECT plan,phase,detail,calls FROM dsa_rebuild.progress WHERE id=1",
                        &[],
                    )
                    .await?;
                return Ok(Building::Stopped {
                    plan: row.get(0),
                    phase: row.get(1),
                    detail: row.get(2),
                    calls: row.get(3),
                });
            }
        }
        match self.destination(profile).await? {
            load::Destination::Blank | load::Destination::Empty => Ok(Building::Empty),
            load::Destination::Leftover => Err(Error::Refusal("destination_not_empty")),
        }
    }

    /// Starts a build with `plan` in a blank or empty database.
    pub async fn start_building(&mut self, profile: &Profile, plan: &Value) -> Result<(), Error> {
        self.lock_writer().await?;
        match self.destination(profile).await? {
            load::Destination::Blank => self.create_layout(profile).await?,
            load::Destination::Empty => {}
            load::Destination::Leftover => return Err(Error::Refusal("destination_not_empty")),
        }
        let transaction = self.writer_transaction().await?;
        transaction.batch_execute(SCHEMA).await?;
        transaction
            .execute(
                "INSERT INTO dsa_rebuild.progress(id,plan,phase) VALUES(1,$1,'positions')",
                &[plan],
            )
            .await?;
        transaction
            .execute(
                "UPDATE dsa.position SET serving=false,reason=$1,updated_at=now() WHERE id=1",
                &[&REBUILDING],
            )
            .await?;
        transaction
            .execute(
                "UPDATE dsa.identity SET origin=jsonb_build_object('kind',$1::text) WHERE id=1",
                &[&REBUILDING],
            )
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Records the phase the build reached, with its detail and calls so far.
    pub async fn record_building(
        &mut self,
        phase: &str,
        detail: &Value,
        calls: i64,
    ) -> Result<(), Error> {
        self.lock_writer().await?;
        let rows = self
            .client
            .execute(
                "UPDATE dsa_rebuild.progress SET phase=$1,detail=$2,calls=greatest(calls,$3),updated_at=clock_timestamp() WHERE id=1",
                &[&phase, detail, &calls],
            )
            .await?;
        if rows != 1 {
            return Err(Error::Refusal("rebuild_progress_missing"));
        }
        Ok(())
    }

    /// The stored items of `kind`, by key.
    pub async fn building_items(&self, kind: &str) -> Result<BTreeMap<String, Value>, Error> {
        Ok(self
            .client
            .query(
                "SELECT key,value FROM dsa_rebuild.item WHERE kind=$1",
                &[&kind],
            )
            .await?
            .into_iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect())
    }

    /// Stores items of `kind`; a stored key takes the new value.
    pub async fn put_building_items(
        &mut self,
        kind: &str,
        items: &[(String, Value)],
    ) -> Result<(), Error> {
        if items.is_empty() {
            return Ok(());
        }
        self.lock_writer().await?;
        let keys = items
            .iter()
            .map(|(key, _)| key.as_str())
            .collect::<Vec<_>>();
        let values = items.iter().map(|(_, value)| value).collect::<Vec<_>>();
        self.client
            .execute(
                "INSERT INTO dsa_rebuild.item(kind,key,value) SELECT $1,k,v FROM unnest($2::text[],$3::jsonb[]) u(k,v)
                 ON CONFLICT(kind,key) DO UPDATE SET value=EXCLUDED.value",
                &[&kind, &keys, &values],
            )
            .await?;
        Ok(())
    }

    /// Discards the stored items of `kinds` and the cursors whose keys start
    /// with one of `cursors`, so that the build reads them again; with
    /// `phase` the build continues from that phase. One transaction.
    pub async fn discard_building_items(
        &mut self,
        kinds: &[&str],
        cursors: &[&str],
        phase: Option<&str>,
    ) -> Result<(), Error> {
        self.lock_writer().await?;
        let transaction = self.writer_transaction().await?;
        transaction
            .execute(
                "DELETE FROM dsa_rebuild.item WHERE kind=ANY($1)
                 OR (kind='cursor' AND EXISTS(SELECT 1 FROM unnest($2::text[]) p(prefix) WHERE starts_with(key,p.prefix)))",
                &[&kinds, &cursors],
            )
            .await?;
        if let Some(phase) = phase {
            transaction
                .execute(
                    "UPDATE dsa_rebuild.progress SET phase=$1,updated_at=clock_timestamp() WHERE id=1",
                    &[&phase],
                )
                .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    /// The number of stored items of each kind, and the stored target.
    pub async fn building_counts(&self) -> Result<(BTreeMap<String, i64>, Option<u32>), Error> {
        let counts = self
            .client
            .query(
                "SELECT kind,count(*) FROM dsa_rebuild.item GROUP BY kind",
                &[],
            )
            .await?
            .into_iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();
        let target: Option<i32> = self
            .client
            .query_one(
                "SELECT target_height::integer FROM dsa.position WHERE id=1",
                &[],
            )
            .await?
            .get(0);
        Ok((counts, target.and_then(|height| u32::try_from(height).ok())))
    }

    /// Writes the rows below the horizon, the supply at H and the position
    /// the follower starts from, in one transaction. The gate stays closed.
    pub async fn write_building_base(
        &mut self,
        profile: &Profile,
        base: &Base,
    ) -> Result<(), Error> {
        self.open(profile).await?;
        let transaction = self.writer_transaction().await?;
        let row = transaction
            .query_one(
                "SELECT p.reason,p.target_height IS NULL,NOT EXISTS(SELECT 1 FROM serve.block)
                 FROM dsa.position p WHERE p.id=1 FOR UPDATE",
                &[],
            )
            .await?;
        if row.get::<_, String>(0) != REBUILDING || !row.get::<_, bool>(1) || !row.get::<_, bool>(2)
        {
            return Err(Error::Refusal("rebuild_base_already_written"));
        }
        insert(&transaction, &base.built, false).await?;
        load::write_supply(&transaction, &base.supply).await?;
        load::write_position(&transaction, &base.state).await?;
        for table in ["datum", "pool_hash", "epoch_stake"] {
            transaction
                .query_one(
                    &format!("SELECT setval('dsa.{table}_id_seq',greatest(coalesce(max(id),0)+1,1),false) FROM serve.{table}"),
                    &[],
                )
                .await?;
        }
        transaction
            .execute(
                "UPDATE dsa_rebuild.progress SET phase='window',updated_at=clock_timestamp() WHERE id=1",
                &[],
            )
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Whether the window H…T holds a spend.
    pub async fn window_has_spend(&self, horizon: u32, target: u32) -> Result<bool, Error> {
        Ok(self
            .client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM serve.tx_in WHERE id BETWEEN $1::bigint*16777216 AND $2::bigint*16777216+16777215)",
                &[&i64::from(horizon), &i64::from(target)],
            )
            .await?
            .get(0))
    }

    /// Publishes a build that reached its target: the newest spend below the
    /// horizon when the window holds none, the keep rule applied once, the
    /// follower's state rows, first-served times now, `verify` with every
    /// check, the open gate. Nothing is served unless all of it commits.
    pub async fn publish_building(
        &mut self,
        profile: &Profile,
        expected: &ServingState,
        newest_spend: Option<&BuiltRows>,
        origin: &Value,
    ) -> Result<Verification, Error> {
        self.open(profile).await?;
        let commit_sent = self.cancellation.commit_sent.clone();
        let transaction = self.writer_transaction().await?;
        let row = transaction
            .query_one(
                "SELECT p.reason,p.serving FROM dsa.position p WHERE p.id=1 FOR UPDATE",
                &[],
            )
            .await?;
        if row.get::<_, String>(0) != REBUILDING || row.get::<_, bool>(1) {
            return Err(Error::Refusal("rebuild_not_publishable"));
        }
        let state = verify::state(&transaction).await?;
        if state.horizon.height != expected.horizon.height
            || state.target != expected.target
            || state.tip.height != expected.tip.height
        {
            return Err(Error::Mismatch {
                error: "rebuild_position_mismatch",
                item: "position".to_owned(),
                expected: serde_json::to_string(expected)?,
                actual: serde_json::to_string(&state)?,
            });
        }
        if let Some(spend) = newest_spend {
            insert(&transaction, spend, true).await?;
        }
        horizon::apply_keep(
            &transaction,
            profile,
            Bounds {
                horizon: state.horizon.height,
                target: state.target.height,
                tip: state.tip.height,
            },
        )
        .await?;
        transaction
            .batch_execute("DELETE FROM dsa.first_served;DELETE FROM dsa.unspent;DELETE FROM dsa.service_state")
            .await?;
        load::analyze_and_sequences(&transaction).await?;
        load::publish_state(&transaction, profile, &state, origin).await?;
        let verified = verify::verify(&transaction, profile, Checks::PUBLISH).await?;
        transaction
            .batch_execute("DROP SCHEMA dsa_rebuild CASCADE")
            .await?;
        commit_sent.store(true, std::sync::atomic::Ordering::SeqCst);
        transaction.commit().await?;
        Ok(verified)
    }

    /// A build's progress, when the database holds one.
    pub async fn building_progress(&self) -> Result<Option<Progress>, Error> {
        let present: bool = self
            .client
            .query_one(
                "SELECT to_regclass('dsa_rebuild.progress') IS NOT NULL",
                &[],
            )
            .await?
            .get(0);
        if !present {
            return Ok(None);
        }
        let row = self
            .client
            .query_opt(
                "SELECT g.phase,p.reason,g.plan,g.detail,g.calls,
                 to_char(g.started_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'),
                 to_char(g.updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
                 FROM dsa_rebuild.progress g CROSS JOIN dsa.position p WHERE g.id=1 AND p.id=1",
                &[],
            )
            .await?;
        Ok(row.map(|row| Progress {
            phase: row.get(0),
            reason: row.get(1),
            plan: row.get(2),
            detail: row.get(3),
            calls: row.get(4),
            started_at: row.get(5),
            updated_at: row.get(6),
        }))
    }
}
