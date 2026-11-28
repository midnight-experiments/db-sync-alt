//! The destination of `create`, `restore` and `convert`, and the steps both
//! loads share: locking an empty layout, writing the serving state, and the
//! publication that records first-served times and opens the gate.

use dsa_chain::{VERSION, profiles::Profile};
use serde_json::Value;
use tokio_postgres::{GenericClient, Transaction};

use crate::{
    Error, NODE_RELATIONS, Store,
    verify::{MARKERS, ServingState},
};

/// The part of `layout.sql` that creates schema `serve` and its tables.
pub(crate) fn served_tables_ddl() -> &'static str {
    let layout = include_str!("layout.sql");
    layout
        .find("-- served tables:")
        .map_or(layout, |start| &layout[start..])
}

/// A restore builds new tables in `serve` and keeps the empty ones the public
/// views read in this schema until it publishes.
pub(crate) const RETIRED: &str = "dsa_retired";
pub(crate) const STATE_TABLES: &str = "dsa.supply,dsa.consumed_before_horizon,dsa.first_served,dsa.calls,\
    dsa.service_state,dsa.unspent,dsa.first_observation,dsa.upkeep";
const LOAD_KINDS: [&str; 2] = ["restore_failed", "convert_failed"];

/// What a database holds before a load.
pub(crate) enum Destination {
    /// No layout and nothing else.
    Blank,
    /// The closed layout with no row: new, or left by a load that failed.
    Empty,
    /// The committed build of a restore that stopped before publishing.
    Leftover,
}

/// The secondary indexes of `indexes.sql`, by name.
pub(crate) fn secondary_index_names() -> impl Iterator<Item = &'static str> {
    include_str!("indexes.sql")
        .lines()
        .filter_map(|line| line.split_once(" INDEX ").map(|(_, rest)| rest))
        .filter_map(|rest| rest.split_whitespace().next())
}

/// Every index of the layout, primary keys included: the names the node looks
/// up in `pg_indexes`.
fn index_names() -> Vec<&'static str> {
    crate::verify::index_definitions()
        .map(|(name, _)| name)
        .collect()
}

fn served_tables() -> String {
    NODE_RELATIONS
        .iter()
        .map(|table| format!("serve.{table}"))
        .collect::<Vec<_>>()
        .join(",")
}

async fn blank<C: GenericClient>(client: &C) -> Result<bool, Error> {
    Ok(client
        .query_one(
            "SELECT NOT (EXISTS(SELECT 1 FROM pg_namespace WHERE nspname NOT LIKE 'pg\\_%' AND nspname NOT IN ('public','information_schema')) \
             OR EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public') \
             OR EXISTS(SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public') \
             OR EXISTS(SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace WHERE n.nspname='public') \
             OR EXISTS(SELECT 1 FROM pg_extension WHERE extname<>'plpgsql'))",
            &[],
        )
        .await?
        .get(0))
}

struct Layout {
    serving: bool,
    origin: Option<String>,
    rows: bool,
    retired: bool,
    foreign: bool,
}

async fn layout<C: GenericClient>(client: &C) -> Result<Layout, Error> {
    let tables = NODE_RELATIONS.to_vec();
    let row = client
        .query_one(include_str!("destination.sql"), &[&tables, &index_names()])
        .await?;
    Ok(Layout {
        serving: row.get(0),
        origin: row.get(1),
        rows: row.get(2),
        retired: row.get(3),
        foreign: row.get(4),
    })
}

impl Layout {
    fn empty_for(&self, kinds: &[&str]) -> bool {
        !self.serving
            && !self.rows
            && !self.retired
            && !self.foreign
            && self
                .origin
                .as_deref()
                .is_some_and(|kind| kinds.contains(&kind))
    }
}

impl Store {
    /// Whether a restore may load this database: it is blank, holds the
    /// closed layout without a row, or the build of an interrupted restore.
    /// A database that serves or holds anything else is not loadable.
    pub async fn loadable(&self, profile: &Profile) -> Result<bool, Error> {
        match self.destination(profile).await {
            Ok(_) => Ok(true),
            Err(Error::Refusal("destination_not_empty")) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Classifies the destination without changing it. Anything that serves,
    /// holds a row or an object of its own is refused.
    pub(crate) async fn destination(&self, profile: &Profile) -> Result<Destination, Error> {
        let identity: bool = self
            .client
            .query_one("SELECT to_regclass('dsa.identity') IS NOT NULL", &[])
            .await?
            .get(0);
        if !identity {
            return if blank(&self.client).await? {
                Ok(Destination::Blank)
            } else {
                Err(Error::Refusal("destination_not_empty"))
            };
        }
        self.open(profile).await?;
        let layout = layout(&self.client).await?;
        if layout.retired && !layout.serving && layout.origin.as_deref() == Some("restore_failed") {
            return Ok(Destination::Leftover);
        }
        if layout.empty_for(&["empty", LOAD_KINDS[0], LOAD_KINDS[1]]) {
            Ok(Destination::Empty)
        } else {
            Err(Error::Refusal("destination_not_empty"))
        }
    }

    /// Creates the closed layout in a blank database.
    pub(crate) async fn create_layout(&mut self, profile: &Profile) -> Result<(), Error> {
        let profile_hash = profile.hash()?;
        let transaction = self.writer_transaction().await?;
        if !blank(&transaction).await? {
            return Err(Error::Refusal("destination_not_empty"));
        }
        let database: String = transaction
            .query_one("SELECT current_database()", &[])
            .await?
            .get(0);
        transaction
            .batch_execute(&format!(
                "REVOKE ALL ON DATABASE {} FROM PUBLIC",
                crate::identifier(&database)
            ))
            .await?;
        for sql in [
            include_str!("layout.sql"),
            &include_str!("state.sql").replace(
                "__KEEP_RULE_VERSION__",
                &crate::KEEP_RULE_VERSION.to_string(),
            ),
            include_str!("indexes.sql"),
            include_str!("views.sql"),
        ] {
            transaction.batch_execute(sql).await?;
        }
        transaction.execute(
            "INSERT INTO dsa.identity(id,database_format,network,profile_hash,keep_rule_version,tool_version,node_release,origin) \
             VALUES(1,$1,$2,$3,$4,$5,$6,$7)",
            &[&crate::DATABASE_FORMAT, &profile.network, &profile_hash, &crate::KEEP_RULE_VERSION, &VERSION, &profile.node_release, &serde_json::json!({"kind":"empty"})]
        ).await?;
        transaction
            .execute("INSERT INTO dsa.position(id) VALUES(1)", &[])
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Brings the destination to an empty closed layout marked with `kind`
    /// (`restore_failed` or `convert_failed` until the load publishes).
    pub(crate) async fn prepare_load(
        &mut self,
        profile: &Profile,
        kind: &'static str,
    ) -> Result<(), Error> {
        self.lock_writer().await?;
        match self.destination(profile).await? {
            Destination::Blank => self.create_layout(profile).await?,
            Destination::Leftover => self.undo_build().await?,
            Destination::Empty => {}
        }
        let transaction = self.writer_transaction().await?;
        transaction
            .execute(
                "UPDATE dsa.position SET serving=false,reason=$1,updated_at=now() WHERE id=1",
                &[&kind],
            )
            .await?;
        transaction
            .execute(
                "UPDATE dsa.identity SET origin=jsonb_build_object('kind',$1::text) WHERE id=1",
                &[&kind],
            )
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Drops the tables a stopped restore built and gives the public views'
    /// empty tables their schema and index names back.
    async fn undo_build(&mut self) -> Result<(), Error> {
        let transaction = self.writer_transaction().await?;
        transaction
            .batch_execute(&format!(
                "LOCK TABLE {},{STATE_TABLES} IN ACCESS EXCLUSIVE MODE",
                served_tables()
            ))
            .await?;
        let retired_rows = NODE_RELATIONS
            .iter()
            .map(|table| format!("EXISTS(SELECT 1 FROM {RETIRED}.{table})"))
            .collect::<Vec<_>>()
            .join(" OR ");
        if transaction
            .query_one(&format!("SELECT {retired_rows}"), &[])
            .await?
            .get::<_, bool>(0)
        {
            return Err(Error::Refusal("destination_not_empty"));
        }
        transaction
            .batch_execute(&format!(
                "DROP TABLE {};DROP SCHEMA serve;ALTER SCHEMA {RETIRED} RENAME TO serve;TRUNCATE {STATE_TABLES}",
                served_tables()
            ))
            .await?;
        rename_indexes(&transaction, "serve", "retired_", "").await?;
        transaction.batch_execute("UPDATE dsa.position SET target_height=NULL,target_hash=NULL,target_time=NULL,tip_height=NULL,tip_hash=NULL,tip_time=NULL,horizon_height=NULL,horizon_hash=NULL,horizon_time=NULL,updated_at=now() WHERE id=1").await?;
        transaction.commit().await?;
        Ok(())
    }
}

/// Renames every index in `schema` named `from` + X to `to` + X.
pub(crate) async fn rename_indexes(
    transaction: &Transaction<'_>,
    schema: &str,
    from: &str,
    to: &str,
) -> Result<(), Error> {
    for row in transaction
        .query(
            "SELECT indexname FROM pg_indexes WHERE schemaname=$1 AND starts_with(indexname,$2)",
            &[&schema, &from],
        )
        .await?
    {
        let name: String = row.get(0);
        let renamed = format!("{to}{}", &name[from.len()..]);
        transaction
            .batch_execute(&format!(
                "ALTER INDEX {schema}.{} RENAME TO {}",
                crate::identifier(&name),
                crate::identifier(&renamed)
            ))
            .await?;
    }
    Ok(())
}

/// The first statements of a load transaction. Other writers stay out until
/// it ends; the served tables take `served_lock` (restore keeps readers
/// running with SHARE, conversion rewrites them with ACCESS EXCLUSIVE). The
/// empty layout is then checked again under those locks.
pub(crate) async fn lock_empty(
    transaction: &Transaction<'_>,
    served_lock: &str,
    kind: &str,
) -> Result<(), Error> {
    transaction
        .batch_execute(&format!(
            "LOCK TABLE {} IN {served_lock} MODE;LOCK TABLE {STATE_TABLES} IN ACCESS EXCLUSIVE MODE",
            served_tables()
        ))
        .await?;
    transaction
        .query_one(
            "SELECT 1 FROM dsa.position p CROSS JOIN dsa.identity i WHERE p.id=1 AND i.id=1 FOR UPDATE OF p,i",
            &[],
        )
        .await?;
    if !layout(transaction).await?.empty_for(&[kind]) {
        return Err(Error::Refusal("destination_not_empty"));
    }
    Ok(())
}

pub(crate) async fn write_position(
    transaction: &Transaction<'_>,
    state: &ServingState,
) -> Result<(), Error> {
    let s = state;
    transaction
        .execute(
            "UPDATE dsa.position SET horizon_height=$1::integer,horizon_hash=decode($2,'hex'),
             horizon_time=$3::text::timestamptz,target_height=$4::integer,target_hash=decode($5,'hex'),
             target_time=$6::text::timestamptz,tip_height=$7::integer,tip_hash=decode($8,'hex'),
             tip_time=$9::text::timestamptz,retention_seconds=$10,updated_at=now() WHERE id=1",
            &[
                &(s.horizon.height as i32),
                &s.horizon.hash,
                &s.horizon.time,
                &(s.target.height as i32),
                &s.target.hash,
                &s.target.time,
                &(s.tip.height as i32),
                &s.tip.hash,
                &s.tip.time,
                &s.retention_seconds,
            ],
        )
        .await?;
    Ok(())
}

pub(crate) async fn write_supply<'a>(
    transaction: &Transaction<'_>,
    supply: impl IntoIterator<Item = (&'a String, &'a String)>,
) -> Result<(), Error> {
    for (unit, quantity) in supply {
        transaction
            .execute(
                "INSERT INTO dsa.supply(unit,quantity) VALUES($1,$2::text::numeric)",
                &[unit, quantity],
            )
            .await?;
    }
    Ok(())
}

/// Statistics for every loaded table, and the private identity sequences
/// started above the loaded identities.
pub(crate) async fn analyze_and_sequences(transaction: &Transaction<'_>) -> Result<(), Error> {
    let tables = NODE_RELATIONS
        .iter()
        .map(|table| format!("serve.{table}"))
        .chain([format!("dsa.{MARKERS}")])
        .collect::<Vec<_>>()
        .join(",");
    transaction
        .batch_execute(&format!("ANALYZE {tables}"))
        .await?;
    for table in ["multi_asset", "datum", "pool_hash", "epoch_stake"] {
        transaction.query_one(&format!("SELECT setval('dsa.{table}_id_seq',greatest(coalesce(max(id),0)+1,1),false) FROM serve.{table}"),&[]).await?;
    }
    Ok(())
}

/// The last step of every load: one first-served time for each height H…T,
/// the origin, the follower's state rows (the service state and the unspent
/// outputs at each profile address), and the open gate.
pub(crate) async fn publish_state(
    transaction: &Transaction<'_>,
    profile: &Profile,
    state: &ServingState,
    origin: &Value,
) -> Result<(), Error> {
    transaction
        .execute(
            "INSERT INTO dsa.first_served SELECT b.id,n.at FROM serve.block b CROSS JOIN (SELECT clock_timestamp() AS at) n WHERE b.id BETWEEN $1 AND $2",
            &[&i64::from(state.horizon.height), &i64::from(state.target.height)],
        )
        .await?;
    transaction
        .execute(
            &format!(
                "INSERT INTO dsa.unspent(address,outputs) {}",
                crate::follow::UNSPENT_COUNTS
            ),
            &[&crate::follow::profile_addresses(profile)],
        )
        .await?;
    transaction
        .execute(
            "INSERT INTO dsa.service_state(id,state,reason,updated_at)
             VALUES(1,'published',coalesce($1::jsonb->>'kind','published'),clock_timestamp())",
            &[origin],
        )
        .await?;
    transaction
        .execute(
            "UPDATE dsa.identity SET origin=$1,tool_version=$2 WHERE id=1",
            &[origin, &VERSION],
        )
        .await?;
    transaction
        .batch_execute(
            "UPDATE dsa.position SET serving=true,reason='serving',updated_at=clock_timestamp() WHERE id=1",
        )
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn the_served_tables_are_the_end_of_the_layout() {
        let ddl = super::served_tables_ddl();
        assert!(ddl.starts_with("-- served tables:"));
        assert!(ddl.contains("CREATE SCHEMA serve;"));
        assert!(!ddl.contains("CREATE SCHEMA dsa") && !ddl.contains("CREATE DOMAIN"));
        assert_eq!(
            ddl.matches("CREATE TABLE serve.").count(),
            crate::NODE_RELATIONS.len()
        );
    }
}
