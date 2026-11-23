//! The service's upkeep in the store: the supply check, the sizes of the
//! indexes and their rebuild, and the records of `dsa.upkeep`, which lie
//! outside the snapshot file and both digests. None of these takes the
//! writer lock: they write only `dsa.upkeep`, which the follower never
//! writes, so they can run on their own connections beside the follower.

use std::{collections::BTreeMap, time::Instant};

use dsa_chain::profiles::Profile;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio_postgres::IsolationLevel;

use crate::{Error, Store, identifier, verify};

/// The `dsa.upkeep` row of the last supply check.
pub const SUPPLY: &str = "supply";
/// The `dsa.upkeep` row of the last daily snapshot file.
pub const SNAPSHOT: &str = "snapshot";
/// The prefix of an index's `dsa.upkeep` row; the index name follows.
pub const INDEX_PREFIX: &str = "index:";

const RECORD: &str = "INSERT INTO dsa.upkeep(name,at,detail) VALUES($1,clock_timestamp(),$2)
 ON CONFLICT(name) DO UPDATE SET at=EXCLUDED.at,detail=EXCLUDED.detail";

/// The stored unspent outputs: no stored spend names them and they are not
/// recorded as spent before the horizon.
const UNSPENT_OUTPUTS: &str = "SELECT count(*) FROM serve.tx_out o
 WHERE NOT EXISTS(SELECT 1 FROM serve.tx_in i WHERE i.tx_out_id=o.tx_id AND i.tx_out_index=o.index)
 AND NOT EXISTS(SELECT 1 FROM dsa.consumed_before_horizon c WHERE c.tx_out_id=o.id)";

/// The result of one supply check: the comparison `verify` runs, alone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupplyCheck {
    pub passed: bool,
    /// The recorded supply of each fixed unit (policy followed by name).
    pub supply: BTreeMap<String, String>,
    /// Every stored unspent output, relevant or not.
    pub unspent_outputs: i64,
    /// The difference found: its code, and for `supply_mismatch` the unit
    /// with the recorded and the unspent amount.
    pub failure: Option<Value>,
}

/// The last daily snapshot file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotFile {
    /// The file name inside the snapshot directory.
    pub file: String,
    pub content_digest: String,
    pub target: u32,
    pub bytes: u64,
}

/// An index's size after its last rebuild (or when the service first saw
/// it), in bytes, and when (Unix seconds).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rebuilt {
    pub at: i64,
    pub bytes: i64,
}

/// One index of the layout with its current size.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IndexSize {
    pub name: String,
    pub bytes: i64,
    pub rebuilt: Option<Rebuilt>,
}

/// One rebuild by `REINDEX INDEX CONCURRENTLY`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Rebuild {
    pub name: String,
    pub before: i64,
    pub after: i64,
    pub seconds: f64,
}

/// The names of the layout's indexes, primary keys included.
fn index_names() -> Vec<&'static str> {
    verify::index_definitions().map(|(name, _)| name).collect()
}

/// A leftover of an interrupted `REINDEX INDEX CONCURRENTLY`: a layout
/// index name followed by `_ccnew` or `_ccold` and an optional number.
fn rebuild_leftover(name: &str) -> bool {
    index_names().into_iter().any(|index| {
        name.strip_prefix(index)
            .and_then(|rest| {
                rest.strip_prefix("_ccnew")
                    .or_else(|| rest.strip_prefix("_ccold"))
            })
            .is_some_and(|digits| digits.bytes().all(|b| b.is_ascii_digit()))
    })
}

impl Store {
    /// The supply check alone: the unspent amount of every fixed unit
    /// equals its recorded supply, read in one snapshot together with the
    /// number of unspent outputs. A named whole-database read. The result,
    /// passed or not, is recorded in `dsa.upkeep`; only a database error is
    /// returned as an error.
    pub async fn check_supply(&mut self, profile: &Profile) -> Result<SupplyCheck, Error> {
        self.open(profile).await?;
        let started = Instant::now();
        let tx = self
            .client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await?;
        let checked = verify::check_supply(&tx, profile).await;
        let unspent_outputs: i64 = tx.query_one(UNSPENT_OUTPUTS, &[]).await?.get(0);
        let supply_rows = tx
            .query(
                "SELECT unit,trunc(quantity)::text FROM dsa.supply ORDER BY unit",
                &[],
            )
            .await?;
        tx.commit().await?;
        let supply = supply_rows
            .iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();
        let failure = match checked {
            Ok(_) => None,
            Err(Error::Mismatch {
                error,
                item,
                expected,
                actual,
            }) => Some(json!({"error":error,"unit":item,"recorded":expected,"unspent":actual})),
            Err(Error::Refusal(code)) => Some(json!({"error":code})),
            Err(error) => return Err(error),
        };
        let check = SupplyCheck {
            passed: failure.is_none(),
            supply,
            unspent_outputs,
            failure,
        };
        let mut detail = serde_json::to_value(&check)?;
        detail["seconds"] = json!(started.elapsed().as_secs_f64());
        self.client.execute(RECORD, &[&SUPPLY, &detail]).await?;
        Ok(check)
    }

    /// Every index of the layout with its size and its size after the last
    /// rebuild: one catalogue statement over the layout's index names.
    pub async fn index_sizes(&self) -> Result<Vec<IndexSize>, Error> {
        let names = index_names();
        let rows = self
            .client
            .query(
                "SELECT e.name,pg_relation_size(c.oid),floor(extract(epoch FROM u.at))::bigint,(u.detail->>'bytes')::bigint
                 FROM unnest($1::text[]) e(name)
                 JOIN pg_class c ON c.relname=e.name AND c.relnamespace='serve'::regnamespace AND c.relkind='i'
                 LEFT JOIN dsa.upkeep u ON u.name=$2||e.name ORDER BY e.name",
                &[&names, &INDEX_PREFIX],
            )
            .await?;
        Ok(rows
            .iter()
            .map(|row| IndexSize {
                name: row.get(0),
                bytes: row.get(1),
                rebuilt: match (row.get::<_, Option<i64>>(2), row.get::<_, Option<i64>>(3)) {
                    (Some(at), Some(bytes)) => Some(Rebuilt { at, bytes }),
                    _ => None,
                },
            })
            .collect())
    }

    /// Records the current size of each index that has no record yet, as
    /// its size after the last rebuild.
    pub async fn record_index_baselines(&self, indexes: &[IndexSize]) -> Result<(), Error> {
        let (names, sizes): (Vec<String>, Vec<i64>) = indexes
            .iter()
            .filter(|index| index.rebuilt.is_none())
            .map(|index| (format!("{INDEX_PREFIX}{}", index.name), index.bytes))
            .unzip();
        if names.is_empty() {
            return Ok(());
        }
        self.client
            .execute(
                "INSERT INTO dsa.upkeep(name,at,detail)
                 SELECT n,clock_timestamp(),jsonb_build_object('bytes',b,'baseline',true)
                 FROM unnest($1::text[],$2::bigint[]) u(n,b) ON CONFLICT(name) DO NOTHING",
                &[&names, &sizes],
            )
            .await?;
        Ok(())
    }

    /// Rebuilds one index of the layout with `REINDEX INDEX CONCURRENTLY`,
    /// outside a transaction, and records its new size. The name stays
    /// valid and unique during the rebuild: the new index is built as
    /// `<name>_ccnew` and takes the name in one catalogue transaction.
    /// Readers and the follower's writes go on meanwhile. Run it on a
    /// connection of its own: it sets the session's statement timeout to
    /// none. A named whole-index operation.
    pub async fn rebuild_index(&self, name: &str) -> Result<Rebuild, Error> {
        if !index_names().contains(&name) {
            return Err(Error::Refusal("unknown_index"));
        }
        let size_sql = "SELECT pg_relation_size(format('serve.%I',$1::text)::regclass)";
        let before: i64 = self.client.query_one(size_sql, &[&name]).await?.get(0);
        let started = Instant::now();
        // A statement of its own: a multi-statement string is one implicit
        // transaction, in which a concurrent rebuild is refused.
        self.client
            .batch_execute("SET statement_timeout TO 0")
            .await?;
        self.client
            .batch_execute(&format!(
                "REINDEX INDEX CONCURRENTLY serve.{}",
                identifier(name)
            ))
            .await?;
        let rebuild = Rebuild {
            name: name.to_owned(),
            before,
            after: self.client.query_one(size_sql, &[&name]).await?.get(0),
            seconds: started.elapsed().as_secs_f64(),
        };
        self.client
            .execute(
                RECORD,
                &[
                    &format!("{INDEX_PREFIX}{name}"),
                    &json!({"bytes":rebuild.after,"before":rebuild.before,"seconds":rebuild.seconds}),
                ],
            )
            .await?;
        Ok(rebuild)
    }

    /// Drops what an interrupted rebuild left in `serve` (`<name>_ccnew` or
    /// `<name>_ccold` of a layout index), one `DROP INDEX CONCURRENTLY` each,
    /// and returns their names. The layout's own index names are never
    /// touched.
    pub async fn drop_interrupted_rebuilds(&self) -> Result<Vec<String>, Error> {
        let leftovers = self
            .client
            .query(
                "SELECT c.relname FROM pg_class c WHERE c.relnamespace='serve'::regnamespace AND c.relkind='i' ORDER BY 1",
                &[],
            )
            .await?
            .iter()
            .map(|row| row.get::<_, String>(0))
            .filter(|name| rebuild_leftover(name))
            .collect::<Vec<_>>();
        for name in &leftovers {
            self.client
                .batch_execute(&format!(
                    "DROP INDEX CONCURRENTLY IF EXISTS serve.{}",
                    identifier(name)
                ))
                .await?;
        }
        Ok(leftovers)
    }

    /// Records the last daily snapshot file.
    pub async fn record_snapshot_file(&self, file: &SnapshotFile) -> Result<(), Error> {
        self.client
            .execute(RECORD, &[&SNAPSHOT, &serde_json::to_value(file)?])
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_rebuild_leftovers_of_layout_indexes_are_named() {
        for name in [
            "idx_tx_out_address_ccnew",
            "idx_tx_out_address_ccnew1",
            "tx_out_pkey_ccold",
            "serve_tx_hash_ccold12",
        ] {
            assert!(rebuild_leftover(name), "{name}");
        }
        for name in [
            "idx_tx_out_address",
            "tx_out_pkey",
            "idx_tx_out_address_ccnewx",
            "other_index_ccnew",
            "retired_tx_pkey_ccnew",
        ] {
            assert!(!rebuild_leftover(name), "{name}");
        }
    }
}
