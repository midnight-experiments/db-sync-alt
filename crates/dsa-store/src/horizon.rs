//! The window's lower edge. Every height H…T has one first-served time; a
//! height becomes deletable once the retention has passed since then. The
//! horizon moves in steps by applying the pruned form of the keep rule to the
//! store itself, so pruning has no second encoding of the rule.

use std::collections::BTreeMap;

use dsa_chain::{keep::Bounds, profiles::Profile};
use serde::Serialize;
use tokio_postgres::Transaction;

use crate::{
    Error, NODE_RELATIONS, Store,
    keep::{Form, Selection},
};

/// Rows deleted by one horizon advance, per table.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Advance {
    pub horizon: u32,
    pub deleted: BTreeMap<String, u64>,
    pub markers_added: u64,
}

impl Store {
    /// The lowest height whose first-served time is younger than the
    /// retention: the horizon the retention allows now. Never above T, never
    /// below H.
    pub async fn horizon_candidate(&self) -> Result<u32, Error> {
        let row = self
            .client
            .query_one(
                "SELECT least(greatest(coalesce((SELECT min(f.height)::bigint FROM dsa.first_served f
                  WHERE f.first_served_at>now()-make_interval(secs=>p.retention_seconds)),p.target_height),
                  p.horizon_height),p.target_height)::bigint FROM dsa.position p WHERE p.id=1",
                &[],
            )
            .await?;
        let candidate: Option<i64> = row.get(0);
        candidate
            .and_then(|height| u32::try_from(height).ok())
            .ok_or(Error::Refusal("missing_serving_position"))
    }

    /// Advances the horizon in its own writer transaction.
    pub async fn advance_horizon(
        &mut self,
        profile: &Profile,
        horizon: u32,
    ) -> Result<Advance, Error> {
        let transaction = self.writer_transaction().await?;
        let advance = advance_horizon(&transaction, profile, horizon).await?;
        transaction.commit().await?;
        Ok(advance)
    }
}

/// Moves H to `horizon` inside the caller's transaction: records the outputs
/// the store keeps although their spend now falls below H, deletes from every
/// served table the rows the pruned form no longer selects, drops markers and
/// first-served times that no longer apply, and stores the new horizon.
pub async fn advance_horizon(
    transaction: &Transaction<'_>,
    profile: &Profile,
    horizon: u32,
) -> Result<Advance, Error> {
    let position = transaction
        .query_opt(
            "SELECT horizon_height::bigint,target_height::bigint,tip_height::bigint FROM dsa.position
             WHERE id=1 AND horizon_height IS NOT NULL AND target_height IS NOT NULL AND tip_height IS NOT NULL FOR UPDATE",
            &[],
        )
        .await?
        .ok_or(Error::Refusal("missing_serving_position"))?;
    let height = |index| {
        u32::try_from(position.get::<_, i64>(index))
            .map_err(|_| Error::Refusal("invalid_serving_position"))
    };
    let (current, target, tip) = (height(0)?, height(1)?, height(2)?);
    if horizon <= current || horizon > target {
        return Err(Error::Refusal("invalid_horizon_advance"));
    }
    let new = i64::from(horizon);
    let header = transaction
        .query_opt(
            "SELECT epoch_no IS NOT NULL FROM serve.block WHERE id=$1",
            &[&new],
        )
        .await?;
    if !header.is_some_and(|row| row.get::<_, bool>(0)) {
        return Err(Error::Refusal("missing_horizon_epoch"));
    }
    // The epoch-boundary headers the rule keeps are found among the headers
    // below H. Applying the rule to an already pruned store finds the same
    // ones only when every header that becomes old has an epoch and a slot,
    // as every header of a Shelley-era chain does.
    let incomplete: bool = transaction
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM serve.block WHERE id>=$1 AND id<$2 AND (epoch_no IS NULL OR slot_no IS NULL))",
            &[&i64::from(current), &new],
        )
        .await?
        .get(0);
    if incomplete {
        return Err(Error::Refusal("horizon_header_incomplete"));
    }
    let markers_added = transaction
        .execute(
            "INSERT INTO dsa.consumed_before_horizon SELECT DISTINCT o.id FROM serve.tx_out o
             JOIN serve.tx_in i ON i.tx_out_id=o.tx_id AND i.tx_out_index=o.index
             WHERE i.id BETWEEN 0 AND $1::bigint*16777216-1 AND o.id BETWEEN 0 AND $1::bigint*16777216-1
             ON CONFLICT DO NOTHING",
            &[&new],
        )
        .await?;
    let selection = Selection::new(
        profile,
        Bounds {
            horizon,
            target,
            tip,
        },
    )?;
    // One statement: every keep set is evaluated once on the same snapshot.
    let deletes = NODE_RELATIONS
        .iter()
        .map(|table| {
            let key = if *table == "epoch_param" { "epoch_no" } else { "id" };
            format!("deleted_{table} AS (DELETE FROM serve.{table} x WHERE NOT EXISTS(SELECT 1 FROM keep_{table} k WHERE k.id=x.{key}) RETURNING 1)")
        })
        .collect::<Vec<_>>()
        .join(",\n");
    let counts = NODE_RELATIONS
        .iter()
        .map(|table| format!("(SELECT count(*) FROM deleted_{table})"))
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "{},\n{deletes}\nSELECT ARRAY[{counts}]",
        Selection::ctes(Form::Pruned).trim_end()
    );
    let counts: Vec<i64> = transaction
        .query_one(&sql, &selection.parameters())
        .await?
        .get(0);
    transaction
        .execute(
            "DELETE FROM dsa.consumed_before_horizon c WHERE NOT EXISTS(SELECT 1 FROM serve.tx_out o WHERE o.id=c.tx_out_id)",
            &[],
        )
        .await?;
    transaction
        .execute(
            "DELETE FROM dsa.first_served WHERE height<$1",
            &[&(horizon as i32)],
        )
        .await?;
    transaction
        .execute(
            "UPDATE dsa.position p SET horizon_height=b.id,horizon_hash=b.hash,horizon_time=b.time AT TIME ZONE 'UTC',updated_at=now()
             FROM serve.block b WHERE p.id=1 AND b.id=$1",
            &[&new],
        )
        .await?;
    Ok(Advance {
        horizon,
        deleted: NODE_RELATIONS
            .iter()
            .zip(counts)
            .map(|(table, count)| (table.to_string(), count as u64))
            .collect(),
        markers_added,
    })
}
