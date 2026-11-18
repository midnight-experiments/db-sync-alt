//! Whole-database integrity and streamed natural-key fingerprints.

use std::collections::{BTreeMap, BTreeSet};

use dsa_chain::{
    digest::{FamilyDigest, FamilyHasher, root},
    keep::Bounds,
    profiles::Profile,
};
use futures_util::{StreamExt, pin_mut};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio_postgres::GenericClient;

use crate::{
    Error, NODE_RELATIONS, Store, canonical,
    keep::{Selection, SourceSchema},
};

pub const MARKERS: &str = "consumed_before_horizon";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Position {
    pub height: u32,
    pub hash: String,
    pub time: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServingState {
    pub horizon: Position,
    pub target: Position,
    pub tip: Position,
    pub retention_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Verification {
    pub state: ServingState,
    pub families: BTreeMap<String, FamilyDigest>,
    pub content_digest: String,
    pub protected_state: FamilyDigest,
    pub state_digest: String,
    pub supply: BTreeMap<String, String>,
    pub database_bytes: i64,
    pub relation_bytes: BTreeMap<String, i64>,
}

impl Store {
    pub async fn verify(&mut self, profile: &Profile) -> Result<Verification, Error> {
        self.open(profile).await?;
        let tx = self
            .client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await?;
        let report = verify(&tx, profile, true).await?;
        tx.commit().await?;
        Ok(report)
    }
}

pub(crate) async fn state<C: GenericClient>(client: &C) -> Result<ServingState, Error> {
    let row=client.query_opt("SELECT horizon_height::bigint,encode(horizon_hash,'hex'),to_char(horizon_time AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),target_height::bigint,encode(target_hash,'hex'),to_char(target_time AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),tip_height::bigint,encode(tip_hash,'hex'),to_char(tip_time AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),retention_seconds FROM dsa.position WHERE id=1 AND horizon_height IS NOT NULL AND target_height IS NOT NULL AND tip_height IS NOT NULL",&[]).await?.ok_or(Error::Refusal("missing_serving_position"))?;
    let position = |offset| -> Result<Position, Error> {
        Ok(Position {
            height: u32::try_from(row.get::<_, i64>(offset))
                .map_err(|_| Error::Refusal("invalid_serving_position"))?,
            hash: row.get(offset + 1),
            time: row.get(offset + 2),
        })
    };
    Ok(ServingState {
        horizon: position(0)?,
        target: position(3)?,
        tip: position(6)?,
        retention_seconds: row.get(9),
    })
}

pub(crate) async fn verify<C: GenericClient>(
    client: &C,
    profile: &Profile,
    serving: bool,
) -> Result<Verification, Error> {
    if serving {
        client.query_one("SELECT dsa.gate()", &[]).await?;
    }
    let state = state(client).await?;
    let bounds = Bounds {
        horizon: state.horizon.height,
        target: state.target.height,
        tip: state.tip.height,
    };
    let selection = Selection::new(profile, bounds)?;
    selection
        .validate_source(client, SourceSchema::Serve)
        .await?;
    let h = i64::from(bounds.horizon);
    let t = i64::from(bounds.target);
    let p = i64::from(bounds.tip);
    if state.retention_seconds < 86400 {
        return Err(Error::Refusal("invalid_retention"));
    }
    for position in [&state.horizon, &state.target, &state.tip] {
        let valid:bool=client.query_one("SELECT EXISTS(SELECT 1 FROM serve.block WHERE id=$1 AND encode(hash,'hex')=$2 AND time=($3::text::timestamptz AT TIME ZONE 'UTC'))",&[&i64::from(position.height),&position.hash,&position.time]).await?.get(0);
        if !valid {
            return Err(Error::Refusal("position_header_mismatch"));
        }
    }
    let checks = [
        (
            "SELECT EXISTS(SELECT 1 FROM serve.tx_out o LEFT JOIN serve.tx t ON t.id=o.tx_id WHERE t.id IS NULL) OR EXISTS(SELECT 1 FROM serve.tx_in i LEFT JOIN serve.tx s ON s.id=i.tx_in_id LEFT JOIN serve.tx o ON o.id=i.tx_out_id LEFT JOIN serve.tx_out x ON x.tx_id=i.tx_out_id AND x.index=i.tx_out_index WHERE s.id IS NULL OR o.id IS NULL OR x.id IS NULL) OR EXISTS(SELECT 1 FROM serve.ma_tx_out a LEFT JOIN serve.tx_out o ON o.id=a.tx_out_id LEFT JOIN serve.multi_asset m ON m.id=a.ident WHERE o.id IS NULL OR m.id IS NULL) OR EXISTS(SELECT 1 FROM serve.epoch_stake s LEFT JOIN serve.pool_hash p ON p.id=s.pool_id WHERE p.id IS NULL) OR EXISTS(SELECT 1 FROM serve.tx_metadata m LEFT JOIN serve.tx t ON t.id=m.tx_id WHERE t.id IS NULL)",
            "referential_closure_failed",
        ),
        (
            "SELECT EXISTS(SELECT 1 FROM serve.block WHERE id<>block_no OR id NOT BETWEEN 0 AND 134217727) OR EXISTS(SELECT 1 FROM serve.tx WHERE block_index NOT BETWEEN 0 AND 4095 OR block_id NOT BETWEEN 0 AND 134217727 OR id<>block_id*4096+block_index) OR EXISTS(SELECT 1 FROM serve.tx_out WHERE index NOT BETWEEN 0 AND 4095 OR tx_id NOT BETWEEN 0 AND 549755813887 OR id<>tx_id*4096+index OR consumed_by_tx_id IS NOT NULL) OR EXISTS(SELECT 1 FROM serve.tx_in WHERE id-tx_in_id*4096 NOT BETWEEN 0 AND 4095 OR tx_out_index NOT BETWEEN 0 AND 4095) OR EXISTS(SELECT 1 FROM serve.ma_tx_out WHERE id-tx_out_id*4096 NOT BETWEEN 0 AND 4095) OR EXISTS(SELECT 1 FROM serve.tx_metadata WHERE id<>tx_id OR key<>6500973)",
            "positional_id_invalid",
        ),
        (
            "SELECT NOT EXISTS(SELECT 1 FROM serve.tx_in)",
            "empty_tx_in",
        ),
        (
            "SELECT EXISTS(SELECT 1 FROM serve.epoch_stake GROUP BY epoch_no,pool_id HAVING count(*)<>1)",
            "duplicate_natural_key",
        ),
    ];
    for (sql, error) in checks {
        if client.query_one(sql, &[]).await?.get::<_, bool>(0) {
            return Err(Error::Refusal(error));
        }
    }
    let contiguous:bool=client.query_one("SELECT count(*)=$3::bigint-$1::bigint+1 AND count(*) FILTER(WHERE id>$3)=0 FROM serve.block WHERE id>=$1::bigint AND $2::bigint<=$3",&[&h,&t,&p]).await?.get(0);
    if !contiguous {
        return Err(Error::Refusal("header_gap"));
    }
    let sql = format!(
        "{} SELECT EXISTS(SELECT 1 FROM serve.block b WHERE b.id<$1 AND NOT EXISTS(SELECT 1 FROM keep_block k WHERE k.id=b.id))",
        Selection::ctes_sql(SourceSchema::Serve)
    );
    if client
        .query_one(&sql, &selection.parameters())
        .await?
        .get::<_, bool>(0)
    {
        return Err(Error::Refusal("extra_old_header"));
    }
    let markers:bool=client.query_one("SELECT EXISTS(SELECT 1 FROM dsa.consumed_before_horizon c LEFT JOIN serve.tx_out o ON o.id=c.tx_out_id WHERE o.id IS NULL OR o.id>=$1::bigint*16777216 OR EXISTS(SELECT 1 FROM serve.tx_in i WHERE i.tx_out_id=o.tx_id AND i.tx_out_index=o.index AND i.id>=$1::bigint*16777216))",&[&h]).await?.get(0);
    if markers {
        return Err(Error::Refusal("invalid_consumed_before_horizon"));
    }
    for (table, factor) in [
        ("tx", 4096_i64),
        ("tx_out", 16777216),
        ("tx_in", 16777216),
        ("ma_tx_out", 68719476736),
    ] {
        let sql = format!(
            "SELECT EXISTS(SELECT 1 FROM serve.{table} WHERE id>$1::bigint*$2::bigint+$2-1)"
        );
        if client
            .query_one(&sql, &[&t, &factor])
            .await?
            .get::<_, bool>(0)
        {
            return Err(Error::Refusal("event_above_target"));
        }
    }
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
        let valid:bool=client.query_one("SELECT count(*)=1 AND bool_and(i.indisvalid AND i.indisready) FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE c.relname=$1",&[&name]).await?.get(0);
        if !valid {
            return Err(Error::Refusal("required_index_missing"));
        }
    }
    let mut supply = BTreeMap::new();
    let units = profile
        .assets
        .values()
        .map(|a| a.unit())
        .collect::<BTreeSet<_>>();
    for row in client
        .query(
            "SELECT unit,trunc(quantity)::text FROM dsa.supply ORDER BY unit",
            &[],
        )
        .await?
    {
        let unit: String = row.get(0);
        let quantity: String = row.get(1);
        if !units.contains(&unit) {
            return Err(Error::Refusal("unknown_supply_unit"));
        }
        let actual:String=client.query_one("SELECT trunc(coalesce(sum(a.quantity),0))::text FROM serve.ma_tx_out a JOIN serve.multi_asset m ON m.id=a.ident JOIN serve.tx_out o ON o.id=a.tx_out_id WHERE encode(m.policy,'hex')||encode(m.name,'hex')=$1 AND NOT EXISTS(SELECT 1 FROM serve.tx_in i WHERE i.tx_out_id=o.tx_id AND i.tx_out_index=o.index) AND NOT EXISTS(SELECT 1 FROM dsa.consumed_before_horizon c WHERE c.tx_out_id=o.id)",&[&unit]).await?.get(0);
        if quantity != actual {
            return Err(Error::Refusal("supply_mismatch"));
        }
        supply.insert(unit, quantity);
    }
    if supply.keys().cloned().collect::<BTreeSet<_>>() != units {
        return Err(Error::Refusal("missing_supply"));
    }
    let mut families = BTreeMap::new();
    let mut protected = None;
    let mut relation_bytes = BTreeMap::new();
    for table in NODE_RELATIONS.into_iter().chain([MARKERS]) {
        let mut digest = FamilyHasher::new(table);
        let stream = client.query_raw(&canonical::query(table)?, [&h]).await?;
        pin_mut!(stream);
        while let Some(row) = stream.next().await {
            let row: Value = row?.get(0);
            digest.row(&row)?;
        }
        let digest = digest.finish();
        let schema = if table == MARKERS { "dsa" } else { "serve" };
        let total: i64 = client
            .query_one(&format!("SELECT count(*) FROM {schema}.{table}"), &[])
            .await?
            .get(0);
        if total as u64 != digest.rows {
            return Err(Error::Refusal("digest_join_lost_rows"));
        }
        let size: i64 = client
            .query_one(
                "SELECT pg_total_relation_size($1::text::regclass)",
                &[&format!("{schema}.{table}")],
            )
            .await?
            .get(0);
        relation_bytes.insert(table.to_owned(), size);
        if table == MARKERS {
            protected = Some(digest);
        } else {
            families.insert(table.to_owned(), digest);
        }
    }
    let protected_state = protected.ok_or(Error::Refusal("missing_protected_state"))?;
    let state_digest = root(&BTreeMap::from([(
        MARKERS.to_owned(),
        protected_state.clone(),
    )]));
    let database_bytes = client
        .query_one("SELECT pg_database_size(current_database())", &[])
        .await?
        .get(0);
    Ok(Verification {
        state,
        content_digest: root(&families),
        families,
        protected_state,
        state_digest,
        supply,
        database_bytes,
        relation_bytes,
    })
}

pub(crate) fn logical_equal(a: &Verification, b: &Verification) -> bool {
    a.state == b.state
        && a.families == b.families
        && a.content_digest == b.content_digest
        && a.protected_state == b.protected_state
        && a.state_digest == b.state_digest
        && a.supply == b.supply
}

pub(crate) fn origin(value: &Value) -> Value {
    json!({"kind":"restored","source":value})
}
