//! Whole-database integrity and streamed natural-key fingerprints.

use std::collections::{BTreeMap, BTreeSet};

use dsa_chain::{
    digest::{FamilyDigest, FamilyHasher, root},
    keep::Bounds,
    profiles::Profile,
};
use futures_util::{StreamExt, pin_mut};
use serde::{Deserialize, Serialize};
use tokio_postgres::{GenericClient, types::ToSql};

use crate::{
    Error, NODE_RELATIONS, Store, canonical,
    keep::{Form, Selection, SourceSchema},
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

/// The checks a verification runs besides digests, supply and structure.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Checks {
    /// The database must be serving.
    gate: bool,
    /// The store holds exactly the keep set of its pruned form, a first-served
    /// time for every height H…T and the parameters of every window epoch.
    /// This reads the whole store again, so loads leave it to `verify`.
    exact: bool,
}
impl Checks {
    pub(crate) const LOAD: Self = Self {
        gate: false,
        exact: false,
    };
    pub(crate) const SNAPSHOT: Self = Self {
        gate: true,
        exact: false,
    };
    const COMMAND: Self = Self {
        gate: true,
        exact: true,
    };
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
        let report = verify(&tx, profile, Checks::COMMAND).await?;
        tx.commit().await?;
        Ok(report)
    }
}

pub(crate) async fn state<C: GenericClient>(client: &C) -> Result<ServingState, Error> {
    let row = client
        .query_opt(
            "SELECT horizon_height::bigint,encode(horizon_hash,'hex'),to_char(horizon_time AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),
             target_height::bigint,encode(target_hash,'hex'),to_char(target_time AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),
             tip_height::bigint,encode(tip_hash,'hex'),to_char(tip_time AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),retention_seconds
             FROM dsa.position WHERE id=1 AND horizon_height IS NOT NULL AND target_height IS NOT NULL AND tip_height IS NOT NULL",
            &[],
        )
        .await?
        .ok_or(Error::Refusal("missing_serving_position"))?;
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

/// `(name, definition)` as PostgreSQL 17 deparses each index the node and the
/// store rely on, without the schema.
pub(crate) fn index_definitions() -> impl Iterator<Item = (&'static str, &'static str)> {
    include_str!("indexes.expected")
        .lines()
        .filter_map(|line| line.split_once(" | "))
}

async fn exists<C: GenericClient>(
    client: &C,
    sql: &str,
    parameters: &[&(dyn ToSql + Sync)],
) -> Result<bool, Error> {
    Ok(client.query_one(sql, parameters).await?.get(0))
}

/// Structural checks that need no parameter, each with its refusal.
const STRUCTURE: [(&str, &str); 4] = [
    (
        "SELECT EXISTS(SELECT 1 FROM serve.tx_out o LEFT JOIN serve.tx t ON t.id=o.tx_id WHERE t.id IS NULL)
         OR EXISTS(SELECT 1 FROM serve.tx_in i LEFT JOIN serve.tx s ON s.id=i.tx_in_id LEFT JOIN serve.tx o ON o.id=i.tx_out_id
          LEFT JOIN serve.tx_out x ON x.tx_id=i.tx_out_id AND x.index=i.tx_out_index WHERE s.id IS NULL OR o.id IS NULL OR x.id IS NULL)
         OR EXISTS(SELECT 1 FROM serve.ma_tx_out a LEFT JOIN serve.tx_out o ON o.id=a.tx_out_id LEFT JOIN serve.multi_asset m ON m.id=a.ident WHERE o.id IS NULL OR m.id IS NULL)
         OR EXISTS(SELECT 1 FROM serve.epoch_stake s LEFT JOIN serve.pool_hash p ON p.id=s.pool_id WHERE p.id IS NULL)
         OR EXISTS(SELECT 1 FROM serve.tx_metadata m LEFT JOIN serve.tx t ON t.id=m.tx_id WHERE t.id IS NULL)",
        "referential_closure_failed",
    ),
    (
        "SELECT EXISTS(SELECT 1 FROM serve.block WHERE id<>block_no OR id NOT BETWEEN 0 AND 134217727)
         OR EXISTS(SELECT 1 FROM serve.tx WHERE block_index NOT BETWEEN 0 AND 4095 OR block_id NOT BETWEEN 0 AND 134217727 OR id<>block_id*4096+block_index)
         OR EXISTS(SELECT 1 FROM serve.tx_out WHERE index NOT BETWEEN 0 AND 4095 OR tx_id NOT BETWEEN 0 AND 549755813887 OR id<>tx_id*4096+index OR consumed_by_tx_id IS NOT NULL)
         OR EXISTS(SELECT 1 FROM serve.tx_in WHERE id-tx_in_id*4096 NOT BETWEEN 0 AND 4095 OR tx_out_index NOT BETWEEN 0 AND 4095)
         OR EXISTS(SELECT 1 FROM serve.ma_tx_out WHERE id-tx_out_id*4096 NOT BETWEEN 0 AND 4095)
         OR EXISTS(SELECT 1 FROM serve.tx_metadata WHERE id<>tx_id OR key<>6500973)",
        "positional_id_invalid",
    ),
    ("SELECT NOT EXISTS(SELECT 1 FROM serve.tx_in)", "empty_tx_in"),
    (
        "SELECT EXISTS(SELECT 1 FROM serve.epoch_stake GROUP BY epoch_no,pool_id HAVING count(*)<>1)
         OR EXISTS(SELECT 1 FROM serve.tx_in GROUP BY tx_in_id,tx_out_id,tx_out_index HAVING count(*)>1)
         OR EXISTS(SELECT 1 FROM serve.ma_tx_out GROUP BY tx_out_id,ident HAVING count(*)>1)",
        "duplicate_natural_key",
    ),
];

pub(crate) async fn verify<C: GenericClient>(
    client: &C,
    profile: &Profile,
    checks: Checks,
) -> Result<Verification, Error> {
    if checks.gate {
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
        let sql = "SELECT EXISTS(SELECT 1 FROM serve.block WHERE id=$1 AND encode(hash,'hex')=$2
                   AND time=($3::text::timestamptz AT TIME ZONE 'UTC'))";
        if !exists(
            client,
            sql,
            &[&i64::from(position.height), &position.hash, &position.time],
        )
        .await?
        {
            return Err(Error::Refusal("position_header_mismatch"));
        }
    }
    for (sql, error) in STRUCTURE {
        if exists(client, sql, &[]).await? {
            return Err(Error::Refusal(error));
        }
    }
    let source = Selection::ctes(Form::Source(SourceSchema::Serve));
    let invalid = format!(
        "{source} SELECT EXISTS(SELECT 1 FROM serve.ma_tx_out a WHERE NOT ({}))",
        Selection::asset_predicate(SourceSchema::Serve, "a")
    );
    if exists(client, &invalid, &selection.parameters()).await? {
        return Err(Error::Refusal("ineligible_asset_row"));
    }
    let addresses = profile
        .addresses
        .iter()
        .map(|(_, address)| address.to_owned())
        .collect::<Vec<_>>();
    let pc = &profile.assets.permissioned_candidates;
    let pc_policy = hex::decode(&pc.policy).map_err(|_| Error::Refusal("invalid_keep_profile"))?;
    let pc_name = hex::decode(&pc.name).map_err(|_| Error::Refusal("invalid_keep_profile"))?;
    let extra_datum = "SELECT EXISTS(SELECT 1 FROM serve.datum d WHERE NOT EXISTS(SELECT 1 FROM serve.tx_out o
        WHERE o.data_hash=d.hash AND (o.address=ANY($1::text[]) OR EXISTS(SELECT 1 FROM serve.ma_tx_out a
        JOIN serve.multi_asset m ON m.id=a.ident WHERE a.tx_out_id=o.id AND m.policy=$2::bytea AND m.name=$3::bytea))))";
    if exists(client, extra_datum, &[&addresses, &pc_policy, &pc_name]).await? {
        return Err(Error::Refusal("extra_datum_row"));
    }
    let fixed = dsa_chain::assets::fixed_units(profile)
        .into_iter()
        .collect::<Vec<_>>();
    let extra_descriptor = "SELECT EXISTS(SELECT 1 FROM serve.multi_asset m
        WHERE NOT(encode(m.policy,'hex')||encode(m.name,'hex')=ANY($1::text[]))
        AND NOT EXISTS(SELECT 1 FROM serve.ma_tx_out a WHERE a.ident=m.id))";
    if exists(client, extra_descriptor, &[&fixed]).await? {
        return Err(Error::Refusal("extra_asset_descriptor"));
    }
    let contiguous = "SELECT count(*)=$3::bigint-$1::bigint+1 AND count(*) FILTER(WHERE id>$3)=0
        FROM serve.block WHERE id>=$1::bigint AND $2::bigint<=$3";
    if !exists(client, contiguous, &[&h, &t, &p]).await? {
        return Err(Error::Refusal("header_gap"));
    }
    let old_header = format!(
        "{source} SELECT EXISTS(SELECT 1 FROM serve.block b WHERE b.id<$1 AND NOT EXISTS(SELECT 1 FROM keep_block k WHERE k.id=b.id))"
    );
    if exists(client, &old_header, &selection.parameters()).await? {
        return Err(Error::Refusal("extra_old_header"));
    }
    let markers = "SELECT EXISTS(SELECT 1 FROM dsa.consumed_before_horizon c LEFT JOIN serve.tx_out o ON o.id=c.tx_out_id
        WHERE o.id IS NULL OR o.id>=$1::bigint*16777216 OR EXISTS(SELECT 1 FROM serve.tx_in i
        WHERE i.tx_out_id=o.tx_id AND i.tx_out_index=o.index AND i.id>=$1::bigint*16777216))";
    if exists(client, markers, &[&h]).await? {
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
        if exists(client, &sql, &[&t, &factor]).await? {
            return Err(Error::Refusal("event_above_target"));
        }
    }
    check_indexes(client).await?;
    let supply = check_supply(client, profile).await?;
    if checks.gate {
        check_unspent(client, profile).await?;
    }
    if checks.exact {
        check_exact(client, &selection, h, t).await?;
    }
    let mut families = BTreeMap::new();
    let mut protected = None;
    let mut relation_bytes = BTreeMap::new();
    for table in NODE_RELATIONS.into_iter().chain([MARKERS]) {
        let mut digest = FamilyHasher::new(table);
        let stream = client.query_raw(&canonical::query(table)?, [&h]).await?;
        pin_mut!(stream);
        while let Some(row) = stream.next().await {
            let row: String = row?.try_get(0)?;
            digest.row(row.as_bytes());
        }
        let digest = digest.finish();
        let schema = if table == MARKERS { "dsa" } else { "serve" };
        let row = client
            .query_one(
                &format!(
                    "SELECT count(*),pg_total_relation_size('{schema}.{table}') FROM {schema}.{table}"
                ),
                &[],
            )
            .await?;
        if row.get::<_, i64>(0) as u64 != digest.rows {
            return Err(Error::Refusal("digest_join_lost_rows"));
        }
        relation_bytes.insert(table.to_owned(), row.get(1));
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

/// Every index the node and the store need exists exactly once under its name
/// among the database's permanent relations (the node creates an index whose
/// name it does not find exactly once), in `serve`, with its definition, valid
/// and ready.
async fn check_indexes<C: GenericClient>(client: &C) -> Result<(), Error> {
    let (names, definitions): (Vec<&str>, Vec<&str>) = index_definitions().unzip();
    let sql = "SELECT e.name,EXISTS(SELECT 1 FROM pg_indexes WHERE schemaname='serve' AND indexname=e.name)
        FROM unnest($1::text[],$2::text[]) e(name,definition)
        WHERE (SELECT count(*) FROM pg_indexes WHERE indexname=e.name AND schemaname !~ '^pg_temp_')<>1
        OR NOT EXISTS(SELECT 1 FROM pg_indexes x JOIN pg_class c ON c.relname=x.indexname AND c.relnamespace='serve'::regnamespace
         JOIN pg_index i ON i.indexrelid=c.oid WHERE x.schemaname='serve' AND x.indexname=e.name
         AND replace(x.indexdef,' ON serve.',' ON ')=e.definition AND i.indisvalid AND i.indisready AND i.indislive)
        LIMIT 1";
    if let Some(row) = client.query_opt(sql, &[&names, &definitions]).await? {
        return Err(Error::InFamily {
            family: row.get(0),
            error: Box::new(Error::Refusal(if row.get(1) {
                "index_shape_mismatch"
            } else {
                "required_index_missing"
            })),
        });
    }
    Ok(())
}

/// The unspent amount of each fixed unit equals its recorded supply.
async fn check_supply<C: GenericClient>(
    client: &C,
    profile: &Profile,
) -> Result<BTreeMap<String, String>, Error> {
    let units = dsa_chain::assets::fixed_units(profile);
    let sql = "SELECT s.unit,trunc(s.quantity)::text,(SELECT trunc(coalesce(sum(a.quantity),0))::text
        FROM serve.ma_tx_out a JOIN serve.multi_asset m ON m.id=a.ident JOIN serve.tx_out o ON o.id=a.tx_out_id
        WHERE encode(m.policy,'hex')||encode(m.name,'hex')=s.unit
        AND NOT EXISTS(SELECT 1 FROM serve.tx_in i WHERE i.tx_out_id=o.tx_id AND i.tx_out_index=o.index)
        AND NOT EXISTS(SELECT 1 FROM dsa.consumed_before_horizon c WHERE c.tx_out_id=o.id))
        FROM dsa.supply s ORDER BY s.unit";
    let mut supply = BTreeMap::new();
    for row in client.query(sql, &[]).await? {
        let (unit, recorded, actual): (String, String, String) =
            (row.get(0), row.get(1), row.get(2));
        if !units.contains(&unit) {
            return Err(Error::Refusal("unknown_supply_unit"));
        }
        if recorded != actual {
            return Err(Error::Mismatch {
                error: "supply_mismatch",
                item: unit,
                expected: recorded,
                actual,
            });
        }
        supply.insert(unit, recorded);
    }
    if supply.keys().cloned().collect::<BTreeSet<_>>() != units {
        return Err(Error::Refusal("missing_supply"));
    }
    Ok(supply)
}

/// The recorded unspent outputs at each profile address equal a recount. A
/// load writes them at publication, so only a serving store has them.
async fn check_unspent<C: GenericClient>(client: &C, profile: &Profile) -> Result<(), Error> {
    let sql = format!(
        "WITH counted(address,outputs) AS ({}) SELECT coalesce(c.address,u.address),u.outputs::text,c.outputs::text
         FROM counted c FULL JOIN dsa.unspent u ON u.address=c.address
         WHERE u.outputs IS DISTINCT FROM c.outputs ORDER BY 1 LIMIT 1",
        crate::follow::UNSPENT_COUNTS
    );
    let addresses = crate::follow::profile_addresses(profile);
    if let Some(row) = client.query_opt(&sql, &[&addresses]).await? {
        let text = |index: usize| {
            row.get::<_, Option<String>>(index)
                .unwrap_or_else(|| "none".to_owned())
        };
        return Err(Error::Mismatch {
            error: "unspent_count_mismatch",
            item: row.get(0),
            expected: text(1),
            actual: text(2),
        });
    }
    Ok(())
}

/// Nothing lies outside the pruned form of the keep rule, every height H…T
/// has a first-served time, and every epoch of the window has parameters.
async fn check_exact<C: GenericClient>(
    client: &C,
    selection: &Selection,
    h: i64,
    t: i64,
) -> Result<(), Error> {
    let strays = NODE_RELATIONS
        .iter()
        .map(|table| {
            let key = if *table == "epoch_param" { "epoch_no" } else { "id" };
            format!("CASE WHEN EXISTS(SELECT 1 FROM serve.{table} x WHERE NOT EXISTS(SELECT 1 FROM keep_{table} k WHERE k.id=x.{key})) THEN '{table}' END")
        })
        .collect::<Vec<_>>()
        .join(",\n");
    let sql = format!(
        "{} SELECT array_remove(ARRAY[{strays}]::text[],NULL)",
        Selection::ctes(Form::Pruned)
    );
    let strays: Vec<String> = client
        .query_one(&sql, &selection.parameters())
        .await?
        .get(0);
    if let Some(family) = strays.into_iter().next() {
        return Err(Error::InFamily {
            family,
            error: Box::new(Error::Refusal("row_outside_keep_rule")),
        });
    }
    let first_served = "SELECT count(*) FILTER(WHERE height BETWEEN $1::bigint AND $2::bigint)=$2::bigint-$1::bigint+1
        AND count(*) FILTER(WHERE height NOT BETWEEN $1::bigint AND $2::bigint)=0 FROM dsa.first_served";
    if !exists(client, first_served, &[&h, &t]).await? {
        return Err(Error::Refusal("first_served_incomplete"));
    }
    let epochs = "SELECT EXISTS(SELECT 1 FROM serve.block b WHERE b.id BETWEEN $1 AND $2 AND b.epoch_no IS NOT NULL
        AND NOT EXISTS(SELECT 1 FROM serve.epoch_param e WHERE e.epoch_no=b.epoch_no))";
    if exists(client, epochs, &[&h, &t]).await? {
        return Err(Error::Refusal("epoch_param_missing"));
    }
    Ok(())
}

/// The loaded content equals the expected content; otherwise the first
/// differing item is named with both values.
pub(crate) fn same(
    expected: &Verification,
    actual: &Verification,
    error: &'static str,
) -> Result<(), Error> {
    fn text(value: &impl Serialize) -> String {
        serde_json::to_string(value).unwrap_or_default()
    }
    let mut differences = Vec::new();
    if expected.state != actual.state {
        differences.push((
            "state".to_owned(),
            text(&expected.state),
            text(&actual.state),
        ));
    }
    for family in expected.families.keys().chain(actual.families.keys()) {
        let (a, b) = (expected.families.get(family), actual.families.get(family));
        if a != b {
            differences.push((family.clone(), text(&a), text(&b)));
        }
    }
    if expected.protected_state != actual.protected_state {
        differences.push((
            MARKERS.to_owned(),
            text(&expected.protected_state),
            text(&actual.protected_state),
        ));
    }
    for unit in expected.supply.keys().chain(actual.supply.keys()) {
        let (a, b) = (expected.supply.get(unit), actual.supply.get(unit));
        if a != b {
            differences.push((unit.clone(), text(&a), text(&b)));
        }
    }
    for (item, a, b) in [
        (
            "content_digest",
            &expected.content_digest,
            &actual.content_digest,
        ),
        ("state_digest", &expected.state_digest, &actual.state_digest),
    ] {
        if a != b {
            differences.push((item.to_owned(), a.clone(), b.clone()));
        }
    }
    match differences.into_iter().next() {
        None => Ok(()),
        Some((item, expected, actual)) => Err(Error::Mismatch {
            error,
            item,
            expected,
            actual,
        }),
    }
}
