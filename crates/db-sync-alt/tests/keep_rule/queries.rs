use std::collections::BTreeMap;

use serde_json::Value;
use tokio_postgres::{
    Client, Statement,
    types::{ToSql, Type},
};

use super::{Result, fixtures::Fixture};
use dsa_chain::keep::RetainedRows;

const CNIGHT: &str = include_str!(
    "../../../../reference/primitives/mainchain-follower/src/db/queries/cnight_observation.rs"
);
const GOVERNANCE: &str = include_str!(
    "../../../../reference/primitives/mainchain-follower/src/db/queries/federated_authority_observation.rs"
);
const CANDIDATES: &str = include_str!(
    "../../../../reference/primitives/mainchain-follower/src/data_source/candidates_data_source/db_model.rs"
);
const PARTNER: &str = include_str!(
    "../../../../reference/partner-chains/toolkit/data-sources/db-sync/src/db_model.rs"
);

fn function<'a>(source: &'a str, name: &str) -> &'a str {
    source
        .split_once(&format!("fn {name}("))
        .unwrap()
        .1
        .split("\npub")
        .next()
        .unwrap()
}
fn literal(source: &str) -> String {
    if let Some((_, rest)) = source.split_once("r#\"") {
        rest.split_once("\"#").unwrap().0.to_owned()
    } else {
        source
            .split_once('"')
            .unwrap()
            .1
            .split_once('"')
            .unwrap()
            .0
            .to_owned()
    }
}
pub fn node_sql(name: &str) -> String {
    let source = match name {
        "get_governance_body_utxo" => GOVERNANCE,
        "get_token_utxo_for_epoch"
        | "get_latest_block_for_epoch"
        | "get_epoch_nonce"
        | "get_stake_distribution" => CANDIDATES,
        _ => CNIGHT,
    };
    literal(function(source, name))
}
fn bridge_sql(cursor: i32) -> String {
    let body = function(PARTNER, "get_bridge_txs");
    let mut substitutions = BTreeMap::new();
    for name in ["bridge_inputs_subquery", "reserve_inputs_subquery"] {
        let block = body
            .split_once(&format!("let {name} = match tx_in_configuration"))
            .unwrap()
            .1;
        substitutions.insert(
            name,
            literal(block.split_once("TxInConfiguration::Enabled =>").unwrap().1),
        );
    }
    for name in ["bridge_outputs_subquery", "reserve_outputs_subquery"] {
        substitutions.insert(
            name,
            literal(body.split_once(&format!("let {name} =")).unwrap().1),
        );
    }
    substitutions.insert(
        "checkpoint_limit",
        format!("block.block_no > {}", cursor.saturating_sub(1)),
    );
    substitutions.insert("max_rows", "null".to_owned());
    let mut query = literal(
        body.split_once("let mut query_builder = QueryBuilder::new")
            .unwrap()
            .1,
    );
    for (name, value) in substitutions {
        query = query.replace(&format!("{{{name}}}"), &value);
    }
    assert!(!query.contains(['{', '}']));
    query
}

pub struct Queries {
    statements: BTreeMap<String, Statement>,
}
impl Queries {
    pub async fn prepare(client: &Client) -> Result<Self> {
        let mut statements = BTreeMap::new();
        for name in [
            "get_low_bounds",
            "get_high_bounds",
            "get_registrations",
            "get_deregistrations",
            "get_asset_creates",
            "get_asset_spends",
            "get_governance_body_utxo",
            "get_token_utxo_for_epoch",
            "get_latest_block_for_epoch",
            "get_epoch_nonce",
            "get_stake_distribution",
            "get_max_tx_size",
        ] {
            let sql = node_sql(name);
            statements.insert(
                name.to_owned(),
                prepare(client, &sql, &parameter_types(name)).await?,
            );
        }
        for cursor in 0..=10 {
            statements.insert(
                format!("bridge_{cursor}"),
                prepare(
                    client,
                    &bridge_sql(cursor),
                    &[
                        Type::TEXT,
                        Type::TEXT,
                        Type::BYTEA,
                        Type::BYTEA,
                        Type::INT8,
                        Type::INT4,
                    ],
                )
                .await?,
            );
        }
        Ok(Self { statements })
    }
    pub async fn rows(
        &self,
        client: &Client,
        name: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Value>> {
        Ok(client
            .query(&self.statements[name], params)
            .await?
            .iter()
            .map(|row| row.get(0))
            .collect())
    }
}
fn parameter_types(name: &str) -> Vec<Type> {
    match name {
        "get_registrations" => [
            vec![Type::TEXT, Type::INT8],
            vec![Type::INT4; 6],
            vec![Type::INT8; 6],
        ]
        .concat(),
        "get_deregistrations" => {
            [vec![Type::TEXT], vec![Type::INT4; 6], vec![Type::INT8; 4]].concat()
        }
        "get_asset_creates" => {
            [vec![Type::INT8], vec![Type::INT4; 6], vec![Type::INT8; 6]].concat()
        }
        "get_asset_spends" => [vec![Type::INT8], vec![Type::INT4; 6], vec![Type::INT8; 4]].concat(),
        "get_governance_body_utxo" => vec![Type::TEXT, Type::BYTEA, Type::INT4],
        "get_token_utxo_for_epoch" => vec![Type::INT8, Type::INT4],
        _ => vec![Type::INT4],
    }
}
async fn prepare(client: &Client, sql: &str, types: &[Type]) -> Result<Statement> {
    Ok(client
        .prepare_typed(
            &format!(
                "SELECT to_jsonb(observation) FROM ({}) observation",
                sql.trim().trim_end_matches(';')
            ),
            types,
        )
        .await?)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdBounds {
    pub tx: i64,
    pub output: i64,
    pub asset: i64,
    pub input: i64,
}
impl IdBounds {
    fn from_rows(rows: &[Value]) -> Self {
        let row = &rows[0];
        Self {
            tx: row["tx_id!"].as_i64().unwrap(),
            output: row["tx_out_id!"].as_i64().unwrap(),
            asset: row["ma_tx_out_id!"].as_i64().unwrap(),
            input: row["tx_in_id!"].as_i64().unwrap(),
        }
    }
}

pub async fn bounds(
    query: &Queries,
    client: &Client,
    fixture: &Fixture,
    available: &RetainedRows,
    block: i32,
    high: bool,
) -> Result<IdBounds> {
    let name = if high {
        "get_high_bounds"
    } else {
        "get_low_bounds"
    };
    let result = IdBounds::from_rows(&query.rows(client, name, &[&block]).await?);
    let c = &fixture.chain;
    let sentinel = if high { i64::MAX } else { 0 };
    let transactions = available["tx"]
        .iter()
        .filter(|id| {
            if high {
                c.transactions[id].block > i64::from(block)
            } else {
                c.transactions[id].block < i64::from(block)
            }
        })
        .collect::<Vec<_>>();
    if transactions.is_empty() {
        assert_eq!(result.tx, sentinel);
    } else {
        assert!(transactions.contains(&&result.tx));
        let parents = transactions.iter().map(|id| c.transactions[id].block);
        assert_eq!(
            c.transactions[&result.tx].block,
            if high { parents.min() } else { parents.max() }.unwrap()
        );
    }
    let outputs = available["tx_out"]
        .iter()
        .filter(|id| {
            if high {
                c.outputs[id].transaction >= result.tx
            } else {
                c.outputs[id].transaction <= result.tx
            }
        })
        .collect::<Vec<_>>();
    if outputs.is_empty() {
        assert_eq!(result.output, sentinel);
    } else {
        assert!(outputs.contains(&&result.output));
        let parents = outputs.iter().map(|id| c.outputs[id].transaction);
        assert_eq!(
            c.outputs[&result.output].transaction,
            if high { parents.min() } else { parents.max() }.unwrap()
        );
    }
    let assets = available["ma_tx_out"]
        .iter()
        .filter(|id| {
            if high {
                c.output_assets[id].output >= result.output
            } else {
                c.output_assets[id].output <= result.output
            }
        })
        .collect::<Vec<_>>();
    if assets.is_empty() {
        assert_eq!(result.asset, sentinel);
    } else {
        assert!(assets.contains(&&result.asset));
        let parents = assets.iter().map(|id| c.output_assets[id].output);
        assert_eq!(
            c.output_assets[&result.asset].output,
            if high { parents.min() } else { parents.max() }.unwrap()
        );
    }
    let inputs = available["tx_in"]
        .iter()
        .filter(|id| {
            if high {
                c.inputs[id].transaction >= result.tx
            } else {
                c.inputs[id].transaction <= result.tx
            }
        })
        .collect::<Vec<_>>();
    if inputs.is_empty() {
        assert_eq!(result.input, sentinel);
    } else {
        assert!(inputs.contains(&&result.input));
        let parents = inputs.iter().map(|id| c.inputs[id].transaction);
        assert_eq!(
            c.inputs[&result.input].transaction,
            if high { parents.min() } else { parents.max() }.unwrap()
        );
    }
    Ok(result)
}

pub async fn events(
    queries: &Queries,
    client: &Client,
    fixture: &Fixture,
    start: i32,
    start_index: i32,
    end: i32,
    bounds: (IdBounds, IdBounds),
) -> Result<BTreeMap<String, Vec<Value>>> {
    let (low, high) = bounds;
    let mapping = &fixture.profile.addresses["mapping"];
    let auth = fixture.asset_id("auth");
    let night = fixture.asset_id("cnight");
    let end_index = 8_i32;
    let limit = 1000_i32;
    let offset = 0_i32;
    let mut result = BTreeMap::new();
    result.insert(
        "registrations".to_owned(),
        queries
            .rows(
                client,
                "get_registrations",
                &[
                    mapping,
                    &auth,
                    &start,
                    &start_index,
                    &end,
                    &end_index,
                    &limit,
                    &offset,
                    &low.tx,
                    &high.tx,
                    &low.output,
                    &high.output,
                    &low.asset,
                    &high.asset,
                ],
            )
            .await?,
    );
    result.insert(
        "deregistrations".to_owned(),
        queries
            .rows(
                client,
                "get_deregistrations",
                &[
                    mapping,
                    &start,
                    &start_index,
                    &end,
                    &end_index,
                    &limit,
                    &offset,
                    &low.tx,
                    &high.tx,
                    &low.input,
                    &high.input,
                ],
            )
            .await?,
    );
    result.insert(
        "asset_creates".to_owned(),
        queries
            .rows(
                client,
                "get_asset_creates",
                &[
                    &night,
                    &start,
                    &start_index,
                    &end,
                    &end_index,
                    &limit,
                    &offset,
                    &low.tx,
                    &high.tx,
                    &low.output,
                    &high.output,
                    &low.asset,
                    &high.asset,
                ],
            )
            .await?,
    );
    result.insert(
        "asset_spends".to_owned(),
        queries
            .rows(
                client,
                "get_asset_spends",
                &[
                    &night,
                    &start,
                    &start_index,
                    &end,
                    &end_index,
                    &limit,
                    &offset,
                    &low.tx,
                    &high.tx,
                    &low.input,
                    &high.input,
                ],
            )
            .await?,
    );
    Ok(result)
}

pub async fn states(
    queries: &Queries,
    client: &Client,
    fixture: &Fixture,
    reference: i32,
) -> Result<BTreeMap<String, Vec<Value>>> {
    let mut result = BTreeMap::new();
    for body in ["council", "technical_committee"] {
        let address = &fixture.profile.addresses[body];
        let policy = hex::decode(&fixture.profile.assets[body].policy)?;
        result.insert(
            body.to_owned(),
            queries
                .rows(
                    client,
                    "get_governance_body_utxo",
                    &[address, &policy, &reference],
                )
                .await?,
        );
    }
    let ident = fixture.asset_id("permissioned_candidates");
    let first_epoch = fixture.chain.blocks[&i64::from(fixture.bounds.horizon)]
        .epoch
        .unwrap()
        - 2;
    let last_epoch = fixture.chain.blocks[&i64::from(reference)].epoch.unwrap();
    for epoch in first_epoch.max(0)..=last_epoch {
        for name in [
            "get_token_utxo_for_epoch",
            "get_latest_block_for_epoch",
            "get_epoch_nonce",
            "get_stake_distribution",
        ] {
            let mut rows = if name == "get_token_utxo_for_epoch" {
                queries.rows(client, name, &[&ident, &epoch]).await?
            } else {
                queries.rows(client, name, &[&epoch]).await?
            };
            // Stake SQL groups by pool hash without an ORDER BY. Compare the
            // same keyed rows while retaining exact field values.
            if name == "get_stake_distribution" {
                rows.sort_by_key(Value::to_string);
            }
            result.insert(format!("{name}_{epoch}"), rows);
        }
    }
    result.insert(
        "max_tx_size".to_owned(),
        queries
            .rows(client, "get_max_tx_size", &[&reference])
            .await?,
    );
    let ics = &fixture.profile.addresses["ics"];
    let reserve = &fixture.profile.addresses["reserve"];
    let policy = hex::decode(&fixture.profile.assets["bridge"].policy)?;
    let name = hex::decode(&fixture.profile.assets["bridge"].name)?;
    let metadata_key = 6500973_i64;
    for cursor in fixture.bounds.horizon..=reference as u32 {
        result.insert(
            format!("bridge_{cursor}"),
            queries
                .rows(
                    client,
                    &format!("bridge_{cursor}"),
                    &[ics, reserve, &policy, &name, &metadata_key, &reference],
                )
                .await?,
        );
    }
    Ok(result)
}
