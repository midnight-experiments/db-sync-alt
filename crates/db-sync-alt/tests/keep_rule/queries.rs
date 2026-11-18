use std::{collections::BTreeMap, sync::Mutex};

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
// A lexical scanner skips quoted comments and nested block comments. It
// accepts Rust cooked/raw literals and fails explicitly on unsupported escapes.
fn strings(source: &str) -> Vec<String> {
    let bytes = source.as_bytes();
    let mut i = 0;
    let mut out = Vec::new();
    while i < bytes.len() {
        if source[i..].starts_with("//") {
            i += source[i..].find('\n').unwrap_or(bytes.len() - i);
            continue;
        }
        if source[i..].starts_with("/*") {
            let mut depth = 1;
            i += 2;
            while depth > 0 {
                assert!(i < bytes.len());
                if source[i..].starts_with("/*") {
                    depth += 1;
                    i += 2;
                } else if source[i..].starts_with("*/") {
                    depth -= 1;
                    i += 2;
                } else {
                    i += source[i..].chars().next().unwrap().len_utf8();
                }
            }
            continue;
        }
        if bytes[i] == b'r' {
            let start = i;
            i += 1;
            while i < bytes.len() && bytes[i] == b'#' {
                i += 1;
            }
            if i < bytes.len() && bytes[i] == b'"' {
                let end = format!("\"{}", "#".repeat(i - start - 1));
                i += 1;
                let length = source[i..].find(&end).expect("unterminated raw literal");
                out.push(source[i..i + length].to_owned());
                i += length + end.len();
                continue;
            }
            i = start;
        }
        if bytes[i] == b'"' {
            i += 1;
            let mut value = String::new();
            while bytes[i] != b'"' {
                let c = source[i..].chars().next().unwrap();
                i += c.len_utf8();
                if c == '\\' {
                    let escaped = source[i..].chars().next().unwrap();
                    i += escaped.len_utf8();
                    value.push(match escaped {
                        'n' => '\n',
                        'r' => '\r',
                        't' => '\t',
                        '"' => '"',
                        '\\' => '\\',
                        _ => panic!("unsupported Rust string escape"),
                    });
                } else {
                    value.push(c);
                }
            }
            out.push(value);
            i += 1;
        } else {
            i += source[i..].chars().next().unwrap().len_utf8();
        }
    }
    out
}
fn literal(source: &str) -> String {
    strings(source)
        .into_iter()
        .next()
        .expect("missing Rust literal")
}

pub fn node_sql(name: &str) -> String {
    let source = match name {
        "get_governance_body_utxo" => GOVERNANCE,
        "get_token_utxo_for_epoch"
        | "get_latest_block_for_epoch"
        | "get_epoch_nonce"
        | "get_stake_distribution"
        | "get_latest_stable_epoch"
        | "get_epoch_for_block_hash" => CANDIDATES,
        "get_block_by_hash" | "get_blocks_by_numbers" => PARTNER,
        _ => CNIGHT,
    };
    literal(function(source, name))
}
#[derive(Debug, Clone, Copy)]
pub enum Checkpoint {
    Block(i32),
    Tx(i32, i32),
    TxReserveTransfer(i32, i32),
}
fn bridge_sql(checkpoint: Checkpoint, limit: Option<u32>) -> String {
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
    let (arm, values) = match checkpoint {
        Checkpoint::Block(block) => ("ResolvedBridgeDataCheckpoint::Block", vec![block]),
        Checkpoint::Tx(block, index) => ("ResolvedBridgeDataCheckpoint::Tx {", vec![block, index]),
        Checkpoint::TxReserveTransfer(block, index) => (
            "ResolvedBridgeDataCheckpoint::TxReserveTransfer",
            vec![block, index],
        ),
    };
    let mut predicate = literal(body.split_once(arm).unwrap().1);
    for value in values {
        predicate = predicate.replacen("{}", &value.to_string(), 1);
    }
    substitutions.insert("checkpoint_limit", predicate);
    substitutions.insert(
        "max_rows",
        limit.map_or_else(|| "null".to_owned(), |n| n.to_string()),
    );
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
    bridge: Mutex<BTreeMap<String, Statement>>,
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
            "get_latest_stable_epoch",
            "get_epoch_for_block_hash",
            "get_block_by_hash",
            "get_blocks_by_numbers",
        ] {
            let sql = node_sql(name);
            statements.insert(
                name.to_owned(),
                prepare(client, &sql, &parameter_types(name)).await?,
            );
        }
        Ok(Self {
            statements,
            bridge: Mutex::new(BTreeMap::new()),
        })
    }
    pub async fn bridge_rows(
        &self,
        client: &Client,
        checkpoint: Checkpoint,
        limit: Option<u32>,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Value>> {
        let sql = bridge_sql(checkpoint, limit);
        let cached = self.bridge.lock().unwrap().get(&sql).cloned();
        let statement = if let Some(statement) = cached {
            statement
        } else {
            let statement = prepare(
                client,
                &sql,
                &[
                    Type::TEXT,
                    Type::TEXT,
                    Type::BYTEA,
                    Type::BYTEA,
                    Type::INT8,
                    Type::INT4,
                ],
            )
            .await?;
            self.bridge.lock().unwrap().insert(sql, statement.clone());
            statement
        };
        Ok(client
            .query(&statement, params)
            .await?
            .iter()
            .map(|row| row.get(0))
            .collect())
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
        "get_block_by_hash" | "get_epoch_for_block_hash" => vec![Type::BYTEA],
        "get_blocks_by_numbers" => vec![Type::INT4, Type::INT4],
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
    paged_events(
        queries,
        client,
        fixture,
        (start, start_index, end, i32::MAX),
        (1000, 0),
        bounds,
    )
    .await
}
pub async fn paged_events(
    queries: &Queries,
    client: &Client,
    fixture: &Fixture,
    range: (i32, i32, i32, i32),
    page: (i32, i32),
    bounds: (IdBounds, IdBounds),
) -> Result<BTreeMap<String, Vec<Value>>> {
    let (start, start_index, end, end_index) = range;
    let (limit, offset) = page;
    let (low, high) = bounds;
    let mapping = &fixture.profile.addresses["mapping"];
    let auth = fixture.asset_id("auth");
    let night = fixture.asset_id("cnight");
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
                .bridge_rows(
                    client,
                    Checkpoint::Block(cursor as i32 - 1),
                    None,
                    &[ics, reserve, &policy, &name, &metadata_key, &reference],
                )
                .await?,
        );
    }
    let header_hash = hex::decode(
        fixture.rows()["block"]
            .iter()
            .find(|row| row["id"].as_i64() == Some(i64::from(reference)))
            .unwrap()["hash"]
            .as_str()
            .unwrap()
            .trim_start_matches("\\x"),
    )?;
    result.insert(
        "block_by_hash".to_owned(),
        queries
            .rows(client, "get_block_by_hash", &[&header_hash])
            .await?,
    );
    result.insert(
        "epoch_by_hash".to_owned(),
        queries
            .rows(client, "get_epoch_for_block_hash", &[&header_hash])
            .await?,
    );
    let horizon = fixture.bounds.horizon as i32;
    result.insert(
        "header_range".to_owned(),
        queries
            .rows(
                client,
                "get_blocks_by_numbers",
                &[&horizon, &(fixture.bounds.tip as i32)],
            )
            .await?,
    );
    let k = fixture.profile.cardano.security_parameter as i32;
    result.insert(
        "latest_stable_epoch".to_owned(),
        queries
            .rows(client, "get_latest_stable_epoch", &[&k])
            .await?,
    );
    Ok(result)
}

pub async fn reader_probes(client: &Client) -> Result<()> {
    let probes = strings(function(PARTNER, "from_connection"));
    assert_eq!(probes, strings(function(CANDIDATES, "from_connection")));
    assert_eq!(client.query_one(&probes[0], &[]).await?.get::<_, i64>(0), 1);
    assert!(client.query_one(&probes[1], &[]).await?.get::<_, bool>(0));
    let sql = literal(function(PARTNER, "index_exists"));
    for index in [
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
        assert_eq!(client.query(&sql, &[&index]).await?.len(), 1, "{index}");
    }
    Ok(())
}

#[test]
fn sql_literal_scanner_ignores_quoted_comments() {
    let source = "// \"misleading\"\n/* \"also misleading\" /* nested */ */ let sql = \"SELECT 1\\nWHERE true\";";
    assert_eq!(literal(source), "SELECT 1\nWHERE true");
    assert_eq!(
        literal("/* \"ignored\" */ r##\"SELECT \"name\" FROM tx\"##"),
        "SELECT \"name\" FROM tx"
    );
}
