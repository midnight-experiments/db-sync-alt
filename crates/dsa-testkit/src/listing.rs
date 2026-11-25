//! The provider's listing endpoints over the served blocks, rendered as the
//! backend's queries over db-sync's tables render them: the unspent outputs
//! of an asset or an address, the transaction lists of an asset or an
//! address, a transaction with its resolved inputs and its position, an
//! asset's quantity, mints and burns, a policy's assets and an epoch's blocks.
//! Everything is derived from the facts the builder recorded for each
//! transaction (`tx::Facts`), as of the newest served block; transactions
//! without facts (fillers) touch nothing listed.
//!
//! What db-sync stores, and so what the backend lists:
//! - a phase-2-valid transaction has its body inputs and outputs as ordinary
//!   rows, its reference and collateral inputs and its collateral return in
//!   tables of their own, and its mint;
//! - a phase-2-invalid transaction has its collateral inputs as ordinary
//!   inputs and its collateral return as its one ordinary output, at the index
//!   after the body's outputs, and nothing else: no body input or output, no
//!   reference input, no mint;
//! - an asset's transaction list names the transactions that create an
//!   ordinary output holding the asset, and nothing else: a transaction that
//!   only spends or burns it is not in the list;
//! - an address's transaction list names the transactions with an ordinary
//!   input or output at the address;
//! - an asset's history lists the mints and burns of valid transactions.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use serde_json::{Value, json};

use crate::{
    chain::Block,
    tx::{Datum, Facts, Input, Output, hash},
};

/// The address an input of unknown origin is rendered with: an ordinary
/// enterprise address of no profile.
const UNKNOWN_ORIGIN_ADDRESS: [u8; 29] = {
    let mut bytes = [0x77; 29];
    bytes[0] = 0x61;
    bytes
};

/// A transaction of a served block.
#[derive(Clone, Debug)]
pub struct Placed {
    pub height: u32,
    pub index: u16,
    pub block: String,
    pub time: u64,
    pub facts: Arc<Facts>,
}

/// An output that exists in the served chain.
#[derive(Clone, Debug)]
struct Created {
    height: u32,
    index: u16,
    block: String,
    time: u64,
    output: Output,
}

#[derive(Clone, Debug, Default)]
struct AssetState {
    quantity: i128,
    first_mint: Option<String>,
    mints: u64,
}

/// The served chain's state, rebuilt whenever the served blocks change.
#[derive(Debug, Default)]
pub struct Index {
    pub transactions: BTreeMap<String, Placed>,
    unspent: BTreeMap<(String, u16), Created>,
    spent_by: BTreeMap<(String, u16), String>,
    /// Per unit, the transactions that create an output holding it.
    unit_transactions: BTreeMap<String, BTreeSet<(u32, u16, String)>>,
    /// Per address, the transactions with an input or output there.
    address_transactions: BTreeMap<String, BTreeSet<(u32, u16, String)>>,
    assets: BTreeMap<String, AssetState>,
    /// Per unit, its mints and burns in chain order: transaction and amount.
    history: BTreeMap<String, Vec<(String, i128)>>,
    epoch_blocks: BTreeMap<u32, Vec<String>>,
    /// The served blocks the index was built from: count and last hash.
    pub signature: (usize, Option<String>),
}

/// The text of an address, as the provider renders it.
pub fn address_text(bytes: &[u8]) -> String {
    match pallas_addresses::Address::from_bytes(bytes) {
        Ok(pallas_addresses::Address::Byron(address)) => address.to_base58(),
        Ok(address) => address.to_bech32().unwrap_or_else(|_| hex::encode(bytes)),
        Err(_) => hex::encode(bytes),
    }
}

fn units(output: &Output) -> BTreeSet<String> {
    output
        .tokens
        .iter()
        .filter(|token| token.quantity != 0)
        .map(|token| format!("{}{}", token.policy, token.name))
        .collect()
}

impl Index {
    pub fn build(blocks: &[Block], facts: &BTreeMap<String, Arc<Facts>>) -> Self {
        let mut index = Self {
            signature: (blocks.len(), blocks.last().map(|block| block.hash.clone())),
            ..Self::default()
        };
        for block in blocks {
            index
                .epoch_blocks
                .entry(block.epoch)
                .or_default()
                .push(block.hash.clone());
            for (position, built) in block.transactions.iter().enumerate() {
                let Some(facts) = facts.get(&built.hash) else {
                    continue;
                };
                let position = position as u16;
                let key = (block.height, position, built.hash.clone());
                for input in facts.spent() {
                    let point = (input.hash.clone(), input.index);
                    if let Some(created) = index.unspent.remove(&point) {
                        index.spent_by.insert(point.clone(), built.hash.clone());
                        index
                            .address_transactions
                            .entry(address_text(&created.output.address))
                            .or_default()
                            .insert(key.clone());
                    }
                }
                for (output_index, output) in facts.created() {
                    index.created(output, &key);
                    index.unspent.insert(
                        (built.hash.clone(), output_index),
                        Created {
                            height: block.height,
                            index: position,
                            block: block.hash.clone(),
                            time: block.time,
                            output: output.clone(),
                        },
                    );
                }
                if facts.valid {
                    let mut net: BTreeMap<String, i128> = BTreeMap::new();
                    for token in &facts.mint {
                        *net.entry(format!("{}{}", token.policy, token.name))
                            .or_default() += token.quantity;
                    }
                    for (unit, quantity) in net.into_iter().filter(|(_, q)| *q != 0) {
                        let state = index.assets.entry(unit.clone()).or_default();
                        state.quantity += quantity;
                        state.mints += 1;
                        if quantity > 0 && state.first_mint.is_none() {
                            state.first_mint = Some(built.hash.clone());
                        }
                        index
                            .history
                            .entry(unit)
                            .or_default()
                            .push((built.hash.clone(), quantity));
                    }
                }
                index.transactions.insert(
                    built.hash.clone(),
                    Placed {
                        height: block.height,
                        index: position,
                        block: block.hash.clone(),
                        time: block.time,
                        facts: facts.clone(),
                    },
                );
            }
        }
        index
    }

    /// A transaction that creates `output`: in the lists of its units and of
    /// its address.
    fn created(&mut self, output: &Output, key: &(u32, u16, String)) {
        for unit in units(output) {
            self.unit_transactions
                .entry(unit)
                .or_default()
                .insert(key.clone());
        }
        self.address_transactions
            .entry(address_text(&output.address))
            .or_default()
            .insert(key.clone());
    }

    /// The output an input names, if the served chain created it.
    fn origin(&self, input: &Input) -> Option<&Output> {
        let placed = self.transactions.get(&input.hash)?;
        let facts = &placed.facts;
        let index = usize::from(input.index);
        facts
            .outputs
            .get(index)
            .or(if index == facts.outputs.len() {
                facts.collateral_return.as_ref()
            } else {
                None
            })
    }
}

/// The query of a list request.
struct Query {
    page: usize,
    count: usize,
    descending: bool,
    from: Option<u32>,
    to: Option<u32>,
}

fn query(text: &str) -> Query {
    let mut query = Query {
        page: 1,
        count: 100,
        descending: false,
        from: None,
        to: None,
    };
    for pair in text.split('&') {
        match pair.split_once('=') {
            Some(("page", value)) => query.page = value.parse().unwrap_or(1).max(1),
            Some(("count", value)) => query.count = value.parse().unwrap_or(100).clamp(1, 100),
            Some(("order", value)) => query.descending = value == "desc",
            Some(("from", value)) => query.from = value.parse().ok(),
            Some(("to", value)) => query.to = value.parse().ok(),
            _ => {}
        }
    }
    query
}

impl Query {
    fn inside(&self, height: u32) -> bool {
        self.from.is_none_or(|from| height >= from) && self.to.is_none_or(|to| height <= to)
    }

    fn page<T>(&self, mut rows: Vec<T>) -> Vec<T> {
        if self.descending {
            rows.reverse();
        }
        rows.into_iter()
            .skip((self.page - 1) * self.count)
            .take(self.count)
            .collect()
    }
}

fn amount(output: &Output, tokens: bool) -> Value {
    let mut rows = vec![json!({"unit": "lovelace", "quantity": output.coin.to_string()})];
    if tokens {
        let mut sorted: BTreeMap<String, i128> = BTreeMap::new();
        for token in &output.tokens {
            *sorted
                .entry(format!("{}{}", token.policy, token.name))
                .or_default() += token.quantity;
        }
        rows.extend(
            sorted
                .into_iter()
                .filter(|(_, quantity)| *quantity != 0)
                .map(|(unit, quantity)| json!({"unit": unit, "quantity": quantity.to_string()})),
        );
    }
    Value::Array(rows)
}

fn datum_fields(output: &Output) -> (Value, Value) {
    match &output.datum {
        Datum::None => (Value::Null, Value::Null),
        Datum::Hash(datum_hash) => (json!(datum_hash), Value::Null),
        Datum::Inline(bytes) => (json!(hash(bytes)), json!(hex::encode(bytes))),
    }
}

fn not_found() -> (u16, Value) {
    (
        404,
        json!({"status_code":404,"error":"Not Found","message":"The requested component has not been found."}),
    )
}

/// The answer to a listing path below `/api/v0` (without the query), or
/// `None` for a path this module does not serve.
pub fn answer(index: &Index, parts: &[&str], query_text: &str) -> Option<(u16, Value)> {
    let q = query(query_text);
    let answer = match parts {
        ["assets", "policy", policy] => {
            let rows: Vec<Value> = index
                .assets
                .iter()
                .filter(|(unit, _)| unit.starts_with(policy) && unit.len() >= 56)
                .map(|(unit, state)| json!({"asset": unit, "quantity": state.quantity.to_string()}))
                .collect();
            if rows.is_empty() {
                not_found()
            } else {
                (200, Value::Array(q.page(rows)))
            }
        }
        ["assets", unit] => match index.assets.get(*unit) {
            Some(state) => (
                200,
                json!({"asset": unit, "policy_id": &unit[..56.min(unit.len())],
                    "asset_name": if unit.len() > 56 { json!(&unit[56..]) } else { Value::Null },
                    "fingerprint": "asset1synthetic", "quantity": state.quantity.to_string(),
                    "initial_mint_tx_hash": state.first_mint, "mint_or_burn_count": state.mints,
                    "onchain_metadata": null, "metadata": null, "onchain_metadata_standard": null,
                    "onchain_metadata_extra": null}),
            ),
            None => not_found(),
        },
        ["assets", unit, "history"] => {
            if !index.assets.contains_key(*unit) {
                return Some(not_found());
            }
            let rows = index
                .history
                .get(*unit)
                .map(|events| {
                    events
                        .iter()
                        .map(|(transaction, amount)| {
                            json!({"tx_hash": transaction, "amount": amount.to_string(),
                                "action": if *amount < 0 { "burned" } else { "minted" }})
                        })
                        .collect()
                })
                .unwrap_or_default();
            (200, Value::Array(q.page(rows)))
        }
        ["assets", unit, "utxos"] => {
            if !index.unit_transactions.contains_key(*unit) {
                return Some(not_found());
            }
            let mut rows: Vec<(&(String, u16), &Created)> = index
                .unspent
                .iter()
                .filter(|(_, created)| {
                    q.inside(created.height) && units(&created.output).contains(*unit)
                })
                .collect();
            rows.sort_by_key(|(point, created)| (created.height, created.index, point.1));
            let rows = rows
                .into_iter()
                .map(|(point, created)| {
                    let (data_hash, inline) = datum_fields(&created.output);
                    json!({"address": address_text(&created.output.address), "tx_hash": point.0,
                        "output_index": point.1, "amount": amount(&created.output, true),
                        "block": created.block, "block_height": created.height,
                        "block_time": created.time, "data_hash": data_hash, "inline_datum": inline,
                        "inline_datum_json": null, "reference_script_hash": null})
                })
                .collect();
            (200, Value::Array(q.page(rows)))
        }
        ["addresses", address, "utxos"] => {
            if !index.address_transactions.contains_key(*address) {
                return Some(not_found());
            }
            let mut rows: Vec<(&(String, u16), &Created)> = index
                .unspent
                .iter()
                .filter(|(_, created)| address_text(&created.output.address) == *address)
                .collect();
            rows.sort_by_key(|(point, created)| (created.height, created.index, point.1));
            let rows = rows
                .into_iter()
                .map(|(point, created)| {
                    let (data_hash, inline) = datum_fields(&created.output);
                    json!({"address": address, "tx_hash": point.0, "tx_index": point.1,
                        "output_index": point.1, "amount": amount(&created.output, true),
                        "block": created.block, "data_hash": data_hash, "inline_datum": inline,
                        "reference_script_hash": null})
                })
                .collect();
            (200, Value::Array(q.page(rows)))
        }
        ["assets", key, "transactions"] | ["addresses", key, "transactions"] => {
            let lists = if parts[0] == "assets" {
                &index.unit_transactions
            } else {
                &index.address_transactions
            };
            let Some(list) = lists.get(*key) else {
                return Some(not_found());
            };
            let rows = list
                .iter()
                .filter(|(height, _, _)| q.inside(*height))
                .map(|(height, position, transaction)| {
                    json!({"tx_hash": transaction, "tx_index": position, "block_height": height,
                        "block_time": index.transactions[transaction].time})
                })
                .collect();
            (200, Value::Array(q.page(rows)))
        }
        ["txs", transaction] => match index.transactions.get(*transaction) {
            Some(placed) => (
                200,
                json!({"hash": transaction, "block": placed.block, "block_height": placed.height,
                    "block_time": placed.time, "slot": placed.time, "index": placed.index,
                    "output_amount": [], "fees": "170000", "deposit": "0", "size": 300,
                    "invalid_before": null, "invalid_hereafter": null,
                    "utxo_count": placed.facts.inputs.len() + placed.facts.outputs.len(),
                    "withdrawal_count": 0, "mir_cert_count": 0, "delegation_count": 0,
                    "stake_cert_count": 0, "pool_update_count": 0, "pool_retire_count": 0,
                    "asset_mint_or_burn_count": if placed.facts.valid { placed.facts.mint.len() } else { 0 },
                    "redeemer_count": 0,
                    "valid_contract": placed.facts.valid, "treasury_donation": "0"}),
            ),
            None => not_found(),
        },
        ["txs", transaction, "utxos"] => match index.transactions.get(*transaction) {
            Some(placed) => (200, utxos(index, transaction, &placed.facts)),
            None => not_found(),
        },
        ["epochs", epoch, "blocks"] => match epoch.parse::<u32>() {
            Ok(epoch) => {
                let rows = index
                    .epoch_blocks
                    .get(&epoch)
                    .map(|hashes| hashes.iter().map(|hash| json!(hash)).collect())
                    .unwrap_or_default();
                (200, Value::Array(q.page(rows)))
            }
            Err(_) => not_found(),
        },
        _ => return None,
    };
    Some(answer)
}

/// `/txs/{hash}/utxos` as the backend renders db-sync's rows. A valid
/// transaction: its body inputs, its reference inputs (`reference`), then its
/// collateral inputs (`collateral`, without native assets); its body outputs
/// with the transaction that spent each, then its collateral return
/// (`collateral`, without native assets, never spent). An invalid transaction:
/// its collateral inputs as ordinary inputs, and its collateral return as its
/// one ordinary output, at the index after the body's outputs.
fn utxos(index: &Index, transaction: &str, facts: &Facts) -> Value {
    let unknown = Output::new(UNKNOWN_ORIGIN_ADDRESS.to_vec(), 5_000_000);
    let input = |input: &Input, collateral: bool, reference: bool| {
        let origin = index.origin(input).unwrap_or(&unknown);
        let (data_hash, inline) = datum_fields(origin);
        json!({"address": address_text(&origin.address), "amount": amount(origin, !collateral),
            "tx_hash": input.hash, "output_index": input.index, "data_hash": data_hash,
            "inline_datum": inline, "reference_script_hash": null, "collateral": collateral,
            "reference": reference})
    };
    let output = |output_index: usize, output: &Output, collateral: bool| {
        let (data_hash, inline) = datum_fields(output);
        let consumed = if collateral {
            None
        } else {
            index
                .spent_by
                .get(&(transaction.to_owned(), output_index as u16))
                .cloned()
        };
        json!({"address": address_text(&output.address), "amount": amount(output, !collateral),
            "output_index": output_index, "data_hash": data_hash, "inline_datum": inline,
            "collateral": collateral, "reference_script_hash": null, "consumed_by_tx": consumed})
    };
    let (inputs, outputs): (Vec<Value>, Vec<Value>) = if facts.valid {
        let inputs = facts
            .inputs
            .iter()
            .map(|i| input(i, false, false))
            .chain(facts.references.iter().map(|i| input(i, false, true)))
            .chain(facts.collateral.iter().map(|i| input(i, true, false)))
            .collect();
        let outputs = facts
            .outputs
            .iter()
            .enumerate()
            .map(|(n, o)| output(n, o, false))
            .chain(
                facts
                    .collateral_return
                    .iter()
                    .map(|o| output(facts.outputs.len(), o, true)),
            )
            .collect();
        (inputs, outputs)
    } else {
        let inputs = facts
            .collateral
            .iter()
            .map(|i| input(i, false, false))
            .collect();
        let outputs = facts
            .collateral_return
            .iter()
            .map(|o| output(facts.outputs.len(), o, false))
            .collect();
        (inputs, outputs)
    };
    json!({"hash": transaction, "inputs": inputs, "outputs": outputs})
}
