//! A scripted chain served the way the provider serves it: headers, the next
//! headers after a block, transaction CBOR in pages of exactly 100, datums,
//! bridge metadata, transaction outputs, epoch parameters, epochs, pools,
//! pool history and each epoch's stake listing. The chain can fork at the
//! tip, contradict a final block and inject faults.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

use serde_json::{Value, json};

use crate::{
    server::{Behavior, Handler},
    tx::{Built, Output, Tx, enterprise_address, hash},
};

/// Blockfrost's page size for lists.
pub const PAGE: usize = 100;

#[derive(Clone, Debug)]
pub struct Block {
    pub hash: String,
    pub height: u32,
    pub slot: u64,
    pub epoch: u32,
    pub time: u64,
    pub previous: Option<String>,
    pub transactions: Vec<Built>,
}

impl Block {
    /// The block as `/blocks/{hash}` renders it.
    pub fn header(&self) -> Value {
        json!({
            "hash": self.hash, "height": self.height, "slot": self.slot, "epoch": self.epoch,
            "epoch_slot": self.slot % 432_000, "time": self.time, "tx_count": self.transactions.len(),
            "size": 1024, "previous_block": self.previous, "next_block": null,
            "slot_leader": "pool1synthetic", "output": "0", "fees": "0", "block_vrf": null,
            "op_cert": null, "op_cert_counter": null, "confirmations": 0
        })
    }

    /// The block after `previous` holding `transactions`, in the same epoch
    /// and twenty slots later.
    pub fn after(previous: &Block, transactions: Vec<Built>) -> Self {
        let mut seed = format!("after {} {}", previous.hash, previous.height + 1).into_bytes();
        for tx in &transactions {
            seed.extend(tx.hash.as_bytes());
        }
        Self {
            hash: hash(&seed),
            height: previous.height + 1,
            slot: previous.slot + 20,
            epoch: previous.epoch,
            time: previous.time + 20,
            previous: Some(previous.hash.clone()),
            transactions,
        }
    }

    /// One page of `/blocks/{hash}/txs/cbor`.
    pub fn page(&self, page: usize) -> Value {
        Value::Array(
            self.transactions
                .iter()
                .skip(page.saturating_sub(1) * PAGE)
                .take(PAGE)
                .map(|tx| json!({"tx_hash": tx.hash, "cbor": hex::encode(&tx.cbor)}))
                .collect(),
        )
    }
}

/// A fault answered to the next `times` requests whose path contains `path`.
#[derive(Clone, Debug)]
pub struct Fault {
    pub path: String,
    pub behavior: Behavior,
    pub times: usize,
}

/// The stake model behind the pool endpoints. The provider's "current"
/// epoch is the epoch of the chain's latest block.
#[derive(Clone, Debug, Default)]
pub struct Pools {
    /// Epoch -> pool (28-byte hash, hexadecimal) -> active stake. A listed
    /// pool holds stake rows in that epoch.
    pub stake: BTreeMap<u32, BTreeMap<String, u64>>,
    /// Pools registered now: `/pools` and `/pools/extended`.
    pub registered: BTreeSet<String>,
    /// Pool -> the epoch it retired in: `/pools/retired`.
    pub retired: BTreeMap<String, u32>,
    /// Epochs whose `/epochs/{n}` reports this active stake instead of the
    /// sum of their distribution.
    pub reported_total: BTreeMap<u32, u64>,
    /// The prefix of the stake addresses in `/epochs/{n}/stakes/{pool}`.
    pub stake_prefix: String,
}

/// The Bech32 pool ID of a 28-byte pool hash in hexadecimal.
pub fn pool_id(hash: &str) -> String {
    let bytes = hex::decode(hash).expect("pool hash");
    bech32::encode::<bech32::Bech32>(bech32::Hrp::parse("pool").expect("hrp"), &bytes)
        .expect("pool id")
}

fn pool_hash(id: &str) -> Option<String> {
    let (hrp, bytes) = bech32::decode(id).ok()?;
    (hrp.as_str() == "pool" && bytes.len() == 28).then(|| hex::encode(bytes))
}

/// One page of 100 of `rows`.
fn paged(rows: Vec<Value>, page: usize) -> Value {
    Value::Array(
        rows.into_iter()
            .skip(page.saturating_sub(1) * PAGE)
            .take(PAGE)
            .collect(),
    )
}

#[derive(Clone, Default)]
pub struct Chain {
    blocks: Vec<Block>,
    routes: BTreeMap<String, Value>,
    faults: Vec<Fault>,
    salt: u64,
    /// Datum bytes by hash: `/scripts/datum/{hash}/cbor`.
    pub datums: BTreeMap<String, Vec<u8>>,
    /// Transaction -> the raw JSON of its bridge metadatum (label 6500973);
    /// `None` and absent transactions have no metadata.
    pub metadata: BTreeMap<String, Option<String>>,
    /// Transaction -> its `/txs/{hash}/utxos` answer.
    pub outputs: BTreeMap<String, Value>,
    /// Epoch -> (nonce, max_tx_size): `/epochs/{n}/parameters`; an absent
    /// epoch is not known yet.
    pub parameters: BTreeMap<u32, (String, u32)>,
    pub pools: Pools,
    /// The network magic `/genesis` names; without one it answers 404.
    pub network_magic: Option<u32>,
}

/// A distinct ordinary transaction, for filling blocks.
pub fn filler(seed: u64) -> Built {
    Tx::new()
        .input(&hash(&seed.to_be_bytes()), 0)
        .output(Output::new(
            enterprise_address(1, [7; 28]),
            1_000_000 + seed,
        ))
        .fee(170_000 + seed)
        .build()
}

impl Chain {
    /// Blocks from `first_height` holding the given numbers of filler
    /// transactions, 20 slots apart.
    pub fn new(first_height: u32, transaction_counts: &[usize]) -> Self {
        let mut chain = Self::default();
        for (block, count) in transaction_counts.iter().enumerate() {
            let transactions = (0..*count)
                .map(|index| filler(((first_height as u64 + block as u64) << 16) | index as u64))
                .collect();
            chain.push_at(first_height + block as u32, transactions);
        }
        chain
    }

    /// A chain of given blocks, such as a generated one.
    pub fn from_blocks(blocks: Vec<Block>) -> Self {
        Self {
            blocks,
            ..Self::default()
        }
    }

    fn push_at(&mut self, height: u32, transactions: Vec<Built>) {
        let previous = self.blocks.last().map(|block| block.hash.clone());
        let slot = u64::from(height) * 20;
        let mut seed = height.to_be_bytes().to_vec();
        seed.extend(self.salt.to_be_bytes());
        seed.extend(previous.as_deref().unwrap_or("").as_bytes());
        for tx in &transactions {
            seed.extend(tx.hash.as_bytes());
        }
        self.blocks.push(Block {
            hash: hash(&seed),
            height,
            slot,
            epoch: (slot / 432_000) as u32,
            time: 1_596_059_091 + slot,
            previous,
            transactions,
        });
    }

    /// Append a block holding these transactions.
    pub fn push(&mut self, transactions: Vec<Built>) -> &Block {
        let height = self.tip().map_or(0, |tip| tip.height + 1);
        self.push_at(height, transactions);
        self.blocks.last().unwrap()
    }

    pub fn tip(&self) -> Option<&Block> {
        self.blocks.last()
    }

    /// The served blocks, to replace or extend while the server runs.
    pub fn blocks_mut(&mut self) -> &mut Vec<Block> {
        &mut self.blocks
    }

    /// The epoch the pool endpoints call current: the latest block's.
    pub fn current_epoch(&self) -> u32 {
        self.tip().map_or(0, |tip| tip.epoch)
    }

    fn pool_stake(&self, epoch: u32, pool: &str) -> Option<u64> {
        self.pools
            .stake
            .get(&epoch)
            .and_then(|pools| pools.get(pool))
            .copied()
    }

    fn pool_row(&self, pool: &str) -> Value {
        let stake = self
            .pool_stake(self.current_epoch(), pool)
            .unwrap_or_default()
            .to_string();
        json!({
            "pool_id": pool_id(pool), "hex": pool, "vrf_key": "0".repeat(64), "blocks_minted": 0,
            "blocks_epoch": 0, "live_stake": stake, "live_size": 0.0, "live_saturation": 0.0,
            "live_delegators": 1, "active_stake": stake, "active_size": 0.0,
            "declared_pledge": "0", "live_pledge": "0", "margin_cost": 0.0, "fixed_cost": "340000000",
            "reward_account": "stake1synthetic", "owners": [], "registration": [], "retirement": []
        })
    }

    /// Answers the provider's datum, metadata, output, epoch and pool
    /// endpoints; `None` for another path.
    fn answer_data(&self, parts: &[&str], page: usize) -> Option<Behavior> {
        let missing = || Behavior::status(404);
        let pools = &self.pools;
        let answer = match parts {
            ["scripts", "datum", hash, "cbor"] => self.datums.get(*hash).map_or_else(missing, |bytes| {
                Behavior::json(json!({"cbor": hex::encode(bytes)}))
            }),
            ["txs", hash, "metadata"] => match self.metadata.get(*hash) {
                Some(Some(raw)) => Behavior::raw(format!(
                    "[{{\"label\":\"6500973\",\"json_metadata\":{raw}}}]"
                )),
                _ => Behavior::json(json!([])),
            },
            ["txs", hash, "utxos"] => self
                .outputs
                .get(*hash)
                .map_or_else(missing, |answer| Behavior::json(answer.clone())),
            ["epochs", epoch, "parameters"] => epoch
                .parse::<u32>()
                .ok()
                .and_then(|epoch| self.parameters.get(&epoch).map(|value| (epoch, value)))
                .map_or_else(missing, |(epoch, (nonce, size))| {
                    Behavior::json(json!({
                        "epoch": epoch, "min_fee_a": 44, "min_fee_b": 155381, "max_block_size": 90112,
                        "max_tx_size": size, "max_block_header_size": 1100, "key_deposit": "2000000",
                        "pool_deposit": "500000000", "nonce": nonce, "protocol_major_ver": 10,
                        "protocol_minor_ver": 0, "coins_per_utxo_size": "4310"
                    }))
                }),
            ["epochs", epoch] => match epoch.parse::<u32>() {
                Ok(epoch) if epoch <= self.current_epoch() => {
                    let sum = pools
                        .stake
                        .get(&epoch)
                        .map(|stake| stake.values().sum::<u64>());
                    let total = pools.reported_total.get(&epoch).copied().or(sum);
                    Behavior::json(json!({
                        "epoch": epoch, "start_time": 1_596_059_091, "end_time": 1_596_491_091,
                        "first_block_time": 1_596_059_091, "last_block_time": 1_596_491_091,
                        "block_count": 0, "tx_count": 0, "output": "0", "fees": "0",
                        "active_stake": total.map(|total| total.to_string())
                    }))
                }
                _ => missing(),
            },
            ["epochs", epoch, "stakes", pool] => {
                let held = epoch
                    .parse::<u32>()
                    .ok()
                    .zip(pool_hash(pool))
                    .and_then(|(epoch, pool)| self.pool_stake(epoch, &pool));
                Behavior::json(match held {
                    Some(amount) if page == 1 => json!([{
                        "stake_address": format!("{}synthetic", pools.stake_prefix),
                        "amount": amount.to_string()
                    }]),
                    _ => json!([]),
                })
            }
            ["pools"] => Behavior::json(paged(
                pools
                    .registered
                    .iter()
                    .map(|pool| json!(pool_id(pool)))
                    .collect(),
                page,
            )),
            ["pools", "extended"] => Behavior::json(paged(
                pools
                    .registered
                    .iter()
                    .map(|pool| self.pool_row(pool))
                    .collect(),
                page,
            )),
            ["pools", "retired"] => {
                let mut retired = pools.retired.iter().collect::<Vec<_>>();
                retired.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));
                Behavior::json(paged(
                    retired
                        .into_iter()
                        .map(|(pool, epoch)| json!({"pool_id": pool_id(pool), "epoch": epoch}))
                        .collect(),
                    page,
                ))
            }
            ["pools", id] => match pool_hash(id) {
                Some(pool) if pools.registered.contains(&pool) || pools.retired.contains_key(&pool) => {
                    Behavior::json(self.pool_row(&pool))
                }
                _ => missing(),
            },
            ["pools", id, "history"] => match pool_hash(id) {
                Some(pool) => Behavior::json(paged(
                    pools
                        .stake
                        .iter()
                        .rev()
                        .filter_map(|(epoch, stake)| {
                            stake.get(&pool).map(|amount| {
                                json!({"epoch": epoch, "blocks": 0, "active_stake": amount.to_string(),
                                    "active_size": 0.0, "delegators_count": 1, "rewards": "0", "fees": "0"})
                            })
                        })
                        .collect(),
                    page,
                )),
                None => missing(),
            },
            _ => return None,
        };
        Some(answer)
    }

    pub fn blocks(&self) -> &[Block] {
        &self.blocks
    }

    pub fn at(&self, height: u32) -> Option<&Block> {
        self.blocks.iter().find(|block| block.height == height)
    }

    pub fn by_hash(&self, hash: &str) -> Option<&Block> {
        self.blocks.iter().find(|block| block.hash == hash)
    }

    /// Replace the top `depth` blocks with blocks of the same heights and
    /// transaction counts but other hashes and other transactions.
    pub fn fork(&mut self, depth: usize) {
        self.salt += 1;
        let keep = self.blocks.len().saturating_sub(depth);
        let replaced: Vec<_> = self.blocks.drain(keep..).collect();
        for block in replaced {
            let transactions = (0..block.transactions.len())
                .map(|index| {
                    filler((self.salt << 40) | (u64::from(block.height) << 16) | index as u64)
                })
                .collect();
            self.push_at(block.height, transactions);
        }
    }

    /// Give the block at `height` another hash while its successor keeps
    /// pointing at the old one, as a provider that contradicts a final block.
    pub fn contradict(&mut self, height: u32) {
        let block = self
            .blocks
            .iter_mut()
            .find(|block| block.height == height)
            .expect("contradicted height");
        block.hash = hash(format!("contradicted {}", block.hash).as_bytes());
    }

    /// Answer `body` for a path below `/api/v0`, such as `/epochs/1/parameters`.
    pub fn route(&mut self, path: &str, body: Value) {
        self.routes.insert(path.to_owned(), body);
    }

    pub fn fault(&mut self, path: &str, behavior: Behavior, times: usize) {
        self.faults.push(Fault {
            path: path.to_owned(),
            behavior,
            times,
        });
    }

    /// Answer one request as the provider would.
    pub fn answer(&mut self, path: &str) -> Behavior {
        if let Some(fault) = self
            .faults
            .iter_mut()
            .find(|fault| fault.times > 0 && path.contains(&fault.path))
        {
            fault.times -= 1;
            return fault.behavior.clone();
        }
        let path = path.strip_prefix("/api/v0").unwrap_or(path);
        let (route, query) = path.split_once('?').unwrap_or((path, ""));
        let page = query
            .split('&')
            .find_map(|pair| pair.strip_prefix("page="))
            .and_then(|page| page.parse::<usize>().ok())
            .unwrap_or(1);
        if let Some(body) = self.routes.get(route) {
            return Behavior::json(body.clone());
        }
        let parts: Vec<_> = route.trim_start_matches('/').split('/').collect();
        let missing = || Behavior::status(404);
        match parts.as_slice() {
            ["genesis"] => self.network_magic.map_or_else(missing, |magic| {
                Behavior::json(json!({
                    "active_slots_coefficient": 0.05, "update_quorum": 5,
                    "max_lovelace_supply": "45000000000000000", "network_magic": magic,
                    "epoch_length": 432000, "system_start": 1654041600,
                    "slots_per_kes_period": 129600, "slot_length": 1,
                    "max_kes_evolutions": 62, "security_param": 2160
                }))
            }),
            ["blocks", "latest"] => self
                .tip()
                .map_or_else(missing, |tip| Behavior::json(tip.header())),
            ["blocks", id] => {
                let block = match id.parse::<u32>() {
                    Ok(height) => self.at(height),
                    Err(_) => self.by_hash(id),
                };
                block.map_or_else(missing, |block| Behavior::json(block.header()))
            }
            ["blocks", id, "next"] => {
                match self.blocks.iter().position(|block| block.hash == *id) {
                    Some(position) => Behavior::json(Value::Array(
                        self.blocks[position + 1..]
                            .iter()
                            .skip((page - 1) * PAGE)
                            .take(PAGE)
                            .map(Block::header)
                            .collect(),
                    )),
                    None => missing(),
                }
            }
            ["blocks", id, "txs", "cbor"] => self
                .by_hash(id)
                .map_or_else(missing, |block| Behavior::json(block.page(page))),
            _ => self.answer_data(&parts, page).unwrap_or_else(missing),
        }
    }

    /// A server handler over a shared chain that a test can change while
    /// the server runs.
    pub fn handler(chain: Arc<Mutex<Self>>) -> Handler {
        Arc::new(move |path, _| chain.lock().unwrap().answer(path))
    }
}
