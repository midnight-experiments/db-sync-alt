//! A scripted chain served the way the provider serves it: headers, the next
//! headers after a block, and transaction CBOR in pages of exactly 100. The
//! chain can fork at the tip, contradict a final block and inject faults.

use std::{
    collections::BTreeMap,
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

#[derive(Clone, Default)]
pub struct Chain {
    blocks: Vec<Block>,
    routes: BTreeMap<String, Value>,
    faults: Vec<Fault>,
    salt: u64,
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
            _ => missing(),
        }
    }

    /// A server handler over a shared chain that a test can change while
    /// the server runs.
    pub fn handler(chain: Arc<Mutex<Self>>) -> Handler {
        Arc::new(move |path, _| chain.lock().unwrap().answer(path))
    }
}
