//! Keep rule v1, bound to Midnight node 2.1.101.
//!
//! H is the oldest event block, T the last served event block and P the stored
//! header tip. All ranges include their endpoints. The input contains the full
//! selected history, with positional IDs and complete transaction/output links.
//!
//! Query paths below are relative to `reference/`. The observation queries are
//! in `primitives/mainchain-follower/src/db/queries/`, candidates in
//! `primitives/mainchain-follower/src/data_source/candidates_data_source/`.
//!
//! * K1 keeps headers H..P. `WHERE block_no >= $1 AND block_no <= $2`
//!   serves header ranges (`partner-chains/toolkit/data-sources/db-sync/src/db_model.rs:253`).
//! * K2 keeps transaction/output/spend/asset/metadata rows in H..T. The event
//!   predicates include `block.block_no >= $2 AND block.block_no <= $4`
//!   (`reference/primitives/mainchain-follower/src/db/queries/cnight_observation.rs:163`).
//! * K3 keeps old outputs without a spend before H, their assets and creating
//!   transactions. `get_asset_spends` joins `tx_out.tx_id = tx_in.tx_out_id`
//!   and the output index (`cnight_observation.rs:214`); it does not join the
//!   creating header. Mapping deregistration has the same old-output dependency.
//! * K4 keeps existing datums of retained outputs at a profile address, holding
//!   a governance-policy asset or the exact permissioned-candidates unit.
//!   Registration/deregistration JOIN datum (`cnight_observation.rs:54,114`),
//!   governance JOINs datum and permissioned candidates LEFT JOINs it.
//! * K5 keeps every output in the newest eligible old governance transaction,
//!   with its transaction/header/assets/datum. Eligibility includes a datum row:
//!   `JOIN datum ON tx_out.data_hash = datum.hash`, address, policy and
//!   `block.block_no <= $3` (`federated_authority_observation.rs:44-54`).
//!   All newest transaction ties stay because the upstream LIMIT order omits
//!   output index. NULL datum values are still existing datum rows.
//! * K6 keeps the newest old permissioned transaction at epoch <= epoch(H)-2,
//!   every later eligible old output, and their headers. Match the exact unit:
//!   `WHERE ma_tx_out.ident = $1 AND origin_block.epoch_no <= $2`
//!   (`candidates_data_source/db_model.rs:279-280`). Its ORDER BY
//!   also omits output index, so complete newest transaction ties stay.
//! * K7 keeps the latest non-NULL-slot header at or before each preceding epoch
//!   cutoff, epoch(H)-2 and epoch(H)-1. Empty epochs can use an earlier header:
//!   `block.epoch_no <= $1 AND block.slot_no IS NOT NULL`
//!   (`candidates_data_source/db_model.rs:218`). Compare slot order, as the SQL does.
//! * K8 keeps epoch parameters/stake at epoch >= epoch(H)-2 and referenced pools.
//!   Nonce/stake bind `WHERE epoch_no = $1` and `WHERE es.epoch_no = $1`
//!   (`candidates_data_source/db_model.rs:293,253`). The cursor's maximum size
//!   uses `epoch_param.epoch_no = block.epoch_no` (`cnight_observation.rs:458`).
//! * K9 keeps the profile units and every asset referenced by a retained row.
//!   The cache resolves `WHERE policy = $1 AND name = $2` once
//!   (`primitives/mainchain-follower/src/db/multi_asset_cache.rs:51`).
//! * K10 keeps the newest old spend, its spending transaction, spent output and
//!   output dependencies when H..T has no spend. The startup `EXISTS(SELECT 1
//!   FROM tx_in)` chooses the node's spend path (`candidates_data_source/db_model.rs:90`).
//!
//! A source with no spend anywhere cannot satisfy the serving invariant and
//! remains subject to verification before publication. Selection invents no row.
//! Coarse helper IDs are checked against each database’s bound guarantees;
//! downstream event and state rows are compared exactly.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    ids::HEIGHT_LIMIT,
    profiles::{Asset, Profile},
};

pub const TABLES: [&str; 11] = [
    "block",
    "tx",
    "tx_out",
    "tx_in",
    "ma_tx_out",
    "multi_asset",
    "datum",
    "epoch_param",
    "pool_hash",
    "epoch_stake",
    "tx_metadata",
];
pub type RetainedRows = BTreeMap<String, BTreeSet<i64>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bounds {
    pub horizon: u32,
    pub target: u32,
    pub tip: u32,
}

impl Bounds {
    pub fn validate(self) -> Result<(), KeepError> {
        if self.horizon > self.target || self.target > self.tip || self.tip >= HEIGHT_LIMIT {
            return Err(KeepError::Bounds);
        }
        Ok(())
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum KeepError {
    #[error("invalid_keep_bounds")]
    Bounds,
    #[error("missing_horizon_epoch")]
    Horizon,
    #[error("incomplete_chain_model")]
    Reference,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Header {
    pub epoch: Option<i32>,
    pub slot: Option<i64>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub block: i64,
    pub index: u16,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Output {
    pub transaction: i64,
    pub index: u16,
    pub address: String,
    pub datum_hash: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Input {
    pub transaction: i64,
    pub ordinal: u16,
    pub output: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputAsset {
    pub output: i64,
    pub asset: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Datum {
    pub hash: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stake {
    pub epoch: i32,
    pub pool: i64,
}

/// A pure selection model. Payload values do not influence row retention.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Chain {
    pub blocks: BTreeMap<i64, Header>,
    pub transactions: BTreeMap<i64, Transaction>,
    pub outputs: BTreeMap<i64, Output>,
    pub inputs: BTreeMap<i64, Input>,
    pub output_assets: BTreeMap<i64, OutputAsset>,
    pub assets: BTreeMap<i64, Asset>,
    pub datums: BTreeMap<i64, Datum>,
    pub epochs: BTreeSet<i32>,
    pub pools: BTreeSet<i64>,
    pub stakes: BTreeMap<i64, Stake>,
    pub metadata: BTreeMap<i64, i64>,
}

impl Chain {
    fn validate(&self) -> Result<(), KeepError> {
        if !self
            .transactions
            .values()
            .all(|tx| self.blocks.contains_key(&tx.block))
            || !self
                .outputs
                .values()
                .all(|output| self.transactions.contains_key(&output.transaction))
            || !self.inputs.values().all(|input| {
                self.transactions.contains_key(&input.transaction)
                    && self.outputs.contains_key(&input.output)
            })
            || !self.output_assets.values().all(|row| {
                self.outputs.contains_key(&row.output) && self.assets.contains_key(&row.asset)
            })
            || !self
                .stakes
                .values()
                .all(|row| self.pools.contains(&row.pool))
            || !self
                .metadata
                .values()
                .all(|tx| self.transactions.contains_key(tx))
        {
            return Err(KeepError::Reference);
        }
        Ok(())
    }

    fn position(&self, transaction: i64) -> (i64, u16) {
        let tx = &self.transactions[&transaction];
        (tx.block, tx.index)
    }

    fn has_asset(&self, output: i64, predicate: impl Fn(&Asset) -> bool) -> bool {
        self.output_assets
            .values()
            .any(|row| row.output == output && predicate(&self.assets[&row.asset]))
    }

    fn newest_outputs(&self, candidates: &BTreeSet<i64>) -> BTreeSet<i64> {
        let newest = candidates
            .iter()
            .map(|id| self.position(self.outputs[id].transaction))
            .max();
        candidates
            .iter()
            .copied()
            .filter(|id| Some(self.position(self.outputs[id].transaction)) == newest)
            .collect()
    }
}

pub fn keep(chain: &Chain, profile: &Profile, bounds: Bounds) -> Result<RetainedRows, KeepError> {
    bounds.validate()?;
    chain.validate()?;
    let h = i64::from(bounds.horizon);
    let t = i64::from(bounds.target);
    let p = i64::from(bounds.tip);
    let epoch = chain
        .blocks
        .get(&h)
        .and_then(|block| block.epoch)
        .ok_or(KeepError::Horizon)?;
    let cutoff = epoch - 2;
    let in_window = |tx: &Transaction| tx.block >= h && tx.block <= t;
    let mut blocks: BTreeSet<_> = chain
        .blocks
        .keys()
        .copied()
        .filter(|height| *height >= h && *height <= p)
        .collect();
    let mut transactions: BTreeSet<_> = chain
        .transactions
        .iter()
        .filter(|(_, tx)| in_window(tx))
        .map(|(id, _)| *id)
        .collect();
    let mut inputs: BTreeSet<_> = chain
        .inputs
        .iter()
        .filter(|(_, input)| in_window(&chain.transactions[&input.transaction]))
        .map(|(id, _)| *id)
        .collect();
    let mut outputs: BTreeSet<_> = chain
        .outputs
        .iter()
        .filter(|(id, output)| {
            let tx = &chain.transactions[&output.transaction];
            in_window(tx)
                || (tx.block < h
                    && !chain.inputs.values().any(|input| {
                        input.output == **id && chain.transactions[&input.transaction].block < h
                    }))
        })
        .map(|(id, _)| *id)
        .collect();
    let mut carry = BTreeSet::new();
    for body in ["council", "technical_committee"] {
        let candidates = chain
            .outputs
            .iter()
            .filter(|(id, output)| {
                chain.transactions[&output.transaction].block < h
                    && output.address == profile.addresses[body]
                    && output
                        .datum_hash
                        .as_ref()
                        .is_some_and(|hash| chain.datums.values().any(|datum| datum.hash == *hash))
                    && chain.has_asset(**id, |asset| asset.policy == profile.assets[body].policy)
            })
            .map(|(id, _)| *id)
            .collect();
        carry.extend(chain.newest_outputs(&candidates));
    }
    let permissioned = &profile.assets["permissioned_candidates"];
    let eligible: BTreeSet<_> = chain
        .outputs
        .iter()
        .filter(|(id, output)| {
            chain.transactions[&output.transaction].block < h
                && chain.has_asset(**id, |asset| asset == permissioned)
        })
        .map(|(id, _)| *id)
        .collect();
    let older = eligible
        .iter()
        .copied()
        .filter(|id| {
            chain.blocks[&chain.transactions[&chain.outputs[id].transaction].block]
                .epoch
                .is_some_and(|epoch| epoch <= cutoff)
        })
        .collect();
    carry.extend(chain.newest_outputs(&older));
    carry.extend(eligible.iter().copied().filter(|id| {
        chain.blocks[&chain.transactions[&chain.outputs[id].transaction].block]
            .epoch
            .is_some_and(|epoch| epoch > cutoff)
    }));
    for id in &carry {
        blocks.insert(chain.transactions[&chain.outputs[id].transaction].block);
    }
    outputs.extend(carry);
    for bound in [cutoff, cutoff + 1] {
        let latest = chain
            .blocks
            .iter()
            .filter(|(id, block)| **id < h && block.epoch.is_some_and(|epoch| epoch <= bound))
            .filter_map(|(_, block)| block.slot)
            .max();
        blocks.extend(
            chain
                .blocks
                .iter()
                .filter(|(id, block)| {
                    **id < h
                        && block.epoch.is_some_and(|epoch| epoch <= bound)
                        && block.slot.is_some()
                        && block.slot == latest
                })
                .map(|(id, _)| *id),
        );
    }
    if inputs.is_empty() {
        let fallback = chain
            .inputs
            .iter()
            .filter(|(_, input)| chain.transactions[&input.transaction].block < h)
            .max_by_key(|(_, input)| (chain.position(input.transaction), input.ordinal));
        if let Some((id, input)) = fallback {
            inputs.insert(*id);
            outputs.insert(input.output);
            transactions.insert(input.transaction);
        }
    }
    transactions.extend(outputs.iter().map(|id| chain.outputs[id].transaction));
    let output_assets: BTreeSet<_> = chain
        .output_assets
        .iter()
        .filter(|(_, row)| outputs.contains(&row.output))
        .map(|(id, _)| *id)
        .collect();
    let referenced_assets: BTreeSet<_> = output_assets
        .iter()
        .map(|id| chain.output_assets[id].asset)
        .collect();
    let fixed_units: BTreeSet<_> = profile.assets.values().map(Asset::unit).collect();
    let assets = chain
        .assets
        .iter()
        .filter(|(id, asset)| referenced_assets.contains(id) || fixed_units.contains(&asset.unit()))
        .map(|(id, _)| *id)
        .collect();
    let addresses: BTreeSet<_> = profile.addresses.values().collect();
    let governance: BTreeSet<_> = [
        &profile.assets["council"].policy,
        &profile.assets["technical_committee"].policy,
    ]
    .into_iter()
    .collect();
    let hashes: BTreeSet<_> = outputs
        .iter()
        .filter_map(|id| {
            let output = &chain.outputs[id];
            (addresses.contains(&output.address)
                || chain.has_asset(*id, |asset| {
                    governance.contains(&asset.policy) || asset == permissioned
                }))
            .then_some(output.datum_hash.as_ref())
            .flatten()
        })
        .collect();
    let datums = chain
        .datums
        .iter()
        .filter(|(_, datum)| hashes.contains(&datum.hash))
        .map(|(id, _)| *id)
        .collect();
    let epochs = chain
        .epochs
        .iter()
        .copied()
        .filter(|epoch| *epoch >= cutoff)
        .map(i64::from)
        .collect();
    let stakes: BTreeSet<_> = chain
        .stakes
        .iter()
        .filter(|(_, row)| row.epoch >= cutoff)
        .map(|(id, _)| *id)
        .collect();
    let pools = stakes.iter().map(|id| chain.stakes[id].pool).collect();
    let metadata = chain
        .metadata
        .iter()
        .filter(|(_, tx)| in_window(&chain.transactions[tx]))
        .map(|(id, _)| *id)
        .collect();
    Ok(BTreeMap::from([
        ("block".to_owned(), blocks),
        ("tx".to_owned(), transactions),
        ("tx_out".to_owned(), outputs),
        ("tx_in".to_owned(), inputs),
        ("ma_tx_out".to_owned(), output_assets),
        ("multi_asset".to_owned(), assets),
        ("datum".to_owned(), datums),
        ("epoch_param".to_owned(), epochs),
        ("pool_hash".to_owned(), pools),
        ("epoch_stake".to_owned(), stakes),
        ("tx_metadata".to_owned(), metadata),
    ]))
}
