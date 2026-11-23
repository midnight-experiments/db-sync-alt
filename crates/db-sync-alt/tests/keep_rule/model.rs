//! An independent model of keep rule version 2 over a generated chain, the
//! oracle for the SQL selection. It shares only the token rule with the product.

use std::collections::{BTreeMap, BTreeSet};

use dsa_chain::{
    assets::AssetRule,
    keep::{Bounds, KeepError, RetainedRows},
    profiles::{Asset, Profile},
};

#[derive(Debug, Clone)]
pub struct Header {
    pub epoch: Option<i32>,
    pub slot: Option<i64>,
}
#[derive(Debug, Clone)]
pub struct Transaction {
    pub block: i64,
    pub index: u16,
}
#[derive(Debug, Clone)]
pub struct Output {
    pub transaction: i64,
    pub index: u16,
    pub address: String,
    pub datum_hash: Option<String>,
}
#[derive(Debug, Clone)]
pub struct Input {
    pub transaction: i64,
    pub ordinal: u16,
    pub output: i64,
}
#[derive(Debug, Clone)]
pub struct OutputAsset {
    pub output: i64,
    pub asset: i64,
}
#[derive(Debug, Clone)]
pub struct Datum {
    pub hash: String,
}
#[derive(Debug, Clone)]
pub struct Stake {
    pub epoch: i32,
    pub pool: i64,
}

/// A pure selection model. Payload values do not influence row retention.
#[derive(Debug, Clone, Default)]
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
    bounds.validate_profile(profile)?;
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
    let rule = AssetRule::from_profile(profile);
    let output_assets: BTreeSet<_> = chain
        .output_assets
        .iter()
        .filter(|(_, row)| {
            let asset = &chain.assets[&row.asset];
            outputs.contains(&row.output) && rule.allows(&asset.policy, &asset.name)
        })
        .map(|(id, _)| *id)
        .collect();
    let referenced_assets: BTreeSet<_> = output_assets
        .iter()
        .map(|id| chain.output_assets[id].asset)
        .collect();
    let fixed_units = dsa_chain::assets::fixed_units(profile);
    let assets = chain
        .assets
        .iter()
        .filter(|(id, asset)| referenced_assets.contains(id) || fixed_units.contains(&asset.unit()))
        .map(|(id, _)| *id)
        .collect();
    let addresses: BTreeSet<_> = [
        "mapping",
        "council",
        "technical_committee",
        "ics",
        "reserve",
    ]
    .iter()
    .map(|role| &profile.addresses[*role])
    .collect();
    let hashes: BTreeSet<_> = outputs
        .iter()
        .filter_map(|id| {
            let output = &chain.outputs[id];
            (addresses.contains(&output.address)
                || chain.has_asset(*id, |asset| asset == permissioned))
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
