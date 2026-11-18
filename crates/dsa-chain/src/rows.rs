//! Pure row construction. I/O callers supply held origins and any explicitly
//! requested output, datum or metadata supplements before publication.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{
    cbor::{Error, error},
    datum, ids,
    profiles::Profile,
    transaction::{Asset, Outpoint, Output, Transaction, hash},
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TxRow {
    pub id: i64,
    pub hash: String,
    pub block_id: i64,
    pub block_index: u16,
    pub valid_contract: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct OutputRow {
    pub id: i64,
    pub tx_id: i64,
    pub index: u16,
    pub address: String,
    pub value: String,
    pub data_hash: Option<String>,
    pub consumed_by_tx_id: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AssetRow {
    pub id: i64,
    pub quantity: String,
    pub tx_out_id: i64,
    pub policy: String,
    pub name: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct InputRow {
    pub id: i64,
    pub tx_in_id: i64,
    pub tx_out_id: i64,
    pub tx_out_index: u16,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DatumRow {
    pub hash: String,
    pub value: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetadataRow {
    pub id: i64,
    pub tx_id: i64,
    pub key: u64,
    pub json: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MintRow {
    pub tx_hash: String,
    pub policy: String,
    pub name: String,
    pub quantity: String,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Rows {
    pub tx: Vec<TxRow>,
    pub tx_out: Vec<OutputRow>,
    pub ma_tx_out: Vec<AssetRow>,
    pub tx_in: Vec<InputRow>,
    pub datum: Vec<DatumRow>,
    pub tx_metadata: Vec<MetadataRow>,
    pub mint: Vec<MintRow>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HeldOutput {
    pub transaction_id: i64,
    pub output: Output,
}

#[derive(Default)]
pub struct Supplements {
    /// Resolved outputs are indexed exactly as in the CBOR body; invalid
    /// transactions have only the collateral return at body.outputs.len().
    pub outputs: BTreeMap<String, BTreeMap<u16, Output>>,
    pub datums: BTreeMap<String, Vec<u8>>,
    /// ICS transaction -> label 6500973 absent, or its exact provider JSON.
    pub metadata: BTreeMap<String, Option<String>>,
}

pub struct Selection<'a> {
    profile: &'a Profile,
    addresses: BTreeSet<String>,
    units: BTreeSet<String>,
    governance_policies: BTreeSet<String>,
}

impl<'a> Selection<'a> {
    pub fn new(profile: &'a Profile) -> Self {
        Self {
            profile,
            addresses: profile.addresses.values().cloned().collect(),
            units: profile.assets.values().map(|a| a.unit()).collect(),
            governance_policies: ["council", "technical_committee"]
                .iter()
                .map(|k| profile.assets[*k].policy.clone())
                .collect(),
        }
    }
    pub fn required(&self, asset: &Asset) -> bool {
        self.units.contains(&asset.unit()) || self.governance_policies.contains(&asset.policy)
    }
    pub fn relevant(&self, output: &Output) -> bool {
        output
            .address
            .as_ref()
            .is_some_and(|a| self.addresses.contains(a))
            || output.assets.iter().any(|a| self.required(a))
    }
    pub fn selected(&self, tx: &Transaction<'_>, held: &BTreeMap<Outpoint, HeldOutput>) -> bool {
        let outputs: Vec<_> = if tx.valid {
            tx.outputs.iter().map(Option::as_ref).collect()
        } else if tx.has_collateral_return {
            vec![tx.collateral_output.as_ref()]
        } else {
            vec![]
        };
        outputs
            .into_iter()
            .any(|o| o.is_none_or(|o| !o.classification_safe || self.relevant(o)))
            || tx.valid && tx.mint.iter().any(|a| self.required(a))
            || (if tx.valid {
                &tx.ordinary
            } else {
                &tx.collateral
            })
            .iter()
            .any(|p| held.contains_key(p))
    }
    fn keep_all_assets(&self, output: &Output) -> bool {
        ["ics", "reserve"]
            .iter()
            .any(|k| output.address.as_ref() == self.profile.addresses.get(*k))
    }
    fn needs_datum(&self, output: &Output) -> bool {
        output
            .address
            .as_ref()
            .is_some_and(|a| self.addresses.contains(a))
            || output
                .assets
                .iter()
                .any(|a| a.unit() == self.profile.assets["permissioned_candidates"].unit())
    }
}

fn ordinal(n: usize) -> Result<u16, Error> {
    let n = u16::try_from(n).map_err(|_| error("invalid_ordinal", 0))?;
    if n >= ids::INDEX_LIMIT {
        Err(error("invalid_ordinal", 0))
    } else {
        Ok(n)
    }
}
fn id(result: Result<i64, ids::IdError>) -> Result<i64, Error> {
    result.map_err(|_| error("invalid_positional_id", 0))
}

fn datum_for(
    output: &Output,
    tx: &Transaction<'_>,
    supplements: &Supplements,
    datums: &mut BTreeMap<String, DatumRow>,
) -> Result<(), Error> {
    if let Some(expected) = &output.data_hash {
        if datums.contains_key(expected) {
            return Ok(());
        }
        let raw = output
            .inline_datum
            .as_ref()
            .or_else(|| tx.datums.get(expected))
            .or_else(|| supplements.datums.get(expected));
        if let Some(raw) = raw {
            if hash(raw) != *expected {
                return Err(error("provider_datum_hash_mismatch", 0));
            }
            datums.insert(
                expected.clone(),
                DatumRow {
                    hash: expected.clone(),
                    value: datum::render(raw)?,
                },
            );
        }
    }
    Ok(())
}

/// Build one block in inventory order. Held origins include all relevant
/// outputs spent by this range, plus creations in earlier blocks. The caller
/// can do one batched database lookup for these outpoints before this function.
pub fn build_block(
    profile: &Profile,
    height: u32,
    transactions: &[Transaction<'_>],
    held: &mut BTreeMap<Outpoint, HeldOutput>,
    supplements: &Supplements,
) -> Result<Rows, Error> {
    let selection = Selection::new(profile);
    let mut rows = Rows::default();
    let mut datums = BTreeMap::new();
    let mut seen = BTreeSet::new();
    for (position, tx) in transactions.iter().enumerate() {
        if !seen.insert(&tx.hash) {
            return Err(error("provider_duplicate_transaction", 0));
        }
        let tx_index = ordinal(position)?;
        let tx_id = id(ids::transaction_id(height, tx_index))?;
        let inputs = if tx.valid {
            &tx.ordinary
        } else {
            &tx.collateral
        };
        let mut kept_outputs = Vec::new();
        let candidates: Vec<_> = if tx.valid {
            tx.outputs
                .iter()
                .enumerate()
                .map(|(i, o)| (i, o.as_ref()))
                .collect()
        } else if tx.has_collateral_return {
            vec![(tx.outputs.len(), tx.collateral_output.as_ref())]
        } else {
            vec![]
        };
        for (index, output) in candidates {
            let index = ordinal(index)?;
            let output = output
                .filter(|o| o.classification_safe)
                .or_else(|| {
                    supplements
                        .outputs
                        .get(&tx.hash)
                        .and_then(|m| m.get(&index))
                })
                .ok_or_else(|| error("provider_output_resolution_required", 0))?;
            if selection.relevant(output) {
                kept_outputs.push((index, output));
            }
        }
        let mints: Vec<_> = tx
            .mint
            .iter()
            .filter(|a| tx.valid && selection.required(a))
            .collect();
        let spends: Vec<_> = inputs
            .iter()
            .enumerate()
            .filter(|(_, p)| held.contains_key(*p))
            .collect();
        if kept_outputs.is_empty() && mints.is_empty() && spends.is_empty() {
            continue;
        }
        rows.tx.push(TxRow {
            id: tx_id,
            hash: tx.hash.clone(),
            block_id: i64::from(height),
            block_index: tx_index,
            valid_contract: tx.valid,
        });
        for (n, point) in spends {
            let origin = held.get(point).expect("selected held origin");
            rows.tx_in.push(InputRow {
                id: id(ids::input_id(height, tx_index, ordinal(n)?))?,
                tx_in_id: tx_id,
                tx_out_id: origin.transaction_id,
                tx_out_index: point.index,
            });
            if selection.needs_datum(&origin.output) {
                datum_for(&origin.output, tx, supplements, &mut datums)?;
            }
        }
        for point in inputs {
            held.remove(point);
        }
        let mut ics = false;
        for (index, output) in kept_outputs {
            let output_id = id(ids::output_id(height, tx_index, index))?;
            let address = output
                .address
                .clone()
                .ok_or_else(|| error("provider_output_resolution_required", 0))?;
            ics |= profile.addresses.get("ics") == Some(&address);
            rows.tx_out.push(OutputRow {
                id: output_id,
                tx_id,
                index,
                address,
                value: output.coin.to_string(),
                data_hash: output.data_hash.clone(),
                consumed_by_tx_id: None,
            });
            for (n, asset) in output.assets.iter().enumerate() {
                if selection.keep_all_assets(output) || selection.required(asset) {
                    rows.ma_tx_out.push(AssetRow {
                        id: id(ids::asset_id(height, tx_index, index, ordinal(n)?))?,
                        quantity: asset.quantity.to_string(),
                        tx_out_id: output_id,
                        policy: asset.policy.clone(),
                        name: asset.name.clone(),
                    });
                }
            }
            if selection.needs_datum(output) {
                datum_for(output, tx, supplements, &mut datums)?;
            }
            held.insert(
                Outpoint {
                    hash: tx.hash.clone(),
                    index,
                },
                HeldOutput {
                    transaction_id: tx_id,
                    output: output.clone(),
                },
            );
        }
        for asset in mints {
            rows.mint.push(MintRow {
                tx_hash: tx.hash.clone(),
                policy: asset.policy.clone(),
                name: asset.name.clone(),
                quantity: asset.quantity.to_string(),
            });
        }
        if ics && tx.valid {
            let metadata = supplements
                .metadata
                .get(&tx.hash)
                .ok_or_else(|| error("provider_metadata_resolution_required", 0))?;
            if let Some(json) = metadata {
                rows.tx_metadata.push(MetadataRow {
                    id: tx_id,
                    tx_id,
                    key: 6500973,
                    json: (json_depth(json) <= datum::JSON_NESTING_LIMIT).then(|| json.clone()),
                });
            }
        }
    }
    rows.datum = datums.into_values().collect();
    Ok(rows)
}

fn json_depth(text: &str) -> usize {
    let (mut quoted, mut escape, mut depth, mut maximum) = (false, false, 0usize, 0usize);
    for byte in text.bytes() {
        if escape {
            escape = false;
        } else if quoted && byte == b'\\' {
            escape = true;
        } else if byte == b'"' {
            quoted = !quoted;
        } else if !quoted && b"{[".contains(&byte) {
            depth += 1;
            maximum = maximum.max(depth);
        } else if !quoted && b"}]".contains(&byte) {
            depth = depth.saturating_sub(1);
        }
    }
    maximum
}
