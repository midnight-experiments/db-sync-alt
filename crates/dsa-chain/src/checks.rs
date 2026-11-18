//! Local checks over finalized facts before a block update is published.
//! Callers provide held relevant inputs from one batched lookup. Untracked
//! inputs contribute no profile assets. A balance mismatch identifies a
//! required holding missing from that lookup without fetching its origin.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    ids,
    profiles::Profile,
    rows::{self, BuildError, HeldChanges, HeldOutput, Selection, Supplements},
    transaction::{self, Asset, Outpoint, Output, Transaction},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Header {
    pub height: u32,
    pub hash: String,
    pub previous_hash: Option<String>,
    pub transaction_count: usize,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum CheckError {
    #[error("header_height at block {height}, expected {expected}")]
    HeaderHeight { height: u32, expected: u32 },
    #[error("header_parent at block {height}")]
    HeaderParent { height: u32 },
    #[error("block_not_above_target at block {height}, previous target {target}")]
    NotAboveTarget { height: u32, target: u32 },
    #[error("transaction_count at block {height}, header {expected}, decoded {actual}")]
    TransactionCount {
        height: u32,
        expected: usize,
        actual: usize,
    },
    #[error("unsupported_position at block {height}")]
    Position { height: u32 },
    #[error("transaction_body_hash at block {height}, transaction {transaction}")]
    BodyHash { height: u32, transaction: String },
    #[error("duplicate_transaction at block {height}, transaction {transaction}")]
    DuplicateTransaction { height: u32, transaction: String },
    #[error(
        "asset_conservation at block {height}, transaction {transaction}, unit {unit}, held {inputs}, mint {mint}, outputs {outputs}"
    )]
    AssetBalance {
        height: u32,
        transaction: String,
        unit: String,
        inputs: i128,
        mint: i128,
        outputs: i128,
    },
    #[error("asset_quantity at block {height}, transaction {transaction:?}, unit {unit}")]
    AssetQuantity {
        height: u32,
        transaction: Option<String>,
        unit: String,
    },
    #[error(
        "supply_mismatch at block {height}, unit {unit}, recorded {expected}, unspent {actual}"
    )]
    SupplyMismatch {
        height: u32,
        unit: String,
        expected: i128,
        actual: i128,
    },
    #[error("supply_units at block {height}")]
    SupplyUnits { height: u32 },
    #[error(transparent)]
    Supplement(#[from] BuildError),
}

#[derive(Debug, PartialEq, Eq)]
pub struct BlockEffects {
    /// Only configured full units participate in the stored supply ledger.
    pub mint_delta: BTreeMap<String, i128>,
    pub transactions_checked: usize,
}

/// Physical retained outputs can include old spent governance records. Their
/// spend state must come from window spends or the protected old-spent marker.
pub struct SupplyOutput<'a> {
    pub output: &'a Output,
    pub spent: bool,
}

fn fixed_units(profile: &Profile) -> BTreeSet<String> {
    profile.assets.values().map(|asset| asset.unit()).collect()
}

fn add_assets(
    totals: &mut BTreeMap<String, i128>,
    assets: &[Asset],
    selection: &Selection<'_>,
    height: u32,
    transaction: &str,
    mint: bool,
) -> Result<(), CheckError> {
    for asset in assets.iter().filter(|asset| selection.required(asset)) {
        let unit = asset.unit();
        let invalid = if mint {
            i64::try_from(asset.quantity).is_err()
        } else {
            u64::try_from(asset.quantity).is_err()
        };
        if invalid {
            return Err(CheckError::AssetQuantity {
                height,
                transaction: Some(transaction.to_owned()),
                unit,
            });
        }
        let quantity = totals.entry(unit.clone()).or_default();
        *quantity =
            quantity
                .checked_add(asset.quantity)
                .ok_or_else(|| CheckError::AssetQuantity {
                    height,
                    transaction: Some(transaction.to_owned()),
                    unit,
                })?;
    }
    Ok(())
}

/// Check a finalized block without mutating the caller's held set or supply.
/// Same-block creations are visible through the same bounded overlay used by
/// row construction. Missing output supplements retain their typed request.
pub fn check_block(
    profile: &Profile,
    previous: &Header,
    header: &Header,
    previous_target: u32,
    transactions: &[Transaction<'_>],
    held: &BTreeMap<Outpoint, HeldOutput>,
    supplements: &Supplements,
) -> Result<BlockEffects, CheckError> {
    if header.height <= previous_target {
        return Err(CheckError::NotAboveTarget {
            height: header.height,
            target: previous_target,
        });
    }
    if ids::block_id(header.height).is_err() || transactions.len() > usize::from(ids::INDEX_LIMIT) {
        return Err(CheckError::Position {
            height: header.height,
        });
    }
    let expected = previous.height.saturating_add(1);
    if header.height != expected {
        return Err(CheckError::HeaderHeight {
            height: header.height,
            expected,
        });
    }
    if header.previous_hash.as_ref() != Some(&previous.hash) {
        return Err(CheckError::HeaderParent {
            height: header.height,
        });
    }
    if transactions.len() != header.transaction_count {
        return Err(CheckError::TransactionCount {
            height: header.height,
            expected: header.transaction_count,
            actual: transactions.len(),
        });
    }
    let selection = Selection::new(profile);
    let units = fixed_units(profile);
    let mut effects = BlockEffects {
        mint_delta: BTreeMap::new(),
        transactions_checked: 0,
    };
    let mut staged = HeldChanges::new(held);
    let mut seen = BTreeSet::new();
    for (index, tx) in transactions.iter().enumerate() {
        if !seen.insert(&tx.hash) {
            return Err(CheckError::DuplicateTransaction {
                height: header.height,
                transaction: tx.hash.clone(),
            });
        }
        if transaction::hash(tx.body) != tx.hash {
            return Err(CheckError::BodyHash {
                height: header.height,
                transaction: tx.hash.clone(),
            });
        }
        let mut inputs = BTreeMap::new();
        for point in tx.effective_inputs() {
            if let Some(origin) = staged.get(point) {
                add_assets(
                    &mut inputs,
                    &origin.output.assets,
                    &selection,
                    header.height,
                    &tx.hash,
                    false,
                )?;
            }
        }
        let effective_outputs = rows::effective_outputs(tx, supplements)?;
        let mut outputs = BTreeMap::new();
        for (_, output) in &effective_outputs {
            add_assets(
                &mut outputs,
                &output.assets,
                &selection,
                header.height,
                &tx.hash,
                false,
            )?;
        }
        let mut mint = BTreeMap::new();
        if tx.valid {
            add_assets(
                &mut mint,
                &tx.mint,
                &selection,
                header.height,
                &tx.hash,
                true,
            )?;
        }
        let checked: BTreeSet<_> = inputs
            .keys()
            .chain(outputs.keys())
            .chain(mint.keys())
            .collect();
        for unit in checked {
            let input = inputs.get(unit).copied().unwrap_or_default();
            let minted = mint.get(unit).copied().unwrap_or_default();
            let output = outputs.get(unit).copied().unwrap_or_default();
            if input.checked_add(minted) != Some(output) {
                return Err(CheckError::AssetBalance {
                    height: header.height,
                    transaction: tx.hash.clone(),
                    unit: unit.clone(),
                    inputs: input,
                    mint: minted,
                    outputs: output,
                });
            }
        }
        for (unit, amount) in mint.into_iter().filter(|(unit, _)| units.contains(unit)) {
            let total = effects.mint_delta.entry(unit.clone()).or_default();
            *total = total
                .checked_add(amount)
                .ok_or_else(|| CheckError::AssetQuantity {
                    height: header.height,
                    transaction: Some(tx.hash.clone()),
                    unit,
                })?;
        }
        for point in tx.effective_inputs() {
            staged.remove(point);
        }
        let transaction_index = u16::try_from(index).map_err(|_| CheckError::Position {
            height: header.height,
        })?;
        let transaction_id =
            ids::transaction_id(header.height, transaction_index).map_err(|_| {
                CheckError::Position {
                    height: header.height,
                }
            })?;
        for (index, output) in effective_outputs
            .into_iter()
            .filter(|(_, output)| selection.relevant(output))
        {
            staged.insert(
                Outpoint {
                    hash: tx.hash.clone(),
                    index,
                },
                HeldOutput {
                    transaction_id,
                    output: output.clone(),
                },
            );
        }
        effects.transactions_checked += 1;
    }
    Ok(effects)
}

/// One pass over the unspent projection, with profile aliases deduplicated.
/// PostgreSQL integration can provide the same totals with one grouped query.
pub fn aggregate_supply<'a>(
    profile: &Profile,
    height: u32,
    outputs: impl IntoIterator<Item = SupplyOutput<'a>>,
) -> Result<BTreeMap<String, i128>, CheckError> {
    let mut totals: BTreeMap<_, _> = fixed_units(profile)
        .into_iter()
        .map(|unit| (unit, 0i128))
        .collect();
    for row in outputs.into_iter().filter(|row| !row.spent) {
        for asset in &row.output.assets {
            let unit = asset.unit();
            if let Some(total) = totals.get_mut(&unit) {
                if u64::try_from(asset.quantity).is_err() {
                    return Err(CheckError::AssetQuantity {
                        height,
                        transaction: None,
                        unit,
                    });
                }
                *total = total
                    .checked_add(asset.quantity)
                    .ok_or(CheckError::AssetQuantity {
                        height,
                        transaction: None,
                        unit,
                    })?;
            }
        }
    }
    Ok(totals)
}

pub fn apply_mint_delta(
    profile: &Profile,
    height: u32,
    supply: &BTreeMap<String, i128>,
    delta: &BTreeMap<String, i128>,
) -> Result<BTreeMap<String, i128>, CheckError> {
    if supply.keys().cloned().collect::<BTreeSet<_>>() != fixed_units(profile)
        || delta.keys().any(|unit| !supply.contains_key(unit))
    {
        return Err(CheckError::SupplyUnits { height });
    }
    let mut updated = supply.clone();
    for (unit, quantity) in &mut updated {
        *quantity = quantity
            .checked_add(delta.get(unit).copied().unwrap_or_default())
            .filter(|quantity| *quantity >= 0)
            .ok_or_else(|| CheckError::AssetQuantity {
                height,
                transaction: None,
                unit: unit.clone(),
            })?;
    }
    Ok(updated)
}

pub fn check_supply(
    height: u32,
    recorded: &BTreeMap<String, i128>,
    actual: &BTreeMap<String, i128>,
) -> Result<(), CheckError> {
    if recorded.keys().ne(actual.keys()) {
        return Err(CheckError::SupplyUnits { height });
    }
    for (unit, expected) in recorded {
        let actual = actual[unit];
        if *expected < 0 || actual < 0 {
            return Err(CheckError::AssetQuantity {
                height,
                transaction: None,
                unit: unit.clone(),
            });
        }
        if *expected != actual {
            return Err(CheckError::SupplyMismatch {
                height,
                unit: unit.clone(),
                expected: *expected,
                actual,
            });
        }
    }
    Ok(())
}
