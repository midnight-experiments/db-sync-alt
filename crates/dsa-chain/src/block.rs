//! One pass over a final block. [`process_block`] checks the transaction
//! count, duplicate transactions and asset conservation, and builds the rows,
//! the supply changes and the held-output changes in the same pass, or
//! returns every supplement the block still needs. Body hashes were checked
//! when the transactions were decoded.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{
    assets::{AssetRule, fixed_units},
    cbor::{self, error},
    datum, ids, metadata,
    observation::{self, Candidate},
    profiles::Profile,
    transaction::{Asset, Outpoint, Output, Transaction, hash},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Header {
    pub height: u32,
    pub hash: String,
    pub previous_hash: Option<String>,
    pub transaction_count: usize,
}

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
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Rows {
    pub tx: Vec<TxRow>,
    pub tx_out: Vec<OutputRow>,
    pub ma_tx_out: Vec<AssetRow>,
    pub tx_in: Vec<InputRow>,
    pub datum: Vec<DatumRow>,
    pub tx_metadata: Vec<MetadataRow>,
}

/// A stored output that is not spent: the creating transaction's ID, its
/// address, its eligible tokens and its datum hash. This is what the database
/// holds for an output, so a held map can be loaded with one lookup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Held {
    pub transaction_id: i64,
    pub address: String,
    pub assets: Vec<Asset>,
    pub data_hash: Option<String>,
}

/// A datum the block needs, as far as the caller knows it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DatumAvailability {
    /// The provider's bytes for the hash.
    Present(Vec<u8>),
    /// The provider does not know the datum for this attempt.
    NotFound,
    /// The caller already holds a datum row under this hash, from the
    /// database or from an earlier block of the same update.
    Stored,
}

#[derive(Clone, Debug, Default)]
pub struct Supplements {
    /// Resolved outputs indexed as in the body; a phase-2-invalid
    /// transaction's collateral return is at the body's output count.
    pub outputs: BTreeMap<String, BTreeMap<u16, Output>>,
    pub datums: BTreeMap<String, DatumAvailability>,
    /// Transaction with an output at the ICS address -> its bridge metadatum
    /// as the provider renders it, or `None` when it has none.
    pub metadata: BTreeMap<String, Option<String>>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SupplementRequest {
    Output { transaction: String, index: u16 },
    Datum(String),
    Metadata(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockOutput {
    pub rows: Rows,
    /// Net mint per fixed-supply unit.
    pub mint_delta: BTreeMap<String, i128>,
    /// Relevant outputs this block creates and does not spend itself.
    pub created: Vec<(Outpoint, Held)>,
    /// Held outputs from before this block that it spends.
    pub spent: Vec<Outpoint>,
    pub observations: Vec<Candidate>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    Ready(Box<BlockOutput>),
    /// Every supplement the block needs that can be named before they are
    /// resolved. A resolved output can reveal further needs (its datum, or
    /// the ICS address), which the next attempt reports.
    Needs(Vec<SupplementRequest>),
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum BlockError {
    #[error("unsupported_position at block {height}")]
    Position { height: u32 },
    #[error("header_height at block {height}, expected {expected}")]
    HeaderHeight { height: u32, expected: u32 },
    #[error("header_parent at block {height}")]
    HeaderParent { height: u32 },
    #[error("transaction_count at block {height}, header {expected}, decoded {actual}")]
    TransactionCount {
        height: u32,
        expected: usize,
        actual: usize,
    },
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
    #[error("asset_quantity at block {height}, transaction {transaction}, unit {unit}")]
    AssetQuantity {
        height: u32,
        transaction: String,
        unit: String,
    },
    #[error("{source} at block {height}, transaction {transaction}")]
    Invalid {
        height: u32,
        transaction: String,
        source: cbor::Error,
    },
}

impl BlockError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Position { .. } => "unsupported_position",
            Self::HeaderHeight { .. } => "header_height",
            Self::HeaderParent { .. } => "header_parent",
            Self::TransactionCount { .. } => "transaction_count",
            Self::DuplicateTransaction { .. } => "duplicate_transaction",
            Self::AssetBalance { .. } => "asset_conservation",
            Self::AssetQuantity { .. } => "asset_quantity",
            Self::Invalid { source, .. } => source.code,
        }
    }
}

/// Check that `header` follows `previous` by height and hash.
pub fn check_header(previous: &Header, header: &Header) -> Result<(), BlockError> {
    let height = header.height;
    if ids::block_id(height).is_err() {
        return Err(BlockError::Position { height });
    }
    let expected = previous.height.saturating_add(1);
    if height != expected {
        return Err(BlockError::HeaderHeight { height, expected });
    }
    if header.previous_hash.as_ref() != Some(&previous.hash) {
        return Err(BlockError::HeaderParent { height });
    }
    Ok(())
}

/// What the profile selects: outputs at its addresses or holding an eligible
/// token, and the tokens kept on them.
pub struct Selection<'a> {
    profile: &'a Profile,
    addresses: BTreeSet<String>,
    assets: AssetRule,
    fixed: BTreeSet<String>,
    permissioned: String,
}

impl<'a> Selection<'a> {
    pub fn new(profile: &'a Profile) -> Self {
        Self {
            profile,
            addresses: profile
                .addresses
                .iter()
                .map(|(_, address)| address.to_owned())
                .collect(),
            assets: AssetRule::from_profile(profile),
            fixed: fixed_units(profile),
            permissioned: profile.assets.permissioned_candidates.unit(),
        }
    }
    pub fn profile(&self) -> &Profile {
        self.profile
    }
    pub fn required(&self, asset: &Asset) -> bool {
        self.assets.allows(&asset.policy, &asset.name)
    }
    pub fn relevant(&self, output: &Output) -> bool {
        output
            .address
            .as_ref()
            .is_some_and(|address| self.addresses.contains(address))
            || output.assets.iter().any(|asset| self.required(asset))
    }
    /// The node reads the datum of outputs at profile addresses and of
    /// outputs holding the permissioned-candidates token.
    fn needs_datum(&self, address: &str, assets: &[Asset]) -> bool {
        self.addresses.contains(address) || assets.iter().any(|a| a.unit() == self.permissioned)
    }
}

fn ordinal(n: usize) -> Result<u16, cbor::Error> {
    u16::try_from(n)
        .ok()
        .filter(|n| *n < ids::INDEX_LIMIT)
        .ok_or_else(|| error("invalid_ordinal", 0))
}

fn id(result: Result<i64, ids::IdError>) -> Result<i64, cbor::Error> {
    result.map_err(|_| error("invalid_positional_id", 0))
}

/// The outputs a transaction creates on chain, in body order. A valid
/// transaction's collateral return and an invalid transaction's ordinary
/// outputs have no effect. `None` marks an output that needs a supplement.
pub fn effective_outputs<'t>(
    tx: &'t Transaction<'_>,
    supplements: &'t Supplements,
) -> Result<Vec<(u16, Option<&'t Output>)>, cbor::Error> {
    let candidates: Vec<_> = if tx.is_valid() {
        tx.outputs()
            .iter()
            .map(Option::as_ref)
            .enumerate()
            .collect()
    } else if tx.has_collateral_return() {
        vec![(tx.outputs().len(), tx.collateral_return())]
    } else {
        Vec::new()
    };
    candidates
        .into_iter()
        .map(|(index, output)| {
            let index = ordinal(index)?;
            let output = output
                .filter(|output| output.address.is_some())
                .or_else(|| {
                    supplements
                        .outputs
                        .get(tx.hash())
                        .and_then(|outputs| outputs.get(&index))
                });
            Ok((index, output))
        })
        .collect()
}

/// The effective inputs of a block that are not created inside it, for the
/// one held-output lookup before [`process_block`].
pub fn inputs(transactions: &[Transaction<'_>]) -> Vec<Outpoint> {
    let created: BTreeSet<_> = transactions.iter().map(Transaction::hash).collect();
    transactions
        .iter()
        .flat_map(Transaction::effective_inputs)
        .filter(|point| !created.contains(point.hash.as_str()))
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// Outputs created or spent by the block, over the caller's held map. A
/// failed block leaves the caller's map as it was.
struct Overlay<'h> {
    held: &'h BTreeMap<Outpoint, Held>,
    changes: BTreeMap<Outpoint, Option<Held>>,
}

impl Overlay<'_> {
    fn get(&self, point: &Outpoint) -> Option<&Held> {
        match self.changes.get(point) {
            Some(change) => change.as_ref(),
            None => self.held.get(point),
        }
    }

    fn apply(&mut self, tx: &Transaction<'_>, created: Vec<(u16, Held)>) {
        for point in tx.effective_inputs() {
            if self.get(point).is_some() {
                self.changes.insert(point.clone(), None);
            }
        }
        for (index, held) in created {
            let point = Outpoint {
                hash: tx.hash().to_owned(),
                index,
            };
            self.changes.insert(point, Some(held));
        }
    }

    fn finish(self) -> (Vec<(Outpoint, Held)>, Vec<Outpoint>) {
        let mut created = Vec::new();
        let mut spent = Vec::new();
        for (point, change) in self.changes {
            match change {
                Some(held) => created.push((point, held)),
                None if self.held.contains_key(&point) => spent.push(point),
                None => {}
            }
        }
        (created, spent)
    }
}

fn add(
    totals: &mut BTreeMap<String, i128>,
    assets: &[Asset],
    selection: &Selection<'_>,
    mint: bool,
) -> Result<(), String> {
    for asset in assets.iter().filter(|asset| selection.required(asset)) {
        let in_range = if mint {
            i64::try_from(asset.quantity).is_ok()
        } else {
            u64::try_from(asset.quantity).is_ok()
        };
        let total = totals.entry(asset.unit()).or_default();
        *total = total
            .checked_add(asset.quantity)
            .filter(|_| in_range)
            .ok_or_else(|| asset.unit())?;
    }
    Ok(())
}

/// Process one final block.
///
/// `held` holds the stored, unspent outputs the block's [`inputs`] name,
/// plus those created by earlier blocks of the same update. The function
/// never changes it; after `Ready`, the caller applies `created` and `spent`
/// so that the next block of the same update sees them. A datum marked
/// `Stored` in `supplements` is not requested again.
pub fn process_block(
    selection: &Selection<'_>,
    header: &Header,
    transactions: &[Transaction<'_>],
    held: &BTreeMap<Outpoint, Held>,
    supplements: &Supplements,
) -> Result<Outcome, BlockError> {
    let height = header.height;
    if ids::block_id(height).is_err() || transactions.len() > usize::from(ids::INDEX_LIMIT) {
        return Err(BlockError::Position { height });
    }
    if transactions.len() != header.transaction_count {
        return Err(BlockError::TransactionCount {
            height,
            expected: header.transaction_count,
            actual: transactions.len(),
        });
    }
    let mut block = Pass {
        selection,
        supplements,
        height,
        overlay: Overlay {
            held,
            changes: BTreeMap::new(),
        },
        rows: Rows::default(),
        datums: BTreeMap::new(),
        mint_delta: BTreeMap::new(),
        needs: BTreeSet::new(),
        outputs_unknown: false,
    };
    let mut seen = BTreeSet::new();
    for (position, tx) in transactions.iter().enumerate() {
        if !seen.insert(tx.hash()) {
            return Err(BlockError::DuplicateTransaction {
                height,
                transaction: tx.hash().to_owned(),
            });
        }
        block
            .transaction(position, tx)
            .map_err(|source| match source {
                Failure::Block(error) => error,
                Failure::Invalid(source) => BlockError::Invalid {
                    height,
                    transaction: tx.hash().to_owned(),
                    source,
                },
            })?;
    }
    if !block.needs.is_empty() {
        return Ok(Outcome::Needs(block.needs.into_iter().collect()));
    }
    let mut rows = block.rows;
    rows.datum = block.datums.into_values().collect();
    let (created, spent) = block.overlay.finish();
    let observations = observation::candidates(transactions, supplements, &rows);
    Ok(Outcome::Ready(Box::new(BlockOutput {
        rows,
        mint_delta: block.mint_delta,
        created,
        spent,
        observations,
    })))
}

enum Failure {
    Block(BlockError),
    Invalid(cbor::Error),
}

impl From<cbor::Error> for Failure {
    fn from(error: cbor::Error) -> Self {
        Self::Invalid(error)
    }
}

struct Pass<'s, 'h> {
    selection: &'s Selection<'s>,
    supplements: &'s Supplements,
    height: u32,
    overlay: Overlay<'h>,
    rows: Rows,
    datums: BTreeMap<String, DatumRow>,
    mint_delta: BTreeMap<String, i128>,
    needs: BTreeSet<SupplementRequest>,
    /// An output of an earlier transaction is unresolved, so held outputs
    /// after it are incomplete and conservation waits for the next attempt.
    outputs_unknown: bool,
}

impl Pass<'_, '_> {
    fn transaction(&mut self, position: usize, tx: &Transaction<'_>) -> Result<(), Failure> {
        let selection = self.selection;
        let tx_index = ordinal(position)?;
        let tx_id = id(ids::transaction_id(self.height, tx_index))?;
        let mut known = Vec::new();
        for (index, output) in effective_outputs(tx, self.supplements)? {
            match output {
                Some(output) => known.push((index, output)),
                None => {
                    self.outputs_unknown = true;
                    self.needs.insert(SupplementRequest::Output {
                        transaction: tx.hash().to_owned(),
                        index,
                    });
                }
            }
        }
        if !self.outputs_unknown {
            self.conserve(tx, &known)?;
        }
        let kept: Vec<_> = known
            .iter()
            .filter(|(_, output)| selection.relevant(output))
            .copied()
            .collect();
        let spends: Vec<_> = tx
            .effective_inputs()
            .iter()
            .enumerate()
            .filter_map(|(n, point)| self.overlay.get(point).map(|origin| (n, point, origin)))
            .collect();
        let minted = tx.is_valid() && tx.mint().iter().any(|asset| selection.required(asset));
        let mut created = Vec::new();
        if !(kept.is_empty() && spends.is_empty() && !minted) {
            self.rows.tx.push(TxRow {
                id: tx_id,
                hash: tx.hash().to_owned(),
                block_id: i64::from(self.height),
                block_index: tx_index,
                valid_contract: tx.is_valid(),
            });
        }
        let mut origins = Vec::new();
        for (n, point, origin) in spends {
            self.rows.tx_in.push(InputRow {
                id: id(ids::input_id(self.height, tx_index, ordinal(n)?))?,
                tx_in_id: tx_id,
                tx_out_id: origin.transaction_id,
                tx_out_index: point.index,
            });
            if selection.needs_datum(&origin.address, &origin.assets) {
                origins.push(origin.data_hash.clone());
            }
        }
        for data_hash in origins.into_iter().flatten() {
            let local = tx.datums().get(&data_hash).copied();
            self.datum(&data_hash, local)?;
        }
        let mut ics = false;
        for (index, output) in kept {
            let Some(address) = output.address.clone() else {
                self.needs.insert(SupplementRequest::Output {
                    transaction: tx.hash().to_owned(),
                    index,
                });
                continue;
            };
            ics |= address == selection.profile.addresses.ics;
            let output_id = id(ids::output_id(self.height, tx_index, index))?;
            self.rows.tx_out.push(OutputRow {
                id: output_id,
                tx_id,
                index,
                address: address.clone(),
                value: output.coin.to_string(),
                data_hash: output.data_hash.clone(),
                consumed_by_tx_id: None,
            });
            let mut eligible = Vec::new();
            for (n, asset) in output.assets.iter().enumerate() {
                if selection.required(asset) {
                    self.rows.ma_tx_out.push(AssetRow {
                        id: id(ids::asset_id(self.height, tx_index, index, ordinal(n)?))?,
                        quantity: asset.quantity.to_string(),
                        tx_out_id: output_id,
                        policy: asset.policy.clone(),
                        name: asset.name.clone(),
                    });
                    eligible.push(asset.clone());
                }
            }
            if selection.needs_datum(&address, &output.assets)
                && let Some(data_hash) = &output.data_hash
            {
                let local = output
                    .inline_datum
                    .as_deref()
                    .or_else(|| tx.datums().get(data_hash).copied());
                self.datum(data_hash, local)?;
            }
            created.push((
                index,
                Held {
                    transaction_id: tx_id,
                    address,
                    assets: eligible,
                    data_hash: output.data_hash.clone(),
                },
            ));
        }
        if ics && tx.is_valid() {
            self.bridge(tx, tx_id)?;
        }
        self.overlay.apply(tx, created);
        Ok(())
    }

    /// For every eligible unit, held inputs plus mint equal outputs.
    fn conserve(
        &mut self,
        tx: &Transaction<'_>,
        outputs: &[(u16, &Output)],
    ) -> Result<(), Failure> {
        let selection = self.selection;
        let quantity = |unit| {
            Failure::Block(BlockError::AssetQuantity {
                height: self.height,
                transaction: tx.hash().to_owned(),
                unit,
            })
        };
        let mut inputs = BTreeMap::new();
        for point in tx.effective_inputs() {
            if let Some(origin) = self.overlay.get(point) {
                add(&mut inputs, &origin.assets, selection, false).map_err(quantity)?;
            }
        }
        let mut created = BTreeMap::new();
        for (_, output) in outputs {
            add(&mut created, &output.assets, selection, false).map_err(quantity)?;
        }
        let mut mint = BTreeMap::new();
        if tx.is_valid() {
            add(&mut mint, tx.mint(), selection, true).map_err(quantity)?;
        }
        let units: BTreeSet<_> = inputs
            .keys()
            .chain(created.keys())
            .chain(mint.keys())
            .collect();
        for unit in units {
            let held = inputs.get(unit).copied().unwrap_or_default();
            let minted = mint.get(unit).copied().unwrap_or_default();
            let output = created.get(unit).copied().unwrap_or_default();
            if held.checked_add(minted) != Some(output) {
                return Err(Failure::Block(BlockError::AssetBalance {
                    height: self.height,
                    transaction: tx.hash().to_owned(),
                    unit: unit.clone(),
                    inputs: held,
                    mint: minted,
                    outputs: output,
                }));
            }
        }
        for (unit, amount) in mint {
            if selection.fixed.contains(&unit) {
                let total = self.mint_delta.entry(unit.clone()).or_default();
                *total = total.checked_add(amount).ok_or_else(|| quantity(unit))?;
            }
        }
        Ok(())
    }

    fn datum(&mut self, data_hash: &str, local: Option<&[u8]>) -> Result<(), cbor::Error> {
        if self.datums.contains_key(data_hash) {
            return Ok(());
        }
        let raw = match (local, self.supplements.datums.get(data_hash)) {
            (Some(raw), _) => raw,
            (None, Some(DatumAvailability::Present(raw))) => raw.as_slice(),
            (None, Some(DatumAvailability::NotFound | DatumAvailability::Stored)) => return Ok(()),
            (None, None) => {
                self.needs
                    .insert(SupplementRequest::Datum(data_hash.to_owned()));
                return Ok(());
            }
        };
        if hash(raw) != data_hash {
            return Err(error("provider_datum_hash_mismatch", 0));
        }
        self.datums.insert(
            data_hash.to_owned(),
            DatumRow {
                hash: data_hash.to_owned(),
                value: datum::render(raw)?,
            },
        );
        Ok(())
    }

    fn bridge(&mut self, tx: &Transaction<'_>, tx_id: i64) -> Result<(), cbor::Error> {
        let Some(provided) = self.supplements.metadata.get(tx.hash()) else {
            self.needs
                .insert(SupplementRequest::Metadata(tx.hash().to_owned()));
            return Ok(());
        };
        let json = metadata::checked(tx, provided.as_deref())?;
        if provided.is_some() {
            self.rows.tx_metadata.push(MetadataRow {
                id: tx_id,
                tx_id,
                key: metadata::LABEL,
                json,
            });
        }
        Ok(())
    }
}
