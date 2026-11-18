//! Pure row construction. I/O callers supply held origins and any explicitly
//! requested output, datum or metadata supplements before publication.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{
    assets::AssetRule,
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HeldOutput {
    pub transaction_id: i64,
    pub output: Output,
}

/// Facts known when a required datum is requested. A 404 applies to this block
/// attempt only. Callers must retry lookup on a later selected use of the hash.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DatumAvailability {
    Present(Vec<u8>),
    NotFound,
    /// The caller already holds a verified datum row under this hash.
    Stored,
}

#[derive(Default)]
pub struct Supplements {
    /// Resolved outputs are indexed exactly as in the CBOR body; invalid
    /// transactions have only the collateral return at body.outputs.len().
    pub outputs: BTreeMap<String, BTreeMap<u16, Output>>,
    pub datums: BTreeMap<String, DatumAvailability>,
    /// ICS transaction -> label 6500973 absent, or its exact provider JSON.
    pub metadata: BTreeMap<String, Option<String>>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum SupplementRequest {
    Output { transaction: String, index: u16 },
    Datum(String),
    Metadata(String),
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum BuildError {
    #[error(transparent)]
    Invalid(#[from] Error),
    #[error("{0:?} requires a provider supplement")]
    Required(SupplementRequest),
    #[error(transparent)]
    Captured(#[from] crate::inventory::IntegrityError),
}

impl BuildError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Captured(_) => "decoded_fact_integrity",
            Self::Invalid(error) => error.code,
            Self::Required(SupplementRequest::Output { .. }) => {
                "provider_output_resolution_required"
            }
            Self::Required(SupplementRequest::Datum(_)) => "provider_datum_resolution_required",
            Self::Required(SupplementRequest::Metadata(_)) => {
                "provider_metadata_resolution_required"
            }
        }
    }
}

/// An overlay contains only outputs created or removed by this block. Failed
/// validation drops it without changing the caller's long-lived held set.
pub(crate) struct HeldChanges<'a> {
    original: &'a BTreeMap<Outpoint, HeldOutput>,
    changes: BTreeMap<Outpoint, Option<HeldOutput>>,
}

impl<'a> HeldChanges<'a> {
    pub(crate) fn new(original: &'a BTreeMap<Outpoint, HeldOutput>) -> Self {
        Self {
            original,
            changes: BTreeMap::new(),
        }
    }

    pub(crate) fn get(&self, point: &Outpoint) -> Option<&HeldOutput> {
        match self.changes.get(point) {
            Some(value) => value.as_ref(),
            None => self.original.get(point),
        }
    }
    fn contains_key(&self, point: &Outpoint) -> bool {
        self.get(point).is_some()
    }
    pub(crate) fn remove(&mut self, point: &Outpoint) {
        if self.contains_key(point) {
            self.changes.insert(point.clone(), None);
        }
    }
    pub(crate) fn insert(&mut self, point: Outpoint, output: HeldOutput) {
        self.changes.insert(point, Some(output));
    }

    /// One transition shared by validation and row construction. The overlay
    /// stays private to the whole block until every transaction succeeds.
    pub(crate) fn apply_transaction(
        &mut self,
        transaction_id: i64,
        tx: &Transaction<'_>,
        outputs: &[(u16, &Output)],
        selection: &Selection<'_>,
    ) {
        for point in tx.effective_inputs() {
            self.remove(point);
        }
        for (index, output) in outputs
            .iter()
            .filter(|(_, output)| selection.relevant(output))
        {
            self.insert(
                Outpoint {
                    hash: tx.hash.clone(),
                    index: *index,
                },
                HeldOutput {
                    transaction_id,
                    output: (*output).clone(),
                },
            );
        }
    }
}

pub struct Selection<'a> {
    profile: &'a Profile,
    addresses: BTreeSet<String>,
    assets: AssetRule,
}

impl<'a> Selection<'a> {
    pub fn new(profile: &'a Profile) -> Self {
        Self {
            profile,
            addresses: profile.addresses.values().cloned().collect(),
            assets: AssetRule::from_profile(profile),
        }
    }
    pub fn required(&self, asset: &Asset) -> bool {
        self.assets.allows(&asset.policy, &asset.name)
    }
    pub fn relevant(&self, output: &Output) -> bool {
        output
            .address
            .as_ref()
            .is_some_and(|a| self.addresses.contains(a))
            || output.assets.iter().any(|a| self.required(a))
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
) -> Result<(), BuildError> {
    let Some(expected) = &output.data_hash else {
        return Ok(());
    };
    if datums.contains_key(expected) {
        return Ok(());
    }
    let local = output
        .inline_datum
        .as_ref()
        .or_else(|| tx.datums.get(expected));
    let raw = match (local, supplements.datums.get(expected)) {
        (Some(raw), _) | (None, Some(DatumAvailability::Present(raw))) => raw,
        (None, Some(DatumAvailability::NotFound | DatumAvailability::Stored)) => return Ok(()),
        (None, None) => {
            return Err(BuildError::Required(SupplementRequest::Datum(
                expected.clone(),
            )));
        }
    };
    if hash(raw) != *expected {
        return Err(error("provider_datum_hash_mismatch", 0).into());
    }
    datums.insert(
        expected.clone(),
        DatumRow {
            hash: expected.clone(),
            value: datum::render(raw)?,
        },
    );
    Ok(())
}

/// Resolve the outputs created on chain. A valid transaction's collateral
/// return and an invalid transaction's ordinary outputs have no chain effect.
pub fn effective_outputs<'a>(
    tx: &'a Transaction<'_>,
    supplements: &'a Supplements,
) -> Result<Vec<(u16, &'a Output)>, BuildError> {
    let candidates: Vec<_> = if tx.valid {
        tx.outputs
            .iter()
            .enumerate()
            .map(|(index, output)| (index, output.as_ref()))
            .collect()
    } else if tx.has_collateral_return {
        vec![(tx.outputs.len(), tx.collateral_output.as_ref())]
    } else {
        Vec::new()
    };
    candidates
        .into_iter()
        .map(|(index, output)| {
            let index = ordinal(index)?;
            let output = output
                .filter(|output| output.classification_safe)
                .or_else(|| {
                    supplements
                        .outputs
                        .get(&tx.hash)
                        .and_then(|outputs| outputs.get(&index))
                })
                .ok_or_else(|| {
                    BuildError::Required(SupplementRequest::Output {
                        transaction: tx.hash.clone(),
                        index,
                    })
                })?;
            output.check_inline_hash()?;
            Ok((index, output))
        })
        .collect()
}

/// Build one block in inventory order. Held origins include all relevant
/// outputs spent by this range, plus creations in earlier blocks. The caller
/// can do one batched database lookup for these outpoints before this function.
/// Build rows from the same immutable decoded capture used by the checks.
pub fn build_block(
    profile: &Profile,
    height: u32,
    transactions: &crate::inventory::DecodedTransactions<'_>,
    held: &mut BTreeMap<Outpoint, HeldOutput>,
    supplements: &Supplements,
) -> Result<Rows, BuildError> {
    transactions.validate()?;
    build_facts(
        profile,
        height,
        transactions.transactions(),
        held,
        supplements,
    )
}

pub(crate) fn build_facts(
    profile: &Profile,
    height: u32,
    transactions: &[Transaction<'_>],
    held: &mut BTreeMap<Outpoint, HeldOutput>,
    supplements: &Supplements,
) -> Result<Rows, BuildError> {
    let (rows, changes) = {
        let mut staged = HeldChanges::new(held);
        let rows = build_rows(profile, height, transactions, &mut staged, supplements)?;
        (rows, staged.changes)
    };
    for (point, output) in changes {
        if let Some(output) = output {
            held.insert(point, output);
        } else {
            held.remove(&point);
        }
    }
    Ok(rows)
}

fn build_rows(
    profile: &Profile,
    height: u32,
    transactions: &[Transaction<'_>],
    held: &mut HeldChanges<'_>,
    supplements: &Supplements,
) -> Result<Rows, BuildError> {
    let selection = Selection::new(profile);
    let mut rows = Rows::default();
    let mut datums = BTreeMap::new();
    let mut seen = BTreeSet::new();
    for (position, tx) in transactions.iter().enumerate() {
        if !seen.insert(&tx.hash) {
            return Err(error("provider_duplicate_transaction", 0).into());
        }
        let tx_index = ordinal(position)?;
        let tx_id = id(ids::transaction_id(height, tx_index))?;
        let inputs = tx.effective_inputs();
        let effective = effective_outputs(tx, supplements)?;
        let kept_outputs: Vec<_> = effective
            .iter()
            .copied()
            .filter(|(_, output)| selection.relevant(output))
            .collect();
        let mints: Vec<_> = tx
            .mint
            .iter()
            .filter(|a| tx.valid && selection.required(a))
            .collect();
        let spends: Vec<_> = inputs
            .iter()
            .enumerate()
            .filter(|(_, p)| held.contains_key(p))
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
        let mut ics = false;
        for (index, output) in kept_outputs {
            let output_id = id(ids::output_id(height, tx_index, index))?;
            let address = output.address.clone().ok_or_else(|| {
                BuildError::Required(SupplementRequest::Output {
                    transaction: tx.hash.clone(),
                    index,
                })
            })?;
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
                if selection.required(asset) {
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
            let metadata = supplements.metadata.get(&tx.hash).ok_or_else(|| {
                BuildError::Required(SupplementRequest::Metadata(tx.hash.clone()))
            })?;
            if let Some(json) = metadata {
                rows.tx_metadata.push(MetadataRow {
                    id: tx_id,
                    tx_id,
                    key: 6500973,
                    json: crate::metadata::checked(tx, Some(json))?,
                });
            } else {
                crate::metadata::checked(tx, None)?;
            }
        }
        held.apply_transaction(tx_id, tx, &effective, &selection);
    }
    rows.datum = datums.into_values().collect();
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datum_selection_depends_on_reader_addresses_and_full_permissioned_unit() {
        let profile = Profile::load("mainnet").unwrap();
        let governance = Asset {
            policy: profile.assets["council"].policy.clone(),
            name: "ff".to_owned(),
            quantity: 1,
        };
        let permissioned = &profile.assets["permissioned_candidates"];
        let cn = &profile.assets["cnight"];
        let cases = [
            ("ordinary".to_owned(), governance.clone(), false),
            (profile.addresses["council"].clone(), governance, true),
            (
                "ordinary".to_owned(),
                Asset {
                    policy: permissioned.policy.clone(),
                    name: permissioned.name.clone(),
                    quantity: 1,
                },
                true,
            ),
            (
                "ordinary".to_owned(),
                Asset {
                    policy: cn.policy.clone(),
                    name: cn.name.clone(),
                    quantity: 1,
                },
                false,
            ),
        ];
        let datum_hash = hash(&[0]);
        for (index, (address, asset, required)) in cases.into_iter().enumerate() {
            let tx = Transaction {
                hash: format!("{index:02x}").repeat(32),
                valid: true,
                ordinary: Vec::new(),
                collateral: Vec::new(),
                reference: Vec::new(),
                outputs: vec![Some(Output {
                    address_raw: Vec::new(),
                    address: Some(address),
                    coin: 1,
                    assets: vec![asset],
                    data_hash: Some(datum_hash.clone()),
                    inline_datum: None,
                    classification_safe: true,
                })],
                collateral_output: None,
                has_collateral_return: false,
                mint: Vec::new(),
                datums: BTreeMap::new(),
                body: &[],
                auxiliary: None,
                auxiliary_hash: None,
            };
            let mut held = BTreeMap::new();
            let mut supplements = Supplements::default();
            if required {
                assert_eq!(
                    build_facts(
                        &profile,
                        10,
                        std::slice::from_ref(&tx),
                        &mut held,
                        &supplements
                    )
                    .unwrap_err(),
                    BuildError::Required(SupplementRequest::Datum(datum_hash.clone()))
                );
                supplements
                    .datums
                    .insert(datum_hash.clone(), DatumAvailability::Present(vec![0]));
            }
            let rows = build_facts(&profile, 10, &[tx], &mut held, &supplements).unwrap();
            assert_eq!(rows.tx_out[0].data_hash.as_ref(), Some(&datum_hash));
            assert_eq!(rows.ma_tx_out.len(), 1);
            assert_eq!(
                rows.datum.len(),
                usize::from(required),
                "an eligible token alone does not request an otherwise unread datum"
            );
        }
    }

    #[test]
    fn version_two_filters_every_address_and_preserves_full_asset_ordinals() {
        // Native selection model only, with no serialized ledger transaction.
        let mut profile = Profile::load("mainnet").unwrap();
        for (role, policy) in [
            ("cnight", "40"),
            ("auth", "80"),
            ("permissioned_candidates", "c0"),
            ("bridge", "e0"),
            ("council", "60"),
            ("technical_committee", "a0"),
        ] {
            profile.assets.get_mut(role).unwrap().policy = policy.repeat(28);
        }
        let required = |role: &str| Asset {
            policy: profile.assets[role].policy.clone(),
            name: profile.assets[role].name.clone(),
            quantity: 1,
        };
        let foreign = |policy: &str| Asset {
            policy: policy.repeat(28),
            name: String::new(),
            quantity: 1,
        };
        let assets = vec![
            foreign("20"),
            required("cnight"),
            foreign("50"),
            Asset {
                policy: "60".repeat(28),
                name: "ff".to_owned(),
                quantity: 1,
            },
            required("auth"),
            foreign("90"),
            required("permissioned_candidates"),
            required("bridge"),
            foreign("f0"),
        ];
        let addresses = [
            profile.addresses["ics"].clone(),
            profile.addresses["reserve"].clone(),
            profile.addresses["mapping"].clone(),
            "unrelated".to_owned(),
            profile.addresses["ics"].clone(),
            "unrelated".to_owned(),
        ];
        let transactions: Vec<_> = addresses
            .into_iter()
            .enumerate()
            .map(|(index, address)| Transaction {
                hash: format!("{index:02x}").repeat(32),
                valid: true,
                ordinary: Vec::new(),
                collateral: Vec::new(),
                reference: Vec::new(),
                outputs: vec![Some(Output {
                    address_raw: Vec::new(),
                    address: Some(address),
                    coin: 100,
                    assets: if index < 4 {
                        assets.clone()
                    } else {
                        vec![foreign("20")]
                    },
                    data_hash: None,
                    inline_datum: None,
                    classification_safe: true,
                })],
                collateral_output: None,
                has_collateral_return: false,
                mint: Vec::new(),
                datums: BTreeMap::new(),
                body: &[],
                auxiliary: None,
                auxiliary_hash: None,
            })
            .collect();
        let mut supplements = Supplements::default();
        for tx in &transactions {
            supplements.metadata.insert(tx.hash.clone(), None);
        }
        let rows = build_facts(
            &profile,
            10,
            &transactions,
            &mut BTreeMap::new(),
            &supplements,
        )
        .unwrap();
        assert_eq!(
            rows.tx_out.len(),
            5,
            "address-relevant outputs survive with zero retained token rows"
        );
        assert_eq!(rows.ma_tx_out.len(), 20);
        for (transaction, chunk) in rows.ma_tx_out.as_chunks::<5>().0.iter().enumerate() {
            for (row, ordinal) in chunk.iter().zip([1, 3, 4, 6, 7]) {
                assert_eq!(
                    row.id,
                    (10i64 << 36) | ((transaction as i64) << 24) | ordinal,
                    "asset IDs retain the ordinal in the complete sorted pre-filter list"
                );
            }
        }
        assert!(
            !rows
                .ma_tx_out
                .iter()
                .any(|row| row.policy == "20".repeat(28)
                    || row.policy == "50".repeat(28)
                    || row.policy == "90".repeat(28)
                    || row.policy == "f0".repeat(28))
        );
    }

    #[test]
    fn requested_datums_distinguish_unknown_404_stored_and_later_reveal() {
        let profile = Profile::load("mainnet").unwrap();
        let raw = vec![0];
        let datum_hash = hash(&raw);
        let output = Output {
            address_raw: Vec::new(),
            address: Some(profile.addresses["mapping"].clone()),
            coin: 100,
            assets: Vec::new(),
            data_hash: Some(datum_hash.clone()),
            inline_datum: None,
            classification_safe: true,
        };
        let make_tx =
            |hash: String, inputs: Vec<Outpoint>, outputs: Vec<Option<Output>>| Transaction {
                hash,
                valid: true,
                ordinary: inputs,
                collateral: Vec::new(),
                reference: Vec::new(),
                outputs,
                collateral_output: None,
                has_collateral_return: false,
                mint: Vec::new(),
                datums: BTreeMap::new(),
                body: &[],
                auxiliary: None,
                auxiliary_hash: None,
            };
        let creation = make_tx("01".repeat(32), Vec::new(), vec![Some(output.clone())]);
        let mut held = BTreeMap::new();
        let request = build_facts(
            &profile,
            10,
            std::slice::from_ref(&creation),
            &mut held,
            &Supplements::default(),
        )
        .unwrap_err();
        assert_eq!(
            request,
            BuildError::Required(SupplementRequest::Datum(datum_hash.clone()))
        );
        assert!(held.is_empty());
        let mut missing = Supplements::default();
        missing
            .datums
            .insert(datum_hash.clone(), DatumAvailability::NotFound);
        let created = build_facts(&profile, 10, &[creation], &mut held, &missing).unwrap();
        assert!(created.datum.is_empty());
        assert_eq!(created.tx_out[0].data_hash.as_ref(), Some(&datum_hash));
        let point = Outpoint {
            hash: "01".repeat(32),
            index: 0,
        };
        let mut spending = make_tx("02".repeat(32), vec![point.clone()], Vec::new());
        assert_eq!(
            build_facts(
                &profile,
                11,
                std::slice::from_ref(&spending),
                &mut held,
                &Supplements::default()
            )
            .unwrap_err(),
            BuildError::Required(SupplementRequest::Datum(datum_hash.clone())),
            "an earlier 404 must not suppress the later lookup"
        );
        spending.datums.insert(datum_hash.clone(), raw.clone());
        let rows = build_facts(
            &profile,
            11,
            &[spending],
            &mut held,
            &Supplements::default(),
        )
        .unwrap();
        assert_eq!(
            rows.datum,
            vec![DatumRow {
                hash: datum_hash.clone(),
                value: datum::render(&raw).unwrap()
            }]
        );
        assert!(!held.contains_key(&point));
        for available in [
            DatumAvailability::Present(raw.clone()),
            DatumAvailability::Stored,
        ] {
            let mut supplements = Supplements::default();
            supplements
                .datums
                .insert(datum_hash.clone(), available.clone());
            let creation = make_tx("03".repeat(32), Vec::new(), vec![Some(output.clone())]);
            let result = build_facts(
                &profile,
                12,
                &[creation],
                &mut BTreeMap::new(),
                &supplements,
            )
            .unwrap();
            assert_eq!(
                result.datum.len(),
                usize::from(matches!(available, DatumAvailability::Present(_)))
            );
        }
        let mut inline = output.clone();
        inline.inline_datum = Some(raw);
        let creation = make_tx("04".repeat(32), Vec::new(), vec![Some(inline)]);
        assert_eq!(
            build_facts(
                &profile,
                13,
                &[creation],
                &mut BTreeMap::new(),
                &Supplements::default()
            )
            .unwrap()
            .datum
            .len(),
            1
        );
        let mut irrelevant = output;
        irrelevant.address = Some("unrelated".to_owned());
        let ignored = make_tx("05".repeat(32), Vec::new(), vec![Some(irrelevant)]);
        assert!(
            build_facts(
                &profile,
                14,
                &[ignored],
                &mut BTreeMap::new(),
                &Supplements::default()
            )
            .unwrap()
            .tx
            .is_empty()
        );
    }

    #[test]
    fn input_ordinals_include_unheld_inputs_in_legacy_sorted_order() {
        let profile = Profile::load("mainnet").unwrap();
        let output = Output {
            address_raw: Vec::new(),
            address: Some(profile.addresses["mapping"].clone()),
            coin: 1,
            assets: Vec::new(),
            data_hash: None,
            inline_datum: None,
            classification_safe: true,
        };
        let inputs: Vec<_> = ["01", "02", "03"]
            .into_iter()
            .map(|h| Outpoint {
                hash: h.repeat(32),
                index: 0,
            })
            .collect();
        let mut held = BTreeMap::from([(
            inputs[1].clone(),
            HeldOutput {
                transaction_id: 17,
                output,
            },
        )]);
        let tx = Transaction {
            hash: "04".repeat(32),
            valid: true,
            ordinary: inputs,
            collateral: Vec::new(),
            reference: Vec::new(),
            outputs: Vec::new(),
            collateral_output: None,
            has_collateral_return: false,
            mint: Vec::new(),
            datums: BTreeMap::new(),
            body: &[],
            auxiliary: None,
            auxiliary_hash: None,
        };
        let result = build_facts(&profile, 10, &[tx], &mut held, &Supplements::default()).unwrap();
        assert_eq!(
            result.tx_in[0].id, 167772161,
            "legacy height*2^24 + transaction*2^12 + rank among all inputs"
        );
    }

    #[test]
    fn failed_later_transaction_preserves_held_and_retry_matches_clean_block() {
        // Native row-model facts only; no ledger serialization or external decoder.
        fn tx(
            hash: String,
            inputs: Vec<Outpoint>,
            outputs: Vec<Option<Output>>,
        ) -> Transaction<'static> {
            Transaction {
                hash,
                valid: true,
                ordinary: inputs,
                collateral: Vec::new(),
                reference: Vec::new(),
                outputs,
                collateral_output: None,
                has_collateral_return: false,
                mint: Vec::new(),
                datums: BTreeMap::new(),
                body: &[],
                auxiliary: None,
                auxiliary_hash: None,
            }
        }
        let profile = Profile::load("mainnet").unwrap();
        let output = Output {
            address_raw: Vec::new(),
            address: Some(profile.addresses["mapping"].clone()),
            coin: 100,
            assets: Vec::new(),
            data_hash: None,
            inline_datum: None,
            classification_safe: true,
        };
        for failure in ["output", "datum", "metadata"] {
            let origin = Outpoint {
                hash: "01".repeat(32),
                index: 0,
            };
            let first = Outpoint {
                hash: "02".repeat(32),
                index: 0,
            };
            let second = Outpoint {
                hash: "03".repeat(32),
                index: 0,
            };
            let untouched = Outpoint {
                hash: "04".repeat(32),
                index: 7,
            };
            let before = BTreeMap::from([
                (
                    origin.clone(),
                    HeldOutput {
                        transaction_id: 1,
                        output: output.clone(),
                    },
                ),
                (
                    untouched.clone(),
                    HeldOutput {
                        transaction_id: 2,
                        output: output.clone(),
                    },
                ),
            ]);
            let mut later = output.clone();
            let mut bad = Supplements::default();
            if failure == "datum" {
                later.data_hash = Some(hash(&[0]));
                bad.datums
                    .insert(hash(&[0]), DatumAvailability::Present(vec![1]));
            }
            if failure == "metadata" {
                later.address = Some(profile.addresses["ics"].clone());
            }
            let transactions = [
                tx(
                    first.hash.clone(),
                    vec![origin.clone()],
                    vec![Some(output.clone())],
                ),
                tx(
                    second.hash.clone(),
                    vec![first.clone()],
                    vec![if failure == "output" {
                        None
                    } else {
                        Some(later.clone())
                    }],
                ),
            ];
            let mut held = before.clone();
            let error = build_facts(&profile, 10, &transactions, &mut held, &bad).unwrap_err();
            assert_eq!(
                error.code(),
                match failure {
                    "output" => "provider_output_resolution_required",
                    "datum" => "provider_datum_hash_mismatch",
                    _ => "provider_metadata_resolution_required",
                }
            );
            assert_eq!(
                held, before,
                "failure must leave the entire original held set unchanged"
            );
            if failure == "output" {
                bad.outputs
                    .insert(second.hash.clone(), BTreeMap::from([(0, later)]));
            }
            if failure == "datum" {
                bad.datums
                    .insert(hash(&[0]), DatumAvailability::Present(vec![0]));
            }
            if failure == "metadata" {
                bad.metadata.insert(second.hash.clone(), None);
            }
            let mut clean = before.clone();
            let expected = build_facts(&profile, 10, &transactions, &mut clean, &bad).unwrap();
            let retried = build_facts(&profile, 10, &transactions, &mut held, &bad).unwrap();
            assert_eq!(retried, expected);
            assert_eq!(held, clean);
            assert_eq!(
                retried.tx_in.len(),
                2,
                "same-block created output must be spent through the staged view"
            );
            assert!(!held.contains_key(&origin));
            assert!(!held.contains_key(&first));
            assert!(held.contains_key(&second));
            assert_eq!(held.get(&untouched), before.get(&untouched));
        }
    }

    #[test]
    fn ra_14_c_3_invalid_projection_uses_collateral_return_and_collateral_only() {
        // Typed row-projection model only. This is never serialized as a
        // ledger transaction or passed to a provider or third-party decoder.
        let profile = Profile::load("mainnet").unwrap();
        let output = Output {
            address_raw: Vec::new(),
            address: Some(profile.addresses["mapping"].clone()),
            coin: 100,
            assets: vec![Asset {
                policy: profile.assets["cnight"].policy.clone(),
                name: profile.assets["cnight"].name.clone(),
                quantity: 1,
            }],
            data_hash: None,
            inline_datum: None,
            classification_safe: true,
        };
        let ordinary = Outpoint {
            hash: "01".repeat(32),
            index: 1,
        };
        let collateral = Outpoint {
            hash: "02".repeat(32),
            index: 2,
        };
        let tx = Transaction {
            hash: "03".repeat(32),
            valid: false,
            ordinary: vec![ordinary.clone()],
            collateral: vec![collateral.clone()],
            reference: Vec::new(),
            outputs: vec![None, None, None],
            collateral_output: Some(output.clone()),
            has_collateral_return: true,
            mint: vec![Asset {
                policy: profile.assets["cnight"].policy.clone(),
                name: profile.assets["cnight"].name.clone(),
                quantity: 999,
            }],
            datums: BTreeMap::new(),
            body: &[],
            auxiliary: None,
            auxiliary_hash: None,
        };
        let mut held = BTreeMap::from([
            (
                ordinary.clone(),
                HeldOutput {
                    transaction_id: 1,
                    output: output.clone(),
                },
            ),
            (
                collateral.clone(),
                HeldOutput {
                    transaction_id: 2,
                    output,
                },
            ),
        ]);
        let rows = build_facts(&profile, 10, &[tx], &mut held, &Supplements::default()).unwrap();
        assert_eq!(rows.tx.len(), 1);
        assert!(!rows.tx[0].valid_contract);
        assert_eq!(rows.tx_in.len(), 1);
        assert_eq!(rows.tx_in[0].tx_out_id, 2);
        assert_eq!(rows.tx_in[0].tx_out_index, 2);
        assert_eq!(rows.tx_out.len(), 1);
        assert_eq!(rows.tx_out[0].index, 3);
        assert_eq!(rows.tx_out[0].id, ids::output_id(10, 0, 3).unwrap());
        assert!(rows.mint.is_empty());
        assert!(rows.tx_metadata.is_empty());
        assert!(held.contains_key(&ordinary));
        assert!(!held.contains_key(&collateral));
    }

    #[test]
    fn ra_14_c_2_14_and_d_2_1_full_units_and_governance_policies() {
        let profile = Profile::load("mainnet").unwrap();
        let selection = Selection::new(&profile);
        for role in ["permissioned_candidates", "auth", "cnight"] {
            let a = &profile.assets[role];
            assert!(selection.required(&Asset {
                policy: a.policy.clone(),
                name: a.name.clone(),
                quantity: 1
            }));
            let other = Asset {
                policy: a.policy.clone(),
                name: format!("{}ff", a.name),
                quantity: 1,
            };
            assert!(!selection.required(&other));
        }
        for role in ["council", "technical_committee"] {
            assert!(selection.required(&Asset {
                policy: profile.assets[role].policy.clone(),
                name: "ff".to_owned(),
                quantity: 1
            }));
        }
    }
}
