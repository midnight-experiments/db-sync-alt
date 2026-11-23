//! Facts extracted from the original transaction body, without decoding unused
//! scripts, metadata, certificates or governance content into value trees.
//! The endpoint supplies the four-element Alonzo-and-later envelope. Byron
//! addresses inside later transactions are supported by address rendering.
//! A [`Transaction`] exists only as the result of [`decode`], so its body hash
//! has always been checked against the bytes it was read from.

use std::collections::{BTreeMap, BTreeSet};

use pallas_addresses::Address;
use pallas_crypto::hash::Hasher;
use serde::{Deserialize, Serialize};

use crate::cbor::{Error, Item, error};

/// The largest transaction read, checked before parsing. It is 16 times the
/// protocol's current maximum transaction size (16,384 bytes) and above the
/// largest block body (90,112 bytes), so it bounds parser memory without
/// refusing a transaction the ledger accepts.
pub const TRANSACTION_SIZE_LIMIT: usize = 262_144;

/// The highest transaction body key of the Conway ledger. A body with a later
/// key stops the block with `unsupported_transaction_body_field`: a field this
/// release cannot read could create or spend outputs, so skipping it could
/// lose a relevant output. Each era that adds a body field needs a release.
pub const LAST_BODY_KEY: u64 = 22;

/// Body keys no era assigns.
const UNASSIGNED_BODY_KEYS: [u64; 2] = [10, 12];

pub fn hash(bytes: &[u8]) -> String {
    Hasher::<256>::hash(bytes).to_string()
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Outpoint {
    pub hash: String,
    pub index: u16,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Asset {
    pub policy: String,
    pub name: String,
    pub quantity: i128,
}

impl Asset {
    pub fn unit(&self) -> String {
        format!("{}{}", self.policy, self.name)
    }
}

/// A decoded output. `address` is `None` when the address cannot be
/// rendered; such an output is resolved through the provider.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Output {
    pub address_raw: Vec<u8>,
    pub address: Option<String>,
    pub coin: u64,
    pub assets: Vec<Asset>,
    pub data_hash: Option<String>,
    pub inline_datum: Option<Vec<u8>>,
}

#[derive(Clone, Debug)]
pub struct Transaction<'a> {
    hash: String,
    valid: bool,
    inputs: Vec<Outpoint>,
    collateral: Vec<Outpoint>,
    outputs: Vec<Option<Output>>,
    collateral_return: Option<Option<Output>>,
    mint: Vec<Asset>,
    datums: BTreeMap<String, &'a [u8]>,
    pub(crate) auxiliary: Option<&'a [u8]>,
    pub(crate) auxiliary_hash: Option<String>,
}

impl<'a> Transaction<'a> {
    pub fn hash(&self) -> &str {
        &self.hash
    }
    /// False for a transaction that failed phase 2: its collateral is spent
    /// and its collateral return is created instead.
    pub fn is_valid(&self) -> bool {
        self.valid
    }
    pub fn inputs(&self) -> &[Outpoint] {
        &self.inputs
    }
    pub fn collateral(&self) -> &[Outpoint] {
        &self.collateral
    }
    pub fn effective_inputs(&self) -> &[Outpoint] {
        if self.valid {
            &self.inputs
        } else {
            &self.collateral
        }
    }
    /// Body outputs in order; `None` where an output could not be decoded.
    pub fn outputs(&self) -> &[Option<Output>] {
        &self.outputs
    }
    pub fn has_collateral_return(&self) -> bool {
        self.collateral_return.is_some()
    }
    pub fn collateral_return(&self) -> Option<&Output> {
        self.collateral_return.as_ref().and_then(Option::as_ref)
    }
    pub fn mint(&self) -> &[Asset] {
        &self.mint
    }
    /// Witness-set datums by hash, as their original bytes.
    pub fn datums(&self) -> &BTreeMap<String, &'a [u8]> {
        &self.datums
    }
    pub fn auxiliary(&self) -> Option<&'a [u8]> {
        self.auxiliary
    }
}

#[derive(Debug, thiserror::Error)]
#[error("transaction_decode at position {position}, transaction {transaction}: {source}")]
pub struct DecodeError {
    pub position: usize,
    pub transaction: String,
    #[source]
    pub source: Error,
}

/// Decode a block's transactions in block order from `(hash, bytes)` pairs.
pub fn decode_all<'a>(
    records: impl IntoIterator<Item = (&'a str, &'a [u8])>,
) -> Result<Vec<Transaction<'a>>, DecodeError> {
    records
        .into_iter()
        .enumerate()
        .map(|(position, (hash, raw))| {
            decode(raw, hash).map_err(|source| DecodeError {
                position,
                transaction: hash.to_owned(),
                source,
            })
        })
        .collect()
}

fn fields(item: Item<'_>) -> Result<BTreeMap<u64, Item<'_>>, Error> {
    let mut fields = BTreeMap::new();
    for (k, v) in item.map()? {
        if fields.insert(k.unsigned()?, v).is_some() {
            return Err(error("cbor_duplicate_map_key", 0));
        }
    }
    Ok(fields)
}

fn bytes_of_length(item: Item<'_>, length: usize) -> Result<Vec<u8>, Error> {
    let bytes = item.bytes()?;
    if bytes.len() == length {
        Ok(bytes)
    } else {
        Err(error("cbor_byte_length", 0))
    }
}

fn input_set(item: Item<'_>) -> Result<Vec<Outpoint>, Error> {
    let item = if item.head()?.major == 6 {
        let (tag, value) = item.tag()?;
        if tag != 258 {
            return Err(error("cbor_input_tag", 0));
        }
        value
    } else {
        item
    };
    let mut inputs = BTreeSet::new();
    for pair in item.array()? {
        let pair = pair.array()?;
        if pair.len() != 2 {
            return Err(error("cbor_input_shape", 0));
        }
        let hash = hex::encode(bytes_of_length(pair[0], 32)?);
        let index = u16::try_from(pair[1].unsigned()?).map_err(|_| error("cbor_input_shape", 0))?;
        // Ledger versions 2 through 8 decode an effective Set.fromList.
        // Later valid transactions are already unique and yield the same set.
        inputs.insert(Outpoint { hash, index });
    }
    Ok(inputs.into_iter().collect())
}

/// Extract effective multiasset facts from finalized transactions. Historical
/// ledger decoders use Map.fromList at both map levels, then prune zeros and
/// empty maps. Later valid encodings are unique and nonzero, yielding the same
/// facts. Original body bytes remain untouched for transaction hashing.
fn assets(item: Item<'_>, mint: bool) -> Result<Vec<Asset>, Error> {
    let mut policies = BTreeMap::new();
    for (policy, names) in item.map()? {
        let policy = hex::encode(bytes_of_length(policy, 28)?);
        let mut values = BTreeMap::new();
        for (name, quantity) in names.map()? {
            let bytes = name.bytes()?;
            if bytes.len() > 32 {
                return Err(error("cbor_asset_shape", 0));
            }
            let quantity = quantity.integer()?;
            if if mint {
                i64::try_from(quantity).is_err()
            } else {
                u64::try_from(quantity).is_err()
            } {
                return Err(error("cbor_asset_quantity", 0));
            }
            values.insert(hex::encode(bytes), quantity);
        }
        policies.insert(policy, values);
    }
    Ok(policies
        .into_iter()
        .flat_map(|(policy, names)| {
            names
                .into_iter()
                .filter(|(_, quantity)| *quantity != 0)
                .map(move |(name, quantity)| Asset {
                    policy: policy.clone(),
                    name,
                    quantity,
                })
        })
        .collect())
}

/// Pallas canonicalizes pointer components and renders Byron addresses as
/// base58, matching cardano-db-sync. Unknown forms stay explicitly undecided.
pub fn address_text(raw: &[u8]) -> Option<String> {
    match Address::from_bytes(raw).ok()? {
        Address::Byron(address) => Some(address.to_base58()),
        address => address.to_bech32().ok(),
    }
}

/// Decode a canonical provider address for supplement validation.
pub fn address_bytes(text: &str) -> Option<Vec<u8>> {
    let address: Address = text.parse().ok()?;
    Some(address.to_vec())
}

fn output(item: Item<'_>) -> Result<Output, Error> {
    let (address, value, datum, legacy, script) = match item.head()?.major {
        4 => {
            let parts = item.array()?;
            if !(2..=3).contains(&parts.len()) {
                return Err(error("cbor_output_shape", 0));
            }
            (parts[0], parts[1], parts.get(2).copied(), true, false)
        }
        5 => {
            let f = fields(item)?;
            if f.keys().any(|k| *k > 3) {
                return Err(error("cbor_output_shape", 0));
            }
            (
                *f.get(&0).ok_or_else(|| error("cbor_output_shape", 0))?,
                *f.get(&1).ok_or_else(|| error("cbor_output_shape", 0))?,
                f.get(&2).copied(),
                false,
                f.get(&3).is_some_and(|x| !x.is_null()),
            )
        }
        _ => return Err(error("cbor_output_shape", 0)),
    };
    let address_raw = address.bytes()?;
    if address_raw.is_empty() {
        return Err(error("cbor_output_address", 0));
    }
    let (coin, assets) = if value.head()?.major == 0 {
        (value.unsigned()?, Vec::new())
    } else {
        let pair = value.array()?;
        if pair.len() != 2 {
            return Err(error("cbor_output_value", 0));
        }
        (pair[0].unsigned()?, assets(pair[1], false)?)
    };
    let mut result = Output {
        address: address_text(&address_raw),
        address_raw,
        coin,
        assets,
        data_hash: None,
        inline_datum: None,
    };
    // A script reference does not alter any consumed output field. Its bytes
    // have already been structurally skipped; the reference is never decoded.
    let _ = script;
    if let Some(datum) = datum.filter(|d| !d.is_null()) {
        if legacy {
            result.data_hash = Some(hex::encode(bytes_of_length(datum, 32)?));
        } else {
            let pair = datum.array()?;
            if pair.len() != 2 {
                return Err(error("cbor_datum_option", 0));
            }
            match pair[0].unsigned()? {
                0 => result.data_hash = Some(hex::encode(bytes_of_length(pair[1], 32)?)),
                1 => {
                    let (tag, bytes) = pair[1].tag()?;
                    if tag != 24 {
                        return Err(error("cbor_datum_option", 0));
                    }
                    let raw = bytes.bytes()?;
                    result.data_hash = Some(hash(&raw));
                    result.inline_datum = Some(raw);
                }
                _ => return Err(error("cbor_datum_option", 0)),
            }
        }
    }
    Ok(result)
}

pub fn decode<'a>(raw: &'a [u8], expected_hash: &str) -> Result<Transaction<'a>, Error> {
    if raw.len() > TRANSACTION_SIZE_LIMIT {
        return Err(error("transaction_size_limit", 0));
    }
    let root = Item::parse(raw)?;
    let envelope = root.array()?;
    if envelope.len() != 4 {
        return Err(error("cbor_unsupported_envelope", 0));
    }
    let body = fields(envelope[0])?;
    if ![0, 1, 2].iter().all(|k| body.contains_key(k)) {
        return Err(error("cbor_transaction_body_shape", 0));
    }
    if body
        .keys()
        .any(|k| *k > LAST_BODY_KEY || UNASSIGNED_BODY_KEYS.contains(k))
    {
        return Err(error("unsupported_transaction_body_field", 0));
    }
    let body_hash = hash(envelope[0].raw);
    if body_hash != expected_hash {
        return Err(error("cbor_body_hash_mismatch", 0));
    }
    body[&2].unsigned()?;
    let valid = envelope[2].boolean()?;
    let ordinary = input_set(body[&0])?;
    let collateral = body
        .get(&13)
        .copied()
        .map(input_set)
        .transpose()?
        .unwrap_or_default();
    let outputs = body[&1]
        .array()?
        .into_iter()
        .map(|x| output(x).ok())
        .collect();
    let collateral_return = body.get(&16).copied().map(|x| output(x).ok());
    let mint = body
        .get(&9)
        .copied()
        .map(|x| assets(x, true))
        .transpose()?
        .unwrap_or_default();
    let witness = fields(envelope[1])?;
    let mut datums = BTreeMap::new();
    if let Some(datum_set) = witness.get(&4).copied() {
        let datum_set = if datum_set.head()?.major == 6 {
            let (tag, value) = datum_set.tag()?;
            if tag != 258 {
                return Err(error("cbor_datum_set_tag", 0));
            }
            value
        } else {
            datum_set
        };
        for datum in datum_set.array()? {
            datums.insert(hash(datum.raw), datum.raw);
        }
    }
    Ok(Transaction {
        hash: body_hash,
        valid,
        inputs: ordinary,
        collateral,
        outputs,
        collateral_return,
        mint,
        datums,
        auxiliary: (!envelope[3].is_null()).then_some(envelope[3].raw),
        auxiliary_hash: body
            .get(&7)
            .copied()
            .map(|x| bytes_of_length(x, 32).map(hex::encode))
            .transpose()?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ra_14_a_4_4_interpreted_map_keys_are_semantically_unique() {
        let raw = hex::decode("a20001180002").unwrap();
        assert_eq!(
            fields(Item::parse(&raw).unwrap()).unwrap_err().code,
            "cbor_duplicate_map_key"
        );
    }

    #[test]
    fn historical_multiasset_helpers_follow_nested_map_and_zero_pruning_rules() {
        // Standalone CBOR map conformance for our helper only, with no ledger
        // transaction envelope or third-party decoder invocation.
        let policy = "01".repeat(28);
        let cases = [
            (format!("a1581c{policy}a24000410102"), vec![("01", 2)]),
            (format!("a1581c{policy}a240014000"), vec![]),
            (
                format!("a2581c{policy}a14001581c{policy}a1410103"),
                vec![("01", 3)],
            ),
            (format!("a1581c{policy}a0"), vec![]),
        ];
        for (raw, expected) in cases {
            let bytes = hex::decode(raw).unwrap();
            let actual = assets(Item::parse(&bytes).unwrap(), false).unwrap();
            assert_eq!(
                actual,
                expected
                    .into_iter()
                    .map(|(name, quantity)| Asset {
                        policy: policy.clone(),
                        name: name.to_owned(),
                        quantity
                    })
                    .collect::<Vec<_>>()
            );
        }
        let raw = hex::decode(format!("a1581c{policy}a14020")).unwrap();
        assert_eq!(
            assets(Item::parse(&raw).unwrap(), true).unwrap()[0].quantity,
            -1
        );
        assert!(assets(Item::parse(&raw).unwrap(), false).is_err());
    }

    #[test]
    fn ra_14_a_5_13_standard_input_set_tags_duplicates_and_domains() {
        let point = format!("825820{}00", "00".repeat(32));
        for raw in [format!("81{point}"), format!("d9010281{point}")] {
            let raw = hex::decode(raw).unwrap();
            assert_eq!(input_set(Item::parse(&raw).unwrap()).unwrap().len(), 1);
        }
        let duplicate = hex::decode(format!("82{point}{point}")).unwrap();
        assert_eq!(
            input_set(Item::parse(&duplicate).unwrap()).unwrap().len(),
            1
        );
        let overflow = hex::decode(format!("81825820{}1a00010000", "00".repeat(32))).unwrap();
        assert!(input_set(Item::parse(&overflow).unwrap()).is_err());
        let wrong_tag = hex::decode("d9010380").unwrap();
        assert_eq!(
            input_set(Item::parse(&wrong_tag).unwrap())
                .unwrap_err()
                .code,
            "cbor_input_tag"
        );
    }
}
