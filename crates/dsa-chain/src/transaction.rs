//! Facts extracted from the original transaction body, without decoding unused
//! scripts, metadata, certificates or governance content into value trees.

use std::collections::{BTreeMap, BTreeSet};

use pallas_addresses::Address;
use pallas_crypto::hash::Hasher;
use serde::{Deserialize, Serialize};

use crate::cbor::{Error, Item, error};

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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Output {
    pub address_raw: Vec<u8>,
    pub address: Option<String>,
    pub coin: u64,
    pub assets: Vec<Asset>,
    pub data_hash: Option<String>,
    pub inline_datum: Option<Vec<u8>>,
    pub classification_safe: bool,
}

impl Output {
    pub fn check_inline_hash(&self) -> Result<(), Error> {
        if let Some(bytes) = &self.inline_datum
            && self.data_hash.as_deref() != Some(hash(bytes).as_str())
        {
            return Err(error("provider_inline_datum_hash_mismatch", 0));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct Transaction<'a> {
    pub hash: String,
    pub valid: bool,
    pub ordinary: Vec<Outpoint>,
    pub collateral: Vec<Outpoint>,
    pub reference: Vec<Outpoint>,
    pub outputs: Vec<Option<Output>>,
    pub collateral_output: Option<Output>,
    pub has_collateral_return: bool,
    pub mint: Vec<Asset>,
    pub datums: BTreeMap<String, Vec<u8>>,
    pub body: &'a [u8],
    pub auxiliary: Option<&'a [u8]>,
    pub auxiliary_hash: Option<String>,
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
        if !inputs.insert(Outpoint { hash, index }) {
            return Err(error("cbor_duplicate_input", 0));
        }
    }
    Ok(inputs.into_iter().collect())
}

fn assets(item: Item<'_>, mint: bool) -> Result<Vec<Asset>, Error> {
    let mut result = BTreeMap::new();
    let mut policies = BTreeSet::new();
    for (policy, names) in item.map()? {
        let policy = hex::encode(bytes_of_length(policy, 28)?);
        if !policies.insert(policy.clone()) {
            return Err(error("cbor_duplicate_asset_policy", 0));
        }
        let pairs = names.map()?;
        if pairs.is_empty() {
            return Err(error("cbor_asset_shape", 0));
        }
        for (name, quantity) in pairs {
            let bytes = name.bytes()?;
            if bytes.len() > 32 {
                return Err(error("cbor_asset_shape", 0));
            }
            let name = hex::encode(bytes);
            let quantity = quantity.integer()?;
            if quantity == 0
                || if mint {
                    i64::try_from(quantity).is_err()
                } else {
                    u64::try_from(quantity).is_err()
                }
            {
                return Err(error("cbor_asset_quantity", 0));
            }
            if result.insert((policy.clone(), name), quantity).is_some() {
                return Err(error("cbor_duplicate_asset", 0));
            }
        }
    }
    Ok(result
        .into_iter()
        .map(|((policy, name), quantity)| Asset {
            policy,
            name,
            quantity,
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
        classification_safe: true,
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
    result.classification_safe = result.address.is_some();
    Ok(result)
}

pub fn decode<'a>(raw: &'a [u8], expected_hash: &str) -> Result<Transaction<'a>, Error> {
    let root = Item::parse(raw)?;
    let envelope = root.array()?;
    if envelope.len() != 4 {
        return Err(error("cbor_unsupported_envelope", 0));
    }
    let body = fields(envelope[0])?;
    if ![0, 1, 2].iter().all(|k| body.contains_key(k))
        || body.keys().any(|k| *k > 22 || [10, 12].contains(k))
    {
        return Err(error("cbor_unsupported_body", 0));
    }
    let hash = hash(envelope[0].raw);
    if hash != expected_hash {
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
    let reference = body
        .get(&18)
        .copied()
        .map(input_set)
        .transpose()?
        .unwrap_or_default();
    let outputs = body[&1]
        .array()?
        .into_iter()
        .map(|x| output(x).ok())
        .collect();
    let collateral_output = body.get(&16).copied().and_then(|x| output(x).ok());
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
            datums.insert(crate::transaction::hash(datum.raw), datum.raw.to_vec());
        }
    }
    Ok(Transaction {
        hash,
        valid,
        ordinary,
        collateral,
        reference,
        outputs,
        collateral_output,
        has_collateral_return: body.contains_key(&16),
        mint,
        datums,
        body: envelope[0].raw,
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
    fn ra_14_b_1_7_inline_bytes_match_hash_even_when_value_not_stored() {
        let mut output = Output {
            address_raw: Vec::new(),
            address: Some("ordinary".to_owned()),
            coin: 1,
            assets: Vec::new(),
            data_hash: None,
            inline_datum: Some(vec![0]),
            classification_safe: true,
        };
        assert_eq!(
            output.check_inline_hash().unwrap_err().code,
            "provider_inline_datum_hash_mismatch"
        );
        output.data_hash = Some("00".repeat(32));
        assert!(output.check_inline_hash().is_err());
        output.data_hash =
            Some("03170a2e7597b7b7e3d84c05391d139a62b157e78786d8c082f29dcf4c111314".to_owned());
        output.check_inline_hash().unwrap();
    }

    #[test]
    fn ra_14_a_4_4_interpreted_map_keys_are_semantically_unique() {
        let raw = hex::decode("a20001180002").unwrap();
        assert_eq!(
            fields(Item::parse(&raw).unwrap()).unwrap_err().code,
            "cbor_duplicate_map_key"
        );
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
            input_set(Item::parse(&duplicate).unwrap())
                .unwrap_err()
                .code,
            "cbor_duplicate_input"
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
