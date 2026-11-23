//! Wire types of the Blockfrost answers the follower reads. Hex is decoded
//! while deserialising; unknown members are skipped without building them.

use std::{fmt, str::FromStr};

use bytes::Bytes;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use serde_json::value::RawValue;

use crate::Error;

fn hex(value: &str) -> bool {
    value.len().is_multiple_of(2)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// A 32-byte hash as lowercase hex.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub struct Hash32(String);

impl TryFrom<String> for Hash32 {
    type Error = &'static str;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.len() != 64 || !hex(&value) {
            return Err("invalid hash");
        }
        Ok(Self(value))
    }
}
impl Hash32 {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl FromStr for Hash32 {
    type Err = Error;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::try_from(value.to_owned()).map_err(|_| Error::configuration("request.hash"))
    }
}
impl fmt::Display for Hash32 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Non-empty bytes carried as lowercase hex.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hex(pub Vec<u8>);

impl<'de> Deserialize<'de> for Hex {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl de::Visitor<'_> for Visitor {
            type Value = Hex;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("non-empty lowercase hex")
            }
            fn visit_str<E: de::Error>(self, value: &str) -> Result<Hex, E> {
                if value.is_empty() || !hex(value) {
                    return Err(E::custom("invalid hex"));
                }
                hex::decode(value)
                    .map(Hex)
                    .map_err(|_| E::custom("invalid hex"))
            }
        }
        deserializer.deserialize_str(Visitor)
    }
}
impl Serialize for Hex {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&hex::encode(&self.0))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub struct PoolId(String);

impl TryFrom<String> for PoolId {
    type Error = &'static str;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        let decoded = bech32::primitives::decode::CheckedHrpstring::new::<bech32::Bech32>(&value)
            .map_err(|_| "invalid pool id")?;
        if !value.starts_with("pool1")
            || decoded.hrp().as_str() != "pool"
            || decoded.byte_iter().count() != 28
        {
            return Err("invalid pool id");
        }
        Ok(Self(value))
    }
}
impl FromStr for PoolId {
    type Err = Error;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::try_from(value.to_owned()).map_err(|_| Error::configuration("request.pool_id"))
    }
}
impl fmt::Display for PoolId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// A non-negative decimal quantity carried as a JSON string.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Quantity(pub u64);

impl From<Quantity> for String {
    fn from(quantity: Quantity) -> Self {
        quantity.0.to_string()
    }
}
impl TryFrom<String> for Quantity {
    type Error = &'static str;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err("invalid quantity");
        }
        value.parse().map(Self).map_err(|_| "invalid quantity")
    }
}

/// A page number of a list endpoint, from 1.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Page(u32);
impl Page {
    pub fn new(number: u32) -> Result<Self, Error> {
        if !(1..=21474836).contains(&number) {
            return Err(Error::configuration("request.page"));
        }
        Ok(Self(number))
    }
    pub fn first() -> Self {
        Self(1)
    }
    pub fn number(self) -> u32 {
        self.0
    }
    pub fn next(self) -> Option<Self> {
        Self::new(self.0 + 1).ok()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Header {
    pub hash: Hash32,
    pub height: u32,
    pub slot: u64,
    pub epoch: u32,
    pub time: u64,
    pub tx_count: u32,
    pub previous_block: Option<Hash32>,
}

impl From<&Header> for dsa_chain::block::Header {
    fn from(header: &Header) -> Self {
        Self {
            height: header.height,
            hash: header.hash.to_string(),
            previous_hash: header.previous_block.as_ref().map(ToString::to_string),
            transaction_count: header.tx_count as usize,
        }
    }
}

/// One transaction of a block's list, with its original CBOR.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct TransactionCbor {
    pub tx_hash: Hash32,
    pub cbor: Hex,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Genesis {
    pub network_magic: u32,
    pub security_param: u32,
    pub epoch_length: u64,
    pub system_start: u64,
    pub slot_length: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct EpochInfo {
    pub epoch: u32,
    pub active_stake: Option<Quantity>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct EpochParameters {
    pub epoch: u32,
    pub nonce: Option<Hash32>,
    pub max_tx_size: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PoolStake {
    pub pool_id: PoolId,
    pub active_stake: Quantity,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct RetiredPool {
    pub pool_id: PoolId,
    pub epoch: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PoolHistory {
    pub epoch: u32,
    pub active_stake: Quantity,
    pub delegators_count: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct StakeAddress {
    pub stake_address: String,
    pub amount: Quantity,
}

/// A datum's CBOR and the answer it came from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Datum {
    pub cbor: Vec<u8>,
    pub body: Bytes,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MetadataEntry {
    pub label: Quantity,
    /// The provider's rendering, kept raw: exact integer tokens, any depth.
    pub json_metadata: Box<RawValue>,
}

/// A transaction's metadata labels and the answer they came from.
#[derive(Clone, Debug)]
pub struct Metadata {
    pub entries: Vec<MetadataEntry>,
    pub body: Bytes,
}

impl Metadata {
    /// The raw JSON of one label; labels are unique in a checked answer.
    pub fn label(&self, label: u64) -> Option<&str> {
        self.entries
            .iter()
            .find(|entry| entry.label.0 == label)
            .map(|entry| entry.json_metadata.get())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct AssetAmount {
    pub unit: String,
    pub quantity: Quantity,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Output {
    pub address: String,
    pub output_index: u32,
    pub amount: Vec<AssetAmount>,
    pub data_hash: Option<Hash32>,
    pub inline_datum: Option<Hex>,
    pub collateral: bool,
}

/// A transaction's outputs. The provider's input rendering is discarded
/// while streaming. `body` is the complete answer when it is at most
/// [`crate::OUTPUT_ANSWER_KEPT`] bytes.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct TransactionOutputs {
    pub hash: Hash32,
    pub outputs: Vec<Output>,
    #[serde(skip)]
    pub body: Option<Bytes>,
}

impl TransactionOutputs {
    pub(crate) fn validate(&self, expected: &Hash32) -> Result<(), &'static str> {
        if self.hash != *expected {
            return Err("provider_invalid_response");
        }
        let mut indexes = std::collections::HashSet::new();
        for output in &self.outputs {
            if output.address.is_empty() || !indexes.insert(output.output_index) {
                return Err("provider_invalid_response");
            }
            let mut units = std::collections::HashSet::new();
            for amount in &output.amount {
                if !units.insert(amount.unit.as_str())
                    || (amount.unit != "lovelace"
                        && (!(56..=120).contains(&amount.unit.len()) || !hex(&amount.unit)))
                {
                    return Err("provider_invalid_response");
                }
            }
            if !units.contains("lovelace") {
                return Err("provider_invalid_response");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_values_are_bounded_before_a_send() {
        assert!(Page::new(0).is_err());
        assert!(Page::new(21474837).is_err());
        assert!("../elsewhere".parse::<Hash32>().is_err());
        assert!("pool1invalid".parse::<PoolId>().is_err());
        assert_eq!(Page::first().next().unwrap().number(), 2);
    }

    #[test]
    fn quantities_and_hex_round_trip_their_wire_strings() {
        let maximum: Quantity = serde_json::from_str("\"18446744073709551615\"").unwrap();
        assert_eq!(maximum.0, u64::MAX);
        assert_eq!(
            serde_json::to_string(&maximum).unwrap(),
            "\"18446744073709551615\""
        );
        for invalid in ["\"-1\"", "\"1.0\"", "\"18446744073709551616\"", "42"] {
            assert!(serde_json::from_str::<Quantity>(invalid).is_err());
        }
        let bytes: Hex = serde_json::from_str("\"00ff\"").unwrap();
        assert_eq!(bytes.0, vec![0, 255]);
        assert_eq!(serde_json::to_string(&bytes).unwrap(), "\"00ff\"");
        for invalid in ["\"\"", "\"0\"", "\"0F\"", "\"zz\"", "1"] {
            assert!(serde_json::from_str::<Hex>(invalid).is_err(), "{invalid}");
        }
    }
}
