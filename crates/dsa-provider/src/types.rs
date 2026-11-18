//! Wire types follow the Blockfrost OpenAPI and backend response shapes.
//! Pagination uses count=100 and ascending order except retired pools and pool
//! history, which use descending order. Transaction CBOR rows use `tx_hash`.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::Error;

fn hex(value: &str) -> bool {
    value.len().is_multiple_of(2)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub struct CborHex(String);

impl TryFrom<String> for CborHex {
    type Error = &'static str;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty() || !hex(&value) {
            return Err("invalid CBOR hex");
        }
        Ok(Self(value))
    }
}
impl CborHex {
    pub fn as_str(&self) -> &str {
        &self.0
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Page(u32);
impl Page {
    pub fn new(number: u32) -> Result<Self, Error> {
        if !(1..=21474836).contains(&number) {
            return Err(Error::configuration("request.page"));
        }
        Ok(Self(number))
    }
    pub fn number(self) -> u32 {
        self.0
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
    pub size: u32,
    pub previous_block: Option<Hash32>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct TransactionCbor {
    pub tx_hash: Hash32,
    pub cbor: CborHex,
}

impl From<&Header> for dsa_chain::checks::Header {
    fn from(header: &Header) -> Self {
        Self {
            height: header.height,
            hash: header.hash.to_string(),
            previous_hash: header.previous_block.as_ref().map(ToString::to_string),
            transaction_count: header.tx_count as usize,
        }
    }
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
    pub max_block_size: u32,
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

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Metadata {
    pub label: Quantity,
    /// Raw JSON preserves the provider rendering, exact integer tokens and deep
    /// content for the bounded iterative metadata conversion.
    pub json_metadata: Box<RawValue>,
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
    pub inline_datum: Option<CborHex>,
    pub collateral: bool,
    pub consumed_by_tx: Option<Hash32>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct TransactionOutputs {
    pub hash: Hash32,
    // Inputs are derived from hash-bound CBOR. Provider input renderings can be
    // large and are ignored by the output-classification fallback.
    pub outputs: Vec<Output>,
}

#[derive(Clone, Debug)]
pub enum Endpoint {
    LatestBlock,
    Block(Hash32),
    BlockAtHeight(u32),
    NextBlocks { block: Hash32, page: Page },
    BlockCbor { block: Hash32, page: Page },
    Genesis,
    Epoch(u32),
    EpochParameters(u32),
    EpochBlocks { epoch: u32, page: Page },
    EpochPoolStake { epoch: u32, pool: PoolId },
    Pools(Page),
    ExtendedPools(Page),
    RetiredPools(Page),
    Pool(PoolId),
    PoolHistory { pool: PoolId, page: Page },
    Datum(Hash32),
    Metadata(Hash32),
    TransactionOutputs(Hash32),
}

#[derive(Clone, Debug)]
pub enum Reply {
    Header(Header),
    Headers(Vec<Header>),
    Transactions(Vec<TransactionCbor>),
    Genesis(Genesis),
    Epoch(EpochInfo),
    EpochParameters(EpochParameters),
    Blocks(Vec<Hash32>),
    StakeAddresses(Vec<StakeAddress>),
    Pools(Vec<PoolId>),
    PoolStakes(Vec<PoolStake>),
    RetiredPools(Vec<RetiredPool>),
    Pool(PoolStake),
    PoolHistory(Vec<PoolHistory>),
    Datum(Option<CborHex>),
    Metadata(Vec<Metadata>),
    TransactionOutputs(TransactionOutputs),
}

#[derive(Clone, Copy)]
pub(crate) enum Family {
    Header,
    BlockCbor,
    Genesis,
    Epoch,
    Pool,
    Datum,
    Metadata,
    Outputs,
}

impl Endpoint {
    pub(crate) fn path(&self) -> Result<String, Error> {
        let paged = |path: String, page: Page, descending: bool| {
            format!(
                "{path}?count=100&page={}&order={}",
                page.0,
                if descending { "desc" } else { "asc" }
            )
        };
        let epoch = match self {
            Self::Epoch(epoch)
            | Self::EpochParameters(epoch)
            | Self::EpochBlocks { epoch, .. }
            | Self::EpochPoolStake { epoch, .. } => Some(*epoch),
            _ => None,
        };
        if epoch.is_some_and(|number| number > i32::MAX as u32) {
            return Err(Error::configuration("request.epoch"));
        }
        Ok(match self {
            Self::LatestBlock => "/blocks/latest".to_owned(),
            Self::Block(hash) => format!("/blocks/{hash}"),
            Self::BlockAtHeight(height) => {
                if *height >= 1 << 27 {
                    return Err(Error::configuration("request.block_height"));
                }
                format!("/blocks/{height}")
            }
            Self::NextBlocks { block, page } => {
                format!("/blocks/{block}/next?count=100&page={}", page.number())
            }
            Self::BlockCbor { block, page } => {
                paged(format!("/blocks/{block}/txs/cbor"), *page, false)
            }
            Self::Genesis => "/genesis".to_owned(),
            Self::Epoch(epoch) => format!("/epochs/{epoch}"),
            Self::EpochParameters(epoch) => format!("/epochs/{epoch}/parameters"),
            Self::EpochBlocks { epoch, page } => {
                paged(format!("/epochs/{epoch}/blocks"), *page, true)
            }
            Self::EpochPoolStake { epoch, pool } => {
                format!("/epochs/{epoch}/stakes/{pool}?count=1&page=1")
            }
            Self::Pools(page) => paged("/pools".to_owned(), *page, false),
            Self::ExtendedPools(page) => paged("/pools/extended".to_owned(), *page, false),
            Self::RetiredPools(page) => paged("/pools/retired".to_owned(), *page, true),
            Self::Pool(pool) => format!("/pools/{pool}"),
            Self::PoolHistory { pool, page } => {
                paged(format!("/pools/{pool}/history"), *page, true)
            }
            Self::Datum(hash) => format!("/scripts/datum/{hash}/cbor"),
            Self::Metadata(hash) => format!("/txs/{hash}/metadata"),
            Self::TransactionOutputs(hash) => format!("/txs/{hash}/utxos"),
        })
    }

    pub(crate) fn family(&self) -> Family {
        match self {
            Self::LatestBlock
            | Self::Block(_)
            | Self::BlockAtHeight(_)
            | Self::NextBlocks { .. } => Family::Header,
            Self::BlockCbor { .. } => Family::BlockCbor,
            Self::Genesis => Family::Genesis,
            Self::Epoch(_) | Self::EpochParameters(_) | Self::EpochBlocks { .. } => Family::Epoch,
            Self::EpochPoolStake { .. }
            | Self::Pools(_)
            | Self::ExtendedPools(_)
            | Self::RetiredPools(_)
            | Self::Pool(_)
            | Self::PoolHistory { .. } => Family::Pool,
            Self::Datum(_) => Family::Datum,
            Self::Metadata(_) => Family::Metadata,
            Self::TransactionOutputs(_) => Family::Outputs,
        }
    }

    pub(crate) fn missing_code(&self) -> &'static str {
        match self.family() {
            Family::Epoch => "provider_epoch_not_ready",
            Family::Header | Family::BlockCbor => "provider_block_not_ready",
            _ => "provider_not_ready",
        }
    }

    pub(crate) fn validate_outputs(&self, row: &TransactionOutputs) -> Result<(), &'static str> {
        let Self::TransactionOutputs(expected) = self else {
            return Err("provider_invalid_response");
        };
        if row.hash != *expected {
            return Err("provider_invalid_response");
        }
        let mut indexes = std::collections::HashSet::new();
        for output in &row.outputs {
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

    pub(crate) fn decode(&self, body: &[u8]) -> Result<Reply, &'static str> {
        fn parse<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, &'static str> {
            serde_json::from_slice(body).map_err(|_| "provider_invalid_response")
        }
        fn page<T>(body: &[u8], maximum: usize) -> Result<Vec<T>, &'static str>
        where
            T: serde::de::DeserializeOwned,
        {
            let rows: Vec<T> = parse(body)?;
            if rows.len() > maximum {
                return Err("provider_bad_page");
            }
            Ok(rows)
        }
        fn header(header: &Header) -> Result<(), &'static str> {
            if header.height >= 1 << 27 || header.epoch > i32::MAX as u32 {
                return Err("provider_invalid_response");
            }
            Ok(())
        }
        Ok(match self {
            Self::LatestBlock | Self::Block(_) | Self::BlockAtHeight(_) => {
                let row: Header = parse(body)?;
                header(&row)?;
                if matches!(self, Self::Block(expected) if expected != &row.hash)
                    || matches!(self, Self::BlockAtHeight(expected) if *expected != row.height)
                {
                    return Err("provider_invalid_response");
                }
                Reply::Header(row)
            }
            Self::NextBlocks { .. } => {
                let rows = page::<Header>(body, 100)?;
                for row in &rows {
                    header(row)?;
                }
                Reply::Headers(rows)
            }
            Self::BlockCbor { .. } => Reply::Transactions(page(body, 100)?),
            Self::Genesis => Reply::Genesis(parse(body)?),
            Self::Epoch(expected) => {
                let row: EpochInfo = parse(body)?;
                if row.epoch != *expected {
                    return Err("provider_invalid_response");
                }
                if row.active_stake.is_none() {
                    return Err("provider_epoch_not_ready");
                }
                Reply::Epoch(row)
            }
            Self::EpochParameters(expected) => {
                let row: EpochParameters = parse(body)?;
                if row.epoch != *expected {
                    return Err("provider_invalid_response");
                }
                if row.nonce.is_none() {
                    return Err("provider_epoch_not_ready");
                }
                if row.max_tx_size > i32::MAX as u32 {
                    return Err("provider_invalid_response");
                }
                Reply::EpochParameters(row)
            }
            Self::EpochBlocks { .. } => Reply::Blocks(page(body, 100)?),
            Self::EpochPoolStake { .. } => Reply::StakeAddresses(page(body, 1)?),
            Self::Pools(_) => Reply::Pools(page(body, 100)?),
            Self::ExtendedPools(_) => Reply::PoolStakes(page(body, 100)?),
            Self::RetiredPools(_) => Reply::RetiredPools(page(body, 100)?),
            Self::Pool(expected) => {
                let row: PoolStake = parse(body)?;
                if row.pool_id != *expected {
                    return Err("provider_invalid_response");
                }
                Reply::Pool(row)
            }
            Self::PoolHistory { .. } => Reply::PoolHistory(page(body, 100)?),
            Self::Datum(_) => {
                #[derive(Deserialize)]
                struct Datum {
                    cbor: CborHex,
                }
                Reply::Datum(Some(parse::<Datum>(body)?.cbor))
            }
            Self::Metadata(_) => Reply::Metadata(parse(body)?),
            Self::TransactionOutputs(_) => {
                let row: TransactionOutputs = parse(body)?;
                self.validate_outputs(&row)?;
                Reply::TransactionOutputs(row)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn request_values_are_bounded_before_a_send() {
        assert!(Page::new(0).is_err());
        assert!(Page::new(21474837).is_err());
        assert!(Endpoint::BlockAtHeight(1 << 27).path().is_err());
        assert!(Endpoint::Epoch(u32::MAX).path().is_err());
        assert!("../elsewhere".parse::<Hash32>().is_err());
        assert!("pool1invalid".parse::<PoolId>().is_err());
        let block: Hash32 = "1".repeat(64).parse().unwrap();
        assert_eq!(
            Endpoint::BlockCbor {
                block,
                page: Page::new(2).unwrap()
            }
            .path()
            .unwrap(),
            format!(
                "/blocks/{}/txs/cbor?count=100&page=2&order=asc",
                "1".repeat(64)
            )
        );
    }

    #[test]
    fn cbor_pages_use_tx_hash_and_enforce_the_api_page_size() {
        let endpoint = Endpoint::BlockCbor {
            block: "1".repeat(64).parse().unwrap(),
            page: Page::new(1).unwrap(),
        };
        let row = json!({"tx_hash":"2".repeat(64),"cbor":"84a000f5f6","extra":null});
        let bytes = serde_json::to_vec(&vec![row.clone(); 100]).unwrap();
        assert!(
            matches!(endpoint.decode(&bytes), Ok(Reply::Transactions(rows)) if rows.len()==100)
        );
        let bytes = serde_json::to_vec(&vec![row; 101]).unwrap();
        assert_eq!(endpoint.decode(&bytes).unwrap_err(), "provider_bad_page");
        let wrong_key =
            serde_json::to_vec(&json!([{"hash":"2".repeat(64),"cbor":"84a000f5f6"}])).unwrap();
        assert_eq!(
            endpoint.decode(&wrong_key).unwrap_err(),
            "provider_invalid_response"
        );
    }

    #[test]
    fn quantities_preserve_decimal_wire_strings_and_bounds() {
        let maximum: Quantity = serde_json::from_str("\"18446744073709551615\"").unwrap();
        assert_eq!(maximum.0, u64::MAX);
        assert_eq!(
            serde_json::to_string(&maximum).unwrap(),
            "\"18446744073709551615\""
        );
        for invalid in ["\"-1\"", "\"1.0\"", "\"18446744073709551616\"", "42"] {
            assert!(serde_json::from_str::<Quantity>(invalid).is_err());
        }
    }
}

#[cfg(test)]
mod epoch_domain_tests {
    use super::*;

    #[test]
    fn epoch_size_domain_includes_zero() {
        let body = format!(
            "{{\"epoch\":667,\"nonce\":\"{}\",\"max_tx_size\":0,\"max_block_size\":0}}",
            "1".repeat(64)
        );
        assert!(
            matches!(Endpoint::EpochParameters(667).decode(body.as_bytes()),Ok(Reply::EpochParameters(row)) if row.max_tx_size==0)
        );
    }
}
