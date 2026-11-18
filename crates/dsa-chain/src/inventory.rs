//! Immutable capture of one received transaction inventory. Initial provider
//! truth is the Q3 trust boundary. Local decoding preserves the captured order
//! and binds interpreted facts to the successful decode of those same bytes.

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::{
    cbor,
    transaction::{self, Asset, Outpoint, Output, Transaction},
};

#[derive(Clone, Debug)]
struct CapturedTransaction {
    hash: String,
    cbor: Vec<u8>,
}

#[derive(Clone, Debug, Default)]
pub struct CapturedTransactions {
    records: Vec<CapturedTransaction>,
}

impl CapturedTransactions {
    /// Capture the supplied read order once. Provider adapters call this before
    /// exposing a fetched page. Cached recordings use the same byte boundary.
    pub fn from_records(records: impl IntoIterator<Item = (String, Vec<u8>)>) -> Self {
        Self {
            records: records
                .into_iter()
                .map(|(hash, cbor)| CapturedTransaction { hash, cbor })
                .collect(),
        }
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Page assembly moves whole captured pages in their checked page order.
    pub fn append(&mut self, mut page: Self) {
        self.records.append(&mut page.records);
    }

    pub fn decode(&self) -> Result<DecodedTransactions<'_>, DecodeError> {
        let transactions = self
            .records
            .iter()
            .enumerate()
            .map(|(position, record)| {
                transaction::decode(&record.cbor, &record.hash).map_err(|source| DecodeError {
                    position,
                    transaction: record.hash.clone(),
                    source,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let seals = transactions.iter().map(fingerprint).collect();
        Ok(DecodedTransactions {
            source: self,
            transactions,
            seals,
        })
    }
}

#[derive(Debug, thiserror::Error)]
#[error("transaction_decode at position {position}, transaction {transaction}: {source}")]
pub struct DecodeError {
    pub position: usize,
    pub transaction: String,
    #[source]
    pub source: cbor::Error,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum IntegrityError {
    #[error("captured_transaction_count, expected {expected}, actual {actual}")]
    Length { expected: usize, actual: usize },
    #[error("captured_transaction_position {position}, expected {expected}, actual {actual}")]
    Position {
        position: usize,
        expected: String,
        actual: String,
    },
    #[error("captured_body_hash at position {position}, transaction {transaction}")]
    Body {
        position: usize,
        transaction: String,
    },
    #[error("decoded_facts_changed at position {position}, transaction {transaction}")]
    Facts {
        position: usize,
        transaction: String,
    },
}

/// Facts have no mutable accessor and no constructor from a supplied fact set.
/// The original capture remains borrowed while these facts are in use.
#[derive(Debug)]
pub struct DecodedTransactions<'a> {
    source: &'a CapturedTransactions,
    transactions: Vec<Transaction<'a>>,
    seals: Vec<[u8; 32]>,
}

impl<'a> DecodedTransactions<'a> {
    pub fn transactions(&self) -> &[Transaction<'a>] {
        &self.transactions
    }
    pub fn len(&self) -> usize {
        self.transactions.len()
    }
    pub fn is_empty(&self) -> bool {
        self.transactions.is_empty()
    }

    pub fn validate(&self) -> Result<(), IntegrityError> {
        if self.seals.len() != self.source.records.len() {
            return Err(IntegrityError::Length {
                expected: self.source.records.len(),
                actual: self.seals.len(),
            });
        }
        if self.transactions.len() != self.source.records.len() {
            return Err(IntegrityError::Length {
                expected: self.source.records.len(),
                actual: self.transactions.len(),
            });
        }
        for (position, (tx, original)) in self
            .transactions
            .iter()
            .zip(&self.source.records)
            .enumerate()
        {
            if tx.hash != original.hash {
                return Err(IntegrityError::Position {
                    position,
                    expected: original.hash.clone(),
                    actual: tx.hash.clone(),
                });
            }
            if transaction::hash(tx.body) != original.hash {
                return Err(IntegrityError::Body {
                    position,
                    transaction: original.hash.clone(),
                });
            }
            if fingerprint(tx) != self.seals[position] {
                return Err(IntegrityError::Facts {
                    position,
                    transaction: original.hash.clone(),
                });
            }
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct OutputFacts<'a> {
    address_bytes_hash: String,
    address: Option<&'a str>,
    coin: u64,
    assets: &'a [Asset],
    data_hash: Option<&'a str>,
    inline_hash: Option<String>,
    classification_safe: bool,
}

fn output_facts(output: &Output) -> OutputFacts<'_> {
    OutputFacts {
        address_bytes_hash: transaction::hash(&output.address_raw),
        address: output.address.as_deref(),
        coin: output.coin,
        assets: &output.assets,
        data_hash: output.data_hash.as_deref(),
        inline_hash: output
            .inline_datum
            .as_ref()
            .map(|bytes| transaction::hash(bytes)),
        classification_safe: output.classification_safe,
    }
}

#[derive(Serialize)]
struct DecodedFacts<'a> {
    valid: bool,
    ordinary: &'a [Outpoint],
    collateral: &'a [Outpoint],
    reference: &'a [Outpoint],
    outputs: Vec<Option<OutputFacts<'a>>>,
    collateral_output: Option<OutputFacts<'a>>,
    has_collateral_return: bool,
    mint: &'a [Asset],
    datum_hashes: Vec<(&'a str, String)>,
    auxiliary_hash: Option<&'a str>,
    auxiliary_bytes_hash: Option<String>,
}

fn fingerprint(tx: &Transaction<'_>) -> [u8; 32] {
    let facts = DecodedFacts {
        valid: tx.valid,
        ordinary: &tx.ordinary,
        collateral: &tx.collateral,
        reference: &tx.reference,
        outputs: tx
            .outputs
            .iter()
            .map(|output| output.as_ref().map(output_facts))
            .collect(),
        collateral_output: tx.collateral_output.as_ref().map(output_facts),
        has_collateral_return: tx.has_collateral_return,
        mint: &tx.mint,
        datum_hashes: tx
            .datums
            .iter()
            .map(|(hash, bytes)| (hash.as_str(), transaction::hash(bytes)))
            .collect(),
        auxiliary_hash: tx.auxiliary_hash.as_deref(),
        auxiliary_bytes_hash: tx.auxiliary.map(transaction::hash),
    };
    // Facts contain only bounded byte hashes, strings, integers and flat lists.
    let encoded = serde_json::to_vec(&facts).expect("decoded facts serialize");
    Sha256::digest(encoded).into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        checks::{self, CheckError, Header},
        profiles::Profile,
        rows::Supplements,
    };
    use std::collections::BTreeMap;
    #[derive(serde::Deserialize)]
    struct Record {
        tx_hash: String,
        cbor: String,
    }
    fn records() -> Vec<(String, Vec<u8>)> {
        let records: Vec<Record> = serde_json::from_str(include_str!(
            "../tests/fixtures/finalized-transactions.json"
        ))
        .unwrap();
        records
            .into_iter()
            .map(|record| (record.tx_hash, hex::decode(record.cbor).unwrap()))
            .collect()
    }
    #[test]
    fn real_decoded_facts_and_item_order_remain_bound_to_the_same_capture() {
        let records = records();
        let source = CapturedTransactions::from_records(records.iter().take(2).cloned());
        let substitute =
            CapturedTransactions::from_records(records.iter().skip(2).take(1).cloned());
        for mutation in [
            "address",
            "datum",
            "validity",
            "order",
            "substitution",
            "drop",
        ] {
            let mut decoded = source.decode().unwrap();
            match mutation {
                "address" => {
                    decoded.transactions[0].outputs[0].as_mut().unwrap().address =
                        Some("changed decoded address".to_owned())
                }
                "datum" => {
                    decoded.transactions[0].outputs[0]
                        .as_mut()
                        .unwrap()
                        .data_hash = Some("00".repeat(32))
                }
                "validity" => decoded.transactions[0].valid = !decoded.transactions[0].valid,
                "order" => decoded.transactions.swap(0, 1),
                "substitution" => {
                    decoded.transactions[1] = transaction::decode(
                        &substitute.records[0].cbor,
                        &substitute.records[0].hash,
                    )
                    .unwrap()
                }
                "drop" => {
                    decoded.transactions.pop();
                }
                _ => unreachable!(),
            }
            let previous = Header {
                height: 9,
                hash: "01".repeat(32),
                previous_hash: None,
                transaction_count: 0,
            };
            let header = Header {
                height: 10,
                hash: "02".repeat(32),
                previous_hash: Some(previous.hash.clone()),
                transaction_count: 2,
            };
            let error = checks::check_block(
                &Profile::load("preprod").unwrap(),
                &previous,
                &header,
                9,
                &decoded,
                &BTreeMap::new(),
                &Supplements::default(),
            )
            .unwrap_err();
            match mutation {
                "address" | "datum" | "validity" => assert!(matches!(
                    error,
                    CheckError::Captured {
                        height: 10,
                        source: IntegrityError::Facts { position: 0, .. }
                    }
                )),
                "order" => assert!(matches!(
                    error,
                    CheckError::Captured {
                        height: 10,
                        source: IntegrityError::Position { position: 0, .. }
                    }
                )),
                "substitution" => assert!(matches!(
                    error,
                    CheckError::Captured {
                        height: 10,
                        source: IntegrityError::Position { position: 1, .. }
                    }
                )),
                "drop" => assert!(matches!(
                    error,
                    CheckError::Captured {
                        height: 10,
                        source: IntegrityError::Length {
                            expected: 2,
                            actual: 1
                        }
                    }
                )),
                _ => unreachable!(),
            }
        }
    }
}
