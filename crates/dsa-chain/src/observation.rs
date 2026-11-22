//! First observations of three stored shapes the recorded corpora did not
//! contain: an output at a pointer address, a datum of more than 10,000 raw
//! bytes and a bridge metadatum. Candidates are derived from a processed
//! block; the follower keeps the first of each shape with the provider
//! answers its sources name.

use crate::{
    block::{Rows, Supplements},
    ids,
    transaction::{Output, Transaction},
};

/// A datum is large above this many raw bytes.
pub const LARGE_DATUM_BYTES: usize = 10_000;

/// Transactions per page of the provider's block transaction list.
const PAGE: usize = 100;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Shape {
    PointerAddress,
    LargeDatum,
    BridgeMetadata,
}

impl Shape {
    pub const ALL: [Self; 3] = [Self::PointerAddress, Self::LargeDatum, Self::BridgeMetadata];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::PointerAddress => "stored_pointer_address",
            Self::LargeDatum => "stored_large_datum",
            Self::BridgeMetadata => "stored_bridge_metadata",
        }
    }
}

/// A provider answer that holds a candidate's bytes.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Source {
    /// This page (from 1) of the block's transaction list.
    BlockPage(u32),
    /// The datum answer for this hash.
    Datum(String),
    /// The output answer for this transaction.
    TransactionOutputs(String),
    /// The metadata answer for this transaction.
    TransactionMetadata(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Candidate {
    pub shape: Shape,
    /// The transaction whose stored row carries the shape; unknown only for a
    /// datum read for a spend and supplied by the provider.
    pub transaction: Option<String>,
    pub output_index: Option<u16>,
    pub datum_hash: Option<String>,
    pub raw_datum_bytes: Option<usize>,
    pub sources: Vec<Source>,
}

fn page(transaction_index: usize) -> Source {
    Source::BlockPage((transaction_index / PAGE + 1) as u32)
}

fn transaction_of<'t, 'a>(
    transactions: &'t [Transaction<'a>],
    transaction_id: i64,
) -> Option<(usize, &'t Transaction<'a>)> {
    let (_, index) = ids::transaction_position(transaction_id).ok()?;
    let index = usize::from(index);
    transactions.get(index).map(|tx| (index, tx))
}

/// The output a row was built from: the body's, or the provider's when the
/// body's could not be rendered.
fn stored_output<'t>(
    tx: &'t Transaction<'_>,
    index: u16,
    supplements: &'t Supplements,
) -> Option<(&'t Output, bool)> {
    let body = if tx.is_valid() {
        tx.outputs()
            .get(usize::from(index))
            .and_then(Option::as_ref)
    } else {
        tx.collateral_return()
    };
    match body.filter(|output| output.address.is_some()) {
        Some(output) => Some((output, false)),
        None => supplements
            .outputs
            .get(tx.hash())
            .and_then(|outputs| outputs.get(&index))
            .map(|output| (output, true)),
    }
}

/// The raw bytes of a stored datum and the answer they came from.
fn datum_bytes<'t>(
    transactions: &'t [Transaction<'_>],
    supplements: &'t Supplements,
    data_hash: &str,
) -> Option<(usize, Source, Option<usize>)> {
    for (index, tx) in transactions.iter().enumerate() {
        let inline = tx
            .outputs()
            .iter()
            .flatten()
            .chain(tx.collateral_return())
            .filter(|output| output.data_hash.as_deref() == Some(data_hash))
            .find_map(|output| output.inline_datum.as_ref().map(Vec::len));
        if let Some(length) = tx.datums().get(data_hash).map(|raw| raw.len()).or(inline) {
            return Some((length, page(index), Some(index)));
        }
    }
    if let Some(crate::block::DatumAvailability::Present(raw)) = supplements.datums.get(data_hash) {
        return Some((raw.len(), Source::Datum(data_hash.to_owned()), None));
    }
    supplements
        .outputs
        .iter()
        .find_map(|(transaction, outputs)| {
            outputs
                .values()
                .filter(|output| output.data_hash.as_deref() == Some(data_hash))
                .find_map(|output| output.inline_datum.as_ref())
                .map(|raw| {
                    let index = transactions.iter().position(|tx| tx.hash() == transaction);
                    (
                        raw.len(),
                        Source::TransactionOutputs(transaction.clone()),
                        index,
                    )
                })
        })
}

/// Every stored shape of the block, in block order within each shape.
pub fn candidates(
    transactions: &[Transaction<'_>],
    supplements: &Supplements,
    rows: &Rows,
) -> Vec<Candidate> {
    let mut found = Vec::new();
    for row in &rows.tx_out {
        let Some((index, tx)) = transaction_of(transactions, row.tx_id) else {
            continue;
        };
        let Some((output, provided)) = stored_output(tx, row.index, supplements) else {
            continue;
        };
        if matches!(
            output.address_raw.first().map(|header| header >> 4),
            Some(4 | 5)
        ) {
            let mut sources = vec![page(index)];
            if provided {
                sources.push(Source::TransactionOutputs(tx.hash().to_owned()));
            }
            found.push(Candidate {
                shape: Shape::PointerAddress,
                transaction: Some(tx.hash().to_owned()),
                output_index: Some(row.index),
                datum_hash: None,
                raw_datum_bytes: None,
                sources,
            });
        }
    }
    for row in &rows.datum {
        let Some((length, source, found_in)) = datum_bytes(transactions, supplements, &row.hash)
        else {
            continue;
        };
        if length <= LARGE_DATUM_BYTES {
            continue;
        }
        let output = rows
            .tx_out
            .iter()
            .find(|output| output.data_hash.as_deref() == Some(row.hash.as_str()));
        let transaction = output
            .and_then(|output| transaction_of(transactions, output.tx_id))
            .map(|(index, _)| index)
            .or(found_in)
            .and_then(|index| transactions.get(index))
            .map(|tx| tx.hash().to_owned());
        found.push(Candidate {
            shape: Shape::LargeDatum,
            transaction,
            output_index: output.map(|output| output.index),
            datum_hash: Some(row.hash.clone()),
            raw_datum_bytes: Some(length),
            sources: vec![source],
        });
    }
    for row in rows.tx_metadata.iter().filter(|row| row.json.is_some()) {
        let Some((index, tx)) = transaction_of(transactions, row.tx_id) else {
            continue;
        };
        found.push(Candidate {
            shape: Shape::BridgeMetadata,
            transaction: Some(tx.hash().to_owned()),
            output_index: None,
            datum_hash: None,
            raw_datum_bytes: None,
            sources: vec![
                page(index),
                Source::TransactionMetadata(tx.hash().to_owned()),
            ],
        });
    }
    found
}
