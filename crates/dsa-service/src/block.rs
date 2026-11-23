//! The final blocks of one update. Their transaction pages are fetched up to
//! `max_in_flight` at a time and consumed in order; each block is decoded
//! once; one held-output lookup serves the whole batch; each block is
//! processed with every supplement it names, and its created and spent
//! outputs are applied before the next block, whose datums emitted by earlier
//! blocks count as stored.

use std::collections::{BTreeMap, BTreeSet};

use dsa_chain::{
    block::{
        BlockError, BlockOutput, DatumAvailability, Header, Held, Outcome, Rows, Selection,
        SupplementRequest, Supplements, inputs, process_block,
    },
    metadata,
    observation::Source,
    transaction::{Outpoint, Transaction, decode_all},
};
use dsa_provider::{
    Provider,
    pages::{BlockPages, BlockTransactions, PAGE_SIZE},
    supplements::resolve_outputs,
    types::{Hash32, Page},
};
use dsa_store::{
    Store,
    follow::{BlockRow, FinalBlock, Observation},
};
use futures_util::StreamExt;
use serde_json::json;

use crate::{
    follow::{Error, Requests, guarded},
    retry::Stop,
};

/// The size of one update while catching up. The batch ends with the block
/// that reaches a limit, so it always holds at least one block.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Limits {
    pub blocks: u32,
    /// Transaction CBOR bytes fetched for one update.
    pub cbor_bytes: usize,
    /// Rows built for one update.
    pub rows: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            blocks: 100,
            cbor_bytes: 8 * 1024 * 1024,
            rows: 50_000,
        }
    }
}

pub(crate) struct Context<'c> {
    pub provider: &'c Provider,
    pub requests: &'c Requests,
    pub selection: &'c Selection<'c>,
    pub store: &'c Store,
    pub stop: &'c Stop,
    pub max_in_flight: usize,
}

#[derive(Debug, Default)]
pub(crate) struct Built {
    pub blocks: Vec<FinalBlock>,
    pub mint_delta: BTreeMap<String, i128>,
    pub observations: Vec<Observation>,
    pub cbor_bytes: usize,
    pub rows: usize,
}

fn hash32(hash: &str) -> Result<Hash32, Error> {
    hash.parse()
        .map_err(|_| Error::answer("invalid_block_hash", json!({"hash":hash})))
}

pub(crate) fn row_count(rows: &Rows) -> usize {
    rows.tx.len()
        + rows.tx_out.len()
        + rows.ma_tx_out.len()
        + rows.tx_in.len()
        + rows.datum.len()
        + rows.tx_metadata.len()
}

/// Builds the final blocks of `finals` (consecutive, in order) until a limit
/// is reached.
pub(crate) async fn build(
    context: &Context<'_>,
    finals: &[BlockRow],
    limits: &Limits,
) -> Result<Built, Error> {
    let fetched = fetch(context, finals, limits.cbor_bytes).await?;
    let finals = &finals[..fetched.len()];
    let decoded = fetched
        .iter()
        .zip(finals)
        .map(|(block, header)| {
            decode_all(block.records()).map_err(|error| {
                Error::answer(
                    error.source.code,
                    json!({"height":header.height,"block":header.hash,"transaction":error.transaction,"position":error.position}),
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let points = decoded
        .iter()
        .flat_map(|transactions| inputs(transactions))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut held = if points.is_empty() {
        BTreeMap::new()
    } else {
        context.store.held_outputs(&points).await?
    };
    let mut emitted = BTreeSet::new();
    let mut seen = BTreeSet::new();
    let mut built = Built {
        cbor_bytes: fetched.iter().map(cbor_bytes).sum(),
        ..Built::default()
    };
    for ((header, transactions), pages) in finals.iter().zip(&decoded).zip(&fetched) {
        let (output, mut answers) = process(context, header, transactions, &held, &emitted).await?;
        for (point, entry) in &output.created {
            held.insert(point.clone(), entry.clone());
        }
        for point in &output.spent {
            held.remove(point);
        }
        emitted.extend(output.rows.datum.iter().map(|row| row.hash.clone()));
        for (unit, delta) in &output.mint_delta {
            *built.mint_delta.entry(unit.clone()).or_default() += delta;
        }
        // The store keeps the first observation of each shape.
        for candidate in &output.observations {
            if !seen.insert(candidate.shape) {
                continue;
            }
            let answers = candidate
                .sources
                .iter()
                .filter_map(|source| {
                    let body = match source {
                        Source::BlockPage(page) => (*page as usize)
                            .checked_sub(1)
                            .and_then(|index| pages.pages.get(index))
                            .map(|body| body.to_vec()),
                        other => answers.remove(other),
                    };
                    body.map(|body| (source.clone(), body))
                })
                .collect();
            built.observations.push(Observation {
                height: header.height,
                candidate: candidate.clone(),
                answers,
            });
        }
        built.rows += row_count(&output.rows);
        built.blocks.push(FinalBlock {
            height: header.height,
            hash: header.hash.clone(),
            transaction_count: header.transaction_count,
            rows: output.rows,
        });
        if built.rows >= limits.rows {
            break;
        }
    }
    Ok(built)
}

fn cbor_bytes(block: &BlockTransactions) -> usize {
    block
        .transactions
        .iter()
        .map(|transaction| transaction.cbor.0.len())
        .sum()
}

/// The transaction lists of `finals` in order, up to and including the block
/// whose CBOR reaches `cbor_limit`.
async fn fetch(
    context: &Context<'_>,
    finals: &[BlockRow],
    cbor_limit: usize,
) -> Result<Vec<BlockTransactions>, Error> {
    let mut wanted = Vec::new();
    for (index, header) in finals.iter().enumerate() {
        let hash = hash32(&header.hash)?;
        for number in 1..=(header.transaction_count as usize).div_ceil(PAGE_SIZE) {
            let page = Page::new(number as u32).map_err(Error::Provider)?;
            wanted.push((index, hash.clone(), page));
        }
    }
    let provider = context.provider;
    let requests = context.requests;
    let mut answers =
        futures_util::stream::iter(wanted.into_iter().map(|(index, hash, page)| async move {
            requests.count("transactions");
            (index, provider.block_transactions(&hash, page).await)
        }))
        .buffered(context.max_in_flight.max(1));
    let mut lists = Vec::new();
    let mut cbor = 0;
    for (index, header) in finals.iter().enumerate() {
        let detail = || json!({"height":header.height,"block":header.hash});
        let mut pages = BlockPages::new(hash32(&header.hash)?, header.transaction_count)
            .map_err(|code| Error::answer(code, detail()))?;
        while pages.next_page().is_some() {
            let answer = tokio::select! {
                answer = answers.next() => answer,
                () = context.stop.wait() => return Err(Error::Stopped),
            };
            let (from, result) =
                answer.ok_or_else(|| Error::answer("provider_page_missing", detail()))?;
            debug_assert_eq!(from, index);
            let page = result
                .map_err(Error::Provider)?
                .ok_or_else(|| Error::answer("provider_block_unknown", detail()))?;
            pages
                .push(page)
                .map_err(|code| Error::answer(code, detail()))?;
        }
        let list = pages
            .finish()
            .map_err(|code| Error::answer(code, detail()))?;
        cbor += cbor_bytes(&list);
        lists.push(list);
        if cbor >= cbor_limit {
            break;
        }
    }
    Ok(lists)
}

/// Processes one block, resolving what it needs, and returns its output and
/// the provider answers of its supplements.
async fn process(
    context: &Context<'_>,
    header: &BlockRow,
    transactions: &[Transaction<'_>],
    held: &BTreeMap<Outpoint, Held>,
    emitted: &BTreeSet<String>,
) -> Result<(BlockOutput, BTreeMap<Source, Vec<u8>>), Error> {
    let block = Header {
        height: header.height,
        hash: header.hash.clone(),
        previous_hash: None,
        transaction_count: header.transaction_count as usize,
    };
    let mut supplements = Supplements::default();
    let mut answers = BTreeMap::new();
    // A resolved output can name further needs (its datum, the ICS address),
    // so a block can need a second round.
    for _ in 0..4 {
        match process_block(context.selection, &block, transactions, held, &supplements) {
            Ok(Outcome::Ready(output)) => return Ok((*output, answers)),
            Ok(Outcome::Needs(needs)) => {
                supply(
                    context,
                    header,
                    transactions,
                    needs,
                    emitted,
                    &mut supplements,
                    &mut answers,
                )
                .await?;
            }
            Err(error) => return Err(block_error(&error)),
        }
    }
    Err(Error::answer(
        "supplements_unresolved",
        json!({"height":header.height,"block":header.hash}),
    ))
}

fn block_error(error: &BlockError) -> Error {
    let detail = json!({"message": error.to_string()});
    match error {
        BlockError::AssetBalance {
            height,
            transaction,
            unit,
            inputs,
            mint,
            outputs,
        } => Error::Check {
            code: "asset_conservation",
            detail: json!({"height":height,"transaction":transaction,"unit":unit,
                "held":inputs.to_string(),"mint":mint.to_string(),"outputs":outputs.to_string()}),
        },
        _ => Error::answer(error.code(), detail),
    }
}

async fn supply(
    context: &Context<'_>,
    header: &BlockRow,
    transactions: &[Transaction<'_>],
    needs: Vec<SupplementRequest>,
    emitted: &BTreeSet<String>,
    supplements: &mut Supplements,
    answers: &mut BTreeMap<Source, Vec<u8>>,
) -> Result<(), Error> {
    let (provider, requests, stop) = (context.provider, context.requests, context.stop);
    let mut datums = Vec::new();
    let mut outputs = BTreeSet::new();
    let mut metadata = Vec::new();
    for need in needs {
        match need {
            SupplementRequest::Datum(hash) => datums.push(hash),
            SupplementRequest::Output { transaction, .. } => {
                outputs.insert(transaction);
            }
            SupplementRequest::Metadata(transaction) => metadata.push(transaction),
        }
    }
    let unknown = datums
        .iter()
        .filter(|hash| !emitted.contains(*hash))
        .cloned()
        .collect::<Vec<_>>();
    let stored = if unknown.is_empty() {
        BTreeSet::new()
    } else {
        context.store.stored_datums(&unknown).await?
    };
    for hash in datums {
        let availability = if emitted.contains(&hash) || stored.contains(&hash) {
            DatumAvailability::Stored
        } else {
            let key = hash32(&hash)?;
            requests.count("datums");
            match guarded(stop, provider.datum(&key)).await? {
                Some(datum) => {
                    answers.insert(Source::Datum(hash.clone()), datum.body.to_vec());
                    DatumAvailability::Present(datum.cbor)
                }
                None => DatumAvailability::NotFound,
            }
        };
        supplements.datums.insert(hash, availability);
    }
    for transaction in metadata {
        let key = hash32(&transaction)?;
        requests.count("metadata");
        let answer = guarded(stop, provider.transaction_metadata(&key)).await?;
        supplements.metadata.insert(
            transaction.clone(),
            answer.label(metadata::LABEL).map(str::to_owned),
        );
        answers.insert(
            Source::TransactionMetadata(transaction),
            answer.body.to_vec(),
        );
    }
    for transaction in outputs {
        let decoded = transactions
            .iter()
            .find(|candidate| candidate.hash() == transaction)
            .ok_or_else(|| {
                Error::answer(
                    "supplement_transaction_unknown",
                    json!({"height":header.height,"transaction":transaction}),
                )
            })?;
        let key = hash32(&transaction)?;
        requests.count("outputs");
        let answer = guarded(stop, provider.transaction_outputs(&key)).await?;
        let resolved = resolve_outputs(decoded, &answer).map_err(|code| {
            Error::answer(
                code,
                json!({"height":header.height,"transaction":transaction}),
            )
        })?;
        if let Some(body) = &answer.body {
            answers.insert(
                Source::TransactionOutputs(transaction.clone()),
                body.to_vec(),
            );
        }
        supplements.outputs.insert(transaction, resolved);
    }
    Ok(())
}
