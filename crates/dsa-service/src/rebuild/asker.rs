//! Provider requests and the list readers of a build.

use std::{collections::BTreeMap, future::Future, sync::Arc, time::Duration};

use dsa_chain::ids;
use dsa_provider::{
    Kind, Provider,
    types::{
        AddressText, Hash32, Heights, ListedTransaction, Page, TransactionInfo, TransactionUtxos,
        Unit,
    },
};
use futures_util::StreamExt;
use serde_json::{Value, json};

use crate::{
    follow::{Error, Requests, guarded},
    retry::{Backoff, Stop, Time, pause_until},
};

/// Passes of a list that has to be read again until two agree.
pub(super) const PASSES: usize = 8;

/// Provider requests with the follower's outcome rules: a refusal of every
/// request is waited out, a transport failure or a refused answer is retried
/// with backoff until it has lasted `fatal_after`, a fatal one ends the build.
#[derive(Clone)]
pub(super) struct Asker {
    pub(super) provider: Provider,
    pub(super) requests: Requests,
    pub(super) time: Arc<dyn Time>,
    pub(super) stop: Stop,
    pub(super) fatal_after: Duration,
}

impl Asker {
    pub(super) async fn ask<T, F, Fut>(&self, family: &'static str, call: F) -> Result<T, Error>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<T, dsa_provider::Error>>,
    {
        let mut backoff = Backoff::default();
        loop {
            self.requests.count(family);
            let error = match guarded(&self.stop, call()).await {
                Ok(value) => return Ok(value),
                Err(Error::Provider(error)) => error,
                Err(other) => return Err(other),
            };
            let now = self.time.unix_seconds();
            let until = match error.kind {
                Kind::Fatal { .. } => return Err(Error::Provider(error)),
                Kind::Wait { until } => {
                    eprintln!(
                        "{}",
                        json!({"event":"rebuild_waiting","error":error.code,"until":until})
                    );
                    until
                }
                Kind::Retry { until } => {
                    let wait = backoff.failed(now).as_secs();
                    let lasted = backoff.lasted(now);
                    if lasted > self.fatal_after.as_secs() {
                        return Err(Error::Lasting {
                            seconds: lasted,
                            error: Box::new(Error::Provider(error)),
                        });
                    }
                    until.max(now + wait)
                }
            };
            if !pause_until(&*self.time, &self.stop, until).await {
                return Err(Error::Stopped);
            }
        }
    }
}

pub(super) fn refusal(code: &'static str, detail: Value) -> Error {
    Error::Refusal { code, detail }
}

pub(super) fn check(code: &'static str, detail: Value) -> Error {
    Error::Check { code, detail }
}

pub(super) fn invalid(error: ids::IdError) -> Error {
    Error::answer(
        "rebuild_position_out_of_range",
        json!({"error": error.to_string()}),
    )
}

pub(super) fn hash32(value: &str) -> Result<Hash32, Error> {
    value
        .parse()
        .map_err(|_| Error::answer("invalid_transaction_hash", json!({"hash": value})))
}

/// An answer the build needs: a 404 is asked again with the outcome rules
/// of every other refused answer, and once that has lasted `fatal_after`
/// the build ends with `provider_transaction_unknown`.
async fn required<T, F, Fut>(
    asker: &Asker,
    family: &'static str,
    transaction: &str,
    call: F,
) -> Result<T, Error>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<Option<T>, dsa_provider::Error>>,
{
    let time = Arc::clone(&asker.time);
    let answer = asker
        .ask(family, || {
            let (answer, time) = (call(), Arc::clone(&time));
            async move {
                answer.await?.ok_or_else(|| dsa_provider::Error {
                    code: "provider_not_found",
                    kind: Kind::Retry {
                        until: time.unix_seconds(),
                    },
                    http_status: Some(404),
                })
            }
        })
        .await;
    match answer {
        Err(Error::Lasting { seconds, error })
            if error.report()["error"] == "provider_not_found" =>
        {
            Err(Error::answer(
                "provider_transaction_unknown",
                json!({"transaction": transaction, "lasted_seconds": seconds}),
            ))
        }
        other => other,
    }
}

/// `/txs/{hash}/utxos` of a transaction the build needs.
pub(super) async fn utxos(asker: &Asker, transaction: &str) -> Result<TransactionUtxos, Error> {
    let key = hash32(transaction)?;
    let provider = asker.provider.clone();
    required(asker, "outputs", transaction, || {
        provider.transaction_utxos(&key)
    })
    .await
}

/// `/txs/{hash}` of a transaction the build needs.
pub(super) async fn info(asker: &Asker, transaction: &str) -> Result<TransactionInfo, Error> {
    let key = hash32(transaction)?;
    let provider = asker.provider.clone();
    required(asker, "transactions", transaction, || {
        provider.transaction(&key)
    })
    .await
}

/// One page of the transaction list of a source, `unit:` or `address:`.
pub(super) async fn transactions_page(
    asker: &Asker,
    source: &str,
    heights: Heights,
    page: Page,
    descending: bool,
) -> Result<Vec<ListedTransaction>, Error> {
    let provider = asker.provider.clone();
    if let Some(unit) = source.strip_prefix("unit:") {
        let unit: Unit = unit.parse().map_err(Error::Provider)?;
        asker
            .ask("listings", || {
                provider.asset_transactions(&unit, heights, page, descending)
            })
            .await
    } else {
        let address: AddressText = source
            .strip_prefix("address:")
            .unwrap_or(source)
            .parse()
            .map_err(Error::Provider)?;
        asker
            .ask("listings", || {
                provider.address_transactions(&address, heights, page, descending)
            })
            .await
    }
}

/// Pages `first`, `first + 1`, … of the list of `source` up to `below`,
/// `in_flight` of them at once (a list below H no longer changes), up to
/// and including the first page with fewer than 100 rows. Returns the pages
/// read and whether the list ended.
pub(super) async fn pages_below(
    asker: &Asker,
    source: &str,
    below: u32,
    first: u32,
    in_flight: usize,
) -> Result<(Vec<Vec<ListedTransaction>>, bool), Error> {
    let heights = Heights {
        from: None,
        to: Some(below),
    };
    let fetches = (first..first + in_flight as u32).map(|number| async move {
        let page = Page::new(number).map_err(Error::Provider)?;
        transactions_page(asker, source, heights, page, false).await
    });
    let mut stream = futures_util::stream::iter(fetches).buffered(in_flight);
    let mut pages = Vec::new();
    while let Some(rows) = stream.next().await {
        let rows = rows?;
        let ended = rows.len() < 100;
        pages.push(rows);
        if ended {
            return Ok((pages, true));
        }
    }
    Ok((pages, false))
}

/// Reads every page of a list (`page` from 1), until a page has fewer than
/// 100 rows.
pub(super) async fn every_page<T, F, Fut>(
    asker: &Asker,
    family: &'static str,
    fetch: F,
) -> Result<Vec<T>, Error>
where
    F: Fn(Page) -> Fut,
    Fut: Future<Output = Result<Vec<T>, dsa_provider::Error>>,
{
    let mut rows = Vec::new();
    let mut page = Page::first();
    loop {
        let answer = asker.ask(family, || fetch(page)).await?;
        let full = answer.len() >= 100;
        rows.extend(answer);
        if !full {
            return Ok(rows);
        }
        page = page
            .next()
            .ok_or_else(|| Error::answer("provider_list_too_long", json!({})))?;
    }
}

/// Reads a list in passes until two consecutive passes agree. A list that
/// keeps moving gives the union of its passes: every row of a pass was
/// unspent when it was read, and a row spent since is found by its spend.
pub(super) async fn stable<T, F, Fut>(
    asker: &Asker,
    family: &'static str,
    key: impl Fn(&T) -> String,
    fetch: F,
) -> Result<Vec<T>, Error>
where
    T: PartialEq + Clone,
    F: Fn(Page) -> Fut,
    Fut: Future<Output = Result<Vec<T>, dsa_provider::Error>>,
{
    let mut last = every_page(asker, family, &fetch).await?;
    let mut union: BTreeMap<String, T> = last.iter().map(|row| (key(row), row.clone())).collect();
    for _ in 1..PASSES {
        let next = every_page(asker, family, &fetch).await?;
        if next == last {
            return Ok(next);
        }
        union.extend(next.iter().map(|row| (key(row), row.clone())));
        last = next;
    }
    eprintln!(
        "{}",
        json!({"event":"rebuild_listing_moving","passes":PASSES,"rows":union.len()})
    );
    Ok(union.into_values().collect())
}
