//! A database built from the provider alone, for horizon H and target T.
//!
//! 1. The unspent outputs created below H that are relevant now: per unit
//!    (the exact eligible units and every unit of the two governance
//!    policies) `/assets/{unit}/utxos` in creation-height ranges answered by
//!    one page each, so that a spend between two pages cannot shift a row out
//!    of the listing; per profile address `/addresses/{address}/utxos`, read
//!    until two passes agree.
//! 2. The quantity of each fixed unit with the provider's tip around it, then
//!    every transaction from H on that moved a unit or touched an address,
//!    with its resolved inputs (`/txs/{hash}/utxos`).
//! 3. Unspent at H: the listed outputs and the relevant inputs of those
//!    transactions, without the outputs they created; each one's creating
//!    transaction placed below H by the full transaction lists (`to` H − 1),
//!    its phase-2 validity, its datum.
//! 4. K5 and K6 from the governance addresses' and the permissioned unit's
//!    lists read backward from H − 1, K7 and K8 from the epochs, the headers
//!    H − 1 … H − 1 + k.
//! 5. Proof: per fixed unit the unspent amount at H equals the provider's
//!    quantity minus the net mint of the transactions from H to the tip the
//!    quantity was read at. Then the base is written with the gate closed.
//! 6. The window H … T through the follower's own updates and checks.
//! 7. K10 when the window holds no spend; publication with `verify`.
//!
//! Every provider result a phase uses is stored in the database being built
//! before the next phase starts, so a stopped build continues where it
//! stopped.

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::DateTime;
use dsa_chain::{
    assets::{AssetRule, fixed_units},
    block::{AssetRow, DatumRow, InputRow, OutputRow, Rows, TxRow},
    ids,
    profiles::{Profile, fixed_descriptor_ids},
    transaction::hash,
};
use dsa_provider::{
    Kind, Provider,
    types::{
        AddressText, AssetAmount, Hash32, Header, Heights, ListedOutput, ListedTransaction, Page,
        TransactionUtxos, Unit,
    },
};
use dsa_store::{
    Store,
    follow::{BlockRow, EpochParamRow},
    rebuild::{Base, Building, BuiltRows},
    verify::{ServingState, Verification},
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tokio::sync::watch;

use crate::{
    epoch,
    follow::{self, Connect, Error, Progress, Requests, Settings, guarded},
    retry::{Backoff, Stop, Time, pause_until},
};

/// Passes of a list that has to be read again until two agree.
const PASSES: usize = 8;
/// Items stored at once.
const BATCH: usize = 200;
/// The progress is recorded at least this often while a phase runs.
const RECORD_INTERVAL: Duration = Duration::from_secs(30);

/// The choices of a build.
#[derive(Clone, Debug, Default)]
pub struct Options {
    pub horizon_block: Option<u32>,
    pub target_block: Option<u32>,
    /// The initial window: H is the newest block at or before the target's
    /// time minus this. The retention when not given.
    pub window_seconds: Option<i64>,
    pub retention_seconds: i64,
}

/// The positions a build works for, fixed when it starts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Plan {
    pub network: String,
    pub horizon: u32,
    pub target: u32,
    pub tip: u32,
    pub window_seconds: i64,
    pub retention_seconds: i64,
}

pub struct Context<'a> {
    pub profile: &'a Profile,
    pub provider: Provider,
    pub connect: Connect,
    /// The follower's settings for the window: limits, in-flight requests
    /// and how long a condition may last.
    pub settings: Settings,
    pub options: Options,
    pub time: Arc<dyn Time>,
    pub stop: Stop,
    pub progress: watch::Sender<Progress>,
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub plan: Plan,
    pub resumed: bool,
    pub verified: Verification,
    /// Provider requests of the whole build, earlier runs included.
    pub calls: i64,
    /// Requests of this run per endpoint family.
    pub requests: BTreeMap<String, u64>,
    pub counts: Value,
    pub seconds: f64,
}

/// The content of an output as a listing or an input row renders it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Content {
    address: String,
    amount: Vec<AssetAmount>,
    data_hash: Option<String>,
    inline_datum: Option<String>,
}

impl Content {
    fn of_listed(row: &ListedOutput) -> Self {
        Self {
            address: row.address.clone(),
            amount: row.amount.clone(),
            data_hash: row.data_hash.as_ref().map(ToString::to_string),
            inline_datum: row.inline_datum.as_ref().map(|bytes| hex::encode(&bytes.0)),
        }
    }

    fn lovelace(&self) -> u64 {
        self.amount
            .iter()
            .find(|amount| amount.unit == "lovelace")
            .map_or(0, |amount| amount.quantity.0)
    }

    /// The native assets in bytewise policy/name order, zero quantities left
    /// out: the order whose rank is an asset row's ordinal.
    fn assets(&self) -> Vec<(String, String, u64)> {
        let mut assets: Vec<(String, String, u64)> = self
            .amount
            .iter()
            .filter(|amount| amount.unit != "lovelace" && amount.quantity.0 > 0)
            .map(|amount| {
                (
                    amount.unit[..56].to_owned(),
                    amount.unit[56..].to_owned(),
                    amount.quantity.0,
                )
            })
            .collect();
        assets.sort();
        assets
    }

    fn quantity(&self, unit: &str) -> u128 {
        self.amount
            .iter()
            .filter(|amount| amount.unit == unit)
            .map(|amount| u128::from(amount.quantity.0))
            .sum()
    }
}

/// An input of a transaction from H on, as stored.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Spent {
    tx_hash: String,
    index: u16,
    collateral: bool,
    reference: bool,
    /// Kept for relevant and collateral inputs only.
    content: Option<Content>,
}

/// An output of a transaction from H on: its eligible amounts only.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Made {
    index: u16,
    collateral: bool,
    amount: Vec<AssetAmount>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Moved {
    unknown: bool,
    inputs: Vec<Spent>,
    outputs: Vec<Made>,
}

/// A position: block height and index in the block.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct Place {
    height: u32,
    index: u16,
}

/// A transaction kept below H by K5 or K6, with the outputs it keeps.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Carried {
    tx: String,
    place: Place,
    valid: bool,
    header: Header,
    outputs: Vec<(u16, Content)>,
}

/// The newest relevant spend below H (K10).
#[derive(Clone, Debug, Serialize, Deserialize)]
struct NewestSpend {
    tx: String,
    place: Place,
    valid: bool,
    ordinal: u16,
    origin: String,
    origin_index: u16,
    origin_place: Place,
    origin_valid: bool,
    content: Content,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Cursor {
    from: u32,
    page: u32,
    done: bool,
}

/// Provider requests with the follower's outcome rules: a refusal of every
/// request is waited out, a transport failure or a refused answer is retried
/// with backoff until it has lasted `fatal_after`, a fatal one ends the build.
#[derive(Clone)]
struct Asker {
    provider: Provider,
    requests: Requests,
    time: Arc<dyn Time>,
    stop: Stop,
    fatal_after: Duration,
}

impl Asker {
    async fn ask<T, F, Fut>(&self, family: &'static str, call: F) -> Result<T, Error>
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

fn refusal(code: &'static str, detail: Value) -> Error {
    Error::Refusal { code, detail }
}

fn check(code: &'static str, detail: Value) -> Error {
    Error::Check { code, detail }
}

fn invalid(error: ids::IdError) -> Error {
    Error::answer(
        "rebuild_position_out_of_range",
        json!({"error": error.to_string()}),
    )
}

fn block_row(header: &Header) -> BlockRow {
    BlockRow {
        height: header.height,
        hash: header.hash.to_string(),
        epoch: header.epoch,
        slot: header.slot,
        time: i64::try_from(header.time)
            .ok()
            .and_then(|seconds| DateTime::from_timestamp(seconds, 0))
            .map_or(chrono::NaiveDateTime::MIN, |time| time.naive_utc()),
        transaction_count: header.tx_count,
    }
}

fn outpoint(hash: &str, index: impl std::fmt::Display) -> String {
    format!("{hash}#{index}")
}

fn to_value(value: impl Serialize) -> Result<Value, Error> {
    serde_json::to_value(value).map_err(|_| refusal("rebuild_progress_invalid", json!({})))
}

fn parse<T: DeserializeOwned>(value: Value) -> Result<T, Error> {
    serde_json::from_value(value).map_err(|_| refusal("rebuild_progress_invalid", json!({})))
}

fn hash32(value: &str) -> Result<Hash32, Error> {
    value
        .parse()
        .map_err(|_| Error::answer("invalid_transaction_hash", json!({"hash": value})))
}

/// One page of the transaction list of a source, `unit:` or `address:`.
async fn transactions_page(
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

/// Reads every page of a list (`page` from 1), until a page has fewer than
/// 100 rows.
async fn every_page<T, F, Fut>(
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
async fn stable<T, F, Fut>(
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

struct Build<'a> {
    profile: &'a Profile,
    rule: AssetRule,
    addresses: BTreeSet<String>,
    permissioned: String,
    asker: Asker,
    max_in_flight: usize,
    store: Option<Store>,
    connect: Arc<Connect>,
    plan: Plan,
    calls_before: i64,
    sent_at_start: i64,
    phase: String,
    /// When the progress was last recorded.
    recorded: Instant,
}

fn sent(provider: &Provider) -> i64 {
    provider.counts().iter().map(|(_, count)| count).sum()
}

const PHASES: [&str; 14] = [
    "positions",
    "listing",
    "quantities",
    "since",
    "transactions",
    "origins",
    "history",
    "validity",
    "datums",
    "carried",
    "epochs",
    "base",
    "window",
    "publish",
];

fn phase_index(phase: &str) -> usize {
    PHASES.iter().position(|p| *p == phase).unwrap_or(0)
}

impl Build<'_> {
    fn calls(&self) -> i64 {
        self.calls_before + (sent(&self.asker.provider) - self.sent_at_start)
    }

    async fn store(&mut self) -> Result<&mut Store, Error> {
        if self.store.is_none() {
            self.store = Some((self.connect)().await?);
        }
        Ok(self.store.as_mut().expect("connected"))
    }

    async fn items(&mut self, kind: &str) -> Result<BTreeMap<String, Value>, Error> {
        Ok(self.store().await?.building_items(kind).await?)
    }

    async fn typed<T: DeserializeOwned>(
        &mut self,
        kind: &str,
    ) -> Result<BTreeMap<String, T>, Error> {
        self.items(kind)
            .await?
            .into_iter()
            .map(|(key, value)| Ok((key, parse(value)?)))
            .collect()
    }

    async fn put(&mut self, kind: &str, items: Vec<(String, Value)>) -> Result<(), Error> {
        for chunk in items.chunks(BATCH) {
            self.store().await?.put_building_items(kind, chunk).await?;
        }
        if self.recorded.elapsed() >= RECORD_INTERVAL {
            let phase = self.phase.clone();
            self.reach(&phase).await?;
        }
        Ok(())
    }

    async fn put_one(&mut self, kind: &str, key: &str, value: impl Serialize) -> Result<(), Error> {
        self.put(kind, vec![(key.to_owned(), to_value(value)?)])
            .await
    }

    /// Records the phase the build reached with its calls so far and the
    /// work it still expects, in its progress row and as the service state
    /// that `status` prints.
    async fn reach(&mut self, phase: &str) -> Result<(), Error> {
        let calls = self.calls();
        let (counts, target) = self.store().await?.building_counts().await?;
        let count = |kind: &str| counts.get(kind).copied().unwrap_or(0);
        let window = target.map_or(self.plan.target - self.plan.horizon + 1, |stored| {
            self.plan
                .target
                .saturating_sub(stored.max(self.plan.horizon - 1))
        });
        let transactions = (count("since") - count("moved")).max(0);
        let detail = json!({"phase": phase, "provider_requests": calls,
            "horizon": self.plan.horizon, "target": self.plan.target,
            "remaining": {"transactions_to_read": transactions, "window_blocks": window},
            "expected_remaining_requests_at_least": transactions + i64::from(window)});
        let store = self.store().await?;
        store.record_building(phase, &detail, calls).await?;
        store
            .record_state("rebuilding", phase, &detail, None)
            .await?;
        self.phase = phase.to_owned();
        self.recorded = Instant::now();
        eprintln!(
            "{}",
            json!({"event":"rebuild_phase","phase":phase,"calls":calls,"detail":detail})
        );
        Ok(())
    }

    fn relevant(&self, content: &Content) -> bool {
        self.addresses.contains(&content.address)
            || content
                .assets()
                .iter()
                .any(|(policy, name, _)| self.rule.allows(policy, name))
    }

    fn needs_datum(&self, content: &Content) -> bool {
        self.addresses.contains(&content.address)
            || content
                .amount
                .iter()
                .any(|amount| amount.unit == self.permissioned && amount.quantity.0 > 0)
    }

    /// The sources of the listings: units and addresses, as `unit:` and
    /// `address:` keys.
    async fn units(&mut self) -> Result<Vec<String>, Error> {
        if self.cursor("units").await?.done {
            return Ok(self.items("unit").await?.into_keys().collect());
        }
        let mut units: BTreeSet<String> = self.rule.exact_units().clone();
        for policy in self.rule.governance_policies().clone() {
            let provider = self.asker.provider.clone();
            let rows = every_page(&self.asker, "listings", |page| {
                let (provider, policy) = (provider.clone(), policy.clone());
                async move { provider.policy_assets(&policy, page).await }
            })
            .await?;
            units.extend(rows.into_iter().map(|row| row.asset));
        }
        let items = units.iter().map(|unit| (unit.clone(), json!({}))).collect();
        self.put("unit", items).await?;
        self.finished("units").await?;
        Ok(units.into_iter().collect())
    }

    // ----- step 1 -----------------------------------------------------------

    async fn listing(&mut self) -> Result<(), Error> {
        for unit in self.units().await? {
            self.list_unit(&unit).await?;
        }
        for address in self.addresses.clone() {
            self.list_address(&address).await?;
        }
        Ok(())
    }

    async fn cursor(&mut self, key: &str) -> Result<Cursor, Error> {
        let stored = self.items("cursor").await?;
        stored
            .get(key)
            .cloned()
            .map_or(Ok(Cursor::default()), parse)
    }

    /// Marks a step whose items are stored in more than one statement as
    /// complete.
    async fn finished(&mut self, key: &str) -> Result<(), Error> {
        self.put_one(
            "cursor",
            key,
            Cursor {
                done: true,
                ..Cursor::default()
            },
        )
        .await
    }

    async fn listed(&mut self, rows: &[ListedOutput]) -> Result<(), Error> {
        let items = rows
            .iter()
            .map(|row| {
                Ok((
                    outpoint(row.tx_hash.as_str(), row.output_index),
                    to_value(row)?,
                ))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        self.put("listed", items).await
    }

    /// The asset's unspent outputs created below H, in height ranges that
    /// each fit one answer.
    async fn list_unit(&mut self, unit: &str) -> Result<(), Error> {
        let key = format!("unit:{unit}");
        let mut cursor = self.cursor(&key).await?;
        let parsed: Unit = unit.parse().map_err(Error::Provider)?;
        let below = self.plan.horizon - 1;
        while !cursor.done {
            let from = cursor.from;
            if from > below {
                cursor.done = true;
                self.put_one("cursor", &key, &cursor).await?;
                break;
            }
            let provider = self.asker.provider.clone();
            let range = Heights {
                from: Some(from),
                to: Some(below),
            };
            let rows = self
                .asker
                .ask("listings", || {
                    provider.asset_outputs(&parsed, range, Page::first())
                })
                .await?;
            let (taken, next) = if rows.len() < 100 {
                (rows, None)
            } else {
                let last = rows[99].block_height.unwrap_or(from);
                if last > from {
                    let taken = rows
                        .into_iter()
                        .filter(|row| row.block_height.is_some_and(|height| height < last))
                        .collect();
                    (taken, Some(last))
                } else {
                    (self.outputs_at_height(&parsed, from).await?, Some(from + 1))
                }
            };
            self.listed(&taken).await?;
            match next {
                Some(next) => cursor.from = next,
                None => cursor.done = true,
            }
            self.put_one("cursor", &key, &cursor).await?;
        }
        Ok(())
    }

    /// The unspent outputs of `unit` created at one height that holds 100
    /// or more of them: the unit's transactions at that height (final: below
    /// H) and the outputs each one created that no transaction has spent.
    /// No page of a moving list is involved.
    async fn outputs_at_height(
        &self,
        unit: &Unit,
        height: u32,
    ) -> Result<Vec<ListedOutput>, Error> {
        let at = Heights {
            from: Some(height),
            to: Some(height),
        };
        let provider = self.asker.provider.clone();
        let transactions = every_page(&self.asker, "listings", |page| {
            let (provider, unit) = (provider.clone(), unit.clone());
            async move { provider.asset_transactions(&unit, at, page, false).await }
        })
        .await?;
        let mut rows = Vec::new();
        for listed in transactions {
            let key = listed.tx_hash.clone();
            let answer = self
                .asker
                .ask("outputs", || provider.transaction_utxos(&key))
                .await?
                .ok_or_else(|| {
                    Error::answer("provider_transaction_unknown", json!({"transaction": key}))
                })?;
            let info = self
                .asker
                .ask("transactions", || provider.transaction(&key))
                .await?
                .ok_or_else(|| {
                    Error::answer("provider_transaction_unknown", json!({"transaction": key}))
                })?;
            for output in answer.outputs {
                let holds = output
                    .amount
                    .iter()
                    .any(|amount| amount.unit == unit.as_str() && amount.quantity.0 > 0);
                if output.collateral == info.valid_contract
                    || output.consumed_by_tx.is_some()
                    || !holds
                {
                    continue;
                }
                rows.push(ListedOutput {
                    address: output.address,
                    tx_hash: listed.tx_hash.clone(),
                    output_index: output.output_index,
                    amount: output.amount,
                    block_height: Some(height),
                    data_hash: output.data_hash,
                    inline_datum: output.inline_datum,
                });
            }
        }
        Ok(rows)
    }

    async fn list_address(&mut self, address: &str) -> Result<(), Error> {
        let key = format!("address:{address}");
        if self.cursor(&key).await?.done {
            return Ok(());
        }
        let parsed: AddressText = address.parse().map_err(Error::Provider)?;
        let provider = self.asker.provider.clone();
        let rows = stable(
            &self.asker,
            "listings",
            |row: &ListedOutput| outpoint(row.tx_hash.as_str(), row.output_index),
            |page| {
                let (provider, parsed) = (provider.clone(), parsed.clone());
                async move { provider.address_outputs(&parsed, page).await }
            },
        )
        .await?;
        self.listed(&rows).await?;
        self.put_one(
            "cursor",
            &key,
            Cursor {
                done: true,
                ..Cursor::default()
            },
        )
        .await
    }

    // ----- step 2 -----------------------------------------------------------

    /// The quantity of each fixed unit and the provider's tip before and
    /// after it, with the height of the unit's first mint.
    async fn quantities(&mut self) -> Result<(), Error> {
        if !self.items("quantity").await?.is_empty() {
            return Ok(());
        }
        let units = fixed_units(self.profile);
        let provider = self.asker.provider.clone();
        let mut last = None;
        for _ in 0..10 {
            let before = self
                .asker
                .ask("headers", || provider.latest_block())
                .await?;
            let mut answers = BTreeMap::new();
            for unit in &units {
                let parsed: Unit = unit.parse().map_err(Error::Provider)?;
                let answer = self.asker.ask("assets", || provider.asset(&parsed)).await?;
                answers.insert(unit.clone(), answer);
            }
            let after = self
                .asker
                .ask("headers", || provider.latest_block())
                .await?;
            let steady = before.hash == after.hash;
            last = Some((before, after, answers));
            if steady {
                break;
            }
        }
        let (before, after, answers) = last.expect("one reading");
        let mut items = Vec::new();
        for (unit, answer) in answers {
            let first_mint = match &answer {
                Some(info) => {
                    let mint = info.initial_mint_tx_hash.clone();
                    self.asker
                        .ask("transactions", || provider.transaction(&mint))
                        .await?
                        .map(|tx| tx.block_height)
                }
                None => None,
            };
            items.push((
                unit,
                json!({"quantity": answer.as_ref().map(|info| info.quantity.clone()),
                    "mints": answer.as_ref().map(|info| info.mint_or_burn_count),
                    "first_mint_height": first_mint, "tip_before": before.height,
                    "tip_after": after.height}),
            ));
        }
        self.put("quantity", items).await
    }

    /// Every transaction from H on of each unit and address, in height
    /// ranges that each fit one answer.
    async fn since(&mut self) -> Result<(), Error> {
        let mut sources: Vec<String> = self
            .units()
            .await?
            .into_iter()
            .map(|unit| format!("unit:{unit}"))
            .collect();
        sources.extend(self.addresses.iter().map(|a| format!("address:{a}")));
        for source in sources {
            let key = format!("since:{source}");
            let mut cursor = self.cursor(&key).await?;
            if cursor.from < self.plan.horizon {
                cursor.from = self.plan.horizon;
            }
            while !cursor.done {
                let from = cursor.from;
                let rows = self
                    .transactions_page(
                        &source,
                        Heights {
                            from: Some(from),
                            to: None,
                        },
                        Page::first(),
                        false,
                    )
                    .await?;
                let (taken, next) = if rows.len() < 100 {
                    (rows, None)
                } else {
                    let last = rows[99].block_height;
                    if last > from {
                        (
                            rows.into_iter()
                                .filter(|row| row.block_height < last)
                                .collect(),
                            Some(last),
                        )
                    } else {
                        let one = Heights {
                            from: Some(from),
                            to: Some(from),
                        };
                        let mut last_pass: Option<Vec<ListedTransaction>> = None;
                        let mut union: BTreeMap<String, ListedTransaction> = BTreeMap::new();
                        let mut agreed = None;
                        for _ in 0..PASSES {
                            let mut pass = Vec::new();
                            let mut page = Page::first();
                            loop {
                                let rows =
                                    self.transactions_page(&source, one, page, false).await?;
                                let full = rows.len() >= 100;
                                pass.extend(rows);
                                if !full {
                                    break;
                                }
                                page = page.next().ok_or_else(|| {
                                    Error::answer("provider_list_too_long", json!({}))
                                })?;
                            }
                            if last_pass.as_ref() == Some(&pass) {
                                agreed = Some(pass);
                                break;
                            }
                            union.extend(
                                pass.iter()
                                    .map(|row| (row.tx_hash.to_string(), row.clone())),
                            );
                            last_pass = Some(pass);
                        }
                        let pass = agreed.unwrap_or_else(|| union.into_values().collect());
                        (pass, Some(from + 1))
                    }
                };
                let items = taken
                    .iter()
                    .map(|row| {
                        (
                            row.tx_hash.to_string(),
                            json!({"height": row.block_height, "index": row.tx_index}),
                        )
                    })
                    .collect();
                self.put("since", items).await?;
                match next {
                    Some(next) => cursor.from = next,
                    None => cursor.done = true,
                }
                self.put_one("cursor", &key, &cursor).await?;
            }
        }
        Ok(())
    }

    async fn transactions_page(
        &self,
        source: &str,
        heights: Heights,
        page: Page,
        descending: bool,
    ) -> Result<Vec<ListedTransaction>, Error> {
        transactions_page(&self.asker, source, heights, page, descending).await
    }

    /// `/txs/{hash}/utxos` of each transaction from H on.
    async fn transactions(&mut self) -> Result<(), Error> {
        let since = self.items("since").await?;
        let done = self.items("moved").await?;
        let missing: Vec<String> = since
            .into_keys()
            .filter(|hash| !done.contains_key(hash))
            .collect();
        let provider = self.asker.provider.clone();
        let asker = self.asker.clone();
        let mut answers = futures_util::stream::iter(missing.into_iter().map(|transaction| {
            let (provider, asker) = (provider.clone(), asker.clone());
            async move {
                let key = hash32(&transaction)?;
                let answer = asker
                    .ask("outputs", || provider.transaction_utxos(&key))
                    .await?;
                Ok::<_, Error>((transaction, answer))
            }
        }))
        .buffered(self.max_in_flight);
        let mut batch = Vec::new();
        while let Some(result) = answers.next().await {
            let (transaction, answer) = result?;
            let moved = self.moved(answer);
            batch.push((transaction, to_value(moved)?));
            if batch.len() >= BATCH {
                self.put("moved", std::mem::take(&mut batch)).await?;
            }
        }
        self.put("moved", batch).await
    }

    fn moved(&self, answer: Option<TransactionUtxos>) -> Moved {
        let Some(answer) = answer else {
            return Moved {
                unknown: true,
                ..Moved::default()
            };
        };
        let inputs = answer
            .inputs
            .iter()
            .map(|row| {
                let content = Content {
                    address: row.address.clone(),
                    amount: row.amount.clone(),
                    data_hash: row.data_hash.as_ref().map(ToString::to_string),
                    inline_datum: row.inline_datum.as_ref().map(|bytes| hex::encode(&bytes.0)),
                };
                let keep = row.collateral || self.relevant(&content);
                Spent {
                    tx_hash: row.tx_hash.to_string(),
                    index: u16::try_from(row.output_index).unwrap_or(u16::MAX),
                    collateral: row.collateral,
                    reference: row.reference,
                    content: keep.then_some(content),
                }
            })
            .collect();
        let outputs = answer
            .outputs
            .iter()
            .map(|row| Made {
                index: u16::try_from(row.output_index).unwrap_or(u16::MAX),
                collateral: row.collateral,
                amount: row
                    .amount
                    .iter()
                    .filter(|amount| {
                        amount.unit != "lovelace"
                            && self.rule.allows(&amount.unit[..56], &amount.unit[56..])
                    })
                    .cloned()
                    .collect(),
            })
            .collect();
        Moved {
            unknown: false,
            inputs,
            outputs,
        }
    }

    // ----- step 3 -----------------------------------------------------------

    /// The original content of every collateral input from H on whose
    /// output is not listed: a collateral row may omit native assets.
    async fn origins(&mut self) -> Result<(), Error> {
        let since = self.items("since").await?;
        let listed = self.items("listed").await?;
        let known = self.items("origin").await?;
        let moved: BTreeMap<String, Moved> = self.typed("moved").await?;
        let wanted: BTreeSet<(String, u16)> = moved
            .values()
            .flat_map(|moved| moved.inputs.iter())
            .filter(|input| {
                input.collateral
                    && !input.reference
                    && !since.contains_key(&input.tx_hash)
                    && !listed.contains_key(&outpoint(&input.tx_hash, input.index))
                    && !known.contains_key(&outpoint(&input.tx_hash, input.index))
            })
            .map(|input| (input.tx_hash.clone(), input.index))
            .collect();
        let provider = self.asker.provider.clone();
        for (transaction, index) in wanted {
            let key = hash32(&transaction)?;
            let answer = self
                .asker
                .ask("outputs", || provider.transaction_utxos(&key))
                .await?
                .ok_or_else(|| {
                    Error::answer(
                        "provider_origin_unknown",
                        json!({"transaction": transaction}),
                    )
                })?;
            let row = answer
                .outputs
                .iter()
                .find(|row| row.output_index == u32::from(index))
                .ok_or_else(|| {
                    Error::answer(
                        "provider_origin_unknown",
                        json!({"transaction": transaction, "index": index}),
                    )
                })?;
            let content = Content {
                address: row.address.clone(),
                amount: row.amount.clone(),
                data_hash: row.data_hash.as_ref().map(ToString::to_string),
                inline_datum: row.inline_datum.as_ref().map(|bytes| hex::encode(&bytes.0)),
            };
            self.put_one("origin", &outpoint(&transaction, index), content)
                .await?;
        }
        Ok(())
    }

    /// The relevant outputs unspent at H, before their creating transactions
    /// are placed: listed outputs and relevant inputs from H on, without the
    /// outputs of transactions from H on.
    async fn candidates(&mut self) -> Result<BTreeMap<(String, u16), Content>, Error> {
        let since = self.items("since").await?;
        let listed: BTreeMap<String, ListedOutput> = self.typed("listed").await?;
        let origins: BTreeMap<String, Content> = self.typed("origin").await?;
        let moved: BTreeMap<String, Moved> = self.typed("moved").await?;
        let mut candidates = BTreeMap::new();
        for row in listed.values() {
            let transaction = row.tx_hash.to_string();
            if since.contains_key(&transaction)
                || row
                    .block_height
                    .is_some_and(|height| height >= self.plan.horizon)
            {
                continue;
            }
            let content = Content::of_listed(row);
            if self.relevant(&content) {
                let index = u16::try_from(row.output_index)
                    .map_err(|_| Error::answer("provider_output_index", json!({})))?;
                candidates.insert((transaction, index), content);
            }
        }
        for input in moved.values().flat_map(|moved| moved.inputs.iter()) {
            if input.reference || since.contains_key(&input.tx_hash) {
                continue;
            }
            let point = (input.tx_hash.clone(), input.index);
            if candidates.contains_key(&point) {
                continue;
            }
            let content = if input.collateral {
                origins.get(&outpoint(&input.tx_hash, input.index)).cloned()
            } else {
                input.content.clone()
            };
            if let Some(content) = content.filter(|content| self.relevant(content)) {
                candidates.insert(point, content);
            }
        }
        Ok(candidates)
    }

    /// The sources whose full list names the creating transaction of a
    /// candidate: its address when it is a profile address, every eligible
    /// unit it holds.
    fn sources_of(&self, content: &Content) -> Vec<String> {
        let mut sources = Vec::new();
        if self.addresses.contains(&content.address) {
            sources.push(format!("address:{}", content.address));
        }
        for (policy, name, _) in content.assets() {
            if self.rule.allows(&policy, &name) {
                sources.push(format!("unit:{policy}{name}"));
            }
        }
        sources
    }

    /// Places the creating transaction of every candidate from the full
    /// lists up to H − 1: the addresses first, then the units with the
    /// fewest transactions to place.
    async fn history(&mut self) -> Result<(), Error> {
        let candidates = self.candidates().await?;
        let mut needed: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for ((transaction, _), content) in &candidates {
            for source in self.sources_of(content) {
                needed
                    .entry(source)
                    .or_default()
                    .insert(transaction.clone());
            }
        }
        let mut order: Vec<(String, BTreeSet<String>)> = needed.into_iter().collect();
        order.sort_by_key(|(source, wanted)| (!source.starts_with("address:"), wanted.len()));
        for (source, wanted) in order {
            let key = format!("history:{source}");
            let mut cursor = self.cursor(&key).await?;
            if cursor.done {
                continue;
            }
            let placed = self.items("position").await?;
            if wanted
                .iter()
                .all(|transaction| placed.contains_key(transaction))
            {
                continue;
            }
            let below = Heights {
                from: None,
                to: Some(self.plan.horizon - 1),
            };
            let mut page = cursor.page.max(1);
            loop {
                // Pages ahead are read in parallel: the list below H no
                // longer changes.
                let numbers: Vec<u32> = (page..page + self.max_in_flight as u32).collect();
                let mut answers = Vec::new();
                {
                    let asker = &self.asker;
                    let fetches = numbers.iter().map(|number| {
                        let source = source.clone();
                        async move {
                            let page = Page::new(*number).map_err(Error::Provider)?;
                            transactions_page(asker, &source, below, page, false).await
                        }
                    });
                    let mut stream =
                        futures_util::stream::iter(fetches).buffered(self.max_in_flight);
                    while let Some(answer) = stream.next().await {
                        answers.push(answer?);
                    }
                }
                let mut finished = false;
                let mut items = Vec::new();
                for rows in answers {
                    for row in &rows {
                        let transaction = row.tx_hash.to_string();
                        if wanted.contains(&transaction) {
                            items.push((
                                transaction,
                                json!({"height": row.block_height, "index": row.tx_index}),
                            ));
                        }
                    }
                    page += 1;
                    if rows.len() < 100 {
                        finished = true;
                        break;
                    }
                }
                self.put("position", items).await?;
                cursor.page = page;
                cursor.done = finished;
                self.put_one("cursor", &key, &cursor).await?;
                if finished {
                    break;
                }
            }
        }
        Ok(())
    }

    /// The candidates whose creating transaction is placed below H.
    async fn unspent_at_horizon(
        &mut self,
    ) -> Result<(BTreeMap<(String, u16), (Content, Place)>, usize), Error> {
        let candidates = self.candidates().await?;
        let placed: BTreeMap<String, Place> = self.typed("position").await?;
        let mut unspent = BTreeMap::new();
        let mut unplaced = 0;
        for (point, content) in candidates {
            match placed.get(&point.0) {
                Some(place) if place.height < self.plan.horizon => {
                    unspent.insert(point, (content, *place));
                }
                _ => unplaced += 1,
            }
        }
        Ok((unspent, unplaced))
    }

    /// Phase-2 validity of each creating transaction below H: a transaction
    /// with two known outputs is valid (a failing one creates one output);
    /// any other is read.
    async fn validity(&mut self) -> Result<(), Error> {
        let (unspent, _) = self.unspent_at_horizon().await?;
        let mut outputs: BTreeMap<String, usize> = BTreeMap::new();
        for (transaction, _) in unspent.keys() {
            *outputs.entry(transaction.clone()).or_default() += 1;
        }
        let known = self.items("valid").await?;
        let mut settled = Vec::new();
        let mut wanted = Vec::new();
        for (transaction, count) in outputs {
            if known.contains_key(&transaction) {
                continue;
            }
            if count >= 2 {
                settled.push((transaction, json!(true)));
            } else {
                wanted.push(transaction);
            }
        }
        self.put("valid", settled).await?;
        let placed: BTreeMap<String, Place> = self.typed("position").await?;
        let provider = self.asker.provider.clone();
        let asker = self.asker.clone();
        let mut answers = futures_util::stream::iter(wanted.into_iter().map(|transaction| {
            let (provider, asker) = (provider.clone(), asker.clone());
            async move {
                let key = hash32(&transaction)?;
                let answer = asker
                    .ask("transactions", || provider.transaction(&key))
                    .await?;
                Ok::<_, Error>((transaction, answer))
            }
        }))
        .buffered(self.max_in_flight);
        let mut batch = Vec::new();
        while let Some(result) = answers.next().await {
            let (transaction, answer) = result?;
            let info = answer.ok_or_else(|| {
                Error::answer(
                    "provider_transaction_unknown",
                    json!({"transaction": transaction}),
                )
            })?;
            let place = placed[&transaction];
            if info.block_height != place.height || info.index != u32::from(place.index) {
                return Err(Error::answer(
                    "provider_position_mismatch",
                    json!({"transaction": transaction, "list": [place.height, place.index],
                        "transaction_answer": [info.block_height, info.index]}),
                ));
            }
            batch.push((transaction, json!(info.valid_contract)));
            if batch.len() >= BATCH {
                self.put("valid", std::mem::take(&mut batch)).await?;
            }
        }
        self.put("valid", batch).await
    }

    /// The datum of each kept output the node reads one for, unless inline.
    async fn datums(&mut self) -> Result<(), Error> {
        let (unspent, _) = self.unspent_at_horizon().await?;
        let mut wanted: BTreeSet<String> = unspent
            .values()
            .filter(|(content, _)| self.needs_datum(content) && content.inline_datum.is_none())
            .filter_map(|(content, _)| content.data_hash.clone())
            .collect();
        let carried: BTreeMap<String, Vec<Carried>> = self.typed("carried").await?;
        for carry in carried.values().flatten() {
            for (_, content) in &carry.outputs {
                if self.needs_datum(content) && content.inline_datum.is_none() {
                    wanted.extend(content.data_hash.clone());
                }
            }
        }
        self.fetch_datums(wanted).await
    }

    async fn fetch_datums(&mut self, wanted: BTreeSet<String>) -> Result<(), Error> {
        let known = self.items("datum").await?;
        let provider = self.asker.provider.clone();
        let asker = self.asker.clone();
        let wanted: Vec<String> = wanted
            .into_iter()
            .filter(|hash| !known.contains_key(hash))
            .collect();
        let mut answers = futures_util::stream::iter(wanted.into_iter().map(|datum| {
            let (provider, asker) = (provider.clone(), asker.clone());
            async move {
                let key = hash32(&datum)?;
                let answer = asker.ask("datums", || provider.datum(&key)).await?;
                Ok::<_, Error>((datum, answer))
            }
        }))
        .buffered(self.max_in_flight);
        let mut batch = Vec::new();
        while let Some(result) = answers.next().await {
            let (datum, answer) = result?;
            batch.push((
                datum,
                json!({"cbor": answer.map(|answer| hex::encode(answer.cbor))}),
            ));
        }
        self.put("datum", batch).await
    }

    /// The datum bytes of a content, inline or from the provider, checked
    /// against its hash.
    fn datum_bytes(
        content: &Content,
        datums: &BTreeMap<String, Value>,
    ) -> Result<Option<Vec<u8>>, Error> {
        let Some(data_hash) = &content.data_hash else {
            return Ok(None);
        };
        let raw = match &content.inline_datum {
            Some(inline) => Some(inline.clone()),
            None => datums
                .get(data_hash)
                .and_then(|value| value["cbor"].as_str().map(str::to_owned)),
        };
        let Some(raw) = raw else {
            return Ok(None);
        };
        let bytes = hex::decode(&raw)
            .map_err(|_| Error::answer("provider_datum_invalid", json!({"hash": data_hash})))?;
        if hash(&bytes) != *data_hash {
            return Err(Error::answer(
                "provider_datum_hash_mismatch",
                json!({"hash": data_hash}),
            ));
        }
        Ok(Some(bytes))
    }

    // ----- step 4 -----------------------------------------------------------

    /// The outputs `transaction` creates, per its validity, from its
    /// `/txs/{hash}/utxos` answer.
    async fn created(&self, transaction: &str) -> Result<(bool, Vec<(u16, Content)>), Error> {
        let key = hash32(transaction)?;
        let provider = self.asker.provider.clone();
        let answer = self
            .asker
            .ask("outputs", || provider.transaction_utxos(&key))
            .await?
            .ok_or_else(|| {
                Error::answer(
                    "provider_transaction_unknown",
                    json!({"transaction": transaction}),
                )
            })?;
        let info = self
            .asker
            .ask("transactions", || provider.transaction(&key))
            .await?
            .ok_or_else(|| {
                Error::answer(
                    "provider_transaction_unknown",
                    json!({"transaction": transaction}),
                )
            })?;
        let outputs = answer
            .outputs
            .iter()
            .filter(|row| row.collateral != info.valid_contract)
            .map(|row| {
                (
                    u16::try_from(row.output_index).unwrap_or(u16::MAX),
                    Content {
                        address: row.address.clone(),
                        amount: row.amount.clone(),
                        data_hash: row.data_hash.as_ref().map(ToString::to_string),
                        inline_datum: row.inline_datum.as_ref().map(|bytes| hex::encode(&bytes.0)),
                    },
                )
            })
            .collect();
        Ok((info.valid_contract, outputs))
    }

    async fn header_at(&self, height: u32) -> Result<Header, Error> {
        let provider = self.asker.provider.clone();
        self.asker
            .ask("headers", || provider.block_at(height))
            .await?
            .ok_or_else(|| Error::answer("provider_block_missing", json!({"height": height})))
    }

    /// K5 per governance body and K6: the transactions kept below H.
    async fn carried(&mut self) -> Result<(), Error> {
        let known = self.items("carried").await?;
        let horizon_epoch = self.header_at(self.plan.horizon).await?.epoch;
        for body in self.profile.governance() {
            if known.contains_key(body.name) {
                continue;
            }
            let source = format!("address:{}", body.address);
            let policy = body.asset.policy.clone();
            let address = body.address.to_owned();
            let mut found = Vec::new();
            let mut page = Page::first();
            'pages: loop {
                let rows = self
                    .transactions_page(
                        &source,
                        Heights {
                            from: None,
                            to: Some(self.plan.horizon - 1),
                        },
                        page,
                        true,
                    )
                    .await?;
                for row in &rows {
                    let transaction = row.tx_hash.to_string();
                    let (valid, outputs) = self.created(&transaction).await?;
                    let mut kept = Vec::new();
                    for (index, content) in outputs {
                        let holds = content.amount.iter().any(|amount| {
                            amount.unit != "lovelace"
                                && amount.unit.starts_with(&policy)
                                && amount.quantity.0 > 0
                        });
                        if content.address != address || !holds || content.data_hash.is_none() {
                            continue;
                        }
                        if content.inline_datum.is_none() {
                            let wanted =
                                BTreeSet::from([content.data_hash.clone().unwrap_or_default()]);
                            self.fetch_datums(wanted).await?;
                        }
                        let datums = self.items("datum").await?;
                        if Self::datum_bytes(&content, &datums)?.is_some() {
                            kept.push((index, content));
                        }
                    }
                    if !kept.is_empty() {
                        found.push(Carried {
                            tx: transaction,
                            place: Place {
                                height: row.block_height,
                                index: row.tx_index as u16,
                            },
                            valid,
                            header: self.header_at(row.block_height).await?,
                            outputs: kept,
                        });
                        break 'pages;
                    }
                }
                if rows.len() < 100 {
                    break;
                }
                page = page
                    .next()
                    .ok_or_else(|| Error::answer("provider_list_too_long", json!({})))?;
            }
            self.put_one("carried", body.name, &found).await?;
        }
        if !known.contains_key("permissioned_candidates") {
            let unit = self.permissioned.clone();
            let source = format!("unit:{unit}");
            let mut found = Vec::new();
            let mut page = Page::first();
            'listing: loop {
                let rows = self
                    .transactions_page(
                        &source,
                        Heights {
                            from: None,
                            to: Some(self.plan.horizon - 1),
                        },
                        page,
                        true,
                    )
                    .await?;
                for row in &rows {
                    let transaction = row.tx_hash.to_string();
                    let (valid, outputs) = self.created(&transaction).await?;
                    let kept: Vec<(u16, Content)> = outputs
                        .into_iter()
                        .filter(|(_, content)| content.quantity(&unit) > 0)
                        .collect();
                    if kept.is_empty() {
                        continue;
                    }
                    let header = self.header_at(row.block_height).await?;
                    let last = i64::from(header.epoch) <= i64::from(horizon_epoch) - 2;
                    found.push(Carried {
                        tx: transaction,
                        place: Place {
                            height: row.block_height,
                            index: row.tx_index as u16,
                        },
                        valid,
                        header,
                        outputs: kept,
                    });
                    if last {
                        break 'listing;
                    }
                }
                if rows.len() < 100 {
                    break;
                }
                page = page
                    .next()
                    .ok_or_else(|| Error::answer("provider_list_too_long", json!({})))?;
            }
            self.put_one("carried", "permissioned_candidates", &found)
                .await?;
        }
        Ok(())
    }

    /// K7 headers, K8 parameters below the window, the headers H − 1 …
    /// H − 1 + k and the target's header.
    async fn epochs(&mut self) -> Result<(), Error> {
        if self.cursor("epochs").await?.done {
            return Ok(());
        }
        let provider = self.asker.provider.clone();
        let horizon = self.header_at(self.plan.horizon).await?;
        let mut headers = vec![
            ("horizon".to_owned(), to_value(&horizon)?),
            (
                "target".to_owned(),
                to_value(self.header_at(self.plan.target).await?)?,
            ),
        ];
        for back in [2_u32, 1] {
            let Some(cutoff) = horizon.epoch.checked_sub(back) else {
                continue;
            };
            let mut epoch_number = cutoff;
            loop {
                let last = self
                    .asker
                    .ask("epochs", || provider.epoch_last_block(epoch_number))
                    .await?;
                if let Some(block) = last {
                    let header = self
                        .asker
                        .ask("headers", || provider.block_by_hash(&block))
                        .await?
                        .ok_or_else(|| {
                            Error::answer("provider_block_missing", json!({"block": block}))
                        })?;
                    headers.push((format!("epoch:{cutoff}"), to_value(header)?));
                    break;
                }
                match epoch_number.checked_sub(1) {
                    Some(earlier) if cutoff - earlier < 16 => epoch_number = earlier,
                    _ => break,
                }
            }
            let requests = self.asker.requests.clone();
            let params = self
                .asker
                .ask("epochs", || epoch::parameters(&provider, &requests, cutoff))
                .await?;
            if let Some(params) = params {
                self.put_one(
                    "param",
                    &cutoff.to_string(),
                    json!({"nonce": params.nonce, "max_tx_size": params.max_tx_size}),
                )
                .await?;
            }
        }
        // H − 1 and the k headers after it, linked.
        let k = self.profile.cardano.security_parameter;
        let first = self.header_at(self.plan.horizon - 1).await?;
        let mut chain = vec![first.clone()];
        let mut page = Page::first();
        while chain.len() < k as usize + 1 {
            let rows = self
                .asker
                .ask("headers", || provider.next_blocks(&first.hash, page))
                .await?
                .ok_or_else(|| Error::answer("provider_fork_point_unknown", json!({})))?;
            if rows.is_empty() {
                return Err(Error::answer("provider_chain_shorter", json!({})));
            }
            for row in rows {
                let previous = chain.last().expect("first");
                if row.height != previous.height + 1
                    || row.previous_block.as_ref() != Some(&previous.hash)
                {
                    return Err(Error::answer(
                        "provider_header_link_mismatch",
                        json!({"height": row.height}),
                    ));
                }
                chain.push(row);
                if chain.len() == k as usize + 1 {
                    break;
                }
            }
            page = page
                .next()
                .ok_or_else(|| Error::answer("provider_list_too_long", json!({})))?;
        }
        for header in chain {
            headers.push((format!("chain:{}", header.height), to_value(header)?));
        }
        self.put("header", headers).await?;
        self.finished("epochs").await
    }

    // ----- step 5 -----------------------------------------------------------

    /// The rows of one kept output below H with its creating transaction.
    fn output_rows(
        &self,
        rows: &mut Rows,
        point: &(String, u16),
        content: &Content,
        place: Place,
        valid: bool,
        datums: &BTreeMap<String, Value>,
    ) -> Result<(), Error> {
        let tx_id = ids::transaction_id(place.height, place.index).map_err(invalid)?;
        if !rows.tx.iter().any(|row| row.id == tx_id) {
            rows.tx.push(TxRow {
                id: tx_id,
                hash: point.0.clone(),
                block_id: i64::from(place.height),
                block_index: place.index,
                valid_contract: valid,
            });
        }
        let output_id = ids::output_id(place.height, place.index, point.1).map_err(invalid)?;
        if rows.tx_out.iter().any(|row| row.id == output_id) {
            return Ok(());
        }
        rows.tx_out.push(OutputRow {
            id: output_id,
            tx_id,
            index: point.1,
            address: content.address.clone(),
            value: content.lovelace().to_string(),
            data_hash: content.data_hash.clone(),
            consumed_by_tx_id: None,
        });
        for (ordinal, (policy, name, quantity)) in content.assets().into_iter().enumerate() {
            if !self.rule.allows(&policy, &name) {
                continue;
            }
            let ordinal = u16::try_from(ordinal).unwrap_or(u16::MAX);
            rows.ma_tx_out.push(AssetRow {
                id: ids::asset_id(place.height, place.index, point.1, ordinal).map_err(invalid)?,
                quantity: quantity.to_string(),
                tx_out_id: output_id,
                policy,
                name,
            });
        }
        if self.needs_datum(content)
            && let Some(bytes) = Self::datum_bytes(content, datums)?
        {
            let data_hash = content.data_hash.clone().expect("hash of the bytes");
            if !rows.datum.iter().any(|row| row.hash == data_hash) {
                rows.datum.push(DatumRow {
                    hash: data_hash,
                    value: dsa_chain::datum::render(&bytes).map_err(|error| {
                        Error::answer(error.code, json!({"datum": content.data_hash}))
                    })?,
                });
            }
        }
        Ok(())
    }

    /// Descriptor IDs: the fixed ones of the network's table when their unit
    /// was first minted at or below T, the others in chain order after them.
    async fn descriptors(&mut self, rows: &Rows) -> Result<Vec<(i64, String, String)>, Error> {
        let quantities = self.items("quantity").await?;
        let table = fixed_descriptor_ids(self.profile);
        let mut descriptors = Vec::new();
        for (unit, id) in &table {
            let available = quantities
                .get(unit)
                .and_then(|value| value["first_mint_height"].as_u64())
                .is_some_and(|height| height <= u64::from(self.plan.target));
            if available {
                descriptors.push((*id, unit[..56].to_owned(), unit[56..].to_owned()));
            }
        }
        let mut next = table.iter().map(|(_, id)| *id).max().unwrap_or(0) + 1;
        let mut others: Vec<(i64, String)> = rows
            .ma_tx_out
            .iter()
            .map(|row| (row.id, format!("{}{}", row.policy, row.name)))
            .filter(|(_, unit)| !table.iter().any(|(known, _)| known == unit))
            .collect();
        others.sort();
        for (_, unit) in others {
            if descriptors
                .iter()
                .any(|(_, policy, name)| format!("{policy}{name}") == unit)
            {
                continue;
            }
            descriptors.push((next, unit[..56].to_owned(), unit[56..].to_owned()));
            next += 1;
        }
        Ok(descriptors)
    }

    /// The base: rows below H, supply at H after the proof, the position.
    async fn base(&mut self) -> Result<Value, Error> {
        let (unspent, unplaced) = self.unspent_at_horizon().await?;
        let valid: BTreeMap<String, bool> = self.typed("valid").await?;
        let datums = self.items("datum").await?;
        let carried: BTreeMap<String, Vec<Carried>> = self.typed("carried").await?;
        let headers: BTreeMap<String, Header> = self.typed("header").await?;
        let params = self.items("param").await?;
        let mut rows = Rows::default();
        for (point, (content, place)) in &unspent {
            let valid = *valid.get(&point.0).ok_or_else(|| {
                refusal("rebuild_validity_missing", json!({"transaction": point.0}))
            })?;
            self.output_rows(&mut rows, point, content, *place, valid, &datums)?;
        }
        let mut block_rows: BTreeMap<u32, BlockRow> = BTreeMap::new();
        let mut markers = Vec::new();
        for carry in carried.values().flatten() {
            block_rows.insert(carry.header.height, block_row(&carry.header));
            for (index, content) in &carry.outputs {
                let point = (carry.tx.clone(), *index);
                self.output_rows(
                    &mut rows,
                    &point,
                    content,
                    carry.place,
                    carry.valid,
                    &datums,
                )?;
                if !unspent.contains_key(&point) {
                    markers.push(
                        ids::output_id(carry.place.height, carry.place.index, *index)
                            .map_err(invalid)?,
                    );
                }
            }
        }
        for (key, header) in &headers {
            if key.starts_with("epoch:") || key.starts_with("chain:") {
                block_rows.insert(header.height, block_row(header));
            }
        }
        let epoch_params = params
            .iter()
            .map(|(epoch, value)| {
                Ok(EpochParamRow {
                    epoch: epoch
                        .parse()
                        .map_err(|_| refusal("rebuild_progress_invalid", json!({})))?,
                    nonce: value["nonce"].as_str().unwrap_or_default().to_owned(),
                    max_tx_size: value["max_tx_size"].as_u64().unwrap_or_default() as u32,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        // Proof: the unspent amount at H of each fixed unit equals the
        // provider's quantity minus the net mint since H up to its tip.
        let supply = self.proof(&unspent).await?;
        let descriptors = self.descriptors(&rows).await?;
        let k = self.profile.cardano.security_parameter;
        let header = |key: &str| {
            headers
                .get(key)
                .map(block_row)
                .ok_or_else(|| refusal("rebuild_header_missing", json!({"header": key})))
        };
        let horizon = header("horizon")?;
        let target = header(&format!("chain:{}", self.plan.horizon - 1))?;
        let tip = header(&format!("chain:{}", self.plan.horizon - 1 + k))?;
        let counts = json!({"unspent_at_horizon": unspent.len(), "unplaced_candidates": unplaced,
            "transactions": rows.tx.len(), "outputs": rows.tx_out.len(), "asset_rows": rows.ma_tx_out.len(),
            "datums": rows.datum.len(), "headers": block_rows.len(), "markers": markers.len(),
            "carried": carried.values().map(Vec::len).sum::<usize>()});
        let base = Base {
            built: BuiltRows {
                headers: block_rows.into_values().collect(),
                rows,
                descriptors,
                epoch_params,
                markers,
            },
            supply,
            state: ServingState::of_headers(&horizon, &target, &tip, self.plan.retention_seconds),
        };
        let profile = self.profile;
        self.store()
            .await?
            .write_building_base(profile, &base)
            .await?;
        Ok(counts)
    }

    async fn proof(
        &mut self,
        unspent: &BTreeMap<(String, u16), (Content, Place)>,
    ) -> Result<BTreeMap<String, String>, Error> {
        let quantities = self.items("quantity").await?;
        let since: BTreeMap<String, Place> = self.typed("since").await?;
        let moved: BTreeMap<String, Moved> = self.typed("moved").await?;
        let mut supply = BTreeMap::new();
        let mut known_valid: BTreeMap<String, bool> = self.typed("valid").await?;
        for unit in fixed_units(self.profile) {
            let at_horizon: u128 = unspent
                .values()
                .map(|(content, _)| content.quantity(&unit))
                .sum();
            let reading = &quantities[&unit];
            let quantity: i128 = reading["quantity"]
                .as_str()
                .map_or(Ok(0), str::parse)
                .map_err(|_| Error::answer("provider_quantity_invalid", json!({"unit": unit})))?;
            let tip_before = reading["tip_before"].as_u64().unwrap_or_default();
            let tip_after = reading["tip_after"].as_u64().unwrap_or_default();
            let mut minted: i128 = 0;
            for (transaction, place) in &since {
                let Some(moved) = moved.get(transaction) else {
                    continue;
                };
                if moved.unknown {
                    continue;
                }
                let made: i128 = moved
                    .outputs
                    .iter()
                    .filter(|output| !output.collateral)
                    .flat_map(|output| output.amount.iter())
                    .filter(|amount| amount.unit == unit)
                    .map(|amount| i128::from(amount.quantity.0))
                    .sum();
                let used: i128 = moved
                    .inputs
                    .iter()
                    .filter(|input| !input.collateral && !input.reference)
                    .filter_map(|input| input.content.as_ref())
                    .map(|content| content.quantity(&unit) as i128)
                    .sum();
                let net = made - used;
                if net == 0 {
                    continue;
                }
                let valid = match known_valid.get(transaction) {
                    Some(valid) => *valid,
                    None => {
                        let key = hash32(transaction)?;
                        let provider = self.asker.provider.clone();
                        let info = self
                            .asker
                            .ask("transactions", || provider.transaction(&key))
                            .await?
                            .ok_or_else(|| {
                                Error::answer(
                                    "provider_transaction_unknown",
                                    json!({"transaction": transaction}),
                                )
                            })?;
                        self.put_one("valid", transaction, info.valid_contract)
                            .await?;
                        known_valid.insert(transaction.clone(), info.valid_contract);
                        info.valid_contract
                    }
                };
                if !valid {
                    continue;
                }
                if u64::from(place.height) > tip_before && u64::from(place.height) <= tip_after {
                    return Err(Error::answer(
                        "provider_mint_while_quantity_read",
                        json!({"unit": unit, "transaction": transaction}),
                    ));
                }
                if u64::from(place.height) <= tip_before {
                    minted += net;
                }
            }
            let expected = quantity - minted;
            if i128::try_from(at_horizon).ok() != Some(expected) {
                return Err(check(
                    "rebuild_supply_mismatch",
                    json!({"unit": unit, "unspent_at_horizon": at_horizon.to_string(),
                        "provider_quantity": quantity.to_string(), "net_mint_since_horizon": minted.to_string(),
                        "provider_tip": tip_before}),
                ));
            }
            supply.insert(unit, at_horizon.to_string());
        }
        Ok(supply)
    }

    // ----- step 7 -----------------------------------------------------------

    /// K10: the newest relevant spend below H, found in the units' and
    /// addresses' lists read backward from H − 1.
    async fn newest_spend(&mut self) -> Result<Option<BuiltRows>, Error> {
        let stored: BTreeMap<String, NewestSpend> = self.typed("spend").await?;
        let spend = match stored.get("newest") {
            Some(spend) => spend.clone(),
            None => {
                let Some(spend) = self.find_newest_spend().await? else {
                    return Ok(None);
                };
                self.put_one("spend", "newest", &spend).await?;
                spend
            }
        };
        let datums = self.items("datum").await?;
        let mut rows = Rows::default();
        let point = (spend.origin.clone(), spend.origin_index);
        self.output_rows(
            &mut rows,
            &point,
            &spend.content,
            spend.origin_place,
            spend.origin_valid,
            &datums,
        )?;
        let tx_id = ids::transaction_id(spend.place.height, spend.place.index).map_err(invalid)?;
        if !rows.tx.iter().any(|row| row.id == tx_id) {
            rows.tx.push(TxRow {
                id: tx_id,
                hash: spend.tx.clone(),
                block_id: i64::from(spend.place.height),
                block_index: spend.place.index,
                valid_contract: spend.valid,
            });
        }
        let origin_tx = ids::transaction_id(spend.origin_place.height, spend.origin_place.index)
            .map_err(invalid)?;
        rows.tx_in.push(InputRow {
            id: ids::input_id(spend.place.height, spend.place.index, spend.ordinal)
                .map_err(invalid)?,
            tx_in_id: tx_id,
            tx_out_id: origin_tx,
            tx_out_index: spend.origin_index,
        });
        let marker = ids::output_id(
            spend.origin_place.height,
            spend.origin_place.index,
            spend.origin_index,
        )
        .map_err(invalid)?;
        Ok(Some(BuiltRows {
            rows,
            markers: vec![marker],
            ..BuiltRows::default()
        }))
    }

    async fn find_newest_spend(&mut self) -> Result<Option<NewestSpend>, Error> {
        let mut sources: Vec<String> = self
            .units()
            .await?
            .into_iter()
            .map(|unit| format!("unit:{unit}"))
            .collect();
        sources.extend(self.addresses.iter().map(|a| format!("address:{a}")));
        let mut best: Option<NewestSpend> = None;
        let below = Heights {
            from: None,
            to: Some(self.plan.horizon - 1),
        };
        for source in sources {
            let mut page = Page::first();
            'pages: loop {
                let rows = self.transactions_page(&source, below, page, true).await?;
                for row in &rows {
                    let place = Place {
                        height: row.block_height,
                        index: row.tx_index as u16,
                    };
                    if best.as_ref().is_some_and(|best| best.place >= place) {
                        break 'pages;
                    }
                    if let Some(spend) = self.spend_of(row, place).await? {
                        best = Some(spend);
                        break 'pages;
                    }
                }
                if rows.len() < 100 {
                    break;
                }
                page = page
                    .next()
                    .ok_or_else(|| Error::answer("provider_list_too_long", json!({})))?;
            }
        }
        Ok(best)
    }

    /// The relevant spend of the highest ordinal in `row`'s transaction.
    async fn spend_of(
        &mut self,
        row: &ListedTransaction,
        place: Place,
    ) -> Result<Option<NewestSpend>, Error> {
        let transaction = row.tx_hash.to_string();
        let key = row.tx_hash.clone();
        let provider = self.asker.provider.clone();
        let answer = self
            .asker
            .ask("outputs", || provider.transaction_utxos(&key))
            .await?
            .ok_or_else(|| {
                Error::answer(
                    "provider_transaction_unknown",
                    json!({"transaction": transaction}),
                )
            })?;
        let mut contents = Vec::new();
        for input in answer.inputs.iter().filter(|input| !input.reference) {
            let content = if input.collateral {
                let origin = input.tx_hash.clone();
                let origin_answer = self
                    .asker
                    .ask("outputs", || provider.transaction_utxos(&origin))
                    .await?;
                origin_answer.and_then(|answer| {
                    answer
                        .outputs
                        .iter()
                        .find(|output| output.output_index == input.output_index)
                        .map(|output| Content {
                            address: output.address.clone(),
                            amount: output.amount.clone(),
                            data_hash: output.data_hash.as_ref().map(ToString::to_string),
                            inline_datum: output.inline_datum.as_ref().map(|b| hex::encode(&b.0)),
                        })
                })
            } else {
                Some(Content {
                    address: input.address.clone(),
                    amount: input.amount.clone(),
                    data_hash: input.data_hash.as_ref().map(ToString::to_string),
                    inline_datum: input.inline_datum.as_ref().map(|b| hex::encode(&b.0)),
                })
            };
            contents.push((input, content));
        }
        if !contents
            .iter()
            .any(|(_, content)| content.as_ref().is_some_and(|c| self.relevant(c)))
        {
            return Ok(None);
        }
        let info = self
            .asker
            .ask("transactions", || provider.transaction(&key))
            .await?
            .ok_or_else(|| {
                Error::answer(
                    "provider_transaction_unknown",
                    json!({"transaction": transaction}),
                )
            })?;
        let mut effective: Vec<_> = contents
            .into_iter()
            .filter(|(input, _)| input.collateral != info.valid_contract)
            .map(|(input, content)| ((input.tx_hash.to_string(), input.output_index), content))
            .collect();
        effective.sort_by(|a, b| a.0.cmp(&b.0));
        let Some((ordinal, ((origin, origin_index), content))) = effective
            .iter()
            .enumerate()
            .filter(|(_, (_, content))| content.as_ref().is_some_and(|c| self.relevant(c)))
            .last()
        else {
            return Ok(None);
        };
        let origin_key = hash32(origin)?;
        let origin_info = self
            .asker
            .ask("transactions", || provider.transaction(&origin_key))
            .await?
            .ok_or_else(|| {
                Error::answer(
                    "provider_transaction_unknown",
                    json!({"transaction": origin}),
                )
            })?;
        let content = content.clone().expect("relevant");
        if self.needs_datum(&content) && content.inline_datum.is_none() {
            self.fetch_datums(content.data_hash.iter().cloned().collect())
                .await?;
        }
        Ok(Some(NewestSpend {
            tx: transaction,
            place,
            valid: info.valid_contract,
            ordinal: u16::try_from(ordinal).unwrap_or(u16::MAX),
            origin: origin.clone(),
            origin_index: u16::try_from(*origin_index).unwrap_or(u16::MAX),
            origin_place: Place {
                height: origin_info.block_height,
                index: origin_info.index as u16,
            },
            origin_valid: origin_info.valid_contract,
            content,
        }))
    }
}

/// The positions of a new build: T = the given target or the provider's tip
/// − k; H = the given horizon or the newest block at or before T's time −
/// the window.
async fn plan(asker: &Asker, profile: &Profile, options: &Options) -> Result<Plan, Error> {
    let provider = asker.provider.clone();
    let k = profile.cardano.security_parameter;
    let latest = asker.ask("headers", || provider.latest_block()).await?;
    let final_height = latest.height.checked_sub(k).ok_or_else(|| {
        refusal(
            "provider_chain_shorter",
            json!({"provider_tip": latest.height}),
        )
    })?;
    let target = options.target_block.unwrap_or(final_height);
    if target > final_height {
        return Err(refusal(
            "target_above_provider_final",
            json!({"target": target, "provider_final": final_height}),
        ));
    }
    let window = options.window_seconds.unwrap_or(options.retention_seconds);
    if window < 86_400 || window > options.retention_seconds {
        return Err(refusal(
            "invalid_window",
            json!({"window_seconds": window, "retention_seconds": options.retention_seconds}),
        ));
    }
    let horizon = match options.horizon_block {
        Some(horizon) => horizon,
        None => {
            let time_of = |height: u32| {
                let provider = provider.clone();
                async move {
                    asker
                        .ask("headers", || provider.block_at(height))
                        .await?
                        .map(|header| header.time)
                        .ok_or_else(|| {
                            Error::answer("provider_block_missing", json!({"height": height}))
                        })
                }
            };
            let cutoff = time_of(target).await?.saturating_sub(window as u64);
            let (mut low, mut high) = (target.saturating_sub(window as u32).max(1), target);
            while low < high {
                let middle = low + (high - low).div_ceil(2);
                if time_of(middle).await? <= cutoff {
                    low = middle;
                } else {
                    high = middle - 1;
                }
            }
            low
        }
    };
    if horizon == 0 || horizon > target {
        return Err(refusal(
            "invalid_horizon",
            json!({"horizon": horizon, "target": target}),
        ));
    }
    Ok(Plan {
        network: profile.network.clone(),
        horizon,
        target,
        tip: target + k,
        window_seconds: window,
        retention_seconds: options.retention_seconds,
    })
}

/// Builds the database from the provider, or continues a build that
/// stopped, and publishes it. A build that ends without publishing records
/// its error as the stopped service state `status` prints.
pub async fn rebuild(context: Context<'_>) -> Result<Report, Error> {
    let connect = Arc::new(context.connect);
    let recorder = Arc::clone(&connect);
    let context = Context {
        connect: Box::new(move || connect()),
        ..context
    };
    let result = run(context).await;
    if let Err(error) = &result {
        let report = error.report();
        let code = report["error"]
            .as_str()
            .unwrap_or("rebuild_failed")
            .to_owned();
        if let Ok(mut store) = recorder().await
            && store.building_progress().await.ok().flatten().is_some()
        {
            let _ = store
                .record_state("stopped", &code, &json!({}), Some(&report))
                .await;
        }
    }
    result
}

async fn run(context: Context<'_>) -> Result<Report, Error> {
    let started = Instant::now();
    let Context {
        profile,
        provider,
        connect,
        settings,
        options,
        time,
        stop,
        progress,
    } = context;
    let connect = Arc::new(connect);
    let asker = Asker {
        provider: provider.clone(),
        requests: Requests::default(),
        time: Arc::clone(&time),
        stop: stop.clone(),
        fatal_after: settings.fatal_after,
    };
    let mut store = connect().await?;
    store.lock_writer().await?;
    let (plan, phase, calls_before, resumed) = match store.building(profile).await? {
        Building::Empty => {
            let plan = plan(&asker, profile, &options).await?;
            store.start_building(profile, &to_value(&plan)?).await?;
            (plan, "listing".to_owned(), sent(&provider), false)
        }
        Building::Stopped {
            plan, phase, calls, ..
        } => {
            let plan: Plan = parse(plan)?;
            if plan.network != profile.network
                || options.horizon_block.is_some_and(|h| h != plan.horizon)
                || options.target_block.is_some_and(|t| t != plan.target)
                || options.retention_seconds != plan.retention_seconds
            {
                return Err(refusal(
                    "rebuild_plan_mismatch",
                    json!({"stored": plan, "given": {"horizon": options.horizon_block,
                        "target": options.target_block, "retention_seconds": options.retention_seconds}}),
                ));
            }
            (plan, phase, calls, true)
        }
    };
    let rule = AssetRule::from_profile(profile);
    let mut build = Build {
        profile,
        addresses: profile
            .addresses
            .iter()
            .map(|(_, address)| address.to_owned())
            .collect(),
        permissioned: profile.assets.permissioned_candidates.unit(),
        rule,
        asker,
        max_in_flight: settings.max_in_flight.max(1),
        store: Some(store),
        connect: Arc::clone(&connect),
        plan: plan.clone(),
        calls_before,
        sent_at_start: sent(&provider),
        phase,
        recorded: Instant::now(),
    };
    eprintln!(
        "{}",
        json!({"event":"rebuild_started","plan":plan,"phase":build.phase,"resumed":resumed})
    );
    let mut counts = json!({});
    let start_at = phase_index(&build.phase).max(1);
    for phase in PHASES.iter().skip(start_at) {
        build.reach(phase).await?;
        match *phase {
            "listing" => build.listing().await?,
            "quantities" => build.quantities().await?,
            "since" => build.since().await?,
            "transactions" => build.transactions().await?,
            "origins" => build.origins().await?,
            "history" => build.history().await?,
            "validity" => build.validity().await?,
            "datums" => {
                build.datums().await?;
            }
            "carried" => {
                build.carried().await?;
                build.datums().await?;
            }
            "epochs" => build.epochs().await?,
            "base" => {
                counts = build.base().await?;
                build.phase = "window".to_owned();
            }
            "window" => {
                // The follower takes the writer lock on its own connection.
                build.store = None;
                let follower: Connect = {
                    let connect = Arc::clone(&connect);
                    Box::new(move || connect())
                };
                let report = follow::follow(follow::Context {
                    profile,
                    provider: provider.clone(),
                    connect: follower,
                    settings: Settings {
                        stop_at_target: Some(plan.target),
                        building: true,
                        ..settings.clone()
                    },
                    time: Arc::clone(&time),
                    stop: stop.clone(),
                    progress: progress.clone(),
                })
                .await?;
                if report.stopped != "stop_target" {
                    return Err(Error::Stopped);
                }
            }
            "publish" => {}
            _ => {}
        }
    }
    let newest_spend = {
        let window_spent = build
            .store()
            .await?
            .window_has_spend(plan.horizon, plan.target)
            .await?;
        if window_spent {
            None
        } else {
            build.newest_spend().await?
        }
    };
    let headers: BTreeMap<String, Header> = build.typed("header").await?;
    let expected = {
        let row = |key: &str| {
            headers
                .get(key)
                .map(block_row)
                .ok_or_else(|| refusal("rebuild_header_missing", json!({"header": key})))
        };
        let tip_row = BlockRow {
            height: plan.tip,
            ..row("target")?
        };
        ServingState::of_headers(
            &row("horizon")?,
            &row("target")?,
            &tip_row,
            plan.retention_seconds,
        )
    };
    let calls = build.calls();
    let origin = json!({"kind":"rebuilt","source":"provider","horizon_height":plan.horizon,
        "target_height":plan.target,"tip_height":plan.tip,"window_seconds":plan.window_seconds,
        "provider_requests":calls});
    let verified = build
        .store()
        .await?
        .publish_building(profile, &expected, newest_spend.as_ref(), &origin)
        .await?;
    eprintln!(
        "{}",
        json!({"event":"rebuild_published","content_digest":verified.content_digest,"calls":calls})
    );
    Ok(Report {
        plan,
        resumed,
        verified,
        calls,
        requests: build.asker.requests.get(),
        counts,
        seconds: started.elapsed().as_secs_f64(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsa_provider::types::Quantity;

    fn amount(unit: &str, quantity: u64) -> AssetAmount {
        AssetAmount {
            unit: unit.to_owned(),
            quantity: Quantity(quantity),
        }
    }

    #[test]
    fn asset_ordinals_rank_the_full_asset_list_in_bytewise_order() {
        let content = Content {
            address: "addr".to_owned(),
            amount: vec![
                amount("lovelace", 5),
                amount(&format!("{}01", "bb".repeat(28)), 1),
                amount(&format!("{}", "aa".repeat(28)), 2),
                amount(&format!("{}00", "bb".repeat(28)), 0),
            ],
            data_hash: None,
            inline_datum: None,
        };
        let assets = content.assets();
        assert_eq!(assets.len(), 2);
        assert_eq!(assets[0].0, "aa".repeat(28));
        assert_eq!(assets[1].1, "01");
        assert_eq!(content.lovelace(), 5);
    }

    #[test]
    fn phases_run_in_their_order() {
        assert_eq!(phase_index("listing"), 1);
        assert_eq!(phase_index("window"), 12);
        assert!(
            PHASES
                .windows(2)
                .all(|pair| phase_index(pair[0]) < phase_index(pair[1]))
        );
    }
}
