//! Epochs. The parameters (nonce, `max_tx_size`) travel with the update that
//! serves an epoch's first block. The stake distribution is read apart from
//! the updates by one of two methods, chosen by the epoch of the provider's
//! latest block as the pool endpoints define "current", and accepted only
//! when its sum equals the epoch's active stake.

use std::{collections::BTreeMap, future::Future, sync::Arc};

use dsa_provider::{
    Kind, Provider,
    types::{Page, PoolId},
};
use dsa_store::follow::EpochParamRow;
use serde::Serialize;
use serde_json::{Value, json};

use crate::{
    follow::Requests,
    retry::{Stop, Time, pause_until},
};

/// Pools retired at the epoch two before the one read, or later, can still
/// hold stake in it: an epoch's distribution is the snapshot taken one epoch
/// earlier, before that boundary removes retiring pools.
pub const RETIRED_WINDOW: u32 = 2;
/// The most pools one distribution may hold.
pub const MAX_POOLS: usize = 100_000;
/// The most pages read of one list.
const MAX_PAGES: u32 = 10_000;
/// Rows of a full page.
const PAGE_ROWS: usize = 100;
/// Attempts of one request of a stake read that fails in transport.
pub const REQUEST_ATTEMPTS: u32 = 5;

/// How the requests of a stake read wait: the follower's clock and its stop.
#[derive(Clone)]
pub struct Waits {
    pub time: Arc<dyn Time>,
    pub stop: Stop,
}

/// The provider, the request counts and the waits of one stake read.
struct Read<'a> {
    provider: &'a Provider,
    requests: &'a Requests,
    waits: &'a Waits,
}

impl Read<'_> {
    /// One request. A transport failure, a timeout or a 5xx is sent again
    /// after the wait the client names, up to `REQUEST_ATTEMPTS` times; a
    /// refusal with its own resume time (402, 418, 429) is waited out and
    /// sent again without counting. Any other error ends the read, so that
    /// one failed request does not cost the whole read.
    async fn send<T, F, R>(&self, family: &'static str, mut request: F) -> Result<T, StakeError>
    where
        F: FnMut() -> R,
        R: Future<Output = Result<T, dsa_provider::Error>>,
    {
        let mut failures = 0;
        loop {
            self.requests.count(family);
            let error = match request().await {
                Ok(answer) => return Ok(answer),
                Err(error) => error,
            };
            let until = match error.kind {
                Kind::Wait { until } => until,
                Kind::Retry { until } if error.transport() => {
                    failures += 1;
                    if failures >= REQUEST_ATTEMPTS {
                        return Err(error.into());
                    }
                    until
                }
                _ => return Err(error.into()),
            };
            if !pause_until(&*self.waits.time, &self.waits.stop, until).await {
                return Err(StakeError::Stopped);
            }
        }
    }
}

/// The parameters of `epoch`; `None` until the provider knows its nonce.
pub async fn parameters(
    provider: &Provider,
    requests: &Requests,
    epoch: u32,
) -> Result<Option<EpochParamRow>, dsa_provider::Error> {
    requests.count("epochs");
    Ok(provider
        .epoch_parameters(epoch)
        .await?
        .and_then(|parameters| {
            parameters.nonce.map(|nonce| EpochParamRow {
                epoch,
                nonce: nonce.to_string(),
                max_tx_size: parameters.max_tx_size,
            })
        }))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Method {
    /// The epoch of the latest block: `/pools/extended`, the retired window
    /// through `/pools/{id}`, and a probe of the epoch's listing for each
    /// pool whose stake is zero.
    Current,
    /// An ended epoch: `/pools`, the retired window and each pool's history.
    History,
}

/// One epoch's stake per pool (28-byte pool hash in hexadecimal, lovelace).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Distribution {
    pub epoch: u32,
    pub method: Method,
    pub active_stake: u64,
    pub pools: BTreeMap<String, u64>,
}

impl Distribution {
    /// The rows `Store::put_epoch_stake` takes.
    pub fn rows(&self) -> Vec<(Vec<u8>, String)> {
        self.pools
            .iter()
            .map(|(pool, amount)| {
                (
                    hex::decode(pool).expect("pool hashes are hexadecimal"),
                    amount.to_string(),
                )
            })
            .collect()
    }
}

/// Why a stake distribution was refused. Each refusal names its cause; one
/// about a pool names the pool.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum StakeError {
    #[error("{0}")]
    Provider(dsa_provider::Error),
    /// The read's first request, the epoch's summary, failed in transport on
    /// every attempt: the provider answered nothing.
    #[error("{0}")]
    Unanswered(dsa_provider::Error),
    /// The provider has no active stake for the epoch yet.
    #[error("provider_epoch_not_ready")]
    NotReady,
    /// The latest block's epoch moved during the read.
    #[error("provider_epoch_changed")]
    Changed,
    /// The pools' stake does not sum to the epoch's active stake.
    #[error("provider_epoch_total_mismatch")]
    Total { expected: u64, actual: u128 },
    /// A pool appears twice in one list.
    #[error("provider_duplicate_pool")]
    DuplicatePool { pool: String, list: &'static str },
    /// A pool ID is not the Bech32 form of a 28-byte pool hash.
    #[error("provider_invalid_pool_id")]
    InvalidPoolId { pool: String },
    /// An answer about one pool fails a check, named by `check`.
    #[error("provider_invalid_stake_row")]
    InvalidRow { pool: String, check: &'static str },
    /// A limit of the read: the code names it.
    #[error("{0}")]
    Invalid(&'static str),
    /// The stop came during a wait.
    #[error("stop_requested")]
    Stopped,
}

impl From<dsa_provider::Error> for StakeError {
    fn from(error: dsa_provider::Error) -> Self {
        Self::Provider(error)
    }
}

impl StakeError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Provider(error) | Self::Unanswered(error) => error.code,
            Self::NotReady => "provider_epoch_not_ready",
            Self::Changed => "provider_epoch_changed",
            Self::Total { .. } => "provider_epoch_total_mismatch",
            Self::DuplicatePool { .. } => "provider_duplicate_pool",
            Self::InvalidPoolId { .. } => "provider_invalid_pool_id",
            Self::InvalidRow { .. } => "provider_invalid_stake_row",
            Self::Invalid(code) => code,
            Self::Stopped => "stop_requested",
        }
    }

    /// The code and what failed: the pool, the list or check, both sums.
    pub fn report(&self) -> Value {
        match self {
            Self::Provider(error) | Self::Unanswered(error) => {
                json!({"error":error.code,"provider":error})
            }
            Self::Total { expected, actual } => {
                json!({"error":self.code(),"active_stake":expected.to_string(),"sum":actual.to_string()})
            }
            Self::DuplicatePool { pool, list } => {
                json!({"error":self.code(),"pool":pool,"list":list})
            }
            Self::InvalidPoolId { pool } => json!({"error":self.code(),"pool":pool}),
            Self::InvalidRow { pool, check } => {
                json!({"error":self.code(),"pool":pool,"check":check})
            }
            _ => json!({"error":self.code()}),
        }
    }

    /// A refusal of what the provider answered, as opposed to a transport
    /// failure or an epoch that moved during the read: retried at growing
    /// waits.
    pub fn refusal(&self) -> bool {
        match self {
            Self::Provider(error) => !error.transport() && matches!(error.kind, Kind::Retry { .. }),
            Self::Unanswered(_) | Self::Changed | Self::Stopped => false,
            _ => true,
        }
    }
}

/// The prefix of the network's stake addresses.
pub fn stake_prefix(network: &str) -> &'static str {
    if network == "mainnet" {
        "stake1"
    } else {
        "stake_test1"
    }
}

/// The 28-byte hash of a pool, in hexadecimal.
fn pool_hash(pool: &PoolId) -> Result<String, StakeError> {
    let invalid = || StakeError::InvalidPoolId {
        pool: pool.to_string(),
    };
    let (hrp, bytes) = bech32::decode(&pool.to_string()).map_err(|_| invalid())?;
    if hrp.as_str() != "pool" || bytes.len() != 28 {
        return Err(invalid());
    }
    Ok(hex::encode(bytes))
}

/// Every row of an ascending list, page by page until one is not full.
async fn every_page<T, F, R>(mut fetch: F) -> Result<Vec<T>, StakeError>
where
    F: FnMut(Page) -> R,
    R: Future<Output = Result<Vec<T>, StakeError>>,
{
    let mut rows = Vec::new();
    let mut page = Page::first();
    for _ in 0..MAX_PAGES {
        let answer = fetch(page).await?;
        let full = answer.len() >= PAGE_ROWS;
        rows.extend(answer);
        if rows.len() > MAX_POOLS {
            return Err(StakeError::Invalid("provider_epoch_size_limit"));
        }
        if !full {
            return Ok(rows);
        }
        page = page
            .next()
            .ok_or(StakeError::Invalid("provider_page_limit"))?;
    }
    Err(StakeError::Invalid("provider_page_limit"))
}

/// Pools retired at `epoch - RETIRED_WINDOW` or later, newest first.
async fn retired(read: &Read<'_>, epoch: u32) -> Result<Vec<PoolId>, StakeError> {
    let floor = epoch.saturating_sub(RETIRED_WINDOW);
    let mut pools = Vec::new();
    let mut page = Page::first();
    for _ in 0..MAX_PAGES {
        let rows = read
            .send("pools", || read.provider.retired_pools(page))
            .await?;
        for row in &rows {
            if row.epoch < floor {
                return Ok(pools);
            }
            pools.push(row.pool_id.clone());
        }
        if rows.len() < PAGE_ROWS {
            return Ok(pools);
        }
        page = page
            .next()
            .ok_or(StakeError::Invalid("provider_page_limit"))?;
    }
    Err(StakeError::Invalid("provider_page_limit"))
}

fn add(
    pools: &mut BTreeMap<String, (PoolId, u64)>,
    pool: PoolId,
    amount: u64,
    list: &'static str,
) -> Result<(), StakeError> {
    let key = pool_hash(&pool)?;
    let id = pool.to_string();
    if pools.insert(key, (pool, amount)).is_some() {
        return Err(StakeError::DuplicatePool { pool: id, list });
    }
    if pools.len() > MAX_POOLS {
        return Err(StakeError::Invalid("provider_epoch_size_limit"));
    }
    Ok(())
}

/// The stake of the current epoch per pool. A pool whose stake is zero holds
/// rows only when the epoch's listing of that pool names a stake address.
pub async fn current_stake(
    provider: &Provider,
    requests: &Requests,
    waits: &Waits,
    epoch: u32,
    stake_prefix: &str,
) -> Result<BTreeMap<String, u64>, StakeError> {
    let read = Read {
        provider,
        requests,
        waits,
    };
    let mut pools = BTreeMap::new();
    let extended =
        every_page(|page| read.send("pools", move || provider.extended_pools(page))).await?;
    for row in extended {
        add(
            &mut pools,
            row.pool_id,
            row.active_stake.0,
            "pools/extended",
        )?;
    }
    for pool in retired(&read, epoch).await? {
        if pools.contains_key(&pool_hash(&pool)?) {
            // Retired, then registered again.
            continue;
        }
        let row = read
            .send("pools", || provider.pool(&pool))
            .await?
            .ok_or_else(|| StakeError::InvalidRow {
                pool: pool.to_string(),
                check: "retired_pool_unknown",
            })?;
        add(&mut pools, pool, row.active_stake.0, "pools/retired")?;
    }
    let mut stake = BTreeMap::new();
    for (key, (pool, amount)) in pools {
        if amount == 0 {
            let listing = read
                .send("epochs", || provider.epoch_pool_stake(epoch, &pool))
                .await?;
            let Some(first) = listing.first() else {
                continue;
            };
            if !first.stake_address.starts_with(stake_prefix) {
                return Err(StakeError::InvalidRow {
                    pool: pool.to_string(),
                    check: "stake_address_of_another_network",
                });
            }
        }
        stake.insert(key, amount);
    }
    Ok(stake)
}

/// The stake of an ended epoch per pool, from the history of every
/// registered pool and of the retired window. A history row of the epoch
/// with stake above zero is taken whatever its delegator count; a row with
/// zero stake is taken only when it counts a delegator (the pool then holds
/// stake rows of amount zero) and skipped otherwise. The sum decides whether
/// the distribution is accepted.
pub async fn historical_stake(
    provider: &Provider,
    requests: &Requests,
    waits: &Waits,
    epoch: u32,
) -> Result<BTreeMap<String, u64>, StakeError> {
    let read = Read {
        provider,
        requests,
        waits,
    };
    let mut candidates = BTreeMap::new();
    let registered = every_page(|page| read.send("pools", move || provider.pools(page))).await?;
    for pool in registered {
        let id = pool.to_string();
        if candidates.insert(pool_hash(&pool)?, pool).is_some() {
            return Err(StakeError::DuplicatePool {
                pool: id,
                list: "pools",
            });
        }
    }
    for pool in retired(&read, epoch).await? {
        candidates.entry(pool_hash(&pool)?).or_insert(pool);
    }
    if candidates.len() > MAX_POOLS {
        return Err(StakeError::Invalid("provider_epoch_size_limit"));
    }
    let mut stake = BTreeMap::new();
    for (key, pool) in candidates {
        let mut page = Page::first();
        let mut done = false;
        for _ in 0..MAX_PAGES {
            let rows = read
                .send("pools", || provider.pool_history(&pool, page))
                .await?;
            done = rows.len() < PAGE_ROWS;
            for row in &rows {
                if row.epoch == epoch && (row.active_stake.0 > 0 || row.delegators_count >= 1) {
                    stake.insert(key.clone(), row.active_stake.0);
                }
                if row.epoch <= epoch {
                    done = true;
                    break;
                }
            }
            if done {
                break;
            }
            page = page
                .next()
                .ok_or(StakeError::Invalid("provider_page_limit"))?;
        }
        if !done {
            return Err(StakeError::Invalid("provider_page_limit"));
        }
    }
    Ok(stake)
}

/// Reads the stake distribution of `epoch` by the method its relation to the
/// latest block selects, and accepts it only when the latest block's epoch
/// did not move during the read and the sum equals the active stake.
pub async fn read_stake(
    provider: &Provider,
    requests: &Requests,
    waits: &Waits,
    epoch: u32,
    network: &str,
) -> Result<Distribution, StakeError> {
    let read = Read {
        provider,
        requests,
        waits,
    };
    let summary = read
        .send("epochs", || provider.epoch(epoch))
        .await
        .map_err(|error| match error {
            StakeError::Provider(error) if error.transport() => StakeError::Unanswered(error),
            error => error,
        })?
        .ok_or(StakeError::NotReady)?;
    let total = summary
        .active_stake
        .map(|stake| stake.0)
        .ok_or(StakeError::NotReady)?;
    if total == 0 {
        return Err(StakeError::Invalid("provider_empty_epoch"));
    }
    let current = read
        .send("headers", || provider.latest_block())
        .await?
        .epoch;
    if current < epoch {
        return Err(StakeError::NotReady);
    }
    let method = if current == epoch {
        Method::Current
    } else {
        Method::History
    };
    let pools = match method {
        Method::Current => {
            current_stake(provider, requests, waits, epoch, stake_prefix(network)).await?
        }
        Method::History => historical_stake(provider, requests, waits, epoch).await?,
    };
    if read
        .send("headers", || provider.latest_block())
        .await?
        .epoch
        != current
    {
        return Err(StakeError::Changed);
    }
    let sum: u128 = pools.values().map(|amount| u128::from(*amount)).sum();
    if pools.is_empty() || sum != u128::from(total) {
        return Err(StakeError::Total {
            expected: total,
            actual: sum,
        });
    }
    Ok(Distribution {
        epoch,
        method,
        active_stake: total,
        pools,
    })
}
