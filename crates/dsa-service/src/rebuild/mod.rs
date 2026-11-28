//! A database built from the provider alone, for horizon H and target T.
//!
//! 1. The unspent outputs created below H that are relevant now: per unit
//!    (the exact eligible units and every unit of the two governance
//!    policies) `/assets/{unit}/utxos` in creation-height ranges answered by
//!    one page each, so that a spend between two pages cannot shift a row out
//!    of the listing; per profile address `/addresses/{address}/utxos`, read
//!    until two passes agree.
//! 2. Each unit's mints and burns (`/assets/{unit}/history`) and then its
//!    quantity, read again until the two agree; then, up to the provider's
//!    tip read after them, every transaction from H on that created an
//!    output of a unit or touched an address, and every burn of a history
//!    that no list names (a burn of a unit's whole input amount is in no list
//!    of the unit), with their resolved inputs (`/txs/{hash}/utxos`).
//! 3. Unspent at H: the listed outputs and the relevant inputs of those
//!    transactions, without the outputs they created; each one's creating
//!    transaction placed below H by the full transaction lists (`to` H − 1),
//!    its phase-2 validity, its datum. An output whose creating transaction
//!    no list places refuses the build. The outputs at each profile address
//!    are derived a second time from the address's full list below H (an
//!    output there is unspent at H unless a transaction of the list spent
//!    it) and must be the same.
//! 4. K5 and K6 from the governance addresses' and the permissioned unit's
//!    lists read backward from H − 1, K7 and K8 from the epochs, the headers
//!    H − 1 … H − 1 + k.
//! 5. Proof: for every unit that has a reading, the unspent amount at H
//!    equals the provider's quantity minus the mints and plus the burns of
//!    its history whose transactions are placed from H on. Then the base is
//!    written with the gate closed.
//! 6. The window H … T through the follower's own updates and checks.
//! 7. K10 when the window holds no spend, from the lists and the burns
//!    below H; publication with `verify`.
//!
//! Every provider result a phase uses is stored in the database being built
//! before the next phase starts, so a stopped build continues where it
//! stopped.
//!
//! The phases by module: `listing` (steps 1 and 2), `positions` (the unspent outputs at H, their
//! creating transactions' places and the second derivation of the profile
//! addresses' outputs), `validity` (their phase-2 validity),
//! `clauses` (K4 datums, K5 and K6 carried transactions, K7 and K8 epochs and
//! the headers after H − 1), `publication` (the base, the proof and the
//! publication), `window` (the window and K10), `progress` (what a build
//! stores to continue). `asker` sends the provider requests, `content` holds
//! the records the phases store.

mod asker;
mod clauses;
mod content;
mod listing;
mod positions;
mod progress;
mod publication;
mod validity;
mod window;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Instant,
};

use chrono::NaiveDate;
use dsa_chain::{
    assets::AssetRule,
    profiles::{Profile, network_magic},
};
use dsa_provider::Provider;
use dsa_store::{
    Store,
    rebuild::{Building, REBUILDING},
    verify::Verification,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::watch;

use crate::{
    follow::{Connect, Error, Progress, Settings},
    retry::{Stop, Time},
};

use self::{
    asker::{Asker, refusal},
    content::Content,
    progress::{PHASES, parse, phase_index, to_value},
};

/// Items a phase holds before it stores them, unless the options say.
const BATCH: usize = 200;

/// Height segments a unit listing with more than one page is read in, side
/// by side, unless the options say. Measured on the hosted provider's
/// Mainnet cNIGHT listing with 2, 4 and 8: a request answers in 0.25 to 7
/// seconds, slower the further its range starts from the unit's first
/// output and nearly as fast with eight in flight as with two; eight
/// segments give the shortest listing, at four requests in flight and at
/// eight.
const LISTING_SEGMENTS: usize = 8;

/// What is discarded to read the listings again: stored kinds, and cursors
/// by key prefix.
const LISTING_KINDS: [&str; 6] = [
    "unit",
    "listed",
    "address_listed",
    "quantity",
    "since",
    "tip",
];
const LISTING_CURSORS: [&str; 6] = ["units", "unit:", "address:", "since:", "burns", "history:"];

/// The profile addresses' full lists below H and the outputs of their
/// transactions, from which their outputs are derived a second time.
const ADDRESS_HISTORY_KINDS: [&str; 2] = ["address_tx", "held"];
const ADDRESS_HISTORY_CURSORS: [&str; 1] = ["held:"];

/// What the next run reads again after a refusal before the window: the
/// stored kinds and the cursors (by key prefix) it discards, and the phase
/// it continues at.
#[derive(Debug, PartialEq)]
struct ReadAgain {
    kinds: Vec<&'static str>,
    cursors: Vec<&'static str>,
    phase: &'static str,
}

/// What a refusal in `phase` discards, by its cause; `None`: nothing, and
/// the next run continues at the stored phase.
///
/// A refusal whose cause can be a moment at which the provider's answers
/// did not fit together reads the listings, the readings of the units and
/// the lists from H on again, and keeps the places, validity answers and
/// other results below H. One of a listing that misses an output also
/// reads the profile addresses' full lists below H again: the answer that
/// missed it may be on either side of their comparison. A tip that left
/// the chain discards only what the lists from H on read: the unit
/// listings and the lists below H are final. A transaction that a list
/// from H on or a history named and the provider does not know is read
/// again with them; one below H (in the listing of a crowded height, the
/// readings, the address histories or the validity answers) is asked
/// again, and listing again does not cure it.
fn read_again(code: &str, phase: &str) -> Option<ReadAgain> {
    if phase_index(phase) >= phase_index("window") {
        return None;
    }
    let listings = ReadAgain {
        kinds: LISTING_KINDS.to_vec(),
        cursors: LISTING_CURSORS.to_vec(),
        phase: "listing",
    };
    match code {
        "rebuild_supply_mismatch" => Some(listings),
        "rebuild_listing_incomplete" | "rebuild_address_outputs_mismatch" => {
            let mut again = listings;
            again.kinds.extend(ADDRESS_HISTORY_KINDS);
            again.cursors.extend(ADDRESS_HISTORY_CURSORS);
            Some(again)
        }
        "provider_tip_left_chain" => Some(ReadAgain {
            kinds: listing::SINCE_KINDS.to_vec(),
            cursors: listing::SINCE_CURSORS.to_vec(),
            phase: "since",
        }),
        "provider_transaction_unknown" if matches!(phase, "since" | "transactions") => {
            Some(listings)
        }
        _ => None,
    }
}

/// The version of the software that begins a build; a build is continued
/// only by the same version.
const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The choices of a build.
#[derive(Clone, Debug, Default)]
pub struct Options {
    pub horizon_block: Option<u32>,
    pub target_block: Option<u32>,
    /// The initial window when the horizon is not given: H is the newest
    /// block at or before the target's time minus this. The retention when
    /// not given.
    pub window_seconds: Option<i64>,
    pub retention_seconds: i64,
    /// The shortest window from H's time to T's that a build accepts.
    pub shortest_window_seconds: i64,
    /// Items a phase holds before it stores them; 200 when 0.
    pub batch_items: usize,
    /// Height segments a unit listing with more than one page is read in;
    /// `LISTING_SEGMENTS` when 0, one after another when 1.
    pub listing_segments: usize,
}

/// The choices a build began with; a stopped build continues only with the
/// same.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Flags {
    pub horizon_block: Option<u32>,
    pub target_block: Option<u32>,
    pub window_seconds: Option<i64>,
    pub retention_seconds: i64,
}

impl Flags {
    fn of(options: &Options) -> Self {
        Self {
            horizon_block: options.horizon_block,
            target_block: options.target_block,
            window_seconds: options.window_seconds,
            retention_seconds: options.retention_seconds,
        }
    }
}

/// The positions a build works for, fixed when it starts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Plan {
    pub network: String,
    pub horizon: u32,
    pub target: u32,
    pub tip: u32,
    /// Seconds of chain time from H to T.
    pub window_seconds: i64,
    pub retention_seconds: i64,
    /// The version that began the build.
    pub version: String,
    pub flags: Flags,
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
    /// Requests of the whole build per endpoint family, the window and its
    /// stake and earlier runs included, counted when they are sent; a
    /// request sent again after a failed attempt counts again. They add up
    /// to `calls`: `unrecorded` holds the requests of earlier runs that
    /// ended before they recorded their families.
    pub requests: BTreeMap<String, u64>,
    /// What the base holds.
    pub counts: Value,
    pub seconds: f64,
}

/// A running build: its phases are the methods of the modules above.
struct Build<'a> {
    profile: &'a Profile,
    relevance: Relevance,
    asker: Asker,
    max_in_flight: usize,
    store: Option<Store>,
    connect: Arc<Connect>,
    plan: Plan,
    phase: String,
    /// When the progress was last recorded.
    recorded: Instant,
    /// The provider requests of the earlier runs of the build.
    calls_before: i64,
    /// The client's requests by family when this run began.
    sent_at_start: BTreeMap<String, u64>,
    /// Items a phase holds before it stores them.
    batch: usize,
    /// Height segments of a unit listing with more than one page.
    listing_segments: usize,
    /// The build's phase is its progress's reason until the window, whose
    /// follower reports its own.
    progress: watch::Sender<Progress>,
}

/// What makes an output one the node reads, and one whose datum it reads:
/// the eligible units, the profile's addresses and the permissioned unit.
struct Relevance {
    rule: AssetRule,
    addresses: BTreeSet<String>,
    permissioned: String,
}

impl Relevance {
    fn of(profile: &Profile) -> Self {
        Self {
            rule: AssetRule::from_profile(profile),
            addresses: profile
                .addresses
                .iter()
                .map(|(_, address)| address.to_owned())
                .collect(),
            permissioned: profile.assets.permissioned_candidates.unit(),
        }
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
}

impl Build<'_> {
    fn relevant(&self, content: &Content) -> bool {
        self.relevance.relevant(content)
    }

    fn needs_datum(&self, content: &Content) -> bool {
        self.relevance.needs_datum(content)
    }
}

/// The provider serves the profile's network: its `/genesis` names the
/// network's magic. Read first, before a new build changes the database, so
/// a provider of another network ends the build with
/// `provider_network_mismatch` before any listing.
async fn check_network(asker: &Asker, profile: &Profile) -> Result<(), Error> {
    let Some(expected) = network_magic(&profile.network) else {
        return Ok(());
    };
    let provider = asker.provider.clone();
    let genesis = asker.ask(|| provider.genesis()).await?;
    if genesis.network_magic != expected {
        return Err(refusal(
            "provider_network_mismatch",
            json!({"network": profile.network, "expected_magic": expected,
                "received_magic": genesis.network_magic}),
        ));
    }
    Ok(())
}

/// The positions of a new build: T = the given target or the provider's tip
/// − k; H = the given horizon or the newest block at or before T's time −
/// the window. The window from H's time to T's must be at least the
/// shortest; above the retention only with a given horizon.
async fn plan(asker: &Asker, profile: &Profile, options: &Options) -> Result<Plan, Error> {
    let provider = asker.provider.clone();
    let k = profile.cardano.security_parameter;
    let latest = asker.ask(|| provider.latest_block()).await?;
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
    let time_of = |height: u32| {
        let provider = provider.clone();
        async move {
            asker
                .ask(|| provider.block_at(height))
                .await?
                .map(|header| header.time)
                .ok_or_else(|| Error::answer("provider_block_missing", json!({"height": height})))
        }
    };
    let window_refused = |window: i64| {
        refusal(
            "invalid_window",
            json!({"window_seconds": window, "shortest_window_seconds": options.shortest_window_seconds,
                "retention_seconds": options.retention_seconds}),
        )
    };
    let (horizon, target_time) = match options.horizon_block {
        Some(horizon) => {
            if horizon == 0 || horizon > target {
                return Err(refusal(
                    "invalid_horizon",
                    json!({"horizon": horizon, "target": target}),
                ));
            }
            (horizon, time_of(target).await?)
        }
        None => {
            let window = options.window_seconds.unwrap_or(options.retention_seconds);
            if window < options.shortest_window_seconds || window > options.retention_seconds {
                return Err(window_refused(window));
            }
            let target_time = time_of(target).await?;
            let cutoff = target_time.saturating_sub(u64::try_from(window).unwrap_or(0));
            // Blocks are at least a second apart: the horizon is within
            // `window` heights below the target.
            let span = u32::try_from(window).unwrap_or(u32::MAX);
            let (mut low, mut high) = (target.saturating_sub(span).max(1), target);
            while low < high {
                let middle = low + (high - low).div_ceil(2);
                if time_of(middle).await? <= cutoff {
                    low = middle;
                } else {
                    high = middle - 1;
                }
            }
            if time_of(low).await? > cutoff {
                // The chain is younger than the window.
                return Err(window_refused(window));
            }
            (low, target_time)
        }
    };
    let window =
        i64::try_from(target_time.saturating_sub(time_of(horizon).await?)).unwrap_or(i64::MAX);
    if window < options.shortest_window_seconds {
        return Err(window_refused(window));
    }
    Ok(Plan {
        network: profile.network.clone(),
        horizon,
        target,
        tip: target + k,
        window_seconds: window,
        retention_seconds: options.retention_seconds,
        version: VERSION.to_owned(),
        flags: Flags::of(options),
    })
}

/// Builds the database from the provider, or continues a build that
/// stopped or whose window a check closed (cleared first), and publishes
/// it. A build that ends without publishing records
/// its requests and its error as the stopped service state `status` prints;
/// after a refusal before the window, the next run of the command reads
/// again what `read_again` names.
pub async fn rebuild(context: Context<'_>) -> Result<Report, Error> {
    let connect = Arc::new(context.connect);
    let recorder = Arc::clone(&connect);
    let provider = context.provider.clone();
    let context = Context {
        connect: Box::new(move || connect()),
        ..context
    };
    // This run's requests per family, the window's included, are the
    // client's counts since now: recorded also when the run ends without
    // publishing.
    let sent_at_start = provider.sent_by_family();
    let result = run(context, sent_at_start.clone()).await;
    if let Err(error) = &result {
        let report = error.report();
        let code = report["error"]
            .as_str()
            .unwrap_or("rebuild_failed")
            .to_owned();
        if let Ok(mut store) = recorder().await
            && let Some(progress) = store.building_progress().await.ok().flatten()
        {
            if let Some(again) = read_again(&code, &progress.phase)
                && store
                    .discard_building_items(&again.kinds, &again.cursors, Some(again.phase))
                    .await
                    .is_ok()
            {
                eprintln!(
                    "{}",
                    json!({"event":"rebuild_listings_discarded","error":code,"phase":progress.phase,
                        "continue_at":again.phase,"unit_listings":again.kinds.contains(&"listed"),
                        "address_histories":again.kinds.contains(&"held")})
                );
            }
            let _ = store.record_calls(&provider.counts()).await;
            // A run that sent no request leaves the stored counts as they
            // are: the build of another version keeps its own.
            let run = sent_since(&provider, &sent_at_start);
            if !run.is_empty() {
                let _ = store
                    .put_building_items("requests", &[("run".to_owned(), json!(run))])
                    .await;
            }
            // A build whose window a check closed keeps the service state
            // the check recorded until it is cleared: a run refused before
            // the clear leaves it, so the clear logs the check's record.
            if progress.reason == REBUILDING || progress.reason == code {
                let _ = store
                    .record_state("stopped", &code, &json!({}), Some(&report))
                    .await;
            }
        }
    }
    result
}

/// The client's requests by family since `start`, families without one
/// left out.
fn sent_since(provider: &Provider, start: &BTreeMap<String, u64>) -> BTreeMap<String, u64> {
    provider
        .sent_by_family()
        .into_iter()
        .map(|(family, count)| {
            let before = start.get(&family).copied().unwrap_or(0);
            (family, count.saturating_sub(before))
        })
        .filter(|(_, count)| *count > 0)
        .collect()
}

/// The requests per family of the runs of a build before this one: the
/// stored total of the earlier runs and the last run's own count, folded
/// into one total once this version continues the build, before this run
/// sends a request. `calls` is what the earlier runs recorded in total; a
/// run that ended before it recorded its families (killed in the window,
/// whose follower records the requests per day as it goes) leaves fewer in
/// the families, and the difference is the family `unrecorded`.
async fn earlier_requests(store: &mut Store, calls: i64) -> Result<BTreeMap<String, u64>, Error> {
    let stored = store.building_items("requests").await?;
    let mut earlier: BTreeMap<String, u64> = BTreeMap::new();
    for kind in ["earlier", "run"] {
        let counts: BTreeMap<String, u64> = stored
            .get(kind)
            .cloned()
            .map_or(Ok(BTreeMap::new()), parse)?;
        for (family, count) in counts {
            *earlier.entry(family).or_default() += count;
        }
    }
    let recorded: u64 = earlier.values().sum();
    let gap = u64::try_from(calls).unwrap_or(0).saturating_sub(recorded);
    if gap > 0 {
        *earlier.entry("unrecorded".to_owned()).or_default() += gap;
    }
    store
        .put_building_items(
            "requests",
            &[
                ("earlier".to_owned(), json!(earlier)),
                ("run".to_owned(), json!({})),
            ],
        )
        .await?;
    Ok(earlier)
}

/// The plan of a stored build that this run continues: begun by this
/// version, for this network and with the same flags. Checked before the
/// run writes anything or sends a request.
fn continued_plan(plan: Value, profile: &Profile, options: &Options) -> Result<Plan, Error> {
    if plan["version"] != json!(VERSION) {
        return Err(refusal(
            "rebuild_version_mismatch",
            json!({"stored": plan["version"], "running": VERSION}),
        ));
    }
    let plan: Plan = parse(plan)?;
    let given = Flags::of(options);
    if plan.network != profile.network || plan.flags != given {
        return Err(refusal(
            "rebuild_plan_mismatch",
            json!({"stored": plan.flags, "given": given}),
        ));
    }
    Ok(plan)
}

async fn run(context: Context<'_>, sent_at_start: BTreeMap<String, u64>) -> Result<Report, Error> {
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
        time,
        stop,
        fatal_after: settings.fatal_after,
    };
    let mut store = connect().await?;
    store.lock_writer().await?;
    let (plan, phase, calls) = match store.building(profile).await? {
        Building::Empty => {
            check_network(&asker, profile).await?;
            let plan = plan(&asker, profile, &options).await?;
            store.start_building(profile, &to_value(&plan)?).await?;
            (plan, "listing".to_owned(), None)
        }
        Building::Stopped { plan, phase, calls } => {
            (continued_plan(plan, profile, &options)?, phase, Some(calls))
        }
        Building::Closed {
            plan,
            calls,
            reason,
            service_state,
            ..
        } => {
            let plan = continued_plan(plan, profile, &options)?;
            // The failed check's record goes with the clear: logged first.
            eprintln!(
                "{}",
                json!({"event":"rebuild_window_cleared","error":reason,"service_state":service_state})
            );
            store.clear_building_window().await?;
            (plan, "listing".to_owned(), Some(calls))
        }
    };
    let resumed = calls.is_some();
    let mut earlier = BTreeMap::new();
    if let Some(calls) = calls {
        // The requests of the earlier runs, as they recorded them per day
        // and in total, before this run sends one.
        let per_day = store.calls(NaiveDate::default()).await?;
        let recorded = calls.max(per_day.iter().map(|(_, count)| count).sum());
        earlier = earlier_requests(&mut store, recorded).await?;
        provider.seed_counts(&per_day);
        check_network(&asker, profile).await?;
    }
    let mut build = Build {
        profile,
        relevance: Relevance::of(profile),
        asker,
        max_in_flight: settings.max_in_flight.max(1),
        store: Some(store),
        connect,
        plan: plan.clone(),
        phase,
        recorded: Instant::now(),
        calls_before: i64::try_from(earlier.values().sum::<u64>()).unwrap_or(i64::MAX),
        sent_at_start,
        batch: if options.batch_items == 0 {
            BATCH
        } else {
            options.batch_items
        },
        listing_segments: if options.listing_segments == 0 {
            LISTING_SEGMENTS
        } else {
            options.listing_segments
        },
        progress,
    };
    eprintln!(
        "{}",
        json!({"event":"rebuild_started","plan":plan,"phase":build.phase,"resumed":resumed})
    );
    let start_at = phase_index(&build.phase).max(1);
    for phase in PHASES
        .iter()
        .skip(start_at)
        .take_while(|phase| **phase != "publish")
    {
        build.reach(phase).await?;
        match *phase {
            "listing" => build.listing().await?,
            "quantities" => build.quantities().await?,
            "since" => build.since().await?,
            "transactions" => build.transactions().await?,
            "addresses" => build.addresses().await?,
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
                build.base().await?;
                build.phase = "window".to_owned();
            }
            "window" => build.window(&settings).await?,
            other => {
                return Err(refusal("rebuild_progress_invalid", json!({"phase": other})));
            }
        }
    }
    let counts = build
        .items("counts")
        .await?
        .remove("base")
        .unwrap_or_else(|| json!({}));
    let (verified, calls) = build.publish().await?;
    Ok(Report {
        plan,
        resumed,
        verified,
        calls,
        requests: {
            let mut requests = earlier;
            for (family, count) in build.sent() {
                *requests.entry(family).or_default() += count;
            }
            requests
        },
        counts,
        seconds: started.elapsed().as_secs_f64(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each refusal before the window discards what its cause requires and
    /// the next run continues where that is read again.
    #[test]
    fn a_refusal_discards_by_its_cause() {
        let discarded = |code, phase| {
            read_again(code, phase).map(|again| {
                (
                    again.phase,
                    again.kinds.contains(&"listed"),
                    again.kinds.contains(&"held"),
                )
            })
        };
        assert_eq!(
            discarded("rebuild_supply_mismatch", "base"),
            Some(("listing", true, false))
        );
        for code in [
            "rebuild_listing_incomplete",
            "rebuild_address_outputs_mismatch",
        ] {
            assert_eq!(discarded(code, "history"), Some(("listing", true, true)));
        }
        assert_eq!(
            discarded("provider_tip_left_chain", "since"),
            Some(("since", false, false))
        );
        let tip = read_again("provider_tip_left_chain", "since").unwrap();
        assert!(
            !tip.cursors
                .iter()
                .any(|prefix| ["unit:", "history:", "held:"].contains(prefix))
        );
        for phase in ["since", "transactions"] {
            assert_eq!(
                discarded("provider_transaction_unknown", phase),
                Some(("listing", true, false))
            );
        }
        for phase in ["listing", "quantities", "addresses", "validity", "carried"] {
            assert_eq!(
                discarded("provider_transaction_unknown", phase),
                None,
                "{phase}"
            );
        }
        assert_eq!(discarded("rebuild_supply_mismatch", "window"), None);
        assert_eq!(
            discarded("provider_mint_history_mismatch", "quantities"),
            None
        );
    }
}
