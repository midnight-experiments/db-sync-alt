//! A database built from the provider alone, for horizon H and target T.
//!
//! 1. The unspent outputs created below H that are relevant now: per unit
//!    (the exact eligible units and every unit of the two governance
//!    policies) `/assets/{unit}/utxos` in creation-height ranges answered by
//!    one page each, so that a spend between two pages cannot shift a row out
//!    of the listing; per profile address `/addresses/{address}/utxos`, read
//!    until two passes agree.
//! 2. The quantity and the mints and burns (`/assets/{unit}/history`) of each
//!    unit with the provider's tip around them, then every transaction from H
//!    on that created an output of a unit or touched an address, and every
//!    mint or burn from H on (a burn of a unit's whole input amount is in no
//!    list of the unit), with its resolved inputs (`/txs/{hash}/utxos`).
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
//! 5. Proof: per fixed unit the unspent amount at H equals the provider's
//!    quantity minus the mints and plus the burns of its history from H to
//!    the tip the quantity was read at. Then the base is written with the
//!    gate closed.
//! 6. The window H … T through the follower's own updates and checks.
//! 7. K10 when the window holds no spend, from the lists and the burns
//!    below H; publication with `verify`.
//!
//! Every provider result a phase uses is stored in the database being built
//! before the next phase starts, so a stopped build continues where it
//! stopped.
//!
//! The phases by module: `listing` (steps 1 and 2 and the origins of
//! collateral inputs), `positions` (the unspent outputs at H, their
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
use dsa_store::{Store, rebuild::Building, verify::Verification};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::watch;

use crate::{
    follow::{Connect, Error, Progress, Requests, Settings},
    retry::{Stop, Time},
};

use self::{
    asker::{Asker, refusal},
    content::Content,
    progress::{PHASES, parse, phase_index, to_value},
};

/// Items stored at once.
const BATCH: usize = 200;

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

/// A running build: its phases are the methods of the modules above.
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
    phase: String,
    /// When the progress was last recorded.
    recorded: Instant,
}

impl Build<'_> {
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

/// The provider serves the profile's network: its `/genesis` names the
/// network's magic. Read first, before a new build changes the database, so
/// a provider of another network ends the build with
/// `provider_network_mismatch` before any listing.
async fn check_network(asker: &Asker, profile: &Profile) -> Result<(), Error> {
    let Some(expected) = network_magic(&profile.network) else {
        return Ok(());
    };
    let provider = asker.provider.clone();
    let genesis = asker.ask("genesis", || provider.genesis()).await?;
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
/// its requests and its error as the stopped service state `status` prints.
pub async fn rebuild(context: Context<'_>) -> Result<Report, Error> {
    let connect = Arc::new(context.connect);
    let recorder = Arc::clone(&connect);
    let provider = context.provider.clone();
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
            let _ = store.record_calls(&provider.counts()).await;
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
        time,
        stop,
        fatal_after: settings.fatal_after,
    };
    let mut store = connect().await?;
    store.lock_writer().await?;
    let (plan, phase, resumed) = match store.building(profile).await? {
        Building::Empty => {
            check_network(&asker, profile).await?;
            let plan = plan(&asker, profile, &options).await?;
            store.start_building(profile, &to_value(&plan)?).await?;
            (plan, "listing".to_owned(), false)
        }
        Building::Stopped { plan, phase, .. } => {
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
            // The requests of the earlier runs, as they recorded them per day,
            // before this run sends one.
            provider.seed_counts(&store.calls(NaiveDate::default()).await?);
            check_network(&asker, profile).await?;
            (plan, phase, true)
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
        connect,
        plan: plan.clone(),
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
                counts = build.base().await?;
                build.phase = "window".to_owned();
            }
            "window" => build.window(&settings, &progress).await?,
            "publish" => {}
            _ => {}
        }
    }
    let (verified, calls) = build.publish().await?;
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
