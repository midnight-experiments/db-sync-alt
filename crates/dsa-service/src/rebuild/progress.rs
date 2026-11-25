//! What a build stores to continue: the phases in their order, the cursors
//! of steps read in more than one request, and the progress row with the
//! service state that `status` prints.

use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use dsa_provider::Provider;
use dsa_store::Store;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

use super::{Build, Plan, asker::refusal};
use crate::follow::Error;

/// The progress is recorded at least this often while a phase runs.
const RECORD_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(super) struct Cursor {
    pub(super) from: u32,
    pub(super) page: u32,
    pub(super) done: bool,
}

pub(super) fn sent(provider: &Provider) -> i64 {
    provider.counts().iter().map(|(_, count)| count).sum()
}

pub(super) const PHASES: [&str; 14] = [
    "positions",
    "listing",
    "quantities",
    "since",
    "transactions",
    "addresses",
    "history",
    "validity",
    "datums",
    "carried",
    "epochs",
    "base",
    "window",
    "publish",
];

pub(super) fn phase_index(phase: &str) -> usize {
    PHASES.iter().position(|p| *p == phase).unwrap_or(0)
}

pub(super) fn to_value(value: impl Serialize) -> Result<Value, Error> {
    serde_json::to_value(value).map_err(|_| refusal("rebuild_progress_invalid", json!({})))
}

pub(super) fn parse<T: DeserializeOwned>(value: Value) -> Result<T, Error> {
    serde_json::from_value(value).map_err(|_| refusal("rebuild_progress_invalid", json!({})))
}

/// The detail of a progress record: the requests so far and the work left,
/// with an estimate of the requests still to send of one per transaction to
/// read and one per window block to make final. Empty blocks cost less, the
/// stake distributions at the window's end more.
pub(super) fn detail(
    phase: &str,
    calls: i64,
    plan: &Plan,
    transactions: i64,
    window_blocks: u32,
) -> Value {
    json!({"phase": phase, "provider_requests": calls,
        "horizon": plan.horizon, "target": plan.target,
        "remaining": {"transactions_to_read": transactions, "window_blocks": window_blocks},
        "expected_remaining_requests": transactions + i64::from(window_blocks)})
}

impl Build<'_> {
    /// The provider requests of the whole build: a continued build's
    /// provider starts from the counts the database holds.
    pub(super) fn calls(&self) -> i64 {
        sent(&self.asker.provider)
    }

    pub(super) async fn store(&mut self) -> Result<&mut Store, Error> {
        if self.store.is_none() {
            self.store = Some((self.connect)().await?);
        }
        Ok(self.store.as_mut().expect("connected"))
    }

    pub(super) async fn items(&mut self, kind: &str) -> Result<BTreeMap<String, Value>, Error> {
        Ok(self.store().await?.building_items(kind).await?)
    }

    pub(super) async fn typed<T: DeserializeOwned>(
        &mut self,
        kind: &str,
    ) -> Result<BTreeMap<String, T>, Error> {
        self.items(kind)
            .await?
            .into_iter()
            .map(|(key, value)| Ok((key, parse(value)?)))
            .collect()
    }

    pub(super) async fn put(
        &mut self,
        kind: &str,
        items: Vec<(String, Value)>,
    ) -> Result<(), Error> {
        for chunk in items.chunks(self.batch) {
            self.store().await?.put_building_items(kind, chunk).await?;
        }
        if self.recorded.elapsed() >= RECORD_INTERVAL {
            let phase = self.phase.clone();
            self.reach(&phase).await?;
        }
        Ok(())
    }

    pub(super) async fn put_one(
        &mut self,
        kind: &str,
        key: &str,
        value: impl Serialize,
    ) -> Result<(), Error> {
        self.put(kind, vec![(key.to_owned(), to_value(value)?)])
            .await
    }

    /// Records the phase the build reached with its calls so far and the
    /// work it still expects, in its progress row, in the request counts per
    /// day and as the service state that `status` prints.
    pub(super) async fn reach(&mut self, phase: &str) -> Result<(), Error> {
        let calls = self.calls();
        let per_day = self.asker.provider.counts();
        let (counts, target) = self.store().await?.building_counts().await?;
        let count = |kind: &str| counts.get(kind).copied().unwrap_or(0);
        let window = target.map_or(self.plan.target - self.plan.horizon + 1, |stored| {
            self.plan
                .target
                .saturating_sub(stored.max(self.plan.horizon - 1))
        });
        let transactions = (count("since") - count("moved")).max(0);
        let detail = detail(phase, calls, &self.plan, transactions, window);
        let run = json!(self.asker.requests.get());
        let store = self.store().await?;
        store.record_calls(&per_day).await?;
        store
            .put_building_items("requests", &[("run".to_owned(), run)])
            .await?;
        store.record_building(phase, &detail, calls).await?;
        store
            .record_state("rebuilding", phase, &detail, None)
            .await?;
        self.phase = phase.to_owned();
        self.recorded = Instant::now();
        self.progress
            .send_modify(|progress| progress.reason = phase.to_owned());
        eprintln!(
            "{}",
            json!({"event":"rebuild_phase","phase":phase,"calls":calls,"detail":detail})
        );
        Ok(())
    }

    pub(super) async fn cursor(&mut self, key: &str) -> Result<Cursor, Error> {
        let stored = self.items("cursor").await?;
        stored
            .get(key)
            .cloned()
            .map_or(Ok(Cursor::default()), parse)
    }

    /// Marks a step whose items are stored in more than one statement as
    /// complete.
    pub(super) async fn finished(&mut self, key: &str) -> Result<(), Error> {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The remaining requests are an estimate, not a bound: on Preprod a
    /// window of 34,653 blocks cost 25,924 requests with its stake.
    #[test]
    fn the_progress_record_estimates_the_remaining_requests() {
        let plan = Plan {
            network: "preprod".to_owned(),
            horizon: 100,
            target: 199,
            tip: 2359,
            window_seconds: 604_800,
            retention_seconds: 604_800,
            version: "0".to_owned(),
            flags: super::super::Flags {
                horizon_block: None,
                target_block: None,
                window_seconds: None,
                retention_seconds: 604_800,
            },
        };
        let value = detail("window", 566, &plan, 35, 100);
        assert_eq!(
            value,
            json!({"phase": "window", "provider_requests": 566, "horizon": 100, "target": 199,
                "remaining": {"transactions_to_read": 35, "window_blocks": 100},
                "expected_remaining_requests": 135})
        );
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
