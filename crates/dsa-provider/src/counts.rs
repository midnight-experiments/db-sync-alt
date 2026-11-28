//! Requests sent per UTC day, kept in memory. The caller persists them with
//! its own updates and seeds them again at start. Requests sent per endpoint
//! family since the client was made, counted with the day.

use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDate};

/// Days kept, today included.
const DAYS: usize = 8;

/// The endpoint family a request is counted in.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RequestFamily {
    /// `/assets/policy/{policy}`, `/assets/{unit}/utxos`,
    /// `/assets/{unit}/transactions`, `/addresses/{address}/utxos` and
    /// `/addresses/{address}/transactions`.
    Listings,
    /// `/assets/{unit}` and `/assets/{unit}/history`.
    Assets,
    /// `/blocks/latest`, `/blocks/{height}`, `/blocks/{hash}` and
    /// `/blocks/{hash}/next`.
    Headers,
    /// `/txs/{hash}` and `/blocks/{hash}/txs/cbor`.
    Transactions,
    /// `/txs/{hash}/utxos`.
    Outputs,
    /// `/scripts/datum/{hash}/cbor`.
    Datums,
    /// `/txs/{hash}/metadata`.
    Metadata,
    /// `/epochs/{epoch}`, its parameters, blocks and stakes.
    Epochs,
    /// `/pools` and the pool endpoints.
    Pools,
    /// `/genesis`.
    Genesis,
}

impl RequestFamily {
    /// The family's name in reports.
    pub fn name(self) -> &'static str {
        match self {
            Self::Listings => "listings",
            Self::Assets => "assets",
            Self::Headers => "headers",
            Self::Transactions => "transactions",
            Self::Outputs => "outputs",
            Self::Datums => "datums",
            Self::Metadata => "metadata",
            Self::Epochs => "epochs",
            Self::Pools => "pools",
            Self::Genesis => "genesis",
        }
    }
}

pub(crate) fn utc_day(unix_seconds: u64) -> NaiveDate {
    i64::try_from(unix_seconds)
        .ok()
        .and_then(|seconds| DateTime::from_timestamp(seconds, 0))
        .map_or(NaiveDate::MIN, |time| time.date_naive())
}

#[derive(Default)]
pub(crate) struct Counts {
    days: BTreeMap<NaiveDate, i64>,
    families: BTreeMap<RequestFamily, u64>,
}

impl Counts {
    /// One request sent now.
    pub(crate) fn count(&mut self, unix_seconds: u64, family: RequestFamily) {
        *self.days.entry(utc_day(unix_seconds)).or_default() += 1;
        *self.families.entry(family).or_default() += 1;
        self.trim();
    }

    /// Keep the larger of the held and the seeded count for each day.
    pub(crate) fn seed(&mut self, counts: &[(NaiveDate, i64)]) {
        for (day, count) in counts {
            let held = self.days.entry(*day).or_default();
            *held = (*held).max(*count);
        }
        self.trim();
    }

    pub(crate) fn get(&self) -> Vec<(NaiveDate, i64)> {
        self.days
            .iter()
            .map(|(day, count)| (*day, *count))
            .collect()
    }

    pub(crate) fn families(&self) -> BTreeMap<String, u64> {
        self.families
            .iter()
            .map(|(family, count)| (family.name().to_owned(), *count))
            .collect()
    }

    fn trim(&mut self) {
        while self.days.len() > DAYS {
            self.days.pop_first();
        }
    }
}
