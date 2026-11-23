//! Requests sent per UTC day, kept in memory. The caller persists them with
//! its own updates and seeds them again at start.

use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDate};

/// Days kept, today included.
const DAYS: usize = 8;

pub(crate) fn utc_day(unix_seconds: u64) -> NaiveDate {
    i64::try_from(unix_seconds)
        .ok()
        .and_then(|seconds| DateTime::from_timestamp(seconds, 0))
        .map_or(NaiveDate::MIN, |time| time.date_naive())
}

#[derive(Default)]
pub(crate) struct Counts(BTreeMap<NaiveDate, i64>);

impl Counts {
    pub(crate) fn count(&mut self, unix_seconds: u64) {
        *self.0.entry(utc_day(unix_seconds)).or_default() += 1;
        self.trim();
    }

    /// Keep the larger of the held and the seeded count for each day.
    pub(crate) fn seed(&mut self, counts: &[(NaiveDate, i64)]) {
        for (day, count) in counts {
            let held = self.0.entry(*day).or_default();
            *held = (*held).max(*count);
        }
        self.trim();
    }

    pub(crate) fn get(&self) -> Vec<(NaiveDate, i64)> {
        self.0.iter().map(|(day, count)| (*day, *count)).collect()
    }

    fn trim(&mut self) {
        while self.0.len() > DAYS {
            self.0.pop_first();
        }
    }
}
