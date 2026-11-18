use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::Serialize;

use crate::Error;

/// UTC attempt time can be injected independently of monotonic request pacing.
pub trait Clock: Send + Sync {
    fn unix_seconds(&self) -> u64;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn unix_seconds(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct DailyCount {
    pub utc_day: String,
    pub requests: u64,
}

#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("provider_accounting_unavailable")]
pub struct RecorderError;

pub type RecordFuture<'a> = Pin<Box<dyn Future<Output = Result<(), RecorderError>> + Send + 'a>>;

/// Persist the new absolute total before a send attempt begins. A reservation
/// remains counted if cancellation or an in-flight refusal prevents the send.
/// The database's single writer owns these totals. Every Provider clone shares
/// the same serialized reservation path. A failed persistence call sends nothing.
pub trait AttemptRecorder: Send + Sync {
    fn record(&self, count: DailyCount) -> RecordFuture<'_>;
}

pub(crate) struct Counts {
    days: BTreeMap<i64, u64>,
}

pub(crate) fn day(now: u64) -> Result<i64, Error> {
    let seconds = i64::try_from(now).map_err(|_| Error::retry("provider_clock_invalid", now))?;
    time::OffsetDateTime::from_unix_timestamp(seconds)
        .map_err(|_| Error::retry("provider_clock_invalid", now))?;
    Ok(seconds / 86400)
}

fn date(day: i64) -> String {
    time::OffsetDateTime::from_unix_timestamp(day * 86400)
        .expect("validated UTC day")
        .date()
        .to_string()
}

impl Counts {
    pub(crate) fn new(seed: Vec<DailyCount>, now: u64) -> Result<Self, Error> {
        let today = day(now)?;
        let format = time::format_description::parse_borrowed::<3>("[year]-[month]-[day]")
            .expect("constant date format");
        let mut days = BTreeMap::new();
        for count in seed {
            let parsed = time::Date::parse(&count.utc_day, &format)
                .map_err(|_| Error::configuration("provider.initial_counts"))?;
            let index = parsed
                .midnight()
                .assume_utc()
                .unix_timestamp()
                .div_euclid(86400);
            if index >= today - 7 && days.insert(index, count.requests).is_some() {
                return Err(Error::configuration("provider.initial_counts"));
            }
        }
        let mut result = Self { days };
        result.trim(today);
        Ok(result)
    }

    fn trim(&mut self, today: i64) {
        let newest = self
            .days
            .last_key_value()
            .map_or(today, |(day, _)| today.max(*day));
        self.days.retain(|day, _| *day >= newest - 7);
    }

    pub(crate) fn next(&self, now: u64) -> Result<DailyCount, Error> {
        let index = day(now)?;
        if self
            .days
            .last_key_value()
            .is_some_and(|(latest, _)| index < latest - 7)
        {
            return Err(Error::retry(
                "provider_clock_invalid",
                now.saturating_add(1),
            ));
        }
        let requests = self
            .days
            .get(&index)
            .copied()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| {
                Error::retry("provider_accounting_unavailable", now.saturating_add(1))
            })?;
        Ok(DailyCount {
            utc_day: date(index),
            requests,
        })
    }

    pub(crate) fn committed(&mut self, now: u64, count: u64) {
        let index = day(now).expect("validated attempt time");
        self.days.insert(index, count);
        self.trim(index);
    }

    pub(crate) fn snapshot(&self, now: u64) -> Result<(String, u64, Vec<DailyCount>), Error> {
        let today = day(now)?;
        let previous = (1..=7)
            .map(|offset| {
                let index = today - offset;
                DailyCount {
                    utc_day: date(index),
                    requests: self.days.get(&index).copied().unwrap_or(0),
                }
            })
            .collect();
        Ok((
            date(today),
            self.days.get(&today).copied().unwrap_or(0),
            previous,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_roll_over_without_a_local_allowance() {
        let mut counts = Counts::new(
            vec![DailyCount {
                utc_day: "2026-11-18".to_owned(),
                requests: 2000000,
            }],
            1795046399,
        )
        .unwrap();
        let reservation = counts.next(1795046399).unwrap();
        assert_eq!(reservation.requests, 2000001);
        counts.committed(1795046399, reservation.requests);
        let next = counts.next(1795046400).unwrap();
        assert_eq!(next.utc_day, "2026-11-19");
        assert_eq!(next.requests, 1);
        counts.committed(1795046400, next.requests);
        let (_, today, prior) = counts.snapshot(1795046400).unwrap();
        assert_eq!(today, 1);
        assert_eq!(prior.len(), 7);
        assert_eq!(prior[0].requests, 2000001);
    }
}
