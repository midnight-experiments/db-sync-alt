//! Waiting: the clock the follower reads and sleeps on, the stop request that
//! ends every wait at once, and the backoff of a repeated temporary failure.

use std::{future::Future, pin::Pin, time::Duration};

use tokio::sync::watch;

/// The waits of a repeated temporary failure, in seconds; the last repeats.
pub const BACKOFF: [u64; 6] = [1, 2, 4, 8, 16, 30];

/// UTC time and sleeping. The provider client reads the same clock, so a
/// refusal's resume time and the follower's waits agree; tests inject a
/// simulated one.
pub trait Time: dsa_provider::Clock {
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

/// The system clock and the runtime's timers.
pub struct WallTime;

impl dsa_provider::Clock for WallTime {
    fn unix_seconds(&self) -> u64 {
        dsa_provider::SystemClock.unix_seconds()
    }
}

impl Time for WallTime {
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(tokio::time::sleep(duration))
    }
}

/// A stop request shared by every wait of the follower.
#[derive(Clone, Debug)]
pub struct Stop(watch::Receiver<bool>);

impl Stop {
    /// A stop and the sender that requests it.
    pub fn new() -> (watch::Sender<bool>, Self) {
        let (sender, receiver) = watch::channel(false);
        (sender, Self(receiver))
    }

    pub fn requested(&self) -> bool {
        *self.0.borrow()
    }

    /// Resolves when the stop is requested; never when its sender is gone
    /// without a request.
    pub async fn wait(&self) {
        let mut receiver = self.0.clone();
        if receiver.wait_for(|stop| *stop).await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

/// Sleeps for `duration`; `false` when the stop came first.
pub async fn pause(time: &dyn Time, stop: &Stop, duration: Duration) -> bool {
    if stop.requested() {
        return false;
    }
    tokio::select! {
        () = time.sleep(duration) => !stop.requested(),
        () = stop.wait() => false,
    }
}

/// Sleeps until the clock reads `until` (Unix seconds); `false` when the
/// stop came first.
pub async fn pause_until(time: &dyn Time, stop: &Stop, until: u64) -> bool {
    loop {
        let now = time.unix_seconds();
        if now >= until {
            return !stop.requested();
        }
        if !pause(time, stop, Duration::from_secs(until - now)).await {
            return false;
        }
    }
}

/// Consecutive failures of one condition: the wait before the next attempt
/// and how long the condition has lasted.
#[derive(Debug, Default)]
pub struct Backoff {
    failures: usize,
    since: Option<u64>,
}

impl Backoff {
    /// Records a failure at `now` (Unix seconds) and returns the wait before
    /// the next attempt.
    pub fn failed(&mut self, now: u64) -> Duration {
        let wait = BACKOFF[self.failures.min(BACKOFF.len() - 1)];
        self.failures += 1;
        self.since.get_or_insert(now);
        Duration::from_secs(wait)
    }

    /// Seconds since the first failure of the condition.
    pub fn lasted(&self, now: u64) -> u64 {
        self.since.map_or(0, |since| now.saturating_sub(since))
    }

    pub fn failures(&self) -> usize {
        self.failures
    }

    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_backoff_doubles_to_thirty_seconds_and_measures_the_condition() {
        let mut backoff = Backoff::default();
        let waits: Vec<u64> = (0..8)
            .map(|step| backoff.failed(1_000 + step).as_secs())
            .collect();
        assert_eq!(waits, [1, 2, 4, 8, 16, 30, 30, 30]);
        assert_eq!(backoff.lasted(1_100), 100);
        backoff.reset();
        assert_eq!(backoff.lasted(5_000), 0);
        assert_eq!(backoff.failed(5_000).as_secs(), 1);
    }

    #[tokio::test]
    async fn a_stop_ends_every_wait_at_once() {
        let (sender, stop) = Stop::new();
        let waiting = tokio::spawn({
            let stop = stop.clone();
            async move { pause(&WallTime, &stop, Duration::from_secs(3_600)).await }
        });
        // The wait has begun before the stop.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!waiting.is_finished());
        sender.send(true).unwrap();
        let finished = tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .expect("the wait ends")
            .unwrap();
        assert!(!finished);
        assert!(!pause_until(&WallTime, &stop, u64::MAX).await);
    }
}
