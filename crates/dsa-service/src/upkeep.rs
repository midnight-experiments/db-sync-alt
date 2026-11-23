//! The service's upkeep besides following: the supply check every six
//! hours, the rebuild of an index that has grown well past its size after
//! the last rebuild, and the daily snapshot file. Each is a store function
//! and a pure decision on an injected clock (Unix seconds); the functions
//! here join them. None of them holds the writer's transaction: the supply
//! check reads in its own snapshot, a rebuild and a snapshot file run on
//! connections of their own.

use std::{
    fs,
    path::{Path, PathBuf},
};

use chrono::{DateTime, NaiveTime};
use dsa_chain::profiles::Profile;
use dsa_store::{
    Error, Store,
    upkeep::{IndexSize, Rebuild, SnapshotFile, SupplyCheck},
};
use serde_json::json;

use crate::config::Snapshots;

/// Seconds between two supply checks; the first runs at start.
pub const SUPPLY_INTERVAL: u64 = 6 * 3600;
/// Seconds between two looks at the index sizes; the first runs at start.
pub const INDEX_INTERVAL: u64 = 3600;
/// Seconds before a failed snapshot file is tried again.
pub const SNAPSHOT_RETRY: u64 = 600;
/// The reason the gate closes when the supply check fails.
pub const SUPPLY_MISMATCH: &str = "supply_mismatch";

/// When an index is rebuilt: when it is more than `growth` times its size
/// after the last rebuild and larger than `min_bytes`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct IndexPolicy {
    pub growth: f64,
    pub min_bytes: i64,
}

impl Default for IndexPolicy {
    /// More than twice the size after the last rebuild, and above 8 MiB.
    fn default() -> Self {
        Self {
            growth: 2.0,
            min_bytes: 8 * 1024 * 1024,
        }
    }
}

/// What to do about the indexes now.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IndexDecision {
    /// Indexes without a record: their current size becomes their size
    /// after the last rebuild.
    pub baselines: Vec<String>,
    /// The one index to rebuild: the one grown most past its record.
    pub rebuild: Option<String>,
}

/// The decision for the indexes of `sizes` under `policy`; one rebuild at a
/// time.
pub fn index_decision(sizes: &[IndexSize], policy: &IndexPolicy) -> IndexDecision {
    let mut decision = IndexDecision::default();
    let mut largest = 0.0;
    for index in sizes {
        let Some(rebuilt) = index.rebuilt else {
            decision.baselines.push(index.name.clone());
            continue;
        };
        let growth = index.bytes as f64 / rebuilt.bytes.max(1) as f64;
        if index.bytes > policy.min_bytes && growth > policy.growth && growth > largest {
            largest = growth;
            decision.rebuild = Some(index.name.clone());
        }
    }
    decision
}

/// The latest time of day `at` (UTC) at or before `now`.
fn latest_slot(at: NaiveTime, now: u64) -> u64 {
    let now = i64::try_from(now).unwrap_or(i64::MAX);
    let today = DateTime::from_timestamp(now, 0)
        .unwrap_or_default()
        .date_naive()
        .and_time(at)
        .and_utc()
        .timestamp();
    let slot = if today <= now { today } else { today - 86_400 };
    u64::try_from(slot).unwrap_or(0)
}

/// Whether the daily snapshot file is due: no file was written since the
/// latest `at`, and no attempt failed in the last ten minutes. A service
/// started after `at` without that day's file writes it at once; a service
/// that missed several days writes one file.
pub fn snapshot_due(
    at: NaiveTime,
    last_written: Option<u64>,
    last_failed: Option<u64>,
    now: u64,
) -> bool {
    let slot = latest_slot(at, now);
    if last_written.is_some_and(|written| written >= slot) {
        return false;
    }
    !last_failed.is_some_and(|failed| failed >= slot && now < failed + SNAPSHOT_RETRY)
}

/// One job of the upkeep.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Job {
    Supply,
    Indexes,
    Snapshot,
}

/// What a running service remembers of its upkeep (Unix seconds); the time
/// of the last written snapshot file comes from the database.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Schedule {
    pub supply_checked: Option<u64>,
    pub indexes_checked: Option<u64>,
    pub snapshot_failed: Option<u64>,
}

impl Schedule {
    /// The jobs due at `now`, in the order to run them.
    pub fn due(
        &self,
        snapshots: Option<&Snapshots>,
        last_written: Option<u64>,
        now: u64,
    ) -> Vec<Job> {
        let mut due = Vec::new();
        if self
            .supply_checked
            .is_none_or(|checked| now >= checked + SUPPLY_INTERVAL)
        {
            due.push(Job::Supply);
        }
        if self
            .indexes_checked
            .is_none_or(|checked| now >= checked + INDEX_INTERVAL)
        {
            due.push(Job::Indexes);
        }
        if let Some(snapshots) = snapshots
            && snapshot_due(snapshots.at, last_written, self.snapshot_failed, now)
        {
            due.push(Job::Snapshot);
        }
        due
    }
}

/// The supply check on the writer's connection: it is recorded, and a
/// failed check closes the gate with `supply_mismatch`, naming the unit.
pub async fn check_supply(store: &mut Store, profile: &Profile) -> Result<SupplyCheck, Error> {
    let check = store.check_supply(profile).await?;
    if !check.passed {
        let detail = json!({"failure": check.failure, "unspent_outputs": check.unspent_outputs});
        store.close_gate(SUPPLY_MISMATCH, &detail).await?;
    }
    Ok(check)
}

/// At start: drops what an interrupted rebuild left.
pub async fn start_indexes(store: &Store) -> Result<Vec<String>, Error> {
    store.drop_interrupted_rebuilds().await
}

/// Records missing index sizes and rebuilds the index the policy names, if
/// any, on `store`, a connection of its own.
pub async fn rebuild_indexes(
    store: &Store,
    policy: &IndexPolicy,
) -> Result<Option<Rebuild>, Error> {
    let sizes = store.index_sizes().await?;
    let decision = index_decision(&sizes, policy);
    store.record_index_baselines(&sizes).await?;
    match decision.rebuild {
        Some(name) => Ok(Some(store.rebuild_index(&name).await?)),
        None => Ok(None),
    }
}

/// The name of a daily snapshot file.
pub fn snapshot_name(network: &str, target: u32, content_digest: &str) -> String {
    let digest = content_digest.get(..12).unwrap_or(content_digest);
    format!("{network}-{target}-{digest}.snapshot")
}

/// The target height of a daily snapshot file of `network`, from its name.
fn snapshot_target(network: &str, name: &str) -> Option<u32> {
    let rest = name.strip_prefix(network)?.strip_prefix('-')?;
    let rest = rest.strip_suffix(".snapshot")?;
    let (target, digest) = rest.split_once('-')?;
    let lowercase_hex = digest.len() == 12
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
    (lowercase_hex && !target.is_empty() && target.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| target.parse().ok())
        .flatten()
}

/// The daily files of `network` among `names` beyond the newest `keep`, by
/// target height.
pub fn files_to_remove(network: &str, names: &[String], keep: u32) -> Vec<String> {
    let mut files = names
        .iter()
        .filter_map(|name| snapshot_target(network, name).map(|target| (target, name.clone())))
        .collect::<Vec<_>>();
    files.sort_by(|a, b| b.cmp(a));
    files
        .into_iter()
        .skip(keep as usize)
        .map(|(_, name)| name)
        .collect()
}

/// The prefix of the temporary directory a daily file is written in.
fn staging_prefix(network: &str) -> String {
    format!(".{network}-snapshot-")
}

fn names(dir: &Path) -> Result<Vec<String>, Error> {
    let mut names = Vec::new();
    for entry in fs::read_dir(dir)? {
        if let Ok(name) = entry?.file_name().into_string() {
            names.push(name);
        }
    }
    Ok(names)
}

/// Writes the daily snapshot file of `store` into `snapshots.dir` on the
/// store's own connection: first under a temporary name, then as
/// `<network>-<target>-<first 12 digits of the content digest>.snapshot`.
/// The file is recorded in `dsa.upkeep`; then the files of the network
/// beyond the newest `keep` and stale temporary directories are removed.
/// A failure leaves the older files as they were.
pub async fn write_snapshot(
    store: &mut Store,
    profile: &Profile,
    snapshots: &Snapshots,
) -> Result<SnapshotFile, Error> {
    let network = profile.network.as_str();
    let prefix = staging_prefix(network);
    for name in names(&snapshots.dir)? {
        let path = snapshots.dir.join(&name);
        if name.starts_with(&prefix) && path.is_dir() {
            fs::remove_dir_all(path)?;
        }
    }
    let staging = tempfile::Builder::new()
        .prefix(&prefix)
        .tempdir_in(&snapshots.dir)?;
    let written = staging.path().join("snapshot");
    let description = store.snapshot(profile, &written).await?;
    let verified = &description.verified;
    let name = snapshot_name(
        network,
        verified.state.target.height,
        &verified.content_digest,
    );
    let path: PathBuf = snapshots.dir.join(&name);
    // A file of this name holds the same target and content: keep it.
    match fs::hard_link(&written, &path) {
        Ok(()) => fs::File::open(&snapshots.dir)?.sync_all()?,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
    }
    let file = SnapshotFile {
        file: name,
        content_digest: verified.content_digest.clone(),
        target: verified.state.target.height,
        bytes: fs::metadata(&path)?.len(),
    };
    store.record_snapshot_file(&file).await?;
    drop(staging);
    for old in files_to_remove(network, &names(&snapshots.dir)?, snapshots.keep) {
        fs::remove_file(snapshots.dir.join(old))?;
    }
    Ok(file)
}

#[cfg(test)]
mod tests {
    use dsa_store::upkeep::Rebuilt;

    use super::*;

    const MIB: i64 = 1024 * 1024;

    fn size(name: &str, bytes: i64, rebuilt: Option<i64>) -> IndexSize {
        IndexSize {
            name: name.to_owned(),
            bytes,
            rebuilt: rebuilt.map(|bytes| Rebuilt { at: 0, bytes }),
        }
    }

    #[test]
    fn an_index_is_rebuilt_when_large_and_twice_its_rebuilt_size() {
        let policy = IndexPolicy::default();
        let decision = index_decision(
            &[
                size("a", 200 * MIB, Some(90 * MIB)),
                size("b", 300 * MIB, Some(100 * MIB)),
                size("c", 7 * MIB, Some(MIB)),
                size("d", 100 * MIB, Some(60 * MIB)),
                size("e", 5 * MIB, None),
            ],
            &policy,
        );
        // b grew 3x, a 2.2x; c grew 7x but is below 8 MiB; d grew only 1.67x.
        assert_eq!(decision.rebuild.as_deref(), Some("b"));
        assert_eq!(decision.baselines, vec!["e".to_owned()]);
        let decision = index_decision(&[size("a", 128 * MIB, Some(64 * MIB))], &policy);
        assert_eq!(decision.rebuild, None, "exactly twice is not more");
        let decision = index_decision(&[size("a", 65 * MIB, None)], &policy);
        assert_eq!(decision.rebuild, None, "no record: the size becomes it");
        // The floor is 8 MiB: an index just above it that has more than
        // doubled is rebuilt; one of exactly 8 MiB is not.
        let decision = index_decision(&[size("a", 9 * MIB, Some(4 * MIB))], &policy);
        assert_eq!(decision.rebuild.as_deref(), Some("a"));
        let decision = index_decision(&[size("a", 8 * MIB, Some(MIB))], &policy);
        assert_eq!(decision.rebuild, None, "exactly 8 MiB is not larger");
    }

    fn at(hours: u32, minutes: u32) -> NaiveTime {
        NaiveTime::from_hms_opt(hours, minutes, 0).unwrap()
    }

    /// 2026-11-18 00:00:00 UTC.
    const DAY: u64 = 1_794_960_000;
    const HOUR: u64 = 3600;

    #[test]
    fn the_snapshot_file_is_written_once_a_day_at_its_time() {
        let three = at(3, 0);
        // Before the time of day, yesterday's file is the latest due one.
        assert!(!snapshot_due(
            three,
            Some(DAY - 21 * HOUR),
            None,
            DAY + 2 * HOUR
        ));
        assert!(snapshot_due(
            three,
            Some(DAY - 21 * HOUR),
            None,
            DAY + 3 * HOUR
        ));
        // A start after the time of day without that day's file writes it.
        assert!(snapshot_due(
            three,
            Some(DAY - 21 * HOUR),
            None,
            DAY + 10 * HOUR
        ));
        assert!(snapshot_due(three, None, None, DAY + 10 * HOUR));
        // Once written, not again that day.
        assert!(!snapshot_due(
            three,
            Some(DAY + 10 * HOUR),
            None,
            DAY + 23 * HOUR
        ));
        assert!(snapshot_due(
            three,
            Some(DAY + 10 * HOUR),
            None,
            DAY + 27 * HOUR
        ));
        // A day without a run: one file at the next look, not one per day.
        let back = DAY + 3 * 24 * HOUR + 5 * HOUR;
        assert!(snapshot_due(three, Some(DAY + 3 * HOUR), None, back));
        assert!(!snapshot_due(three, Some(back), None, back + HOUR));
        // A failure is tried again after ten minutes.
        let failed = DAY + 3 * HOUR + 10;
        assert!(!snapshot_due(
            three,
            Some(DAY - 21 * HOUR),
            Some(failed),
            failed + 599
        ));
        assert!(snapshot_due(
            three,
            Some(DAY - 21 * HOUR),
            Some(failed),
            failed + 600
        ));
        // Midnight and the last minute of the day.
        assert!(snapshot_due(at(0, 0), Some(DAY - 1), None, DAY));
        assert!(!snapshot_due(at(23, 59), Some(DAY - 60), None, DAY + 10));
    }

    #[test]
    fn the_schedule_names_the_due_jobs() {
        let snapshots = Snapshots {
            dir: PathBuf::from("."),
            keep: 2,
            at: at(3, 0),
        };
        let start = Schedule::default();
        assert_eq!(
            start.due(Some(&snapshots), Some(DAY + 4 * HOUR), DAY + 5 * HOUR),
            vec![Job::Supply, Job::Indexes]
        );
        assert_eq!(start.due(None, None, DAY), vec![Job::Supply, Job::Indexes]);
        let running = Schedule {
            supply_checked: Some(DAY),
            indexes_checked: Some(DAY + 4 * HOUR),
            snapshot_failed: None,
        };
        assert_eq!(
            running.due(Some(&snapshots), Some(DAY + 4 * HOUR), DAY + 6 * HOUR - 1),
            vec![Job::Indexes]
        );
        assert_eq!(
            running.due(Some(&snapshots), Some(DAY + 4 * HOUR), DAY + 6 * HOUR),
            vec![Job::Supply, Job::Indexes]
        );
        assert_eq!(
            running.due(Some(&snapshots), Some(DAY + 4 * HOUR), DAY + 27 * HOUR),
            vec![Job::Supply, Job::Indexes, Job::Snapshot]
        );
    }

    #[test]
    fn only_the_newest_files_of_the_network_are_kept() {
        let names = [
            "preprod-100-0123456789ab.snapshot",
            "preprod-300-0123456789ab.snapshot",
            "preprod-200-aaaaaaaaaaaa.snapshot",
            "preprod-1000-bbbbbbbbbbbb.snapshot",
            "mainnet-50-0123456789ab.snapshot",
            "preprod-400-0123456789AB.snapshot",
            "preprod-400-0123456789ab.snapshot.tmp",
            "preprod-x-0123456789ab.snapshot",
            ".preprod-snapshot-abc",
            "notes.txt",
        ]
        .map(str::to_owned);
        assert_eq!(
            files_to_remove("preprod", &names, 2),
            vec![
                "preprod-200-aaaaaaaaaaaa.snapshot".to_owned(),
                "preprod-100-0123456789ab.snapshot".to_owned()
            ]
        );
        assert!(files_to_remove("preprod", &names, 4).is_empty());
        assert_eq!(files_to_remove("mainnet", &names, 1), Vec::<String>::new());
        assert_eq!(
            snapshot_name("preprod", 4_200, &"0123456789abcdef".repeat(4)),
            "preprod-4200-0123456789ab.snapshot"
        );
    }
}
