//! The service's own settings. Every value is checked before anything
//! connects, and a refusal names the flag that carries the value. There is
//! no configuration file: values come from flags or their `DSA_`
//! environment variables, and secrets only ever as file paths. The
//! `--snapshot` file is not opened here: it is read only when a restore is
//! needed, so a start on a serving database does not need it.

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use chrono::NaiveTime;
use serde::Serialize;

/// The smallest retention: 24 hours.
pub const MIN_RETENTION_SECONDS: i64 = 86_400;
/// The shortest time a database may stay unreachable before the service
/// ends.
pub const MIN_DATABASE_FATAL_AFTER_SECONDS: u64 = 60;
/// How long the service waits for the writer lock that another session
/// holds, at start and on a reconnect, before it refuses to run: a session of
/// a process that is gone keeps the lock until the server ends it.
pub const WRITER_BUSY_AFTER_SECONDS: u64 = 120;

/// The values as given, before they are checked.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Input {
    pub listen: String,
    pub reader: String,
    pub snapshot: Option<PathBuf>,
    pub expect_content_digest: Option<String>,
    pub retention_seconds: Option<i64>,
    pub change_retention: bool,
    pub snapshot_dir: Option<PathBuf>,
    pub snapshot_keep: u32,
    pub snapshot_at: String,
    pub health_max_lag_blocks: u32,
    pub database_fatal_after_seconds: u64,
}

/// The snapshot file to restore into an empty database.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Restore {
    pub file: PathBuf,
    pub expect_content_digest: Option<String>,
}

/// A retention to apply; `change` allows it to differ from the stored one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct Retention {
    pub seconds: i64,
    pub change: bool,
}

/// The daily snapshot files.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Snapshots {
    pub dir: PathBuf,
    /// The newest files of the network kept.
    pub keep: u32,
    /// The UTC time of day of the daily file.
    pub at: NaiveTime,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Settings {
    pub listen: SocketAddr,
    pub reader: String,
    pub restore: Option<Restore>,
    pub retention: Option<Retention>,
    pub snapshots: Option<Snapshots>,
    pub health_max_lag_blocks: u32,
    pub database_fatal_after: Duration,
    /// See [`WRITER_BUSY_AFTER_SECONDS`].
    pub writer_busy_after: Duration,
}

/// A refused value: the error code and the flag that carries the value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Invalid {
    pub error: &'static str,
    pub flag: &'static str,
}

fn invalid(error: &'static str, flag: &'static str) -> Invalid {
    Invalid { error, flag }
}

fn content_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// `HH:MM`, both two digits.
fn time_of_day(value: &str) -> Option<NaiveTime> {
    let (hours, minutes) = value.split_once(':')?;
    if hours.len() != 2 || minutes.len() != 2 {
        return None;
    }
    NaiveTime::from_hms_opt(hours.parse().ok()?, minutes.parse().ok()?, 0)
}

/// A directory that exists and takes a new file.
fn writable_directory(path: &Path) -> bool {
    path.is_dir() && tempfile::NamedTempFile::new_in(path).is_ok()
}

impl Settings {
    /// Checks every value, in flag order, without connecting anywhere.
    pub fn check(input: Input) -> Result<Self, Invalid> {
        let listen = input
            .listen
            .parse()
            .map_err(|_| invalid("invalid_listen_address", "--listen"))?;
        if input.reader.is_empty() || input.reader.len() > 63 || input.reader.contains('\0') {
            return Err(invalid("invalid_reader_role", "--reader"));
        }
        if let Some(digest) = &input.expect_content_digest {
            if input.snapshot.is_none() {
                return Err(invalid(
                    "expect_content_digest_requires_snapshot",
                    "--expect-content-digest",
                ));
            }
            if !content_digest(digest) {
                return Err(invalid("invalid_content_digest", "--expect-content-digest"));
            }
        }
        if input
            .retention_seconds
            .is_some_and(|seconds| seconds < MIN_RETENTION_SECONDS)
        {
            return Err(invalid("invalid_retention", "--retention-seconds"));
        }
        if input.change_retention && input.retention_seconds.is_none() {
            return Err(invalid(
                "change_retention_requires_retention_seconds",
                "--change-retention",
            ));
        }
        if let Some(dir) = &input.snapshot_dir
            && !writable_directory(dir)
        {
            return Err(invalid("invalid_snapshot_dir", "--snapshot-dir"));
        }
        if input.snapshot_keep == 0 {
            return Err(invalid("invalid_snapshot_keep", "--snapshot-keep"));
        }
        let at = time_of_day(&input.snapshot_at)
            .ok_or(invalid("invalid_snapshot_at", "--snapshot-at"))?;
        if input.database_fatal_after_seconds < MIN_DATABASE_FATAL_AFTER_SECONDS {
            return Err(invalid(
                "invalid_database_fatal_after",
                "--database-fatal-after-seconds",
            ));
        }
        Ok(Self {
            listen,
            reader: input.reader,
            restore: input.snapshot.map(|file| Restore {
                file,
                expect_content_digest: input.expect_content_digest,
            }),
            retention: input.retention_seconds.map(|seconds| Retention {
                seconds,
                change: input.change_retention,
            }),
            snapshots: input.snapshot_dir.map(|dir| Snapshots {
                dir,
                keep: input.snapshot_keep,
                at,
            }),
            health_max_lag_blocks: input.health_max_lag_blocks,
            database_fatal_after: Duration::from_secs(input.database_fatal_after_seconds),
            writer_busy_after: Duration::from_secs(WRITER_BUSY_AFTER_SECONDS),
        })
    }

    /// The thresholds of the status document for `network`.
    pub fn status(&self, network: &str) -> crate::status::Settings {
        crate::status::Settings {
            max_lag_blocks: self.health_max_lag_blocks,
            ..crate::status::Settings::new(network)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input() -> Input {
        Input {
            listen: "0.0.0.0:18080".to_owned(),
            reader: "db_sync_alt_reader".to_owned(),
            snapshot: None,
            expect_content_digest: None,
            retention_seconds: None,
            change_retention: false,
            snapshot_dir: None,
            snapshot_keep: 3,
            snapshot_at: "00:30".to_owned(),
            health_max_lag_blocks: 9,
            database_fatal_after_seconds: 1800,
        }
    }

    #[test]
    fn the_defaults_pass() {
        let settings = Settings::check(input()).unwrap();
        assert_eq!(settings.listen.port(), 18080);
        assert_eq!(settings.database_fatal_after, Duration::from_secs(1800));
        assert!(settings.restore.is_none() && settings.retention.is_none());
        assert!(settings.snapshots.is_none());
        assert_eq!(settings.status("preprod").max_lag_blocks, 9);
    }

    #[test]
    fn every_invalid_value_is_refused_with_its_flag() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("file.snapshot");
        std::fs::write(&file, b"x").unwrap();
        let digest = "a".repeat(64);
        type Change = Box<dyn Fn(&mut Input)>;
        let cases: Vec<(Change, &str, &str)> = vec![
            (
                Box::new(|i| i.listen = "18080".into()),
                "invalid_listen_address",
                "--listen",
            ),
            (
                Box::new(|i| i.listen = "host:18080".into()),
                "invalid_listen_address",
                "--listen",
            ),
            (
                Box::new(|i| i.reader = String::new()),
                "invalid_reader_role",
                "--reader",
            ),
            (
                Box::new(|i| i.reader = "r".repeat(64)),
                "invalid_reader_role",
                "--reader",
            ),
            (
                Box::new({
                    let digest = digest.clone();
                    move |i| i.expect_content_digest = Some(digest.clone())
                }),
                "expect_content_digest_requires_snapshot",
                "--expect-content-digest",
            ),
            (
                Box::new({
                    let file = file.clone();
                    move |i| {
                        i.snapshot = Some(file.clone());
                        i.expect_content_digest = Some("A".repeat(64));
                    }
                }),
                "invalid_content_digest",
                "--expect-content-digest",
            ),
            (
                Box::new(|i| i.retention_seconds = Some(86_399)),
                "invalid_retention",
                "--retention-seconds",
            ),
            (
                Box::new(|i| i.change_retention = true),
                "change_retention_requires_retention_seconds",
                "--change-retention",
            ),
            (
                Box::new(|i| i.snapshot_dir = Some("/nonexistent/directory".into())),
                "invalid_snapshot_dir",
                "--snapshot-dir",
            ),
            (
                Box::new({
                    let file = file.clone();
                    move |i| i.snapshot_dir = Some(file.clone())
                }),
                "invalid_snapshot_dir",
                "--snapshot-dir",
            ),
            (
                Box::new(|i| i.snapshot_keep = 0),
                "invalid_snapshot_keep",
                "--snapshot-keep",
            ),
            (
                Box::new(|i| i.snapshot_at = "24:00".into()),
                "invalid_snapshot_at",
                "--snapshot-at",
            ),
            (
                Box::new(|i| i.snapshot_at = "3:00".into()),
                "invalid_snapshot_at",
                "--snapshot-at",
            ),
            (
                Box::new(|i| i.snapshot_at = "03:60".into()),
                "invalid_snapshot_at",
                "--snapshot-at",
            ),
            (
                Box::new(|i| i.snapshot_at = "03:00:00".into()),
                "invalid_snapshot_at",
                "--snapshot-at",
            ),
            (
                Box::new(|i| i.database_fatal_after_seconds = 59),
                "invalid_database_fatal_after",
                "--database-fatal-after-seconds",
            ),
        ];
        for (change, error, flag) in cases {
            let mut value = input();
            change(&mut value);
            assert_eq!(
                Settings::check(value.clone()),
                Err(Invalid { error, flag }),
                "{value:?}"
            );
        }
    }

    /// The snapshot file is needed only for a restore: the check does not
    /// open it.
    #[test]
    fn a_missing_snapshot_file_passes_the_check() {
        let mut value = input();
        value.snapshot = Some("/nonexistent/file.snapshot".into());
        value.expect_content_digest = Some("a".repeat(64));
        let restore = Settings::check(value).unwrap().restore.unwrap();
        assert_eq!(restore.file, PathBuf::from("/nonexistent/file.snapshot"));
    }

    #[test]
    fn valid_values_are_kept() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("file.snapshot");
        std::fs::write(&file, b"x").unwrap();
        let mut value = input();
        value.listen = "[::1]:9".into();
        value.snapshot = Some(file.clone());
        value.expect_content_digest = Some("0123456789abcdef".repeat(4));
        value.retention_seconds = Some(86_400);
        value.change_retention = true;
        value.snapshot_dir = Some(dir.path().to_owned());
        value.snapshot_keep = 1;
        value.snapshot_at = "23:59".into();
        value.health_max_lag_blocks = 0;
        value.database_fatal_after_seconds = 60;
        let settings = Settings::check(value).unwrap();
        assert_eq!(settings.listen.to_string(), "[::1]:9");
        assert_eq!(settings.restore.unwrap().file, file);
        assert_eq!(
            settings.retention,
            Some(Retention {
                seconds: 86_400,
                change: true
            })
        );
        let snapshots = settings.snapshots.unwrap();
        assert_eq!(
            (snapshots.keep, snapshots.at),
            (1, NaiveTime::from_hms_opt(23, 59, 0).unwrap())
        );
        // The writability check leaves nothing behind.
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }
}
