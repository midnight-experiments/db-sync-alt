//! The flags of the service besides the database, the provider and the
//! follower: the HTTP address, the node's reader, the first restore, the
//! retention, the daily snapshot files and the limits of `/health` and of an
//! unreachable database. Every flag is also read from `DSA_` plus its name
//! in upper case with underscores; a flag on the command line wins.

use std::path::PathBuf;

use clap::Args;
use dsa_service::config::{Input, Settings};

use crate::{Usage, usage};

#[derive(Args, Clone, Debug)]
pub(crate) struct ServiceArguments {
    /// The address and port of /health and /status.
    #[arg(long, env = "DSA_LISTEN", default_value = "0.0.0.0:18080")]
    pub(crate) listen: String,
    /// The node's PostgreSQL login: granted its rights at start and checked.
    #[arg(long, env = "DSA_READER")]
    pub(crate) reader: String,
    /// A snapshot file to restore when the database is empty; read only when
    /// a restore is needed.
    #[arg(long, env = "DSA_SNAPSHOT")]
    pub(crate) snapshot: Option<PathBuf>,
    /// Refuse the --snapshot file unless its content digest is this one.
    #[arg(long, env = "DSA_EXPECT_CONTENT_DIGEST")]
    pub(crate) expect_content_digest: Option<String>,
    /// Seconds an event stays after it is first served, at least 86400
    /// [default: the stored value].
    #[arg(long, env = "DSA_RETENTION_SECONDS")]
    pub(crate) retention_seconds: Option<i64>,
    /// Store --retention-seconds when it differs from the stored value.
    #[arg(long, env = "DSA_CHANGE_RETENTION")]
    pub(crate) change_retention: bool,
    /// A directory for the daily snapshot files [default: none are written].
    #[arg(long, env = "DSA_SNAPSHOT_DIR")]
    pub(crate) snapshot_dir: Option<PathBuf>,
    /// The newest daily snapshot files of the network kept in --snapshot-dir.
    #[arg(long, env = "DSA_SNAPSHOT_KEEP", default_value_t = 3)]
    pub(crate) snapshot_keep: u32,
    /// The UTC time of day of the daily snapshot file, as HH:MM.
    #[arg(long, env = "DSA_SNAPSHOT_AT", default_value = "00:30")]
    pub(crate) snapshot_at: String,
    /// /health answers 503 when the served target is more blocks behind the
    /// provider's tip minus the security parameter.
    #[arg(long, env = "DSA_HEALTH_MAX_LAG_BLOCKS", default_value_t = 9)]
    pub(crate) health_max_lag_blocks: u32,
    /// A database that stays unreachable, or a provider whose transport
    /// fails or that answers 5xx, for this many seconds ends the service
    /// with exit status 75 (at least 60).
    #[arg(long, env = "DSA_DATABASE_FATAL_AFTER_SECONDS", default_value_t = 1800)]
    pub(crate) database_fatal_after_seconds: u64,
}

impl ServiceArguments {
    /// Every value checked before any connection.
    pub(crate) fn settings(&self) -> Result<Settings, Usage> {
        Settings::check(Input {
            listen: self.listen.clone(),
            reader: self.reader.clone(),
            snapshot: self.snapshot.clone(),
            expect_content_digest: self.expect_content_digest.clone(),
            retention_seconds: self.retention_seconds,
            change_retention: self.change_retention,
            snapshot_dir: self.snapshot_dir.clone(),
            snapshot_keep: self.snapshot_keep,
            snapshot_at: self.snapshot_at.clone(),
            health_max_lag_blocks: self.health_max_lag_blocks,
            database_fatal_after_seconds: self.database_fatal_after_seconds,
        })
        .map_err(|invalid| usage(invalid.error, invalid.flag))
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use serde_json::{Value, json};

    use super::*;
    use crate::{DATABASE, DatabaseArguments, connection, profile};

    /// The service's flags beside the database flags, as the command that
    /// runs the service has them.
    #[derive(Parser)]
    struct Service {
        #[command(flatten)]
        database: DatabaseArguments,
        #[command(flatten)]
        service: ServiceArguments,
    }

    /// Nothing listens on port 1.
    const UNREACHABLE: &str = "host=127.0.0.1 port=1 user=nobody sslmode=disable";

    /// Parses and checks as the service does before it connects: the
    /// database flags, then the service's settings.
    fn check(arguments: &[&str]) -> Value {
        let mut all = vec!["service", "--database", UNREACHABLE, "--network", "preprod"];
        all.extend(arguments);
        let parsed = match Service::try_parse_from(all) {
            Ok(parsed) => parsed,
            Err(error) => return json!({"clap": error.to_string()}),
        };
        let checked = profile(&parsed.database.network).and_then(|_| {
            connection(
                &DATABASE,
                &parsed.database.database,
                parsed.database.password_file.as_deref(),
                parsed.database.tls_root_cert.as_deref(),
            )
        });
        if let Err(refused) = checked {
            return json!({"error": refused.error, "flag": refused.flag});
        }
        match parsed.service.settings() {
            Ok(settings) => json!({"settings": settings}),
            Err(refused) => json!({"error": refused.error, "flag": refused.flag}),
        }
    }

    #[test]
    fn every_invalid_flag_is_refused_before_a_connection_with_its_name() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("f.snapshot");
        std::fs::write(&file, b"x").unwrap();
        let file = file.to_str().unwrap();
        let digest = "a".repeat(64);
        let result = check(&["--reader", ""]);
        assert_eq!(result["flag"], "--reader", "{result}");
        for (arguments, flag) in [
            (vec!["--listen", "nowhere"], "--listen"),
            (
                vec!["--expect-content-digest", &digest],
                "--expect-content-digest",
            ),
            (
                vec!["--snapshot", file, "--expect-content-digest", "xyz"],
                "--expect-content-digest",
            ),
            (vec!["--retention-seconds", "3600"], "--retention-seconds"),
            (vec!["--change-retention"], "--change-retention"),
            (vec!["--snapshot-dir", "/nonexistent/dir"], "--snapshot-dir"),
            (vec!["--snapshot-keep", "0"], "--snapshot-keep"),
            (vec!["--snapshot-at", "25:00"], "--snapshot-at"),
            (
                vec!["--database-fatal-after-seconds", "10"],
                "--database-fatal-after-seconds",
            ),
        ] {
            let mut all = vec!["--reader", "node"];
            all.extend(arguments.iter().copied());
            let result = check(&all);
            assert_eq!(result["flag"], flag, "{arguments:?}: {result}");
        }
        // A value of the wrong type is refused by the parser, which names the
        // flag too.
        for (arguments, flag) in [
            (vec!["--snapshot-keep", "x"], "--snapshot-keep"),
            (
                vec!["--health-max-lag-blocks", "nine"],
                "--health-max-lag-blocks",
            ),
            (vec!["--retention-seconds", "week"], "--retention-seconds"),
        ] {
            let mut all = vec!["--reader", "node"];
            all.extend(arguments.iter().copied());
            let result = check(&all);
            assert!(
                result["clap"]
                    .as_str()
                    .is_some_and(|text| text.contains(flag)),
                "{arguments:?}: {result}"
            );
        }
        // The reader is required.
        let result = check(&[]);
        assert!(
            result["clap"].as_str().unwrap().contains("--reader"),
            "{result}"
        );
        // Valid values pass the checks; nothing was contacted.
        let result = check(&["--reader", "node", "--snapshot", file]);
        assert_eq!(result["settings"]["reader"], "node", "{result}");
    }

    /// Runs this test binary again with `environment` and the marker of
    /// `the_environment_child`, which prints what the flags gave.
    fn in_child(environment: &[(&str, &str)], arguments: &[&str]) -> Value {
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command.args([
            "--exact",
            "service_arguments::tests::the_environment_child",
            "--nocapture",
            "--test-threads=1",
        ]);
        for (name, _) in std::env::vars().filter(|(name, _)| name.starts_with("DSA_")) {
            command.env_remove(name);
        }
        command.env(
            "DSA_TEST_SERVICE_ARGUMENTS",
            serde_json::to_string(arguments).unwrap(),
        );
        for (name, value) in environment {
            command.env(name, value);
        }
        let output = command.output().unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        let line = stdout
            .lines()
            .find_map(|line| line.split_once("SERVICE_ARGUMENTS ").map(|(_, json)| json))
            .unwrap_or_else(|| panic!("no result: {stdout}"));
        serde_json::from_str(line).unwrap()
    }

    /// Prints the result of `check` for the arguments its parent gave; does
    /// nothing in a normal test run.
    #[test]
    fn the_environment_child() {
        let Ok(arguments) = std::env::var("DSA_TEST_SERVICE_ARGUMENTS") else {
            return;
        };
        let arguments: Vec<String> = serde_json::from_str(&arguments).unwrap();
        let arguments = arguments.iter().map(String::as_str).collect::<Vec<_>>();
        println!("SERVICE_ARGUMENTS {}", check(&arguments));
    }

    #[test]
    fn every_flag_is_read_from_its_variable_and_the_flag_wins() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("f.snapshot");
        std::fs::write(&file, b"x").unwrap();
        let (dir_text, file_text) = (dir.path().to_str().unwrap(), file.to_str().unwrap());
        let digest = "0123456789abcdef".repeat(4);
        let result = in_child(
            &[
                ("DSA_LISTEN", "127.0.0.1:19999"),
                ("DSA_READER", "node_from_environment"),
                ("DSA_SNAPSHOT", file_text),
                ("DSA_EXPECT_CONTENT_DIGEST", &digest),
                ("DSA_RETENTION_SECONDS", "172800"),
                ("DSA_CHANGE_RETENTION", "true"),
                ("DSA_SNAPSHOT_DIR", dir_text),
                ("DSA_SNAPSHOT_KEEP", "5"),
                ("DSA_SNAPSHOT_AT", "04:15"),
                ("DSA_HEALTH_MAX_LAG_BLOCKS", "20"),
                ("DSA_DATABASE_FATAL_AFTER_SECONDS", "900"),
            ],
            &[],
        );
        let settings = &result["settings"];
        assert_eq!(settings["listen"], "127.0.0.1:19999", "{result}");
        assert_eq!(settings["reader"], "node_from_environment");
        assert_eq!(settings["restore"]["file"], file_text);
        assert_eq!(settings["restore"]["expect_content_digest"], digest);
        assert_eq!(
            settings["retention"],
            json!({"seconds": 172_800, "change": true})
        );
        assert_eq!(
            settings["snapshots"],
            json!({"dir": dir_text, "keep": 5, "at": "04:15:00"})
        );
        assert_eq!(settings["health_max_lag_blocks"], 20);
        assert_eq!(settings["database_fatal_after"]["secs"], 900);
        // The flag wins over its variable.
        let result = in_child(
            &[
                ("DSA_LISTEN", "nowhere"),
                ("DSA_READER", "node_from_environment"),
                ("DSA_SNAPSHOT_KEEP", "0"),
            ],
            &[
                "--listen",
                "127.0.0.1:1",
                "--reader",
                "node",
                "--snapshot-keep",
                "2",
            ],
        );
        assert_eq!(result["settings"]["listen"], "127.0.0.1:1", "{result}");
        assert_eq!(result["settings"]["reader"], "node");
        // An invalid value from a variable is named by its flag.
        for (name, value, flag) in [
            ("DSA_LISTEN", "nowhere", "--listen"),
            ("DSA_RETENTION_SECONDS", "10", "--retention-seconds"),
            ("DSA_SNAPSHOT_DIR", "/nonexistent/d", "--snapshot-dir"),
            ("DSA_SNAPSHOT_KEEP", "0", "--snapshot-keep"),
            ("DSA_SNAPSHOT_AT", "noon", "--snapshot-at"),
            (
                "DSA_DATABASE_FATAL_AFTER_SECONDS",
                "1",
                "--database-fatal-after-seconds",
            ),
        ] {
            let result = in_child(&[("DSA_READER", "node"), (name, value)], &[]);
            assert_eq!(result["flag"], flag, "{name}: {result}");
        }
    }
}
