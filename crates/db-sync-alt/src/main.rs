//! The command line: argument parsing and checking, and the dispatch of each
//! command to the store through the wrapper of `wrapper.rs`.

mod wrapper;

use std::{
    path::{Path, PathBuf},
    process::ExitCode,
    str::FromStr,
};

use clap::{Args, Parser, Subcommand};
use dsa_chain::profiles::Profile;
use dsa_store::{Error, Store, compare::Positions};
use serde_json::{Value, json};
use tokio_postgres::config::SslMode;
use wrapper::{EXIT_STATUSES, Effect, Exit, Job, Operation, Session, run};

#[derive(Parser)]
#[command(
    version = dsa_chain::VERSION,
    about = "Cardano data for Midnight nodes",
    after_help = EXIT_STATUSES
)]
struct Arguments {
    #[command(subcommand)]
    command: Command,
}

#[derive(Args)]
struct DatabaseArguments {
    /// PostgreSQL connection string. Read the password from --password-file.
    #[arg(long)]
    database: String,
    #[arg(long)]
    network: String,
    #[arg(long)]
    password_file: Option<PathBuf>,
    /// PEM roots for verified TLS, including server hostname verification.
    #[arg(long)]
    tls_root_cert: Option<PathBuf>,
    /// Explicit session statement limit; default preserves PostgreSQL settings.
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..=86400))]
    statement_timeout_seconds: Option<u32>,
    /// Explicit idle transaction limit; default preserves PostgreSQL settings.
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..=86400))]
    idle_transaction_timeout_seconds: Option<u32>,
    /// Stop the command after this many seconds (default: none; convert: 3600).
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..=86400))]
    overall_timeout_seconds: Option<u32>,
}

#[derive(Args)]
struct CompareArguments {
    #[arg(
        long,
        required_unless_present = "a_capture",
        conflicts_with = "a_capture"
    )]
    a: Option<String>,
    #[arg(long, conflicts_with_all = ["a", "a_password_file", "a_tls_root_cert"])]
    a_capture: Option<PathBuf>,
    #[arg(long)]
    b: String,
    #[arg(long)]
    network: String,
    /// JSON positions file; auto uses --horizon-block or a source capture.
    #[arg(long, default_value = "auto")]
    positions: String,
    #[arg(long)]
    horizon_block: Option<u32>,
    #[arg(long, default_value_t = 500, value_parser = clap::value_parser!(u32).range(1..=5000))]
    max_pairs: u32,
    #[arg(long)]
    a_password_file: Option<PathBuf>,
    #[arg(long)]
    b_password_file: Option<PathBuf>,
    #[arg(long)]
    a_tls_root_cert: Option<PathBuf>,
    #[arg(long)]
    b_tls_root_cert: Option<PathBuf>,
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..=86400))]
    statement_timeout_seconds: Option<u32>,
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..=86400))]
    idle_transaction_timeout_seconds: Option<u32>,
    /// Also repeat side A's event queries with bounds 0..i64::MAX (side B always
    /// does). This reads A's whole event history.
    #[arg(long)]
    a_verify_unbounded: bool,
    /// Differing rows reported per query.
    #[arg(long, default_value_t = 1000, value_parser = clap::value_parser!(u32).range(1..=1_000_000))]
    max_differing_rows: u32,
    #[arg(long)]
    candidate_address: Option<String>,
    #[arg(long, default_value_t = 3600, value_parser = clap::value_parser!(u32).range(1..=86400))]
    overall_timeout_seconds: u32,
}

#[derive(Subcommand)]
enum Command {
    /// Compare the exact node catalogue on two public-reader snapshots.
    Compare(CompareArguments),
    /// Print a supported network's node profile.
    Profile {
        #[arg(long)]
        network: String,
    },
    /// Initialize an empty, operator-owned PostgreSQL 17 database.
    Create {
        #[command(flatten)]
        database: DatabaseArguments,
    },
    /// Read identity, serving position and capabilities.
    Status {
        #[command(flatten)]
        database: DatabaseArguments,
    },
    /// Verify logical content, supply, references and positional integrity.
    Verify {
        #[command(flatten)]
        database: DatabaseArguments,
    },
    /// Write a new snapshot of one consistent serving database state.
    Snapshot {
        #[command(flatten)]
        database: DatabaseArguments,
        #[arg(long)]
        file: PathBuf,
    },
    /// Restore a snapshot into an empty database, then verify and serve.
    Restore {
        #[command(flatten)]
        database: DatabaseArguments,
        #[arg(long)]
        file: PathBuf,
        /// Refuse the file unless its content digest is this one.
        #[arg(long, value_parser = content_digest)]
        expect_content_digest: Option<String>,
    },
    /// Convert a serving legacy public reader snapshot into this database.
    Convert {
        #[command(flatten)]
        database: DatabaseArguments,
        #[arg(long)]
        source: String,
        #[arg(long)]
        source_password_file: Option<PathBuf>,
        #[arg(long)]
        source_tls_root_cert: Option<PathBuf>,
        #[arg(long)]
        horizon_block: Option<u32>,
        #[arg(long, default_value_t = 604800, value_parser = clap::value_parser!(i64).range(86400..))]
        retention_seconds: i64,
        #[arg(long, default_value_t = 4096, value_parser = clap::value_parser!(u32).range(1..=65536))]
        batch_rows: u32,
        #[arg(long, default_value_t = 4194304, value_parser = clap::value_parser!(u32).range(1024..=67108864))]
        batch_bytes: u32,
        #[arg(long)]
        snapshot: Option<PathBuf>,
        /// Save literal source answers in the conversion's own RR snapshot.
        #[arg(long)]
        capture_source: Option<PathBuf>,
        /// Preserve the reader's limit unless explicitly overridden.
        #[arg(long, value_parser = clap::value_parser!(u32).range(1..=86400))]
        source_statement_timeout_seconds: Option<u32>,
        #[arg(long, value_parser = clap::value_parser!(u32).range(1..=86400))]
        source_idle_transaction_timeout_seconds: Option<u32>,
    },
    /// Grant and check an existing node reader's exact privileges.
    GrantReader {
        #[command(flatten)]
        database: DatabaseArguments,
        #[arg(long)]
        role: String,
        /// Check existing grants without changing them.
        #[arg(long)]
        check: bool,
    },
}

impl Command {
    fn database(&self) -> Option<&DatabaseArguments> {
        match self {
            Self::Create { database }
            | Self::Status { database }
            | Self::Verify { database }
            | Self::Snapshot { database, .. }
            | Self::Restore { database, .. }
            | Self::Convert { database, .. }
            | Self::GrantReader { database, .. } => Some(database),
            Self::Profile { .. } | Self::Compare(_) => None,
        }
    }
}

fn content_digest(value: &str) -> Result<String, String> {
    if value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        Ok(value.to_owned())
    } else {
        Err("expected 64 lowercase hexadecimal characters".to_owned())
    }
}

/// An invalid flag value, found before any connection.
#[derive(Debug)]
struct Usage {
    error: &'static str,
    flag: &'static str,
}

fn usage(error: &'static str, flag: &'static str) -> Usage {
    Usage { error, flag }
}

/// The flags that name one connection.
struct Flags {
    connection: &'static str,
    password: &'static str,
    root: &'static str,
}

const DATABASE: Flags = Flags {
    connection: "--database",
    password: "--password-file",
    root: "--tls-root-cert",
};
const SOURCE: Flags = Flags {
    connection: "--source",
    password: "--source-password-file",
    root: "--source-tls-root-cert",
};
const SIDE_A: Flags = Flags {
    connection: "--a",
    password: "--a-password-file",
    root: "--a-tls-root-cert",
};
const SIDE_B: Flags = Flags {
    connection: "--b",
    password: "--b-password-file",
    root: "--b-tls-root-cert",
};

/// A connection named on the command line, checked before connecting: the
/// connection string parses and holds no password, the password file holds
/// one line, and the TLS root file is readable while TLS is not disabled.
struct Connection {
    config: tokio_postgres::Config,
    root: Option<PathBuf>,
}

fn connection(
    flags: &Flags,
    text: &str,
    password_file: Option<&Path>,
    root: Option<&Path>,
) -> Result<Connection, Usage> {
    let mut config = tokio_postgres::Config::from_str(text)
        .map_err(|_| usage("invalid_connection_string", flags.connection))?;
    if config.get_password().is_some() {
        return Err(usage("password_requires_file", flags.connection));
    }
    if let Some(path) = password_file {
        let password = std::fs::read_to_string(path)
            .map_err(|_| usage("invalid_password_file", flags.password))?;
        let password = password.trim_end_matches(['\r', '\n']);
        if password.contains(['\r', '\n', '\0']) {
            return Err(usage("invalid_password_file", flags.password));
        }
        config.password(password);
    }
    if let Some(path) = root {
        if config.get_ssl_mode() == SslMode::Disable {
            return Err(usage("conflicting_tls_configuration", flags.root));
        }
        std::fs::File::open(path).map_err(|_| usage("invalid_tls_root_certificate", flags.root))?;
    }
    config.application_name("db-sync-alt");
    Ok(Connection {
        config,
        root: root.map(Path::to_owned),
    })
}

fn profile(network: &str) -> Result<Profile, Usage> {
    Profile::load(network).map_err(|_| usage("unsupported_network", "--network"))
}

fn positions_file(path: &str) -> Result<Positions, Usage> {
    use std::io::Read;
    let invalid = || usage("invalid_comparison_positions_file", "--positions");
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .map_err(|_| invalid())?
        .take(1_048_577)
        .read_to_end(&mut bytes)
        .map_err(|_| invalid())?;
    if bytes.len() > 1_048_576 {
        return Err(usage("comparison_positions_file_too_large", "--positions"));
    }
    let positions: Positions = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
    positions.validate().map_err(|_| invalid())?;
    Ok(positions)
}

#[tokio::main]
async fn main() -> ExitCode {
    let arguments = match Arguments::try_parse() {
        Ok(arguments) => arguments,
        Err(error) => {
            let _ = error.print();
            let exit = if error.use_stderr() {
                Exit::Usage
            } else {
                Exit::Success
            };
            return ExitCode::from(exit as u8);
        }
    };
    match prepare(arguments.command) {
        Ok((operation, command)) => run(operation, command).await,
        Err(usage) => {
            eprintln!("{}", json!({"error": usage.error, "flag": usage.flag}));
            ExitCode::from(Exit::Usage as u8)
        }
    }
}

/// Checks every flag before anything connects, and returns the operation with
/// its command.
fn prepare(command: Command) -> Result<(Operation, Job), Usage> {
    match command {
        Command::Profile { network } => {
            let profile = profile(&network)?;
            Ok((
                Operation::new("profile", Effect::Reads, None),
                Box::new(move |_| Box::pin(async move { Ok(serde_json::to_value(profile)?) })),
            ))
        }
        Command::Compare(args) => compare(args),
        command => database_command(command),
    }
}

fn compare(args: CompareArguments) -> Result<(Operation, Job), Usage> {
    use dsa_store::compare::{Request, SideA, compare_databases};
    let profile = profile(&args.network)?;
    let positions = if args.positions == "auto" {
        None
    } else {
        let positions = positions_file(&args.positions)?;
        if args.horizon_block.is_some_and(|h| h != positions.horizon) {
            return Err(usage("comparison_horizon_mismatch", "--horizon-block"));
        }
        Some(positions)
    };
    let a = match (&args.a, &args.a_capture) {
        (_, Some(file)) => SideA::Capture(file.clone()),
        (Some(a), None) => {
            let a = connection(
                &SIDE_A,
                a,
                args.a_password_file.as_deref(),
                args.a_tls_root_cert.as_deref(),
            )?;
            SideA::Database {
                config: Box::new(a.config),
                root: a.root,
            }
        }
        (None, None) => return Err(usage("comparison_source_required", "--a")),
    };
    let b = connection(
        &SIDE_B,
        &args.b,
        args.b_password_file.as_deref(),
        args.b_tls_root_cert.as_deref(),
    )?;
    let request = Request {
        profile,
        a,
        b: b.config,
        b_root: b.root,
        positions,
        horizon_block: args.horizon_block,
        max_pairs: args.max_pairs as usize,
        statement_timeout_seconds: args.statement_timeout_seconds,
        idle_transaction_timeout_seconds: args.idle_transaction_timeout_seconds,
        a_verify_unbounded: args.a_verify_unbounded,
        candidate_address: args.candidate_address,
        max_differing_rows: args.max_differing_rows as usize,
    };
    Ok((
        Operation::new("compare", Effect::Reads, Some(args.overall_timeout_seconds)),
        Box::new(move |session: Session| {
            Box::pin(async move {
                let report = compare_databases(request, |c| session.watch(c)).await?;
                Ok(serde_json::to_value(report)?)
            })
        }),
    ))
}

fn database_command(command: Command) -> Result<(Operation, Job), Usage> {
    use Effect::{Changes, Checks, Reads};
    let database = command.database().expect("a database command");
    let profile = profile(&database.network)?;
    let destination = connection(
        &DATABASE,
        &database.database,
        database.password_file.as_deref(),
        database.tls_root_cert.as_deref(),
    )?;
    let deadline = database.overall_timeout_seconds;
    let (name, effect, deadline) = match &command {
        Command::Create { .. } => ("create", Changes, deadline),
        Command::Status { .. } => ("status", Reads, deadline),
        Command::Verify { .. } => ("verify", Checks, deadline),
        Command::Snapshot { .. } => ("snapshot", Changes, deadline),
        Command::Restore { .. } => ("restore", Changes, deadline),
        Command::Convert { .. } => ("convert", Changes, deadline.or(Some(3600))),
        Command::GrantReader { check: true, .. } => ("grant-reader", Checks, deadline),
        Command::GrantReader { .. } => ("grant-reader", Changes, deadline),
        Command::Profile { .. } | Command::Compare(_) => unreachable!("not a database command"),
    };
    let source = match &command {
        Command::Convert {
            source,
            source_password_file,
            source_tls_root_cert,
            capture_source,
            ..
        } => {
            if let Some(path) = capture_source {
                dsa_store::source_capture::preflight(path).map_err(|error| match error {
                    Error::Refusal(code) => usage(code, "--capture-source"),
                    _ => usage("invalid_source_capture_path", "--capture-source"),
                })?;
            }
            Some(connection(
                &SOURCE,
                source,
                source_password_file.as_deref(),
                source_tls_root_cert.as_deref(),
            )?)
        }
        _ => None,
    };
    Ok((
        Operation::new(name, effect, deadline),
        Box::new(move |session: Session| {
            Box::pin(async move { execute(command, profile, destination, source, session).await })
        }),
    ))
}

/// Connects the destination, registers it for cancellation and applies the
/// session limits.
async fn open(
    connection: Connection,
    database: &DatabaseArguments,
    session: &Session,
) -> Result<Store, Error> {
    let store = Store::connect(connection.config, connection.root.as_deref()).await?;
    session.watch(store.cancellation());
    for (name, seconds) in [
        ("statement_timeout", database.statement_timeout_seconds),
        (
            "idle_in_transaction_session_timeout",
            database.idle_transaction_timeout_seconds,
        ),
    ] {
        if let Some(seconds) = seconds {
            store
                .client()
                .query_one(
                    "SELECT set_config($1,$2,false)",
                    &[&name, &format!("{seconds}s")],
                )
                .await?;
        }
    }
    Ok(store)
}

async fn execute(
    command: Command,
    profile: Profile,
    destination: Connection,
    source: Option<Connection>,
    session: Session,
) -> Result<Value, Error> {
    let database = command.database().expect("a database command");
    let mut store = open(destination, database, &session).await?;
    match &command {
        Command::Create { .. } => {
            store.create(&profile).await?;
            store.status(&profile).await
        }
        Command::Status { .. } => {
            let mut value = store.status(&profile).await?;
            value["capabilities"] =
                serde_json::to_value(dsa_chain::capabilities::NodeCapabilities::pinned())?;
            value["unsupported_node_statements"] =
                serde_json::to_value(dsa_store::catalogue::unsupported().collect::<Vec<_>>())?;
            Ok(value)
        }
        Command::Verify { .. } => Ok(serde_json::to_value(store.verify(&profile).await?)?),
        Command::Snapshot { file, .. } => {
            Ok(serde_json::to_value(store.snapshot(&profile, file).await?)?)
        }
        Command::Restore {
            file,
            expect_content_digest,
            ..
        } => Ok(serde_json::to_value(
            store
                .restore(&profile, file, expect_content_digest.as_deref())
                .await?,
        )?),
        Command::Convert {
            horizon_block,
            retention_seconds,
            batch_rows,
            batch_bytes,
            snapshot,
            capture_source,
            source_statement_timeout_seconds,
            source_idle_transaction_timeout_seconds,
            ..
        } => {
            let source = source.expect("convert names its source");
            let mut source =
                dsa_store::convert::LegacySource::connect(source.config, source.root.as_deref())
                    .await?;
            session.watch(source.cancellation());
            source
                .limits(
                    *source_statement_timeout_seconds,
                    *source_idle_transaction_timeout_seconds,
                )
                .await?;
            let options = dsa_store::convert::ConvertOptions {
                horizon_block: *horizon_block,
                retention_seconds: *retention_seconds,
                batch_rows: *batch_rows as usize,
                batch_bytes: *batch_bytes as usize,
                snapshot: snapshot.clone(),
            };
            match capture_source {
                Some(path) => Ok(serde_json::to_value(
                    store
                        .convert_with_capture(&profile, &mut source, &options, path)
                        .await?,
                )?),
                None => Ok(serde_json::to_value(
                    store.convert(&profile, &mut source, &options).await?,
                )?),
            }
        }
        Command::GrantReader {
            role, check: true, ..
        } => store.check_reader(&profile, role).await,
        Command::GrantReader { role, .. } => store.grant_reader(&profile, role).await,
        Command::Profile { .. } | Command::Compare(_) => unreachable!("not a database command"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flags_are_checked_before_any_connection() {
        let dir = tempfile::tempdir().unwrap();
        let two_lines = dir.path().join("password");
        std::fs::write(&two_lines, "one\ntwo\n").unwrap();
        let missing = dir.path().join("missing.pem");
        for (text, password, root, error, flag) in [
            (
                "not a connection = string = x",
                None,
                None,
                "invalid_connection_string",
                "--database",
            ),
            (
                "host=h user=u password=p",
                None,
                None,
                "password_requires_file",
                "--database",
            ),
            (
                "host=h user=u",
                Some(two_lines.as_path()),
                None,
                "invalid_password_file",
                "--password-file",
            ),
            (
                "host=h user=u",
                None,
                Some(missing.as_path()),
                "invalid_tls_root_certificate",
                "--tls-root-cert",
            ),
            (
                "host=h user=u sslmode=disable",
                None,
                Some(two_lines.as_path()),
                "conflicting_tls_configuration",
                "--tls-root-cert",
            ),
        ] {
            let refused = connection(&DATABASE, text, password, root).err().unwrap();
            assert_eq!((refused.error, refused.flag), (error, flag), "{text}");
        }
        assert_eq!(profile("preview").err().unwrap().flag, "--network");
    }
}
