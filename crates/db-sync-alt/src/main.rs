use std::{path::PathBuf, str::FromStr};

use clap::{Args, Parser, Subcommand};
use dsa_chain::profiles::Profile;
use dsa_store::{Error, Store};
use serde_json::{Value, json};

#[derive(Parser)]
#[command(version = dsa_chain::VERSION, about = "Cardano data for Midnight nodes")]
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
    #[arg(long, value_parser=clap::value_parser!(u32).range(1..=86400))]
    statement_timeout_seconds: Option<u32>,
    /// Explicit idle transaction limit; default preserves PostgreSQL settings.
    #[arg(long, value_parser=clap::value_parser!(u32).range(1..=86400))]
    idle_transaction_timeout_seconds: Option<u32>,
}

#[derive(Subcommand)]
enum Command {
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
    /// Read the database identity and serving position.
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
        #[arg(long,default_value_t=604800,value_parser=clap::value_parser!(i64).range(86400..))]
        retention_seconds: i64,
        #[arg(long,default_value_t=4096,value_parser=clap::value_parser!(u32).range(1..=65536))]
        batch_rows: u32,
        #[arg(long,default_value_t=4194304,value_parser=clap::value_parser!(u32).range(1024..=67108864))]
        batch_bytes: u32,
        #[arg(long)]
        snapshot: Option<PathBuf>,
        /// Preserve the reader's limit unless explicitly overridden.
        #[arg(long,value_parser=clap::value_parser!(u32).range(1..=86400))]
        source_statement_timeout_seconds: Option<u32>,
        #[arg(long,value_parser=clap::value_parser!(u32).range(1..=86400))]
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

#[tokio::main]
async fn main() -> std::process::ExitCode {
    match execute(Arguments::parse().command).await {
        Ok(value) => {
            println!("{value}");
            std::process::ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{}", error.report());
            std::process::ExitCode::FAILURE
        }
    }
}

async fn execute(command: Command) -> Result<Value, Error> {
    let (database, operation) = match &command {
        Command::Profile { network } => return Ok(serde_json::to_value(profile(network)?)?),
        Command::Create { database } => (database, "create"),
        Command::Status { database } => (database, "status"),
        Command::Verify { database } => (database, "verify"),
        Command::Snapshot { database, .. } => (database, "snapshot"),
        Command::Restore { database, .. } => (database, "restore"),
        Command::Convert { database, .. } => (database, "convert"),
        Command::GrantReader { database, .. } => (database, "grant-reader"),
    };
    let profile = profile(&database.network)?;
    let mut config = tokio_postgres::Config::from_str(&database.database)
        .map_err(|_| Error::Refusal("invalid_database_configuration"))?;
    if config.get_password().is_some() {
        return Err(Error::Refusal("database_password_requires_file"));
    }
    if let Some(path) = &database.password_file {
        let password =
            std::fs::read_to_string(path).map_err(|_| Error::Refusal("invalid_password_file"))?;
        let password = password.trim_end_matches(['\r', '\n']);
        if password.contains(['\r', '\n', '\0']) {
            return Err(Error::Refusal("invalid_password_file"));
        }
        config.password(password);
    }
    config.application_name("db-sync-alt");
    eprintln!("{}", json!({"operation":operation,"event":"started"}));
    let result = database_command(
        &command,
        config,
        &profile,
        database.tls_root_cert.as_deref(),
        database,
    )
    .await;
    eprintln!(
        "{}",
        json!({"operation":operation,"event":"finished","success":result.is_ok()})
    );
    result
}

fn profile(network: &str) -> Result<Profile, Error> {
    Profile::load(network).map_err(|_| Error::Refusal("unsupported_network"))
}

async fn database_command(
    command: &Command,
    config: tokio_postgres::Config,
    profile: &Profile,
    root: Option<&std::path::Path>,
    limits: &DatabaseArguments,
) -> Result<Value, Error> {
    let mut store = Store::connect(config, root).await?;
    for (name, seconds) in [
        ("statement_timeout", limits.statement_timeout_seconds),
        (
            "idle_in_transaction_session_timeout",
            limits.idle_transaction_timeout_seconds,
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
    match command {
        Command::Create { .. } => {
            store.create(profile).await?;
            store.status(profile).await
        }
        Command::Status { .. } => store.status(profile).await,
        Command::Verify { .. } => Ok(serde_json::to_value(store.verify(profile).await?)?),
        Command::Snapshot { file, .. } => {
            Ok(serde_json::to_value(store.snapshot(profile, file).await?)?)
        }
        Command::Restore { file, .. } => {
            Ok(serde_json::to_value(store.restore(profile, file).await?)?)
        }
        Command::Convert {
            source,
            source_password_file,
            source_tls_root_cert,
            horizon_block,
            retention_seconds,
            batch_rows,
            batch_bytes,
            snapshot,
            source_statement_timeout_seconds,
            source_idle_transaction_timeout_seconds,
            ..
        } => {
            let mut config = tokio_postgres::Config::from_str(source)
                .map_err(|_| Error::Refusal("invalid_source_configuration"))?;
            if config.get_password().is_some() {
                return Err(Error::Refusal("source_password_requires_file"));
            }
            if let Some(path) = source_password_file {
                let password = std::fs::read_to_string(path)
                    .map_err(|_| Error::Refusal("invalid_source_password_file"))?;
                let password = password.trim_end_matches(['\r', '\n']);
                if password.contains(['\r', '\n', '\0']) {
                    return Err(Error::Refusal("invalid_source_password_file"));
                }
                config.password(password);
            }
            let mut source =
                dsa_store::convert::LegacySource::connect(config, source_tls_root_cert.as_deref())
                    .await?;
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
            Ok(serde_json::to_value(
                store.convert(profile, &mut source, &options).await?,
            )?)
        }
        Command::GrantReader {
            role, check: true, ..
        } => store.check_reader(profile, role).await,
        Command::GrantReader {
            role, check: false, ..
        } => store.grant_reader(profile, role).await,
        Command::Profile { .. } => unreachable!("profile commands require no database"),
    }
}
