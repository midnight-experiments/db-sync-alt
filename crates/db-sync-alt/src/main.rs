use std::{
    path::PathBuf,
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};

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

#[derive(Args)]
struct CompareArguments {
    #[arg(
        long,
        required_unless_present = "a_capture",
        conflicts_with = "a_capture"
    )]
    a: Option<String>,
    #[arg(long, conflicts_with = "a")]
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
    #[arg(long,default_value_t=500,value_parser=clap::value_parser!(u32).range(1..=5000))]
    max_pairs: u32,
    #[arg(long)]
    a_password_file: Option<PathBuf>,
    #[arg(long)]
    b_password_file: Option<PathBuf>,
    #[arg(long)]
    a_tls_root_cert: Option<PathBuf>,
    #[arg(long)]
    b_tls_root_cert: Option<PathBuf>,
    #[arg(long,value_parser=clap::value_parser!(u32).range(1..=86400))]
    statement_timeout_seconds: Option<u32>,
    #[arg(long,value_parser=clap::value_parser!(u32).range(1..=86400))]
    idle_transaction_timeout_seconds: Option<u32>,
    /// Scratch verification: repeat events with bounds0..i64::MAX.
    #[arg(long)]
    verify_unbounded: bool,
    #[arg(long)]
    candidate_address: Option<String>,
    #[arg(long,default_value_t=3600,value_parser=clap::value_parser!(u32).range(1..=86400))]
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
        /// Save literal source answers in the conversion's own RR snapshot.
        #[arg(long)]
        capture_source: Option<PathBuf>,
        #[arg(long,default_value_t=3600,value_parser=clap::value_parser!(u32).range(1..=86400))]
        overall_timeout_seconds: u32,
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
            if value.get("matched") == Some(&Value::Bool(false)) {
                std::process::ExitCode::from(2)
            } else {
                std::process::ExitCode::SUCCESS
            }
        }
        Err(error) => {
            eprintln!("{}", error.report());
            std::process::ExitCode::FAILURE
        }
    }
}

#[derive(Clone, Default)]
struct CancelGroup(Arc<Mutex<Vec<dsa_store::Cancellation>>>);
impl CancelGroup {
    fn add(&self, value: dsa_store::Cancellation) {
        self.0.lock().expect("cancellation registry").push(value);
    }
    async fn cancel(&self) {
        let handles = self.0.lock().expect("cancellation registry").clone();
        let mut pending = tokio::task::JoinSet::new();
        for (index, handle) in handles.into_iter().enumerate() {
            pending.spawn(async move {let result=tokio::time::timeout(Duration::from_secs(5),handle.cancel()).await; eprintln!("{}",json!({"event":"cancel_request","connection":index,"sent":matches!(result,Ok(Ok(())))}));});
        }
        while pending.join_next().await.is_some() {}
    }
}
async fn stopped() -> Result<(), Error> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .map_err(|_| Error::Refusal("signal_listener_failed"))?;
        tokio::select! {r=tokio::signal::ctrl_c()=>r.map_err(|_|Error::Refusal("signal_listener_failed")),_=terminate.recv()=>Ok(())}
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .map_err(|_| Error::Refusal("signal_listener_failed"))
    }
}
async fn execute(command: Command) -> Result<Value, Error> {
    let seconds = match &command {
        Command::Compare(args) => Some(args.overall_timeout_seconds),
        Command::Convert {
            overall_timeout_seconds,
            ..
        } => Some(*overall_timeout_seconds),
        _ => None,
    };
    let group = CancelGroup::default();
    if let Some(seconds) = seconds {
        let work = execute_inner(command, &group);
        tokio::pin!(work);
        let result = tokio::select! {
            result=&mut work=>result,
            signal=stopped()=>match signal {Ok(())=>Err(Error::Refusal("operation_interrupted")),Err(error)=>Err(error)},
            _=tokio::time::sleep(Duration::from_secs(u64::from(seconds)))=>Err(Error::Refusal("operation_deadline_exceeded"))
        };
        if result.is_err() {
            group.cancel().await;
        }
        result
    } else {
        execute_inner(command, &group).await
    }
}

async fn execute_inner(command: Command, group: &CancelGroup) -> Result<Value, Error> {
    let (database, operation) = match &command {
        Command::Compare(args) => return compare_command(args, group).await,
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
        group,
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
    group: &CancelGroup,
) -> Result<Value, Error> {
    let mut store = Store::connect(config, root).await?;
    group.add(store.cancellation());
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
            capture_source,
            source_statement_timeout_seconds,
            source_idle_transaction_timeout_seconds,
            ..
        } => {
            if let Some(path) = capture_source {
                dsa_store::source_capture::preflight(path)?;
            }
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
            group.add(source.cancellation());
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
            if let Some(path) = capture_source {
                let tx = source.snapshot().await?;
                let staged = store.stage_conversion(profile, &tx, &options).await?;
                let state = &staged.source().state;
                let positions = dsa_store::compare::Positions::generate(
                    &tx,
                    state.horizon.height,
                    state.target.height,
                    500,
                )
                .await?;
                let captured =
                    dsa_store::compare::capture(&tx, profile, &positions, &Default::default())
                        .await?;
                tx.commit().await?;
                // Observation publication precedes destination publication. File
                // failure drops the staged conversion with its gate closed.
                let path = path.clone();
                let published = tokio::task::spawn_blocking(move || {
                    dsa_store::source_capture::write(&path, &captured)
                })
                .await
                .map_err(|_| Error::Refusal("source_capture_io_error"))??;
                let mut converted = staged.finish(profile).await?;
                if let Some(snapshot) = &options.snapshot {
                    converted.snapshot =
                        Some(store.snapshot(profile, snapshot).await.map_err(|error| {
                            Error::ConvertedSnapshot {
                                error: Box::new(error),
                                content_digest: converted.verified.content_digest.clone(),
                                state_digest: converted.verified.state_digest.clone(),
                            }
                        })?);
                }
                let mut value = serde_json::to_value(converted)?;
                value["source_capture"] = serde_json::to_value(published)?;
                Ok(value)
            } else {
                Ok(serde_json::to_value(
                    store.convert(profile, &mut source, &options).await?,
                )?)
            }
        }
        Command::GrantReader {
            role, check: true, ..
        } => store.check_reader(profile, role).await,
        Command::GrantReader {
            role, check: false, ..
        } => store.grant_reader(profile, role).await,
        Command::Profile { .. } | Command::Compare(_) => {
            unreachable!("handled before database commands")
        }
    }
}

fn hex_policy(value: &str) -> Result<Vec<u8>, Error> {
    if value.len() != 56 {
        return Err(Error::Refusal("comparison_probe_policy_invalid"));
    }
    (0..56)
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&value[i..i + 2], 16)
                .map_err(|_| Error::Refusal("comparison_probe_policy_invalid"))
        })
        .collect()
}

async fn compare_command(args: &CompareArguments, group: &CancelGroup) -> Result<Value, Error> {
    use dsa_store::compare::{Options, Positions, Reader, capture, compare};
    fn config(
        text: &str,
        password: Option<&std::path::Path>,
    ) -> Result<tokio_postgres::Config, Error> {
        let mut config = tokio_postgres::Config::from_str(text)
            .map_err(|_| Error::Refusal("invalid_comparison_configuration"))?;
        if config.get_password().is_some() {
            return Err(Error::Refusal("comparison_password_requires_file"));
        }
        if let Some(path) = password {
            let value = std::fs::read_to_string(path)
                .map_err(|_| Error::Refusal("invalid_comparison_password_file"))?;
            let value = value.trim_end_matches(['\r', '\n']);
            if value.contains(['\r', '\n', '\0']) {
                return Err(Error::Refusal("invalid_comparison_password_file"));
            }
            config.password(value);
        }
        Ok(config)
    }
    let wrap = |side, error| Error::ComparisonSource {
        side,
        error: Box::new(error),
    };
    let profile = profile(&args.network)?;
    let explicit = if args.positions == "auto" {
        None
    } else {
        use std::io::Read;
        let mut bytes = Vec::new();
        std::fs::File::open(&args.positions)
            .map_err(|_| Error::Refusal("invalid_comparison_positions_file"))?
            .take(1_048_577)
            .read_to_end(&mut bytes)
            .map_err(|_| Error::Refusal("invalid_comparison_positions_file"))?;
        if bytes.len() > 1_048_576 {
            return Err(Error::Refusal("comparison_positions_file_too_large"));
        }
        let positions: Positions = serde_json::from_slice(&bytes)
            .map_err(|_| Error::Refusal("invalid_comparison_positions_file"))?;
        positions.validate()?;
        if args.horizon_block.is_some_and(|h| h != positions.horizon) {
            return Err(Error::Refusal("comparison_horizon_mismatch"));
        }
        Some(positions)
    };
    eprintln!("{}", json!({"operation":"compare","event":"started"}));
    if let Some(file) = &args.a_capture {
        if args.a_password_file.is_some() || args.a_tls_root_cert.is_some() {
            return Err(Error::Refusal("source_capture_has_no_connection"));
        }
        let left = dsa_store::source_capture::read(file, &profile)?;
        let positions = if let Some(positions) = explicit {
            if positions != left.positions {
                return Err(Error::Refusal("comparison_capture_context_mismatch"));
            }
            positions
        } else {
            left.positions.clone()
        };
        if args.horizon_block.is_some_and(|h| h != positions.horizon) {
            return Err(Error::Refusal("comparison_horizon_mismatch"));
        }
        let mut b = Reader::connect(
            config(&args.b, args.b_password_file.as_deref()).map_err(|e| wrap("b", e))?,
            args.b_tls_root_cert.as_deref(),
        )
        .await
        .map_err(|e| wrap("b", e))?;
        group.add(b.cancellation());
        b.limits(
            args.statement_timeout_seconds,
            args.idle_transaction_timeout_seconds,
        )
        .await
        .map_err(|e| wrap("b", e))?;
        let tx = b.snapshot().await.map_err(|e| wrap("b", e))?;
        let mut options = Options {
            check_chain_order: true,
            verify_unbounded: args.verify_unbounded,
            missing_policy: Some(hex_policy(&left.missing_policy)?),
            ..Default::default()
        };
        if let Some(address) = &args.candidate_address {
            options.candidate_address = address.clone();
        }
        let right = capture(&tx, &profile, &positions, &options)
            .await
            .map_err(|e| wrap("b", e))?;
        tx.commit()
            .await
            .map_err(|e| wrap("b", Error::Postgres(e)))?;
        return Ok(serde_json::to_value(compare(&left, &right)?)?);
    }
    let mut a = Reader::connect(
        config(
            args.a
                .as_deref()
                .ok_or(Error::Refusal("comparison_source_required"))?,
            args.a_password_file.as_deref(),
        )
        .map_err(|e| wrap("a", e))?,
        args.a_tls_root_cert.as_deref(),
    )
    .await
    .map_err(|e| wrap("a", e))?;
    let mut b = Reader::connect(
        config(&args.b, args.b_password_file.as_deref()).map_err(|e| wrap("b", e))?,
        args.b_tls_root_cert.as_deref(),
    )
    .await
    .map_err(|e| wrap("b", e))?;
    group.add(a.cancellation());
    group.add(b.cancellation());
    a.limits(
        args.statement_timeout_seconds,
        args.idle_transaction_timeout_seconds,
    )
    .await
    .map_err(|e| wrap("a", e))?;
    b.limits(
        args.statement_timeout_seconds,
        args.idle_transaction_timeout_seconds,
    )
    .await
    .map_err(|e| wrap("b", e))?;
    let tx = a.snapshot().await.map_err(|e| wrap("a", e))?;
    let right_tx = b.snapshot().await.map_err(|e| wrap("b", e))?;
    let missing_policy = dsa_store::compare::choose_missing_policy(&[&tx, &right_tx]).await?;
    let positions = if let Some(p) = explicit {
        p
    } else {
        let h = args
            .horizon_block
            .ok_or(Error::Refusal("comparison_horizon_required"))?;
        let p: i64 = tx
            .query_one("SELECT id FROM public.block ORDER BY id DESC LIMIT 1", &[])
            .await
            .map_err(|e| {
                wrap(
                    "a",
                    if e.code().is_some_and(|c| c.code() == "55000") {
                        Error::Refusal("comparison_not_serving")
                    } else {
                        Error::Postgres(e)
                    },
                )
            })?
            .get(0);
        let t = u32::try_from(p)
            .ok()
            .and_then(|p| p.checked_sub(profile.cardano.security_parameter))
            .ok_or(Error::Refusal("comparison_target_not_final"))?;
        Positions::generate(&tx, h, t, args.max_pairs as usize)
            .await
            .map_err(|e| wrap("a", e))?
    };
    let mut options = Options {
        verify_unbounded: args.verify_unbounded,
        missing_policy: Some(missing_policy),
        ..Default::default()
    };
    if let Some(address) = &args.candidate_address {
        options.candidate_address = address.clone();
    }
    let mut right_options = options.clone();
    right_options.check_chain_order = true;
    // Both snapshots pin the same absence probe. Execute independently so one
    // idle snapshot cannot expire merely while the other endpoint is captured.
    let left_read = async {
        let value = capture(&tx, &profile, &positions, &options)
            .await
            .map_err(|e| wrap("a", e))?;
        tx.commit()
            .await
            .map_err(|e| wrap("a", Error::Postgres(e)))?;
        Ok::<_, Error>(value)
    };
    let right_read = async {
        let value = capture(&right_tx, &profile, &positions, &right_options)
            .await
            .map_err(|e| wrap("b", e))?;
        right_tx
            .commit()
            .await
            .map_err(|e| wrap("b", Error::Postgres(e)))?;
        Ok::<_, Error>(value)
    };
    let (left, right) = tokio::try_join!(left_read, right_read)?;
    let report = compare(&left, &right)?;
    eprintln!(
        "{}",
        json!({"operation":"compare","event":"finished","matched":report.matched,"differing_cases":report.differences.iter().filter(|d|!d.informational).count(),"helper_id_differences":report.helper_id_differences})
    );
    Ok(serde_json::to_value(report)?)
}
