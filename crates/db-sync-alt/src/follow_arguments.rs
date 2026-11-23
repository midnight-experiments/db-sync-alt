//! The provider and follower flags of the commands that follow the chain,
//! and the hidden `follow` command. Every flag is also read from `DSA_` plus
//! its name in upper case with underscores; a flag on the command line wins.

use std::{path::PathBuf, rc::Rc, sync::Arc, time::Duration};

use clap::Args;
use dsa_service::{
    block::Limits,
    follow::{self, Connect, Context, Progress, Settings},
    retry::{Stop, WallTime},
};
use serde_json::json;

use crate::{
    Connection, DATABASE, DatabaseArguments, Usage, connection, open, profile, usage,
    wrapper::{Effect, Job, Operation, Session},
};

#[derive(Args, Clone, Debug)]
pub(crate) struct ProviderArguments {
    /// The provider's base URL [default: Blockfrost for --network].
    #[arg(long, env = "DSA_PROVIDER_URL")]
    pub(crate) provider_url: Option<String>,
    /// A file holding the provider key.
    #[arg(long, env = "DSA_PROVIDER_KEY_FILE")]
    pub(crate) provider_key_file: PathBuf,
    /// PEM roots trusted for the provider in addition to the public ones.
    #[arg(long, env = "DSA_PROVIDER_ROOT_CERT")]
    pub(crate) provider_root_cert: Option<PathBuf>,
    /// Requests sent per second at most (at most 10).
    #[arg(long, env = "DSA_REQUESTS_PER_SECOND", default_value_t = 8.0)]
    pub(crate) requests_per_second: f64,
    /// Requests in flight at once at most.
    #[arg(long, env = "DSA_MAX_IN_FLIGHT", default_value_t = 4,
        value_parser = clap::value_parser!(u32).range(1..=64))]
    pub(crate) max_in_flight: u32,
}

/// The provider's address for a supported network.
fn default_url(network: &str) -> Option<&'static str> {
    match network {
        "mainnet" => Some("https://cardano-mainnet.blockfrost.io/api/v0"),
        "preprod" => Some("https://cardano-preprod.blockfrost.io/api/v0"),
        "preview" => Some("https://cardano-preview.blockfrost.io/api/v0"),
        _ => None,
    }
}

impl ProviderArguments {
    /// The provider client: every value checked and the key file read,
    /// before any connection.
    pub(crate) fn provider(&self, network: &str) -> Result<dsa_provider::Provider, Usage> {
        let url = match &self.provider_url {
            Some(url) => url.clone(),
            None => default_url(network)
                .ok_or(usage("provider_url_required", "--provider-url"))?
                .to_owned(),
        };
        let mut config = dsa_provider::Config::new(url, &self.provider_key_file);
        config.root_certificate = self.provider_root_cert.clone();
        config.requests_per_second = self.requests_per_second;
        config.max_in_flight = self.max_in_flight as usize;
        dsa_provider::Provider::new(config).map_err(|error| {
            let flag = match error.kind {
                dsa_provider::Kind::Fatal {
                    field: "provider.key_file",
                } => "--provider-key-file",
                dsa_provider::Kind::Fatal {
                    field: "provider.root_certificate",
                } => "--provider-root-cert",
                dsa_provider::Kind::Fatal {
                    field: "provider.requests_per_second",
                } => "--requests-per-second",
                dsa_provider::Kind::Fatal {
                    field: "provider.max_in_flight",
                } => "--max-in-flight",
                _ => "--provider-url",
            };
            usage(error.code, flag)
        })
    }
}

#[derive(Args, Clone, Debug)]
pub(crate) struct FollowerArguments {
    /// Seconds between two polls at the tip.
    #[arg(long, env = "DSA_POLL_INTERVAL_SECONDS", default_value_t = 20,
        value_parser = clap::value_parser!(u32).range(1..=3600))]
    pub(crate) poll_interval_seconds: u32,
    /// Seconds between two horizon advances at least.
    #[arg(long, env = "DSA_PRUNE_INTERVAL_SECONDS", default_value_t = 3600,
        value_parser = clap::value_parser!(u32).range(60..=86400))]
    pub(crate) prune_interval_seconds: u32,
    /// Blocks made final by one update at most.
    #[arg(long, env = "DSA_MAX_BATCH_BLOCKS", default_value_t = 100,
        value_parser = clap::value_parser!(u32).range(1..=1000))]
    pub(crate) max_batch_blocks: u32,
    /// Transaction CBOR bytes of one update; the update ends with the block
    /// that reaches it.
    #[arg(long, env = "DSA_MAX_BATCH_CBOR_BYTES", default_value_t = 8 * 1024 * 1024,
        value_parser = clap::value_parser!(u32).range(65_536..=67_108_864))]
    pub(crate) max_batch_cbor_bytes: u32,
    /// Rows of one update; the update ends with the block that reaches it.
    #[arg(long, env = "DSA_MAX_BATCH_ROWS", default_value_t = 50_000,
        value_parser = clap::value_parser!(u32).range(1_000..=1_000_000))]
    pub(crate) max_batch_rows: u32,
}

impl FollowerArguments {
    pub(crate) fn settings(&self, max_in_flight: usize) -> Settings {
        Settings {
            poll_interval: Duration::from_secs(u64::from(self.poll_interval_seconds)),
            prune_interval: Duration::from_secs(u64::from(self.prune_interval_seconds)),
            limits: Limits {
                blocks: self.max_batch_blocks,
                cbor_bytes: self.max_batch_cbor_bytes as usize,
                rows: self.max_batch_rows as usize,
            },
            max_in_flight,
            ..Settings::default()
        }
    }
}

/// The store error the command wrapper reports and maps to an exit status.
/// A database condition keeps its own error, so one that lasted ends with
/// the temporary-failure status.
fn reported(error: follow::Error) -> dsa_store::Error {
    let report = error.report();
    match error {
        follow::Error::Store(error) => error,
        follow::Error::Lasting { error, .. } => match *error {
            follow::Error::Store(error) => error,
            other => reported(other),
        },
        other => {
            eprintln!("{}", json!({"event":"follow_failed","error":report}));
            dsa_store::Error::Refusal(match other {
                follow::Error::Provider(error) => error.code,
                follow::Error::Answer { code, .. }
                | follow::Error::Check { code, .. }
                | follow::Error::Refusal { code, .. } => code,
                _ => "stop_requested",
            })
        }
    }
}

/// `follow`: the loop over the provider until a stop request or the stop
/// target, through the command wrapper with a grace of 5 seconds.
pub(crate) fn command(
    database: DatabaseArguments,
    provider: ProviderArguments,
    follower: FollowerArguments,
    stop_at_target: Option<u32>,
) -> Result<(Operation, Job), Usage> {
    let profile = profile(&database.network)?;
    let destination = connection(
        &DATABASE,
        &database.database,
        database.password_file.as_deref(),
        database.tls_root_cert.as_deref(),
    )?;
    let client = provider.provider(&database.network)?;
    let mut settings = follower.settings(provider.max_in_flight as usize);
    settings.stop_at_target = stop_at_target;
    let mut operation = Operation::new("follow", Effect::Changes, database.overall_timeout_seconds);
    operation.grace = Duration::from_secs(5);
    let database = Rc::new(database);
    Ok((
        operation,
        Box::new(move |session: Session| {
            Box::pin(async move {
                let connect: Connect = {
                    let session = session.clone();
                    Box::new(move || {
                        let connection = Connection {
                            config: destination.config.clone(),
                            root: destination.root.clone(),
                        };
                        let (database, session) = (Rc::clone(&database), session.clone());
                        Box::pin(async move { open(connection, &database, &session).await })
                    })
                };
                let (stopper, stop) = Stop::new();
                let (progress, _view) = tokio::sync::watch::channel(Progress::default());
                let mut run = std::pin::pin!(follow::follow(Context {
                    profile: &profile,
                    provider: client,
                    connect,
                    settings,
                    time: Arc::new(WallTime),
                    stop,
                    progress,
                }));
                let result = tokio::select! {
                    result = &mut run => result,
                    () = session.stopping() => {
                        let _ = stopper.send(true);
                        run.await
                    }
                };
                let report = result.map_err(reported)?;
                Ok(serde_json::to_value(report)?)
            })
        }),
    ))
}
