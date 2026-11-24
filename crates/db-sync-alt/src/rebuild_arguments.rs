//! The hidden `rebuild` command: an empty database built from the provider
//! alone, verified and left serving.

use std::{sync::Arc, time::Duration};

use clap::Args;
use dsa_service::{
    follow::{Connect, Progress},
    rebuild::{self, Context, Options},
    retry::{Stop, WallTime},
};

use crate::{
    Connection, DATABASE, DatabaseArguments, Usage, connection,
    follow_arguments::{FollowerArguments, ProviderArguments, reported},
    open, profile,
    wrapper::{Effect, Job, Operation, Session},
};

#[derive(Args, Clone, Debug)]
pub(crate) struct RebuildArguments {
    /// The retention the built database keeps.
    #[arg(long, default_value_t = 604_800,
        value_parser = clap::value_parser!(i64).range(86_400..))]
    pub(crate) retention_seconds: i64,
    /// The window the build starts with, at least 24 hours and at most the
    /// retention [default: the retention].
    #[arg(long,
        value_parser = clap::value_parser!(i64).range(86_400..))]
    pub(crate) window_seconds: Option<i64>,
    /// The horizon block [default: the newest block at or before the
    /// target's time minus the window].
    #[arg(long)]
    pub(crate) horizon_block: Option<u32>,
    /// The target block [default: the provider's tip minus k].
    #[arg(long)]
    pub(crate) target_block: Option<u32>,
}

/// `rebuild`: builds or continues a build, then publishes it, through the
/// command wrapper with a grace of 5 seconds.
pub(crate) fn command(
    database: DatabaseArguments,
    provider: ProviderArguments,
    follower: FollowerArguments,
    choices: RebuildArguments,
) -> Result<(Operation, Job), Usage> {
    let profile = profile(&database.network)?;
    let destination = connection(
        &DATABASE,
        &database.database,
        database.password_file.as_deref(),
        database.tls_root_cert.as_deref(),
    )?;
    if choices
        .window_seconds
        .is_some_and(|window| window > choices.retention_seconds)
    {
        return Err(crate::usage("window_above_retention", "--window-seconds"));
    }
    if let (Some(horizon), Some(target)) = (choices.horizon_block, choices.target_block)
        && horizon > target
    {
        return Err(crate::usage("horizon_above_target", "--horizon-block"));
    }
    let client = provider.provider(&database.network)?;
    let settings = follower.settings(provider.max_in_flight as usize);
    let mut operation =
        Operation::new("rebuild", Effect::Changes, database.overall_timeout_seconds);
    operation.grace = Duration::from_secs(5);
    let database = Arc::new(database);
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
                        let (database, session) = (Arc::clone(&database), session.clone());
                        Box::pin(async move {
                            open(connection, &database, &session, Some("writer")).await
                        })
                    })
                };
                let (stopper, stop) = Stop::new();
                let (progress, _view) = tokio::sync::watch::channel(Progress::default());
                let mut run = std::pin::pin!(rebuild::rebuild(Context {
                    profile: &profile,
                    provider: client,
                    connect,
                    settings,
                    options: Options {
                        horizon_block: choices.horizon_block,
                        target_block: choices.target_block,
                        window_seconds: choices.window_seconds,
                        retention_seconds: choices.retention_seconds,
                    },
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
