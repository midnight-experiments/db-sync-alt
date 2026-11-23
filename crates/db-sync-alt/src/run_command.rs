//! The `run` command: the service with the database, provider, follower and
//! service flags, every value checked before anything connects or listens.

use std::{sync::Arc, time::Duration};

use dsa_service::{
    follow,
    retry::{Stop, WallTime},
    run::{Context, Open, REFRESH_INTERVAL, Role, UPKEEP_TICK, run},
    upkeep::IndexPolicy,
};
use serde_json::json;

use crate::{
    Connection, DATABASE, DatabaseArguments, Usage, connection,
    follow_arguments::{FollowerArguments, ProviderArguments},
    open, profile,
    service_arguments::ServiceArguments,
    usage,
    wrapper::{Effect, Job, Operation, Session},
};

pub(crate) const EXIT_STATUSES: &str = "\
Exit status:
  0   stopped by SIGINT or SIGTERM
  1   a condition that retrying cannot cure: a rejected provider key, a
      database of another network or format, a second writer, a retention
      that differs from the stored one, a reader with more rights than the
      node needs, an empty database without --snapshot, a closed gate found
      at start, or a check that closed it
  2   an invalid flag value, found before any connection
  75  the database stayed unreachable, or the provider's transport failed,
      for longer than --database-fatal-after-seconds";

/// The service's error as the command wrapper reports it: named, and
/// temporary when it is a condition that lasted.
fn ended(error: follow::Error) -> dsa_store::Error {
    let temporary = match &error {
        follow::Error::Lasting { .. } => true,
        follow::Error::Store(error) => error.temporary(),
        _ => false,
    };
    dsa_store::Error::Reported {
        report: error.report(),
        temporary,
    }
}

/// `run`: the service until SIGINT or SIGTERM, through the command wrapper
/// with a grace of 10 seconds. It runs until it is stopped, so it refuses an
/// overall timeout.
pub(crate) fn command(
    database: DatabaseArguments,
    provider: ProviderArguments,
    follower: FollowerArguments,
    service: ServiceArguments,
) -> Result<(Operation, Job), Usage> {
    if database.overall_timeout_seconds.is_some() {
        return Err(usage(
            "overall_timeout_not_supported",
            "--overall-timeout-seconds",
        ));
    }
    let profile = profile(&database.network)?;
    let destination = connection(
        &DATABASE,
        &database.database,
        database.password_file.as_deref(),
        database.tls_root_cert.as_deref(),
    )?;
    let client = provider.provider(&database.network)?;
    let settings = service.settings()?;
    let follower = follower.settings(provider.max_in_flight as usize);
    let mut operation = Operation::new("run", Effect::Serves, None);
    operation.grace = Duration::from_secs(10);
    let database = Arc::new(database);
    Ok((
        operation,
        Box::new(move |session: Session| {
            Box::pin(async move {
                let listener = tokio::net::TcpListener::bind(settings.listen)
                    .await
                    .map_err(|error| dsa_store::Error::Reported {
                        report: json!({"error":"listen_address_unavailable","flag":"--listen",
                            "address":settings.listen.to_string(),"kind":format!("{:?}",error.kind())}),
                        temporary: false,
                    })?;
                let open: Open = {
                    let session = session.clone();
                    Arc::new(move |role: Role| {
                        let connection = Connection {
                            config: destination.config.clone(),
                            root: destination.root.clone(),
                        };
                        let (database, session) = (Arc::clone(&database), session.clone());
                        Box::pin(async move {
                            open(connection, &database, &session, Some(role.as_str())).await
                        })
                    })
                };
                let (stopper, stop) = Stop::new();
                let mut service = std::pin::pin!(run(Context {
                    profile,
                    provider: client,
                    open,
                    settings,
                    follower,
                    index_policy: IndexPolicy::default(),
                    refresh_interval: REFRESH_INTERVAL,
                    upkeep_tick: UPKEEP_TICK,
                    listener,
                    time: Arc::new(WallTime),
                    stop,
                }));
                let result = tokio::select! {
                    result = &mut service => result,
                    () = session.stopping() => {
                        let _ = stopper.send(true);
                        service.await
                    }
                };
                let report = result.map_err(ended)?;
                Ok(serde_json::to_value(report)?)
            })
        }),
    ))
}
