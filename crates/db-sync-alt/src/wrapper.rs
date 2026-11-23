//! One wrapper for every command: signal handling that cancels the command's
//! statements and drops it (removing its temporary files), an optional
//! deadline, one start and one end log line, and the exit status. A
//! long-running command watches `Session::stopping` and ends within its grace
//! period; the wrapper never assumes that a command ends by itself.

use std::{
    future::Future,
    pin::Pin,
    process::ExitCode,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use dsa_store::{Cancellation, Error};
use serde_json::{Value, json};
use tokio::sync::watch;

pub(crate) const EXIT_STATUSES: &str = "\
Exit status:
  0   success
  1   the operation failed
  2   usage error: an invalid command line or flag value, found before any connection
  3   a check found a difference: compare did not match, or verify or
      grant-reader --check refused the database
  75  temporary failure: the database could not be reached or is restarting;
      the same command may succeed later";

/// The exit statuses of every command; see `EXIT_STATUSES`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Exit {
    Success = 0,
    Failed = 1,
    Usage = 2,
    Difference = 3,
    Temporary = 75,
}

/// How a command's refusal and its stop are reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Effect {
    /// Reads only.
    Reads,
    /// Changes the destination: a stop before the commit that publishes
    /// reports `published: false`, a stop after it an unknown outcome.
    Changes,
    /// Checks the database: its refusal is a difference (exit status 3).
    Checks,
}

pub(crate) struct Operation {
    pub(crate) name: &'static str,
    pub(crate) effect: Effect,
    pub(crate) deadline: Option<Duration>,
    /// How long a stopped command may take to end by itself before its
    /// statements are cancelled and it is dropped. A long-running command
    /// watches `Session::stopping` and finishes its step within this time.
    pub(crate) grace: Duration,
}

impl Operation {
    pub(crate) fn new(name: &'static str, effect: Effect, deadline: Option<u32>) -> Self {
        Self {
            name,
            effect,
            deadline: deadline.map(|seconds| Duration::from_secs(u64::from(seconds))),
            grace: Duration::ZERO,
        }
    }
}

/// A handle of one database session the wrapper may cancel.
trait Handle {
    fn closed(&self) -> bool;
    fn commit_sent(&self) -> bool;
}

impl Handle for Cancellation {
    fn closed(&self) -> bool {
        Cancellation::closed(self)
    }
    fn commit_sent(&self) -> bool {
        Cancellation::commit_sent(self)
    }
}

/// The handles of a command's sessions. A connection registered under a
/// role (the writer, the status read, the upkeep) replaces the earlier one of
/// that role, so a command that reconnects keeps one handle per live
/// connection. Other connections are kept until the server has ended them,
/// because the backend of a dropped connection may still run a statement
/// that a stop must cancel. Whether any of them sent a commit that publishes
/// is kept.
struct Registry<H> {
    roles: std::collections::BTreeMap<&'static str, H>,
    others: Vec<H>,
    commit_sent: bool,
}

impl<H> Default for Registry<H> {
    fn default() -> Self {
        Self {
            roles: std::collections::BTreeMap::new(),
            others: Vec::new(),
            commit_sent: false,
        }
    }
}

impl<H: Handle + Clone> Registry<H> {
    fn add(&mut self, handle: H) {
        let (closed, live): (Vec<H>, Vec<H>) = self.others.drain(..).partition(H::closed);
        self.commit_sent |= closed.iter().any(H::commit_sent);
        self.others = live;
        self.others.push(handle);
    }

    fn replace(&mut self, role: &'static str, handle: H) {
        if let Some(earlier) = self.roles.insert(role, handle) {
            self.commit_sent |= earlier.commit_sent();
        }
    }

    fn commit_sent(&self) -> bool {
        self.commit_sent || self.roles.values().chain(&self.others).any(H::commit_sent)
    }

    fn live(&self) -> Vec<H> {
        self.roles
            .values()
            .chain(&self.others)
            .filter(|handle| !handle.closed())
            .cloned()
            .collect()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.roles.len() + self.others.len()
    }
}

/// What a running command shares with the wrapper: its database sessions, to
/// cancel when it is stopped or fails, and the stop request.
#[derive(Clone)]
pub(crate) struct Session {
    cancellations: Arc<Mutex<Registry<Cancellation>>>,
    stop: watch::Receiver<bool>,
}

impl Session {
    /// Registers a new connection; the handles of connections the server
    /// ended go.
    pub(crate) fn watch(&self, cancellation: Cancellation) {
        self.cancellations
            .lock()
            .expect("cancellation registry")
            .add(cancellation);
    }

    /// Registers the connection of `role`, replacing the earlier one.
    pub(crate) fn watch_as(&self, role: &'static str, cancellation: Cancellation) {
        self.cancellations
            .lock()
            .expect("cancellation registry")
            .replace(role, cancellation);
    }

    /// Whether a commit that publishes was sent.
    fn commit_sent(&self) -> bool {
        self.cancellations
            .lock()
            .expect("cancellation registry")
            .commit_sent()
    }

    /// Resolves when the command is asked to stop.
    pub(crate) async fn stopping(&self) {
        let mut stop = self.stop.clone();
        let _ = stop.wait_for(|stop| *stop).await;
    }

    /// Cancels the running statement of every session, each within 5 s.
    async fn cancel(&self) {
        let handles = self
            .cancellations
            .lock()
            .expect("cancellation registry")
            .live();
        let mut pending = tokio::task::JoinSet::new();
        for (index, handle) in handles.into_iter().enumerate() {
            pending.spawn(async move {
                let result = tokio::time::timeout(Duration::from_secs(5), handle.cancel()).await;
                let sent = matches!(result, Ok(Ok(())));
                eprintln!(
                    "{}",
                    json!({"event": "cancel_request", "connection": index, "sent": sent})
                );
            });
        }
        while pending.join_next().await.is_some() {}
    }
}

/// Resolves with the reason to stop: SIGINT, SIGTERM or the deadline.
async fn stop_requested(deadline: Option<Duration>) -> Result<&'static str, Error> {
    let deadline = async {
        match deadline {
            Some(deadline) => tokio::time::sleep(deadline).await,
            None => std::future::pending().await,
        }
    };
    let failed = |_| Error::Refusal("signal_listener_failed");
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate()).map_err(failed)?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.map(|()| "operation_interrupted").map_err(failed),
            _ = terminate.recv() => Ok("operation_interrupted"),
            () = deadline => Ok("operation_deadline_exceeded"),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.map(|()| "operation_interrupted").map_err(failed),
            () = deadline => Ok("operation_deadline_exceeded"),
        }
    }
}

pub(crate) type Work = Pin<Box<dyn Future<Output = Result<Value, Error>>>>;
pub(crate) type Job = Box<dyn FnOnce(Session) -> Work>;

/// Runs one command: logs its start, races it against a stop request, cancels
/// its statements and drops it (which removes its temporary files) when it is
/// stopped or fails, prints the result, logs the end with outcome and duration
/// and returns the exit status. It never assumes that the command ends.
pub(crate) async fn run(operation: Operation, command: Job) -> ExitCode {
    let started = Instant::now();
    eprintln!(
        "{}",
        json!({"event": "started", "operation": operation.name})
    );
    let (stop, stop_requests) = watch::channel(false);
    let session = Session {
        cancellations: Arc::default(),
        stop: stop_requests,
    };
    let (result, stopped) = supervise(
        &operation,
        &session,
        command(session.clone()),
        stop_requested(operation.deadline),
        &stop,
    )
    .await;
    let exit = match &result {
        Ok(value) => {
            println!("{value}");
            if value.get("matched") == Some(&Value::Bool(false)) {
                Exit::Difference
            } else {
                Exit::Success
            }
        }
        Err(error) => {
            eprintln!("{}", error.report());
            match stopped {
                Some(_) => Exit::Failed,
                None => exit_status(error, operation.effect),
            }
        }
    };
    let outcome = match (stopped, exit) {
        (Some("operation_deadline_exceeded"), _) => "deadline_exceeded",
        (Some(_), _) => "interrupted",
        (None, Exit::Success) => "success",
        (None, Exit::Difference) => "difference",
        (None, Exit::Temporary) => "temporary_failure",
        (None, _) => "failed",
    };
    let mut end = json!({
        "event": "finished",
        "operation": operation.name,
        "outcome": outcome,
        "exit": exit as u8,
        "duration_seconds": started.elapsed().as_secs_f64(),
    });
    if let Ok(value) = &result
        && let Some(matched) = value.get("matched")
    {
        end["matched"] = matched.clone();
    }
    eprintln!("{end}");
    ExitCode::from(exit as u8)
}

/// The race of `run`, apart from the process signals so that tests can drive
/// it. Returns the result and, when the command was stopped, the reason.
async fn supervise(
    operation: &Operation,
    session: &Session,
    mut work: Work,
    stop_request: impl Future<Output = Result<&'static str, Error>>,
    stop: &watch::Sender<bool>,
) -> (Result<Value, Error>, Option<&'static str>) {
    let mut stopped = None;
    let result = tokio::select! {
        biased;
        result = &mut work => result,
        request = stop_request => match request {
            Err(error) => Err(error),
            Ok(reason) => {
                stopped = Some(reason);
                let _ = stop.send(true);
                match tokio::time::timeout(operation.grace, &mut work).await {
                    Ok(result) => result,
                    Err(_) => Err(interrupted(operation, session, reason)),
                }
            }
        },
    };
    if result.is_err() {
        session.cancel().await;
    }
    // Dropping the command closes its connections and removes its temporary
    // files before anything is printed.
    drop(work);
    (result, stopped)
}

fn interrupted(operation: &Operation, session: &Session, reason: &'static str) -> Error {
    match operation.effect {
        Effect::Changes if session.commit_sent() => Error::OutcomeUnknown {
            operation: operation.name,
            reason,
        },
        Effect::Changes => Error::Interrupted {
            operation: operation.name,
            reason,
        },
        Effect::Reads | Effect::Checks => Error::Refusal(reason),
    }
}

fn exit_status(error: &Error, effect: Effect) -> Exit {
    if error.temporary() {
        Exit::Temporary
    } else if effect == Effect::Checks
        && matches!(
            error,
            Error::Refusal(_) | Error::Mismatch { .. } | Error::ReaderPrivileges(_)
        )
    {
        Exit::Difference
    } else {
        Exit::Failed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session() -> (Session, watch::Sender<bool>) {
        let (stop, requests) = watch::channel(false);
        (
            Session {
                cancellations: Arc::default(),
                stop: requests,
            },
            stop,
        )
    }

    /// A long-running command ends by itself within its grace period when it
    /// watches the stop request; the wrapper waits for it.
    #[tokio::test]
    async fn a_long_running_command_finishes_its_step_on_a_stop() {
        let (session, stop) = session();
        let mut operation = Operation::new("follow", Effect::Changes, None);
        operation.grace = Duration::from_secs(5);
        let watched = session.clone();
        let work: Work = Box::pin(async move {
            let mut steps = 0_u64;
            loop {
                tokio::select! {
                    () = watched.stopping() => return Ok(json!({"steps": steps})),
                    () = tokio::time::sleep(Duration::from_millis(5)) => steps += 1,
                }
            }
        });
        let request = async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok("operation_interrupted")
        };
        let (result, stopped) = supervise(&operation, &session, work, request, &stop).await;
        assert_eq!(stopped, Some("operation_interrupted"));
        assert!(result.unwrap()["steps"].as_u64().unwrap() > 0);
    }

    /// A command that does not end within its grace period is dropped and
    /// reported as interrupted; before any commit it published nothing.
    #[tokio::test]
    async fn a_command_that_ignores_the_stop_is_dropped_unpublished() {
        let (session, stop) = session();
        let operation = Operation::new("restore", Effect::Changes, None);
        struct Dropped(Arc<Mutex<bool>>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                *self.0.lock().unwrap() = true;
            }
        }
        let dropped = Arc::new(Mutex::new(false));
        let guard = Dropped(Arc::clone(&dropped));
        let work: Work = Box::pin(async move {
            let _guard = guard;
            std::future::pending::<()>().await;
            Ok(Value::Null)
        });
        let (result, stopped) = supervise(
            &operation,
            &session,
            work,
            async { Ok("operation_deadline_exceeded") },
            &stop,
        )
        .await;
        assert_eq!(stopped, Some("operation_deadline_exceeded"));
        assert!(*dropped.lock().unwrap());
        let report = result.unwrap_err().report();
        assert_eq!(report["published"], false);
        assert_eq!(report["error"], "operation_deadline_exceeded");
    }

    #[derive(Clone)]
    struct Fake {
        closed: Arc<std::sync::atomic::AtomicBool>,
        commit_sent: bool,
    }

    impl Handle for Fake {
        fn closed(&self) -> bool {
            self.closed.load(std::sync::atomic::Ordering::SeqCst)
        }
        fn commit_sent(&self) -> bool {
            self.commit_sent
        }
    }

    fn fake(commit_sent: bool) -> Fake {
        Fake {
            closed: Arc::default(),
            commit_sent,
        }
    }

    /// A service whose writer reconnects a thousand times keeps one handle
    /// for it; a connection the server ended goes when the next one is
    /// registered; a dropped one that may still run a statement stays; a
    /// commit sent on a replaced or ended connection is still known.
    #[test]
    fn a_reconnect_replaces_the_handle_of_its_role() {
        let mut registry = Registry::default();
        registry.replace("status", fake(false));
        let mut writer = fake(true);
        registry.replace("writer", writer.clone());
        for _ in 0..1_000 {
            writer
                .closed
                .store(true, std::sync::atomic::Ordering::SeqCst);
            writer = fake(false);
            registry.replace("writer", writer.clone());
            assert_eq!(registry.len(), 2);
        }
        assert!(registry.commit_sent(), "the first writer sent a commit");
        // Other connections: one the server ended goes, a dropped one stays.
        let ended = fake(false);
        registry.add(ended.clone());
        let dropped = fake(false);
        registry.add(dropped.clone());
        assert_eq!(registry.len(), 4);
        ended
            .closed
            .store(true, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(registry.live().len(), 3, "an ended one is not cancelled");
        registry.add(fake(false));
        assert_eq!(
            registry.len(),
            4,
            "the ended one went, the dropped one stayed"
        );
    }

    #[test]
    fn exit_statuses_are_distinct_per_class() {
        let statuses = [
            Exit::Success,
            Exit::Failed,
            Exit::Usage,
            Exit::Difference,
            Exit::Temporary,
        ]
        .map(|exit| exit as u8);
        assert_eq!(statuses, [0, 1, 2, 3, 75]);
        assert_eq!(
            exit_status(&Error::Refusal("ineligible_asset_row"), Effect::Checks),
            Exit::Difference
        );
        assert_eq!(
            exit_status(&Error::Refusal("ineligible_asset_row"), Effect::Reads),
            Exit::Failed
        );
        assert_eq!(
            exit_status(
                &Error::ComparisonQuery {
                    query: "q".to_owned(),
                    sqlstate: Some("57P03".to_owned())
                },
                Effect::Checks
            ),
            Exit::Temporary
        );
        for status in statuses {
            assert!(EXIT_STATUSES.contains(&format!("  {status} ")));
        }
    }
}
