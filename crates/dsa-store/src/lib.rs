//! PostgreSQL layout, database identity and least-privilege node access.

mod canonical;
pub mod catalogue;
pub mod compare;
mod conversion_stage;
pub mod convert;
pub mod follow;
pub mod horizon;
pub mod keep;
mod load;
mod restore;
pub mod service_view;
pub mod snapshot;
pub mod source_capture;
pub mod upkeep;
pub mod verify;

use std::{
    io::BufReader,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use dsa_chain::profiles::Profile;
use serde_json::{Value, json};
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_postgres::{Client, Config, GenericClient, Transaction, config::SslMode};
use tokio_postgres_rustls::MakeRustlsConnect;

pub const DATABASE_FORMAT: i32 = 1;
pub const KEEP_RULE_VERSION: i32 = dsa_chain::assets::RULE_VERSION as i32;
pub const NODE_RELATIONS: [&str; 11] = dsa_chain::keep::TABLES;
const WRITER_NAMESPACE: i32 = 0x647361;
const WRITER_LOCK: i32 = 1;
const READER_SETTINGS: [&str; 4] = [
    "statement_timeout",
    "idle_in_transaction_session_timeout",
    "search_path",
    "default_transaction_read_only",
];

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Refusal(&'static str),
    #[error("reader_privileges_not_exact")]
    ReaderPrivileges(Vec<String>),
    #[error("database_error")]
    Postgres(#[from] tokio_postgres::Error),
    #[error("comparison_query_failed")]
    ComparisonQuery {
        query: String,
        sqlstate: Option<String>,
    },
    #[error("comparison_source_failed")]
    ComparisonSource {
        side: &'static str,
        error: Box<Error>,
    },
    #[error("profile_error")]
    Profile(#[from] serde_json::Error),
    #[error("snapshot_io_error")]
    Io(#[from] std::io::Error),
    #[error("snapshot_published_durability_uncertain")]
    PublishedDurabilityUncertain,
    #[error("conversion_completed_snapshot_failed")]
    ConvertedSnapshot {
        error: Box<Error>,
        content_digest: String,
        state_digest: String,
    },
    /// Stopped by a signal or deadline before the publishing commit was sent.
    #[error("{reason}")]
    Interrupted {
        operation: &'static str,
        reason: &'static str,
    },
    /// Stopped after the publishing commit was sent.
    #[error("{reason}")]
    OutcomeUnknown {
        operation: &'static str,
        reason: &'static str,
    },
    #[error("{error}")]
    InFamily {
        family: String,
        #[source]
        error: Box<Error>,
    },
    #[error("{error}")]
    Mismatch {
        error: &'static str,
        item: String,
        expected: String,
        actual: String,
    },
    #[error("source_read_failed")]
    SourceRead {
        family: &'static str,
        elapsed_seconds: f64,
        error: tokio_postgres::Error,
    },
    /// A failure named by the layer above the store, reported as it named
    /// it; `temporary` when the same command may succeed later.
    #[error("{}", reported_code(report))]
    Reported { report: Value, temporary: bool },
}

fn reported_code(report: &Value) -> &str {
    report["error"].as_str().unwrap_or("service_error")
}

impl Error {
    /// The named cause, with the server's message for the store's own
    /// statements. Never a connection string or password.
    pub fn report(&self) -> Value {
        match self {
            Self::ReaderPrivileges(details) => {
                json!({"error": self.to_string(), "details": details})
            }
            Self::Postgres(error) => postgres_report(error),
            Self::SourceRead {
                family,
                elapsed_seconds,
                error,
            } => {
                let mut report = postgres_report(error);
                report["error"] = json!(if error.code().map(|c| c.code()) == Some("57014") {
                    "source_statement_timeout"
                } else {
                    "source_read_failed"
                });
                report["family"] = json!(family);
                report["elapsed_seconds"] = json!(elapsed_seconds);
                report
            }
            Self::ComparisonQuery { query, sqlstate } => {
                let error = match sqlstate.as_deref() {
                    Some("55000") => "comparison_not_serving",
                    Some("57014") => "comparison_statement_timeout",
                    _ => "comparison_query_failed",
                };
                json!({"error":error,"query":query,"sqlstate":sqlstate})
            }
            Self::ComparisonSource { side, error } => {
                json!({"error":"comparison_source_failed","side":side,"cause":error.report()})
            }
            Self::Io(error) => {
                json!({"error":"snapshot_io_error","kind":format!("{:?}",error.kind()),"os_error":error.raw_os_error()})
            }
            Self::PublishedDurabilityUncertain => {
                json!({"error":"snapshot_published_durability_uncertain","published":true,"durability":"uncertain"})
            }
            Self::ConvertedSnapshot {
                error,
                content_digest,
                state_digest,
            } => {
                json!({"error":"conversion_completed_snapshot_failed","converted":true,"content_digest":content_digest,"state_digest":state_digest,"recovery":"The database is serving. Run snapshot with this destination and a new writable --file path; do not repeat convert.","snapshot_error":error.report()})
            }
            Self::Interrupted { operation, reason } => {
                let recovery = match *operation {
                    "restore" | "convert" => {
                        "Nothing was published and the database is not serving. Run the command again."
                    }
                    "snapshot" => "No snapshot file was written. Run the command again.",
                    _ => "Nothing was changed. Run the command again.",
                };
                json!({"error":reason,"operation":operation,"published":false,"recovery":recovery})
            }
            Self::OutcomeUnknown { operation, reason } => {
                let phase = if *operation == "convert" {
                    "conversion_or_optional_snapshot"
                } else {
                    "publication"
                };
                json!({"error":reason,"operation":operation,"phase":phase,"outcome":"unknown","rollback_confirmed":false,"recovery":"Run status and verify on this exact destination. If it is serving and verifies, it is published; keep it. Otherwise run the command again."})
            }
            Self::InFamily { family, error } => {
                let mut report = error.report();
                report["family"] = json!(family);
                report
            }
            Self::Mismatch {
                error,
                item,
                expected,
                actual,
            } => json!({"error":error,"item":item,"expected":expected,"actual":actual}),
            Self::Reported { report, .. } => report.clone(),
            _ => json!({"error": self.to_string()}),
        }
    }
}

impl Error {
    /// The database could not be reached, closed the connection or is
    /// starting or stopping: the same command may succeed later.
    pub fn temporary(&self) -> bool {
        match self {
            Self::Postgres(error) | Self::SourceRead { error, .. } => postgres_temporary(error),
            Self::ComparisonQuery { sqlstate, .. } => {
                sqlstate.as_deref().is_some_and(temporary_state)
            }
            Self::ComparisonSource { error, .. } | Self::InFamily { error, .. } => {
                error.temporary()
            }
            Self::Reported { temporary, .. } => *temporary,
            _ => false,
        }
    }
}

/// Connection exceptions, administrator or crash shutdown, a server that is
/// starting, and too many connections.
fn temporary_state(code: &str) -> bool {
    code.starts_with("08") || matches!(code, "57P01" | "57P02" | "57P03" | "53300")
}

fn postgres_temporary(error: &tokio_postgres::Error) -> bool {
    if let Some(code) = error.code() {
        return temporary_state(code.code());
    }
    if error.as_db_error().is_some() {
        return false;
    }
    if error.is_closed() {
        return true;
    }
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error);
    for _ in 0..16 {
        let Some(cause) = current else { break };
        if cause.downcast_ref::<rustls::Error>().is_some() {
            return false;
        }
        if let Some(io) = cause.downcast_ref::<std::io::Error>() {
            use std::io::ErrorKind::*;
            if matches!(
                io.kind(),
                ConnectionRefused
                    | ConnectionReset
                    | ConnectionAborted
                    | NotConnected
                    | BrokenPipe
                    | TimedOut
                    | HostUnreachable
                    | NetworkUnreachable
                    | UnexpectedEof
            ) {
                return true;
            }
        }
        current = cause.source();
    }
    false
}

// Error strings and Debug output of IO errors can carry paths or context.
// Only their standard kinds and OS error numbers are reported.
fn io_causes(mut error: Option<&(dyn std::error::Error + 'static)>) -> Vec<Value> {
    let mut causes = Vec::new();
    for _ in 0..16 {
        let Some(current) = error else { break };
        if let Some(io) = current.downcast_ref::<std::io::Error>() {
            causes.push(json!({"kind":format!("{:?}",io.kind()),"os_error":io.raw_os_error()}));
        }
        error = current.source();
    }
    causes
}

fn tls_cause(error: &rustls::Error) -> &'static str {
    use rustls::CertificateError as Certificate;
    match error {
        rustls::Error::InvalidCertificate(
            Certificate::NotValidForName | Certificate::NotValidForNameContext { .. },
        ) => "tls_host_name_mismatch",
        rustls::Error::InvalidCertificate(Certificate::UnknownIssuer) => "tls_unknown_ca",
        rustls::Error::InvalidCertificate(_) => "tls_certificate_invalid",
        _ => "tls_error",
    }
}

/// Names why a connection failed: TLS host name, CA or another TLS failure, a
/// timeout, a refused connection, or a closed one.
fn connection_cause(error: &tokio_postgres::Error) -> &'static str {
    cause_in_chain(error).unwrap_or(if error.is_closed() {
        "connection_closed"
    } else {
        "client_error"
    })
}

fn cause_in_chain(error: &(dyn std::error::Error + 'static)) -> Option<&'static str> {
    let mut current = Some(error);
    for _ in 0..16 {
        let Some(cause) = current else { break };
        if let Some(tls) = cause.downcast_ref::<rustls::Error>() {
            return Some(tls_cause(tls));
        }
        if let Some(io) = cause.downcast_ref::<std::io::Error>() {
            if let Some(tls) = io
                .get_ref()
                .and_then(|inner| inner.downcast_ref::<rustls::Error>())
            {
                return Some(tls_cause(tls));
            }
            match io.kind() {
                std::io::ErrorKind::TimedOut => return Some("connection_timeout"),
                std::io::ErrorKind::ConnectionRefused => return Some("connection_refused"),
                _ => {}
            }
        }
        current = cause.source();
    }
    None
}

pub(crate) fn postgres_report(error: &tokio_postgres::Error) -> Value {
    let mut report =
        json!({"error":"database_error","sqlstate":error.code().map(|code|code.code())});
    if let Some(server) = error.as_db_error() {
        report["message"] = json!(server.message());
        for (key, value) in [
            ("detail", server.detail()),
            ("hint", server.hint()),
            ("schema", server.schema()),
            ("table", server.table()),
            ("column", server.column()),
            ("constraint", server.constraint()),
        ] {
            if let Some(value) = value {
                report[key] = json!(value);
            }
        }
    } else {
        // The driver's own text names the failure (a closed connection, an
        // unexpected type); it never contains the connection settings.
        report["message"] = json!(error.to_string());
        report["cause"] = json!(connection_cause(error));
        report["io_causes"] = json!(io_causes(Some(error)));
    }
    report
}

pub struct Store {
    client: Client,
    connection: JoinHandle<()>,
    writer_locked: bool,
    cancellation: Cancellation,
    probe: std::sync::Mutex<follow::Probe>,
}
/// Cancels this backend's statement using the same TLS validation, and tells
/// whether the store sent the commit that publishes a load and whether its
/// connection has closed.
#[derive(Clone)]
pub struct Cancellation {
    token: tokio_postgres::CancelToken,
    tls: MakeRustlsConnect,
    commit_sent: Arc<AtomicBool>,
    closed: Arc<AtomicBool>,
}
impl Cancellation {
    pub async fn cancel(&self) -> Result<(), Error> {
        Ok(self.token.cancel_query(self.tls.clone()).await?)
    }
    pub fn commit_sent(&self) -> bool {
        self.commit_sent.load(Ordering::SeqCst)
    }
    /// The connection ended or its store was dropped: nothing is left to
    /// cancel.
    pub fn closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        // Closing the socket also releases this session's advisory lock.
        self.connection.abort();
        self.cancellation.closed.store(true, Ordering::SeqCst);
    }
}

impl Store {
    /// A custom CA requires TLS and verifies the server hostname. Explicit
    /// sslmode=disable supports operator-provided local connections.
    pub async fn connect(
        mut config: Config,
        root_certificate: Option<&Path>,
    ) -> Result<Self, Error> {
        if config.get_connect_timeout().is_none() {
            config.connect_timeout(Duration::from_secs(10));
        }
        if config.get_tcp_user_timeout().is_none() {
            config.tcp_user_timeout(Duration::from_secs(30));
        }
        config.keepalives_idle(config.get_keepalives_idle().min(Duration::from_secs(30)));
        let mut roots = rustls::RootCertStore::empty();
        if let Some(path) = root_certificate {
            if config.get_ssl_mode() == SslMode::Disable {
                return Err(Error::Refusal("conflicting_tls_configuration"));
            }
            let file = std::fs::File::open(path)
                .map_err(|_| Error::Refusal("invalid_tls_root_certificate"))?;
            let certificates = rustls_pemfile::certs(&mut BufReader::new(file))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| Error::Refusal("invalid_tls_root_certificate"))?;
            if certificates.is_empty() {
                return Err(Error::Refusal("invalid_tls_root_certificate"));
            }
            for certificate in certificates {
                roots
                    .add(certificate)
                    .map_err(|_| Error::Refusal("invalid_tls_root_certificate"))?;
            }
            config.ssl_mode(SslMode::Require);
        } else {
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        }
        // Prefer is the driver's default and permits plaintext fallback. All
        // implicit/default connections require authenticated TLS. Local use
        // remains possible only through an explicit sslmode=disable.
        if config.get_ssl_mode() == SslMode::Prefer {
            config.ssl_mode(SslMode::Require);
        }
        let tls = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let tls = MakeRustlsConnect::new(tls);
        let (client, connection) = config.connect(tls.clone()).await?;
        let cancellation = Cancellation {
            token: client.cancel_token(),
            tls,
            commit_sent: Arc::new(AtomicBool::new(false)),
            closed: Arc::new(AtomicBool::new(false)),
        };
        let closed = Arc::clone(&cancellation.closed);
        let connection = tokio::spawn(async move {
            if let Err(error) = connection.await {
                eprintln!(
                    "{}",
                    json!({"event":"postgres_connection_failed","diagnostic":postgres_report(&error)})
                );
            }
            closed.store(true, Ordering::SeqCst);
        });
        let store = Self {
            client,
            connection,
            writer_locked: false,
            cancellation,
            probe: Default::default(),
        };
        let version: i32 = store
            .client
            .query_one("SELECT current_setting('server_version_num')::int", &[])
            .await?
            .get(0);
        if !(170000..180000).contains(&version) {
            return Err(Error::Refusal("postgresql_17_required"));
        }
        Ok(store)
    }

    pub fn cancellation(&self) -> Cancellation {
        self.cancellation.clone()
    }
    /// Supply finite per-session bounds only when the owner has left them at0.
    /// Existing positive limits, including shorter values, are preserved.
    pub async fn finite_read_limits(&self) -> Result<(), Error> {
        for name in ["statement_timeout", "idle_in_transaction_session_timeout"] {
            self.client.query_one("SELECT set_config($1,CASE WHEN current_setting($1)::interval=interval '0' THEN '60s' ELSE current_setting($1) END,false)",&[&name]).await?;
        }
        Ok(())
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Every writing operation takes the database-local session lock first.
    /// It stays held until this Store closes, including between transactions.
    /// A writer that just ended may still be rolling back on the server, so a
    /// held lock is retried for a few seconds before `writer_busy`.
    pub async fn lock_writer(&mut self) -> Result<(), Error> {
        validate_owner(&self.client).await?;
        let mut attempts = 0;
        while !self.writer_locked {
            self.writer_locked = self
                .client
                .query_one(
                    "SELECT pg_try_advisory_lock($1,$2)",
                    &[&WRITER_NAMESPACE, &WRITER_LOCK],
                )
                .await?
                .get(0);
            attempts += 1;
            if !self.writer_locked {
                if attempts == 30 {
                    return Err(Error::Refusal("writer_busy"));
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        Ok(())
    }

    /// Ends a failed operation: waits for the rollback of its transaction and
    /// releases the writer lock, so that a retry does not find it held.
    pub async fn release_writer(&mut self) {
        if self.writer_locked
            && self
                .client
                .batch_execute("SELECT pg_advisory_unlock_all()")
                .await
                .is_ok()
        {
            self.writer_locked = false;
        }
    }

    pub async fn writer_transaction(&mut self) -> Result<Transaction<'_>, Error> {
        self.lock_writer().await?;
        Ok(self.client.transaction().await?)
    }

    /// Initializes the closed layout in a blank database.
    pub async fn create(&mut self, profile: &Profile) -> Result<(), Error> {
        self.lock_writer().await?;
        match self.destination(profile).await? {
            load::Destination::Blank => {
                // From here a stop cannot tell whether the layout committed.
                self.cancellation.commit_sent.store(true, Ordering::SeqCst);
                self.create_layout(profile).await
            }
            _ => Err(Error::Refusal("destination_not_empty")),
        }
    }

    pub async fn open(&self, profile: &Profile) -> Result<(), Error> {
        let exists: bool = self
            .client
            .query_one("SELECT to_regclass('dsa.identity') IS NOT NULL", &[])
            .await?
            .get(0);
        if !exists {
            return Err(Error::Refusal("unsupported_database_format"));
        }
        let identity = self.client.query_opt(
            "SELECT database_format,network,profile_hash,keep_rule_version FROM dsa.identity WHERE id=1", &[]
        ).await?.ok_or(Error::Refusal("unsupported_database_format"))?;
        if identity.get::<_, i32>(0) != DATABASE_FORMAT {
            return Err(Error::Refusal("unsupported_database_format"));
        }
        if identity.get::<_, String>(1) != profile.network {
            return Err(Error::Refusal("network_mismatch"));
        }
        if identity.get::<_, String>(2) != profile.hash()? {
            return Err(Error::Refusal("profile_hash_mismatch"));
        }
        if identity.get::<_, i32>(3) != KEEP_RULE_VERSION {
            return Err(Error::Refusal("keep_rule_version_mismatch"));
        }
        Ok(())
    }

    pub async fn grant_reader(&mut self, profile: &Profile, role: &str) -> Result<Value, Error> {
        self.open(profile).await?;
        let commit_sent = self.cancellation.commit_sent.clone();
        let transaction = self.writer_transaction().await?;
        reader_attributes(&transaction, role).await?;
        let database: String = transaction
            .query_one("SELECT current_database()", &[])
            .await?
            .get(0);
        let role_name = identifier(role);
        let relations = NODE_RELATIONS
            .iter()
            .map(|name| format!("public.{}", identifier(name)))
            .collect::<Vec<_>>()
            .join(",");
        transaction.batch_execute(&format!(
            "GRANT CONNECT ON DATABASE {} TO {role_name}; GRANT USAGE ON SCHEMA public TO {role_name}; \
             GRANT SELECT ON {relations} TO {role_name}; GRANT EXECUTE ON FUNCTION dsa.gate() TO {role_name}", identifier(&database)
        )).await?;
        let report = verify_reader(&transaction, role).await?;
        commit_sent.store(true, Ordering::SeqCst);
        transaction.commit().await?;
        Ok(report)
    }

    pub async fn check_reader(&self, profile: &Profile, role: &str) -> Result<Value, Error> {
        self.open(profile).await?;
        verify_reader(&self.client, role).await
    }

    /// Stores a new retention, at least 24 hours, under the writer lock. The
    /// horizon follows it at its next advance.
    pub async fn set_retention(&mut self, profile: &Profile, seconds: i64) -> Result<(), Error> {
        if seconds < 86_400 {
            return Err(Error::Refusal("retention_below_minimum"));
        }
        self.open(profile).await?;
        let transaction = self.writer_transaction().await?;
        transaction
            .execute(
                "UPDATE dsa.position SET retention_seconds=$1,updated_at=clock_timestamp() WHERE id=1",
                &[&seconds],
            )
            .await?;
        transaction.commit().await?;
        Ok(())
    }
}

pub(crate) fn identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

async fn validate_owner(client: &Client) -> Result<(), Error> {
    let valid: bool = client.query_one(
        "SELECT r.rolcanlogin AND NOT (r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls) \
         AND r.oid=d.datdba FROM pg_roles r CROSS JOIN pg_database d WHERE r.rolname=current_user AND d.datname=current_database()", &[]
    ).await?.get(0);
    if !valid {
        return Err(Error::Refusal("invalid_owner_role"));
    }
    Ok(())
}

async fn reader_attributes<C: GenericClient>(client: &C, role: &str) -> Result<(), Error> {
    let row = client.query_opt(
        "SELECT r.rolcanlogin AND NOT (r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls) \
         AND r.oid<>d.datdba AND r.rolname<>current_user FROM pg_roles r CROSS JOIN pg_database d \
         WHERE r.rolname=$1 AND d.datname=current_database()", &[&role]
    ).await?.ok_or(Error::Refusal("reader_role_missing"))?;
    if !row.get::<_, bool>(0) {
        return Err(Error::Refusal("invalid_reader_role"));
    }
    Ok(())
}

async fn verify_reader<C: GenericClient>(client: &C, role: &str) -> Result<Value, Error> {
    reader_attributes(client, role).await?;
    let relations: Vec<&str> = NODE_RELATIONS.to_vec();
    let violations = client
        .query(include_str!("reader_check.sql"), &[&role, &relations])
        .await?
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>();
    if !violations.is_empty() {
        return Err(Error::ReaderPrivileges(violations));
    }
    let rows = client.query(
        "SELECT s.setconfig FROM pg_db_role_setting s JOIN pg_roles r ON r.oid=s.setrole \
         WHERE r.rolname=$1 AND s.setdatabase IN (0,(SELECT oid FROM pg_database WHERE datname=current_database())) ORDER BY s.setdatabase", &[&role]
    ).await?;
    let mut settings = READER_SETTINGS
        .into_iter()
        .map(|name| (name.to_owned(), Value::Null))
        .collect::<serde_json::Map<_, _>>();
    for row in rows {
        for item in row.get::<_, Option<Vec<String>>>(0).unwrap_or_default() {
            if let Some((name, value)) = item.split_once('=')
                && READER_SETTINGS.contains(&name)
            {
                settings.insert(name.to_owned(), json!(value));
            }
        }
    }
    let limit: i32 = client
        .query_one(
            "SELECT rolconnlimit FROM pg_roles WHERE rolname=$1",
            &[&role],
        )
        .await?
        .get(0);
    settings.insert(
        "connection_limit".to_owned(),
        if limit < 0 { Value::Null } else { json!(limit) },
    );
    let mut warnings = Vec::new();
    for name in ["statement_timeout", "idle_in_transaction_session_timeout"] {
        if settings
            .get(name)
            .and_then(Value::as_str)
            .is_none_or(|value| matches!(value, "0" | "0ms"))
        {
            warnings.push(if name == "idle_in_transaction_session_timeout" {
                "reader_idle_in_transaction_timeout_unset".to_owned()
            } else {
                format!("reader_{name}_unset")
            });
        }
    }
    if settings
        .get("search_path")
        .and_then(Value::as_str)
        .is_none_or(|value| value.replace([' ', '"'], "") != "public")
    {
        warnings.push("reader_search_path_not_public".to_owned());
    }
    if limit < 0 {
        warnings.push("reader_connection_limit_unset".to_owned());
    } else if limit < 36 {
        warnings.push("reader_connection_limit_below_node_pool".to_owned());
    }
    Ok(
        json!({"role":role,"privileges":"node_relations_only","settings":settings,"warnings":warnings}),
    )
}

#[cfg(test)]
mod diagnostic_tests {
    use super::*;
    #[derive(Debug)]
    struct SecretContext(std::io::Error);
    impl std::fmt::Display for SecretContext {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("generated-secret context private-host private-db")
        }
    }
    impl std::error::Error for SecretContext {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }
    #[test]
    fn connection_failures_name_tls_host_name_ca_and_timeout() {
        use rustls::CertificateError;
        let tls = |error: rustls::Error| {
            cause_in_chain(&std::io::Error::new(std::io::ErrorKind::InvalidData, error))
        };
        assert_eq!(
            tls(rustls::Error::InvalidCertificate(
                CertificateError::NotValidForName
            )),
            Some("tls_host_name_mismatch")
        );
        assert_eq!(
            tls(rustls::Error::InvalidCertificate(
                CertificateError::UnknownIssuer
            )),
            Some("tls_unknown_ca")
        );
        assert_eq!(
            tls(rustls::Error::InvalidCertificate(CertificateError::Expired)),
            Some("tls_certificate_invalid")
        );
        assert_eq!(tls(rustls::Error::DecryptError), Some("tls_error"));
        let timeout = SecretContext(std::io::Error::from(std::io::ErrorKind::TimedOut));
        assert_eq!(cause_in_chain(&timeout), Some("connection_timeout"));
        assert_eq!(
            cause_in_chain(&std::io::Error::from(std::io::ErrorKind::ConnectionRefused)),
            Some("connection_refused")
        );
        assert_eq!(cause_in_chain(&std::io::Error::other("other")), None);
    }

    #[tokio::test]
    async fn a_refused_connection_is_named_without_its_settings() {
        let config: Config = "host=127.0.0.1 port=1 user=generated-user password=generated-secret dbname=x sslmode=disable connect_timeout=2"
            .parse()
            .unwrap();
        let report = match Store::connect(config, None).await {
            Err(error) => error.report(),
            Ok(_) => panic!("connected to port 1"),
        };
        assert_eq!(report["error"], "database_error");
        assert_eq!(report["cause"], "connection_refused");
        let text = report.to_string();
        assert!(
            !text.contains("generated-secret") && !text.contains("generated-user"),
            "{text}"
        );
    }

    #[test]
    fn io_diagnostics_keep_kind_errno_without_context_or_message() {
        let error = SecretContext(std::io::Error::from_raw_os_error(110));
        let value = serde_json::to_value(io_causes(Some(&error))).unwrap();
        assert_eq!(value[0]["os_error"], 110);
        #[cfg(target_os = "linux")]
        assert_eq!(value[0]["kind"], "TimedOut");
        let serialized = value.to_string();
        for secret in [
            "generated-secret",
            "private-host",
            "private-db",
            "postgres://",
        ] {
            assert!(!serialized.contains(secret));
        }
        let other = std::io::Error::other("another-secret context private-host");
        assert_eq!(
            io_causes(Some(&other)),
            vec![json!({"kind":"Other","os_error":null})]
        );
        assert_eq!(
            Error::Io(std::io::Error::from_raw_os_error(110)).report()["os_error"],
            110
        );
        let staged = Error::InFamily {
            family: "block".to_owned(),
            error: Box::new(Error::Io(other)),
        }
        .report();
        assert_eq!(staged["family"], "block");
        assert!(!staged.to_string().contains("another-secret"));
    }
}
