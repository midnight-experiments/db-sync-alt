//! PostgreSQL layout, database identity and least-privilege node access.

use std::{io::BufReader, path::Path};

use dsa_chain::{VERSION, profiles::Profile};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_postgres::{Client, Config, GenericClient, Transaction, config::SslMode};
use tokio_postgres_rustls::MakeRustlsConnect;

pub const DATABASE_FORMAT: i32 = 1;
pub const KEEP_RULE_VERSION: i32 = 1;
pub const NODE_RELATIONS: [&str; 11] = [
    "block",
    "tx",
    "tx_out",
    "tx_in",
    "ma_tx_out",
    "multi_asset",
    "datum",
    "epoch_param",
    "pool_hash",
    "epoch_stake",
    "tx_metadata",
];
const WRITER_NAMESPACE: i32 = 0x647361;
const WRITER_LOCK: i32 = 1;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Refusal(&'static str),
    #[error("reader_privileges_not_exact")]
    ReaderPrivileges(Vec<String>),
    #[error("database_error")]
    Postgres(#[from] tokio_postgres::Error),
    #[error("profile_error")]
    Profile(#[from] serde_json::Error),
}

impl Error {
    /// Public diagnostics contain a named cause and never connection credentials.
    pub fn report(&self) -> Value {
        match self {
            Self::ReaderPrivileges(details) => {
                json!({"error": self.to_string(), "details": details})
            }
            Self::Postgres(error) => {
                json!({"error": "database_error", "sqlstate": error.code().map(|code| code.code())})
            }
            _ => json!({"error": self.to_string()}),
        }
    }
}

pub struct Store {
    client: Client,
    connection: JoinHandle<()>,
    writer_locked: bool,
}

impl Drop for Store {
    fn drop(&mut self) {
        // Closing the socket also releases this session's advisory lock.
        self.connection.abort();
    }
}

impl Store {
    /// A custom CA requires TLS and verifies the server hostname. Explicit
    /// sslmode=disable supports operator-provided local connections.
    pub async fn connect(
        mut config: Config,
        root_certificate: Option<&Path>,
    ) -> Result<Self, Error> {
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
        let tls = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let (client, connection) = config.connect(MakeRustlsConnect::new(tls)).await?;
        let connection = tokio::spawn(async move {
            let _ = connection.await;
        });
        let store = Self {
            client,
            connection,
            writer_locked: false,
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

    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Every writing operation takes the database-local session lock first.
    /// It stays held until this Store closes, including between transactions.
    pub async fn writer_transaction(&mut self) -> Result<Transaction<'_>, Error> {
        validate_owner(&self.client).await?;
        if !self.writer_locked {
            let locked: bool = self
                .client
                .query_one(
                    "SELECT pg_try_advisory_lock($1,$2)",
                    &[&WRITER_NAMESPACE, &WRITER_LOCK],
                )
                .await?
                .get(0);
            if !locked {
                return Err(Error::Refusal("writer_busy"));
            }
            self.writer_locked = true;
        }
        Ok(self.client.transaction().await?)
    }

    pub async fn create(&mut self, profile: &Profile) -> Result<(), Error> {
        let profile_hash = profile.hash()?;
        let transaction = self.writer_transaction().await?;
        let occupied: bool = transaction.query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname NOT LIKE 'pg\\_%' AND nspname NOT IN ('public','information_schema')) \
             OR EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public') \
             OR EXISTS(SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public') \
             OR EXISTS(SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace WHERE n.nspname='public') \
             OR EXISTS(SELECT 1 FROM pg_extension WHERE extname<>'plpgsql')", &[]
        ).await?.get(0);
        if occupied {
            return Err(Error::Refusal("destination_not_empty"));
        }
        let database: String = transaction
            .query_one("SELECT current_database()", &[])
            .await?
            .get(0);
        transaction
            .batch_execute(&format!(
                "REVOKE ALL ON DATABASE {} FROM PUBLIC",
                identifier(&database)
            ))
            .await?;
        transaction
            .batch_execute(include_str!("layout.sql"))
            .await?;
        transaction.batch_execute(include_str!("state.sql")).await?;
        transaction
            .batch_execute(include_str!("indexes.sql"))
            .await?;
        transaction.batch_execute(include_str!("views.sql")).await?;
        transaction.execute(
            "INSERT INTO dsa.identity(id,database_format,network,profile_hash,keep_rule_version,tool_version,node_release,origin) \
             VALUES(1,$1,$2,$3,$4,$5,$6,$7)",
            &[&DATABASE_FORMAT, &profile.network, &profile_hash, &KEEP_RULE_VERSION, &VERSION, &profile.node_release, &json!({"kind":"empty"})]
        ).await?;
        transaction
            .execute("INSERT INTO dsa.position(id) VALUES(1)", &[])
            .await?;
        transaction.commit().await?;
        Ok(())
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

    pub async fn status(&self, profile: &Profile) -> Result<Value, Error> {
        self.open(profile).await?;
        Ok(self.client.query_one(
            "SELECT jsonb_build_object('identity',(SELECT to_jsonb(i)-'id' FROM dsa.identity i WHERE id=1), \
             'position',(SELECT to_jsonb(p)-'id' FROM dsa.position p WHERE id=1))", &[]
        ).await?.get(0))
    }

    pub async fn grant_reader(&mut self, profile: &Profile, role: &str) -> Result<Value, Error> {
        self.open(profile).await?;
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
        transaction.commit().await?;
        Ok(report)
    }

    pub async fn check_reader(&self, profile: &Profile, role: &str) -> Result<Value, Error> {
        self.open(profile).await?;
        verify_reader(&self.client, role).await
    }
}

fn identifier(name: &str) -> String {
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
    let mut settings = serde_json::Map::new();
    for row in rows {
        for item in row.get::<_, Option<Vec<String>>>(0).unwrap_or_default() {
            if let Some((name, value)) = item.split_once('=') {
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
    if settings.get("search_path").and_then(Value::as_str) != Some("public") {
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
