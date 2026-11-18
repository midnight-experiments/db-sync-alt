//! Run against an isolated PostgreSQL 17 instance with trust authentication.

use std::{
    error::Error as StdError,
    sync::atomic::{AtomicU32, Ordering},
};

use dsa_chain::profiles::Profile;
use dsa_store::{NODE_RELATIONS, Store};
use serde_json::{Value, json};
use tokio::process::Command;
use tokio_postgres::{
    Client, Config, NoTls,
    config::{Host, SslMode},
};

type Result<T = ()> = std::result::Result<T, Box<dyn StdError>>;
static NEXT_ID: AtomicU32 = AtomicU32::new(0);

struct Database {
    admin: Client,
    config: Config,
    owner: String,
    reader: String,
    database: String,
}

async fn connect(config: &Config) -> Result<Client> {
    let (client, connection) = config.connect(NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

impl Database {
    async fn new() -> Result<Self> {
        let root: Config = std::env::var("DSA_TEST_DATABASE")?.parse()?;
        let admin = connect(&root).await?;
        let suffix = format!(
            "{}_{}",
            std::process::id(),
            NEXT_ID.fetch_add(1, Ordering::SeqCst)
        );
        let owner = format!("owner_{suffix}");
        let reader = format!("reader_{suffix}");
        let database = format!("database_{suffix}");
        admin.batch_execute(&format!(
            "CREATE ROLE {owner} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS; \
             CREATE ROLE {reader} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 40; \
             ALTER ROLE {reader} SET search_path=public; ALTER ROLE {reader} SET statement_timeout='60s'; \
             ALTER ROLE {reader} SET idle_in_transaction_session_timeout='60s'; ALTER ROLE {reader} SET default_transaction_read_only=on"
        )).await?;
        admin
            .batch_execute(&format!("CREATE DATABASE {database} OWNER {owner}"))
            .await?;
        let mut config = root.clone();
        config
            .user(&owner)
            .dbname(&database)
            .ssl_mode(SslMode::Disable);
        Ok(Self {
            admin,
            config,
            owner,
            reader,
            database,
        })
    }

    async fn store(&self) -> Result<Store> {
        Ok(Store::connect(self.config.clone(), None).await?)
    }

    fn dsn(&self) -> String {
        let host = match &self.config.get_hosts()[0] {
            Host::Tcp(host) => host.clone(),
            Host::Unix(path) => path.to_string_lossy().into_owned(),
        };
        let host = host.replace('\\', "\\\\").replace('\'', "\\'");
        let port = self.config.get_ports().first().copied().unwrap_or(5432);
        format!(
            "host='{host}' port={port} dbname={} user={} sslmode=disable",
            self.database, self.owner
        )
    }

    async fn cli(
        &self,
        command: &str,
        network: &str,
        extra: &[&str],
    ) -> Result<std::process::Output> {
        Ok(Command::new(env!("CARGO_BIN_EXE_db-sync-alt"))
            .args([command, "--database", &self.dsn(), "--network", network])
            .args(extra)
            .output()
            .await?)
    }

    async fn create(&self) -> Result {
        let output = self.cli("create", "preprod", &[]).await?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let result: Value = serde_json::from_slice(&output.stdout)?;
        assert_eq!(result["identity"]["database_format"], 1);
        assert_eq!(result["identity"]["network"], "preprod");
        assert_eq!(result["position"]["serving"], false);
        assert_eq!(result["position"]["retention_seconds"], 604800);
        Ok(())
    }

    async fn grant(&self) -> Result {
        let output = self
            .cli("grant-reader", "preprod", &["--role", &self.reader])
            .await?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let result: Value = serde_json::from_slice(&output.stdout)?;
        assert_eq!(result["privileges"], "node_relations_only");
        assert_eq!(result["warnings"], json!([]));
        Ok(())
    }

    async fn reader(&self) -> Result<Client> {
        let mut config = self.config.clone();
        config.user(&self.reader);
        connect(&config).await
    }

    async fn cleanup(self) -> Result {
        self.admin
            .batch_execute(&format!("DROP DATABASE {} WITH (FORCE)", self.database))
            .await?;
        self.admin
            .batch_execute(&format!("DROP ROLE {},{}", self.owner, self.reader))
            .await?;
        Ok(())
    }
}

fn refusal(output: &std::process::Output) -> Value {
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    serde_json::from_str(stderr.lines().last().unwrap()).unwrap()
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL 17"]
async fn catalogue_matches_legacy_and_node_reader_probes() -> Result {
    let db = Database::new().await?;
    db.create().await?;
    db.grant().await?;
    let store = db.store().await?;
    let version: bool=store.client().query_one(
        "SELECT NOT r.rolsuper AND NOT r.rolcreatedb AND NOT r.rolcreaterole AND r.rolname=$1 FROM pg_roles r WHERE rolname=current_user",&[&db.owner]
    ).await?.get(0);
    assert!(version);
    let legacy_database = format!("{}_legacy", db.database);
    db.admin
        .batch_execute(&format!(
            "CREATE DATABASE {legacy_database} OWNER {}",
            db.owner
        ))
        .await?;
    let mut legacy_config = db.config.clone();
    legacy_config.dbname(&legacy_database);
    let legacy = connect(&legacy_config).await?;
    for ddl in [
        include_str!("../../../src/db_sync_alt/store/schema.sql"),
        include_str!("../../../src/db_sync_alt/store/schema_v2.sql"),
        include_str!("../../../src/db_sync_alt/store/schema_v3.sql"),
        include_str!("../../../src/db_sync_alt/store/schema_v3_after_load.sql"),
        include_str!("../../../src/db_sync_alt/store/schema_v4.sql"),
        include_str!("../../../src/db_sync_alt/store/schema_v5.sql"),
    ] {
        legacy.batch_execute(ddl).await?;
    }
    for query in [
        "SELECT jsonb_agg(to_jsonb(c) ORDER BY table_schema,table_name,ordinal_position) FROM \
         (SELECT table_schema,table_name,column_name,ordinal_position,data_type,domain_schema,domain_name,udt_schema,udt_name,is_nullable,column_default \
          FROM information_schema.columns WHERE table_schema IN ('serve','public')) c",
        "SELECT jsonb_agg(to_jsonb(i) ORDER BY indexname) FROM (SELECT indexname,indexdef FROM pg_indexes WHERE schemaname='serve') i",
    ] {
        let expected: Value = legacy.query_one(query, &[]).await?.get(0);
        let actual: Value = store.client().query_one(query, &[]).await?.get(0);
        assert_eq!(actual, expected);
    }
    let constraints: i64=store.client().query_one(
        "SELECT count(*) FROM pg_constraint c JOIN pg_namespace n ON n.oid=c.connamespace WHERE n.nspname='serve' AND c.contype='c'",&[]
    ).await?.get(0);
    assert_eq!(constraints, 7);
    let reader = db.reader().await?;
    let count: i64 = reader
        .query_one(
            "SELECT count(*) FROM information_schema.tables WHERE table_name='tx_in'",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(count, 1);
    for name in [
        "idx_block_block_no",
        "idx_tx_block_id",
        "idx_tx_out_tx_id",
        "idx_tx_out_data_hash",
        "idx_tx_out_address",
        "idx_tx_in_tx_in_id",
        "idx_tx_in_tx_out_id_tx_out_index",
        "idx_multi_asset_policy_name",
        "idx_multi_asset_policy_name_hex",
        "idx_ma_tx_out_ident",
        "idx_ma_tx_out_id_ident",
    ] {
        let count: i64 = reader
            .query_one(
                "SELECT count(*) FROM pg_indexes WHERE indexname=$1",
                &[&name],
            )
            .await?
            .get(0);
        assert_eq!(count, 1, "{name}");
    }
    let private: bool=reader.query_one("SELECT has_schema_privilege(current_user,'serve','USAGE') OR has_schema_privilege(current_user,'dsa','USAGE')",&[]).await?.get(0);
    assert!(!private);
    let public_connect: bool=store.client().query_one("SELECT EXISTS(SELECT 1 FROM pg_database d,LATERAL aclexplode(d.datacl) a WHERE d.datname=current_database() AND a.grantee=0)",&[]).await?.get(0);
    assert!(!public_connect);
    drop(reader);
    drop(legacy);
    drop(store);
    db.admin
        .batch_execute(&format!("DROP DATABASE {legacy_database} WITH (FORCE)"))
        .await?;
    db.cleanup().await
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL 17"]
async fn every_view_gates_cached_prepared_queries() -> Result {
    let db = Database::new().await?;
    db.create().await?;
    db.grant().await?;
    let store = db.store().await?;
    let reader = db.reader().await?;
    let mut statements = Vec::new();
    for table in NODE_RELATIONS {
        let statement = reader
            .prepare(&format!("SELECT * FROM public.{table}"))
            .await?;
        for _ in 0..10 {
            let error = reader.query(&statement, &[]).await.unwrap_err();
            assert_eq!(
                error.code().map(|code| code.code()),
                Some("55000"),
                "{table}"
            );
        }
        statements.push(statement);
    }
    store.client().batch_execute(
        "UPDATE dsa.position SET serving=true,reason='ready',target_height=1,target_hash=decode(repeat('01',32),'hex'),target_time=now(), \
         tip_height=1,tip_hash=decode(repeat('01',32),'hex'),tip_time=now(),horizon_height=1,horizon_hash=decode(repeat('01',32),'hex'),horizon_time=now() WHERE id=1"
    ).await?;
    for statement in &statements {
        for _ in 0..10 {
            assert!(reader.query(statement, &[]).await?.is_empty());
        }
    }
    store
        .client()
        .batch_execute("UPDATE dsa.position SET serving=false,reason='test' WHERE id=1")
        .await?;
    for statement in &statements {
        let error = reader.query(statement, &[]).await.unwrap_err();
        assert_eq!(error.code().map(|code| code.code()), Some("55000"));
    }
    drop(reader);
    drop(store);
    db.cleanup().await
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL 17"]
async fn reader_refuses_extra_effective_privileges() -> Result {
    let db = Database::new().await?;
    db.create().await?;
    db.grant().await?;
    let profile = Profile::load("preprod")?;
    let store = db.store().await?;
    for (grant, revoke) in [
        (
            format!(
                "GRANT TEMPORARY ON DATABASE {} TO {}",
                db.database, db.reader
            ),
            format!(
                "REVOKE TEMPORARY ON DATABASE {} FROM {}",
                db.database, db.reader
            ),
        ),
        (
            format!("GRANT USAGE ON SCHEMA serve TO {}", db.reader),
            format!("REVOKE USAGE ON SCHEMA serve FROM {}", db.reader),
        ),
        (
            format!("GRANT SELECT(serving) ON dsa.position TO {}", db.reader),
            format!("REVOKE SELECT(serving) ON dsa.position FROM {}", db.reader),
        ),
        (
            format!("GRANT DELETE ON public.block TO {}", db.reader),
            format!("REVOKE DELETE ON public.block FROM {}", db.reader),
        ),
        (
            format!(
                "GRANT SELECT ON public.block TO {} WITH GRANT OPTION",
                db.reader
            ),
            format!(
                "REVOKE GRANT OPTION FOR SELECT ON public.block FROM {}",
                db.reader
            ),
        ),
    ] {
        store.client().batch_execute(&grant).await?;
        assert!(matches!(
            store.check_reader(&profile, &db.reader).await,
            Err(dsa_store::Error::ReaderPrivileges(_))
        ));
        let output = db
            .cli(
                "grant-reader",
                "preprod",
                &["--role", &db.reader, "--check"],
            )
            .await?;
        assert_eq!(refusal(&output)["error"], "reader_privileges_not_exact");
        store.client().batch_execute(&revoke).await?;
        store.check_reader(&profile, &db.reader).await?;
    }
    db.admin
        .batch_execute(&format!("GRANT {} TO {}", db.owner, db.reader))
        .await?;
    assert!(store.check_reader(&profile, &db.reader).await.is_err());
    db.admin
        .batch_execute(&format!("REVOKE {} FROM {}", db.owner, db.reader))
        .await?;
    // A failed grant must roll back all grants attempted in that transaction.
    store
        .client()
        .batch_execute(&format!(
            "REVOKE SELECT ON public.block FROM {}; GRANT TEMPORARY ON DATABASE {} TO {}",
            db.reader, db.database, db.reader
        ))
        .await?;
    let failed = db
        .cli("grant-reader", "preprod", &["--role", &db.reader])
        .await?;
    assert_eq!(refusal(&failed)["error"], "reader_privileges_not_exact");
    let readable: bool = store
        .client()
        .query_one(
            "SELECT has_table_privilege($1,'public.block','SELECT')",
            &[&db.reader],
        )
        .await?
        .get(0);
    assert!(!readable);
    store
        .client()
        .batch_execute(&format!(
            "REVOKE TEMPORARY ON DATABASE {} FROM {}",
            db.database, db.reader
        ))
        .await?;
    db.grant().await?;
    let invalid = db
        .cli("grant-reader", "preprod", &["--role", &db.owner])
        .await?;
    assert_eq!(refusal(&invalid)["error"], "invalid_reader_role");
    drop(store);
    db.cleanup().await
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL 17"]
async fn identity_refusals_status_and_single_writer() -> Result {
    let db = Database::new().await?;
    let preprod = Profile::load("preprod")?;
    let mainnet = Profile::load("mainnet")?;
    let admin_config: Config = std::env::var("DSA_TEST_DATABASE")?.parse()?;
    let mut admin_store = Store::connect(admin_config, None).await?;
    assert!(matches!(
        admin_store.create(&preprod).await,
        Err(dsa_store::Error::Refusal("invalid_owner_role"))
    ));
    drop(admin_store);
    let mut first = db.store().await?;
    first.create(&preprod).await?;
    let mut second = db.store().await?;
    assert!(matches!(
        second.writer_transaction().await,
        Err(dsa_store::Error::Refusal("writer_busy"))
    ));
    assert!(matches!(
        second.open(&mainnet).await,
        Err(dsa_store::Error::Refusal("network_mismatch"))
    ));
    for (column, value, code) in [
        ("database_format", "99", "unsupported_database_format"),
        ("profile_hash", "repeat('0',64)", "profile_hash_mismatch"),
        ("keep_rule_version", "99", "keep_rule_version_mismatch"),
    ] {
        // The owner session still holds its writer lock. Restore each identity
        // field after the named open refusal so the following case is independent.
        let original: Value = first.status(&preprod).await?;
        first
            .client()
            .batch_execute(&format!(
                "UPDATE dsa.identity SET {column}={value} WHERE id=1"
            ))
            .await?;
        let error = second.open(&preprod).await.unwrap_err();
        assert_eq!(error.to_string(), code);
        let original = &original["identity"][column];
        let restore = if let Some(text) = original.as_str() {
            format!("'{text}'")
        } else {
            original.to_string()
        };
        first
            .client()
            .batch_execute(&format!(
                "UPDATE dsa.identity SET {column}={restore} WHERE id=1"
            ))
            .await?;
    }
    let output = db.cli("status", "mainnet", &[]).await?;
    assert_eq!(refusal(&output)["error"], "network_mismatch");
    let output = db.cli("status", "preprod", &[]).await?;
    assert!(output.status.success());
    let status: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(status["position"]["serving"], false);
    drop(first);
    tokio::task::yield_now().await;
    second.writer_transaction().await?.commit().await?;
    assert!(matches!(
        second.create(&preprod).await,
        Err(dsa_store::Error::Refusal("destination_not_empty"))
    ));
    drop(second);
    db.cleanup().await
}
