//! Generated true schema5 sources with the unchanged legacy gate and reader.
#[path = "../keep_rule/fixtures.rs"]
#[allow(dead_code)]
pub(crate) mod fixtures;

use dsa_chain::{keep::TABLES, profiles::Profile};
use fixtures::{Fixture, Rows, model::Header};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    error::Error as StdError,
    path::Path,
    process::{Output, Stdio},
    time::{Duration, Instant},
};
use tokio::process::{Child, Command};
use tokio_postgres::{
    Client, Config, NoTls,
    config::{Host, SslMode},
};
type Result<T = ()> = std::result::Result<T, Box<dyn StdError + Send + Sync>>;

pub(crate) async fn connect(config: &Config) -> Result<Client> {
    let (client, connection) = config.connect(NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

pub(crate) struct Database {
    pub(crate) admin: Client,
    pub(crate) config: Config,
    pub(crate) owner: String,
    pub(crate) reader: String,
    pub(crate) name: String,
}
impl Database {
    pub(crate) async fn new() -> Result<Self> {
        let root: Config = std::env::var("DSA_TEST_DATABASE")?.parse()?;
        let admin = connect(&root).await?;
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let owner = format!("conversion_owner_{}_{nonce:x}", std::process::id());
        let reader = format!("conversion_reader_{}_{nonce:x}", std::process::id());
        let name = format!("conversion_{}_{nonce:x}", std::process::id());
        admin.batch_execute(&format!("CREATE ROLE {owner} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;CREATE ROLE {reader} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 36;ALTER ROLE {reader} SET default_transaction_read_only=on;ALTER ROLE {reader} SET search_path=public;ALTER ROLE {reader} SET statement_timeout='60s';ALTER ROLE {reader} SET idle_in_transaction_session_timeout='60s'")).await?;
        admin
            .batch_execute(&format!("CREATE DATABASE {name} OWNER {owner};"))
            .await?;
        let mut config = root;
        config.user(&owner).dbname(&name).ssl_mode(SslMode::Disable);
        Ok(Self {
            admin,
            config,
            owner,
            reader,
            name,
        })
    }
    pub(crate) fn dsn(&self, reader: bool) -> String {
        let host = match &self.config.get_hosts()[0] {
            Host::Tcp(s) => s.clone(),
            Host::Unix(p) => p.display().to_string(),
        };
        format!(
            "host='{}' port={} user={} dbname={} sslmode=disable",
            host.replace('\\', "\\\\").replace('\'', "\\'"),
            self.config.get_ports().first().copied().unwrap_or(5432),
            if reader { &self.reader } else { &self.owner },
            self.name
        )
    }
    pub(crate) async fn cli(&self, command: &str, network: &str, args: &[&str]) -> Result<Value> {
        let output = self
            .start_cli(command, network, args)?
            .wait_with_output()
            .await?;
        assert!(
            output.status.success(),
            "{command}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(serde_json::from_slice(&output.stdout)?)
    }
    pub(crate) fn start_cli(&self, command: &str, network: &str, args: &[&str]) -> Result<Child> {
        Ok(Command::new(env!("CARGO_BIN_EXE_db-sync-alt"))
            .args([
                command,
                "--database",
                &self.dsn(false),
                "--network",
                network,
            ])
            .args(args)
            .kill_on_drop(true)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?)
    }
    pub(crate) async fn refusal(
        &self,
        network: &str,
        source: &str,
        args: &[&str],
        expected: &str,
    ) -> Result<Value> {
        let mut all = vec!["--source", source];
        all.extend(args);
        let output = self
            .start_cli("convert", network, &all)?
            .wait_with_output()
            .await?;
        let report = failure(&output)?;
        assert_eq!(report["error"], expected, "{report}");
        Ok(report)
    }
    pub(crate) fn reader_config(&self) -> Config {
        let mut c = self.config.clone();
        c.user(&self.reader);
        c
    }
    pub(crate) async fn write(&self, sql: &str) -> Result {
        connect(&self.config).await?.batch_execute(&format!("BEGIN;SELECT pg_advisory_xact_lock(36,1);SET LOCAL db_sync_alt.projection_write=on;{sql};COMMIT")).await?;
        Ok(())
    }
    pub(crate) async fn catalog(&self) -> Result<Value> {
        let c = connect(&self.config).await?;
        let row = c.query_one("SELECT jsonb_build_object('acl',(SELECT datacl::text FROM pg_database WHERE datname=current_database()),'namespaces',(SELECT jsonb_agg(jsonb_build_array(nspname,nspacl::text) ORDER BY nspname) FROM pg_namespace WHERE nspname NOT LIKE 'pg_%'),'relations',(SELECT jsonb_agg(jsonb_build_array(n.nspname,c.relname,c.relkind,c.relacl::text) ORDER BY n.nspname,c.relname) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('dsa','serve','public')),'functions',(SELECT jsonb_agg(jsonb_build_array(n.nspname,p.proname,p.proacl::text) ORDER BY n.nspname,p.proname) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname IN ('dsa','serve','public')))", &[]).await?;
        let mut v: Value = row.get(0);
        if c.query_one("SELECT to_regclass('dsa.identity') IS NOT NULL", &[])
            .await?
            .get::<_, bool>(0)
        {
            v["identity"] = c
                .query_one("SELECT to_jsonb(i) FROM dsa.identity i", &[])
                .await?
                .get(0);
            v["position"] = c
                .query_one("SELECT to_jsonb(p) FROM dsa.position p", &[])
                .await?
                .get(0);
        }
        Ok(v)
    }
    pub(crate) async fn closed_empty(&self) -> Result {
        let c = connect(&self.config).await?;
        let row = c
            .query_one("SELECT serving,reason FROM dsa.position", &[])
            .await?;
        assert!(!row.get::<_, bool>(0));
        assert_eq!(row.get::<_, String>(1), "convert_failed");
        for table in TABLES {
            assert_eq!(
                c.query_one(&format!("SELECT count(*) FROM serve.{table}"), &[])
                    .await?
                    .get::<_, i64>(0),
                0,
                "{table}"
            );
        }
        Ok(())
    }
    pub(crate) async fn barrier(&self) -> Result<Client> {
        let c = connect(&self.config).await?;
        let original: String = c
            .query_one(
                "SELECT pg_get_functiondef('chain.gate()'::regprocedure)",
                &[],
            )
            .await?
            .get(0);
        let altered = original.replace(" RETURN true;", " IF position('to_jsonb' in current_query())>0 THEN PERFORM pg_advisory_xact_lock(726,1); END IF; RETURN true;");
        assert_ne!(original, altered);
        c.batch_execute(&altered).await?;
        c.query_one("SELECT pg_advisory_lock(726,1)", &[]).await?;
        Ok(c)
    }
    pub(crate) async fn wait_barrier(&self) -> Result {
        let start = Instant::now();
        loop {
            let blocked: bool = self.admin.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=$1 AND application_name='db-sync-alt-convert-source' AND wait_event='advisory')", &[&self.name]).await?.get(0);
            if blocked {
                return Ok(());
            }
            if start.elapsed() > Duration::from_secs(10) {
                return Err("source did not reach export barrier".into());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    pub(crate) async fn legacy(&self, f: &Fixture) -> Result {
        let client = connect(&self.config).await?;
        for ddl in [
            include_str!("../../../../src/db_sync_alt/store/schema.sql"),
            include_str!("../../../../src/db_sync_alt/store/schema_v2.sql"),
            include_str!("../../../../src/db_sync_alt/store/schema_v3.sql"),
            include_str!("../../../../src/db_sync_alt/store/schema_v3_after_load.sql"),
            include_str!("../../../../src/db_sync_alt/store/schema_v4.sql"),
            include_str!("../../../../src/db_sync_alt/store/schema_v5.sql"),
        ] {
            client.batch_execute(ddl).await?;
        }
        client.batch_execute(&format!("INSERT INTO chain.generations VALUES(1,NULL,'ready','generated',0,0,{},{},'{{}}');INSERT INTO chain.service_state VALUES(1,'db-sync-alt',5,1,false,0,'importing');INSERT INTO chain.integrity_state(id,base_generation,revision,projection_generation,serve_generation) VALUES(1,1,0,1,1)",f.bounds.target,f.bounds.tip)).await?;
        let rows = f.rows();
        for table in TABLES {
            client.execute(&format!("INSERT INTO serve.{table} SELECT * FROM jsonb_populate_recordset(NULL::serve.{table},$1)"),&[&json!(rows[table])]).await?;
        }
        client.batch_execute(&format!("UPDATE chain.service_state SET migration_state='ready';REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA chain FROM PUBLIC;REVOKE ALL ON DATABASE {} FROM PUBLIC;GRANT CONNECT ON DATABASE {} TO {};GRANT USAGE ON SCHEMA public TO {};GRANT SELECT ON {} TO {};GRANT EXECUTE ON FUNCTION chain.gate() TO {}",self.name,self.name,self.reader,self.reader,TABLES.iter().map(|t|format!("public.{t}")).collect::<Vec<_>>().join(","),self.reader,self.reader)).await?;
        Ok(())
    }
    pub(crate) async fn rows(&self) -> Result<Rows> {
        let client = connect(&self.config).await?;
        let mut rows = BTreeMap::new();
        for table in TABLES {
            let key = if table == "epoch_param" {
                "epoch_no"
            } else {
                "id"
            };
            let value:Value=client.query_one(&format!("SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY {key}),'[]'::jsonb) FROM serve.{table} t"),&[]).await?.get(0);
            rows.insert(table.to_owned(), serde_json::from_value(value)?);
        }
        Ok(rows)
    }
    pub(crate) async fn cleanup(self) -> Result {
        self.admin
            .batch_execute(&format!("DROP DATABASE {} WITH (FORCE)", self.name))
            .await?;
        self.admin
            .batch_execute(&format!(
                "DROP ROLE {};DROP ROLE {}",
                self.owner, self.reader
            ))
            .await?;
        Ok(())
    }
}

pub(crate) fn fixture(seed: u64) -> Fixture {
    let mut f = Fixture::base(seed);
    f.profile = Profile::load(&f.profile.network).unwrap();
    f.bounds.horizon = 6;
    f.bounds.tip = f.bounds.target + f.profile.cardano.security_parameter;
    for h in 0..=f.bounds.tip {
        f.chain.blocks.entry(i64::from(h)).or_insert(Header {
            epoch: Some((h / 2) as i32),
            slot: Some(i64::from(h) * 100 + 1),
        });
    }
    let out = f.output(8, 0, 11, "ics", "ics", Some(999));
    f.add_asset(out, 9001, 19);
    f
}

pub(crate) fn file_args(path: &Path) -> [&str; 2] {
    ["--file", path.to_str().unwrap()]
}

pub(crate) fn failure(output: &Output) -> Result<Value> {
    assert!(!output.status.success(), "unexpected successful conversion");
    let text = String::from_utf8_lossy(&output.stderr);
    Ok(text
        .lines()
        .filter_map(|s| serde_json::from_str::<Value>(s).ok())
        .find(|v| v.get("error").is_some())
        .ok_or_else(|| format!("no structured failure: {text}"))?)
}
