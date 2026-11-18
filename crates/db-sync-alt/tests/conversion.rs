//! Generated true schema5 sources with the unchanged legacy gate and reader.
#[path = "keep_rule/fixtures.rs"]
#[allow(dead_code)]
mod fixtures;

use dsa_chain::{
    keep::{Header, TABLES, keep},
    profiles::Profile,
};
use fixtures::{Fixture, Rows, selected};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error as StdError,
    path::Path,
};
use tokio::process::Command;
use tokio_postgres::{
    Client, Config, NoTls,
    config::{Host, SslMode},
};
type Result<T = ()> = std::result::Result<T, Box<dyn StdError + Send + Sync>>;

async fn connect(config: &Config) -> Result<Client> {
    let (client, connection) = config.connect(NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

struct Database {
    admin: Client,
    config: Config,
    owner: String,
    reader: String,
    name: String,
}
impl Database {
    async fn new() -> Result<Self> {
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
    fn dsn(&self, reader: bool) -> String {
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
    async fn cli(&self, command: &str, network: &str, args: &[&str]) -> Result<Value> {
        let output = Command::new(env!("CARGO_BIN_EXE_db-sync-alt"))
            .args([
                command,
                "--database",
                &self.dsn(false),
                "--network",
                network,
            ])
            .args(args)
            .output()
            .await?;
        assert!(
            output.status.success(),
            "{command}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(serde_json::from_slice(&output.stdout)?)
    }
    async fn legacy(&self, f: &Fixture) -> Result {
        let client = connect(&self.config).await?;
        for ddl in [
            include_str!("../../../src/db_sync_alt/store/schema.sql"),
            include_str!("../../../src/db_sync_alt/store/schema_v2.sql"),
            include_str!("../../../src/db_sync_alt/store/schema_v3.sql"),
            include_str!("../../../src/db_sync_alt/store/schema_v3_after_load.sql"),
            include_str!("../../../src/db_sync_alt/store/schema_v4.sql"),
            include_str!("../../../src/db_sync_alt/store/schema_v5.sql"),
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
    async fn rows(&self) -> Result<Rows> {
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
    async fn cleanup(self) -> Result {
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

fn fixture(seed: u64) -> Fixture {
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

fn file_args(path: &Path) -> [&str; 2] {
    ["--file", path.to_str().unwrap()]
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn convert_true_legacy_public_reader_model_and_snapshot() -> Result {
    let f = fixture(201);
    let dir = tempfile::tempdir()?;
    let source = Database::new().await?;
    source.legacy(&f).await?;
    let target = Database::new().await?;
    let path = dir.path().join("converted.dsa");
    let source_dsn = source.dsn(true);
    let converted = target
        .cli(
            "convert",
            &f.profile.network,
            &[
                "--source",
                &source_dsn,
                "--horizon-block",
                "6",
                "--batch-rows",
                "3",
                "--batch-bytes",
                "2048",
                "--snapshot",
                path.to_str().unwrap(),
            ],
        )
        .await?;
    let expected = selected(&f.rows(), &keep(&f.chain, &f.profile, f.bounds)?);
    assert_eq!(target.rows().await?, expected);
    assert_eq!(
        converted["verified"]["state"]["target"]["height"],
        f.bounds.target
    );
    assert_eq!(
        converted["source"]["settings"]["transaction_read_only"],
        "on"
    );
    assert_eq!(
        converted["source"]["settings"]["default_transaction_read_only"],
        "on"
    );
    let dest = Database::new().await?;
    let restored = dest
        .cli("restore", &f.profile.network, &file_args(&path))
        .await?;
    assert_eq!(
        restored["content_digest"],
        converted["verified"]["content_digest"]
    );
    assert_eq!(
        restored["state_digest"],
        converted["verified"]["state_digest"]
    );
    assert_eq!(dest.rows().await?, expected);
    let old_spent = f
        .chain
        .inputs
        .values()
        .filter(|i| f.chain.transactions[&i.transaction].block < i64::from(f.bounds.horizon))
        .map(|i| i.output)
        .collect::<BTreeSet<_>>();
    let expected_markers = expected["tx_out"]
        .iter()
        .map(|r| r["id"].as_i64().unwrap())
        .filter(|id| old_spent.contains(id))
        .collect::<BTreeSet<_>>();
    let client = connect(&target.config).await?;
    let actual = client
        .query("SELECT tx_out_id FROM dsa.consumed_before_horizon", &[])
        .await?
        .into_iter()
        .map(|r| r.get::<_, i64>(0))
        .collect::<BTreeSet<_>>();
    assert_eq!(actual, expected_markers);
    assert!(
        !std::fs::read(&path)?
            .windows(source_dsn.len())
            .any(|b| b == source_dsn.as_bytes()),
        "snapshot disclosed source DSN"
    );
    drop(client);
    source.cleanup().await?;
    target.cleanup().await?;
    dest.cleanup().await?;
    Ok(())
}
