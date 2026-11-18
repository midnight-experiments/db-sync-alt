//! Generated true schema5 sources with the unchanged legacy gate and reader.
#[path = "keep_rule/fixtures.rs"]
#[allow(dead_code)]
mod fixtures;

use dsa_chain::{
    keep::{Header, TABLES, keep},
    profiles::Profile,
};
use dsa_store::{
    Store,
    convert::{ConvertOptions, LegacySource},
};
use fixtures::{Fixture, Rows, selected};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
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
    fn start_cli(&self, command: &str, network: &str, args: &[&str]) -> Result<Child> {
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
    async fn refusal(
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
    fn reader_config(&self) -> Config {
        let mut c = self.config.clone();
        c.user(&self.reader);
        c
    }
    async fn write(&self, sql: &str) -> Result {
        connect(&self.config).await?.batch_execute(&format!("BEGIN;SELECT pg_advisory_xact_lock(36,1);SET LOCAL db_sync_alt.projection_write=on;{sql};COMMIT")).await?;
        Ok(())
    }
    async fn catalog(&self) -> Result<Value> {
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
    async fn closed_empty(&self) -> Result {
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
    async fn barrier(&self) -> Result<Client> {
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
    async fn wait_barrier(&self) -> Result {
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

fn failure(output: &Output) -> Result<Value> {
    assert!(!output.status.success(), "unexpected successful conversion");
    let text = String::from_utf8_lossy(&output.stderr);
    Ok(text
        .lines()
        .filter_map(|s| serde_json::from_str::<Value>(s).ok())
        .find(|v| v.get("error").is_some())
        .ok_or_else(|| format!("no structured failure: {text}"))?)
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn convert_preflight_refusals_preserve_pristine_and_created_destinations() -> Result {
    let f = fixture(203);
    let source = Database::new().await?;
    source.legacy(&f).await?;
    let target = Database::new().await?;
    let dir = tempfile::tempdir()?;
    for created in [false, true] {
        if created {
            target.cli("create", "mainnet", &[]).await?;
        }
        let before = target.catalog().await?;
        source
            .write("UPDATE chain.service_state SET revoked=true")
            .await?;
        target
            .refusal("mainnet", &source.dsn(true), &[], "source_not_serving")
            .await?;
        source
            .write("UPDATE chain.service_state SET revoked=false")
            .await?;
        target
            .refusal(
                "mainnet",
                &source.dsn(false),
                &[],
                "invalid_source_reader_role",
            )
            .await?;
        target
            .refusal(
                "mainnet",
                &source.dsn(true),
                &["--horizon-block", "9"],
                "horizon_above_source_target",
            )
            .await?;
        target
            .refusal(
                "mainnet",
                &source.dsn(true),
                &[
                    "--snapshot",
                    dir.path().join("missing/file.dsa").to_str().unwrap(),
                ],
                "invalid_snapshot_destination",
            )
            .await?;
        target
            .refusal(
                "mainnet",
                &target.dsn(false),
                &[],
                "invalid_source_reader_role",
            )
            .await?;
        assert_eq!(target.catalog().await?, before);
    }
    // Occupied legacy source as its own destination is refused before mutation.
    let before = source.catalog().await?;
    source
        .refusal("mainnet", &source.dsn(true), &[], "destination_not_empty")
        .await?;
    assert_eq!(source.catalog().await?, before);
    // A completed destination is also protected, including its gate and ACL.
    target
        .cli(
            "convert",
            "mainnet",
            &["--source", &source.dsn(true), "--horizon-block", "6"],
        )
        .await?;
    let before = target.catalog().await?;
    let rows = target.rows().await?;
    target
        .refusal("mainnet", &source.dsn(true), &[], "destination_not_empty")
        .await?;
    assert_eq!(target.catalog().await?, before);
    assert_eq!(target.rows().await?, rows);
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn convert_network_identity_rejects_shared_units_and_uses_positive_anchor() -> Result {
    // Preview-like schema5 data: only shared Preprod/Preview policies, never
    // placeholder multi_asset rows as evidence of a held network-specific unit.
    let mut f = fixture(204);
    let shared = f.profile.assets["cnight"].policy.clone();
    f.chain
        .output_assets
        .retain(|_, a| f.chain.assets[&a.asset].policy == shared);
    let source = Database::new().await?;
    source.legacy(&f).await?;
    let target = Database::new().await?;
    let before = target.catalog().await?;
    target
        .refusal(
            "preprod",
            &source.dsn(true),
            &[],
            "source_network_ambiguous",
        )
        .await?;
    // Preview is deliberately unselectable before provider rebuild support.
    target
        .refusal("preview", &source.dsn(true), &[], "unsupported_network")
        .await?;
    assert_eq!(target.catalog().await?, before);
    let preview: Profile =
        serde_json::from_str(include_str!("../../dsa-chain/profiles/preview.json"))?;
    let mut session = LegacySource::connect(source.reader_config(), None).await?;
    let tx = session.snapshot().await?;
    let mut store = Store::connect(target.config.clone(), None).await?;
    let error = match store
        .stage_conversion(&preview, &tx, &ConvertOptions::default())
        .await
    {
        Ok(_) => panic!("placeholder Preview profile was accepted"),
        Err(error) => error,
    };
    assert_eq!(
        error.report()["error"],
        "source_profile_has_no_positive_anchor"
    );
    drop(tx);
    drop(session);
    drop(store);
    assert_eq!(target.catalog().await?, before);
    source.cleanup().await?;
    // A genuine positive Preprod anchor also works before any exclusive mint.
    let mut f = fixture(206);
    let offset = f.profile.start.block_number;
    f.translate_heights(offset);
    let source = Database::new().await?;
    source.legacy(&f).await?;
    source
        .write(&format!(
            "UPDATE serve.block SET hash=decode('{}','hex') WHERE id={offset}",
            f.profile.start.block_hash
        ))
        .await?;
    let out = target
        .cli(
            "convert",
            "preprod",
            &[
                "--source",
                &source.dsn(true),
                "--horizon-block",
                &f.bounds.horizon.to_string(),
            ],
        )
        .await?;
    assert_eq!(out["source"]["network_evidence"], "pinned_start_header");
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn convert_sparse_origins_continuous_suffix_and_missing_window_headers() -> Result {
    let mut f = fixture(208);
    let start = f.profile.start.block_number;
    f.translate_heights(start);
    // Move an ordinary still-unspent origin below the initial cursor, leaving
    // one isolated header there and the complete later header suffix intact.
    let old = f.output(4_318_350, 0, 0, "ics", "ics", None);
    f.chain.blocks.insert(
        4_318_350,
        Header {
            epoch: Some(0),
            slot: Some(1),
        },
    );
    let source = Database::new().await?;
    source.legacy(&f).await?;
    source.write(&format!("UPDATE serve.block SET hash=decode('{}','hex') WHERE id={start};UPDATE serve.block SET time=timestamp '2026-01-01'+id*interval '1second'", f.profile.start.block_hash)).await?;
    let target = Database::new().await?;
    // Short history clamps to the verified start, not to an isolated old row.
    let out = target
        .cli("convert", "preprod", &["--source", &source.dsn(true)])
        .await?;
    assert_eq!(out["source"]["state"]["horizon"]["height"], start);
    assert_eq!(out["source"]["horizon_search_start_height"], start);
    assert_eq!(out["source"]["source_first_header"], 4_318_350);
    assert_eq!(out["source"]["horizon_clamped_to_source_start"], true);
    assert!(
        target.rows().await?["tx_out"]
            .iter()
            .any(|r| r["id"] == old)
    );
    target.cleanup().await?;
    // An adjacent prefix before the known anchor is valid too. Stop at its
    // actual beginning, not at either the profile anchor or the isolated row.
    source.write(&format!("INSERT INTO serve.block SELECT n,decode(md5(n::text)||md5('prefix/'||n::text),'hex'),0,n,n,timestamp '2026-01-01'+n*interval '1second',0 FROM generate_series({}::bigint,{}::bigint)n",start-3,start-1)).await?;
    let extended = Database::new().await?;
    let out = extended
        .cli("convert", "preprod", &["--source", &source.dsn(true)])
        .await?;
    assert_eq!(out["source"]["state"]["horizon"]["height"], start - 3);
    assert!(out["source"]["horizon_prefix_chunks"].as_u64().unwrap() > 0);
    extended.cleanup().await?;
    let explicit = Database::new().await?;
    let out = explicit
        .cli(
            "convert",
            "preprod",
            &[
                "--source",
                &source.dsn(true),
                "--horizon-block",
                &(start - 2).to_string(),
            ],
        )
        .await?;
    assert_eq!(out["source"]["state"]["horizon"]["height"], start - 2);
    explicit.cleanup().await?;
    let target = Database::new().await?;
    let before = target.catalog().await?;
    target
        .refusal(
            "preprod",
            &source.dsn(true),
            &["--horizon-block", "4318351"],
            "source_header_gap",
        )
        .await?;
    source
        .write(&format!("DELETE FROM serve.block WHERE id={}", start + 1))
        .await?;
    target
        .refusal("preprod", &source.dsn(true), &[], "source_header_gap")
        .await?;
    target
        .refusal(
            "preprod",
            &source.dsn(true),
            &["--horizon-block", &(start + 1).to_string()],
            "source_header_gap",
        )
        .await?;
    assert_eq!(target.catalog().await?, before);
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn convert_target_time_cutoff_clamp_k10_and_first_served() -> Result {
    for seed in [209, 210] {
        let mut f = fixture(seed);
        f.no_window_spend();
        // T=8 is weeks in the past relative to the test; H=1 has exact 7-day age.
        let source = Database::new().await?;
        source.legacy(&f).await?;
        source
            .write("UPDATE serve.block SET time=timestamp '2020-01-01'+id*interval '1 day'")
            .await?;
        let target = Database::new().await?;
        let out = target
            .cli(
                "convert",
                &f.profile.network,
                &["--source", &source.dsn(true), "--batch-bytes", "1024"],
            )
            .await?;
        assert_eq!(out["source"]["state"]["horizon"]["height"], 1);
        assert_eq!(out["source"]["covered_seconds"], "604800.000000");
        assert_eq!(out["source"]["coverage_shorter_than_retention"], false);
        let mut bounds = f.bounds;
        bounds.horizon = 1;
        assert_eq!(
            target.rows().await?,
            selected(&source.rows().await?, &keep(&f.chain, &f.profile, bounds)?)
        );
        let c = connect(&target.config).await?;
        let row=c.query_one("SELECT count(*),min(height)::bigint,max(height)::bigint,bool_and(first_served_at>clock_timestamp()-interval '1 minute') FROM dsa.first_served",&[]).await?;
        assert_eq!(row.get::<_, i64>(0), 8);
        assert_eq!(row.get::<_, i64>(1), 1);
        assert_eq!(row.get::<_, i64>(2), 8);
        assert!(row.get::<_, bool>(3));
        drop(c);
        source.cleanup().await?;
        target.cleanup().await?;
    }
    let mut f = fixture(211);
    f.bounds.tip += 100;
    f.bounds.target += 100;
    for h in 0..=f.bounds.tip {
        f.chain.blocks.entry(i64::from(h)).or_insert(Header {
            epoch: Some((h / 2) as i32),
            slot: Some(i64::from(h) * 100 + 1),
        });
    }
    let source = Database::new().await?;
    source.legacy(&f).await?;
    let target = Database::new().await?;
    let out = target
        .cli("convert", "mainnet", &["--source", &source.dsn(true)])
        .await?;
    assert_eq!(out["source"]["state"]["target"]["height"], 108);
    assert_eq!(out["source"]["state"]["horizon"]["height"], 0);
    assert_eq!(out["source"]["horizon_clamped_to_source_start"], true);
    assert_eq!(out["source"]["coverage_shorter_than_retention"], true);
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn convert_corrupt_selected_parents_are_not_silently_dropped() -> Result {
    for orphan_asset in [false, true] {
        let mut f = fixture(213);
        if !orphan_asset {
            let old = f.output(1, 9, 12, "ics", "ics", None);
            f.chain.output_assets.retain(|_, a| a.output != old);
            f.add_asset(old, 9001, 19);
            f.chain
                .transactions
                .remove(&f.chain.outputs[&old].transaction);
        }
        let source = Database::new().await?;
        source.legacy(&f).await?;
        if orphan_asset {
            let id = dsa_chain::ids::asset_id(8, 0, 13, 19)?;
            source
                .write(&format!(
                    "INSERT INTO serve.ma_tx_out VALUES({id},1,{},9001)",
                    dsa_chain::ids::output_id(8, 0, 13)?
                ))
                .await?;
        }
        let target = Database::new().await?;
        target
            .refusal(
                "mainnet",
                &source.dsn(true),
                &["--horizon-block", "6"],
                "referential_closure_failed",
            )
            .await?;
        target.closed_empty().await?;
        source.cleanup().await?;
        target.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn convert_readonly_session_counters_and_coordinator_snapshot() -> Result {
    let f = fixture(215);
    let source = Database::new().await?;
    source.legacy(&f).await?;
    let target = Database::new().await?;
    source
        .admin
        .batch_execute(&format!(
            "ALTER ROLE {} SET default_transaction_read_only=off",
            source.reader
        ))
        .await?;
    let mut reader = LegacySource::connect(source.reader_config(), None).await?;
    assert_eq!(
        reader
            .client()
            .query_one("SHOW default_transaction_read_only", &[])
            .await?
            .get::<_, String>(0),
        "on"
    );
    // It is the same actual product source connection, without private/TEMP grants.
    assert!(
        reader
            .client()
            .batch_execute("CREATE TEMP TABLE forbidden(x int)")
            .await
            .is_err()
    );
    assert!(
        reader
            .client()
            .query("SELECT * FROM chain.service_state", &[])
            .await
            .is_err()
    );
    let before:Value=source.admin.query_one("SELECT jsonb_build_array(tup_inserted,tup_updated,tup_deleted) FROM pg_stat_database WHERE datname=$1",&[&source.name]).await?.get(0);
    let mut store = Store::connect(target.config.clone(), None).await?;
    let snapshot = reader.snapshot().await?;
    let stage = store
        .stage_conversion(
            &f.profile,
            &snapshot,
            &ConvertOptions {
                horizon_block: Some(6),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(
        snapshot
            .query_one("SELECT max(id) FROM public.block", &[])
            .await?
            .get::<_, i64>(0),
        i64::from(stage.source().state.tip.height)
    );
    // Read counters before the independent generated follower writes.
    let after:Value=source.admin.query_one("SELECT jsonb_build_array(tup_inserted,tup_updated,tup_deleted) FROM pg_stat_database WHERE datname=$1",&[&source.name]).await?.get(0);
    assert_eq!(before, after);
    source.write(&format!("INSERT INTO serve.block VALUES({},decode('{}','hex'),999,999999,{},timestamp '2026-01-02',0)",f.bounds.tip+1,"ef".repeat(32),f.bounds.tip+1)).await?;
    assert_eq!(
        snapshot
            .query_one("SELECT max(id) FROM public.block", &[])
            .await?
            .get::<_, i64>(0),
        i64::from(f.bounds.tip)
    );
    let fresh = connect(&source.reader_config()).await?;
    assert_eq!(
        fresh
            .query_one("SELECT max(id) FROM public.block", &[])
            .await?
            .get::<_, i64>(0),
        i64::from(f.bounds.tip + 1)
    );
    snapshot.commit().await?;
    let out = stage.finish(&f.profile).await?;
    assert_eq!(out.verified.state.tip.height, f.bounds.tip);
    // An actual source write is refused by default; this attempt cannot alter
    // counters or commit a row, independently of its missing table privilege.
    let tx = reader.snapshot().await?;
    let err = tx
        .batch_execute("CREATE TABLE public.forbidden(x int)")
        .await
        .unwrap_err();
    assert_eq!(err.code().unwrap().code(), "25006");
    drop(tx);
    drop(fresh);
    drop(reader);
    drop(store);
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn convert_tls_default_refuses_plaintext_explicit_local_works() -> Result {
    let db = Database::new().await?;
    assert_eq!(
        connect(&db.config)
            .await?
            .query_one("SHOW ssl", &[])
            .await?
            .get::<_, String>(0),
        "off"
    );
    let mut implicit = db.config.clone();
    implicit.ssl_mode(SslMode::Prefer);
    assert!(Store::connect(implicit.clone(), None).await.is_err());
    assert!(LegacySource::connect(implicit, None).await.is_err());
    drop(Store::connect(db.config.clone(), None).await?);
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn convert_cancellation_and_explicit_destination_idle_timeout_retry() -> Result {
    for cancel in [true, false] {
        let f = fixture(217);
        let source = Database::new().await?;
        source.legacy(&f).await?;
        let target = Database::new().await?;
        let barrier = source.barrier().await?;
        let dsn = source.dsn(true);
        let mut child = target.start_cli(
            "convert",
            "mainnet",
            &[
                "--source",
                &dsn,
                "--horizon-block",
                "6",
                "--idle-transaction-timeout-seconds",
                "1",
            ],
        )?;
        source.wait_barrier().await?;
        if cancel {
            child.kill().await?;
        } else {
            // The destination waits for the source sort; its explicit one-second
            // idle limit must remain effective, never silently changed to zero.
            tokio::time::sleep(Duration::from_millis(1600)).await;
        }
        barrier
            .query_one("SELECT pg_advisory_unlock(726,1)", &[])
            .await?;
        let out = child.wait_with_output().await?;
        assert!(!out.status.success());
        target.closed_empty().await?;
        let out = target
            .cli(
                "convert",
                "mainnet",
                &[
                    "--source",
                    &dsn,
                    "--horizon-block",
                    "6",
                    "--idle-transaction-timeout-seconds",
                    "120",
                ],
            )
            .await?;
        assert!(out["verified"]["content_digest"].is_string());
        drop(barrier);
        source.cleanup().await?;
        target.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn convert_late_snapshot_failure_reports_published_digest_and_recovery() -> Result {
    let f = fixture(219);
    let source = Database::new().await?;
    source.legacy(&f).await?;
    let target = Database::new().await?;
    let root = tempfile::tempdir()?;
    let parent = root.path().join("output");
    std::fs::create_dir(&parent)?;
    let path = parent.join("snap.dsa");
    let barrier = source.barrier().await?;
    let child = target.start_cli(
        "convert",
        "mainnet",
        &[
            "--source",
            &source.dsn(true),
            "--horizon-block",
            "6",
            "--snapshot",
            path.to_str().unwrap(),
        ],
    )?;
    source.wait_barrier().await?;
    std::fs::rename(&parent, root.path().join("moved"))?;
    barrier
        .query_one("SELECT pg_advisory_unlock(726,1)", &[])
        .await?;
    let report = failure(&child.wait_with_output().await?)?;
    assert_eq!(report["error"], "conversion_completed_snapshot_failed");
    assert_eq!(report["converted"], true);
    let verified = target.cli("verify", "mainnet", &[]).await?;
    assert_eq!(report["content_digest"], verified["content_digest"]);
    assert_eq!(report["state_digest"], verified["state_digest"]);
    assert!(report["recovery"].as_str().unwrap().contains("snapshot"));
    std::fs::create_dir(&parent)?;
    target.cli("snapshot", "mainnet", &file_args(&path)).await?;
    drop(barrier);
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn convert_large_lossless_rows_and_k10_fallback() -> Result {
    let mut f = fixture(221);
    f.no_window_spend();
    let zero = f.asset_id("auth");
    f.chain.output_assets.retain(|_, a| a.asset != zero);
    let source = Database::new().await?;
    source.legacy(&f).await?;
    // Use our own supported structured JSONB, never third-party reproduction.
    let nested = format!(
        "{}340282366920938463463374607431768211456{}",
        "[".repeat(200),
        "]".repeat(200)
    );
    source.write(&format!("UPDATE serve.datum SET value='{nested}'::jsonb;UPDATE serve.tx_metadata SET json=jsonb_build_object('structured','{nested}'::jsonb,'bytes',repeat('x',8192));UPDATE serve.tx_out SET value=(value::text||'.00000')::numeric")).await?;
    let target = Database::new().await?;
    let converted = target
        .cli(
            "convert",
            "mainnet",
            &[
                "--source",
                &source.dsn(true),
                "--horizon-block",
                "6",
                "--batch-bytes",
                "1024",
                "--batch-rows",
                "2",
            ],
        )
        .await?;
    assert_eq!(
        converted["verified"]["supply"][f.profile.assets["auth"].unit()],
        "0"
    );
    let c = connect(&target.config).await?;
    let original = connect(&source.config).await?;
    for table in ["datum", "tx_metadata", "tx_out"] {
        let rows = c
            .query(
                &format!("SELECT id,to_jsonb(r)::text FROM serve.{table} r ORDER BY id"),
                &[],
            )
            .await?;
        for row in rows {
            let text: String = original
                .query_one(
                    &format!("SELECT to_jsonb(r)::text FROM serve.{table} r WHERE id=$1"),
                    &[&row.get::<_, i64>(0)],
                )
                .await?
                .get(0);
            assert_eq!(row.get::<_, String>(1), text);
        }
    }
    let expected = keep(&f.chain, &f.profile, f.bounds)?;
    let count: i64 = c
        .query_one("SELECT count(*) FROM serve.tx_in", &[])
        .await?
        .get(0);
    assert_eq!(count as usize, expected["tx_in"].len());
    assert_eq!(count, 1);
    assert!(
        converted["source"]["families"]["datum"]["jsonb_utf8_bytes"]
            .as_u64()
            .unwrap()
            > 1024
    );
    assert!(
        converted["source"]["families"]["datum"]["batches"]
            .as_u64()
            .unwrap()
            > 0
    );
    drop(c);
    drop(original);
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn convert_source_statement_timeout_is_named_closed_and_retryable() -> Result {
    let f = fixture(223);
    let source = Database::new().await?;
    source.legacy(&f).await?;
    let target = Database::new().await?;
    let barrier = source.barrier().await?;
    let child = target.start_cli(
        "convert",
        "mainnet",
        &[
            "--source",
            &source.dsn(true),
            "--horizon-block",
            "6",
            "--source-statement-timeout-seconds",
            "1",
        ],
    )?;
    source.wait_barrier().await?;
    let report = failure(&child.wait_with_output().await?)?;
    assert_eq!(report["error"], "source_statement_timeout");
    assert_eq!(report["family"], "block");
    assert_eq!(report["sqlstate"], "57014");
    assert!(report["elapsed_seconds"].as_f64().unwrap() >= 0.9);
    barrier
        .query_one("SELECT pg_advisory_unlock(726,1)", &[])
        .await?;
    target.closed_empty().await?;
    target
        .cli(
            "convert",
            "mainnet",
            &[
                "--source",
                &source.dsn(true),
                "--horizon-block",
                "6",
                "--source-statement-timeout-seconds",
                "60",
            ],
        )
        .await?;
    drop(barrier);
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn convert_destination_termination_stream_finish_and_post_source_commit_kill() -> Result {
    for mode in ["stream", "finish", "kill"] {
        let f = fixture(225);
        let source = Database::new().await?;
        source.legacy(&f).await?;
        let target = Database::new().await?;
        target.cli("create", "mainnet", &[]).await?;
        let owner = connect(&target.config).await?;
        let table = if mode == "stream" {
            "serve.block"
        } else {
            "dsa.first_served"
        };
        owner.batch_execute(&format!("CREATE FUNCTION dsa.test_barrier() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM pg_advisory_xact_lock(726,2);RETURN NEW;END$$;CREATE TRIGGER test_barrier BEFORE INSERT ON {table} FOR EACH STATEMENT EXECUTE FUNCTION dsa.test_barrier()")).await?;
        owner
            .query_one("SELECT pg_advisory_lock(726,2)", &[])
            .await?;
        let mut child = target.start_cli(
            "convert",
            "mainnet",
            &[
                "--source",
                &source.dsn(true),
                "--horizon-block",
                "6",
                "--batch-rows",
                "2",
            ],
        )?;
        let started = Instant::now();
        let pid = loop {
            if let Some(r)=target.admin.query_opt("SELECT pid FROM pg_stat_activity WHERE datname=$1 AND application_name='db-sync-alt' AND wait_event='advisory'",&[&target.name]).await? {break r.get::<_,i32>(0);}
            if started.elapsed() > Duration::from_secs(10) {
                return Err(format!("no destination {mode} barrier").into());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        if mode != "stream" {
            let source_idle:bool=source.admin.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=$1 AND application_name='db-sync-alt-convert-source' AND state='idle' AND xact_start IS NULL)",&[&source.name]).await?.get(0);
            assert!(
                source_idle,
                "source transaction must be committed before finish barrier"
            );
        }
        if mode == "kill" {
            child.kill().await?;
        } else {
            target
                .admin
                .query_one("SELECT pg_terminate_backend($1)", &[&pid])
                .await?;
        }
        owner
            .query_one("SELECT pg_advisory_unlock(726,2)", &[])
            .await?;
        assert!(!child.wait_with_output().await?.status.success());
        target.closed_empty().await?;
        owner
            .batch_execute(&format!(
                "DROP TRIGGER test_barrier ON {table};DROP FUNCTION dsa.test_barrier()"
            ))
            .await?;
        let retried = target
            .cli(
                "convert",
                "mainnet",
                &["--source", &source.dsn(true), "--horizon-block", "6"],
            )
            .await?;
        let pristine = Database::new().await?;
        let oracle = pristine
            .cli(
                "convert",
                "mainnet",
                &["--source", &source.dsn(true), "--horizon-block", "6"],
            )
            .await?;
        assert_eq!(
            retried["verified"]["content_digest"],
            oracle["verified"]["content_digest"]
        );
        assert_eq!(
            retried["verified"]["state_digest"],
            oracle["verified"]["state_digest"]
        );
        drop(owner);
        pristine.cleanup().await?;
        source.cleanup().await?;
        target.cleanup().await?;
    }
    Ok(())
}

/// On-demand only: all data are deterministic/generated; normal branch CI
/// excludes this test. Hosted runs archive its full JSON plans and SQL hashes.
#[tokio::test]
#[ignore = "on-demand generated multi-million-header source cost"]
async fn convert_scaled_source_cost() -> Result {
    let headers: u32 = std::env::var("DSA_CONVERSION_HEADERS")
        .unwrap_or_else(|_| "2432170".to_owned())
        .parse()?;
    assert!(headers >= 10000);
    let offset = 13_200_000;
    let mut f = fixture(227);
    f.translate_heights(offset);
    let p = offset + headers - 1;
    let t = p - f.profile.cardano.security_parameter;
    let h = t - 30009.min(headers / 10);
    let source = Database::new().await?;
    source.legacy(&f).await?;
    let began = Instant::now();
    // The real Mainnet source's physical header count is about1.2M, despite its
    // height near14M. Here header count, height, and density are all explicit.
    source.write(&format!("UPDATE serve.block SET epoch_no=(id-{offset})/20000,slot_no=id*20,time=timestamp '2026-01-01'+(id-{offset})*interval '20seconds'")).await?;
    let asset = f.asset_id("cnight");
    let mut counts = BTreeMap::new();
    for low in (offset..=p).step_by(5000) {
        let high = (low + 4999).min(p);
        source.write(&format!("INSERT INTO serve.block SELECT n,decode(md5('block/'||n::text)||md5('block2/'||n::text),'hex'),(n-{offset})/20000,n*20,n,timestamp '2026-01-01'+(n-{offset})*interval '20seconds',0 FROM generate_series({low}::bigint,{high}::bigint)n ON CONFLICT DO NOTHING;
          INSERT INTO serve.tx SELECT n*4096+7,decode(md5('tx/'||n::text)||md5('tx2/'||n::text),'hex'),n,7,true FROM generate_series({low}::bigint,least({high},{t})::bigint)n WHERE n%2=0;
          INSERT INTO serve.tx_out SELECT n*16777216+7*4096,n*4096+7,0,'external-'||(n%10007)::text,1,NULL,NULL FROM generate_series({low}::bigint,least({high},{t})::bigint)n WHERE n%2=0;
          INSERT INTO serve.ma_tx_out SELECT (n*16777216+7*4096)*4096,1,n*16777216+7*4096,{asset} FROM generate_series({low}::bigint,least({high},{t})::bigint)n WHERE n%2=0;
          INSERT INTO serve.tx SELECT (n+1)*4096+8,decode(md5('spend/'||n::text)||md5('spend2/'||n::text),'hex'),n+1,8,true FROM generate_series({low}::bigint,least({high},{h}-2)::bigint)n WHERE n%2=0 AND n%10<>0;
          INSERT INTO serve.tx_in SELECT (n+1)*16777216+8*4096,(n+1)*4096+8,n*4096+7,0 FROM generate_series({low}::bigint,least({high},{h}-2)::bigint)n WHERE n%2=0 AND n%10<>0")).await?;
        if (low - offset).is_multiple_of(100000) {
            println!(
                "{}",
                json!({"event":"cost_fixture_progress","last_header":high,"elapsed_seconds":began.elapsed().as_secs_f64()})
            );
        }
    }
    let owner = connect(&source.config).await?;
    for table in TABLES {
        owner
            .batch_execute(&format!("ANALYZE serve.{table}"))
            .await?;
        counts.insert(
            table,
            owner
                .query_one(&format!("SELECT count(*) FROM serve.{table}"), &[])
                .await?
                .get::<_, i64>(0),
        );
    }
    assert_eq!(counts["block"], i64::from(headers));
    println!(
        "{}",
        json!({"event":"cost_fixture_ready","headers":headers,"physical_rows":counts,"H":h,"T":t,"P":p,"setup_seconds":began.elapsed().as_secs_f64()})
    );
    let mut config = source.reader_config();
    config.options("-c work_mem=4MB -c jit=off -c max_parallel_workers_per_gather=0");
    for limit in [None, Some(300)] {
        let target = Database::new().await?;
        let mut source_session = LegacySource::connect(config.clone(), None).await?;
        source_session.limits(limit, Some(300)).await?;
        source
            .admin
            .batch_execute("SELECT pg_stat_clear_snapshot()")
            .await?;
        let before: i64 = source
            .admin
            .query_one(
                "SELECT temp_bytes FROM pg_stat_database WHERE datname=$1",
                &[&source.name],
            )
            .await?
            .get(0);
        let started = Instant::now();
        let mut store = Store::connect(target.config.clone(), None).await?;
        let result = store
            .convert(
                &f.profile,
                &mut source_session,
                &ConvertOptions {
                    horizon_block: Some(h),
                    ..Default::default()
                },
            )
            .await;
        // Flush this generated source backend's statistics before measuring;
        // this does not write any served/source relation or alter server config.
        drop(source_session);
        tokio::time::sleep(Duration::from_millis(100)).await;
        source
            .admin
            .batch_execute("SELECT pg_stat_clear_snapshot()")
            .await?;
        let after: i64 = source
            .admin
            .query_one(
                "SELECT temp_bytes FROM pg_stat_database WHERE datname=$1",
                &[&source.name],
            )
            .await?
            .get(0);
        match result {
            Ok(result) => println!(
                "{}",
                json!({"event":"cost_conversion","explicit_statement_seconds":limit,"elapsed_seconds":started.elapsed().as_secs_f64(),"source_temp_bytes_delta":after-before,"result":result})
            ),
            Err(error) => {
                println!(
                    "{}",
                    json!({"event":"cost_conversion_failure","explicit_statement_seconds":limit,"elapsed_seconds":started.elapsed().as_secs_f64(),"source_temp_bytes_delta":after-before,"error":error.report()})
                );
                if limit.is_some() {
                    return Err(error.into());
                }
                assert_eq!(error.report()["error"], "source_statement_timeout");
                target.closed_empty().await?;
            }
        }
        drop(store);
        target.cleanup().await?;
    }
    let mut session = LegacySource::connect(config, None).await?;
    session.limits(Some(300), Some(300)).await?;
    let selection = dsa_store::keep::Selection::new(
        &f.profile,
        dsa_chain::keep::Bounds {
            horizon: h,
            target: t,
            tip: p,
        },
    )?;
    // Both K10 paths on the same generated source; no fixture normalization.
    for present in [false, true] {
        if present {
            source.write(&format!("INSERT INTO serve.tx VALUES({}::bigint*4096+8,decode('{}','hex'),{h},8,true);INSERT INTO serve.tx_in VALUES({}::bigint*16777216+8*4096,{}::bigint*4096+8,{}::bigint*4096+7,0)",h,"ac".repeat(32),h,h,offset)).await?;
        }
        let tx = session.snapshot().await?;
        let settings:Value=tx.query_one("SELECT jsonb_build_object('statement_timeout',current_setting('statement_timeout'),'work_mem',current_setting('work_mem'),'jit',current_setting('jit'),'parallel',current_setting('max_parallel_workers_per_gather'),'server_version',current_setting('server_version'))",&[]).await?.get(0);
        for table in ["block", "tx_out", "tx_in"] {
            let sql = selection.conversion_rows_sql(table)?;
            let plan: Value = tx
                .query_one(
                    &format!("EXPLAIN(ANALYZE,BUFFERS,SETTINGS,FORMAT JSON) {sql}"),
                    &selection.parameters(),
                )
                .await?
                .get(0);
            println!(
                "{}",
                json!({"event":"cost_plan","family":table,"window_spend_present":present,"settings":settings,"sql_sha256":format!("{:x}",<sha2::Sha256 as sha2::Digest>::digest(sql.as_bytes())),"plan":plan})
            );
        }
        tx.commit().await?;
    }
    drop(session);
    drop(owner);
    source.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn convert_source_preflight_rejects_extra_rights_empty_spends_and_old_schema() -> Result {
    let f = fixture(229);
    let source = Database::new().await?;
    source.legacy(&f).await?;
    let target = Database::new().await?;
    let before = target.catalog().await?;
    source
        .admin
        .batch_execute(&format!(
            "GRANT TEMP ON DATABASE {} TO {}",
            source.name, source.reader
        ))
        .await?;
    target
        .refusal(
            "mainnet",
            &source.dsn(true),
            &[],
            "reader_privileges_not_exact",
        )
        .await?;
    source
        .admin
        .batch_execute(&format!(
            "REVOKE TEMP ON DATABASE {} FROM {}",
            source.name, source.reader
        ))
        .await?;
    source.write("DELETE FROM serve.tx_in").await?;
    target
        .refusal("mainnet", &source.dsn(true), &[], "source_has_no_spends")
        .await?;
    source.write(&format!("DROP VIEW public.epoch_param;CREATE VIEW public.epoch_param AS SELECT epoch_no,nonce FROM serve.epoch_param WHERE chain.gate();GRANT SELECT ON public.epoch_param TO {}",source.reader)).await?;
    target
        .refusal(
            "mainnet",
            &source.dsn(true),
            &[],
            "unsupported_legacy_schema",
        )
        .await?;
    assert_eq!(target.catalog().await?, before);
    source.cleanup().await?;
    target.cleanup().await?;
    Ok(())
}
