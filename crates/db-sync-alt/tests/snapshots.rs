//! Generated-only PG17/CLI snapshot proofs. No operator source or provider.
#[path = "keep_rule/fixtures.rs"]
#[allow(dead_code)]
mod fixtures;

use dsa_chain::{
    keep::{Bounds, TABLES},
    profiles::Profile,
};
use dsa_store::Store;
use fixtures::{
    Fixture, Rows,
    model::{Header, keep},
    selected,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error as StdError,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tokio::{process::Command, sync::oneshot};
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
        let owner = format!("snapshot_owner_{}_{nonce:x}", std::process::id());
        let reader = format!("snapshot_reader_{}_{nonce:x}", std::process::id());
        let name = format!("snapshot_{}_{nonce:x}", std::process::id());
        admin.batch_execute(&format!("CREATE ROLE {owner} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;CREATE ROLE {reader} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS")).await?;
        admin
            .batch_execute(&format!("CREATE DATABASE {name} OWNER {owner}"))
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
    fn dsn(&self) -> String {
        let host = match &self.config.get_hosts()[0] {
            Host::Tcp(s) => s.clone(),
            Host::Unix(p) => p.display().to_string(),
        };
        format!(
            "host='{}' port={} user={} dbname={} sslmode=disable",
            host.replace('\\', "\\\\").replace('\'', "\\'"),
            self.config.get_ports().first().copied().unwrap_or(5432),
            self.owner,
            self.name
        )
    }
    async fn cli_raw(
        &self,
        command: &str,
        network: &str,
        args: &[&str],
    ) -> Result<std::process::Output> {
        Ok(Command::new(env!("CARGO_BIN_EXE_db-sync-alt"))
            .args([command, "--database", &self.dsn(), "--network", network])
            .args(args)
            .kill_on_drop(true)
            .output()
            .await?)
    }
    async fn cli(&self, command: &str, network: &str, args: &[&str]) -> Result<Value> {
        let output = self.cli_raw(command, network, args).await?;
        assert!(
            output.status.success(),
            "{command}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(serde_json::from_slice(&output.stdout)?)
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
    f.bounds.tip = f.bounds.target + f.profile.cardano.security_parameter;
    for h in 0..=f.bounds.tip {
        f.chain.blocks.entry(i64::from(h)).or_insert(Header {
            epoch: Some((h / 2) as i32),
            slot: Some(i64::from(h) * 100 + 1),
        });
    }
    // Deliberately mixed assets and non-dense original ordinals: snapshot must
    // preserve eligible IDs with gaps after rule-2 source selection.
    let output = f.output(8, 0, 11, "ics", "ics", Some(999));
    f.add_asset(output, 9001, 19);
    f
}

async fn load(client: &Client, rows: &Rows) -> Result {
    for table in TABLES {
        client.execute(&format!("INSERT INTO serve.{table} SELECT * FROM jsonb_populate_recordset(NULL::serve.{table},$1)"),&[&json!(rows[table])]).await?;
    }
    Ok(())
}

fn markers(f: &Fixture, kept: &Rows, h: u32) -> BTreeSet<i64> {
    let outputs = kept["tx_out"]
        .iter()
        .map(|r| r["id"].as_i64().unwrap())
        .collect::<BTreeSet<_>>();
    f.chain
        .inputs
        .values()
        .filter(|i| {
            f.chain.transactions[&i.transaction].block < i64::from(h) && outputs.contains(&i.output)
        })
        .map(|i| i.output)
        .collect()
}

async fn position(client: &Client, b: Bounds) -> Result {
    client.execute("UPDATE dsa.position SET horizon_height=$1::integer,horizon_hash=(SELECT hash FROM serve.block WHERE id=$1::bigint),horizon_time=(SELECT time AT TIME ZONE 'UTC' FROM serve.block WHERE id=$1::bigint),target_height=$2::integer,target_hash=(SELECT hash FROM serve.block WHERE id=$2::bigint),target_time=(SELECT time AT TIME ZONE 'UTC' FROM serve.block WHERE id=$2::bigint),tip_height=$3::integer,tip_hash=(SELECT hash FROM serve.block WHERE id=$3::bigint),tip_time=(SELECT time AT TIME ZONE 'UTC' FROM serve.block WHERE id=$3::bigint),serving=true,reason='generated',updated_at=now() WHERE id=1",&[&(b.horizon as i32),&(b.target as i32),&(b.tip as i32)]).await?;
    Ok(())
}

async fn populate(db: &Database, f: &Fixture) -> Result<Rows> {
    db.cli("create", &f.profile.network, &[]).await?;
    let client = connect(&db.config).await?;
    let kept = selected(&f.rows(), &keep(&f.chain, &f.profile, f.bounds)?);
    load(&client, &kept).await?;
    for id in markers(f, &kept, f.bounds.horizon) {
        client
            .execute("INSERT INTO dsa.consumed_before_horizon VALUES($1)", &[&id])
            .await?;
    }
    for unit in f
        .profile
        .assets
        .iter()
        .map(|(_, a)| a.unit())
        .collect::<BTreeSet<_>>()
    {
        let spent = f
            .chain
            .inputs
            .values()
            .filter(|i| f.chain.transactions[&i.transaction].block <= i64::from(f.bounds.target))
            .map(|i| i.output)
            .collect::<BTreeSet<_>>();
        let quantity: u64 = f
            .chain
            .output_assets
            .iter()
            .filter(|(_, a)| {
                f.chain.assets[&a.asset].unit() == unit
                    && !spent.contains(&a.output)
                    && f.chain.transactions[&f.chain.outputs[&a.output].transaction].block
                        <= i64::from(f.bounds.target)
            })
            .map(|(id, _)| f.quantities.get(id).copied().unwrap_or(1))
            .sum();
        client
            .execute(
                "INSERT INTO dsa.supply VALUES($1,$2::text::numeric)",
                &[&unit, &quantity.to_string()],
            )
            .await?;
    }
    position(&client, f.bounds).await?;
    client.batch_execute("INSERT INTO dsa.first_served SELECT id,now()-interval '2 days' FROM serve.block WHERE id>=(SELECT horizon_height FROM dsa.position) AND id<=(SELECT target_height FROM dsa.position)").await?;
    Ok(kept)
}

async fn rows(db: &Database) -> Result<Rows> {
    let client = connect(&db.config).await?;
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
fn file_args(path: &Path) -> [&str; 2] {
    ["--file", path.to_str().unwrap()]
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn snapshot_generated_roundtrips_and_reader() -> Result {
    let dir = tempfile::tempdir()?;
    for seed in 0..8 {
        let f = fixture(seed);
        let source = Database::new().await?;
        populate(&source, &f).await?;
        let before = source.cli("verify", &f.profile.network, &[]).await?;
        let path = dir.path().join(format!("seed{seed}.dsa"));
        source
            .cli("snapshot", &f.profile.network, &file_args(&path))
            .await?;
        let dest = Database::new().await?;
        let restored = dest
            .cli("restore", &f.profile.network, &file_args(&path))
            .await?;
        for key in [
            "content_digest",
            "state_digest",
            "families",
            "protected_state",
            "state",
            "supply",
        ] {
            assert_eq!(before[key], restored[key], "{seed}/{key}");
        }
        assert_eq!(rows(&source).await?, rows(&dest).await?);
        let client = connect(&dest.config).await?;
        for table in ["multi_asset", "datum", "pool_hash", "epoch_stake"] {
            let row=client.query_one(&format!("SELECT nextval('dsa.{table}_id_seq'),coalesce(max(id),0) FROM serve.{table}"),&[]).await?;
            assert!(row.get::<_, i64>(0) > row.get::<_, i64>(1));
        }
        let refreshed:bool=client.query_one("SELECT bool_and(first_served_at>now()-interval '1 minute') AND count(*)=(SELECT target_height-horizon_height+1 FROM dsa.position) FROM dsa.first_served",&[]).await?.get(0);
        assert!(refreshed);
        dest.cli(
            "grant-reader",
            &f.profile.network,
            &["--role", &dest.reader],
        )
        .await?;
        let mut config = dest.config.clone();
        config.user(&dest.reader);
        let reader = connect(&config).await?;
        for table in TABLES {
            reader
                .query_one(&format!("SELECT count(*) FROM public.{table}"), &[])
                .await?;
        }
        assert!(
            reader
                .batch_execute("SELECT * FROM dsa.consumed_before_horizon")
                .await
                .is_err()
        );
        assert!(
            source
                .cli_raw("snapshot", &f.profile.network, &file_args(&path))
                .await?
                .status
                .code()
                != Some(0)
        );
        drop(reader);
        drop(client);
        source.cleanup().await?;
        dest.cleanup().await?;
    }
    Ok(())
}

fn rewrite_description(bytes: &[u8], change: impl FnOnce(&mut Value)) -> Vec<u8> {
    let n = u64::from_be_bytes(bytes[12..20].try_into().unwrap()) as usize;
    let mut description: Value = serde_json::from_slice(&bytes[52..52 + n]).unwrap();
    change(&mut description);
    let json = serde_json::to_vec(&description).unwrap();
    let mut result = bytes[..12].to_vec();
    result.extend((json.len() as u64).to_be_bytes());
    result.extend(Sha256::digest(&json));
    result.extend(json);
    result.extend(&bytes[52 + n..]);
    result
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn snapshot_corruption_refusal_retry_and_serving_protection() -> Result {
    let dir = tempfile::tempdir()?;
    let f = fixture(41);
    let source = Database::new().await?;
    populate(&source, &f).await?;
    let good = dir.path().join("good.dsa");
    let description = source
        .cli("snapshot", &f.profile.network, &file_args(&good))
        .await?;
    let bytes = std::fs::read(&good)?;
    let n = u64::from_be_bytes(bytes[12..20].try_into().unwrap()) as usize;
    let mut cases = Vec::new();
    let mut damaged = bytes.clone();
    damaged[52 + n / 2] ^= 1;
    cases.push(("description", damaged));
    let mut offset = 52 + n;
    for section in description["sections"].as_array().unwrap() {
        let size = section["compressed_bytes"].as_u64().unwrap() as usize;
        let mut damaged = bytes.clone();
        damaged[offset + size / 2] ^= 1;
        cases.push((section["table"].as_str().unwrap(), damaged));
        offset += size;
    }
    cases.push(("truncated", bytes[..bytes.len() - 1].to_vec()));
    let mut format = bytes.clone();
    format[11] = 2;
    cases.push(("format", format));
    cases.push((
        "network",
        rewrite_description(&bytes, |v| v["network"] = json!("preprod")),
    ));
    cases.push((
        "rule",
        rewrite_description(&bytes, |v| v["keep_rule_version"] = json!(1)),
    ));
    let mut trailing = bytes.clone();
    trailing.push(0);
    cases.push(("trailing", trailing));
    cases.push((
        "content",
        rewrite_description(&bytes, |v| {
            v["verified"]["content_digest"] = json!("00".repeat(32))
        }),
    ));
    let dest = Database::new().await?;
    let client = connect(&dest.config).await?;
    let mut previous = database_state(&client).await?;
    for (name, bytes) in cases {
        let bad = dir.path().join(format!("bad-{name}.dsa"));
        std::fs::write(&bad, bytes)?;
        let output = dest
            .cli_raw("restore", &f.profile.network, &file_args(&bad))
            .await?;
        assert!(!output.status.success(), "accepted {name}");
        if name == "content" {
            // A consistent file whose content differs from its description is
            // refused by verification inside the build, which rolls back.
            let state = dest.cli("status", &f.profile.network, &[]).await?;
            assert_eq!(state["position"]["serving"], false);
            assert_eq!(state["position"]["reason"], "restore_failed");
            assert_eq!(
                client.query("SELECT * FROM serve.block", &[]).await?.len(),
                0
            );
            assert!(
                client
                    .query("SELECT * FROM public.block", &[])
                    .await
                    .is_err()
            );
            previous = database_state(&client).await?;
        } else {
            assert_eq!(
                database_state(&client).await?,
                previous,
                "{name} changed the database"
            );
        }
    }
    drop(client);
    dest.cli("restore", &f.profile.network, &file_args(&good))
        .await?;
    let before = dest.cli("verify", &f.profile.network, &[]).await?;
    let before_status = dest.cli("status", &f.profile.network, &[]).await?;
    let client = connect(&dest.config).await?;
    let before_storage: i64 = client
        .query_one(
            "SELECT pg_relation_filenode('serve.tx_out'::regclass)::bigint",
            &[],
        )
        .await?
        .get(0);
    assert!(
        !dest
            .cli_raw("restore", &f.profile.network, &file_args(&good))
            .await?
            .status
            .success()
    );
    let after = dest.cli("verify", &f.profile.network, &[]).await?;
    assert_eq!(before["content_digest"], after["content_digest"]);
    assert_eq!(
        before_status,
        dest.cli("status", &f.profile.network, &[]).await?
    );
    let after_storage: i64 = client
        .query_one(
            "SELECT pg_relation_filenode('serve.tx_out'::regclass)::bigint",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(
        before_storage, after_storage,
        "serving storage was rewritten"
    );
    assert_eq!(rows(&source).await?, rows(&dest).await?);
    drop(client);
    source.cleanup().await?;
    dest.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn snapshot_digest_identity_independence_and_payload_sensitivity() -> Result {
    let f = fixture(51);
    let db = Database::new().await?;
    populate(&db, &f).await?;
    let before = db.cli("verify", &f.profile.network, &[]).await?;
    let client = connect(&db.config).await?;
    client.batch_execute("UPDATE serve.multi_asset SET id=id+100000;UPDATE serve.ma_tx_out SET ident=ident+100000;UPDATE serve.datum SET id=id+200000;UPDATE serve.pool_hash SET id=id+300000;UPDATE serve.epoch_stake SET id=id+400000,pool_id=pool_id+300000").await?;
    let after = db.cli("verify", &f.profile.network, &[]).await?;
    assert_eq!(before["content_digest"], after["content_digest"]);
    assert_eq!(before["state_digest"], after["state_digest"]);
    // The domains are integral; numeric display scale is not a logical amount.
    // Physical COPY continues to preserve the source's actual numeric value.
    client.batch_execute("UPDATE serve.tx_out SET value=value::numeric(30,2);UPDATE serve.ma_tx_out SET quantity=quantity::numeric(30,2);UPDATE serve.epoch_stake SET amount=amount::numeric(30,2);UPDATE dsa.supply SET quantity=quantity::numeric(30,2)").await?;
    assert_eq!(
        after["content_digest"],
        db.cli("verify", &f.profile.network, &[]).await?["content_digest"]
    );
    let output: i64 = client
        .query_one(
            "SELECT id FROM serve.tx_out WHERE id>=8*16777216 AND data_hash IS NULL ORDER BY id LIMIT 1",
            &[],
        )
        .await?
        .get(0);
    for (change, undo) in [
        (
            "address=address||'_changed'",
            "address=left(address,length(address)-8)",
        ),
        ("value=value+1", "value=value-1"),
    ] {
        client
            .execute(
                &format!("UPDATE serve.tx_out SET {change} WHERE id=$1"),
                &[&output],
            )
            .await?;
        let changed = db.cli("verify", &f.profile.network, &[]).await?;
        assert_ne!(after["content_digest"], changed["content_digest"]);
        client
            .execute(
                &format!("UPDATE serve.tx_out SET {undo} WHERE id=$1"),
                &[&output],
            )
            .await?;
    }
    let datum: i64 = client
        .query_one("SELECT id FROM serve.datum ORDER BY id LIMIT 1", &[])
        .await?
        .get(0);
    let original: Option<Value> = client
        .query_one("SELECT value FROM serve.datum WHERE id=$1", &[&datum])
        .await?
        .get(0);
    client
        .execute(
            "UPDATE serve.datum SET value='{\"bytes\":\"01\"}'::jsonb WHERE id=$1",
            &[&datum],
        )
        .await?;
    assert_ne!(
        after["content_digest"],
        db.cli("verify", &f.profile.network, &[]).await?["content_digest"]
    );
    client
        .execute(
            "UPDATE serve.datum SET value=$1 WHERE id=$2",
            &[&original, &datum],
        )
        .await?;
    let asset: i64 = client
        .query_one(
            "SELECT id FROM serve.ma_tx_out WHERE id>=8::bigint*68719476736 ORDER BY id LIMIT 1",
            &[],
        )
        .await?
        .get(0);
    client
        .execute(
            "UPDATE serve.ma_tx_out SET id=id+1000 WHERE id=$1",
            &[&asset],
        )
        .await?;
    assert_ne!(
        after["content_digest"],
        db.cli("verify", &f.profile.network, &[]).await?["content_digest"]
    );
    drop(client);
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn snapshot_verify_named_corruptions() -> Result {
    let dir = tempfile::tempdir()?;
    let f = fixture(61);
    let source = Database::new().await?;
    let kept = populate(&source, &f).await?;
    let good = dir.path().join("verified.dsa");
    source
        .cli("snapshot", &f.profile.network, &file_args(&good))
        .await?;
    let ordinary = f
        .tags
        .iter()
        .filter(|(k, _)| k.starts_with("ordinary_"))
        .map(|(_, v)| *v)
        .find(|id| {
            kept["tx_out"].iter().any(|r| r["id"].as_i64() == Some(*id))
                && !f.chain.inputs.values().any(|i| i.output == *id)
        })
        .unwrap();
    let creating = kept["tx_out"][0]["tx_id"].as_i64().unwrap();
    let old_header = (0..f.bounds.horizon)
        .find(|h| {
            !kept["block"]
                .iter()
                .any(|b| b["id"].as_i64() == Some(i64::from(*h)))
        })
        .unwrap();
    let cases = [
        (
            format!(
                "DELETE FROM serve.ma_tx_out WHERE tx_out_id={ordinary};DELETE FROM serve.tx_out WHERE id={ordinary}"
            ),
            "supply_mismatch",
        ),
        (
            format!("DELETE FROM serve.tx WHERE id={creating}"),
            "referential_closure_failed",
        ),
        (
            format!("DELETE FROM serve.block WHERE id={}", f.bounds.target + 1),
            "header_gap",
        ),
        (
            format!(
                "INSERT INTO serve.block VALUES({old_header},decode(repeat('fe',32),'hex'),0,{old_header},{old_header},timestamp '2026-01-01',0)"
            ),
            "extra_old_header",
        ),
        ("TRUNCATE serve.tx_in".to_owned(), "empty_tx_in"),
        (
            "DROP INDEX serve.idx_tx_out_address".to_owned(),
            "required_index_missing",
        ),
        (
            format!(
                "INSERT INTO dsa.consumed_before_horizon VALUES({})",
                i64::from(f.bounds.target) * 16777216
            ),
            "invalid_consumed_before_horizon",
        ),
        ("INSERT INTO serve.tx_in SELECT tx_in_id*4096+4095,tx_in_id,tx_out_id,tx_out_index FROM serve.tx_in ORDER BY id LIMIT 1".to_owned(),"duplicate_natural_key"),
        ("INSERT INTO serve.ma_tx_out SELECT tx_out_id*4096+4095,quantity+1,tx_out_id,ident FROM serve.ma_tx_out ORDER BY id LIMIT 1".to_owned(),"duplicate_natural_key"),
    ];
    for (sql, expected) in cases {
        let db = Database::new().await?;
        db.cli("restore", &f.profile.network, &file_args(&good))
            .await?;
        let client = connect(&db.config).await?;
        client.batch_execute(&sql).await?;
        let result = db.cli_raw("verify", &f.profile.network, &[]).await?;
        assert!(!result.status.success(), "{expected}");
        let error = last_error(&result);
        assert_eq!(error["error"], expected, "{sql}");
        if expected == "supply_mismatch" {
            // The mismatch names the unit and both amounts.
            assert!(
                error["item"].as_str().is_some_and(|unit| unit.len() >= 56),
                "{error}"
            );
            assert!(
                error["expected"].is_string() && error["actual"].is_string(),
                "{error}"
            );
            assert_ne!(error["expected"], error["actual"]);
        }
        drop(client);
        db.cleanup().await?;
    }
    source.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn snapshot_sparse_spent_state_survives_restore_and_horizon_advances() -> Result {
    let dir = tempfile::tempdir()?;
    let mut f = fixture(71);
    f.bounds.horizon = 4;
    let source = Database::new().await?;
    populate(&source, &f).await?;
    let path = dir.path().join("carry.dsa");
    source
        .cli("snapshot", &f.profile.network, &file_args(&path))
        .await?;
    let db = Database::new().await?;
    db.cli("restore", &f.profile.network, &file_args(&path))
        .await?;
    let client = connect(&db.config).await?;
    let before = db.cli("verify", &f.profile.network, &[]).await?;
    let old_candidate = f.tags["permissioned_0"];
    assert!(
        client
            .query_opt(
                "SELECT 1 FROM dsa.consumed_before_horizon WHERE tx_out_id=$1",
                &[&old_candidate]
            )
            .await?
            .is_some()
    );
    assert!(
        client
            .query_opt(
                "SELECT 1 FROM serve.tx_in WHERE tx_out_id=$1",
                &[&(old_candidate / 4096)]
            )
            .await?
            .is_none()
    );
    // Each advance applies the pruned form to the restored store itself.
    let mut store = Store::connect(db.config.clone(), None).await?;
    for horizon in [6, 8] {
        let bounds = Bounds {
            horizon,
            ..f.bounds
        };
        store.advance_horizon(&f.profile, horizon).await?;
        let retained = rows(&db).await?;
        let expected = selected(&f.rows(), &keep(&f.chain, &f.profile, bounds)?);
        assert_eq!(retained, expected, "advance H{horizon}");
        let checked = db.cli("verify", &f.profile.network, &[]).await?;
        assert_eq!(before["supply"], checked["supply"]);
        assert!(
            !retained["tx_out"]
                .iter()
                .any(|r| r["id"].as_i64() == Some(old_candidate)),
            "old spent anchor resurrected"
        );
        let snapshot = dir.path().join(format!("advanced{horizon}.dsa"));
        db.cli("snapshot", &f.profile.network, &file_args(&snapshot))
            .await?;
        let restored = Database::new().await?;
        let report = restored
            .cli("restore", &f.profile.network, &file_args(&snapshot))
            .await?;
        assert_eq!(report["supply"], before["supply"]);
        assert_eq!(report["state_digest"], checked["state_digest"]);
        assert_eq!(rows(&restored).await?, retained);
        restored.cleanup().await?;
    }
    drop(client);
    source.cleanup().await?;
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn snapshot_repeatable_read_while_writer_commits() -> Result {
    let f = fixture(81);
    let db = Database::new().await?;
    populate(&db, &f).await?;
    let dir = tempfile::tempdir()?;
    let stop = Arc::new(AtomicBool::new(false));
    let commits = Arc::new(AtomicU64::new(0));
    let config = db.config.clone();
    let stopped = stop.clone();
    let count = commits.clone();
    let (started, ready) = oneshot::channel();
    let writer = tokio::spawn(async move {
        let mut store = Store::connect(config, None).await?;
        let mut started = Some(started);
        while !stopped.load(Ordering::Relaxed) {
            let tx = store.writer_transaction().await?;
            tx.batch_execute(
                "UPDATE serve.tx_out SET value=value+1 WHERE id=(SELECT min(id) FROM serve.tx_out)",
            )
            .await?;
            tx.commit().await?;
            count.fetch_add(1, Ordering::Relaxed);
            if let Some(started) = started.take() {
                let _ = started.send(());
            }
        }
        Ok::<(), Box<dyn StdError + Send + Sync>>(())
    });
    ready.await?;
    let initial = commits.load(Ordering::Relaxed);
    for index in 0..3 {
        let path = dir.path().join(format!("concurrent{index}.dsa"));
        let description = db
            .cli("snapshot", &f.profile.network, &file_args(&path))
            .await?;
        let restored = Database::new().await?;
        let report = restored
            .cli("restore", &f.profile.network, &file_args(&path))
            .await?;
        assert_eq!(
            report["content_digest"],
            description["verified"]["content_digest"]
        );
        restored.cleanup().await?;
    }
    stop.store(true, Ordering::Relaxed);
    writer.await??;
    assert!(
        commits.load(Ordering::Relaxed) > initial,
        "writer could not commit while snapshot ran"
    );
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn snapshot_lossless_structured_jsonb_and_flat_generations() -> Result {
    let f = fixture(91);
    let dir = tempfile::tempdir()?;
    let source = Database::new().await?;
    populate(&source, &f).await?;
    let wide = "340282366920938463463374607431768211456";
    // An ordinary constructor/list-shaped datum, about200JSON levels. This is
    // generated SQL content, not an imported third-party adversarial fixture.
    let mut datum = format!("{{\"int\":{wide}}}");
    for _ in 0..100 {
        datum = format!("{{\"constructor\":0,\"fields\":[{datum}]}}");
    }
    let client = connect(&source.config).await?;
    client.execute("UPDATE serve.datum SET value=$1::text::jsonb WHERE id=(SELECT min(id) FROM serve.datum)",&[&datum]).await?;
    client.execute("UPDATE serve.tx_metadata SET json=$1::text::jsonb WHERE id=(SELECT min(id) FROM serve.tx_metadata)",&[&datum]).await?;
    let before = source.cli("verify", &f.profile.network, &[]).await?;
    let mut current = source;
    let mut original_origin = None;
    let mut lengths = Vec::new();
    for generation in 0..4 {
        let file = dir.path().join(format!("generation{generation}.dsa"));
        let description = current
            .cli("snapshot", &f.profile.network, &file_args(&file))
            .await?;
        let mut origin = description["origin"].as_object().unwrap().clone();
        for key in ["kind", "snapshot_content_digest", "snapshot_state_digest"] {
            origin.remove(key);
        }
        assert!(
            origin.values().all(|v| !v.is_object() && !v.is_array()),
            "recursive provenance"
        );
        if let Some(original) = &original_origin {
            assert_eq!(&origin, original);
        } else {
            original_origin = Some(origin);
        }
        let length = serde_json::to_vec(&description["origin"])?.len();
        assert!(length < 2048);
        lengths.push(length);
        let dest = Database::new().await?;
        let restored = dest
            .cli("restore", &f.profile.network, &file_args(&file))
            .await?;
        assert_eq!(restored["content_digest"], before["content_digest"]);
        assert_eq!(restored["state_digest"], before["state_digest"]);
        let destination = connect(&dest.config).await?;
        let origin_client = connect(&current.config).await?;
        for (table, column) in [("datum", "value"), ("tx_metadata", "json")] {
            let query = format!("SELECT {column}::text FROM serve.{table} ORDER BY id");
            let expected = origin_client
                .query(&query, &[])
                .await?
                .into_iter()
                .map(|r| r.get::<_, Option<String>>(0))
                .collect::<Vec<_>>();
            assert!(expected.iter().flatten().any(|v| v.contains(wide)));
            let actual = destination
                .query(&query, &[])
                .await?
                .into_iter()
                .map(|r| r.get::<_, Option<String>>(0))
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
        }
        drop(origin_client);
        drop(destination);
        current.cleanup().await?;
        current = dest;
    }
    // Initial null links become one fixed pair of digests on the first restore;
    // generations thereafter have exactly constant flat provenance size.
    assert_eq!(lengths[1], lengths[2]);
    assert_eq!(lengths[2], lengths[3]);
    drop(client);
    current.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn snapshot_preflight_limits_lock_and_relative_paths() -> Result {
    let f = fixture(101);
    let dir = tempfile::tempdir()?;
    let source = Database::new().await?;
    populate(&source, &f).await?;
    source.admin.batch_execute(&format!("ALTER ROLE {} SET statement_timeout='60s';ALTER ROLE {} SET idle_in_transaction_session_timeout='60s'",source.owner,source.owner)).await?;
    let output = Command::new(env!("CARGO_BIN_EXE_db-sync-alt"))
        .args([
            "snapshot",
            "--database",
            &source.dsn(),
            "--network",
            &f.profile.network,
            "--file",
            "relative.dsa",
        ])
        .current_dir(dir.path())
        .output()
        .await?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let good = dir.path().join("relative.dsa");
    assert!(good.is_file());
    let description: Value = serde_json::from_slice(&output.stdout)?;
    let dest = Database::new().await?;
    dest.cli("create", &f.profile.network, &[]).await?;
    dest.admin.batch_execute(&format!("ALTER ROLE {} SET statement_timeout='60s';ALTER ROLE {} SET idle_in_transaction_session_timeout='60s'",dest.owner,dest.owner)).await?;
    let mut lock = Store::connect(dest.config.clone(), None).await?;
    lock.writer_transaction().await?.commit().await?;
    let busy = dest
        .cli_raw("restore", &f.profile.network, &file_args(&good))
        .await?;
    assert!(!busy.status.success());
    assert!(String::from_utf8(busy.stderr)?.contains("writer_busy"));
    lock.client()
        .batch_execute("SELECT pg_advisory_unlock_all()")
        .await?;
    drop(lock);
    let client = connect(&dest.config).await?;
    let before = database_state(&client).await?;
    let mut bytes = std::fs::read(&good)?;
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    let bad = dir.path().join("late-corruption.dsa");
    std::fs::write(&bad, &bytes)?;
    let rejected = dest
        .cli_raw("restore", &f.profile.network, &file_args(&bad))
        .await?;
    assert!(!rejected.status.success());
    assert_eq!(
        last_error(&rejected)["error"],
        "snapshot_section_hash_mismatch"
    );
    assert_eq!(
        database_state(&client).await?,
        before,
        "late corruption changed the database"
    );
    client.batch_execute("CREATE FUNCTION dsa.timeout_guard() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF current_setting('statement_timeout')<>'2min' OR current_setting('idle_in_transaction_session_timeout')<>'2min' THEN RAISE EXCEPTION 'explicit120s timeouts were not applied to this restore session';END IF;RETURN NEW;END$$;CREATE TRIGGER timeout_guard BEFORE INSERT ON dsa.supply FOR EACH ROW EXECUTE FUNCTION dsa.timeout_guard()").await?;
    let args = [
        "--file",
        good.to_str().unwrap(),
        "--statement-timeout-seconds",
        "120",
        "--idle-transaction-timeout-seconds",
        "120",
    ];
    let restored = dest.cli("restore", &f.profile.network, &args).await?;
    assert_eq!(
        restored["content_digest"],
        description["verified"]["content_digest"]
    );
    let settings=client.query_one("SELECT current_setting('statement_timeout'),current_setting('idle_in_transaction_session_timeout')",&[]).await?;
    assert_eq!(settings.get::<_, String>(0), "1min");
    assert_eq!(settings.get::<_, String>(1), "1min");
    drop(client);
    source.cleanup().await?;
    dest.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn snapshot_input_change_after_preflight_is_refused() -> Result {
    let f = fixture(111);
    let dir = tempfile::tempdir()?;
    let source = Database::new().await?;
    populate(&source, &f).await?;
    let file = dir.path().join("changing.dsa");
    source
        .cli("snapshot", &f.profile.network, &file_args(&file))
        .await?;
    let original = std::fs::read(&file)?;
    let dest = Database::new().await?;
    dest.cli("create", &f.profile.network, &[]).await?;
    let client = connect(&dest.config).await?;
    // Holds the build at its first lock, after the whole file was checked.
    let holder = connect(&dest.config).await?;
    holder
        .batch_execute("BEGIN;LOCK TABLE serve.block IN SHARE ROW EXCLUSIVE MODE")
        .await?;
    let mut command = Command::new(env!("CARGO_BIN_EXE_db-sync-alt"));
    command
        .args([
            "restore",
            "--database",
            &dest.dsn(),
            "--network",
            &f.profile.network,
        ])
        .args(file_args(&file));
    let restore = tokio::spawn(async move { command.output().await });
    let mut blocked = false;
    for _ in 0..500 {
        blocked=client.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=$1 AND application_name='db-sync-alt' AND wait_event_type='Lock' AND query LIKE 'LOCK TABLE%')",&[&dest.name]).await?.get(0);
        if blocked {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(blocked, "restore never reached its build lock");
    let mut changed = original.clone();
    let last = changed.len() - 1;
    changed[last] ^= 1;
    std::fs::write(&file, changed)?;
    holder.batch_execute("COMMIT").await?;
    let failed = restore.await??;
    assert!(
        !failed.status.success(),
        "accepted changed input after preflight"
    );
    let status = dest.cli("status", &f.profile.network, &[]).await?;
    assert_eq!(status["position"]["serving"], false);
    assert_eq!(
        client
            .query_one("SELECT count(*) FROM serve.block", &[])
            .await?
            .get::<_, i64>(0),
        0
    );
    assert_eq!(
        last_error(&failed)["error"],
        "snapshot_section_hash_mismatch"
    );
    std::fs::write(&file, original)?;
    dest.cli("restore", &f.profile.network, &file_args(&file))
        .await?;
    drop((client, holder));
    source.cleanup().await?;
    dest.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL 17"]
async fn restore_load_failure_names_its_cause_and_retry_succeeds() -> Result {
    let mut f = fixture(39);
    f.bounds.horizon = 6;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("copy-error.dsa");
    let source = Database::new().await?;
    populate(&source, &f).await?;
    let description = source
        .cli("snapshot", &f.profile.network, &file_args(&path))
        .await?;
    assert!(description["verified"]["protected_state"]["rows"].as_u64() > Some(0));
    let dest = Database::new().await?;
    dest.cli("create", &f.profile.network, &[]).await?;
    let client = connect(&dest.config).await?;
    let before = database_state(&client).await?;
    // The marker section loads into a table the restore keeps; a trigger on it
    // makes the load fail inside the build transaction.
    client.batch_execute("CREATE FUNCTION dsa.copy_error_guard() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='generated load failure';END$$;CREATE TRIGGER copy_error_guard BEFORE INSERT ON dsa.consumed_before_horizon FOR EACH ROW EXECUTE FUNCTION dsa.copy_error_guard()").await?;
    let guarded = database_state(&client).await?;
    let failed = dest
        .cli_raw("restore", &f.profile.network, &file_args(&path))
        .await?;
    assert_eq!(failed.status.code(), Some(1));
    let report = last_error(&failed);
    assert_eq!(report["error"], "database_error", "{report}");
    assert_eq!(report["sqlstate"], "P0001");
    assert_eq!(report["message"], "generated load failure");
    assert_eq!(report["family"], "consumed_before_horizon");
    let stderr = String::from_utf8(failed.stderr)?;
    assert!(
        !stderr.contains("password") && !stderr.contains("host="),
        "{stderr}"
    );
    // The build rolled back: the same empty layout, readers closed.
    let mut after = database_state(&client).await?;
    after["position"]["updated_at"] = guarded["position"]["updated_at"].clone();
    after["position"]["reason"] = guarded["position"]["reason"].clone();
    after["identity"]["origin"] = guarded["identity"]["origin"].clone();
    assert_eq!(after, guarded);
    assert_eq!(
        client
            .query("SELECT * FROM public.block", &[])
            .await
            .unwrap_err()
            .code()
            .map(|c| c.code()),
        Some("55000")
    );
    client
        .batch_execute("DROP TRIGGER copy_error_guard ON dsa.consumed_before_horizon;DROP FUNCTION dsa.copy_error_guard()")
        .await?;
    assert_eq!(
        database_state(&client).await?["relations"],
        before["relations"]
    );
    let restored = dest
        .cli("restore", &f.profile.network, &file_args(&path))
        .await?;
    for key in ["content_digest", "state_digest"] {
        assert_eq!(restored[key], description["verified"][key]);
    }
    drop(client);
    source.cleanup().await?;
    dest.cleanup().await?;
    Ok(())
}

fn node_index_names() -> Vec<String> {
    let mut names = include_str!("../../dsa-store/src/indexes.sql")
        .lines()
        .filter(|line| line.starts_with("CREATE"))
        .map(|line| {
            line.split("INDEX ")
                .nth(1)
                .and_then(|rest| rest.split_whitespace().next())
                .unwrap()
                .to_owned()
        })
        .collect::<Vec<_>>();
    names.extend(TABLES.iter().map(|table| format!("{table}_pkey")));
    names
}

/// The node creates an index when `pg_indexes` does not hold exactly one row
/// with its name, anywhere in the database.
async fn one_index_row_per_name(client: &Client) -> Result {
    let names = node_index_names();
    for row in client
        .query(
            "SELECT n,(SELECT count(*) FROM pg_indexes WHERE indexname=n) FROM unnest($1::text[]) n",
            &[&names],
        )
        .await?
    {
        assert_eq!(
            row.get::<_, i64>(1),
            1,
            "pg_indexes rows for {}",
            row.get::<_, String>(0)
        );
    }
    Ok(())
}

fn latest_block_sql() -> String {
    let catalogue: Value =
        serde_json::from_str(include_str!("../../dsa-store/catalogue.json")).unwrap();
    catalogue["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["name"] == "get_latest_block_info" && entry["kind"] == "read")
        .unwrap()["sql"]
        .as_str()
        .unwrap()
        .to_owned()
}

async fn closed_for_node(
    reader: &Client,
    literal: &str,
    prepared: &tokio_postgres::Statement,
) -> Result {
    for table in TABLES {
        let error = reader
            .query(&format!("SELECT * FROM public.{table}"), &[])
            .await
            .unwrap_err();
        assert_eq!(error.code().map(|c| c.code()), Some("55000"), "{table}");
    }
    for error in [
        reader.query(literal, &[]).await.unwrap_err(),
        reader.query(prepared, &[]).await.unwrap_err(),
    ] {
        assert_eq!(error.code().map(|c| c.code()), Some("55000"));
    }
    Ok(())
}

async fn wait_until(client: &Client, sql: &str, what: &str) -> Result {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
    while !client.query_one(sql, &[]).await?.get::<_, bool>(0) {
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("timed out waiting for {what}").into());
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    Ok(())
}

fn restore_command(db: &Database, network: &str, path: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_db-sync-alt"));
    command
        .args(["restore", "--database", &db.dsn(), "--network", network])
        .args(file_args(path))
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    command
}

fn last_error(output: &std::process::Output) -> Value {
    String::from_utf8_lossy(&output.stderr)
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .rfind(|value| value.get("error").is_some())
        .unwrap_or(Value::Null)
}

async fn one_layout(client: &Client) -> Result {
    let row = client
        .query_one(
            "SELECT to_regnamespace('dsa_retired') IS NULL,NOT EXISTS(SELECT 1 FROM pg_inherits)",
            &[],
        )
        .await?;
    assert!(row.get::<_, bool>(0), "a second storage schema exists");
    assert!(row.get::<_, bool>(1), "inherited storage exists");
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn restore_publishes_by_view_switch_while_readers_see_55000() -> Result {
    let f = fixture(301);
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("switch.dsa");
    let source = Database::new().await?;
    populate(&source, &f).await?;
    let description = source
        .cli("snapshot", &f.profile.network, &file_args(&path))
        .await?;
    let dest = Database::new().await?;
    dest.cli("create", &f.profile.network, &[]).await?;
    dest.cli(
        "grant-reader",
        &f.profile.network,
        &["--role", &dest.reader],
    )
    .await?;
    let owner = connect(&dest.config).await?;
    // Holds the build transaction open at its commit, after every load step.
    owner.batch_execute("CREATE FUNCTION dsa.test_build_barrier() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM pg_advisory_xact_lock(901,1);RETURN NEW;END$$;REVOKE ALL ON FUNCTION dsa.test_build_barrier() FROM PUBLIC;CREATE CONSTRAINT TRIGGER test_build_barrier AFTER UPDATE ON dsa.position DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN(OLD.horizon_height IS NULL AND NEW.horizon_height IS NOT NULL AND NOT NEW.serving) EXECUTE FUNCTION dsa.test_build_barrier();SELECT pg_advisory_lock(901,1)").await?;
    let mut reader_config = dest.config.clone();
    reader_config
        .user(&dest.reader)
        .options("-c default_transaction_read_only=on -c statement_timeout=5s");
    let reader = connect(&reader_config).await?;
    let literal = latest_block_sql();
    let prepared = reader.prepare(&literal).await?;
    closed_for_node(&reader, &literal, &prepared).await?;
    one_index_row_per_name(&reader).await?;
    let stop = Arc::new(AtomicBool::new(false));
    let prober = {
        let stop = stop.clone();
        let config = reader_config.clone();
        let literal = literal.clone();
        tokio::spawn(async move {
            let client = connect(&config).await?;
            let (mut closed, mut serving) = (0_u64, 0_u64);
            while !stop.load(Ordering::Relaxed) {
                match client.query(&literal, &[]).await {
                    Ok(rows) => {
                        assert!(!rows.is_empty(), "empty answer during restore");
                        serving += 1;
                    }
                    Err(error) => {
                        assert_eq!(error.code().map(|c| c.code()), Some("55000"), "{error}");
                        closed += 1;
                    }
                }
                one_index_row_per_name(&client).await?;
            }
            Ok::<_, Box<dyn StdError + Send + Sync>>((closed, serving))
        })
    };
    let started: String = owner
        .query_one("SELECT clock_timestamp()::text", &[])
        .await?
        .get(0);
    let mut child = restore_command(&dest, &f.profile.network, &path).spawn()?;
    wait_until(&dest.admin, &format!("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname='{}' AND application_name='db-sync-alt' AND wait_event='advisory' AND query LIKE 'COMMIT%')", dest.name), "the build commit").await?;
    closed_for_node(&reader, &literal, &prepared).await?;
    one_index_row_per_name(&reader).await?;
    // A reader in an open transaction that touched a view delays publication.
    let holder = connect(&reader_config).await?;
    holder
        .batch_execute("BEGIN;DECLARE held CURSOR FOR SELECT * FROM public.block")
        .await?;
    owner
        .batch_execute("SELECT pg_advisory_unlock(901,1)")
        .await?;
    wait_until(
        &owner,
        "SELECT to_regnamespace('dsa_retired') IS NOT NULL",
        "the committed build",
    )
    .await?;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(
        child.try_wait()?.is_none(),
        "publication did not wait for the reader's lock"
    );
    closed_for_node(&reader, &literal, &prepared).await?;
    one_index_row_per_name(&reader).await?;
    holder.batch_execute("COMMIT").await?;
    let output = child.wait_with_output().await?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout)?;
    for key in [
        "content_digest",
        "state_digest",
        "families",
        "state",
        "supply",
    ] {
        assert_eq!(report[key], description["verified"][key], "{key}");
    }
    assert!(!reader.query(&literal, &[]).await?.is_empty());
    assert!(!reader.query(&prepared, &[]).await?.is_empty());
    assert!(
        holder
            .query_one("SELECT count(*) FROM public.block", &[])
            .await?
            .get::<_, i64>(0)
            > 0
    );
    stop.store(true, Ordering::Relaxed);
    let (closed, serving) = prober.await??;
    assert!(closed > 0 && serving > 0, "{closed}/{serving}");
    one_layout(&owner).await?;
    one_index_row_per_name(&owner).await?;
    let fresh:bool=owner.query_one("SELECT count(*)=(SELECT target_height-horizon_height+1 FROM dsa.position) AND bool_and(first_served_at>=$1::text::timestamptz) FROM dsa.first_served",&[&started]).await?.get(0);
    assert!(fresh, "first-served rows are not the publication time");
    assert_eq!(rows(&source).await?, rows(&dest).await?);
    drop((reader, holder, owner));
    source.cleanup().await?;
    dest.cleanup().await?;
    Ok(())
}

async fn database_state(client: &Client) -> Result<Value> {
    let mut state:Value=client.query_one("SELECT jsonb_build_object('namespaces',(SELECT jsonb_agg(nspname ORDER BY nspname) FROM pg_namespace WHERE nspname NOT LIKE 'pg\\_%' AND nspname<>'information_schema'),'relations',(SELECT coalesce(jsonb_agg(jsonb_build_array(n.nspname,c.relname,c.relkind,c.oid::bigint,pg_relation_filenode(c.oid)::bigint,c.relacl::text) ORDER BY n.nspname,c.relname),'[]') FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname NOT LIKE 'pg\\_%' AND n.nspname<>'information_schema'))",&[]).await?.get(0);
    if client
        .query_one("SELECT to_regclass('dsa.identity') IS NOT NULL", &[])
        .await?
        .get::<_, bool>(0)
    {
        state["identity"] = client
            .query_one("SELECT to_jsonb(i) FROM dsa.identity i", &[])
            .await?
            .get(0);
        state["position"] = client
            .query_one("SELECT to_jsonb(p) FROM dsa.position p", &[])
            .await?
            .get(0);
    }
    Ok(state)
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn restore_refusals_leave_database_unchanged_and_interrupted_publication_retries() -> Result {
    let dir = tempfile::tempdir()?;
    let mut sources = Vec::new();
    let mut files = Vec::new();
    for seed in [311, 313, 312] {
        let f = fixture(seed);
        let source = Database::new().await?;
        populate(&source, &f).await?;
        let path = dir.path().join(format!("seed{seed}.dsa"));
        let description = source
            .cli("snapshot", &f.profile.network, &file_args(&path))
            .await?;
        sources.push((f, source));
        files.push((path, description));
    }
    let network = sources[0].0.profile.network.clone();
    assert_eq!(sources[1].0.profile.network, network);
    assert_ne!(sources[2].0.profile.network, network);
    let good = files[0].0.clone();
    let bytes = std::fs::read(&good)?;
    let mut format = bytes.clone();
    format[11] = 2;
    let format_file = dir.path().join("format.dsa");
    std::fs::write(&format_file, format)?;
    let rule_file = dir.path().join("rule.dsa");
    std::fs::write(
        &rule_file,
        rewrite_description(&bytes, |v| v["keep_rule_version"] = json!(1)),
    )?;
    let missing = dir.path().join("missing.dsa");
    let zeros = "00".repeat(32);
    let cases: [(Vec<&str>, &str); 5] = [
        (file_args(&missing).to_vec(), "snapshot_io_error"),
        (file_args(&files[2].0).to_vec(), "network_mismatch"),
        (
            file_args(&format_file).to_vec(),
            "unsupported_snapshot_format",
        ),
        (file_args(&rule_file).to_vec(), "keep_rule_version_mismatch"),
        (
            [
                file_args(&good).as_slice(),
                &["--expect-content-digest", &zeros],
            ]
            .concat(),
            "snapshot_content_digest_unexpected",
        ),
    ];
    for created in [false, true] {
        let dest = Database::new().await?;
        if created {
            dest.cli("create", &network, &[]).await?;
            dest.cli("grant-reader", &network, &["--role", &dest.reader])
                .await?;
        }
        let client = connect(&dest.config).await?;
        let before = database_state(&client).await?;
        for (args, expected) in &cases {
            let output = dest.cli_raw("restore", &network, args).await?;
            assert!(!output.status.success(), "accepted {expected}");
            assert_eq!(last_error(&output)["error"], *expected, "{args:?}");
            assert_eq!(
                database_state(&client).await?,
                before,
                "{expected} changed the database"
            );
        }
        let expected = files[0].1["verified"]["content_digest"]
            .as_str()
            .unwrap()
            .to_owned();
        let restored = dest
            .cli(
                "restore",
                &network,
                &[
                    file_args(&good).as_slice(),
                    &["--expect-content-digest", &expected],
                ]
                .concat(),
            )
            .await?;
        assert_eq!(restored["content_digest"], json!(expected));
        drop(client);
        dest.cleanup().await?;
    }
    // Interrupted after the build commit and before publication.
    for retry in [0, 1] {
        let dest = Database::new().await?;
        dest.cli("create", &network, &[]).await?;
        dest.cli("grant-reader", &network, &["--role", &dest.reader])
            .await?;
        let owner = connect(&dest.config).await?;
        let mut reader_config = dest.config.clone();
        reader_config.user(&dest.reader);
        let holder = connect(&reader_config).await?;
        holder
            .batch_execute("BEGIN;DECLARE held CURSOR FOR SELECT * FROM public.block")
            .await?;
        let child = restore_command(&dest, &network, &good).spawn()?;
        let pid = child.id().ok_or("restore exited")?.to_string();
        wait_until(
            &owner,
            "SELECT to_regnamespace('dsa_retired') IS NOT NULL",
            "the committed build",
        )
        .await?;
        assert!(
            Command::new("kill")
                .args(["-TERM", &pid])
                .status()
                .await?
                .success()
        );
        let output = child.wait_with_output().await?;
        assert!(!output.status.success());
        let report = last_error(&output);
        assert_eq!(report["error"], "operation_interrupted", "{report}");
        assert_eq!(report["published"], false, "{report}");
        holder.batch_execute("COMMIT").await?;
        let reader = connect(&reader_config).await?;
        assert_eq!(
            reader
                .query("SELECT * FROM public.block", &[])
                .await
                .unwrap_err()
                .code()
                .map(|c| c.code()),
            Some("55000")
        );
        assert!(
            owner
                .query_one("SELECT to_regnamespace('dsa_retired') IS NOT NULL", &[])
                .await?
                .get::<_, bool>(0)
        );
        let (file, description) = &files[retry];
        let restored = dest.cli("restore", &network, &file_args(file)).await?;
        for key in [
            "content_digest",
            "state_digest",
            "families",
            "state",
            "supply",
        ] {
            assert_eq!(restored[key], description["verified"][key], "{key}");
        }
        one_layout(&owner).await?;
        one_index_row_per_name(&owner).await?;
        assert!(
            !reader
                .query("SELECT * FROM public.block", &[])
                .await?
                .is_empty()
        );
        assert_eq!(rows(&sources[retry].1).await?, rows(&dest).await?);
        drop((reader, holder, owner));
        dest.cleanup().await?;
    }
    for (_, source) in sources {
        source.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn restored_database_survives_rename_and_owner_change() -> Result {
    let f = fixture(321);
    let dir = tempfile::tempdir()?;
    let source = Database::new().await?;
    populate(&source, &f).await?;
    let path = dir.path().join("rename.dsa");
    let description = source
        .cli("snapshot", &f.profile.network, &file_args(&path))
        .await?;
    let mut dest = Database::new().await?;
    dest.cli("restore", &f.profile.network, &file_args(&path))
        .await?;
    dest.cli(
        "grant-reader",
        &f.profile.network,
        &["--role", &dest.reader],
    )
    .await?;
    let renamed = format!("{}_renamed", dest.name);
    dest.admin
        .batch_execute(&format!("ALTER DATABASE {} RENAME TO {renamed}", dest.name))
        .await?;
    dest.name = renamed.clone();
    dest.config.dbname(&renamed);
    let digest = &description["verified"]["content_digest"];
    assert_eq!(
        dest.cli("verify", &f.profile.network, &[]).await?["content_digest"],
        *digest
    );
    let after_rename = dir.path().join("after-rename.dsa");
    let snapshot = dest
        .cli("snapshot", &f.profile.network, &file_args(&after_rename))
        .await?;
    assert_eq!(snapshot["verified"]["content_digest"], *digest);
    let new_owner = format!("{}_next", dest.owner);
    dest.admin.batch_execute(&format!("CREATE ROLE {new_owner} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS")).await?;
    let admin_user: String = dest
        .admin
        .query_one("SELECT current_user", &[])
        .await?
        .get(0);
    let mut inside = dest.config.clone();
    inside.user(&admin_user);
    connect(&inside)
        .await?
        .batch_execute(&format!(
            "REASSIGN OWNED BY {} TO {new_owner};ALTER DATABASE {renamed} OWNER TO {new_owner}",
            dest.owner
        ))
        .await?;
    let old_owner = std::mem::replace(&mut dest.owner, new_owner.clone());
    dest.config.user(&new_owner);
    assert_eq!(
        dest.cli("verify", &f.profile.network, &[]).await?["content_digest"],
        *digest
    );
    let after_owner = dir.path().join("after-owner.dsa");
    let snapshot = dest
        .cli("snapshot", &f.profile.network, &file_args(&after_owner))
        .await?;
    assert_eq!(snapshot["verified"]["content_digest"], *digest);
    dest.cli(
        "grant-reader",
        &f.profile.network,
        &["--role", &dest.reader, "--check"],
    )
    .await?;
    let copy = Database::new().await?;
    let restored = copy
        .cli("restore", &f.profile.network, &file_args(&after_owner))
        .await?;
    assert_eq!(restored["content_digest"], *digest);
    copy.cleanup().await?;
    dest.cleanup().await?;
    source
        .admin
        .batch_execute(&format!("DROP ROLE {old_owner}"))
        .await?;
    source.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn verify_refuses_rows_outside_the_keep_rule_and_missing_coverage() -> Result {
    let mut f = fixture(331);
    f.bounds.horizon = 6;
    let dir = tempfile::tempdir()?;
    let source = Database::new().await?;
    populate(&source, &f).await?;
    let path = dir.path().join("exact.dsa");
    source
        .cli("snapshot", &f.profile.network, &file_args(&path))
        .await?;
    let client = connect(&source.config).await?;
    let old_tx: i64 = client
        .query_one(
            "SELECT min(id) FROM serve.tx WHERE block_id<(SELECT horizon_height FROM dsa.position)",
            &[],
        )
        .await?
        .get(0);
    let marked = client
        .query_one("SELECT o.tx_id,o.index::integer FROM dsa.consumed_before_horizon c JOIN serve.tx_out o ON o.id=c.tx_out_id ORDER BY c.tx_out_id LIMIT 1", &[])
        .await?;
    let (marked_tx, marked_index): (i64, i32) = (marked.get(0), marked.get(1));
    let pool: i64 = client
        .query_one("SELECT min(id) FROM serve.pool_hash", &[])
        .await?
        .get(0);
    let epoch: i32 = client
        .query_one(
            "SELECT epoch_no-3 FROM serve.block WHERE id=(SELECT horizon_height FROM dsa.position)",
            &[],
        )
        .await?
        .get(0);
    assert!(epoch >= 0);
    drop(client);
    let block = old_tx / 4096;
    let cases = [
        (
            format!("INSERT INTO serve.tx VALUES({block}*4096+4000,decode(repeat('5a',32),'hex'),{block},4000,true)"),
            "row_outside_keep_rule",
            Some("tx"),
        ),
        (
            format!("INSERT INTO serve.tx_out VALUES({old_tx}*4096+4000,{old_tx},4000,'generated_old_spent',1,NULL,NULL);INSERT INTO dsa.consumed_before_horizon VALUES({old_tx}*4096+4000)"),
            "row_outside_keep_rule",
            Some("tx_out"),
        ),
        (
            format!("INSERT INTO serve.tx_in VALUES({old_tx}*4096+4000,{old_tx},{marked_tx},{marked_index})"),
            "row_outside_keep_rule",
            Some("tx_in"),
        ),
        (
            format!("INSERT INTO serve.tx_metadata VALUES({old_tx},6500973,{old_tx},'{{}}')"),
            "row_outside_keep_rule",
            Some("tx_metadata"),
        ),
        (
            format!("INSERT INTO serve.epoch_param VALUES({epoch},decode(repeat('5b',32),'hex'),16384)"),
            "row_outside_keep_rule",
            Some("epoch_param"),
        ),
        (
            format!("INSERT INTO serve.epoch_stake VALUES(987654,{pool},1,{epoch})"),
            "row_outside_keep_rule",
            Some("epoch_stake"),
        ),
        (
            "DELETE FROM dsa.first_served WHERE height=(SELECT horizon_height FROM dsa.position)".to_owned(),
            "first_served_incomplete",
            None,
        ),
        (
            "DELETE FROM serve.epoch_param WHERE epoch_no=(SELECT epoch_no FROM serve.block WHERE id=(SELECT target_height FROM dsa.position))".to_owned(),
            "epoch_param_missing",
            None,
        ),
    ];
    for (sql, expected, family) in cases {
        let db = Database::new().await?;
        db.cli("restore", &f.profile.network, &file_args(&path))
            .await?;
        db.cli("verify", &f.profile.network, &[]).await?;
        let client = connect(&db.config).await?;
        client.batch_execute(&sql).await?;
        let result = db.cli_raw("verify", &f.profile.network, &[]).await?;
        assert!(!result.status.success(), "{expected}: {sql}");
        let error = last_error(&result);
        assert_eq!(error["error"], expected, "{sql}");
        if let Some(family) = family {
            assert_eq!(error["family"], family, "{sql}");
        }
        drop(client);
        db.cleanup().await?;
    }
    source.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn restore_rechecks_the_empty_layout_under_its_locks() -> Result {
    let f = fixture(341);
    let dir = tempfile::tempdir()?;
    let source = Database::new().await?;
    populate(&source, &f).await?;
    let path = dir.path().join("recheck.dsa");
    source
        .cli("snapshot", &f.profile.network, &file_args(&path))
        .await?;
    // An object of its own in `serve` makes the database occupied.
    let dest = Database::new().await?;
    dest.cli("create", &f.profile.network, &[]).await?;
    let client = connect(&dest.config).await?;
    client
        .batch_execute("CREATE TABLE serve.idx_multi_asset_policy_name_hex_extra(id bigint)")
        .await?;
    let before = database_state(&client).await?;
    let refused = dest
        .cli_raw("restore", &f.profile.network, &file_args(&path))
        .await?;
    assert_eq!(last_error(&refused)["error"], "destination_not_empty");
    assert_eq!(database_state(&client).await?, before);
    drop(client);
    dest.cleanup().await?;
    // A row committed while the restore waits for its lock is found.
    let dest = Database::new().await?;
    dest.cli("create", &f.profile.network, &[]).await?;
    let holder = connect(&dest.config).await?;
    holder
        .batch_execute("BEGIN;LOCK TABLE serve.block IN SHARE ROW EXCLUSIVE MODE")
        .await?;
    let child = restore_command(&dest, &f.profile.network, &path).spawn()?;
    wait_until(&dest.admin, &format!("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname='{}' AND application_name='db-sync-alt' AND wait_event_type='Lock' AND query LIKE 'LOCK TABLE%')", dest.name), "the build lock").await?;
    holder.batch_execute("INSERT INTO serve.block VALUES(1,decode(repeat('ab',32),'hex'),0,1,1,timestamp '2026-01-01',0);COMMIT").await?;
    let output = child.wait_with_output().await?;
    assert_eq!(last_error(&output)["error"], "destination_not_empty");
    assert_eq!(
        holder
            .query_one("SELECT count(*) FROM serve.block", &[])
            .await?
            .get::<_, i64>(0),
        1
    );
    assert_eq!(
        dest.cli("status", &f.profile.network, &[]).await?["position"]["serving"],
        false
    );
    drop(holder);
    dest.cleanup().await?;
    source.cleanup().await?;
    Ok(())
}
