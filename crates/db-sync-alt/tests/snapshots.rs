//! Generated-only PG17/CLI snapshot proofs. No operator source or provider.
#[path = "keep_rule/fixtures.rs"]
#[allow(dead_code)]
mod fixtures;

use dsa_chain::{
    keep::{Bounds, Header, TABLES, keep},
    profiles::Profile,
};
use dsa_store::{Store, keep::Selection};
use fixtures::{Fixture, Rows, selected};
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
    // preserve them, regardless of Q18's future source projection decision.
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
        .values()
        .map(|a| a.unit())
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
        rewrite_description(&bytes, |v| v["keep_rule_version"] = json!(999)),
    ));
    cases.push((
        "content",
        rewrite_description(&bytes, |v| {
            v["verified"]["content_digest"] = json!("00".repeat(32))
        }),
    ));
    let mut trailing = bytes.clone();
    trailing.push(0);
    cases.push(("trailing", trailing));
    let dest = Database::new().await?;
    for (name, bytes) in cases {
        let bad = dir.path().join(format!("bad-{name}.dsa"));
        std::fs::write(&bad, bytes)?;
        let output = dest
            .cli_raw("restore", &f.profile.network, &file_args(&bad))
            .await?;
        assert!(!output.status.success(), "accepted {name}");
        let state = dest.cli("status", &f.profile.network, &[]).await?;
        assert_eq!(state["position"]["serving"], false);
        assert_eq!(state["position"]["reason"], "restore_failed");
        let client = connect(&dest.config).await?;
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
    }
    dest.cli("restore", &f.profile.network, &file_args(&good))
        .await?;
    let before = dest.cli("verify", &f.profile.network, &[]).await?;
    assert!(
        !dest
            .cli_raw("restore", &f.profile.network, &file_args(&good))
            .await?
            .status
            .success()
    );
    let after = dest.cli("verify", &f.profile.network, &[]).await?;
    assert_eq!(before["content_digest"], after["content_digest"]);
    assert_eq!(rows(&source).await?, rows(&dest).await?);
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
    let output: i64 = client
        .query_one(
            "SELECT id FROM serve.tx_out WHERE id>=8*16777216 ORDER BY id LIMIT 1",
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
            format!("DELETE FROM serve.block WHERE id={}", f.bounds.horizon + 1),
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
    ];
    for (sql, expected) in cases {
        let db = Database::new().await?;
        db.cli("restore", &f.profile.network, &file_args(&good))
            .await?;
        let client = connect(&db.config).await?;
        client.batch_execute(&sql).await?;
        let result = db.cli_raw("verify", &f.profile.network, &[]).await?;
        assert!(!result.status.success(), "{expected}");
        let lines = String::from_utf8(result.stderr)?;
        let error: Value = serde_json::from_str(lines.lines().last().unwrap())?;
        assert_eq!(error["error"], expected, "{sql}");
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
    // The production owned-store selection is distinct from complete-history
    // A3 selection. Before each simulated expiry capture newly old spend bits;
    // actual follower mutation/expiry implementation belongs to B4.
    for horizon in [6, 8] {
        let bounds = Bounds {
            horizon,
            ..f.bounds
        };
        let selection = Selection::new(&f.profile, bounds)?;
        client.execute("INSERT INTO dsa.consumed_before_horizon SELECT DISTINCT o.id FROM serve.tx_out o JOIN serve.tx_in i ON i.tx_out_id=o.tx_id AND i.tx_out_index=o.index WHERE i.id<$1::bigint*16777216 AND o.id<$1::bigint*16777216 ON CONFLICT DO NOTHING",&[&i64::from(horizon)]).await?;
        let mut retained = Rows::new();
        for table in TABLES {
            let stream = client
                .query(
                    &format!(
                        "SELECT to_jsonb(t) FROM ({}) t",
                        selection.pruned_rows_sql(table)?
                    ),
                    &selection.parameters(),
                )
                .await?;
            retained.insert(
                table.to_owned(),
                stream.into_iter().map(|r| r.get(0)).collect(),
            );
        }
        let expected = selected(&f.rows(), &keep(&f.chain, &f.profile, bounds)?);
        assert_eq!(retained, expected, "advance H{horizon}");
        let active = retained["tx_out"]
            .iter()
            .map(|r| r["id"].as_i64().unwrap())
            .collect::<Vec<_>>();
        client
            .execute(
                "DELETE FROM dsa.consumed_before_horizon WHERE NOT(tx_out_id=ANY($1))",
                &[&active],
            )
            .await?;
        client
            .batch_execute(&format!(
                "TRUNCATE {}",
                TABLES
                    .iter()
                    .map(|t| format!("serve.{t}"))
                    .collect::<Vec<_>>()
                    .join(",")
            ))
            .await?;
        load(&client, &retained).await?;
        position(&client, bounds).await?;
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
