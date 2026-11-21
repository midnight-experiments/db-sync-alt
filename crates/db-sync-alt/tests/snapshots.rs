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
        rewrite_description(&bytes, |v| v["keep_rule_version"] = json!(1)),
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
    client.batch_execute("CREATE FUNCTION serve.preflight_guard() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'COPY started before preflight rejected the last section';END$$;CREATE TRIGGER preflight_guard BEFORE INSERT ON serve.block FOR EACH ROW EXECUTE FUNCTION serve.preflight_guard()").await?;
    let mut bytes = std::fs::read(&good)?;
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    let bad = dir.path().join("late-corruption.dsa");
    std::fs::write(&bad, &bytes)?;
    let rejected = dest
        .cli_raw("restore", &f.profile.network, &file_args(&bad))
        .await?;
    assert!(!rejected.status.success());
    assert!(
        String::from_utf8(rejected.stderr)?.contains("snapshot_section_hash_mismatch"),
        "late corruption reached COPY trigger"
    );
    client.batch_execute("DROP TRIGGER preflight_guard ON serve.block;DROP FUNCTION serve.preflight_guard();CREATE FUNCTION serve.timeout_guard() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF current_setting('statement_timeout')<>'2min' OR current_setting('idle_in_transaction_session_timeout')<>'2min' THEN RAISE EXCEPTION 'explicit120s timeouts were not applied to this restore session';END IF;RETURN NEW;END$$;CREATE TRIGGER timeout_guard BEFORE INSERT ON serve.block FOR EACH ROW EXECUTE FUNCTION serve.timeout_guard()").await?;
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
    client.batch_execute("CREATE FUNCTION serve.copy_barrier() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM pg_advisory_xact_lock(724,1);RETURN NEW;END$$;CREATE TRIGGER copy_barrier BEFORE INSERT ON serve.block FOR EACH ROW EXECUTE FUNCTION serve.copy_barrier();SELECT pg_advisory_lock(724,1)").await?;
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
        blocked=client.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=$1 AND application_name='db-sync-alt' AND wait_event='advisory')",&[&dest.name]).await?.get(0);
        if blocked {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    // This barrier is inside the first COPY: all preflight hashes are complete,
    // but the later internal-state section has not been opened for loading.
    assert!(
        blocked,
        "restore never reached its post-preflight COPY barrier"
    );
    let mut changed = original.clone();
    let last = changed.len() - 1;
    changed[last] ^= 1;
    std::fs::write(&file, changed)?;
    client
        .batch_execute("SELECT pg_advisory_unlock_all()")
        .await?;
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
    std::fs::write(&file, original)?;
    client
        .batch_execute(
            "DROP TRIGGER copy_barrier ON serve.block;DROP FUNCTION serve.copy_barrier()",
        )
        .await?;
    dest.cli("restore", &f.profile.network, &file_args(&file))
        .await?;
    drop(client);
    source.cleanup().await?;
    dest.cleanup().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL 17"]
async fn restore_copy_sqlstate_privacy_and_pipeline_timings() -> Result {
    let f = fixture(39);
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("copy-error.dsa");
    let source = Database::new().await?;
    populate(&source, &f).await?;
    let description = source
        .cli("snapshot", &f.profile.network, &file_args(&path))
        .await?;
    let dest = Database::new().await?;
    dest.cli("create", &f.profile.network, &[]).await?;
    let client = connect(&dest.config).await?;
    let original_storage: i64 = client
        .query_one(
            "SELECT pg_relation_filenode('serve.block'::regclass)::bigint",
            &[],
        )
        .await?
        .get(0);
    client.batch_execute("CREATE FUNCTION serve.copy_error_guard() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='generated-private-message-marker',DETAIL='generated-private-detail-marker',HINT='generated-private-hint-marker';END$$;CREATE TRIGGER copy_error_guard BEFORE INSERT ON serve.block FOR EACH ROW EXECUTE FUNCTION serve.copy_error_guard()").await?;
    let failed = dest
        .cli_raw("restore", &f.profile.network, &file_args(&path))
        .await?;
    assert_eq!(failed.status.code(), Some(1));
    let stderr = String::from_utf8(failed.stderr)?;
    let report = stderr
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|value| value["error"] == "database_error")
        .ok_or("missing database error report")?;
    assert_eq!(report["sqlstate"], "P0001");
    assert_eq!(report["family"], "block");
    assert!(matches!(
        report["stage"].as_str(),
        Some("restore_copy_send" | "restore_copy_finish")
    ));
    assert!(
        report["elapsed_seconds"]
            .as_f64()
            .is_some_and(|n| n.is_finite() && n >= 0.0)
    );
    assert!(
        report["wait_seconds"]
            .as_f64()
            .is_some_and(|n| n.is_finite() && n >= 0.0)
    );
    assert!(report["queued_plain_bytes"].is_u64());
    for private in [
        "generated-private-message-marker",
        "generated-private-detail-marker",
        "generated-private-hint-marker",
        "copy_error_guard",
        "RAISE EXCEPTION",
    ] {
        assert!(
            !stderr.contains(private),
            "server context leaked: {private}"
        );
    }
    let closed = dest.cli("status", &f.profile.network, &[]).await?;
    assert_eq!(closed["position"]["serving"], false);
    assert_eq!(closed["position"]["reason"], "restore_failed");
    let failed_storage: i64 = client
        .query_one(
            "SELECT pg_relation_filenode('serve.block'::regclass)::bigint",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(
        original_storage, failed_storage,
        "failed COPY did not roll back its empty-table rewrite"
    );
    assert_eq!(
        client
            .query_one("SELECT count(*) FROM serve.block", &[])
            .await?
            .get::<_, i64>(0),
        0
    );
    assert_eq!(
        client
            .query("SELECT * FROM public.block", &[])
            .await
            .unwrap_err()
            .code()
            .unwrap()
            .code(),
        "55000"
    );
    client
        .batch_execute(
            "DROP TRIGGER copy_error_guard ON serve.block;DROP FUNCTION serve.copy_error_guard()",
        )
        .await?;
    let retry = dest
        .cli_raw("restore", &f.profile.network, &file_args(&path))
        .await?;
    assert!(
        retry.status.success(),
        "{}",
        String::from_utf8_lossy(&retry.stderr)
    );
    let restored: Value = serde_json::from_slice(&retry.stdout)?;
    let restored_storage: i64 = client
        .query_one(
            "SELECT pg_relation_filenode('serve.block'::regclass)::bigint",
            &[],
        )
        .await?
        .get(0);
    assert_ne!(
        original_storage, restored_storage,
        "successful retry did not replace empty storage in its ingest transaction"
    );
    assert_eq!(
        restored["content_digest"],
        description["verified"]["content_digest"]
    );
    assert_eq!(
        restored["state_digest"],
        description["verified"]["state_digest"]
    );
    let events = String::from_utf8(retry.stderr)?
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .collect::<Vec<_>>();
    for name in [
        "restore_prepare",
        "restore_preflight",
        "restore_recheck_empty",
        "restore_drop_indexes",
        "restore_load_state",
        "restore_indexes",
        "restore_analyze",
        "restore_sequences",
        "restore_verify",
        "restore_publish",
        "restore_commit",
    ] {
        assert!(
            events.iter().any(|v| v["event"] == "restore_stage_finished"
                && v["stage"] == name
                && v["elapsed_seconds"]
                    .as_f64()
                    .is_some_and(|n| n.is_finite() && n >= 0.0)),
            "missing completed stage: {name}"
        );
    }
    println!(
        "{}",
        json!({"restore_copy_server_failure":report,"server_message_detail_hint_and_context_omitted":true,"failed_gate_closed":true,"failed_storage_rewrite_rolled_back":true,"clean_retry_verified":true,"successful_same_transaction_empty_rewrite":true,"pipeline_stages_observed":11})
    );
    drop(client);
    source.cleanup().await?;
    dest.cleanup().await?;
    Ok(())
}
