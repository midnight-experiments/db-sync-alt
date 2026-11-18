//! Snapshot container and restore protocol, development format v1.
//!
//! This format is unreleased; Q18 does not freeze an asset projection or a real
//! network golden digest. A snapshot preserves the complete selected rows and
//! original asset ordinals, independently of how the producer selected them.
//!
//! File: eight-byte magic `DSASNAP\0`, u32 BE format number, u64 BE description
//! byte length, SHA-256(description), then the UTF-8 JSON description. Description
//! fields bind network/profile/node/rule/tool, H/T/P(height/hash/time), retention,
//! provenance, canonical content and protected-state digests, natural-key family
//! counts/hashes, supply
//! quantities, and ordered section names/counts/compressed+plain sizes/hashes.
//! Each section is one independently zstd-compressed PostgreSQL17 binary COPY
//! stream. Sections cover all eleven served tables and sparse internal
//! consumed-before-horizon state. Supply is protected in the description and
//! canonical digest. First-served timestamps are deliberately reset on restore;
//! provider accounting belongs to the instance and is not imported.
//!
//! Snapshot opens exactly one REPEATABLE READ READ ONLY transaction, verifies
//! serving state and integrity in that snapshot, and streams COPY per family to
//! temporary compressed sections beside the requested output. It takes no writer
//! lock. Only after all hashes/sizes are known is a new complete file atomically
//! published; an existing file is never overwritten. Every temporary is removed
//! on error. Compression memory is bounded; whole tables are never buffered.
//!
//! Restore validates the destination before any destructive change: an existing
//! serving/nonempty database is preserved and refused. An empty database gets
//! the layout without secondary indexes and a closed gate; an explicitly owned
//! failed restore may retry. Identity/state are committed closed before ingest.
//! A writer-locked transaction streams bounded decompression into binary COPY,
//! checks exact sizes/hashes/trailing EOF, creates indexes, analyzes, initializes
//! private allocation sequences above imported identity IDs, verifies closure,
//! positional ranges, header continuity, supply, sparse markers and digest, then
//! publishes state/gate in that same transaction. Any failure rolls back imported
//! rows and leaves the committed failed-restore state closed and retryable.
//!
//! Sparse state stores the retained outpoint ID; presence means consumed before
//! H, derived inside the same source snapshot. It stores no spending height. Its
//! separate natural-key state_digest uses creator hash/index instead of numeric
//! ID, preserving the eleven served families' node-content digest definition.
//! Verification rejects missing/non-old outputs or contradictory window spends.
//! It is never exposed through public views, and consumed_by_tx_id stays NULL.
//! This bit survives snapshots so an old spent carry output cannot become an
//! unspent K3 output when its spend is no longer served and H advances again.

use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

use bytes::Bytes;
use dsa_chain::{VERSION, profiles::Profile};
use futures_util::{SinkExt, StreamExt, pin_mut};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio_postgres::IsolationLevel;

use crate::{
    DATABASE_FORMAT, Error, KEEP_RULE_VERSION, NODE_RELATIONS, Store,
    verify::{self, MARKERS, Verification},
};

const MAGIC: &[u8; 8] = b"DSASNAP\0";
const FORMAT: u32 = 1;
const MAX_DESCRIPTION: u64 = 1024 * 1024;
const MAX_SECTION: u64 = 1 << 40;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Section {
    pub table: String,
    pub rows: u64,
    pub compressed_bytes: u64,
    pub plain_bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Description {
    pub database_format: i32,
    pub network: String,
    pub profile_hash: String,
    pub keep_rule_version: i32,
    pub node_release: String,
    pub tool_version: String,
    pub origin: Value,
    pub verified: Verification,
    pub sections: Vec<Section>,
}

fn tables() -> impl Iterator<Item = &'static str> {
    NODE_RELATIONS.into_iter().chain([MARKERS])
}
fn qualified(table: &str) -> Result<String, Error> {
    if table == MARKERS {
        Ok(format!("dsa.{table}"))
    } else if NODE_RELATIONS.contains(&table) {
        Ok(format!("serve.{table}"))
    } else {
        Err(Error::Refusal("invalid_snapshot_table"))
    }
}

fn hash_reader(reader: &mut impl Read) -> Result<(u64, String), Error> {
    let mut hash = Sha256::new();
    let mut size = 0;
    let mut buf = [0; 65536];
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        size += n as u64;
        hash.update(&buf[..n]);
    }
    Ok((size, format!("{:x}", hash.finalize())))
}

fn read_description(file: &mut File, profile: &Profile) -> Result<Description, Error> {
    let mut magic = [0; 8];
    file.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(Error::Refusal("unsupported_snapshot_magic"));
    }
    let mut format = [0; 4];
    file.read_exact(&mut format)?;
    if u32::from_be_bytes(format) != FORMAT {
        return Err(Error::Refusal("unsupported_snapshot_format"));
    }
    let mut length = [0; 8];
    file.read_exact(&mut length)?;
    let length = u64::from_be_bytes(length);
    if length > MAX_DESCRIPTION {
        return Err(Error::Refusal("snapshot_description_too_large"));
    }
    let mut expected = [0; 32];
    file.read_exact(&mut expected)?;
    let mut json = vec![0; length as usize];
    file.read_exact(&mut json)?;
    if Sha256::digest(&json)[..] != expected {
        return Err(Error::Refusal("snapshot_description_hash_mismatch"));
    }
    let description: Description = serde_json::from_slice(&json)
        .map_err(|_| Error::Refusal("invalid_snapshot_description"))?;
    if description.database_format != DATABASE_FORMAT {
        return Err(Error::Refusal("unsupported_database_format"));
    }
    if description.network != profile.network {
        return Err(Error::Refusal("network_mismatch"));
    }
    if description.profile_hash != profile.hash()? {
        return Err(Error::Refusal("profile_hash_mismatch"));
    }
    if description.keep_rule_version != KEEP_RULE_VERSION {
        return Err(Error::Refusal("keep_rule_version_mismatch"));
    }
    if description.node_release != profile.node_release {
        return Err(Error::Refusal("node_release_mismatch"));
    }
    if description
        .sections
        .iter()
        .map(|s| s.table.as_str())
        .collect::<Vec<_>>()
        != tables().collect::<Vec<_>>()
    {
        return Err(Error::Refusal("snapshot_section_inventory"));
    }
    for section in &description.sections {
        let expected = if section.table == MARKERS {
            description.verified.protected_state.rows
        } else {
            description
                .verified
                .families
                .get(&section.table)
                .ok_or(Error::Refusal("snapshot_family_inventory"))?
                .rows
        };
        if section.rows != expected
            || section.compressed_bytes == 0
            || section.compressed_bytes > MAX_SECTION
            || section.plain_bytes > MAX_SECTION
        {
            return Err(Error::Refusal("snapshot_section_bounds"));
        }
    }
    Ok(description)
}

impl Store {
    pub async fn snapshot(&mut self, profile: &Profile, path: &Path) -> Result<Description, Error> {
        self.open(profile).await?;
        if path.exists() {
            return Err(Error::Refusal("snapshot_already_exists"));
        }
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let staging = tempfile::Builder::new()
            .prefix(".dsa-snapshot-")
            .tempdir_in(parent)?;
        let tx = self
            .client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await?;
        let verified = verify::verify(&tx, profile, true).await?;
        let origin: Value = tx
            .query_one("SELECT origin FROM dsa.identity WHERE id=1", &[])
            .await?
            .get(0);
        let mut sections = Vec::new();
        for table in tables() {
            let file = File::options()
                .read(true)
                .write(true)
                .create_new(true)
                .open(staging.path().join(table))?;
            let mut encoder = zstd::stream::write::Encoder::new(file, 3)?;
            let key = if table == "epoch_param" {
                "epoch_no"
            } else if table == MARKERS {
                "tx_out_id"
            } else {
                "id"
            };
            let query = format!(
                "COPY (SELECT * FROM {} ORDER BY {key}) TO STDOUT (FORMAT binary)",
                qualified(table)?
            );
            let stream = tx.copy_out(&query).await?;
            pin_mut!(stream);
            let mut plain_bytes = 0;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                plain_bytes += chunk.len() as u64;
                encoder.write_all(&chunk)?;
            }
            let mut file = encoder.finish()?;
            file.sync_all()?;
            file.rewind()?;
            let (compressed_bytes, sha256) = hash_reader(&mut file)?;
            let rows = if table == MARKERS {
                verified.protected_state.rows
            } else {
                verified.families[table].rows
            };
            sections.push(Section {
                table: table.to_owned(),
                rows,
                compressed_bytes,
                plain_bytes,
                sha256,
            });
        }
        tx.commit().await?;
        let description = Description {
            database_format: DATABASE_FORMAT,
            network: profile.network.clone(),
            profile_hash: profile.hash()?,
            keep_rule_version: KEEP_RULE_VERSION,
            node_release: profile.node_release.clone(),
            tool_version: VERSION.to_owned(),
            origin,
            verified,
            sections,
        };
        let json = serde_json::to_vec(&description)?;
        if json.len() as u64 > MAX_DESCRIPTION {
            return Err(Error::Refusal("snapshot_description_too_large"));
        }
        let mut output = tempfile::NamedTempFile::new_in(parent)?;
        output.write_all(MAGIC)?;
        output.write_all(&FORMAT.to_be_bytes())?;
        output.write_all(&(json.len() as u64).to_be_bytes())?;
        output.write_all(&Sha256::digest(&json))?;
        output.write_all(&json)?;
        for table in tables() {
            std::io::copy(&mut File::open(staging.path().join(table))?, &mut output)?;
        }
        output.as_file().sync_all()?;
        output
            .persist_noclobber(path)
            .map_err(|_| Error::Refusal("snapshot_publish_failed"))?;
        File::open(parent)?.sync_all()?;
        Ok(description)
    }

    /// Preserve serving/nonempty destinations. A failed restore owns only its
    /// explicitly marked empty layout; its load transaction always rolls back.
    async fn restore_destination(&mut self, profile: &Profile) -> Result<(), Error> {
        let identity: bool = self
            .client
            .query_one("SELECT to_regclass('dsa.identity') IS NOT NULL", &[])
            .await?
            .get(0);
        if !identity {
            self.create_layout(profile, false).await?;
        }
        self.open(profile).await?;
        let tx = self.writer_transaction().await?;
        let row=tx.query_one("SELECT p.serving,i.origin FROM dsa.position p CROSS JOIN dsa.identity i WHERE p.id=1 AND i.id=1",&[]).await?;
        let origin: Value = row.get(1);
        if row.get::<_, bool>(0) {
            return Err(Error::Refusal("destination_not_empty"));
        }
        let counts = tables()
            .map(|t| qualified(t).map(|q| format!("(SELECT count(*) FROM {q})")))
            .collect::<Result<Vec<_>, _>>()?
            .join("+");
        let occupied: i64 = tx.query_one(&format!("SELECT {counts}"), &[]).await?.get(0);
        if occupied != 0 || !matches!(origin["kind"].as_str(), Some("empty" | "restore_failed")) {
            return Err(Error::Refusal("destination_not_empty"));
        }
        tx.batch_execute("UPDATE dsa.position SET serving=false,reason='restore_failed',updated_at=now() WHERE id=1; UPDATE dsa.identity SET origin='{\"kind\":\"restore_failed\"}'::jsonb WHERE id=1").await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn restore(&mut self, profile: &Profile, path: &Path) -> Result<Verification, Error> {
        self.restore_destination(profile).await?;
        // Opening/parsing happens after a closed, identifiable retry state exists.
        let mut file = File::open(path)?;
        let description = read_description(&mut file, profile)?;
        let tx = self.writer_transaction().await?;
        // A manually initialized empty layout may already have secondary indexes.
        for line in include_str!("indexes.sql")
            .lines()
            .filter(|line| line.starts_with("CREATE"))
        {
            let name = line
                .split("INDEX ")
                .nth(1)
                .and_then(|s| s.split_whitespace().next())
                .ok_or(Error::Refusal("invalid_index_catalogue"))?;
            tx.batch_execute(&format!("DROP INDEX IF EXISTS serve.{name}"))
                .await?;
        }
        for section in &description.sections {
            let start = file.stream_position()?;
            let (size, hash) =
                hash_reader(&mut Read::by_ref(&mut file).take(section.compressed_bytes))?;
            if size != section.compressed_bytes || hash != section.sha256 {
                return Err(Error::Refusal("snapshot_section_hash_mismatch"));
            }
            file.seek(SeekFrom::Start(start))?;
            {
                let mut decoder = zstd::stream::read::Decoder::new(
                    Read::by_ref(&mut file).take(section.compressed_bytes),
                )?;
                decoder.window_log_max(27)?;
                let query = format!(
                    "COPY {} FROM STDIN (FORMAT binary)",
                    qualified(&section.table)?
                );
                let sink = tx.copy_in::<_, Bytes>(&query).await?;
                pin_mut!(sink);
                let mut bytes = 0;
                let mut buf = [0; 65536];
                loop {
                    let n = decoder.read(&mut buf)?;
                    if n == 0 {
                        break;
                    }
                    bytes += n as u64;
                    if bytes > section.plain_bytes {
                        return Err(Error::Refusal("snapshot_plain_size_mismatch"));
                    }
                    sink.as_mut()
                        .send(Bytes::copy_from_slice(&buf[..n]))
                        .await?;
                }
                let rows = sink.as_mut().finish().await?;
                if bytes != section.plain_bytes || rows != section.rows {
                    return Err(Error::Refusal("snapshot_row_count_mismatch"));
                }
            }
            file.seek(SeekFrom::Start(start + section.compressed_bytes))?;
        }
        let mut extra = [0];
        if file.read(&mut extra)? != 0 {
            return Err(Error::Refusal("snapshot_trailing_bytes"));
        }
        for (unit, quantity) in &description.verified.supply {
            tx.execute(
                "INSERT INTO dsa.supply(unit,quantity) VALUES($1,$2::text::numeric)",
                &[unit, quantity],
            )
            .await?;
        }
        let s = &description.verified.state;
        tx.execute("UPDATE dsa.position SET horizon_height=$1::integer,horizon_hash=decode($2,'hex'),horizon_time=$3::text::timestamptz,target_height=$4::integer,target_hash=decode($5,'hex'),target_time=$6::text::timestamptz,tip_height=$7::integer,tip_hash=decode($8,'hex'),tip_time=$9::text::timestamptz,retention_seconds=$10,updated_at=now() WHERE id=1",&[&(s.horizon.height as i32),&s.horizon.hash,&s.horizon.time,&(s.target.height as i32),&s.target.hash,&s.target.time,&(s.tip.height as i32),&s.tip.hash,&s.tip.time,&s.retention_seconds]).await?;
        tx.batch_execute(include_str!("indexes.sql")).await?;
        for table in tables() {
            tx.batch_execute(&format!("ANALYZE {}", qualified(table)?))
                .await?;
        }
        for table in ["multi_asset", "datum", "pool_hash", "epoch_stake"] {
            tx.query_one(&format!("SELECT setval('dsa.{table}_id_seq',greatest(coalesce(max(id),0)+1,1),false) FROM serve.{table}"),&[]).await?;
        }
        let verified = verify::verify(&tx, profile, false).await?;
        if !verify::logical_equal(&description.verified, &verified) {
            return Err(Error::Refusal("snapshot_content_digest_mismatch"));
        }
        tx.execute("INSERT INTO dsa.first_served SELECT id,transaction_timestamp() FROM serve.block WHERE id BETWEEN $1 AND $2",&[&i64::from(s.horizon.height),&i64::from(s.target.height)]).await?;
        tx.execute("UPDATE dsa.identity SET origin=$1,tool_version=$2 WHERE id=1",&[&verify::origin(&json!({"origin":description.origin,"content_digest":verified.content_digest,"state_digest":verified.state_digest})),&VERSION]).await?;
        tx.batch_execute(
            "UPDATE dsa.position SET serving=true,reason='serving',updated_at=now() WHERE id=1",
        )
        .await?;
        tx.commit().await?;
        Ok(verified)
    }
}
