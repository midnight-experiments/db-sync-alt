//! Snapshot container and restore protocol, development format v1.
//!
//! Wire format 1 carries keep-rule version 2. Old rule-1 files are refused,
//! never relabelled; regenerate by conversion from a genuine legacy source.
//! Snapshots preserve all selected payloads and original pre-filter ordinals.
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
//! Once COPY finishes, the read transaction commits before spool fsync/hash I/O.
//! Temporary sections plus the final temporary output require roughly twice
//! the compressed file size. Bytes copied into the final file are re-hashed.
//! A directory fsync failure after publication is reported as published with
//! durability uncertain; the file remains present and must not be overwritten.
//!
//! Provenance is a fixed flat record, not a recursively embedded snapshot chain.
//! Original source kind/network/target/tip/creation time survive every restore;
//! only the latest snapshot's two digests are retained. Each text field is
//! bounded (64 bytes except small kind/network labels), so depth and maximum
//! description size do not grow with generations.
//!
//! Restore validates the destination before any destructive change: an existing
//! serving/nonempty database is preserved and refused. An empty database gets
//! the layout without secondary indexes and a closed gate; an explicitly owned
//! failed restore may retry. Identity/state are committed closed before ingest.
//! All section hashes and total file framing are preflighted outside the load
//! transaction. Compressed bytes actually decoded are hashed again during COPY.
//! Empty COPY tables are rechecked under relation/identity locks. Before a node
//! reader has gate access, the ingest transaction truncates them, allowing
//! minimal-WAL COPY when the server already uses that WAL mode. Once gate access
//! is granted, compatible relation locks preserve prompt closed-reader errors:
//! no TRUNCATE or index drop, matching indexes stay and missing ones are built.
//! No CASCADE, sequence reset or server-policy change occurs. Product reader
//! grants share the writer lock, so they cannot race the isolated fast path.
//! Caller statement/idle transaction timeouts remain in force; no timeout is
//! silently disabled. Long operations need explicit operator-chosen limits.
//! Restore reports separate COPY/index/analyze/verification/publication times.
//! On a send failure, queued bytes count only earlier successful send awaits,
//! not the attempted chunk or any server row acknowledgement.
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
    time::Instant,
};

use bytes::Bytes;
use dsa_chain::{VERSION, profiles::Profile};
use futures_util::{SinkExt, StreamExt, pin_mut};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_postgres::{GenericClient, IsolationLevel};

use crate::{
    DATABASE_FORMAT, Error, KEEP_RULE_VERSION, NODE_RELATIONS, Store,
    verify::{self, MARKERS, Verification},
};

const MAGIC: &[u8; 8] = b"DSASNAP\0";
const FORMAT: u32 = 1;
const MAX_DESCRIPTION: u64 = 1024 * 1024;
const MAX_SECTION: u64 = 1 << 40;

async fn restore_access<C: GenericClient>(client: &C) -> Result<(bool, &'static str), Error> {
    let row = client.query_one(
        "SELECT p.proowner=(SELECT oid FROM pg_roles WHERE rolname=current_user),EXISTS(SELECT 1 FROM aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a WHERE a.privilege_type='EXECUTE' AND a.grantee<>p.proowner) FROM pg_proc p WHERE p.oid='dsa.gate()'::regprocedure", &[],
    ).await?;
    let owner_matches: bool = row.get(0);
    let other_grants: bool = row.get(1);
    Ok((
        owner_matches && !other_grants,
        if !owner_matches {
            "owner_changed"
        } else if other_grants {
            "non_owner_grant"
        } else {
            "owner_only"
        },
    ))
}

// Compare the server's index deparse with our fixed DDL, allowing only its
// default btree spelling, quoted `index` column and implicit encode argument
// casts from our bytea domains / hex literal. Other casts remain significant.
// Keep quoted contents intact: whitespace inside a literal is significant.
fn index_shape(sql: &str) -> Vec<String> {
    let mut chars = sql.trim_end_matches(';').chars().peekable();
    let mut tokens = Vec::new();
    while let Some(ch) = chars.next() {
        if ch.is_whitespace() {
            continue;
        }
        let mut token = ch.to_string();
        if ch == '\'' || ch == '"' {
            while let Some(next) = chars.next() {
                token.push(next);
                if next == ch {
                    if chars.peek() == Some(&ch) {
                        token.push(chars.next().expect("peeked quote"));
                    } else {
                        break;
                    }
                }
            }
        } else if ch.is_alphanumeric() || ch == '_' {
            while chars
                .peek()
                .is_some_and(|c| c.is_alphanumeric() || *c == '_')
            {
                token.push(chars.next().expect("peeked identifier"));
            }
        }
        tokens.push(if token == "\"index\"" {
            "index".into()
        } else {
            token
        });
    }
    let mut shape = Vec::new();
    let mut i = 0;
    while i < tokens.len() {
        // pg_get_indexdef may omit the table schema when it is in search_path.
        // The catalogue query independently requires the exact served table OID.
        if tokens[i..].starts_with(&["ON".into(), "serve".into(), ".".into()]) {
            shape.push("ON".into());
            i += 3;
        } else if tokens[i..].starts_with(&["USING".into(), "btree".into()]) {
            i += 2;
        } else if i + 9 <= tokens.len()
            && tokens[i] == "encode"
            && tokens[i + 1] == "("
            && tokens[i + 2] == "("
            && matches!(tokens[i + 3].as_str(), "policy" | "name")
            && tokens[i + 4..i + 9] == [")", ":", ":", "bytea", ","]
        {
            shape.extend([
                "encode".into(),
                "(".into(),
                tokens[i + 3].clone(),
                ",".into(),
            ]);
            i += 9;
        } else if tokens[i..].starts_with(&["'hex'".into(), ":".into(), ":".into(), "text".into()])
        {
            shape.push("'hex'".into());
            i += 4;
        } else {
            shape.push(tokens[i].clone());
            i += 1;
        }
    }
    shape
}

async fn missing_restore_indexes<C: GenericClient>(tx: &C) -> Result<Vec<&'static str>, Error> {
    let mut missing = Vec::new();
    for sql in include_str!("indexes.sql")
        .lines()
        .filter(|line| line.starts_with("CREATE"))
    {
        let name = sql
            .split("INDEX ")
            .nth(1)
            .and_then(|s| s.split_whitespace().next())
            .ok_or(Error::Refusal("invalid_index_catalogue"))?;
        let table = sql
            .split(" ON ")
            .nth(1)
            .and_then(|s| s.split(['(', ' ']).next())
            .ok_or(Error::Refusal("invalid_index_catalogue"))?;
        let row = tx.query_opt(
            "SELECT CASE WHEN i.indexrelid IS NOT NULL THEN pg_get_indexdef(c.oid) ELSE '' END,coalesce(i.indisvalid AND i.indisready AND i.indislive AND i.indrelid=to_regclass($2),false) FROM pg_class c LEFT JOIN pg_index i ON i.indexrelid=c.oid WHERE c.oid=to_regclass($1)",
            &[&format!("serve.{name}"), &table],
        ).await?;
        match row {
            None => missing.push(sql),
            Some(row)
                if row.get::<_, bool>(1)
                    && index_shape(&row.get::<_, String>(0)) == index_shape(sql) => {}
            Some(_) => {
                eprintln!(
                    "{}",
                    serde_json::json!({"event":"restore_index_mismatch","index":name})
                );
                return Err(Error::Refusal("destination_index_mismatch"));
            }
        }
    }
    Ok(missing)
}

// Restore-local stage timings contain only fixed operation and table labels.
// They neither change the SQL grouping nor adjust any operational timeout.
struct RestoreStage<'a> {
    stage: &'static str,
    family: &'a str,
    began: Instant,
}
impl<'a> RestoreStage<'a> {
    fn start(stage: &'static str, family: &'a str) -> Self {
        eprintln!(
            "{}",
            serde_json::json!({"event":"restore_stage_started","stage":stage,"family":family})
        );
        Self {
            stage,
            family,
            began: Instant::now(),
        }
    }
    fn error(&self, error: impl Into<Error>) -> Error {
        Error::AtStage {
            stage: self.stage,
            family: self.family.to_owned(),
            elapsed_seconds: self.began.elapsed().as_secs_f64(),
            wait_seconds: None,
            queued_plain_bytes: None,
            error: Box::new(error.into()),
        }
    }
    fn finish(&self) {
        eprintln!(
            "{}",
            serde_json::json!({"event":"restore_stage_finished","stage":self.stage,"family":self.family,"elapsed_seconds":self.began.elapsed().as_secs_f64()})
        );
    }
}

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
    pub origin: Provenance,
    pub verified: Verification,
    pub sections: Vec<Section>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Provenance {
    pub kind: String,
    pub source_kind: String,
    pub source_network: String,
    pub source_target_height: u32,
    pub source_target_hash: String,
    pub source_target_time: String,
    pub source_tip_height: u32,
    pub source_tip_hash: String,
    pub source_tip_time: String,
    pub source_created_at: String,
    pub snapshot_content_digest: Option<String>,
    pub snapshot_state_digest: Option<String>,
}

impl Provenance {
    fn validate(&self, profile: &Profile) -> Result<(), Error> {
        if self.kind.is_empty()
            || self.kind.len() > 32
            || self.source_kind.is_empty()
            || self.source_kind.len() > 32
            || self.source_network != profile.network
            || self.source_tip_height >= 134217728
            || self.source_target_height > self.source_tip_height
        {
            return Err(Error::Refusal("invalid_snapshot_provenance"));
        }
        for value in [&self.source_target_hash, &self.source_tip_hash]
            .into_iter()
            .chain(self.snapshot_content_digest.iter())
            .chain(self.snapshot_state_digest.iter())
        {
            if value.len() != 64
                || !value
                    .bytes()
                    .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
            {
                return Err(Error::Refusal("invalid_snapshot_provenance"));
            }
        }
        for value in [
            &self.source_target_time,
            &self.source_tip_time,
            &self.source_created_at,
        ] {
            if value.is_empty() || value.len() > 64 {
                return Err(Error::Refusal("invalid_snapshot_provenance"));
            }
        }
        Ok(())
    }

    async fn capture<C: GenericClient>(
        client: &C,
        profile: &Profile,
        verified: &Verification,
    ) -> Result<Self, Error> {
        let row=client.query_one("SELECT origin->>'kind',origin->>'source_kind',origin->>'source_network',origin->>'source_target_height',origin->>'source_target_hash',origin->>'source_target_time',origin->>'source_tip_height',origin->>'source_tip_hash',origin->>'source_tip_time',coalesce(origin->>'source_created_at',to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')),origin->>'snapshot_content_digest',origin->>'snapshot_state_digest' FROM dsa.identity WHERE id=1",&[]).await?;
        let text = |index: usize, default: &str| {
            row.get::<_, Option<String>>(index)
                .unwrap_or_else(|| default.to_owned())
        };
        let kind = text(0, "unknown");
        let height = |index: usize, default: u32| -> Result<u32, Error> {
            match row.get::<_, Option<String>>(index) {
                Some(value) => value
                    .parse()
                    .map_err(|_| Error::Refusal("invalid_snapshot_provenance")),
                None => Ok(default),
            }
        };
        let result = Self {
            kind: kind.clone(),
            source_kind: text(1, &kind),
            source_network: text(2, &profile.network),
            source_target_height: height(3, verified.state.target.height)?,
            source_target_hash: text(4, &verified.state.target.hash),
            source_target_time: text(5, &verified.state.target.time),
            source_tip_height: height(6, verified.state.tip.height)?,
            source_tip_hash: text(7, &verified.state.tip.hash),
            source_tip_time: text(8, &verified.state.tip.time),
            source_created_at: row.get(9),
            snapshot_content_digest: row.get(10),
            snapshot_state_digest: row.get(11),
        };
        result.validate(profile)?;
        Ok(result)
    }

    fn restored(&self, verified: &Verification) -> Self {
        Self {
            kind: "restored".to_owned(),
            snapshot_content_digest: Some(verified.content_digest.clone()),
            snapshot_state_digest: Some(verified.state_digest.clone()),
            ..self.clone()
        }
    }
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

struct CheckedReader<R> {
    inner: R,
    hash: Sha256,
    bytes: u64,
}
impl<R> CheckedReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            hash: Sha256::new(),
            bytes: 0,
        }
    }
    fn finish(self) -> (u64, String) {
        (self.bytes, format!("{:x}", self.hash.finalize()))
    }
}
impl<R: Read> Read for CheckedReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buffer)?;
        self.hash.update(&buffer[..n]);
        self.bytes += n as u64;
        Ok(n)
    }
}

fn preflight(file: &mut File, description: &Description) -> Result<(), Error> {
    let first = file.stream_position()?;
    let expected = description
        .sections
        .iter()
        .try_fold(first, |offset, s| offset.checked_add(s.compressed_bytes))
        .ok_or(Error::Refusal("snapshot_section_bounds"))?;
    if file.metadata()?.len() != expected {
        return Err(Error::Refusal("snapshot_file_size_mismatch"));
    }
    for section in &description.sections {
        let (size, hash) = hash_reader(&mut Read::by_ref(file).take(section.compressed_bytes))?;
        if size != section.compressed_bytes || hash != section.sha256 {
            return Err(Error::Refusal("snapshot_section_hash_mismatch"));
        }
    }
    file.seek(SeekFrom::Start(first))?;
    Ok(())
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
    description.origin.validate(profile)?;
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
        let origin = Provenance::capture(&tx, profile, &verified).await?;
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
            let file = encoder.finish()?;
            let compressed_bytes = file.metadata()?.len();
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
                sha256: String::new(),
            });
        }
        tx.commit().await?;
        // COPY fixed the source snapshot in private spool files. Avoid holding
        // an idle transaction during local sync/hash I/O.
        for section in &mut sections {
            let mut file = File::options()
                .read(true)
                .write(true)
                .open(staging.path().join(&section.table))?;
            file.sync_all()?;
            let (size, hash) = hash_reader(&mut file)?;
            if size != section.compressed_bytes {
                return Err(Error::Refusal("snapshot_spool_changed"));
            }
            section.sha256 = hash;
        }
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
        for section in &description.sections {
            let mut reader = CheckedReader::new(File::open(staging.path().join(&section.table))?);
            std::io::copy(&mut reader, &mut output)?;
            let (size, hash) = reader.finish();
            if size != section.compressed_bytes || hash != section.sha256 {
                return Err(Error::Refusal("snapshot_spool_changed"));
            }
        }
        output.as_file().sync_all()?;
        output
            .persist_noclobber(path)
            .map_err(|_| Error::Refusal("snapshot_publish_failed"))?;
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| Error::PublishedDurabilityUncertain)?;
        Ok(description)
    }

    /// Preserve serving/nonempty destinations. A failed restore owns only its
    /// explicitly marked empty layout; its load transaction always rolls back.
    pub(crate) async fn prepare_destination(
        &mut self,
        profile: &Profile,
        failed_kind: &'static str,
    ) -> Result<(), Error> {
        if !matches!(failed_kind, "restore_failed" | "convert_failed") {
            return Err(Error::Refusal("invalid_load_operation"));
        }
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
        let row=tx.query_one("SELECT p.serving,i.origin->>'kind' FROM dsa.position p CROSS JOIN dsa.identity i WHERE p.id=1 AND i.id=1",&[]).await?;
        let origin: Option<String> = row.get(1);
        if row.get::<_, bool>(0) {
            return Err(Error::Refusal("destination_not_empty"));
        }
        let counts = tables()
            .map(|t| qualified(t).map(|q| format!("(SELECT count(*) FROM {q})")))
            .collect::<Result<Vec<_>, _>>()?
            .join("+");
        let occupied: i64 = tx.query_one(&format!("SELECT {counts}+(SELECT count(*) FROM dsa.supply)+(SELECT count(*) FROM dsa.first_served)+(SELECT count(*) FROM dsa.calls)"), &[]).await?.get(0);
        if occupied != 0
            || !(origin.as_deref() == Some("empty") || origin.as_deref() == Some(failed_kind))
        {
            return Err(Error::Refusal("destination_not_empty"));
        }
        tx.execute(
            "UPDATE dsa.position SET serving=false,reason=$1,updated_at=now() WHERE id=1",
            &[&failed_kind],
        )
        .await?;
        tx.execute(
            "UPDATE dsa.identity SET origin=jsonb_build_object('kind',$1::text) WHERE id=1",
            &[&failed_kind],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn restore(&mut self, profile: &Profile, path: &Path) -> Result<Verification, Error> {
        let stage = RestoreStage::start("restore_prepare", "database");
        self.prepare_destination(profile, "restore_failed")
            .await
            .map_err(|error| stage.error(error))?;
        stage.finish();
        // Opening/parsing happens after a closed, identifiable retry state exists.
        let stage = RestoreStage::start("restore_preflight", "database");
        let mut file = File::open(path).map_err(|error| stage.error(error))?;
        let description =
            read_description(&mut file, profile).map_err(|error| stage.error(error))?;
        preflight(&mut file, &description).map_err(|error| stage.error(error))?;
        stage.finish();
        let stage = RestoreStage::start("restore_recheck_empty", "database");
        // A supported reader needs gate EXECUTE. Conservatively keep read locks
        // compatible if any other role (including PUBLIC or a group) has it, or
        // the gate owner differs. Do not base this on currently active sessions.
        // grant-reader uses this same session writer lock, across preflight and
        // commit. Concurrent manual owner DDL/GRANT is outside that protocol.
        // This metadata read stays outside ingest, so even REPEATABLE READ
        // starts its data snapshot only after the load locks. The session writer
        // advisory lock is already held across prepare/preflight/load.
        let (isolated_bulk, reason) = restore_access(self.client())
            .await
            .map_err(|error| stage.error(error))?;
        eprintln!(
            "{}",
            serde_json::json!({"event":"restore_load_mode","mode":if isolated_bulk {"isolated_bulk"} else {"reader_compatible"},"reason":reason})
        );
        let tx = self
            .writer_transaction()
            .await
            .map_err(|error| stage.error(error))?;
        // Recheck the committed closed layout under locks excluding other DML.
        // Only the isolated path replaces empty storage for minimal-WAL COPY.
        let copied = tables().map(qualified).collect::<Result<Vec<_>, _>>()?;
        let mut locked = copied.clone();
        locked.extend([
            "dsa.supply".into(),
            "dsa.first_served".into(),
            "dsa.calls".into(),
        ]);
        tx.batch_execute(&format!(
            "LOCK TABLE {} IN {} MODE",
            locked.join(","),
            if isolated_bulk {
                "ACCESS EXCLUSIVE"
            } else {
                "SHARE ROW EXCLUSIVE"
            }
        ))
        .await
        .map_err(|error| stage.error(error))?;
        // Narrow an out-of-protocol manual GRANT race while we waited. This is
        // after load locks, preserving the RR recheck snapshot ordering. Owner
        // DDL after this check still bypasses the product's writer protocol.
        if isolated_bulk
            && !restore_access(&tx)
                .await
                .map_err(|error| stage.error(error))?
                .0
        {
            return Err(stage.error(Error::Refusal("gate_access_changed")));
        }
        let row = tx.query_one("SELECT p.serving,i.origin->>'kind' FROM dsa.position p CROSS JOIN dsa.identity i WHERE p.id=1 AND i.id=1 FOR UPDATE OF p,i", &[])
            .await.map_err(|error| stage.error(error))?;
        let occupied = locked
            .iter()
            .map(|name| format!("EXISTS(SELECT 1 FROM {name})"))
            .collect::<Vec<_>>()
            .join(" OR ");
        let occupied: bool = tx
            .query_one(&format!("SELECT {occupied}"), &[])
            .await
            .map_err(|error| stage.error(error))?
            .get(0);
        if row.get::<_, bool>(0)
            || row.get::<_, Option<String>>(1).as_deref() != Some("restore_failed")
            || occupied
        {
            return Err(stage.error(Error::Refusal("destination_not_empty")));
        }
        let missing_indexes = if isolated_bulk {
            // No CASCADE/identity reset; failure rolls back this rewrite.
            tx.batch_execute(&format!("TRUNCATE TABLE {}", copied.join(",")))
                .await
                .map_err(|error| stage.error(error))?;
            Vec::new()
        } else {
            // DROP INDEX also takes ACCESS EXCLUSIVE on its table. Retain
            // matching indexes and reject conflicting definitions before COPY.
            missing_restore_indexes(&tx)
                .await
                .map_err(|error| stage.error(error))?
        };
        stage.finish();
        let stage = RestoreStage::start("restore_drop_indexes", "database");
        // A manually initialized empty layout may already have secondary indexes.
        for line in include_str!("indexes.sql")
            .lines()
            .filter(|line| isolated_bulk && line.starts_with("CREATE"))
        {
            let name = line
                .split("INDEX ")
                .nth(1)
                .and_then(|s| s.split_whitespace().next())
                .ok_or(Error::Refusal("invalid_index_catalogue"))?;
            tx.batch_execute(&format!("DROP INDEX IF EXISTS serve.{name}"))
                .await
                .map_err(|error| stage.error(error))?;
        }
        stage.finish();
        for section in &description.sections {
            {
                let table = qualified(&section.table)?;
                let began = Instant::now();
                let database_error =
                    |stage, error: tokio_postgres::Error, wait_seconds, queued_plain_bytes| {
                        Error::AtStage {
                            stage,
                            family: section.table.clone(),
                            elapsed_seconds: began.elapsed().as_secs_f64(),
                            wait_seconds,
                            queued_plain_bytes,
                            error: Box::new(Error::Postgres(error)),
                        }
                    };
                eprintln!(
                    "{}",
                    serde_json::json!({"event":"restore_section_started","family":section.table,"plain_bytes":section.plain_bytes,"compressed_bytes":section.compressed_bytes,"expected_rows":section.rows})
                );
                let mut decoder = zstd::stream::read::Decoder::new(CheckedReader::new(
                    Read::by_ref(&mut file).take(section.compressed_bytes),
                ))?;
                decoder.window_log_max(27)?;
                let query = format!("COPY {table} FROM STDIN (FORMAT binary)");
                let sink = tx
                    .copy_in::<_, Bytes>(&query)
                    .await
                    .map_err(|error| database_error("restore_copy_open", error, None, None))?;
                eprintln!(
                    "{}",
                    serde_json::json!({"event":"restore_copy_opened","family":section.table,"elapsed_seconds":began.elapsed().as_secs_f64()})
                );
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
                    let send = Instant::now();
                    sink.as_mut()
                        .send(Bytes::copy_from_slice(&buf[..n]))
                        .await
                        .map_err(|error| {
                            database_error(
                                "restore_copy_send",
                                error,
                                Some(send.elapsed().as_secs_f64()),
                                Some(bytes - n as u64),
                            )
                        })?;
                    if send.elapsed().as_secs() >= 1 {
                        eprintln!(
                            "{}",
                            serde_json::json!({"event":"restore_copy_backpressure","family":section.table,"queued_plain_bytes":bytes,"send_seconds":send.elapsed().as_secs_f64(),"elapsed_seconds":began.elapsed().as_secs_f64(),"note":"queued bytes are not server row acknowledgements"})
                        );
                    }
                }
                let (size, hash) = decoder.finish().into_inner().finish();
                if size != section.compressed_bytes || hash != section.sha256 {
                    return Err(Error::Refusal("snapshot_section_hash_mismatch"));
                }
                let finish = Instant::now();
                let rows = sink.as_mut().finish().await.map_err(|error| {
                    database_error(
                        "restore_copy_finish",
                        error,
                        Some(finish.elapsed().as_secs_f64()),
                        Some(bytes),
                    )
                })?;
                if bytes != section.plain_bytes || rows != section.rows {
                    return Err(Error::Refusal("snapshot_row_count_mismatch"));
                }
                eprintln!(
                    "{}",
                    serde_json::json!({"event":"restore_section_finished","family":section.table,"plain_bytes":bytes,"acknowledged_rows":rows,"elapsed_seconds":began.elapsed().as_secs_f64()})
                );
            }
        }
        let stage = RestoreStage::start("restore_load_state", "database");
        let mut extra = [0];
        if file.read(&mut extra).map_err(|error| stage.error(error))? != 0 {
            return Err(stage.error(Error::Refusal("snapshot_trailing_bytes")));
        }
        for (unit, quantity) in &description.verified.supply {
            tx.execute(
                "INSERT INTO dsa.supply(unit,quantity) VALUES($1,$2::text::numeric)",
                &[unit, quantity],
            )
            .await
            .map_err(|error| stage.error(error))?;
        }
        let s = &description.verified.state;
        tx.execute("UPDATE dsa.position SET horizon_height=$1::integer,horizon_hash=decode($2,'hex'),horizon_time=$3::text::timestamptz,target_height=$4::integer,target_hash=decode($5,'hex'),target_time=$6::text::timestamptz,tip_height=$7::integer,tip_hash=decode($8,'hex'),tip_time=$9::text::timestamptz,retention_seconds=$10,updated_at=now() WHERE id=1",&[&(s.horizon.height as i32),&s.horizon.hash,&s.horizon.time,&(s.target.height as i32),&s.target.hash,&s.target.time,&(s.tip.height as i32),&s.tip.hash,&s.tip.time,&s.retention_seconds]).await.map_err(|error| stage.error(error))?;
        stage.finish();
        let stage = RestoreStage::start("restore_indexes", "database");
        tx.batch_execute(&if isolated_bulk {
            include_str!("indexes.sql").to_owned()
        } else {
            missing_indexes.join("\n")
        })
        .await
        .map_err(|error| stage.error(error))?;
        stage.finish();
        for table in tables() {
            let stage = RestoreStage::start("restore_analyze", table);
            tx.batch_execute(&format!("ANALYZE {}", qualified(table)?))
                .await
                .map_err(|error| stage.error(error))?;
            stage.finish();
        }
        for table in ["multi_asset", "datum", "pool_hash", "epoch_stake"] {
            let stage = RestoreStage::start("restore_sequences", table);
            tx.query_one(&format!("SELECT setval('dsa.{table}_id_seq',greatest(coalesce(max(id),0)+1,1),false) FROM serve.{table}"),&[]).await.map_err(|error| stage.error(error))?;
            stage.finish();
        }
        let stage = RestoreStage::start("restore_verify", "database");
        let verified = verify::verify(&tx, profile, false)
            .await
            .map_err(|error| stage.error(error))?;
        if !verify::logical_equal(&description.verified, &verified) {
            return Err(stage.error(Error::Refusal("snapshot_content_digest_mismatch")));
        }
        stage.finish();
        let stage = RestoreStage::start("restore_publish", "database");
        tx.execute("INSERT INTO dsa.first_served SELECT id,transaction_timestamp() FROM serve.block WHERE id BETWEEN $1 AND $2",&[&i64::from(s.horizon.height),&i64::from(s.target.height)]).await.map_err(|error| stage.error(error))?;
        tx.execute(
            "UPDATE dsa.identity SET origin=$1,tool_version=$2 WHERE id=1",
            &[
                &serde_json::to_value(description.origin.restored(&verified))
                    .map_err(|error| stage.error(error))?,
                &VERSION,
            ],
        )
        .await
        .map_err(|error| stage.error(error))?;
        tx.batch_execute(
            "UPDATE dsa.position SET serving=true,reason='serving',updated_at=now() WHERE id=1",
        )
        .await
        .map_err(|error| stage.error(error))?;
        stage.finish();
        let stage = RestoreStage::start("restore_commit", "database");
        tx.commit().await.map_err(|error| stage.error(error))?;
        stage.finish();
        Ok(verified)
    }
}

#[cfg(test)]
mod index_tests {
    use super::index_shape;

    #[test]
    fn restore_index_shapes_preserve_literals_and_nondefault_semantics() {
        let expected = "CREATE INDEX i ON serve.tx_out(tx_id,index);";
        assert_eq!(
            index_shape(expected),
            index_shape("CREATE INDEX i ON serve.tx_out USING btree (tx_id, \"index\")")
        );
        let expression =
            "CREATE INDEX i ON serve.multi_asset(encode(policy,'hex'),encode(name,'hex'));";
        assert_eq!(
            index_shape(expression),
            index_shape(
                "CREATE INDEX i ON serve.multi_asset USING btree (encode((policy)::bytea, 'hex'::text), encode((name)::bytea, 'hex'::text))"
            )
        );
        for wrong in [
            "CREATE INDEX i ON serve.tx_out(index,tx_id)",
            "CREATE UNIQUE INDEX i ON serve.tx_out(tx_id,index)",
            "CREATE INDEX i ON serve.tx_out(tx_id,index) WHERE tx_id>0",
            "CREATE INDEX i ON serve.tx_out(tx_id DESC,index)",
            "CREATE INDEX i ON serve.tx_out(tx_id,index) INCLUDE(address)",
        ] {
            assert_ne!(index_shape(expected), index_shape(wrong));
        }
        for literal in ["h e x", "USINGbtreehex", "hex'':text"] {
            assert_ne!(
                index_shape(expression),
                index_shape(&expression.replace("'hex'", &format!("'{literal}'")))
            );
        }
    }
}
