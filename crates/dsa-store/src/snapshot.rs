//! The snapshot file: one consistent state of a serving database.
//!
//! Layout: the eight bytes `DSASNAP\0`, the format number (u32, big endian),
//! the description length (u64, big endian), the SHA-256 of the description,
//! the description (UTF-8 JSON), then one section per table. The description
//! binds network, profile hash, keep-rule version, node and tool release, the
//! origin, the verified state (horizon, target, tip, retention, per-family
//! digests, content and state digests, supply) and, per section, its row
//! count, compressed and plain sizes and hash. Each section is one
//! zstd-compressed PostgreSQL binary COPY stream: the eleven served tables and
//! the outputs consumed before the horizon. First-served times and provider
//! call counts belong to an instance and are not in the file.
//!
//! `snapshot` reads one REPEATABLE READ READ ONLY transaction, verifies it,
//! streams each table into a compressed temporary section beside the output,
//! then writes the complete file under a temporary name and publishes it
//! without overwriting; it needs about twice the file size in free space.

use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

use dsa_chain::{VERSION, profiles::Profile};
use futures_util::{StreamExt, pin_mut};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_postgres::{GenericClient, IsolationLevel};

use crate::{
    DATABASE_FORMAT, Error, KEEP_RULE_VERSION, NODE_RELATIONS, Store,
    verify::{self, Checks, MARKERS, Verification},
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
        let row = client
            .query_one(
                "SELECT origin->>'kind',origin->>'source_kind',origin->>'source_network',
                 origin->>'source_target_height',origin->>'source_target_hash',origin->>'source_target_time',
                 origin->>'source_tip_height',origin->>'source_tip_hash',origin->>'source_tip_time',
                 coalesce(origin->>'source_created_at',to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')),
                 origin->>'snapshot_content_digest',origin->>'snapshot_state_digest'
                 FROM dsa.identity WHERE id=1",
                &[],
            )
            .await?;
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

    pub(crate) fn restored(&self, verified: &Verification) -> Self {
        Self {
            kind: "restored".to_owned(),
            snapshot_content_digest: Some(verified.content_digest.clone()),
            snapshot_state_digest: Some(verified.state_digest.clone()),
            ..self.clone()
        }
    }
}

pub(crate) fn tables() -> impl Iterator<Item = &'static str> {
    NODE_RELATIONS.into_iter().chain([MARKERS])
}
pub(crate) fn qualified(table: &str) -> Result<String, Error> {
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

pub(crate) struct CheckedReader<R> {
    inner: R,
    hash: Sha256,
    bytes: u64,
}
impl<R> CheckedReader<R> {
    pub(crate) fn new(inner: R) -> Self {
        Self {
            inner,
            hash: Sha256::new(),
            bytes: 0,
        }
    }
    pub(crate) fn finish(self) -> (u64, String) {
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

pub(crate) fn preflight(file: &mut File, description: &Description) -> Result<(), Error> {
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

/// The description of the snapshot file at `path`, checked against
/// `profile` (format, network, profile hash, rule version), without reading
/// its sections.
pub fn describe(path: &Path, profile: &Profile) -> Result<Description, Error> {
    read_description(&mut File::open(path)?, profile)
}

pub(crate) fn read_description(file: &mut File, profile: &Profile) -> Result<Description, Error> {
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
        let verified = verify::verify(&tx, profile, Checks::SNAPSHOT).await?;
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
        // The spool files hold the snapshot; do not stay idle in it while they
        // are synced and hashed.
        tx.commit().await?;
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
}
