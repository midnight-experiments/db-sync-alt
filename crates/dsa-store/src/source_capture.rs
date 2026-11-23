//! Development source-answer file, format2 with an enforced keep-rule version.
//!
//! A conversion captures the literal node answers in its source RR/RO snapshot,
//! commits that source, then publishes this independently useful observation
//! before destination finish. File failure therefore cannot publish a converted
//! database without its requested reference. If destination finish subsequently
//! fails, the file remains an observation of that source state, not a claim that
//! the conversion published. A new conversion uses a new file path.
//!
//! The envelope hashes the exact serde serialization of Capture. It contains no
//! connection configuration or credential paths. Existing files are refused;
//! same-directory temporary publication is atomic/no-clobber and fsynced. Hashing
//! streams to a sink, so serializing a capture does not clone all retained rows.
//! Loading is bounded, verifies format/hash/rule/profile/catalogue and never runs
//! SQL from the file. This checksum detects accidental changes, not authorship.
use crate::{Error, KEEP_RULE_VERSION, catalogue, compare::Capture};
use dsa_chain::profiles::Profile;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{self, BufReader, Read, Write},
    path::Path,
};
const FORMAT: u32 = 2;
const MAX_BYTES: u64 = 1 << 30;
#[derive(Serialize)]
struct BorrowedEnvelope<'a> {
    format: u32,
    sha256: &'a str,
    capture: &'a Capture,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    format: u32,
    sha256: String,
    capture: Capture,
}
#[derive(Debug, Serialize)]
pub struct Published {
    pub format: u32,
    pub sha256: String,
    pub cases: usize,
    pub executions: usize,
}
struct Hasher {
    hash: Sha256,
    bytes: u64,
}
impl Write for Hasher {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(data.len() as u64)
            .ok_or_else(|| io::Error::other("capture limit"))?;
        if self.bytes > MAX_BYTES - 1024 {
            return Err(io::Error::other("capture limit"));
        }
        self.hash.update(data);
        Ok(data.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn digest(capture: &Capture) -> Result<String, Error> {
    let mut writer = Hasher {
        hash: Sha256::new(),
        bytes: 0,
    };
    serde_json::to_writer(&mut writer, capture)
        .map_err(|_| Error::Refusal("source_capture_limit"))?;
    Ok(format!("{:x}", writer.hash.finalize()))
}
pub fn preflight(path: &Path) -> Result<(), Error> {
    if std::fs::symlink_metadata(path).is_ok() {
        return Err(Error::Refusal("source_capture_exists"));
    }
    if path.file_name().is_none() {
        return Err(Error::Refusal("invalid_source_capture_path"));
    }
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    tempfile::Builder::new()
        .prefix(".dsa-source-capture-")
        .tempfile_in(parent)
        .map_err(|_| Error::Refusal("invalid_source_capture_path"))?;
    Ok(())
}
pub fn write(path: &Path, capture: &Capture) -> Result<Published, Error> {
    if capture.observation_format != crate::compare::OBSERVATION_FORMAT {
        return Err(Error::Refusal("comparison_observation_format_mismatch"));
    }
    preflight(path)?;
    let sha256 = digest(capture)?;
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut temporary = tempfile::Builder::new()
        .prefix(".dsa-source-capture-")
        .tempfile_in(parent)
        .map_err(|_| Error::Refusal("source_capture_io_error"))?;
    serde_json::to_writer(
        temporary.as_file_mut(),
        &BorrowedEnvelope {
            format: FORMAT,
            sha256: &sha256,
            capture,
        },
    )
    .map_err(|_| Error::Refusal("source_capture_io_error"))?;
    temporary
        .as_file()
        .sync_all()
        .map_err(|_| Error::Refusal("source_capture_io_error"))?;
    temporary
        .persist_noclobber(path)
        .map_err(|_| Error::Refusal("source_capture_publication_failed"))?;
    File::open(parent)
        .and_then(|f| f.sync_all())
        .map_err(|_| Error::Refusal("source_capture_published_durability_uncertain"))?;
    Ok(Published {
        format: FORMAT,
        sha256,
        cases: capture.cases.len(),
        executions: capture.executions,
    })
}
pub fn read(path: &Path, profile: &Profile) -> Result<Capture, Error> {
    let file = File::open(path).map_err(|_| Error::Refusal("invalid_source_capture_file"))?;
    if file
        .metadata()
        .map_err(|_| Error::Refusal("invalid_source_capture_file"))?
        .len()
        > MAX_BYTES
    {
        return Err(Error::Refusal("source_capture_limit"));
    }
    let envelope: Envelope = serde_json::from_reader(BufReader::new(file).take(MAX_BYTES + 1))
        .map_err(|_| Error::Refusal("invalid_source_capture_file"))?;
    if envelope.format != FORMAT {
        return Err(Error::Refusal("source_capture_format_mismatch"));
    }
    if envelope.capture.observation_format != crate::compare::OBSERVATION_FORMAT {
        return Err(Error::Refusal("comparison_observation_format_mismatch"));
    }
    if envelope.capture.keep_rule_version != KEEP_RULE_VERSION {
        return Err(Error::Refusal("comparison_keep_rule_mismatch"));
    }
    if digest(&envelope.capture)? != envelope.sha256 {
        return Err(Error::Refusal("source_capture_hash_mismatch"));
    }
    let capture = envelope.capture;
    if capture.profile_hash != profile.hash()? || capture.network != profile.network {
        return Err(Error::Refusal("comparison_capture_context_mismatch"));
    }
    if capture.node_commit != catalogue::catalogue().node_commit
        || capture.catalogue_sha256 != catalogue::sha256()
    {
        return Err(Error::Refusal("comparison_capture_catalogue_mismatch"));
    }
    capture.positions.validate()?;
    Ok(capture)
}
