//! Optional retention of an already received, credential-checked response.
//! No request headers, base URL, key or host path are exposed by this type.
use crate::{Endpoint, Reply};
use dsa_chain::observations::Shape;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    io::{self, Read, Seek, SeekFrom, Write},
    path::Path,
};

pub struct CapturedReply {
    pub(crate) endpoint: Endpoint,
    pub(crate) reply: Reply,
    pub(crate) evidence: Option<RawResponse>,
}
impl std::fmt::Debug for CapturedReply {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CapturedReply")
            .field("has_evidence", &self.evidence.is_some())
            .finish_non_exhaustive()
    }
}
impl CapturedReply {
    pub fn reply(&self) -> &Reply {
        &self.reply
    }
    pub fn into_parts(self) -> (Endpoint, Reply, Option<RawResponse>) {
        (self.endpoint, self.reply, self.evidence)
    }
}
enum Body {
    Buffered(Vec<u8>),
    Spooled(std::fs::File),
}
pub struct RawResponse {
    endpoint: Endpoint,
    body: Body,
    sha256: String,
    length: u64,
    received_at: u64,
}
impl RawResponse {
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }
    pub fn path(&self) -> Result<String, crate::Error> {
        self.endpoint.path()
    }
    pub fn sha256(&self) -> &str {
        &self.sha256
    }
    pub fn length(&self) -> u64 {
        self.length
    }
    pub fn received_at(&self) -> u64 {
        self.received_at
    }
    pub fn reader(&mut self) -> io::Result<Box<dyn Read + Send + '_>> {
        match &mut self.body {
            Body::Buffered(bytes) => Ok(Box::new(io::Cursor::new(bytes))),
            Body::Spooled(file) => {
                file.seek(SeekFrom::Start(0))?;
                Ok(Box::new(file))
            }
        }
    }
    pub(crate) fn buffered(endpoint: Endpoint, body: Vec<u8>, received_at: u64) -> Self {
        Self {
            sha256: format!("{:x}", Sha256::digest(&body)),
            length: body.len() as u64,
            body: Body::Buffered(body),
            endpoint,
            received_at,
        }
    }
}
pub(crate) fn wanted(endpoint: &Endpoint, pending: &BTreeSet<Shape>) -> bool {
    match endpoint {
        Endpoint::BlockCbor { .. } => !pending.is_empty(),
        Endpoint::TransactionOutputs(_) => {
            pending.contains(&Shape::StoredPointerAddress)
                || pending.contains(&Shape::StoredLargeDatum)
        }
        Endpoint::Datum(_) => pending.contains(&Shape::StoredLargeDatum),
        Endpoint::Metadata(_) => pending.contains(&Shape::StoredBridgeMetadata),
        _ => false,
    }
}
pub(crate) struct Spool {
    file: std::fs::File,
    hash: Sha256,
    length: u64,
    scanner: SecretScanner,
}
impl Spool {
    pub(crate) fn new(directory: &Path, key: &str) -> io::Result<Self> {
        Ok(Self {
            file: tempfile::tempfile_in(directory)?,
            hash: Sha256::new(),
            length: 0,
            scanner: SecretScanner::new(key),
        })
    }
    pub(crate) fn write(&mut self, chunk: &[u8]) -> io::Result<()> {
        for &byte in chunk {
            self.scanner.byte(byte)?;
        }
        self.file.write_all(chunk)?;
        self.hash.update(chunk);
        self.length += chunk.len() as u64;
        Ok(())
    }
    pub(crate) fn finish(self, endpoint: Endpoint, received_at: u64) -> RawResponse {
        RawResponse {
            endpoint,
            body: Body::Spooled(self.file),
            sha256: format!("{:x}", self.hash.finalize()),
            length: self.length,
            received_at,
        }
    }
}
/// Constant-space incremental JSON-string credential scan, including escaped
/// spellings and chunk boundaries. The normal decoder validates JSON syntax.
struct SecretScanner {
    key: Vec<u8>,
    prefix: Vec<usize>,
    matched: usize,
    string: bool,
    escape: bool,
    unicode: Option<(u16, u8)>,
    surrogate: Option<u16>,
}
impl SecretScanner {
    fn new(key: &str) -> Self {
        let key = key.as_bytes().to_vec();
        let mut prefix = vec![0; key.len()];
        for i in 1..key.len() {
            let mut j = prefix[i - 1];
            while j > 0 && key[i] != key[j] {
                j = prefix[j - 1];
            }
            if key[i] == key[j] {
                j += 1;
            }
            prefix[i] = j;
        }
        Self {
            key,
            prefix,
            matched: 0,
            string: false,
            escape: false,
            unicode: None,
            surrogate: None,
        }
    }
    fn decoded(&mut self, byte: u8) -> io::Result<()> {
        if self.key.is_empty() {
            return Err(io::Error::other("unsafe_provider_response"));
        }
        while self.matched > 0 && byte != self.key[self.matched] {
            self.matched = self.prefix[self.matched - 1];
        }
        if byte == self.key[self.matched] {
            self.matched += 1;
        }
        if self.matched == self.key.len() {
            return Err(io::Error::other("unsafe_provider_response"));
        }
        Ok(())
    }
    fn byte(&mut self, byte: u8) -> io::Result<()> {
        if !self.string {
            if byte == b'"' {
                self.string = true;
                self.matched = 0;
            }
            return Ok(());
        }
        if let Some((value, digits)) = self.unicode {
            let digit = (byte as char)
                .to_digit(16)
                .ok_or_else(|| io::Error::other("provider_invalid_response"))?
                as u16;
            let value = value * 16 + digit;
            if digits < 3 {
                self.unicode = Some((value, digits + 1));
                return Ok(());
            }
            self.unicode = None;
            if (0xd800..=0xdbff).contains(&value) {
                self.surrogate = Some(value);
                return Ok(());
            }
            let code = if let Some(high) = self.surrogate.take() {
                if !(0xdc00..=0xdfff).contains(&value) {
                    return Err(io::Error::other("provider_invalid_response"));
                }
                0x10000 + ((u32::from(high) - 0xd800) << 10) + (u32::from(value) - 0xdc00)
            } else {
                u32::from(value)
            };
            let character = char::from_u32(code)
                .ok_or_else(|| io::Error::other("provider_invalid_response"))?;
            let mut buffer = [0; 4];
            for &byte in character.encode_utf8(&mut buffer).as_bytes() {
                self.decoded(byte)?;
            }
            return Ok(());
        }
        if self.escape {
            self.escape = false;
            if byte == b'u' {
                self.unicode = Some((0, 0));
                return Ok(());
            }
            return self.decoded(match byte {
                b'b' => 8,
                b'f' => 12,
                b'n' => 10,
                b'r' => 13,
                b't' => 9,
                _ => byte,
            });
        }
        match byte {
            b'\\' => self.escape = true,
            b'"' => {
                self.string = false;
                self.matched = 0;
            }
            _ => self.decoded(byte)?,
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn escaped_credentials_in_discarded_fields_never_enter_evidence() {
        for raw in [
            br#"{"inputs":["synthetic-key"]}"#.as_slice(),
            br#"{"inputs":["synthetic-\u006bey"]}"#.as_slice(),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let mut spool = Spool::new(dir.path(), "synthetic-key").unwrap();
            let mut rejected = false;
            for chunk in raw.chunks(1) {
                if spool.write(chunk).is_err() {
                    rejected = true;
                    break;
                }
            }
            assert!(rejected);
        }
    }
    #[test]
    fn raw_spool_preserves_whitespace_and_unknown_fields_exactly() {
        let raw = b" {\n\"unknown\": [1,2], \"outputs\": [] }\n";
        let dir = tempfile::tempdir().unwrap();
        let mut spool = Spool::new(dir.path(), "synthetic-key").unwrap();
        for chunk in raw.chunks(3) {
            spool.write(chunk).unwrap();
        }
        let mut captured = spool.finish(Endpoint::Metadata("01".repeat(32).parse().unwrap()), 1);
        let mut actual = Vec::new();
        captured.reader().unwrap().read_to_end(&mut actual).unwrap();
        assert_eq!(actual, raw);
        assert_eq!(captured.sha256(), format!("{:x}", Sha256::digest(raw)));
    }
}
