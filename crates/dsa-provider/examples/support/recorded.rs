//! Resolve only supplements requested by the current row-building attempt.

use dsa_chain::{
    rows::{DatumAvailability, SupplementRequest, Supplements},
    transaction::{self, Transaction},
};
use dsa_provider::{
    supplements::resolve_outputs,
    types::{CborHex, Metadata, TransactionOutputs},
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Deserialize)]
struct DatumCapture {
    format: String,
    datum_hash: String,
    response_sha256: String,
    response_observed_no_later_than_unix: u64,
    time_basis: String,
    capture_source_commit: String,
    capture_working_tree: String,
    capture_code_blob: String,
    capture_code_basis: String,
}

fn verified_shared_datum(path: &Path, hash: &str) -> Result<bool, Box<dyn std::error::Error>> {
    let provenance = path.with_extension("provenance.json");
    if !provenance.exists() {
        return Ok(false);
    }
    let capture: DatumCapture = serde_json::from_reader(File::open(provenance)?)?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    if capture.format != "blockfrost-datum-capture-v1"
        || capture.datum_hash != hash
        || capture.response_sha256 != format!("{:x}", Sha256::digest(std::fs::read(path)?))
        || capture.response_observed_no_later_than_unix == 0
        || capture.response_observed_no_later_than_unix > now
        || !matches!(
            capture.time_basis.as_str(),
            "recorder_completion_bound" | "request_completion"
        )
        || capture.capture_source_commit.len() != 40
        || !capture
            .capture_source_commit
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || capture.capture_code_blob.len() != 40
        || !capture
            .capture_code_blob
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || !matches!(
            (
                capture.capture_working_tree.as_str(),
                capture.capture_code_basis.as_str()
            ),
            ("clean", "exact_source_commit") | ("dirty", "identical_datum_capture_branch")
        )
    {
        return Err("invalid_shared_datum_capture_provenance".into());
    }
    Ok(true)
}

pub fn load(
    root: &Path,
    request: &SupplementRequest,
    height: u32,
    transactions: &[Transaction<'_>],
    supplements: &mut Supplements,
) -> Result<bool, Box<dyn std::error::Error>> {
    let path = match request {
        SupplementRequest::Datum(hash) => {
            let shared = root.join(format!("datums/{hash}.json"));
            if shared.exists() {
                let value: Option<CborHex> = serde_json::from_reader(File::open(&shared)?)?;
                if value.is_some() && verified_shared_datum(&shared, hash)? {
                    shared
                } else {
                    root.join(format!("datums/{height}-{hash}.json"))
                }
            } else {
                root.join(format!("datums/{height}-{hash}.json"))
            }
        }
        SupplementRequest::Metadata(hash) => root.join(format!("metadata/{hash}.json")),
        SupplementRequest::Output { transaction, .. } => {
            root.join(format!("outputs/{transaction}.json"))
        }
    };
    if !path.exists() {
        return Ok(false);
    }
    let file = File::open(path)?;
    match request {
        SupplementRequest::Datum(hash) => {
            let datum: Option<CborHex> = serde_json::from_reader(file)?;
            let availability = match datum {
                Some(raw) => {
                    let bytes = hex::decode(raw.as_str())?;
                    if transaction::hash(&bytes) != *hash {
                        return Err("provider_datum_hash_mismatch".into());
                    }
                    DatumAvailability::Present(bytes)
                }
                None => DatumAvailability::NotFound,
            };
            supplements.datums.insert(hash.clone(), availability);
        }
        SupplementRequest::Metadata(hash) => {
            let entries: Vec<Metadata> = serde_json::from_reader(file)?;
            let matches: Vec<_> = entries
                .into_iter()
                .filter(|m| m.label.0 == 6500973)
                .collect();
            if matches.len() > 1 {
                return Err("duplicate bridge metadata label".into());
            }
            supplements.metadata.insert(
                hash.clone(),
                matches.first().map(|m| m.json_metadata.get().to_owned()),
            );
        }
        SupplementRequest::Output { transaction, index } => {
            let reply: TransactionOutputs = serde_json::from_reader(file)?;
            let tx = transactions
                .iter()
                .find(|tx| tx.hash == *transaction)
                .ok_or("unknown requested transaction")?;
            let outputs = resolve_outputs(tx, &reply)?;
            if !outputs.contains_key(index) {
                return Err("recorded_output_missing_requested_index".into());
            }
            supplements.outputs.insert(transaction.clone(), outputs);
        }
    }
    Ok(true)
}
