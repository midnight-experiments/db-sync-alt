//! Resolve only supplements requested by the current row-building attempt.

use dsa_chain::{
    rows::{DatumAvailability, SupplementRequest, Supplements},
    transaction::{self, Transaction},
};
use dsa_provider::{
    supplements::resolve_outputs,
    types::{CborHex, Metadata, TransactionOutputs},
};
use std::{fs::File, path::Path};

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
                if value.is_some() {
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
        SupplementRequest::Output { transaction, .. } => {
            let reply: TransactionOutputs = serde_json::from_reader(file)?;
            let tx = transactions
                .iter()
                .find(|tx| tx.hash == *transaction)
                .ok_or("unknown requested transaction")?;
            supplements
                .outputs
                .insert(transaction.clone(), resolve_outputs(tx, &reply)?);
        }
    }
    Ok(true)
}
