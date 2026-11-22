//! Initial held origins exported independently from the legacy public views.

use dsa_chain::{
    ids,
    rows::HeldOutput,
    transaction::{Asset, Outpoint, Output},
};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    path::Path,
};

#[derive(Deserialize)]
struct Origins {
    outputs: Vec<Origin>,
    assets: Vec<OriginAsset>,
}
#[derive(Deserialize)]
struct Origin {
    tx_id: i64,
    index: u16,
    hash: String,
    address: String,
    value: String,
    data_hash: Option<String>,
}
#[derive(Deserialize)]
struct OriginAsset {
    tx_out_id: i64,
    policy: String,
    name: String,
    quantity: String,
}

pub fn load(
    path: &Path,
    projected: &BTreeSet<u32>,
) -> Result<BTreeMap<Outpoint, HeldOutput>, Box<dyn std::error::Error>> {
    let origins: Origins = serde_json::from_reader(File::open(path)?)?;
    let mut assets = BTreeMap::<i64, Vec<Asset>>::new();
    for asset in origins.assets {
        assets.entry(asset.tx_out_id).or_default().push(Asset {
            policy: asset.policy,
            name: asset.name,
            quantity: asset.quantity.parse()?,
        });
    }
    let mut held = BTreeMap::new();
    for origin in origins.outputs {
        let (height, transaction) = ids::transaction_position(origin.tx_id)?;
        if projected.contains(&height) {
            continue;
        }
        let output_id = ids::output_id(height, transaction, origin.index)?;
        held.insert(
            Outpoint {
                hash: origin.hash,
                index: origin.index,
            },
            HeldOutput {
                transaction_id: origin.tx_id,
                output: Output {
                    address_raw: Vec::new(),
                    address: Some(origin.address),
                    coin: origin.value.parse()?,
                    assets: assets.remove(&output_id).unwrap_or_default(),
                    data_hash: origin.data_hash,
                    inline_datum: None,
                    classification_safe: true,
                },
            },
        );
    }
    Ok(held)
}
