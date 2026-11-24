//! The records a build stores about outputs and transactions.

use dsa_provider::types::{AssetAmount, Header, ListedOutput};
use serde::{Deserialize, Serialize};

/// The content of an output as a listing or an input row renders it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Content {
    pub(super) address: String,
    pub(super) amount: Vec<AssetAmount>,
    pub(super) data_hash: Option<String>,
    pub(super) inline_datum: Option<String>,
}

impl Content {
    pub(super) fn of_listed(row: &ListedOutput) -> Self {
        Self {
            address: row.address.clone(),
            amount: row.amount.clone(),
            data_hash: row.data_hash.as_ref().map(ToString::to_string),
            inline_datum: row.inline_datum.as_ref().map(|bytes| hex::encode(&bytes.0)),
        }
    }

    pub(super) fn lovelace(&self) -> u64 {
        self.amount
            .iter()
            .find(|amount| amount.unit == "lovelace")
            .map_or(0, |amount| amount.quantity.0)
    }

    /// The native assets in bytewise policy/name order, zero quantities left
    /// out: the order whose rank is an asset row's ordinal.
    pub(super) fn assets(&self) -> Vec<(String, String, u64)> {
        let mut assets: Vec<(String, String, u64)> = self
            .amount
            .iter()
            .filter(|amount| amount.unit != "lovelace" && amount.quantity.0 > 0)
            .map(|amount| {
                (
                    amount.unit[..56].to_owned(),
                    amount.unit[56..].to_owned(),
                    amount.quantity.0,
                )
            })
            .collect();
        assets.sort();
        assets
    }

    pub(super) fn quantity(&self, unit: &str) -> u128 {
        self.amount
            .iter()
            .filter(|amount| amount.unit == unit)
            .map(|amount| u128::from(amount.quantity.0))
            .sum()
    }
}

/// An input of a transaction from H on, as stored.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Spent {
    pub(super) tx_hash: String,
    pub(super) index: u16,
    pub(super) collateral: bool,
    pub(super) reference: bool,
    /// Kept for relevant and collateral inputs only.
    pub(super) content: Option<Content>,
}

/// An output of a transaction from H on: its eligible amounts only.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Made {
    pub(super) index: u16,
    pub(super) collateral: bool,
    pub(super) amount: Vec<AssetAmount>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(super) struct Moved {
    pub(super) unknown: bool,
    pub(super) inputs: Vec<Spent>,
    pub(super) outputs: Vec<Made>,
}

/// A position: block height and index in the block.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(super) struct Place {
    pub(super) height: u32,
    pub(super) index: u16,
}

/// A transaction kept below H by K5 or K6, with the outputs it keeps.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Carried {
    pub(super) tx: String,
    pub(super) place: Place,
    pub(super) valid: bool,
    pub(super) header: Header,
    pub(super) outputs: Vec<(u16, Content)>,
}

/// The newest relevant spend below H (K10).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct NewestSpend {
    pub(super) tx: String,
    pub(super) place: Place,
    pub(super) valid: bool,
    pub(super) ordinal: u16,
    pub(super) origin: String,
    pub(super) origin_index: u16,
    pub(super) origin_place: Place,
    pub(super) origin_valid: bool,
    pub(super) content: Content,
}

pub(super) fn outpoint(hash: &str, index: impl std::fmt::Display) -> String {
    format!("{hash}#{index}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsa_provider::types::Quantity;

    fn amount(unit: &str, quantity: u64) -> AssetAmount {
        AssetAmount {
            unit: unit.to_owned(),
            quantity: Quantity(quantity),
        }
    }

    #[test]
    fn asset_ordinals_rank_the_full_asset_list_in_bytewise_order() {
        let content = Content {
            address: "addr".to_owned(),
            amount: vec![
                amount("lovelace", 5),
                amount(&format!("{}01", "bb".repeat(28)), 1),
                amount(&"aa".repeat(28), 2),
                amount(&format!("{}00", "bb".repeat(28)), 0),
            ],
            data_hash: None,
            inline_datum: None,
        };
        let assets = content.assets();
        assert_eq!(assets.len(), 2);
        assert_eq!(assets[0].0, "aa".repeat(28));
        assert_eq!(assets[1].1, "01");
        assert_eq!(content.lovelace(), 5);
    }
}
