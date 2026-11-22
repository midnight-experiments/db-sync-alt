//! Bind output-classification supplements to the decoded transaction's effects.

use std::collections::BTreeMap;

use dsa_chain::transaction::{self, Asset, Output, Transaction};

use crate::types::{Hash32, TransactionOutputs};

/// A valid transaction exposes ordinary outputs plus an optional collateral
/// return. An invalid transaction exposes its collateral return as an ordinary
/// stored output at the original body output count, matching db-sync.
pub fn resolve_outputs(
    transaction: &Transaction<'_>,
    reply: &TransactionOutputs,
) -> Result<BTreeMap<u16, Output>, &'static str> {
    let hash: Hash32 = transaction
        .hash()
        .parse()
        .map_err(|_| "provider_invalid_transaction_hash")?;
    reply.validate(&hash)?;
    let count = transaction.outputs().len();
    let valid = transaction.is_valid();
    let has_return = transaction.has_collateral_return();
    let expected_count = if valid {
        count + usize::from(has_return)
    } else {
        usize::from(has_return)
    };
    if reply.outputs.len() != expected_count {
        return Err("provider_output_count_mismatch");
    }
    let mut resolved = BTreeMap::new();
    for row in &reply.outputs {
        let index =
            usize::try_from(row.output_index).map_err(|_| "provider_output_count_mismatch")?;
        let collateral = index == count && has_return;
        if !(collateral || valid && index < count) {
            return Err("provider_output_count_mismatch");
        }
        if row.collateral != (valid && collateral) {
            return Err("provider_output_role_mismatch");
        }
        // A valid transaction never creates its collateral return. The real
        // provider may omit its native assets, so only its index/role matters.
        if valid && collateral {
            continue;
        }
        let original = if collateral {
            transaction.collateral_return()
        } else {
            transaction.outputs()[index].as_ref()
        };
        let parsed =
            transaction::address_bytes(&row.address).ok_or("provider_output_address_mismatch")?;
        if transaction::address_text(&parsed).as_deref() != Some(row.address.as_str()) {
            return Err("provider_output_address_mismatch");
        }
        let mut coin = None;
        let mut assets = Vec::new();
        for amount in &row.amount {
            if amount.unit == "lovelace" {
                coin = Some(amount.quantity.0);
            } else {
                assets.push(Asset {
                    policy: amount.unit[..56].to_owned(),
                    name: amount.unit[56..].to_owned(),
                    quantity: i128::from(amount.quantity.0),
                });
            }
        }
        assets.sort_by(|a, b| (&a.policy, &a.name).cmp(&(&b.policy, &b.name)));
        let output = Output {
            address_raw: original.map(|o| o.address_raw.clone()).unwrap_or(parsed),
            address: Some(row.address.clone()),
            coin: coin.ok_or("provider_output_value_mismatch")?,
            assets,
            data_hash: row.data_hash.as_ref().map(ToString::to_string),
            inline_datum: row.inline_datum.as_ref().map(|bytes| bytes.0.clone()),
        };
        if let Some(bytes) = &output.inline_datum
            && output.data_hash.as_deref() != Some(transaction::hash(bytes).as_str())
        {
            return Err("provider_inline_datum_hash_mismatch");
        }
        if let Some(original) = original {
            if original.coin != output.coin
                || original.assets != output.assets
                || original.data_hash != output.data_hash
                || original.inline_datum != output.inline_datum
            {
                return Err("provider_output_value_mismatch");
            }
            if original
                .address
                .as_ref()
                .is_some_and(|address| Some(address) != output.address.as_ref())
            {
                return Err("provider_output_address_mismatch");
            }
        }
        let index = u16::try_from(index).map_err(|_| "provider_output_count_mismatch")?;
        if index >= dsa_chain::ids::INDEX_LIMIT {
            return Err("provider_output_count_mismatch");
        }
        resolved.insert(index, output);
    }
    Ok(resolved)
}
