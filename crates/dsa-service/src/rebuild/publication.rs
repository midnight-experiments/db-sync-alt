//! The base and the publication: the rows below H with the proof of the
//! supply at H, written with the gate closed; after the window, K10 when the
//! window holds no spend, the pruned form of the keep rule, `verify` and the
//! open gate in one transaction.

use std::collections::BTreeMap;

use chrono::DateTime;
use dsa_chain::{
    assets::fixed_units,
    block::{AssetRow, DatumRow, OutputRow, Rows, TxRow},
    ids,
    profiles::fixed_descriptor_ids,
};
use dsa_provider::types::Header;
use dsa_store::{
    follow::{BlockRow, EpochParamRow},
    rebuild::{Base, BuiltRows},
    verify::{ServingState, Verification},
};
use serde_json::{Value, json};

use super::{
    Build,
    asker::{check, invalid, refusal},
    content::{Carried, Content, Event, Place},
};
use crate::follow::Error;

fn block_row(header: &Header) -> BlockRow {
    BlockRow {
        height: header.height,
        hash: header.hash.to_string(),
        epoch: header.epoch,
        slot: header.slot,
        time: i64::try_from(header.time)
            .ok()
            .and_then(|seconds| DateTime::from_timestamp(seconds, 0))
            .map_or(chrono::NaiveDateTime::MIN, |time| time.naive_utc()),
        transaction_count: header.tx_count,
    }
}

impl Build<'_> {
    /// The rows of one kept output below H with its creating transaction.
    pub(super) fn output_rows(
        &self,
        rows: &mut Rows,
        point: &(String, u16),
        content: &Content,
        place: Place,
        valid: bool,
        datums: &BTreeMap<String, Value>,
    ) -> Result<(), Error> {
        let tx_id = ids::transaction_id(place.height, place.index).map_err(invalid)?;
        if !rows.tx.iter().any(|row| row.id == tx_id) {
            rows.tx.push(TxRow {
                id: tx_id,
                hash: point.0.clone(),
                block_id: i64::from(place.height),
                block_index: place.index,
                valid_contract: valid,
            });
        }
        let output_id = ids::output_id(place.height, place.index, point.1).map_err(invalid)?;
        if rows.tx_out.iter().any(|row| row.id == output_id) {
            return Ok(());
        }
        rows.tx_out.push(OutputRow {
            id: output_id,
            tx_id,
            index: point.1,
            address: content.address.clone(),
            value: content.lovelace().to_string(),
            data_hash: content.data_hash.clone(),
            consumed_by_tx_id: None,
        });
        for (ordinal, (policy, name, quantity)) in content.assets().into_iter().enumerate() {
            if !self.rule.allows(&policy, &name) {
                continue;
            }
            let ordinal = u16::try_from(ordinal).unwrap_or(u16::MAX);
            rows.ma_tx_out.push(AssetRow {
                id: ids::asset_id(place.height, place.index, point.1, ordinal).map_err(invalid)?,
                quantity: quantity.to_string(),
                tx_out_id: output_id,
                policy,
                name,
            });
        }
        if self.needs_datum(content)
            && let Some(bytes) = Self::datum_bytes(content, datums)?
        {
            let data_hash = content.data_hash.clone().expect("hash of the bytes");
            if !rows.datum.iter().any(|row| row.hash == data_hash) {
                rows.datum.push(DatumRow {
                    hash: data_hash,
                    value: dsa_chain::datum::render(&bytes).map_err(|error| {
                        Error::answer(error.code, json!({"datum": content.data_hash}))
                    })?,
                });
            }
        }
        Ok(())
    }

    /// Descriptor IDs: the fixed ones of the network's table when their unit
    /// was first minted at or below T, the others in chain order after them.
    async fn descriptors(&mut self, rows: &Rows) -> Result<Vec<(i64, String, String)>, Error> {
        let quantities = self.items("quantity").await?;
        let table = fixed_descriptor_ids(self.profile);
        let mut descriptors = Vec::new();
        for (unit, id) in &table {
            let available = quantities
                .get(unit)
                .and_then(|value| value["first_mint_height"].as_u64())
                .is_some_and(|height| height <= u64::from(self.plan.target));
            if available {
                descriptors.push((*id, unit[..56].to_owned(), unit[56..].to_owned()));
            }
        }
        let mut next = table.iter().map(|(_, id)| *id).max().unwrap_or(0) + 1;
        let mut others: Vec<(i64, String)> = rows
            .ma_tx_out
            .iter()
            .map(|row| (row.id, format!("{}{}", row.policy, row.name)))
            .filter(|(_, unit)| !table.iter().any(|(known, _)| known == unit))
            .collect();
        others.sort();
        for (_, unit) in others {
            if descriptors
                .iter()
                .any(|(_, policy, name)| format!("{policy}{name}") == unit)
            {
                continue;
            }
            descriptors.push((next, unit[..56].to_owned(), unit[56..].to_owned()));
            next += 1;
        }
        Ok(descriptors)
    }

    /// The base: rows below H, supply at H after the proof, the position.
    pub(super) async fn base(&mut self) -> Result<Value, Error> {
        let unspent = self.unspent_at_horizon().await?;
        let valid: BTreeMap<String, bool> = self.typed("valid").await?;
        let datums = self.items("datum").await?;
        let carried: BTreeMap<String, Vec<Carried>> = self.typed("carried").await?;
        let headers: BTreeMap<String, Header> = self.typed("header").await?;
        let params = self.items("param").await?;
        let mut rows = Rows::default();
        for (point, (content, place)) in &unspent {
            let valid = *valid.get(&point.0).ok_or_else(|| {
                refusal("rebuild_validity_missing", json!({"transaction": point.0}))
            })?;
            self.output_rows(&mut rows, point, content, *place, valid, &datums)?;
        }
        let mut block_rows: BTreeMap<u32, BlockRow> = BTreeMap::new();
        let mut markers = Vec::new();
        for carry in carried.values().flatten() {
            block_rows.insert(carry.header.height, block_row(&carry.header));
            for (index, content) in &carry.outputs {
                let point = (carry.tx.clone(), *index);
                self.output_rows(
                    &mut rows,
                    &point,
                    content,
                    carry.place,
                    carry.valid,
                    &datums,
                )?;
                if !unspent.contains_key(&point) {
                    markers.push(
                        ids::output_id(carry.place.height, carry.place.index, *index)
                            .map_err(invalid)?,
                    );
                }
            }
        }
        for (key, header) in &headers {
            if key.starts_with("epoch:") || key.starts_with("chain:") {
                block_rows.insert(header.height, block_row(header));
            }
        }
        let epoch_params = params
            .iter()
            .map(|(epoch, value)| {
                Ok(EpochParamRow {
                    epoch: epoch
                        .parse()
                        .map_err(|_| refusal("rebuild_progress_invalid", json!({})))?,
                    nonce: value["nonce"].as_str().unwrap_or_default().to_owned(),
                    max_tx_size: value["max_tx_size"].as_u64().unwrap_or_default() as u32,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        // Proof: the unspent amount at H of each fixed unit equals the
        // provider's quantity minus the net mint since H up to its tip.
        let supply = self.proof(&unspent).await?;
        let descriptors = self.descriptors(&rows).await?;
        let k = self.profile.cardano.security_parameter;
        let header = |key: &str| {
            headers
                .get(key)
                .map(block_row)
                .ok_or_else(|| refusal("rebuild_header_missing", json!({"header": key})))
        };
        let horizon = header("horizon")?;
        let target = header(&format!("chain:{}", self.plan.horizon - 1))?;
        let tip = header(&format!("chain:{}", self.plan.horizon - 1 + k))?;
        let counts = json!({"unspent_at_horizon": unspent.len(),
            "transactions": rows.tx.len(), "outputs": rows.tx_out.len(), "asset_rows": rows.ma_tx_out.len(),
            "datums": rows.datum.len(), "headers": block_rows.len(), "markers": markers.len(),
            "carried": carried.values().map(Vec::len).sum::<usize>()});
        let base = Base {
            built: BuiltRows {
                headers: block_rows.into_values().collect(),
                rows,
                descriptors,
                epoch_params,
                markers,
            },
            supply,
            state: ServingState::of_headers(&horizon, &target, &tip, self.plan.retention_seconds),
        };
        let profile = self.profile;
        self.store()
            .await?
            .write_building_base(profile, &base)
            .await?;
        Ok(counts)
    }

    async fn proof(
        &mut self,
        unspent: &BTreeMap<(String, u16), (Content, Place)>,
    ) -> Result<BTreeMap<String, String>, Error> {
        let quantities = self.items("quantity").await?;
        let since: BTreeMap<String, Place> = self.typed("since").await?;
        let mut supply = BTreeMap::new();
        for unit in fixed_units(self.profile) {
            let at_horizon: u128 = unspent
                .values()
                .map(|(content, _)| content.quantity(&unit))
                .sum();
            let reading = &quantities[&unit];
            let quantity: i128 = reading["quantity"]
                .as_str()
                .map_or(Ok(0), str::parse)
                .map_err(|_| Error::answer("provider_quantity_invalid", json!({"unit": unit})))?;
            let tip_before = reading["tip_before"].as_u64().unwrap_or_default();
            let tip_after = reading["tip_after"].as_u64().unwrap_or_default();
            // The mints and burns from H on, from the unit's history: every
            // one of them is a transaction from H on, placed by a list or by
            // its own answer; one that is not is below H.
            let mut minted: i128 = 0;
            for event in Event::of(reading)? {
                let Some(place) = since.get(&event.tx) else {
                    continue;
                };
                let height = u64::from(place.height);
                if height > tip_before && height <= tip_after {
                    return Err(Error::answer(
                        "provider_mint_while_quantity_read",
                        json!({"unit": unit, "transaction": event.tx}),
                    ));
                }
                if height <= tip_before {
                    minted += event.quantity()?;
                }
            }
            let expected = quantity - minted;
            if i128::try_from(at_horizon).ok() != Some(expected) {
                return Err(check(
                    "rebuild_supply_mismatch",
                    json!({"unit": unit, "unspent_at_horizon": at_horizon.to_string(),
                        "provider_quantity": quantity.to_string(), "net_mint_since_horizon": minted.to_string(),
                        "provider_tip": tip_before}),
                ));
            }
            supply.insert(unit, at_horizon.to_string());
        }
        Ok(supply)
    }

    /// K10 when the window holds no spend, then the publication of the
    /// build with the serving state of its stored headers. Returns the
    /// verification and the build's provider requests.
    pub(super) async fn publish(&mut self) -> Result<(Verification, i64), Error> {
        let plan = self.plan.clone();
        let newest_spend = {
            let window_spent = self
                .store()
                .await?
                .window_has_spend(plan.horizon, plan.target)
                .await?;
            if window_spent {
                None
            } else {
                self.newest_spend().await?
            }
        };
        let headers: BTreeMap<String, Header> = self.typed("header").await?;
        let expected = {
            let row = |key: &str| {
                headers
                    .get(key)
                    .map(block_row)
                    .ok_or_else(|| refusal("rebuild_header_missing", json!({"header": key})))
            };
            let tip_row = BlockRow {
                height: plan.tip,
                ..row("target")?
            };
            ServingState::of_headers(
                &row("horizon")?,
                &row("target")?,
                &tip_row,
                plan.retention_seconds,
            )
        };
        let calls = self.calls();
        let origin = json!({"kind":"rebuilt","source":"provider","horizon_height":plan.horizon,
            "target_height":plan.target,"tip_height":plan.tip,"window_seconds":plan.window_seconds,
            "provider_requests":calls});
        let profile = self.profile;
        let per_day = self.asker.provider.counts();
        let store = self.store().await?;
        store.record_calls(&per_day).await?;
        let verified = store
            .publish_building(profile, &expected, newest_spend.as_ref(), &origin)
            .await?;
        eprintln!(
            "{}",
            json!({"event":"rebuild_published","content_digest":verified.content_digest,"calls":calls})
        );
        Ok((verified, calls))
    }
}
