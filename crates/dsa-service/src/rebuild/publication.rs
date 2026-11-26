//! The base and the publication: the rows below H with the proof of the
//! supply at H, written with the gate closed; after the window, K10 when the
//! window holds no spend, the pruned form of the keep rule, `verify` and the
//! open gate in one transaction.

use std::collections::{BTreeMap, HashSet};

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
    Build, Relevance,
    asker::{check, index16, invalid, refusal},
    content::{Carried, Content, Event, Place, Unspent},
};
use crate::follow::{Error, block_row};

/// The rows of kept outputs below H and of their creating transactions,
/// with the keys written so far: each row is written once, found by key.
#[derive(Debug, Default)]
pub(super) struct Collected {
    pub(super) rows: Rows,
    transactions: HashSet<i64>,
    outputs: HashSet<i64>,
    datums: HashSet<String>,
}

impl Collected {
    /// A transaction row, written once.
    pub(super) fn transaction(&mut self, row: TxRow) {
        if self.transactions.insert(row.id) {
            self.rows.tx.push(row);
        }
    }

    /// The rows of one kept output below H with its creating transaction,
    /// each written once.
    pub(super) fn output(
        &mut self,
        relevance: &Relevance,
        point: &(String, u16),
        content: &Content,
        place: Place,
        valid: bool,
        datums: &BTreeMap<String, Value>,
    ) -> Result<(), Error> {
        let tx_id = ids::transaction_id(place.height, place.index).map_err(invalid)?;
        self.transaction(TxRow {
            id: tx_id,
            hash: point.0.clone(),
            block_id: i64::from(place.height),
            block_index: place.index,
            valid_contract: valid,
        });
        let output_id = ids::output_id(place.height, place.index, point.1).map_err(invalid)?;
        if !self.outputs.insert(output_id) {
            return Ok(());
        }
        self.rows.tx_out.push(OutputRow {
            id: output_id,
            tx_id,
            index: point.1,
            address: content.address.clone(),
            value: content.lovelace().to_string(),
            data_hash: content.data_hash.clone(),
            consumed_by_tx_id: None,
        });
        for (ordinal, (policy, name, quantity)) in content.assets().into_iter().enumerate() {
            if !relevance.rule.allows(&policy, &name) {
                continue;
            }
            let ordinal = index16(ordinal)?;
            self.rows.ma_tx_out.push(AssetRow {
                id: ids::asset_id(place.height, place.index, point.1, ordinal).map_err(invalid)?,
                quantity: quantity.to_string(),
                tx_out_id: output_id,
                policy,
                name,
            });
        }
        if relevance.needs_datum(content)
            && let Some(bytes) = Build::datum_bytes(content, datums)?
        {
            let data_hash = content.data_hash.clone().expect("hash of the bytes");
            if self.datums.insert(data_hash.clone()) {
                self.rows.datum.push(DatumRow {
                    hash: data_hash,
                    value: dsa_chain::datum::render(&bytes).map_err(|error| {
                        Error::answer(error.code, json!({"datum": content.data_hash}))
                    })?,
                });
            }
        }
        Ok(())
    }
}

/// The rows below H with the carried transactions' headers by height and
/// the outputs marked spent before H.
pub(super) struct Below {
    pub(super) rows: Collected,
    pub(super) headers: BTreeMap<u32, BlockRow>,
    pub(super) markers: Vec<i64>,
}

/// The rows below H: every output unspent at H with its creating
/// transaction, and the outputs of the carried transactions, those of them
/// spent before H marked.
pub(super) fn rows_below_horizon(
    relevance: &Relevance,
    unspent: &Unspent,
    valid: &BTreeMap<String, bool>,
    datums: &BTreeMap<String, Value>,
    carried: &BTreeMap<String, Vec<Carried>>,
) -> Result<Below, Error> {
    let mut rows = Collected::default();
    for (point, (content, place)) in unspent {
        let valid = *valid
            .get(&point.0)
            .ok_or_else(|| refusal("rebuild_validity_missing", json!({"transaction": point.0})))?;
        rows.output(relevance, point, content, *place, valid, datums)?;
    }
    let mut block_rows: BTreeMap<u32, BlockRow> = BTreeMap::new();
    let mut markers = Vec::new();
    for carry in carried.values().flatten() {
        block_rows.insert(carry.header.height, block_row(&carry.header));
        for (index, content) in &carry.outputs {
            let point = (carry.tx.clone(), *index);
            rows.output(relevance, &point, content, carry.place, carry.valid, datums)?;
            if !unspent.contains_key(&point) {
                markers.push(
                    ids::output_id(carry.place.height, carry.place.index, *index)
                        .map_err(invalid)?,
                );
            }
        }
    }
    Ok(Below {
        rows,
        headers: block_rows,
        markers,
    })
}

impl Build<'_> {
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
        let fixed: HashSet<&str> = table.iter().map(|(unit, _)| unit.as_str()).collect();
        let mut others: Vec<(i64, String)> = rows
            .ma_tx_out
            .iter()
            .map(|row| (row.id, format!("{}{}", row.policy, row.name)))
            .filter(|(_, unit)| !fixed.contains(unit.as_str()))
            .collect();
        others.sort();
        let mut written: HashSet<String> = HashSet::new();
        for (_, unit) in others {
            if !written.insert(unit.clone()) {
                continue;
            }
            descriptors.push((next, unit[..56].to_owned(), unit[56..].to_owned()));
            next += 1;
        }
        Ok(descriptors)
    }

    /// The base: rows below H, supply at H after the proof, the position.
    pub(super) async fn base(&mut self) -> Result<(), Error> {
        let unspent = self.unspent_at_horizon().await?;
        let valid: BTreeMap<String, bool> = self.typed("valid").await?;
        let datums = self.items("datum").await?;
        let carried: BTreeMap<String, Vec<Carried>> = self.typed("carried").await?;
        let headers: BTreeMap<String, Header> = self.typed("header").await?;
        let params = self.items("param").await?;
        let Below {
            rows,
            headers: mut block_rows,
            markers,
        } = rows_below_horizon(&self.relevance, &unspent, &valid, &datums, &carried)?;
        let rows = rows.rows;
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
        // Proof: for every unit that has a reading, the unspent amount at H
        // equals the provider's quantity minus the net mint of the events of
        // its history whose transactions are placed from H on.
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
        self.put_one("counts", "base", &counts).await?;
        let profile = self.profile;
        self.store()
            .await?
            .write_building_base(profile, &base)
            .await?;
        Ok(())
    }

    /// Proof: for every unit that has a reading, the unspent amount at H
    /// equals the provider's quantity minus the mints and plus the burns of
    /// its history whose transactions are placed from H on, by a list from
    /// H on or by their own answer, whatever their height: the quantity holds
    /// every event of the history. Returns the supply at H of the fixed
    /// units.
    async fn proof(&mut self, unspent: &Unspent) -> Result<BTreeMap<String, String>, Error> {
        let readings = self.items("quantity").await?;
        let since: BTreeMap<String, Place> = self.typed("since").await?;
        let fixed = fixed_units(self.profile);
        let mut supply = BTreeMap::new();
        for (unit, reading) in &readings {
            let at_horizon: u128 = unspent
                .values()
                .map(|(content, _)| content.quantity(unit))
                .sum();
            let quantity: i128 = reading["quantity"]
                .as_str()
                .map_or(Ok(0), str::parse)
                .map_err(|_| Error::answer("provider_quantity_invalid", json!({"unit": unit})))?;
            let mut minted: i128 = 0;
            for event in Event::of(reading)? {
                if since.contains_key(&event.tx) {
                    minted += event.quantity()?;
                }
            }
            let expected = quantity - minted;
            if i128::try_from(at_horizon).ok() != Some(expected) {
                return Err(check(
                    "rebuild_supply_mismatch",
                    json!({"unit": unit, "unspent_at_horizon": at_horizon.to_string(),
                        "provider_quantity": quantity.to_string(), "net_mint_since_horizon": minted.to_string()}),
                ));
            }
            if fixed.contains(unit) {
                supply.insert(unit.clone(), at_horizon.to_string());
            }
        }
        for unit in &fixed {
            if !supply.contains_key(unit) {
                return Err(refusal("rebuild_reading_missing", json!({"unit": unit})));
            }
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

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use dsa_chain::profiles::Profile;
    use dsa_provider::types::{AssetAmount, Quantity};

    use super::*;

    /// `count` outputs of cNIGHT at addresses of no profile, two to a
    /// creating transaction, each transaction at a place of its own.
    fn outputs(profile: &Profile, count: usize) -> (Unspent, BTreeMap<String, bool>) {
        let unit = profile.assets.cnight.unit();
        let mut unspent = BTreeMap::new();
        let mut valid = BTreeMap::new();
        for n in 0..count {
            let transaction = n / 2;
            let hash = format!("{transaction:064x}");
            let place = Place {
                height: 1_000 + (transaction / 100) as u32,
                index: (transaction % 100) as u16,
            };
            let content = Content {
                address: format!("addr_test1q{n}"),
                amount: vec![
                    AssetAmount {
                        unit: "lovelace".to_owned(),
                        quantity: Quantity(1_000_000),
                    },
                    AssetAmount {
                        unit: unit.clone(),
                        quantity: Quantity(1),
                    },
                ],
                data_hash: None,
                inline_datum: None,
            };
            unspent.insert((hash.clone(), (n % 2) as u16), (content, place));
            valid.insert(hash, true);
        }
        (unspent, valid)
    }

    fn collect(count: usize) -> (Collected, f64) {
        let profile = Profile::load("mainnet").expect("the profile");
        let relevance = Relevance::of(&profile);
        let (unspent, valid) = outputs(&profile, count);
        let started = Instant::now();
        let below = rows_below_horizon(
            &relevance,
            &unspent,
            &valid,
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .expect("the rows");
        assert!(below.markers.is_empty());
        (below.rows, started.elapsed().as_secs_f64())
    }

    #[test]
    fn the_rows_below_the_horizon_hold_each_row_once() {
        let (rows, _) = collect(1_000);
        assert_eq!(
            (
                rows.rows.tx.len(),
                rows.rows.tx_out.len(),
                rows.rows.ma_tx_out.len()
            ),
            (500, 1_000, 1_000)
        );
    }

    /// The rows of the base for 250,000 outputs (DSA_BASE_OUTPUTS sets the
    /// number), timed: about the unspent cNIGHT outputs of Mainnet.
    #[test]
    #[ignore = "a timing, run on its own"]
    fn the_rows_of_many_outputs_are_collected() {
        let count = std::env::var("DSA_BASE_OUTPUTS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(250_000);
        let (rows, seconds) = collect(count);
        println!(
            "{}",
            json!({"event":"base_rows","outputs":count,"transactions":rows.rows.tx.len(),
                "seconds":seconds})
        );
        assert_eq!(rows.rows.tx_out.len(), count);
    }
}
