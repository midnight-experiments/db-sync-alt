//! Steps 1 and 2: the relevant unspent outputs listed now, the quantity of
//! each fixed unit, every transaction from H on with its resolved inputs,
//! and the original content of collateral inputs.

use std::collections::{BTreeMap, BTreeSet};

use dsa_chain::assets::fixed_units;
use dsa_provider::types::{
    AddressText, Heights, ListedOutput, ListedTransaction, Page, TransactionUtxos, Unit,
};
use futures_util::StreamExt;
use serde_json::json;

use super::{
    BATCH, Build,
    asker::{PASSES, every_page, hash32, stable, transactions_page},
    content::{Content, Event, Made, Moved, Spent, outpoint},
    progress::{Cursor, to_value},
};
use crate::follow::Error;

impl Build<'_> {
    /// The sources of the listings: units and addresses, as `unit:` and
    /// `address:` keys.
    pub(super) async fn units(&mut self) -> Result<Vec<String>, Error> {
        if self.cursor("units").await?.done {
            return Ok(self.items("unit").await?.into_keys().collect());
        }
        let mut units: BTreeSet<String> = self.rule.exact_units().clone();
        for policy in self.rule.governance_policies().clone() {
            let provider = self.asker.provider.clone();
            let rows = every_page(&self.asker, "listings", |page| {
                let (provider, policy) = (provider.clone(), policy.clone());
                async move { provider.policy_assets(&policy, page).await }
            })
            .await?;
            units.extend(rows.into_iter().map(|row| row.asset));
        }
        let items = units.iter().map(|unit| (unit.clone(), json!({}))).collect();
        self.put("unit", items).await?;
        self.finished("units").await?;
        Ok(units.into_iter().collect())
    }

    pub(super) async fn listing(&mut self) -> Result<(), Error> {
        for unit in self.units().await? {
            self.list_unit(&unit).await?;
        }
        for address in self.addresses.clone() {
            self.list_address(&address).await?;
        }
        Ok(())
    }

    async fn listed(&mut self, rows: &[ListedOutput]) -> Result<(), Error> {
        let items = rows
            .iter()
            .map(|row| {
                Ok((
                    outpoint(row.tx_hash.as_str(), row.output_index),
                    to_value(row)?,
                ))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        self.put("listed", items).await
    }

    /// The asset's unspent outputs created below H, in height ranges that
    /// each fit one answer.
    async fn list_unit(&mut self, unit: &str) -> Result<(), Error> {
        let key = format!("unit:{unit}");
        let mut cursor = self.cursor(&key).await?;
        let parsed: Unit = unit.parse().map_err(Error::Provider)?;
        let below = self.plan.horizon - 1;
        while !cursor.done {
            let from = cursor.from;
            if from > below {
                cursor.done = true;
                self.put_one("cursor", &key, &cursor).await?;
                break;
            }
            let provider = self.asker.provider.clone();
            let range = Heights {
                from: Some(from),
                to: Some(below),
            };
            let rows = self
                .asker
                .ask("listings", || {
                    provider.asset_outputs(&parsed, range, Page::first())
                })
                .await?;
            let (taken, next) = if rows.len() < 100 {
                (rows, None)
            } else {
                let last = rows[99].block_height.unwrap_or(from);
                if last > from {
                    let taken = rows
                        .into_iter()
                        .filter(|row| row.block_height.is_some_and(|height| height < last))
                        .collect();
                    (taken, Some(last))
                } else {
                    (self.outputs_at_height(&parsed, from).await?, Some(from + 1))
                }
            };
            self.listed(&taken).await?;
            match next {
                Some(next) => cursor.from = next,
                None => cursor.done = true,
            }
            self.put_one("cursor", &key, &cursor).await?;
        }
        Ok(())
    }

    /// The unspent outputs of `unit` created at one height that holds 100
    /// or more of them: the unit's transactions at that height (final: below
    /// H) and the outputs each one created that no transaction has spent.
    /// No page of a moving list is involved.
    async fn outputs_at_height(
        &self,
        unit: &Unit,
        height: u32,
    ) -> Result<Vec<ListedOutput>, Error> {
        let at = Heights {
            from: Some(height),
            to: Some(height),
        };
        let provider = self.asker.provider.clone();
        let transactions = every_page(&self.asker, "listings", |page| {
            let (provider, unit) = (provider.clone(), unit.clone());
            async move { provider.asset_transactions(&unit, at, page, false).await }
        })
        .await?;
        let mut rows = Vec::new();
        for listed in transactions {
            let key = listed.tx_hash.clone();
            let answer = self
                .asker
                .ask("outputs", || provider.transaction_utxos(&key))
                .await?
                .ok_or_else(|| {
                    Error::answer("provider_transaction_unknown", json!({"transaction": key}))
                })?;
            let info = self
                .asker
                .ask("transactions", || provider.transaction(&key))
                .await?
                .ok_or_else(|| {
                    Error::answer("provider_transaction_unknown", json!({"transaction": key}))
                })?;
            for output in answer.outputs {
                let holds = output
                    .amount
                    .iter()
                    .any(|amount| amount.unit == unit.as_str() && amount.quantity.0 > 0);
                if output.collateral == info.valid_contract
                    || output.consumed_by_tx.is_some()
                    || !holds
                {
                    continue;
                }
                rows.push(ListedOutput {
                    address: output.address,
                    tx_hash: listed.tx_hash.clone(),
                    output_index: output.output_index,
                    amount: output.amount,
                    block_height: Some(height),
                    data_hash: output.data_hash,
                    inline_datum: output.inline_datum,
                });
            }
        }
        Ok(rows)
    }

    async fn list_address(&mut self, address: &str) -> Result<(), Error> {
        let key = format!("address:{address}");
        if self.cursor(&key).await?.done {
            return Ok(());
        }
        let parsed: AddressText = address.parse().map_err(Error::Provider)?;
        let provider = self.asker.provider.clone();
        let rows = stable(
            &self.asker,
            "listings",
            |row: &ListedOutput| outpoint(row.tx_hash.as_str(), row.output_index),
            |page| {
                let (provider, parsed) = (provider.clone(), parsed.clone());
                async move { provider.address_outputs(&parsed, page).await }
            },
        )
        .await?;
        self.listed(&rows).await?;
        self.put_one(
            "cursor",
            &key,
            Cursor {
                done: true,
                ..Cursor::default()
            },
        )
        .await
    }

    /// The quantity and the mints and burns (`/assets/{unit}/history`) of
    /// each unit of the listing and each fixed unit, read between two
    /// readings of the provider's tip, with the height of each fixed unit's
    /// first mint. A unit's history must hold as many mints and burns as its
    /// quantity's answer counts.
    pub(super) async fn quantities(&mut self) -> Result<(), Error> {
        if !self.items("quantity").await?.is_empty() {
            return Ok(());
        }
        let fixed = fixed_units(self.profile);
        let mut units: BTreeSet<String> = self.units().await?.into_iter().collect();
        units.extend(fixed.iter().cloned());
        let provider = self.asker.provider.clone();
        let mut last = None;
        for _ in 0..10 {
            let before = self
                .asker
                .ask("headers", || provider.latest_block())
                .await?;
            let mut answers = BTreeMap::new();
            for unit in &units {
                let parsed: Unit = unit.parse().map_err(Error::Provider)?;
                let answer = self.asker.ask("assets", || provider.asset(&parsed)).await?;
                let events = every_page(&self.asker, "assets", |page| {
                    let (provider, parsed) = (provider.clone(), parsed.clone());
                    async move { provider.asset_history(&parsed, page).await }
                })
                .await?;
                answers.insert(unit.clone(), (answer, events));
            }
            let after = self
                .asker
                .ask("headers", || provider.latest_block())
                .await?;
            let steady = before.hash == after.hash;
            last = Some((before, after, answers));
            if steady {
                break;
            }
        }
        let (before, after, answers) = last.expect("one reading");
        let steady = before.hash == after.hash;
        let mut items = Vec::new();
        for (unit, (answer, events)) in answers {
            let count = answer.as_ref().map_or(0, |info| info.mint_or_burn_count);
            if events.len() as u64 != count {
                // At one tip the two answers must agree; across a moving tip
                // a mint or burn may have landed between them.
                let code = if steady {
                    "provider_mint_history_mismatch"
                } else {
                    "provider_mint_while_quantity_read"
                };
                return Err(Error::answer(
                    code,
                    json!({"unit": unit, "mint_or_burn_count": count, "history_rows": events.len(),
                        "tip_before": before.height, "tip_after": after.height}),
                ));
            }
            let first_mint = match &answer {
                Some(info) if fixed.contains(&unit) => {
                    let mint = info.initial_mint_tx_hash.clone();
                    self.asker
                        .ask("transactions", || provider.transaction(&mint))
                        .await?
                        .map(|tx| tx.block_height)
                }
                _ => None,
            };
            let events: Vec<Event> = events
                .iter()
                .map(|event| Event {
                    tx: event.tx_hash.to_string(),
                    amount: event.amount.clone(),
                })
                .collect();
            items.push((
                unit,
                json!({"quantity": answer.as_ref().map(|info| info.quantity.clone()),
                    "mints": count, "events": events,
                    "first_mint_height": first_mint, "tip_before": before.height,
                    "tip_after": after.height}),
            ));
        }
        self.put("quantity", items).await
    }

    /// The mints and burns that no list from H on names. A mint creates an
    /// output holding its unit, so a mint missing from the unit's list from
    /// H on is below H. A burn of a unit's whole input amount creates none and
    /// is in no list of the unit: every burn no list names is placed by
    /// `/txs/{hash}`; from H on it joins the transactions whose inputs are
    /// read, below H it is kept as a spend the lists may not name.
    async fn place_burns(&mut self) -> Result<(), Error> {
        if self.cursor("burns").await?.done {
            return Ok(());
        }
        let since = self.items("since").await?;
        let placed = self.items("burn").await?;
        let readings = self.items("quantity").await?;
        let mut wanted = BTreeSet::new();
        for reading in readings.values() {
            for event in Event::of(reading)? {
                if event.quantity()? < 0
                    && !since.contains_key(&event.tx)
                    && !placed.contains_key(&event.tx)
                {
                    wanted.insert(event.tx);
                }
            }
        }
        let provider = self.asker.provider.clone();
        for transaction in wanted {
            let key = hash32(&transaction)?;
            let info = self
                .asker
                .ask("transactions", || provider.transaction(&key))
                .await?
                .ok_or_else(|| {
                    Error::answer(
                        "provider_transaction_unknown",
                        json!({"transaction": transaction}),
                    )
                })?;
            let place = json!({"height": info.block_height, "index": info.index});
            let kind = if info.block_height >= self.plan.horizon {
                "since"
            } else {
                "burn"
            };
            self.put_one(kind, &transaction, place).await?;
        }
        self.finished("burns").await
    }

    /// Every transaction from H on of each unit and address, in height
    /// ranges that each fit one answer.
    pub(super) async fn since(&mut self) -> Result<(), Error> {
        let mut sources: Vec<String> = self
            .units()
            .await?
            .into_iter()
            .map(|unit| format!("unit:{unit}"))
            .collect();
        sources.extend(self.addresses.iter().map(|a| format!("address:{a}")));
        for source in sources {
            let key = format!("since:{source}");
            let mut cursor = self.cursor(&key).await?;
            if cursor.from < self.plan.horizon {
                cursor.from = self.plan.horizon;
            }
            while !cursor.done {
                let from = cursor.from;
                let rows = self
                    .transactions_page(
                        &source,
                        Heights {
                            from: Some(from),
                            to: None,
                        },
                        Page::first(),
                        false,
                    )
                    .await?;
                let (taken, next) = if rows.len() < 100 {
                    (rows, None)
                } else {
                    let last = rows[99].block_height;
                    if last > from {
                        (
                            rows.into_iter()
                                .filter(|row| row.block_height < last)
                                .collect(),
                            Some(last),
                        )
                    } else {
                        let one = Heights {
                            from: Some(from),
                            to: Some(from),
                        };
                        let mut last_pass: Option<Vec<ListedTransaction>> = None;
                        let mut union: BTreeMap<String, ListedTransaction> = BTreeMap::new();
                        let mut agreed = None;
                        for _ in 0..PASSES {
                            let mut pass = Vec::new();
                            let mut page = Page::first();
                            loop {
                                let rows =
                                    self.transactions_page(&source, one, page, false).await?;
                                let full = rows.len() >= 100;
                                pass.extend(rows);
                                if !full {
                                    break;
                                }
                                page = page.next().ok_or_else(|| {
                                    Error::answer("provider_list_too_long", json!({}))
                                })?;
                            }
                            if last_pass.as_ref() == Some(&pass) {
                                agreed = Some(pass);
                                break;
                            }
                            union.extend(
                                pass.iter()
                                    .map(|row| (row.tx_hash.to_string(), row.clone())),
                            );
                            last_pass = Some(pass);
                        }
                        let pass = agreed.unwrap_or_else(|| union.into_values().collect());
                        (pass, Some(from + 1))
                    }
                };
                let items = taken
                    .iter()
                    .map(|row| {
                        (
                            row.tx_hash.to_string(),
                            json!({"height": row.block_height, "index": row.tx_index}),
                        )
                    })
                    .collect();
                self.put("since", items).await?;
                match next {
                    Some(next) => cursor.from = next,
                    None => cursor.done = true,
                }
                self.put_one("cursor", &key, &cursor).await?;
            }
        }
        self.place_burns().await
    }

    pub(super) async fn transactions_page(
        &self,
        source: &str,
        heights: Heights,
        page: Page,
        descending: bool,
    ) -> Result<Vec<ListedTransaction>, Error> {
        transactions_page(&self.asker, source, heights, page, descending).await
    }

    /// `/txs/{hash}/utxos` of each transaction from H on.
    pub(super) async fn transactions(&mut self) -> Result<(), Error> {
        let since = self.items("since").await?;
        let done = self.items("moved").await?;
        let missing: Vec<String> = since
            .into_keys()
            .filter(|hash| !done.contains_key(hash))
            .collect();
        let provider = self.asker.provider.clone();
        let asker = self.asker.clone();
        let mut answers = futures_util::stream::iter(missing.into_iter().map(|transaction| {
            let (provider, asker) = (provider.clone(), asker.clone());
            async move {
                let key = hash32(&transaction)?;
                let answer = asker
                    .ask("outputs", || provider.transaction_utxos(&key))
                    .await?;
                Ok::<_, Error>((transaction, answer))
            }
        }))
        .buffered(self.max_in_flight);
        let mut batch = Vec::new();
        while let Some(result) = answers.next().await {
            let (transaction, answer) = result?;
            let moved = self.moved(answer);
            batch.push((transaction, to_value(moved)?));
            if batch.len() >= BATCH {
                self.put("moved", std::mem::take(&mut batch)).await?;
            }
        }
        self.put("moved", batch).await
    }

    fn moved(&self, answer: Option<TransactionUtxos>) -> Moved {
        let Some(answer) = answer else {
            return Moved {
                unknown: true,
                ..Moved::default()
            };
        };
        let inputs = answer
            .inputs
            .iter()
            .map(|row| {
                let content = Content {
                    address: row.address.clone(),
                    amount: row.amount.clone(),
                    data_hash: row.data_hash.as_ref().map(ToString::to_string),
                    inline_datum: row.inline_datum.as_ref().map(|bytes| hex::encode(&bytes.0)),
                };
                let keep = row.collateral || self.relevant(&content);
                Spent {
                    tx_hash: row.tx_hash.to_string(),
                    index: u16::try_from(row.output_index).unwrap_or(u16::MAX),
                    collateral: row.collateral,
                    reference: row.reference,
                    content: keep.then_some(content),
                }
            })
            .collect();
        let outputs = answer
            .outputs
            .iter()
            .map(|row| Made {
                index: u16::try_from(row.output_index).unwrap_or(u16::MAX),
                collateral: row.collateral,
                amount: row
                    .amount
                    .iter()
                    .filter(|amount| {
                        amount.unit != "lovelace"
                            && self.rule.allows(&amount.unit[..56], &amount.unit[56..])
                    })
                    .cloned()
                    .collect(),
            })
            .collect();
        Moved {
            unknown: false,
            inputs,
            outputs,
        }
    }

    /// The original content of every collateral input from H on whose
    /// output is not listed: a collateral row may omit native assets.
    pub(super) async fn origins(&mut self) -> Result<(), Error> {
        let since = self.items("since").await?;
        let listed = self.items("listed").await?;
        let known = self.items("origin").await?;
        let moved: BTreeMap<String, Moved> = self.typed("moved").await?;
        let wanted: BTreeSet<(String, u16)> = moved
            .values()
            .flat_map(|moved| moved.inputs.iter())
            .filter(|input| {
                input.collateral
                    && !input.reference
                    && !since.contains_key(&input.tx_hash)
                    && !listed.contains_key(&outpoint(&input.tx_hash, input.index))
                    && !known.contains_key(&outpoint(&input.tx_hash, input.index))
            })
            .map(|input| (input.tx_hash.clone(), input.index))
            .collect();
        let provider = self.asker.provider.clone();
        for (transaction, index) in wanted {
            let key = hash32(&transaction)?;
            let answer = self
                .asker
                .ask("outputs", || provider.transaction_utxos(&key))
                .await?
                .ok_or_else(|| {
                    Error::answer(
                        "provider_origin_unknown",
                        json!({"transaction": transaction}),
                    )
                })?;
            let row = answer
                .outputs
                .iter()
                .find(|row| row.output_index == u32::from(index))
                .ok_or_else(|| {
                    Error::answer(
                        "provider_origin_unknown",
                        json!({"transaction": transaction, "index": index}),
                    )
                })?;
            let content = Content {
                address: row.address.clone(),
                amount: row.amount.clone(),
                data_hash: row.data_hash.as_ref().map(ToString::to_string),
                inline_datum: row.inline_datum.as_ref().map(|bytes| hex::encode(&bytes.0)),
            };
            self.put_one("origin", &outpoint(&transaction, index), content)
                .await?;
        }
        Ok(())
    }
}
