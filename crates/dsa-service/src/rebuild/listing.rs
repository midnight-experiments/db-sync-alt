//! Steps 1 and 2: the relevant unspent outputs listed now, the quantity of
//! each fixed unit, every transaction from H on with its resolved inputs,
//! and the original content of collateral inputs.
//!
//! The lists from H on are read up to the provider's tip as read after the
//! address listings, and that block must still be on the chain after them:
//! otherwise a spend the address listings saw may have left the chain with
//! its block, and the address listings and the lists from H on are read
//! again.

use std::collections::{BTreeMap, BTreeSet};

use dsa_chain::{assets::fixed_units, profiles::fixed_descriptor_ids};
use dsa_provider::types::{
    AddressText, Header, Heights, ListedOutput, ListedTransaction, Page, TransactionUtxos, Unit,
};
use futures_util::StreamExt;
use serde_json::{Value, json};

use super::{
    BATCH, Build,
    asker::{PASSES, every_page, index16, info, required, stable, transactions_page, utxos},
    content::{Content, Event, Moved, Spent, outpoint},
    progress::{Cursor, parse, to_value},
};
use crate::follow::Error;

/// Readings of a unit whose history and quantity disagree before the build
/// refuses.
const READINGS: usize = 5;

/// Times the address listings and the lists from H on are read again when
/// the tip they were read up to left the chain.
const REREADS: usize = 3;

/// The items an address listing and the lists from H on store, discarded
/// when they are read again: kinds, and cursor keys by prefix.
const SINCE_KINDS: [&str; 3] = ["address_listed", "since", "tip"];
const SINCE_CURSORS: [&str; 3] = ["address:", "since:", "burns"];

impl Build<'_> {
    /// The sources of the listings: units and addresses, as `unit:` and
    /// `address:` keys.
    pub(super) async fn units(&mut self) -> Result<Vec<String>, Error> {
        if self.cursor("units").await?.done {
            return Ok(self.items("unit").await?.into_keys().collect());
        }
        let mut units: BTreeSet<String> = self.relevance.rule.exact_units().clone();
        for policy in self.relevance.rule.governance_policies().clone() {
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
        for address in self.relevance.addresses.clone() {
            self.list_address(&address).await?;
        }
        Ok(())
    }

    async fn listed(&mut self, kind: &str, rows: &[ListedOutput]) -> Result<(), Error> {
        let items = rows
            .iter()
            .map(|row| {
                Ok((
                    outpoint(row.tx_hash.as_str(), row.output_index),
                    to_value(row)?,
                ))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        self.put(kind, items).await
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
            self.listed("listed", &taken).await?;
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
    /// H) and the outputs each one created that no transaction has spent,
    /// the rows not flagged `collateral` (a valid transaction's outputs, an
    /// invalid one's collateral return). No page of a moving list is
    /// involved.
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
            let answer = utxos(&self.asker, listed.tx_hash.as_str()).await?;
            for output in answer.outputs {
                let holds = output
                    .amount
                    .iter()
                    .any(|amount| amount.unit == unit.as_str() && amount.quantity.0 > 0);
                if output.collateral || output.consumed_by_tx.is_some() || !holds {
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
        self.listed("address_listed", &rows).await?;
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
    /// each unit of the listing and each fixed unit, one unit at a time,
    /// with the height of each fixed unit's first mint. Each unit's reading
    /// is stored on its own: a continued phase reads the units without one.
    pub(super) async fn quantities(&mut self) -> Result<(), Error> {
        let fixed = fixed_units(self.profile);
        let known: BTreeSet<String> = fixed_descriptor_ids(self.profile)
            .into_iter()
            .map(|(unit, _)| unit)
            .collect();
        let mut units: BTreeSet<String> = self.units().await?.into_iter().collect();
        units.extend(fixed.iter().cloned());
        let read = self.items("quantity").await?;
        for unit in units {
            if read.contains_key(&unit) {
                continue;
            }
            let reading = self
                .reading(&unit, fixed.contains(&unit), known.contains(&unit))
                .await?;
            self.put_one("quantity", &unit, reading).await?;
        }
        Ok(())
    }

    /// One unit's reading: every page of its history in chain order, then
    /// its quantity. It is taken when no transaction occurs twice in the
    /// history, the history has as many rows as the quantity's answer counts
    /// and its amounts add up to the quantity; otherwise the unit is read
    /// again, `READINGS` times in all, and the build ends with
    /// `provider_mint_history_mismatch`. A unit of the network's table of
    /// fixed descriptors exists on the network: the provider must know it.
    async fn reading(&self, unit: &str, fixed: bool, known: bool) -> Result<Value, Error> {
        let parsed: Unit = unit.parse().map_err(Error::Provider)?;
        let provider = self.asker.provider.clone();
        let mut last = json!({});
        for attempt in 1..=READINGS {
            let events = every_page(&self.asker, "assets", |page| {
                let (provider, parsed) = (provider.clone(), parsed.clone());
                async move { provider.asset_history(&parsed, page).await }
            })
            .await?;
            let answer = if known {
                let unknown = ("provider_unit_unknown", json!({"unit": unit}));
                Some(required(&self.asker, "assets", unknown, || provider.asset(&parsed)).await?)
            } else {
                self.asker.ask("assets", || provider.asset(&parsed)).await?
            };
            let count = answer.as_ref().map_or(0, |info| info.mint_or_burn_count);
            let quantity: Option<i128> = answer
                .as_ref()
                .map_or(Some(0), |info| info.quantity.parse().ok());
            let sum = events.iter().try_fold(0_i128, |sum, event| {
                event.quantity().and_then(|amount| sum.checked_add(amount))
            });
            let distinct = events
                .iter()
                .map(|event| &event.tx_hash)
                .collect::<BTreeSet<_>>()
                .len()
                == events.len();
            if let Some(quantity) = quantity.filter(|quantity| {
                distinct && events.len() as u64 == count && sum == Some(*quantity)
            }) {
                let first_mint_height = match &answer {
                    Some(asset) if fixed => {
                        let mint = asset.initial_mint_tx_hash.to_string();
                        Some(info(&self.asker, &mint).await?.block_height)
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
                return Ok(json!({"quantity": quantity.to_string(), "events": events,
                    "first_mint_height": first_mint_height}));
            }
            last = json!({"unit": unit, "history_rows": events.len(), "mint_or_burn_count": count,
                "history_sum": sum.map(|sum| sum.to_string()),
                "quantity": answer.as_ref().map(|info| info.quantity.clone()),
                "distinct_transactions": distinct, "readings": attempt});
            eprintln!(
                "{}",
                json!({"event":"rebuild_reading_again","reading":last})
            );
        }
        Err(Error::answer("provider_mint_history_mismatch", last))
    }

    /// The mints and burns that no list from H on names. The lists from H
    /// on are read after the histories, up to a tip read after them, so
    /// they cover every height a stored history reaches. A mint creates an
    /// output holding its unit and is then in the unit's list from H on:
    /// one that the list does not name is below H. A burn of a unit's whole
    /// input amount creates none and is in no list of the unit: every burn
    /// no list names is placed by `/txs/{hash}`; from H on it joins the
    /// transactions whose inputs are read, below H it is kept as a spend the
    /// lists may not name.
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
        for transaction in wanted {
            let info = info(&self.asker, &transaction).await?;
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

    /// Every transaction from H on of each unit and address up to the tip
    /// read after the address listings, in height ranges that each fit one
    /// answer; then the mints and burns no list names.
    pub(super) async fn since(&mut self) -> Result<(), Error> {
        let mut rereads = 0;
        loop {
            // A reading of the lists stopped while the address listings were
            // read again continues with them.
            for address in self.relevance.addresses.clone() {
                self.list_address(&address).await?;
            }
            let tip = self.since_tip().await?;
            self.since_lists(tip.height).await?;
            let provider = self.asker.provider.clone();
            let now = self
                .asker
                .ask("headers", || provider.block_at(tip.height))
                .await?;
            if now.as_ref().is_some_and(|header| header.hash == tip.hash) {
                break;
            }
            if rereads == REREADS {
                return Err(Error::answer(
                    "provider_tip_left_chain",
                    json!({"height": tip.height, "hash": tip.hash, "reads": rereads + 1}),
                ));
            }
            rereads += 1;
            eprintln!(
                "{}",
                json!({"event":"rebuild_tip_left_chain","height":tip.height,"read_again":rereads})
            );
            self.store()
                .await?
                .discard_building_items(&SINCE_KINDS, &SINCE_CURSORS, None)
                .await?;
        }
        self.place_burns().await
    }

    /// The provider's tip the lists from H on are read up to, read once.
    async fn since_tip(&mut self) -> Result<Header, Error> {
        if let Some(stored) = self.items("tip").await?.remove("since") {
            return parse(stored);
        }
        let provider = self.asker.provider.clone();
        let tip = self
            .asker
            .ask("headers", || provider.latest_block())
            .await?;
        self.put_one("tip", "since", &tip).await?;
        Ok(tip)
    }

    /// The lists from H on of every unit that has a reading (a fixed unit
    /// first minted during the build included) and of every address.
    async fn since_lists(&mut self, tip: u32) -> Result<(), Error> {
        let mut sources: Vec<String> = self
            .items("quantity")
            .await?
            .into_keys()
            .map(|unit| format!("unit:{unit}"))
            .collect();
        sources.extend(
            self.relevance
                .addresses
                .iter()
                .map(|a| format!("address:{a}")),
        );
        for source in sources {
            let key = format!("since:{source}");
            let mut cursor = self.cursor(&key).await?;
            if cursor.from < self.plan.horizon {
                cursor.from = self.plan.horizon;
            }
            while !cursor.done {
                let from = cursor.from;
                if from > tip {
                    cursor.done = true;
                    self.put_one("cursor", &key, &cursor).await?;
                    break;
                }
                let rows = self
                    .transactions_page(
                        &source,
                        Heights {
                            from: Some(from),
                            to: Some(tip),
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
                        (self.since_at_height(&source, from).await?, Some(from + 1))
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
        Ok(())
    }

    /// The transactions of `source` at one height that holds 100 or more of
    /// them, read in passes until two agree, or the union of the passes.
    async fn since_at_height(
        &self,
        source: &str,
        height: u32,
    ) -> Result<Vec<ListedTransaction>, Error> {
        let one = Heights {
            from: Some(height),
            to: Some(height),
        };
        let mut last_pass: Option<Vec<ListedTransaction>> = None;
        let mut union: BTreeMap<String, ListedTransaction> = BTreeMap::new();
        for _ in 0..PASSES {
            let mut pass = Vec::new();
            let mut page = Page::first();
            loop {
                let rows = self.transactions_page(source, one, page, false).await?;
                let full = rows.len() >= 100;
                pass.extend(rows);
                if !full {
                    break;
                }
                page = page
                    .next()
                    .ok_or_else(|| Error::answer("provider_list_too_long", json!({})))?;
            }
            if last_pass.as_ref() == Some(&pass) {
                return Ok(pass);
            }
            union.extend(
                pass.iter()
                    .map(|row| (row.tx_hash.to_string(), row.clone())),
            );
            last_pass = Some(pass);
        }
        Ok(union.into_values().collect())
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
        let asker = self.asker.clone();
        let mut answers = futures_util::stream::iter(missing.into_iter().map(|transaction| {
            let asker = asker.clone();
            async move {
                let answer = utxos(&asker, &transaction).await?;
                Ok::<_, Error>((transaction, answer))
            }
        }))
        .buffered(self.max_in_flight);
        let mut batch = Vec::new();
        while let Some(result) = answers.next().await {
            let (transaction, answer) = result?;
            let moved = self.moved(answer)?;
            batch.push((transaction, to_value(moved)?));
            if batch.len() >= BATCH {
                self.put("moved", std::mem::take(&mut batch)).await?;
            }
        }
        self.put("moved", batch).await
    }

    /// The inputs of a transaction from H on, with the content of the
    /// relevant ones and of collateral inputs.
    fn moved(&self, answer: TransactionUtxos) -> Result<Moved, Error> {
        let inputs = answer
            .inputs
            .iter()
            .map(|row| {
                let content = Content::of_input(row);
                let keep = row.collateral || self.relevant(&content);
                Ok(Spent {
                    tx_hash: row.tx_hash.to_string(),
                    index: index16(row.output_index)?,
                    collateral: row.collateral,
                    reference: row.reference,
                    content: keep.then_some(content),
                })
            })
            .collect::<Result<_, Error>>()?;
        Ok(Moved { inputs })
    }

    /// The original content of every collateral input from H on whose
    /// output is not listed: a collateral row may omit native assets.
    pub(super) async fn origins(&mut self) -> Result<(), Error> {
        let since = self.items("since").await?;
        let mut listed = self.items("listed").await?;
        listed.extend(self.items("address_listed").await?);
        let known = self.items("origin").await?;
        let moved = self.moved_since().await?;
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
        for (transaction, index) in wanted {
            let answer = utxos(&self.asker, &transaction).await?;
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
            let content = Content::of_output(row);
            self.put_one("origin", &outpoint(&transaction, index), content)
                .await?;
        }
        Ok(())
    }

    /// The resolved transactions of the current lists from H on: answers
    /// stored for transactions no list names any more are not used.
    pub(super) async fn moved_since(&mut self) -> Result<BTreeMap<String, Moved>, Error> {
        let since = self.items("since").await?;
        let mut moved: BTreeMap<String, Moved> = self.typed("moved").await?;
        moved.retain(|transaction, _| since.contains_key(transaction));
        Ok(moved)
    }
}
