//! The relevant outputs unspent at H and the places of their creating
//! transactions, from the full lists up to H − 1; and the unspent outputs
//! at each profile address derived a second time, from the address's full
//! list up to H − 1, which must give the same outputs.

use std::collections::{BTreeMap, BTreeSet};

use dsa_provider::types::ListedOutput;
use futures_util::StreamExt;
use serde_json::{Value, json};

use super::{
    BATCH, Build,
    asker::{pages_below, utxos},
    content::{Content, Held, Place, outpoint},
};
use crate::follow::Error;

/// A listing that misses an output: a candidate whose creating transaction
/// no list places, or two derivations of an address's outputs that differ.
fn incomplete(code: &'static str, detail: Value) -> Error {
    Error::answer(code, detail)
}

impl Build<'_> {
    /// The relevant outputs unspent at H, before their creating transactions
    /// are placed: listed outputs and relevant inputs from H on, without the
    /// outputs of transactions from H on. A listed output created from H on
    /// must be an output of a transaction from H on.
    async fn candidates(&mut self) -> Result<BTreeMap<(String, u16), Content>, Error> {
        let since = self.items("since").await?;
        let mut listed: BTreeMap<String, ListedOutput> = self.typed("listed").await?;
        listed.extend(self.typed::<ListedOutput>("address_listed").await?);
        let origins: BTreeMap<String, Content> = self.typed("origin").await?;
        let moved = self.moved_since().await?;
        let mut candidates = BTreeMap::new();
        for row in listed.values() {
            let transaction = row.tx_hash.to_string();
            if since.contains_key(&transaction) {
                continue;
            }
            if let Some(height) = row
                .block_height
                .filter(|height| *height >= self.plan.horizon)
            {
                return Err(incomplete(
                    "rebuild_listing_incomplete",
                    json!({"output": outpoint(&transaction, row.output_index), "height": height,
                        "reason": "created from the horizon on by a transaction no list from the horizon on names"}),
                ));
            }
            let content = Content::of_listed(row);
            if self.relevant(&content) {
                let index = u16::try_from(row.output_index)
                    .map_err(|_| Error::answer("provider_output_index", json!({})))?;
                candidates.insert((transaction, index), content);
            }
        }
        for input in moved.values().flat_map(|moved| moved.inputs.iter()) {
            if input.reference || since.contains_key(&input.tx_hash) {
                continue;
            }
            let point = (input.tx_hash.clone(), input.index);
            if candidates.contains_key(&point) {
                continue;
            }
            let content = if input.collateral {
                origins.get(&outpoint(&input.tx_hash, input.index)).cloned()
            } else {
                input.content.clone()
            };
            if let Some(content) = content.filter(|content| self.relevant(content)) {
                candidates.insert(point, content);
            }
        }
        Ok(candidates)
    }

    /// The sources whose full list names the creating transaction of a
    /// candidate: its address when it is a profile address, every eligible
    /// unit it holds.
    fn sources_of(&self, content: &Content) -> Vec<String> {
        let mut sources = Vec::new();
        if self.addresses.contains(&content.address) {
            sources.push(format!("address:{}", content.address));
        }
        for (policy, name, _) in content.assets() {
            if self.rule.allows(&policy, &name) {
                sources.push(format!("unit:{policy}{name}"));
            }
        }
        sources
    }

    /// Places the creating transaction of every candidate from the full
    /// lists up to H − 1: the addresses first, then the units with the
    /// fewest transactions to place. Then every candidate must be placed
    /// and the outputs at each profile address must be those of the
    /// address's own derivation.
    pub(super) async fn history(&mut self) -> Result<(), Error> {
        let candidates = self.candidates().await?;
        let mut needed: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for ((transaction, _), content) in &candidates {
            for source in self.sources_of(content) {
                needed
                    .entry(source)
                    .or_default()
                    .insert(transaction.clone());
            }
        }
        let mut order: Vec<(String, BTreeSet<String>)> = needed.into_iter().collect();
        order.sort_by_key(|(source, wanted)| (!source.starts_with("address:"), wanted.len()));
        for (source, wanted) in order {
            let key = format!("history:{source}");
            let mut cursor = self.cursor(&key).await?;
            if cursor.done {
                continue;
            }
            let placed = self.items("position").await?;
            if wanted
                .iter()
                .all(|transaction| placed.contains_key(transaction))
            {
                continue;
            }
            let mut page = cursor.page.max(1);
            loop {
                let (pages, finished) = pages_below(
                    &self.asker,
                    &source,
                    self.plan.horizon - 1,
                    page,
                    self.max_in_flight,
                )
                .await?;
                let mut items = Vec::new();
                for rows in &pages {
                    for row in rows {
                        let transaction = row.tx_hash.to_string();
                        if wanted.contains(&transaction) {
                            items.push((
                                transaction,
                                json!({"height": row.block_height, "index": row.tx_index}),
                            ));
                        }
                    }
                }
                page += pages.len() as u32;
                self.put("position", items).await?;
                cursor.page = page;
                cursor.done = finished;
                self.put_one("cursor", &key, &cursor).await?;
                if finished {
                    break;
                }
            }
        }
        let unspent = self.unspent_at_horizon().await?;
        self.check_addresses(&unspent).await
    }

    /// The candidates, each with the place of its creating transaction below
    /// H. A candidate that no list places is a listing that misses an output
    /// and refuses the build.
    pub(super) async fn unspent_at_horizon(
        &mut self,
    ) -> Result<BTreeMap<(String, u16), (Content, Place)>, Error> {
        let candidates = self.candidates().await?;
        let placed: BTreeMap<String, Place> = self.typed("position").await?;
        let mut unspent = BTreeMap::new();
        for (point, content) in candidates {
            match placed.get(&point.0) {
                Some(place) if place.height < self.plan.horizon => {
                    unspent.insert(point, (content, *place));
                }
                _ => {
                    return Err(incomplete(
                        "rebuild_listing_incomplete",
                        json!({"output": outpoint(&point.0, point.1),
                            "sources": self.sources_of(&content),
                            "reason": "its creating transaction is in no list below the horizon and in no list from the horizon on"}),
                    ));
                }
            }
        }
        Ok(unspent)
    }

    /// Each profile address's full list up to H − 1, every transaction of
    /// it placed, and the outputs at profile addresses that each of those
    /// transactions created, with the transaction that spent each one.
    pub(super) async fn addresses(&mut self) -> Result<(), Error> {
        for address in self.addresses.clone() {
            let source = format!("address:{address}");
            let key = format!("held:{source}");
            let mut cursor = self.cursor(&key).await?;
            let mut page = cursor.page.max(1);
            while !cursor.done {
                let (pages, finished) = pages_below(
                    &self.asker,
                    &source,
                    self.plan.horizon - 1,
                    page,
                    self.max_in_flight,
                )
                .await?;
                let mut listed = Vec::new();
                let mut positions = Vec::new();
                for row in pages.iter().flatten() {
                    let place = json!({"height": row.block_height, "index": row.tx_index});
                    listed.push((format!("{address} {}", row.tx_hash), place.clone()));
                    positions.push((row.tx_hash.to_string(), place));
                }
                page += pages.len() as u32;
                self.put("address_tx", listed).await?;
                self.put("position", positions).await?;
                cursor.page = page;
                cursor.done = finished;
                self.put_one("cursor", &key, &cursor).await?;
            }
        }
        let read = self.items("held").await?;
        let wanted: BTreeSet<String> = self
            .items("address_tx")
            .await?
            .into_keys()
            .filter_map(|key| key.split_once(' ').map(|(_, hash)| hash.to_owned()))
            .filter(|transaction| !read.contains_key(transaction))
            .collect();
        let asker = self.asker.clone();
        let mut answers = futures_util::stream::iter(wanted.into_iter().map(|transaction| {
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
            let held: Vec<Held> = answer
                .outputs
                .iter()
                .filter(|row| !row.collateral && self.addresses.contains(&row.address))
                .map(|row| {
                    Ok(Held {
                        index: u16::try_from(row.output_index)
                            .map_err(|_| Error::answer("provider_output_index", json!({})))?,
                        content: Content::of_output(row),
                        consumed_by: row.consumed_by_tx.as_ref().map(ToString::to_string),
                    })
                })
                .collect::<Result<_, Error>>()?;
            batch.push((transaction, json!(held)));
            if batch.len() >= BATCH {
                self.put("held", std::mem::take(&mut batch)).await?;
            }
        }
        self.put("held", batch).await
    }

    /// The outputs unspent at H at each profile address, from the address's
    /// full list below H: the outputs its transactions created there that
    /// no transaction of the list spent. They must be the outputs at the
    /// address that the listings and the lists from H on gave.
    async fn check_addresses(
        &mut self,
        unspent: &BTreeMap<(String, u16), (Content, Place)>,
    ) -> Result<(), Error> {
        let mut lists: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for key in self.items("address_tx").await?.into_keys() {
            if let Some((address, transaction)) = key.split_once(' ') {
                lists
                    .entry(address.to_owned())
                    .or_default()
                    .insert(transaction.to_owned());
            }
        }
        let held: BTreeMap<String, Vec<Held>> = self.typed("held").await?;
        for address in self.addresses.clone() {
            let list = lists.remove(&address).unwrap_or_default();
            let mut derived: BTreeMap<String, Value> = BTreeMap::new();
            for transaction in &list {
                for output in held.get(transaction).into_iter().flatten() {
                    let spent_below = output
                        .consumed_by
                        .as_ref()
                        .is_some_and(|spender| list.contains(spender));
                    if output.content.address == address && !spent_below {
                        derived
                            .insert(outpoint(transaction, output.index), output.content.normal());
                    }
                }
            }
            let listed: BTreeMap<String, Value> = unspent
                .iter()
                .filter(|(_, (content, _))| content.address == address)
                .map(|((transaction, index), (content, _))| {
                    (outpoint(transaction, index), content.normal())
                })
                .collect();
            if derived != listed {
                let only = |a: &BTreeMap<String, Value>, b: &BTreeMap<String, Value>| {
                    a.iter()
                        .filter(|(point, content)| b.get(*point) != Some(content))
                        .map(|(point, _)| point.clone())
                        .take(5)
                        .collect::<Vec<_>>()
                };
                return Err(incomplete(
                    "rebuild_address_outputs_mismatch",
                    json!({"address": address, "from_listings": listed.len(),
                        "from_address_history": derived.len(),
                        "only_from_listings": only(&listed, &derived),
                        "only_from_address_history": only(&derived, &listed)}),
                ));
            }
        }
        Ok(())
    }
}
