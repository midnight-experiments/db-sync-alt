//! The relevant outputs unspent at H and the places of their creating
//! transactions, from the full lists up to H − 1.

use std::collections::{BTreeMap, BTreeSet};

use dsa_provider::types::{Heights, ListedOutput, Page};
use futures_util::StreamExt;
use serde_json::json;

use super::{
    Build,
    asker::transactions_page,
    content::{Content, Moved, Place, outpoint},
};
use crate::follow::Error;

impl Build<'_> {
    /// The relevant outputs unspent at H, before their creating transactions
    /// are placed: listed outputs and relevant inputs from H on, without the
    /// outputs of transactions from H on.
    async fn candidates(&mut self) -> Result<BTreeMap<(String, u16), Content>, Error> {
        let since = self.items("since").await?;
        let listed: BTreeMap<String, ListedOutput> = self.typed("listed").await?;
        let origins: BTreeMap<String, Content> = self.typed("origin").await?;
        let moved: BTreeMap<String, Moved> = self.typed("moved").await?;
        let mut candidates = BTreeMap::new();
        for row in listed.values() {
            let transaction = row.tx_hash.to_string();
            if since.contains_key(&transaction)
                || row
                    .block_height
                    .is_some_and(|height| height >= self.plan.horizon)
            {
                continue;
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
    /// fewest transactions to place.
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
            let below = Heights {
                from: None,
                to: Some(self.plan.horizon - 1),
            };
            let mut page = cursor.page.max(1);
            loop {
                // Pages ahead are read in parallel: the list below H no
                // longer changes.
                let numbers: Vec<u32> = (page..page + self.max_in_flight as u32).collect();
                let mut answers = Vec::new();
                {
                    let asker = &self.asker;
                    let fetches = numbers.iter().map(|number| {
                        let source = source.clone();
                        async move {
                            let page = Page::new(*number).map_err(Error::Provider)?;
                            transactions_page(asker, &source, below, page, false).await
                        }
                    });
                    let mut stream =
                        futures_util::stream::iter(fetches).buffered(self.max_in_flight);
                    while let Some(answer) = stream.next().await {
                        answers.push(answer?);
                    }
                }
                let mut finished = false;
                let mut items = Vec::new();
                for rows in answers {
                    for row in &rows {
                        let transaction = row.tx_hash.to_string();
                        if wanted.contains(&transaction) {
                            items.push((
                                transaction,
                                json!({"height": row.block_height, "index": row.tx_index}),
                            ));
                        }
                    }
                    page += 1;
                    if rows.len() < 100 {
                        finished = true;
                        break;
                    }
                }
                self.put("position", items).await?;
                cursor.page = page;
                cursor.done = finished;
                self.put_one("cursor", &key, &cursor).await?;
                if finished {
                    break;
                }
            }
        }
        Ok(())
    }

    /// The candidates whose creating transaction is placed below H.
    pub(super) async fn unspent_at_horizon(
        &mut self,
    ) -> Result<(BTreeMap<(String, u16), (Content, Place)>, usize), Error> {
        let candidates = self.candidates().await?;
        let placed: BTreeMap<String, Place> = self.typed("position").await?;
        let mut unspent = BTreeMap::new();
        let mut unplaced = 0;
        for (point, content) in candidates {
            match placed.get(&point.0) {
                Some(place) if place.height < self.plan.horizon => {
                    unspent.insert(point, (content, *place));
                }
                _ => unplaced += 1,
            }
        }
        Ok((unspent, unplaced))
    }
}
