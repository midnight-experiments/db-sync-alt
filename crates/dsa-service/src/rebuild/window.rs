//! The window H … T through the follower's own updates and checks, and K10
//! after it: the newest relevant spend below H when the window holds none.

use std::{collections::BTreeMap, sync::Arc};

use dsa_chain::{
    block::{InputRow, TxRow},
    ids,
};
use dsa_provider::types::{Hash32, Heights, Page};
use dsa_store::rebuild::BuiltRows;
use serde_json::json;

use super::{
    Build,
    asker::{hash32, index16, info, invalid, utxos},
    content::{Content, NewestSpend, Place},
    publication::Collected,
};
use crate::follow::{self, Connect, Error, Settings};

impl Build<'_> {
    /// The follower in its building mode up to T: the database does not
    /// serve, the horizon does not advance, and every check of following
    /// applies; the conservation check proves the unspent outputs at H for
    /// each spend in the window. The follower takes the writer lock on its
    /// own connection.
    pub(super) async fn window(&mut self, settings: &Settings) -> Result<(), Error> {
        self.store = None;
        let follower: Connect = {
            let connect = Arc::clone(&self.connect);
            Box::new(move || connect())
        };
        let report = follow::follow(follow::Context {
            profile: self.profile,
            provider: self.asker.provider.clone(),
            connect: follower,
            settings: Settings {
                stop_at_target: Some(self.plan.target),
                building: true,
                ..settings.clone()
            },
            time: Arc::clone(&self.asker.time),
            stop: self.asker.stop.clone(),
            progress: self.progress.clone(),
            requests: Some(self.asker.requests.clone()),
        })
        .await?;
        if report.stopped != "stop_target" {
            return Err(Error::Stopped);
        }
        Ok(())
    }

    /// K10: the newest relevant spend below H, found in the units' and
    /// addresses' lists read backward from H − 1 and among the burns below
    /// H, which a unit's list does not name when they leave no output of the
    /// unit.
    pub(super) async fn newest_spend(&mut self) -> Result<Option<BuiltRows>, Error> {
        let stored: BTreeMap<String, NewestSpend> = self.typed("spend").await?;
        let spend = match stored.get("newest") {
            Some(spend) => spend.clone(),
            None => {
                let Some(spend) = self.find_newest_spend().await? else {
                    return Ok(None);
                };
                self.put_one("spend", "newest", &spend).await?;
                spend
            }
        };
        let datums = self.items("datum").await?;
        let mut rows = Collected::default();
        let point = (spend.origin.clone(), spend.origin_index);
        rows.output(
            &self.relevance,
            &point,
            &spend.content,
            spend.origin_place,
            spend.origin_valid,
            &datums,
        )?;
        let tx_id = ids::transaction_id(spend.place.height, spend.place.index).map_err(invalid)?;
        rows.transaction(TxRow {
            id: tx_id,
            hash: spend.tx.clone(),
            block_id: i64::from(spend.place.height),
            block_index: spend.place.index,
            valid_contract: spend.valid,
        });
        let mut rows = rows.rows;
        let origin_tx = ids::transaction_id(spend.origin_place.height, spend.origin_place.index)
            .map_err(invalid)?;
        rows.tx_in.push(InputRow {
            id: ids::input_id(spend.place.height, spend.place.index, spend.ordinal)
                .map_err(invalid)?,
            tx_in_id: tx_id,
            tx_out_id: origin_tx,
            tx_out_index: spend.origin_index,
        });
        let marker = ids::output_id(
            spend.origin_place.height,
            spend.origin_place.index,
            spend.origin_index,
        )
        .map_err(invalid)?;
        Ok(Some(BuiltRows {
            rows,
            markers: vec![marker],
            ..BuiltRows::default()
        }))
    }

    async fn find_newest_spend(&mut self) -> Result<Option<NewestSpend>, Error> {
        let mut sources: Vec<String> = self
            .units()
            .await?
            .into_iter()
            .map(|unit| format!("unit:{unit}"))
            .collect();
        sources.extend(
            self.relevance
                .addresses
                .iter()
                .map(|a| format!("address:{a}")),
        );
        let mut best: Option<NewestSpend> = None;
        let below = Heights {
            from: None,
            to: Some(self.plan.horizon - 1),
        };
        for source in sources {
            let mut page = Page::first();
            'pages: loop {
                let rows = self.transactions_page(&source, below, page, true).await?;
                for row in &rows {
                    let place = Place {
                        height: row.block_height,
                        index: row.tx_index as u16,
                    };
                    if best.as_ref().is_some_and(|best| best.place >= place) {
                        break 'pages;
                    }
                    if let Some(spend) = self.spend_of(&row.tx_hash, place).await? {
                        best = Some(spend);
                        break 'pages;
                    }
                }
                if rows.len() < 100 {
                    break;
                }
                page = page
                    .next()
                    .ok_or_else(|| Error::answer("provider_list_too_long", json!({})))?;
            }
        }
        let burns: BTreeMap<String, Place> = self.typed("burn").await?;
        let mut burns: Vec<(Place, String)> = burns
            .into_iter()
            .map(|(transaction, place)| (place, transaction))
            .collect();
        burns.sort();
        for (place, transaction) in burns.into_iter().rev() {
            if best.as_ref().is_some_and(|best| best.place >= place) {
                break;
            }
            if let Some(spend) = self.spend_of(&hash32(&transaction)?, place).await? {
                best = Some(spend);
                break;
            }
        }
        Ok(best)
    }

    /// The relevant spend of the highest ordinal in `transaction`.
    async fn spend_of(
        &mut self,
        transaction: &Hash32,
        place: Place,
    ) -> Result<Option<NewestSpend>, Error> {
        let transaction = transaction.to_string();
        let answer = utxos(&self.asker, &transaction).await?;
        // The transaction's spends are its rows not flagged `reference` or
        // `collateral`: a valid transaction's inputs, an invalid one's
        // collateral, which the provider shows as ordinary inputs.
        let contents: Vec<_> = answer
            .inputs
            .iter()
            .filter(|input| !input.reference && !input.collateral)
            .map(|input| (input, Content::of_input(input)))
            .collect();
        if !contents.iter().any(|(_, content)| self.relevant(content)) {
            return Ok(None);
        }
        let spender = info(&self.asker, &transaction).await?;
        let mut effective: Vec<_> = contents
            .into_iter()
            .map(|(input, content)| ((input.tx_hash.to_string(), input.output_index), content))
            .collect();
        effective.sort_by(|a, b| a.0.cmp(&b.0));
        let Some((ordinal, ((origin, origin_index), content))) = effective
            .iter()
            .enumerate()
            .filter(|(_, (_, content))| self.relevant(content))
            .last()
        else {
            return Ok(None);
        };
        let origin_info = info(&self.asker, origin).await?;
        let content = content.clone();
        if self.needs_datum(&content) && content.inline_datum.is_none() {
            self.fetch_datums(content.data_hash.iter().cloned().collect())
                .await?;
        }
        Ok(Some(NewestSpend {
            tx: transaction,
            place,
            valid: spender.valid_contract,
            ordinal: index16(ordinal)?,
            origin: origin.clone(),
            origin_index: index16(*origin_index)?,
            origin_place: Place {
                height: origin_info.block_height,
                index: origin_info.index as u16,
            },
            origin_valid: origin_info.valid_contract,
            content,
        }))
    }
}
