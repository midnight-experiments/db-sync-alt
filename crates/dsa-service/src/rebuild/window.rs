//! The window H … T through the follower's own updates and checks, and K10
//! after it: the newest relevant spend below H when the window holds none.

use std::{collections::BTreeMap, sync::Arc};

use dsa_chain::{
    block::{InputRow, Rows, TxRow},
    ids,
};
use dsa_provider::types::{Heights, ListedTransaction, Page};
use dsa_store::rebuild::BuiltRows;
use serde_json::json;
use tokio::sync::watch;

use super::{
    Build,
    asker::{hash32, invalid},
    content::{Content, NewestSpend, Place},
};
use crate::follow::{self, Connect, Error, Progress, Settings};

impl Build<'_> {
    /// The follower in its building mode up to T: the database does not
    /// serve, the horizon does not advance, and every check of following
    /// applies; the conservation check proves the unspent outputs at H for
    /// each spend in the window. The follower takes the writer lock on its
    /// own connection.
    pub(super) async fn window(
        &mut self,
        settings: &Settings,
        progress: &watch::Sender<Progress>,
    ) -> Result<(), Error> {
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
            progress: progress.clone(),
        })
        .await?;
        if report.stopped != "stop_target" {
            return Err(Error::Stopped);
        }
        Ok(())
    }

    /// K10: the newest relevant spend below H, found in the units' and
    /// addresses' lists read backward from H − 1.
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
        let mut rows = Rows::default();
        let point = (spend.origin.clone(), spend.origin_index);
        self.output_rows(
            &mut rows,
            &point,
            &spend.content,
            spend.origin_place,
            spend.origin_valid,
            &datums,
        )?;
        let tx_id = ids::transaction_id(spend.place.height, spend.place.index).map_err(invalid)?;
        if !rows.tx.iter().any(|row| row.id == tx_id) {
            rows.tx.push(TxRow {
                id: tx_id,
                hash: spend.tx.clone(),
                block_id: i64::from(spend.place.height),
                block_index: spend.place.index,
                valid_contract: spend.valid,
            });
        }
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
        sources.extend(self.addresses.iter().map(|a| format!("address:{a}")));
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
                    if let Some(spend) = self.spend_of(row, place).await? {
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
        Ok(best)
    }

    /// The relevant spend of the highest ordinal in `row`'s transaction.
    async fn spend_of(
        &mut self,
        row: &ListedTransaction,
        place: Place,
    ) -> Result<Option<NewestSpend>, Error> {
        let transaction = row.tx_hash.to_string();
        let key = row.tx_hash.clone();
        let provider = self.asker.provider.clone();
        let answer = self
            .asker
            .ask("outputs", || provider.transaction_utxos(&key))
            .await?
            .ok_or_else(|| {
                Error::answer(
                    "provider_transaction_unknown",
                    json!({"transaction": transaction}),
                )
            })?;
        let mut contents = Vec::new();
        for input in answer.inputs.iter().filter(|input| !input.reference) {
            let content = if input.collateral {
                let origin = input.tx_hash.clone();
                let origin_answer = self
                    .asker
                    .ask("outputs", || provider.transaction_utxos(&origin))
                    .await?;
                origin_answer.and_then(|answer| {
                    answer
                        .outputs
                        .iter()
                        .find(|output| output.output_index == input.output_index)
                        .map(|output| Content {
                            address: output.address.clone(),
                            amount: output.amount.clone(),
                            data_hash: output.data_hash.as_ref().map(ToString::to_string),
                            inline_datum: output.inline_datum.as_ref().map(|b| hex::encode(&b.0)),
                        })
                })
            } else {
                Some(Content {
                    address: input.address.clone(),
                    amount: input.amount.clone(),
                    data_hash: input.data_hash.as_ref().map(ToString::to_string),
                    inline_datum: input.inline_datum.as_ref().map(|b| hex::encode(&b.0)),
                })
            };
            contents.push((input, content));
        }
        if !contents
            .iter()
            .any(|(_, content)| content.as_ref().is_some_and(|c| self.relevant(c)))
        {
            return Ok(None);
        }
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
        let mut effective: Vec<_> = contents
            .into_iter()
            .filter(|(input, _)| input.collateral != info.valid_contract)
            .map(|(input, content)| ((input.tx_hash.to_string(), input.output_index), content))
            .collect();
        effective.sort_by(|a, b| a.0.cmp(&b.0));
        let Some((ordinal, ((origin, origin_index), content))) = effective
            .iter()
            .enumerate()
            .filter(|(_, (_, content))| content.as_ref().is_some_and(|c| self.relevant(c)))
            .last()
        else {
            return Ok(None);
        };
        let origin_key = hash32(origin)?;
        let origin_info = self
            .asker
            .ask("transactions", || provider.transaction(&origin_key))
            .await?
            .ok_or_else(|| {
                Error::answer(
                    "provider_transaction_unknown",
                    json!({"transaction": origin}),
                )
            })?;
        let content = content.clone().expect("relevant");
        if self.needs_datum(&content) && content.inline_datum.is_none() {
            self.fetch_datums(content.data_hash.iter().cloned().collect())
                .await?;
        }
        Ok(Some(NewestSpend {
            tx: transaction,
            place,
            valid: info.valid_contract,
            ordinal: u16::try_from(ordinal).unwrap_or(u16::MAX),
            origin: origin.clone(),
            origin_index: u16::try_from(*origin_index).unwrap_or(u16::MAX),
            origin_place: Place {
                height: origin_info.block_height,
                index: origin_info.index as u16,
            },
            origin_valid: origin_info.valid_contract,
            content,
        }))
    }
}
