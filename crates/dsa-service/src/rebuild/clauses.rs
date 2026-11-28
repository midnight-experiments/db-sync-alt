//! The other clauses below H: K4 datums, K5 and K6 carried transactions,
//! K7 and K8 from the epochs, and the headers H − 1 … H − 1 + k.

use std::collections::{BTreeMap, BTreeSet};

use dsa_chain::transaction::hash;
use dsa_provider::types::{Header, Heights, Page};
use futures_util::StreamExt;
use serde_json::{Value, json};

use super::{
    Build,
    asker::{hash32, index16, info, utxos},
    content::{Carried, Content, Place},
    progress::to_value,
};
use crate::follow::Error;

impl Build<'_> {
    /// The datum of each kept output the node reads one for, unless inline.
    pub(super) async fn datums(&mut self) -> Result<(), Error> {
        let unspent = self.unspent_at_horizon().await?;
        let mut wanted: BTreeSet<String> = unspent
            .values()
            .filter(|(content, _)| self.needs_datum(content) && content.inline_datum.is_none())
            .filter_map(|(content, _)| content.data_hash.clone())
            .collect();
        let carried: BTreeMap<String, Vec<Carried>> = self.typed("carried").await?;
        for carry in carried.values().flatten() {
            for (_, content) in &carry.outputs {
                if self.needs_datum(content) && content.inline_datum.is_none() {
                    wanted.extend(content.data_hash.clone());
                }
            }
        }
        self.fetch_datums(wanted).await
    }

    pub(super) async fn fetch_datums(&mut self, wanted: BTreeSet<String>) -> Result<(), Error> {
        let known = self.items("datum").await?;
        let provider = self.asker.provider.clone();
        let asker = self.asker.clone();
        let wanted: Vec<String> = wanted
            .into_iter()
            .filter(|hash| !known.contains_key(hash))
            .collect();
        let mut answers = futures_util::stream::iter(wanted.into_iter().map(|datum| {
            let (provider, asker) = (provider.clone(), asker.clone());
            async move {
                let key = hash32(&datum)?;
                let answer = asker.ask("datums", || provider.datum(&key)).await?;
                Ok::<_, Error>((datum, answer))
            }
        }))
        .buffered(self.max_in_flight);
        let mut batch = Vec::new();
        while let Some(result) = answers.next().await {
            let (datum, answer) = result?;
            batch.push((
                datum,
                json!({"cbor": answer.map(|answer| hex::encode(answer.cbor))}),
            ));
        }
        self.put("datum", batch).await
    }

    /// The datum bytes of a content, inline or from the provider, checked
    /// against its hash.
    pub(super) fn datum_bytes(
        content: &Content,
        datums: &BTreeMap<String, Value>,
    ) -> Result<Option<Vec<u8>>, Error> {
        let Some(data_hash) = &content.data_hash else {
            return Ok(None);
        };
        let raw = match &content.inline_datum {
            Some(inline) => Some(inline.clone()),
            None => datums
                .get(data_hash)
                .and_then(|value| value["cbor"].as_str().map(str::to_owned)),
        };
        let Some(raw) = raw else {
            return Ok(None);
        };
        let bytes = hex::decode(&raw)
            .map_err(|_| Error::answer("provider_datum_invalid", json!({"hash": data_hash})))?;
        if hash(&bytes) != *data_hash {
            return Err(Error::answer(
                "provider_datum_hash_mismatch",
                json!({"hash": data_hash}),
            ));
        }
        Ok(Some(bytes))
    }

    /// The outputs `transaction` creates, from its `/txs/{hash}/utxos`
    /// answer: the rows not flagged `collateral`, which are a valid
    /// transaction's outputs and an invalid one's collateral return, and the
    /// transaction's validity.
    async fn created(&self, transaction: &str) -> Result<(bool, Vec<(u16, Content)>), Error> {
        let answer = utxos(&self.asker, transaction).await?;
        let info = info(&self.asker, transaction).await?;
        let outputs = answer
            .outputs
            .iter()
            .filter(|row| !row.collateral)
            .map(|row| Ok((index16(row.output_index)?, Content::of_output(row))))
            .collect::<Result<_, Error>>()?;
        Ok((info.valid_contract, outputs))
    }

    async fn header_at(&self, height: u32) -> Result<Header, Error> {
        let provider = self.asker.provider.clone();
        self.asker
            .ask("headers", || provider.block_at(height))
            .await?
            .ok_or_else(|| Error::answer("provider_block_missing", json!({"height": height})))
    }

    /// K5 per governance body and K6: the transactions kept below H.
    pub(super) async fn carried(&mut self) -> Result<(), Error> {
        let known = self.items("carried").await?;
        let horizon_epoch = self.header_at(self.plan.horizon).await?.epoch;
        for body in self.profile.governance() {
            if known.contains_key(body.name) {
                continue;
            }
            let source = format!("address:{}", body.address);
            let policy = body.asset.policy.clone();
            let address = body.address.to_owned();
            let mut found = Vec::new();
            let mut page = Page::first();
            'pages: loop {
                let rows = self
                    .transactions_page(
                        &source,
                        Heights {
                            from: None,
                            to: Some(self.plan.horizon - 1),
                        },
                        page,
                        true,
                    )
                    .await?;
                for row in &rows {
                    let transaction = row.tx_hash.to_string();
                    let (valid, outputs) = self.created(&transaction).await?;
                    let mut kept = Vec::new();
                    for (index, content) in outputs {
                        let holds = content.amount.iter().any(|amount| {
                            amount.unit != "lovelace"
                                && amount.unit.starts_with(&policy)
                                && amount.quantity.0 > 0
                        });
                        if content.address != address || !holds || content.data_hash.is_none() {
                            continue;
                        }
                        if content.inline_datum.is_none() {
                            let wanted =
                                BTreeSet::from([content.data_hash.clone().unwrap_or_default()]);
                            self.fetch_datums(wanted).await?;
                        }
                        let datums = self.items("datum").await?;
                        if Self::datum_bytes(&content, &datums)?.is_some() {
                            kept.push((index, content));
                        }
                    }
                    if !kept.is_empty() {
                        found.push(Carried {
                            tx: transaction,
                            place: Place {
                                height: row.block_height,
                                index: row.tx_index as u16,
                            },
                            valid,
                            header: self.header_at(row.block_height).await?,
                            outputs: kept,
                        });
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
            self.put_one("carried", body.name, &found).await?;
        }
        if !known.contains_key("permissioned_candidates") {
            let unit = self.relevance.permissioned.clone();
            let source = format!("unit:{unit}");
            let mut found = Vec::new();
            let mut page = Page::first();
            'listing: loop {
                let rows = self
                    .transactions_page(
                        &source,
                        Heights {
                            from: None,
                            to: Some(self.plan.horizon - 1),
                        },
                        page,
                        true,
                    )
                    .await?;
                for row in &rows {
                    let transaction = row.tx_hash.to_string();
                    let (valid, outputs) = self.created(&transaction).await?;
                    let kept: Vec<(u16, Content)> = outputs
                        .into_iter()
                        .filter(|(_, content)| content.quantity(&unit) > 0)
                        .collect();
                    if kept.is_empty() {
                        continue;
                    }
                    let header = self.header_at(row.block_height).await?;
                    let last = i64::from(header.epoch) <= i64::from(horizon_epoch) - 2;
                    found.push(Carried {
                        tx: transaction,
                        place: Place {
                            height: row.block_height,
                            index: row.tx_index as u16,
                        },
                        valid,
                        header,
                        outputs: kept,
                    });
                    if last {
                        break 'listing;
                    }
                }
                if rows.len() < 100 {
                    break;
                }
                page = page
                    .next()
                    .ok_or_else(|| Error::answer("provider_list_too_long", json!({})))?;
            }
            self.put_one("carried", "permissioned_candidates", &found)
                .await?;
        }
        Ok(())
    }

    /// K7 headers, K8 parameters below the window, the headers H − 1 …
    /// H − 1 + k and the target's header.
    pub(super) async fn epochs(&mut self) -> Result<(), Error> {
        if self.cursor("epochs").await?.done {
            return Ok(());
        }
        let provider = self.asker.provider.clone();
        let horizon = self.header_at(self.plan.horizon).await?;
        let mut headers = vec![
            ("horizon".to_owned(), to_value(&horizon)?),
            (
                "target".to_owned(),
                to_value(self.header_at(self.plan.target).await?)?,
            ),
        ];
        for back in [2_u32, 1] {
            let Some(cutoff) = horizon.epoch.checked_sub(back) else {
                continue;
            };
            // An epoch without a block has no parameters at a provider fed
            // by db-sync, which writes them at the epoch's first block.
            let block = self
                .asker
                .ask("epochs", || provider.epoch_last_block(cutoff))
                .await?
                .ok_or_else(|| {
                    Error::answer("provider_epoch_blocks_missing", json!({"epoch": cutoff}))
                })?;
            let header = self
                .asker
                .ask("headers", || provider.block_by_hash(&block))
                .await?
                .ok_or_else(|| Error::answer("provider_block_missing", json!({"block": block})))?;
            headers.push((format!("epoch:{cutoff}"), to_value(header)?));
            // Asked and counted here once: not through the follower's
            // reader, which counts the request itself.
            let params = self
                .asker
                .ask("epochs", || provider.epoch_parameters(cutoff))
                .await?
                .and_then(|params| params.nonce.map(|nonce| (nonce, params.max_tx_size)));
            let (nonce, max_tx_size) = params.ok_or_else(|| {
                Error::answer(
                    "provider_epoch_parameters_missing",
                    json!({"epoch": cutoff}),
                )
            })?;
            self.put_one(
                "param",
                &cutoff.to_string(),
                json!({"nonce": nonce.to_string(), "max_tx_size": max_tx_size}),
            )
            .await?;
        }
        // H − 1 and the k headers after it, linked.
        let k = self.profile.cardano.security_parameter;
        let first = self.header_at(self.plan.horizon - 1).await?;
        let mut chain = vec![first.clone()];
        let mut page = Page::first();
        while chain.len() < k as usize + 1 {
            let rows = self
                .asker
                .ask("headers", || provider.next_blocks(&first.hash, page))
                .await?
                .ok_or_else(|| Error::answer("provider_fork_point_unknown", json!({})))?;
            if rows.is_empty() {
                return Err(Error::answer("provider_chain_shorter", json!({})));
            }
            for row in rows {
                let previous = chain.last().expect("first");
                if row.height != previous.height + 1
                    || row.previous_block.as_ref() != Some(&previous.hash)
                {
                    return Err(Error::answer(
                        "provider_header_link_mismatch",
                        json!({"height": row.height}),
                    ));
                }
                chain.push(row);
                if chain.len() == k as usize + 1 {
                    break;
                }
            }
            page = page
                .next()
                .ok_or_else(|| Error::answer("provider_list_too_long", json!({})))?;
        }
        for header in chain {
            headers.push((format!("chain:{}", header.height), to_value(header)?));
        }
        self.put("header", headers).await?;
        self.finished("epochs").await
    }
}
