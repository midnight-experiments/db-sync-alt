//! Phase-2 validity of the creating transactions below H.

use std::collections::BTreeMap;

use futures_util::StreamExt;
use serde_json::json;

use super::{Build, asker::info, content::Place};
use crate::follow::Error;

impl Build<'_> {
    /// Phase-2 validity of each creating transaction below H: a transaction
    /// with two known outputs is valid (a failing one creates one output);
    /// any other is read.
    pub(super) async fn validity(&mut self) -> Result<(), Error> {
        let unspent = self.unspent_at_horizon().await?;
        let mut outputs: BTreeMap<String, usize> = BTreeMap::new();
        for (transaction, _) in unspent.keys() {
            *outputs.entry(transaction.clone()).or_default() += 1;
        }
        let known = self.items("valid").await?;
        let mut settled = Vec::new();
        let mut wanted = Vec::new();
        for (transaction, count) in outputs {
            if known.contains_key(&transaction) {
                continue;
            }
            if count >= 2 {
                settled.push((transaction, json!(true)));
            } else {
                wanted.push(transaction);
            }
        }
        self.put("valid", settled).await?;
        let placed: BTreeMap<String, Place> = self.typed("position").await?;
        let asker = self.asker.clone();
        let mut answers = futures_util::stream::iter(wanted.into_iter().map(|transaction| {
            let asker = asker.clone();
            async move {
                let answer = info(&asker, &transaction).await?;
                Ok::<_, Error>((transaction, answer))
            }
        }))
        .buffered(self.max_in_flight);
        let mut batch = Vec::new();
        while let Some(result) = answers.next().await {
            let (transaction, info) = result?;
            let place = placed[&transaction];
            if info.block_height != place.height || info.index != u32::from(place.index) {
                return Err(Error::answer(
                    "provider_position_mismatch",
                    json!({"transaction": transaction, "list": [place.height, place.index],
                        "transaction_answer": [info.block_height, info.index]}),
                ));
            }
            batch.push((transaction, json!(info.valid_contract)));
            if batch.len() >= self.batch {
                self.put("valid", std::mem::take(&mut batch)).await?;
            }
        }
        self.put("valid", batch).await
    }
}
