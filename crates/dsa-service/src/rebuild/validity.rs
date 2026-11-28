//! Phase-2 validity of the creating transactions below H.

use std::collections::{BTreeMap, BTreeSet};

use futures_util::StreamExt;
use serde_json::json;

use super::{
    Build,
    asker::info,
    content::{Place, Unspent},
};
use crate::follow::Error;

/// The creating transactions below H with two or more outputs unspent at
/// H: valid, as a failing transaction creates one output. Worked out from
/// each run's candidates and never stored, so that every stored validity is
/// the provider's answer.
pub(super) fn inferred_valid(unspent: &Unspent) -> BTreeSet<String> {
    let mut outputs: BTreeMap<&str, usize> = BTreeMap::new();
    for (transaction, _) in unspent.keys() {
        *outputs.entry(transaction.as_str()).or_default() += 1;
    }
    outputs
        .into_iter()
        .filter(|(_, count)| *count >= 2)
        .map(|(transaction, _)| transaction.to_owned())
        .collect()
}

impl Build<'_> {
    /// Phase-2 validity of each creating transaction below H that the
    /// candidates do not tell (`inferred_valid`) and the provider has not
    /// answered yet, from `/txs/{hash}`; the answers are stored.
    pub(super) async fn validity(&mut self) -> Result<(), Error> {
        let unspent = self.unspent_at_horizon().await?;
        let inferred = inferred_valid(&unspent);
        let known = self.items("valid").await?;
        let wanted: Vec<String> = unspent
            .keys()
            .map(|(transaction, _)| transaction.as_str())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter(|transaction| {
                !inferred.contains(*transaction) && !known.contains_key(*transaction)
            })
            .map(str::to_owned)
            .collect();
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
