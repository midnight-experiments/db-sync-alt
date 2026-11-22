//! One method per endpoint. `Ok(None)` stands for an answer that has a
//! meaning: no block at that height yet, a block that left the chain, an
//! unknown datum, an epoch that is not ready. Lists use pages of 100 in
//! ascending order, except retired pools and pool history (descending).

use bytes::Bytes;

use crate::{
    Error, Provider,
    client::Family,
    pages::{PAGE_SIZE, TransactionPage},
    types::{
        Datum, EpochInfo, EpochParameters, Genesis, Hash32, Header, Hex, Metadata, MetadataEntry,
        Page, PoolHistory, PoolId, PoolStake, RetiredPool, StakeAddress, TransactionCbor,
        TransactionOutputs,
    },
};

/// The highest block height a positional ID can hold.
const HEIGHT_LIMIT: u32 = dsa_chain::ids::HEIGHT_LIMIT;

fn paged(path: &str, page: Page, descending: bool) -> String {
    format!(
        "{path}?count=100&page={}&order={}",
        page.number(),
        if descending { "desc" } else { "asc" }
    )
}

fn epoch_path(epoch: u32) -> Result<u32, Error> {
    if epoch > i32::MAX as u32 {
        return Err(Error::configuration("request.epoch"));
    }
    Ok(epoch)
}

/// Pure answer readers, one per endpoint, so that every byte the client
/// receives goes through the same function in tests.
pub(crate) mod parse {
    use super::*;
    use serde::de::DeserializeOwned;

    pub(crate) fn json<T: DeserializeOwned>(body: &[u8]) -> Result<T, &'static str> {
        serde_json::from_slice(body).map_err(|_| "provider_invalid_response")
    }

    pub(crate) fn list<T: DeserializeOwned>(
        body: &[u8],
        maximum: usize,
    ) -> Result<Vec<T>, &'static str> {
        let rows: Vec<T> = json(body)?;
        if rows.len() > maximum {
            return Err("provider_bad_page");
        }
        Ok(rows)
    }

    fn checked(header: Header) -> Result<Header, &'static str> {
        if header.height >= HEIGHT_LIMIT || header.epoch > i32::MAX as u32 {
            return Err("provider_invalid_response");
        }
        Ok(header)
    }

    pub(crate) fn header(body: &[u8], height: Option<u32>) -> Result<Header, &'static str> {
        let header = checked(json(body)?)?;
        if height.is_some_and(|height| height != header.height) {
            return Err("provider_invalid_response");
        }
        Ok(header)
    }

    pub(crate) fn headers(body: &[u8]) -> Result<Vec<Header>, &'static str> {
        list(body, PAGE_SIZE)?.into_iter().map(checked).collect()
    }

    pub(crate) fn page(
        block: &Hash32,
        page: Page,
        body: Bytes,
    ) -> Result<TransactionPage, &'static str> {
        Ok(TransactionPage {
            block: block.clone(),
            page,
            transactions: list::<TransactionCbor>(&body, PAGE_SIZE)?,
            body,
        })
    }

    pub(crate) fn datum(body: Bytes) -> Result<Datum, &'static str> {
        #[derive(serde::Deserialize)]
        struct Wire {
            cbor: Hex,
        }
        Ok(Datum {
            cbor: json::<Wire>(&body)?.cbor.0,
            body,
        })
    }

    pub(crate) fn metadata(body: Bytes) -> Result<Metadata, &'static str> {
        let entries: Vec<MetadataEntry> = json(&body)?;
        let mut labels = std::collections::BTreeSet::new();
        if !entries.iter().all(|entry| labels.insert(entry.label.0)) {
            return Err("provider_invalid_response");
        }
        Ok(Metadata { entries, body })
    }

    pub(crate) fn epoch_parameters(
        epoch: u32,
        body: &[u8],
    ) -> Result<Option<EpochParameters>, &'static str> {
        let row: EpochParameters = json(body)?;
        if row.epoch != epoch || row.max_tx_size > i32::MAX as u32 {
            return Err("provider_invalid_response");
        }
        Ok(row.nonce.is_some().then_some(row))
    }

    pub(crate) fn epoch(epoch: u32, body: &[u8]) -> Result<Option<EpochInfo>, &'static str> {
        let row: EpochInfo = json(body)?;
        if row.epoch != epoch {
            return Err("provider_invalid_response");
        }
        Ok(row.active_stake.is_some().then_some(row))
    }

    pub(crate) fn pool(pool: &PoolId, body: &[u8]) -> Result<PoolStake, &'static str> {
        let row: PoolStake = json(body)?;
        if row.pool_id != *pool {
            return Err("provider_invalid_response");
        }
        Ok(row)
    }
}

impl Provider {
    fn parsed<T>(&self, result: Result<T, &'static str>) -> Result<T, Error> {
        result.map_err(|code| self.invalid(code))
    }

    /// A 404 where the endpoint gives it no meaning.
    fn found(&self, body: Option<Bytes>) -> Result<Bytes, Error> {
        body.ok_or_else(|| self.invalid("provider_not_found"))
    }

    pub async fn latest_block(&self) -> Result<Header, Error> {
        let body = self.found(self.get("/blocks/latest", Family::Small).await?)?;
        self.parsed(parse::header(&body, None))
    }

    /// `None` when no block has that height yet.
    pub async fn block_at(&self, height: u32) -> Result<Option<Header>, Error> {
        if height >= HEIGHT_LIMIT {
            return Err(Error::configuration("request.block_height"));
        }
        let body = self
            .get(&format!("/blocks/{height}"), Family::Small)
            .await?;
        body.map(|body| self.parsed(parse::header(&body, Some(height))))
            .transpose()
    }

    /// Up to 100 headers after `block`; `None` when `block` left the chain.
    pub async fn next_blocks(
        &self,
        block: &Hash32,
        page: Page,
    ) -> Result<Option<Vec<Header>>, Error> {
        let path = format!("/blocks/{block}/next?count=100&page={}", page.number());
        let body = self.get(&path, Family::Small).await?;
        body.map(|body| self.parsed(parse::headers(&body)))
            .transpose()
    }

    /// One page of a block's transactions; `None` when the block is unknown.
    pub async fn block_transactions(
        &self,
        block: &Hash32,
        page: Page,
    ) -> Result<Option<TransactionPage>, Error> {
        let path = paged(&format!("/blocks/{block}/txs/cbor"), page, false);
        let body = self.get(&path, Family::Block).await?;
        body.map(|body| self.parsed(parse::page(block, page, body)))
            .transpose()
    }

    /// `None` when the provider does not know the datum.
    pub async fn datum(&self, hash: &Hash32) -> Result<Option<Datum>, Error> {
        let body = self
            .get(&format!("/scripts/datum/{hash}/cbor"), Family::Datum)
            .await?;
        body.map(|body| self.parsed(parse::datum(body))).transpose()
    }

    pub async fn transaction_metadata(&self, transaction: &Hash32) -> Result<Metadata, Error> {
        let body = self
            .get(&format!("/txs/{transaction}/metadata"), Family::Metadata)
            .await?;
        let body = self.found(body)?;
        self.parsed(parse::metadata(body))
    }

    pub async fn transaction_outputs(
        &self,
        transaction: &Hash32,
    ) -> Result<TransactionOutputs, Error> {
        self.get_outputs(transaction)
            .await?
            .ok_or_else(|| self.invalid("provider_not_found"))
    }

    /// `None` until the epoch's nonce is known.
    pub async fn epoch_parameters(&self, epoch: u32) -> Result<Option<EpochParameters>, Error> {
        let path = format!("/epochs/{}/parameters", epoch_path(epoch)?);
        match self.get(&path, Family::Small).await? {
            Some(body) => self.parsed(parse::epoch_parameters(epoch, &body)),
            None => Ok(None),
        }
    }

    /// `None` until the epoch's active stake is known.
    pub async fn epoch(&self, epoch: u32) -> Result<Option<EpochInfo>, Error> {
        let path = format!("/epochs/{}", epoch_path(epoch)?);
        match self.get(&path, Family::Small).await? {
            Some(body) => self.parsed(parse::epoch(epoch, &body)),
            None => Ok(None),
        }
    }

    pub async fn pools(&self, page: Page) -> Result<Vec<PoolId>, Error> {
        let body = self.found(
            self.get(&paged("/pools", page, false), Family::Small)
                .await?,
        )?;
        self.parsed(parse::list(&body, PAGE_SIZE))
    }

    pub async fn extended_pools(&self, page: Page) -> Result<Vec<PoolStake>, Error> {
        let path = paged("/pools/extended", page, false);
        let body = self.found(self.get(&path, Family::Small).await?)?;
        self.parsed(parse::list(&body, PAGE_SIZE))
    }

    pub async fn retired_pools(&self, page: Page) -> Result<Vec<RetiredPool>, Error> {
        let path = paged("/pools/retired", page, true);
        let body = self.found(self.get(&path, Family::Small).await?)?;
        self.parsed(parse::list(&body, PAGE_SIZE))
    }

    /// `None` when the provider does not know the pool.
    pub async fn pool(&self, pool: &PoolId) -> Result<Option<PoolStake>, Error> {
        let body = self.get(&format!("/pools/{pool}"), Family::Small).await?;
        body.map(|body| self.parsed(parse::pool(pool, &body)))
            .transpose()
    }

    pub async fn pool_history(&self, pool: &PoolId, page: Page) -> Result<Vec<PoolHistory>, Error> {
        let path = paged(&format!("/pools/{pool}/history"), page, true);
        let body = self.found(self.get(&path, Family::Small).await?)?;
        self.parsed(parse::list(&body, PAGE_SIZE))
    }

    /// The first stake address of a pool in an epoch's distribution.
    pub async fn epoch_pool_stake(
        &self,
        epoch: u32,
        pool: &PoolId,
    ) -> Result<Vec<StakeAddress>, Error> {
        let path = format!(
            "/epochs/{}/stakes/{pool}?count=1&page=1",
            epoch_path(epoch)?
        );
        let body = self.found(self.get(&path, Family::Small).await?)?;
        self.parsed(parse::list(&body, 1))
    }

    pub async fn genesis(&self) -> Result<Genesis, Error> {
        let body = self.found(self.get("/genesis", Family::Small).await?)?;
        self.parsed(parse::json(&body))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    /// One member of the recorded Mainnet corpus.
    fn recorded(name: &str) -> Vec<u8> {
        let compressed = include_bytes!("../tests/fixtures/mainnet-corpus.json.gz");
        let mut raw = Vec::new();
        flate2::read::GzDecoder::new(&compressed[..])
            .read_to_end(&mut raw)
            .unwrap();
        let bundle: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        bundle["files"][name].as_str().unwrap().as_bytes().to_vec()
    }

    #[test]
    fn every_prefix_of_a_recorded_answer_is_refused_or_read() {
        let block: Hash32 = "01".repeat(32).parse().unwrap();
        let datum = format!(
            "{{\"cbor\":{}}}",
            String::from_utf8(recorded(
                "provider/datums/13145387-f625fa929349faa5e028a84ab331df37d8230862cec28979e3aa17961e1e8082.json"
            ))
            .unwrap()
        );
        type Reader = Box<dyn Fn(Bytes) -> bool>;
        let block_for_page = block.clone();
        let answers: Vec<(&str, Vec<u8>, Reader)> = vec![
            (
                "header",
                recorded("provider/headers/13922321.json"),
                Box::new(|body| parse::header(&body, None).is_ok()),
            ),
            (
                "transaction page",
                recorded("provider/transactions/13922321.json"),
                Box::new(move |body| parse::page(&block_for_page, Page::first(), body).is_ok()),
            ),
            (
                "metadata",
                recorded(
                    "provider/metadata/03b5d301678204ee48c7f93d3446fd4ad0427da6d1966e8d3df423c720657b0e.json",
                ),
                Box::new(|body| parse::metadata(body).is_ok()),
            ),
            (
                "datum",
                datum.into_bytes(),
                Box::new(|body| parse::datum(body).is_ok()),
            ),
        ];
        for (name, answer, read) in answers {
            assert!(
                read(Bytes::from(answer.clone())),
                "{name}: the whole answer is read"
            );
            let mut refused = 0;
            for end in 0..answer.len() {
                if !read(Bytes::copy_from_slice(&answer[..end])) {
                    refused += 1;
                }
            }
            assert!(
                refused + 1 >= answer.len(),
                "{name}: a cut answer is refused (accepted {} prefixes)",
                answer.len() - refused
            );
            let mut escaped = answer[..answer.len() / 2].to_vec();
            escaped.extend_from_slice(b"\"\\");
            assert!(
                !read(Bytes::from(escaped)),
                "{name}: an unterminated escape"
            );
        }
    }
}
