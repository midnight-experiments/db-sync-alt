//! Freeze the hash/CBOR order at the successful provider-response boundary.

use dsa_chain::{ids, inventory::CapturedTransactions};

use crate::types::{Hash32, Page, TransactionCbor};

#[derive(Clone, Debug)]
pub struct TransactionPage {
    block: Hash32,
    page: Page,
    records: Vec<TransactionCbor>,
    captured: CapturedTransactions,
}

impl TransactionPage {
    pub(crate) fn capture(
        block: Hash32,
        page: Page,
        records: Vec<TransactionCbor>,
    ) -> Result<Self, &'static str> {
        let source = records
            .iter()
            .map(|record| {
                let bytes =
                    hex::decode(record.cbor.as_str()).map_err(|_| "provider_invalid_response")?;
                Ok((record.tx_hash.to_string(), bytes))
            })
            .collect::<Result<Vec<_>, &'static str>>()?;
        Ok(Self {
            block,
            page,
            records,
            captured: CapturedTransactions::from_records(source),
        })
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
    pub fn records(&self) -> &[TransactionCbor] {
        &self.records
    }
}

/// B4 can append received pages directly. Their requested block/page metadata
/// and within-page order are immutable. The initial provider header/list is trusted.
pub struct CapturedBlock {
    block: Hash32,
    expected_count: usize,
    next_page: u32,
    complete: bool,
    captured: CapturedTransactions,
}

impl CapturedBlock {
    pub fn new(block: Hash32, expected_count: u32) -> Result<Self, &'static str> {
        if expected_count > u32::from(ids::INDEX_LIMIT) {
            return Err("provider_transaction_count_limit");
        }
        Ok(Self {
            block,
            expected_count: expected_count as usize,
            next_page: 1,
            complete: expected_count == 0,
            captured: CapturedTransactions::default(),
        })
    }

    pub fn push(&mut self, page: TransactionPage) -> Result<(), &'static str> {
        if self.complete {
            return Err("provider_page_after_complete");
        }
        if page.block != self.block {
            return Err("provider_page_block_mismatch");
        }
        if page.page.number() != self.next_page {
            return Err("provider_page_order_mismatch");
        }
        let count = self.captured.len() + page.len();
        if count > self.expected_count || page.len() < 100 && count != self.expected_count {
            return Err("provider_transaction_count_mismatch");
        }
        self.next_page = self
            .next_page
            .checked_add(1)
            .ok_or("provider_page_order_mismatch")?;
        self.complete = count == self.expected_count;
        self.captured.append(page.captured);
        Ok(())
    }

    pub fn is_complete(&self) -> bool {
        self.complete
    }

    pub fn finish(self) -> Result<CapturedTransactions, &'static str> {
        if !self.complete || self.captured.len() != self.expected_count {
            return Err("provider_transaction_count_mismatch");
        }
        Ok(self.captured)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn records() -> Vec<TransactionCbor> {
        let mut records: Vec<TransactionCbor> = serde_json::from_str(include_str!(
            "../../dsa-chain/tests/fixtures/finalized-transactions.json"
        ))
        .unwrap();
        records.truncate(2);
        records
    }

    #[test]
    fn captured_pages_preserve_original_items_and_refuse_page_changes() {
        let block: Hash32 = "01".repeat(32).parse().unwrap();
        let original = records();
        let page = TransactionPage::capture(block.clone(), Page::new(1).unwrap(), original.clone())
            .unwrap();
        let mut detached = page.records().to_vec();
        detached.reverse();
        assert_eq!(
            page.records(),
            original,
            "a detached cache copy cannot mutate the received page"
        );
        let mut collected = CapturedBlock::new(block.clone(), 2).unwrap();
        collected.push(page).unwrap();
        assert!(collected.is_complete());
        let captured = collected.finish().unwrap();
        let decoded = captured.decode().unwrap();
        assert_eq!(
            decoded
                .transactions()
                .iter()
                .map(|tx| tx.hash.as_str())
                .collect::<Vec<_>>(),
            original
                .iter()
                .map(|record| record.tx_hash.as_str())
                .collect::<Vec<_>>()
        );
        decoded.validate().unwrap();
        let mut out_of_order = CapturedBlock::new(block.clone(), 2).unwrap();
        assert_eq!(
            out_of_order
                .push(
                    TransactionPage::capture(
                        block.clone(),
                        Page::new(2).unwrap(),
                        original.clone()
                    )
                    .unwrap()
                )
                .unwrap_err(),
            "provider_page_order_mismatch"
        );
        let mut wrong_block = CapturedBlock::new(block, 2).unwrap();
        assert_eq!(
            wrong_block
                .push(
                    TransactionPage::capture(
                        "02".repeat(32).parse().unwrap(),
                        Page::new(1).unwrap(),
                        original
                    )
                    .unwrap()
                )
                .unwrap_err(),
            "provider_page_block_mismatch"
        );
    }

    #[test]
    fn endpoint_pages_retry_atomically_and_preserve_all_103_original_records() {
        use sha2::{Digest, Sha256};
        let raw = include_bytes!("../tests/fixtures/pagination-finalized.json");
        let manifest: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/pagination-finalized.manifest.json"
        ))
        .unwrap();
        assert_eq!(
            format!("{:x}", Sha256::digest(raw)),
            manifest["fixture_sha256"].as_str().unwrap()
        );
        assert_eq!(raw.len() as u64, manifest["bytes"].as_u64().unwrap());
        let bundle: serde_json::Value = serde_json::from_slice(raw).unwrap();
        assert_eq!(bundle["format"], "finalized-pagination-inputs-v1");
        assert_eq!(
            bundle["recording_plan_sha256"],
            manifest["recording_plan_sha256"]
        );
        let records: Vec<TransactionCbor> =
            serde_json::from_value(bundle["records"].clone()).unwrap();
        assert_eq!(records.len(), 103);
        for record in bundle["records"].as_array().unwrap() {
            let source_height = record["source_height"].as_u64().unwrap();
            assert!((5395925..=5397924).contains(&source_height));
            assert_eq!(
                record["source_sha256"],
                manifest["source_files"][source_height.to_string()]
            );
        }
        // Controlled transport pages, composed of intact finalized transactions
        // from several actual blocks. This declares no real 103-item block.
        let block: Hash32 = "01".repeat(32).parse().unwrap();
        let page = |number, items: &[TransactionCbor]| {
            let endpoint = crate::types::Endpoint::BlockCbor {
                block: block.clone(),
                page: Page::new(number).unwrap(),
            };
            let crate::types::Reply::Transactions(page) = endpoint
                .decode(&serde_json::to_vec(items).unwrap())
                .unwrap()
            else {
                panic!("CBOR page reply")
            };
            page
        };
        let mut captured = CapturedBlock::new(block.clone(), 103).unwrap();
        assert_eq!(
            captured.push(page(2, &records[100..])).unwrap_err(),
            "provider_page_order_mismatch"
        );
        assert_eq!(
            captured.push(page(1, &records[..99])).unwrap_err(),
            "provider_transaction_count_mismatch"
        );
        assert!(!captured.is_complete());
        captured.push(page(1, &records[..100])).unwrap();
        captured.push(page(2, &records[100..])).unwrap();
        assert!(captured.is_complete());
        assert_eq!(
            captured.push(page(2, &records[100..])).unwrap_err(),
            "provider_page_after_complete"
        );
        let source = captured.finish().unwrap();
        let decoded = source.decode().unwrap();
        assert_eq!(
            decoded
                .transactions()
                .iter()
                .map(|tx| tx.hash.as_str())
                .collect::<Vec<_>>(),
            records
                .iter()
                .map(|record| record.tx_hash.as_str())
                .collect::<Vec<_>>()
        );
        let profile = dsa_chain::profiles::Profile::load("preprod").unwrap();
        let previous = dsa_chain::checks::Header {
            height: 9,
            hash: "02".repeat(32),
            previous_hash: None,
            transaction_count: 0,
        };
        let header = dsa_chain::checks::Header {
            height: 10,
            hash: block.to_string(),
            previous_hash: Some(previous.hash.clone()),
            transaction_count: 103,
        };
        let mut held = std::collections::BTreeMap::new();
        let supplements = dsa_chain::rows::Supplements::default();
        assert_eq!(
            dsa_chain::checks::check_block(
                &profile,
                &previous,
                &header,
                9,
                &decoded,
                &held,
                &supplements
            )
            .unwrap()
            .transactions_checked,
            103
        );
        let rows =
            dsa_chain::rows::build_block(&profile, 10, &decoded, &mut held, &supplements).unwrap();
        assert!(
            rows.tx.is_empty() && held.is_empty(),
            "the quiet real sources have no selected facts"
        );
        let mut duplicate_tail = records[100..].to_vec();
        duplicate_tail[0] = records[0].clone();
        let mut duplicate = CapturedBlock::new(block.clone(), 103).unwrap();
        duplicate.push(page(1, &records[..100])).unwrap();
        duplicate.push(page(2, &duplicate_tail)).unwrap();
        let duplicate_source = duplicate.finish().unwrap();
        let duplicate_decoded = duplicate_source.decode().unwrap();
        assert_eq!(
            dsa_chain::checks::check_block(
                &profile,
                &previous,
                &header,
                9,
                &duplicate_decoded,
                &held,
                &supplements
            )
            .unwrap_err(),
            dsa_chain::checks::CheckError::DuplicateTransaction {
                height: 10,
                transaction: records[0].tx_hash.to_string()
            }
        );
        assert!(matches!(
            CapturedBlock::new("03".repeat(32).parse().unwrap(), 4097),
            Err("provider_transaction_count_limit")
        ));
    }
}
