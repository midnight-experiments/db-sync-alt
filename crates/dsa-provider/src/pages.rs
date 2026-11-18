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
/// and within-page order are immutable. Header/list truth remains Q3's trust.
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
}
