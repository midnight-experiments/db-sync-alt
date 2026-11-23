//! A block's transaction list arrives in pages of exactly 100.
//! [`BlockPages`] binds each page to the block hash, the page number and the
//! header's transaction count, and keeps each page's answer bytes.

use bytes::Bytes;
use dsa_chain::ids;

use crate::types::{Hash32, Page, TransactionCbor};

/// Transactions per page.
pub const PAGE_SIZE: usize = 100;

#[derive(Clone, Debug)]
pub struct TransactionPage {
    pub block: Hash32,
    pub page: Page,
    pub transactions: Vec<TransactionCbor>,
    pub body: Bytes,
}

/// A complete transaction list in block order and the answers it came from.
#[derive(Clone, Debug)]
pub struct BlockTransactions {
    pub transactions: Vec<TransactionCbor>,
    /// `pages[n - 1]` is the answer for page `n`.
    pub pages: Vec<Bytes>,
}

impl BlockTransactions {
    /// `(hash, CBOR)` pairs for `dsa_chain::transaction::decode_all`.
    pub fn records(&self) -> impl Iterator<Item = (&str, &[u8])> {
        self.transactions
            .iter()
            .map(|tx| (tx.tx_hash.as_str(), tx.cbor.0.as_slice()))
    }
}

pub struct BlockPages {
    block: Hash32,
    expected: usize,
    next: Option<Page>,
    received: BlockTransactions,
}

impl BlockPages {
    pub fn new(block: Hash32, expected_count: u32) -> Result<Self, &'static str> {
        if expected_count > u32::from(ids::INDEX_LIMIT) {
            return Err("provider_transaction_count_limit");
        }
        Ok(Self {
            block,
            expected: expected_count as usize,
            next: (expected_count > 0).then(Page::first),
            received: BlockTransactions {
                transactions: Vec::new(),
                pages: Vec::new(),
            },
        })
    }

    /// The page to request next, or `None` when the list is complete.
    pub fn next_page(&self) -> Option<Page> {
        self.next
    }

    pub fn push(&mut self, page: TransactionPage) -> Result<(), &'static str> {
        let Some(expected) = self.next else {
            return Err("provider_page_after_complete");
        };
        if page.block != self.block {
            return Err("provider_page_block_mismatch");
        }
        if page.page != expected {
            return Err("provider_page_order_mismatch");
        }
        let count = self.received.transactions.len() + page.transactions.len();
        if count > self.expected || page.transactions.len() < PAGE_SIZE && count != self.expected {
            return Err("provider_transaction_count_mismatch");
        }
        self.next = if count == self.expected {
            None
        } else {
            Some(expected.next().ok_or("provider_page_order_mismatch")?)
        };
        self.received.transactions.extend(page.transactions);
        self.received.pages.push(page.body);
        Ok(())
    }

    pub fn is_complete(&self) -> bool {
        self.next.is_none()
    }

    pub fn finish(self) -> Result<BlockTransactions, &'static str> {
        if self.next.is_some() {
            return Err("provider_transaction_count_mismatch");
        }
        Ok(self.received)
    }
}
