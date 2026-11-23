//! Page assembly binds pages to their block, number and count, and the
//! assembled list feeds the block function in order.

use std::collections::BTreeMap;

use bytes::Bytes;
use dsa_chain::{
    block::{BlockError, Header, Outcome, Selection, Supplements, process_block},
    profiles::Profile,
    transaction,
};
use dsa_provider::{
    pages::{BlockPages, TransactionPage},
    types::{Hash32, Page, TransactionCbor},
};
use sha2::{Digest, Sha256};

fn page(block: &Hash32, number: u32, transactions: &[TransactionCbor]) -> TransactionPage {
    TransactionPage {
        block: block.clone(),
        page: Page::new(number).unwrap(),
        transactions: transactions.to_vec(),
        body: Bytes::from(format!("page {number}")),
    }
}

#[test]
fn pages_of_a_103_transaction_list_are_bound_and_assembled_in_order() {
    let raw = include_bytes!("fixtures/pagination-finalized.json");
    let manifest: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/pagination-finalized.manifest.json")).unwrap();
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
    for record in bundle["records"].as_array().unwrap() {
        let source_height = record["source_height"].as_u64().unwrap();
        assert!((5395925..=5397924).contains(&source_height));
        assert_eq!(
            record["source_sha256"],
            manifest["source_files"][source_height.to_string()]
        );
    }
    // Controlled transport pages of intact finalized transactions from
    // several real blocks; no real block holds these 103.
    let records: Vec<TransactionCbor> = serde_json::from_value(bundle["records"].clone()).unwrap();
    assert_eq!(records.len(), 103);
    let block: Hash32 = "01".repeat(32).parse().unwrap();
    let mut pages = BlockPages::new(block.clone(), 103).unwrap();
    assert_eq!(pages.next_page(), Some(Page::first()));
    assert_eq!(
        pages.push(page(&block, 2, &records[100..])).unwrap_err(),
        "provider_page_order_mismatch"
    );
    assert_eq!(
        pages.push(page(&block, 1, &records[..99])).unwrap_err(),
        "provider_transaction_count_mismatch"
    );
    let other: Hash32 = "02".repeat(32).parse().unwrap();
    assert_eq!(
        pages.push(page(&other, 1, &records[..100])).unwrap_err(),
        "provider_page_block_mismatch"
    );
    pages.push(page(&block, 1, &records[..100])).unwrap();
    assert_eq!(pages.next_page(), Some(Page::new(2).unwrap()));
    pages.push(page(&block, 2, &records[100..])).unwrap();
    assert!(pages.is_complete());
    assert_eq!(
        pages.push(page(&block, 3, &[])).unwrap_err(),
        "provider_page_after_complete"
    );
    let assembled = pages.finish().unwrap();
    assert_eq!(assembled.transactions, records);
    assert_eq!(
        assembled.pages,
        vec![Bytes::from("page 1"), Bytes::from("page 2")]
    );
    let profile = Profile::load("preprod").unwrap();
    let selection = Selection::new(&profile);
    let header = Header {
        height: 10,
        hash: block.to_string(),
        previous_hash: Some("02".repeat(32)),
        transaction_count: 103,
    };
    let held = BTreeMap::new();
    let supplements = Supplements::default();
    let decoded = transaction::decode_all(assembled.records()).unwrap();
    let Outcome::Ready(output) =
        process_block(&selection, &header, &decoded, &held, &supplements).unwrap()
    else {
        panic!("the quiet real transactions need nothing")
    };
    assert!(output.rows.tx.is_empty() && output.created.is_empty());
    let mut tail = records[100..].to_vec();
    tail[0] = records[0].clone();
    let mut duplicate = BlockPages::new(block.clone(), 103).unwrap();
    duplicate.push(page(&block, 1, &records[..100])).unwrap();
    duplicate.push(page(&block, 2, &tail)).unwrap();
    let duplicate = duplicate.finish().unwrap();
    assert_eq!(
        process_block(
            &selection,
            &header,
            &transaction::decode_all(duplicate.records()).unwrap(),
            &held,
            &supplements
        )
        .unwrap_err(),
        BlockError::DuplicateTransaction {
            height: 10,
            transaction: records[0].tx_hash.to_string()
        }
    );
    let empty = BlockPages::new(block.clone(), 0).unwrap();
    assert!(empty.is_complete() && empty.next_page().is_none());
    assert!(empty.finish().unwrap().transactions.is_empty());
    assert!(matches!(
        BlockPages::new(block, 4097),
        Err("provider_transaction_count_limit")
    ));
}
