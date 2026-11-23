//! Mutations of real finalized transactions through decoding, datum
//! rendering and metadata reading: each returns a value or an error and
//! never panics. Mutated bodies are also decoded under their own hash, so
//! the field readers see the changed bytes.

use dsa_chain::{cbor::Item, datum, metadata, transaction};
use serde::Deserialize;

#[derive(Deserialize)]
struct Record {
    tx_hash: String,
    cbor: String,
}

fn next(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}

/// Decode `raw`, also under the hash of its own first element.
fn exercise(raw: &[u8], expected: &str, outcomes: &mut [usize; 2]) {
    outcomes[usize::from(transaction::decode(raw, expected).is_ok())] += 1;
    let Ok(envelope) = Item::parse(raw).and_then(Item::array) else {
        return;
    };
    let Some(body) = envelope.first() else {
        return;
    };
    let Ok(tx) = transaction::decode(raw, &transaction::hash(body.raw)) else {
        outcomes[0] += 1;
        return;
    };
    outcomes[1] += 1;
    for datum in tx.datums().values() {
        let _ = datum::render(datum);
    }
    for output in tx.outputs().iter().flatten().chain(tx.collateral_return()) {
        if let Some(inline) = &output.inline_datum {
            let _ = datum::render(inline);
        }
    }
    let _ = metadata::observe(tx.auxiliary());
}

#[test]
fn mutated_real_transactions_decode_or_fail_without_panicking() {
    let records: Vec<Record> =
        serde_json::from_str(include_str!("fixtures/finalized-transactions.json")).unwrap();
    let mut seed = 0x51d4_a386_febc_0091u64;
    let mut outcomes = [0usize; 2];
    let mut mutations = 0;
    for record in &records {
        let original = hex::decode(&record.cbor).unwrap();
        for _ in 0..400 {
            let mut raw = original.clone();
            let at = (next(&mut seed) as usize) % raw.len();
            match next(&mut seed) % 5 {
                0 => raw[at] ^= 1 << (next(&mut seed) % 8),
                1 => raw[at] = next(&mut seed) as u8,
                2 => raw.truncate(at),
                3 => {
                    raw.insert(at, next(&mut seed) as u8);
                }
                _ => {
                    raw.remove(at);
                }
            }
            exercise(&raw, &record.tx_hash, &mut outcomes);
            mutations += 1;
        }
        // Each datum alone, cut at every offset.
        let tx = transaction::decode(&original, &record.tx_hash).unwrap();
        for datum in tx.datums().values() {
            for end in 0..datum.len() {
                assert!(datum::render(&datum[..end]).is_err());
            }
        }
    }
    println!(
        "mutations={mutations} decoded={} refused={}",
        outcomes[1], outcomes[0]
    );
    assert_eq!(mutations, records.len() * 400);
    assert!(
        outcomes[1] > 0,
        "some mutations keep a decodable transaction"
    );
    assert!(outcomes[0] > 0);
}
