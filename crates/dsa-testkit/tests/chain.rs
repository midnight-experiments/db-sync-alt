//! The scripted chain answers as the provider does.

use dsa_testkit::{
    chain::{Chain, PAGE},
    server::Behavior,
};
use serde_json::Value;

fn body(behavior: &Behavior) -> Value {
    assert_eq!(behavior.status, 200, "{}", behavior.body);
    serde_json::from_str(&behavior.body).unwrap()
}

#[test]
fn pages_hold_exactly_one_hundred_and_link_by_hash() {
    let mut chain = Chain::new(10, &[0, 100, 201]);
    let full = chain.at(11).unwrap().hash.clone();
    let three = chain.at(12).unwrap().hash.clone();
    let page = |chain: &mut Chain, hash: &str, page: usize| {
        body(&chain.answer(&format!(
            "/api/v0/blocks/{hash}/txs/cbor?count=100&page={page}&order=asc"
        )))
        .as_array()
        .unwrap()
        .len()
    };
    assert_eq!(page(&mut chain, &full, 1), PAGE);
    assert_eq!(page(&mut chain, &full, 2), 0);
    assert_eq!(
        [1, 2, 3, 4].map(|number| page(&mut chain, &three, number)),
        [100, 100, 1, 0]
    );
    let tip = body(&chain.answer("/api/v0/blocks/latest"));
    assert_eq!(tip["height"], 12);
    assert_eq!(tip["tx_count"], 201);
    assert_eq!(tip["previous_block"], full.as_str());
    let first = chain.at(10).unwrap().hash.clone();
    let next = body(&chain.answer(&format!("/api/v0/blocks/{first}/next?count=100&page=1")));
    assert_eq!(next.as_array().unwrap().len(), 2);
    assert_eq!(next[0]["hash"], full.as_str());
    assert_eq!(
        body(&chain.answer("/api/v0/blocks/11"))["hash"],
        full.as_str()
    );
    assert_eq!(chain.answer("/api/v0/blocks/13").status, 404);
}

#[test]
fn a_tip_fork_replaces_only_the_top_blocks() {
    let mut chain = Chain::new(100, &[1; 10]);
    let before: Vec<_> = chain.blocks().iter().map(|b| b.hash.clone()).collect();
    chain.fork(3);
    let after: Vec<_> = chain.blocks().iter().map(|b| b.hash.clone()).collect();
    assert_eq!(before[..7], after[..7]);
    assert!(before[7..].iter().zip(&after[7..]).all(|(a, b)| a != b));
    assert_eq!(chain.at(107).unwrap().previous.as_ref(), Some(&after[6]));
    assert_eq!(
        chain
            .answer(&format!(
                "/api/v0/blocks/{}/next?count=100&page=1",
                before[9]
            ))
            .status,
        404,
        "the old tip left the chain"
    );
    let next = body(&chain.answer(&format!(
        "/api/v0/blocks/{}/next?count=100&page=1",
        before[6]
    )));
    assert_eq!(next[0]["hash"], after[7].as_str());
    assert_ne!(
        chain.at(109).unwrap().transactions[0].hash,
        Chain::new(100, &[1; 10]).at(109).unwrap().transactions[0].hash
    );
}

#[test]
fn a_contradicted_block_and_faults_are_served() {
    let mut chain = Chain::new(1, &[1, 1, 1]);
    let old = chain.at(2).unwrap().hash.clone();
    chain.contradict(2);
    let new = body(&chain.answer("/api/v0/blocks/2"))["hash"].clone();
    assert_ne!(new, old.as_str());
    assert_eq!(chain.at(3).unwrap().previous.as_ref(), Some(&old));
    chain.fault("/blocks/latest", Behavior::status(402), 2);
    chain.fault("/txs/cbor", Behavior::dropped(), 1);
    assert_eq!(chain.answer("/api/v0/blocks/latest").status, 402);
    assert_eq!(chain.answer("/api/v0/blocks/latest").status, 402);
    assert_eq!(chain.answer("/api/v0/blocks/latest").status, 200);
    let hash = chain.at(1).unwrap().hash.clone();
    let path = format!("/api/v0/blocks/{hash}/txs/cbor?count=100&page=1&order=asc");
    assert!(chain.answer(&path).dropped);
    assert_eq!(body(&chain.answer(&path)).as_array().unwrap().len(), 1);
    chain.route("/epochs/0/parameters", serde_json::json!({"epoch": 0}));
    assert_eq!(
        body(&chain.answer("/api/v0/epochs/0/parameters"))["epoch"],
        0
    );
}
