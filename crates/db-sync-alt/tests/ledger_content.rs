//! Ledger-valid content and provider renderings that must never stop the
//! follower. Each test puts its content into one block next to an ordinary
//! relevant transaction (a held cNIGHT output moved to a new holder and a
//! registration datum paid to the mapping address), serves the chain from the
//! fake provider the way the provider renders it, and follows it through the
//! loop into PostgreSQL 17 in DSA_TEST_DATABASE. Every test asserts that the
//! block is ingested, that the ordinary transaction is served through the
//! public views with its rows, that the gate stays open, that the loop never
//! retried and that `verify` passes.
//!
//! The recorded transactions come from the Mainnet corpus of the provider
//! crate (`tests/fixtures/recorded-transactions.json` names each member); a
//! Preprod profile makes them unrelated, as they are to any store that does
//! not hold their inputs.

#[path = "common/legacy.rs"]
#[allow(dead_code)]
mod common;
#[path = "follow/harness.rs"]
#[allow(dead_code)]
mod harness;
#[path = "follower/support.rs"]
#[allow(dead_code)]
mod support;

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use dsa_chain::{
    block::Held,
    transaction::{Outpoint, address_bytes, address_text},
};
use dsa_service::{
    epoch::stake_prefix,
    follow::{self, Progress, Report},
    retry::Stop,
};
use dsa_testkit::{
    chain::{self as scripted, Block, Chain},
    server::Behavior,
    tx::{self, Built, Output, Tx, enterprise_address},
};
use harness::{K, Result, apply, chain, named, profile};
use serde_json::{Value, json};
use support::{Fake, Hook, Start, context, until};
use tokio::sync::watch;
use tokio_postgres::Config;

/// A registration datum: `Constr 0 [bytes(28), 7]`, and its db-sync value.
const REGISTRATION: &str = "d87982581c0101010101010101010101010101010101010101010101010101010107";
const REGISTRATION_VALUE: &str = r#"{"constructor":0,"fields":[{"bytes":"01010101010101010101010101010101010101010101010101010101"},{"int":7}]}"#;

/// `levels` nested lists around 0, and db-sync's value: NULL past 2,000 JSON
/// levels (the value nests 2 × `levels` + 1 deep).
fn nested_lists(levels: usize) -> (Vec<u8>, Option<String>) {
    let mut raw = vec![0x81; levels];
    raw.push(0);
    let value = (2 * levels < 2000).then(|| {
        format!(
            "{}{{\"int\":0}}{}",
            "{\"list\":[".repeat(levels),
            "]}".repeat(levels)
        )
    });
    (raw, value)
}

/// `levels` nested `Constr 0 [...]` around 0, and db-sync's value.
fn nested_constructors(levels: usize) -> (Vec<u8>, Option<String>) {
    let mut raw = [0xd8, 0x79, 0x81].repeat(levels);
    raw.push(0);
    let value = (2 * levels < 2000).then(|| {
        format!(
            "{}{{\"int\":0}}{}",
            "{\"constructor\":0,\"fields\":[".repeat(levels),
            "]}".repeat(levels)
        )
    });
    (raw, value)
}

/// A JSON array nested `levels` deep around 0, as the provider renders a
/// metadatum of `levels` nested lists.
fn nested_json(levels: usize) -> String {
    format!("{}0{}", "[".repeat(levels), "]".repeat(levels))
}

/// Datums with exact integers that the provider's JSON rendering rounds, and
/// a map with a repeated key: (CBOR, db-sync value).
fn integers(offset: u64) -> Vec<(Vec<u8>, String)> {
    let big = (1u64 << 53) + offset;
    let bignum: u128 = 123_456_789_012_345_678_901_234_567_890 + u128::from(offset);
    let mut huge = vec![0xff_u8; 2000];
    let last = huge.len() - 1;
    huge[last] = 0xff - offset as u8;
    let mut chunked = vec![0xc2, 0x5f];
    for chunk in huge.chunks(64) {
        if chunk.len() == 64 {
            chunked.extend([0x58, 0x40]);
        } else {
            chunked.push(0x40 | chunk.len() as u8);
        }
        chunked.extend_from_slice(chunk);
    }
    chunked.push(0xff);
    let huge_value = {
        // 2^16000 - 1 - offset, written out in decimal.
        let mut digits = vec![0u64];
        for byte in &huge {
            let mut carry = u64::from(*byte);
            for digit in digits.iter_mut() {
                let value = *digit * 256 + carry;
                *digit = value % 1_000_000_000;
                carry = value / 1_000_000_000;
            }
            while carry > 0 {
                digits.push(carry % 1_000_000_000);
                carry /= 1_000_000_000;
            }
        }
        let mut text = digits.last().unwrap().to_string();
        for digit in digits.iter().rev().skip(1) {
            text.push_str(&format!("{digit:09}"));
        }
        text
    };
    let mut bignum_raw = vec![0xc2, 0x4d];
    bignum_raw.extend_from_slice(&bignum.to_be_bytes()[3..]);
    let mut constructor = vec![0xd8, 0x66, 0x82, 0x1b];
    constructor.extend(big.to_be_bytes());
    constructor.push(0x80);
    let mut word = vec![0x1b];
    word.extend(big.to_be_bytes());
    vec![
        (word, format!("{{\"int\":{big}}}")),
        (bignum_raw, format!("{{\"int\":{bignum}}}")),
        (
            constructor,
            format!("{{\"constructor\":{big},\"fields\":[]}}"),
        ),
        (chunked, format!("{{\"int\":{huge_value}}}")),
        (
            vec![0xa2, offset as u8, 0x00, offset as u8, 0x00],
            format!(
                "{{\"map\":[{{\"k\":{{\"int\":{offset}}},\"v\":{{\"int\":0}}}},{{\"k\":{{\"int\":{offset}}},\"v\":{{\"int\":0}}}}]}}"
            ),
        ),
    ]
}

/// The CBOR of a bytes item.
fn cbor_bytes(bytes: &[u8]) -> Vec<u8> {
    assert!(bytes.len() < 24);
    let mut out = vec![0x40 | bytes.len() as u8];
    out.extend_from_slice(bytes);
    out
}

/// Recorded Mainnet transactions by their shape.
fn recorded() -> Vec<(String, Built)> {
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/recorded-transactions.json"
        ))
        .unwrap(),
    )
    .unwrap();
    fixture["transactions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| {
            (
                entry["shape"].as_str().unwrap().to_owned(),
                Built {
                    hash: entry["tx_hash"].as_str().unwrap().to_owned(),
                    cbor: hex::decode(entry["cbor"].as_str().unwrap()).unwrap(),
                },
            )
        })
        .collect()
}

/// A converted store on a generated chain, the cNIGHT outputs still unspent
/// at the chain's end, and the fake provider with answers a test sets.
struct Scenario {
    start: Start,
    fake: Fake,
    night: Vec<(Outpoint, Held)>,
    /// Exact answers by path below the provider's base URL.
    answers: Arc<Mutex<BTreeMap<String, String>>>,
    network: u8,
}

/// What an ordinary relevant transaction should be served as.
struct Ordinary {
    built: Built,
    expected: Value,
    datum: String,
}

impl Scenario {
    async fn new(seed: u64) -> Result<Self> {
        let profile = profile(seed);
        let g = chain(&profile, seed, 1, 30, false).known_from_start();
        let start = Start::from_chain(profile, g, 10).await?;
        let mut held = BTreeMap::new();
        for output in &start.history {
            apply(&mut held, output);
        }
        let unit = start.profile.assets.cnight.unit();
        let night = held
            .into_iter()
            .filter(|(_, held)| held.assets.iter().any(|asset| asset.unit() == unit))
            .collect::<Vec<_>>();
        assert!(night.len() >= 2, "{} unspent cNIGHT outputs", night.len());
        let answers = Arc::new(Mutex::new(BTreeMap::<String, String>::new()));
        let hook: Hook = {
            let answers = answers.clone();
            Arc::new(move |path: &str, _: &mut Chain, _: u64| {
                let route = path.split('?').next().unwrap_or(path);
                let route = route.strip_prefix("/api/v0").unwrap_or(route);
                answers
                    .lock()
                    .unwrap()
                    .get(route)
                    .map(|body| Behavior::raw(body.clone()))
            })
        };
        let fake = Fake::with_chain(
            Chain::from_generated(&start.g, stake_prefix(&start.profile.network)),
            Some(hook),
        )
        .await;
        let network = u8::from(start.profile.network == "mainnet");
        Ok(Self {
            start,
            fake,
            night,
            answers,
            network,
        })
    }

    fn answer(&self, path: &str, body: String) {
        self.answers.lock().unwrap().insert(path.to_owned(), body);
    }

    fn address(&self, text: &str) -> Vec<u8> {
        address_bytes(text).unwrap()
    }

    /// An address nobody selects, from a key seed.
    fn stranger(&self, seed: u8) -> Vec<u8> {
        enterprise_address(self.network, [seed; 28])
    }

    /// Spends the `n`th unspent cNIGHT output to a new holder and pays a
    /// registration datum to the mapping address.
    fn ordinary(&self, n: usize) -> Ordinary {
        let (point, held) = &self.night[n];
        let holder = enterprise_address(self.network, [0x40 + n as u8; 28]);
        let mut output = Output::new(holder.clone(), 2_000_000);
        for asset in &held.assets {
            output = output.token(&asset.policy, &asset.name, asset.quantity);
        }
        let registration = hex::decode(REGISTRATION).unwrap();
        let datum = tx::hash(&registration);
        let mapping = self.address(&self.start.profile.addresses.mapping);
        let built = Tx::new()
            .input(&point.hash, point.index)
            .output(output)
            .output(Output::new(mapping, 1_500_000).inline_datum(registration))
            .fee(171_000 + n as u64)
            .build();
        let assets = held
            .assets
            .iter()
            .map(|asset| json!([asset.policy, asset.name, asset.quantity.to_string()]))
            .collect::<Vec<_>>();
        let expected = json!({
            "index": 0, "valid": true,
            "inputs": [[held.transaction_id, point.index]],
            "outputs": [
                {"index": 0, "address": address_text(&holder).unwrap(), "value": "2000000",
                 "data_hash": null, "assets": assets},
                {"index": 1, "address": self.start.profile.addresses.mapping, "value": "1500000",
                 "data_hash": datum, "assets": []},
            ],
            "metadata": [],
        });
        Ordinary {
            built,
            expected,
            datum,
        }
    }

    /// Serves the generated chain, then `block` after it and `k` empty blocks.
    fn serve(&self, transactions: Vec<Built>) -> Block {
        let mut blocks = self.start.g.blocks.clone();
        let block = Block::after(blocks.last().unwrap(), transactions);
        blocks.push(block.clone());
        for _ in 0..K {
            let next = Block::after(blocks.last().unwrap(), Vec::new());
            blocks.push(next);
        }
        self.fake.serve(blocks);
        block
    }

    /// Follows up to `block` and checks what every test asserts.
    async fn follow(&self, block: &Block, ordinary: &Ordinary) -> Result<Progress> {
        let (_stopper, stop) = Stop::new();
        let (progress, view) = watch::channel(Progress::default());
        let report: Report = follow::follow(context(
            &self.start.profile,
            &self.fake,
            &self.start.database.config,
            until(block.height),
            stop,
            progress,
        ))
        .await
        .map_err(|error| error.report().to_string())?;
        let last = view.borrow().clone();
        assert_eq!(report.position.target.height, block.height);
        assert_eq!(
            last.last_error, None,
            "the loop retried or stopped on the way"
        );
        assert_eq!(
            self.fake
                .requests(&format!("/blocks/{}/txs/cbor", block.hash)),
            block.transactions.len().div_ceil(scripted::PAGE),
            "each page of the block was read once"
        );
        let mut store = self.start.store().await?;
        let state = store
            .follow_state(&self.start.profile)
            .await
            .map_err(named)?;
        assert!(state.serving, "the gate is open: {}", state.reason);
        let mut ordinary_served = served(self.config(), &ordinary.built.hash).await?;
        ordinary_served["block"] = Value::Null;
        let mut expected = ordinary.expected.clone();
        expected["block"] = Value::Null;
        assert_eq!(ordinary_served, expected, "the ordinary transaction");
        assert_eq!(
            datum_value(self.config(), &ordinary.datum).await?,
            Some(Some(true)),
            "the ordinary registration datum"
        );
        assert_eq!(
            datum_matches(self.config(), &ordinary.datum, Some(REGISTRATION_VALUE)).await?,
            Some(true)
        );
        store.verify(&self.start.profile).await.map_err(named)?;
        Ok(last)
    }

    fn config(&self) -> &Config {
        &self.start.database.config
    }
}

/// Everything the public views serve for one transaction, or null.
async fn served(config: &Config, hash: &str) -> Result<Value> {
    let client = common::connect(config).await?;
    let Some(tx) = client
        .query_opt(
            "SELECT t.id,b.block_no::bigint,t.block_index::bigint,t.valid_contract
             FROM public.tx t JOIN public.block b ON b.id=t.block_id WHERE t.hash=$1",
            &[&hex::decode(hash)?],
        )
        .await?
    else {
        return Ok(Value::Null);
    };
    let id: i64 = tx.get(0);
    let inputs = client
        .query(
            "SELECT tx_out_id,tx_out_index::integer FROM public.tx_in WHERE tx_in_id=$1 ORDER BY id",
            &[&id],
        )
        .await?
        .iter()
        .map(|row| json!([row.get::<_, i64>(0), row.get::<_, i32>(1)]))
        .collect::<Vec<_>>();
    let mut outputs = Vec::new();
    for row in client
        .query(
            "SELECT id,index::integer,address,value::text,encode(data_hash,'hex')
             FROM public.tx_out WHERE tx_id=$1 ORDER BY index",
            &[&id],
        )
        .await?
    {
        let assets = client
            .query(
                "SELECT encode(m.policy,'hex'),encode(m.name,'hex'),a.quantity::text
                 FROM public.ma_tx_out a JOIN public.multi_asset m ON m.id=a.ident
                 WHERE a.tx_out_id=$1 ORDER BY a.id",
                &[&row.get::<_, i64>(0)],
            )
            .await?
            .iter()
            .map(|asset| {
                json!([
                    asset.get::<_, String>(0),
                    asset.get::<_, String>(1),
                    asset.get::<_, String>(2)
                ])
            })
            .collect::<Vec<_>>();
        outputs.push(json!({
            "index": row.get::<_, i32>(1), "address": row.get::<_, String>(2),
            "value": row.get::<_, String>(3), "data_hash": row.get::<_, Option<String>>(4),
            "assets": assets,
        }));
    }
    let metadata = client
        .query(
            "SELECT key::text FROM public.tx_metadata WHERE tx_id=$1",
            &[&id],
        )
        .await?
        .iter()
        .map(|row| json!(row.get::<_, String>(0)))
        .collect::<Vec<_>>();
    Ok(json!({
        "block": tx.get::<_, i64>(1), "index": tx.get::<_, i64>(2), "valid": tx.get::<_, bool>(3),
        "inputs": inputs, "outputs": outputs, "metadata": metadata,
    }))
}

/// `None` without a datum row; otherwise whether its value is not NULL.
async fn datum_value(config: &Config, hash: &str) -> Result<Option<Option<bool>>> {
    let client = common::connect(config).await?;
    Ok(client
        .query_opt(
            "SELECT value IS NOT NULL FROM public.datum WHERE hash=$1",
            &[&hex::decode(hash)?],
        )
        .await?
        .map(|row| Some(row.get(0))))
}

/// `None` without a datum row; otherwise whether its value equals `expected`
/// as JSON (both NULL counts as equal).
async fn datum_matches(
    config: &Config,
    hash: &str,
    expected: Option<&str>,
) -> Result<Option<bool>> {
    let client = common::connect(config).await?;
    Ok(client
        .query_opt(
            "SELECT value IS NOT DISTINCT FROM $2::text::jsonb FROM public.datum WHERE hash=$1",
            &[&hex::decode(hash)?, &expected],
        )
        .await?
        .map(|row| row.get(0)))
}

/// `None` without a bridge metadata row of the transaction; otherwise
/// whether its JSON equals `expected` (both NULL counts as equal).
async fn metadata_matches(
    config: &Config,
    transaction: &str,
    expected: Option<&str>,
) -> Result<Option<bool>> {
    let client = common::connect(config).await?;
    Ok(client
        .query_opt(
            "SELECT m.json IS NOT DISTINCT FROM $2::text::jsonb
             FROM public.tx_metadata m JOIN public.tx t ON t.id=m.tx_id WHERE t.hash=$1",
            &[&hex::decode(transaction)?, &expected],
        )
        .await?
        .map(|row| row.get(0)))
}

/// One output that holds `assets` at `address`, as served.
fn output(index: u16, address: &str, value: u64, data_hash: Option<&str>, assets: Value) -> Value {
    json!({"index": index, "address": address, "value": value.to_string(),
        "data_hash": data_hash, "assets": assets})
}

fn assets_of(held: &Held) -> Value {
    held.assets
        .iter()
        .map(|asset| json!([asset.policy, asset.name, asset.quantity.to_string()]))
        .collect()
}

/// An unrelated transaction carrying `witness_set` or `auxiliary_data`.
fn unrelated(scenario: &Scenario, seed: u8) -> Tx {
    Tx::new()
        .input(&tx::hash(&[b'u', seed]), 0)
        .output(Output::new(scenario.stranger(seed), 1_000_000))
        .fee(180_000 + u64::from(seed))
}

/// Transaction metadata nested 1,000 levels deep, a repeated key in a
/// metadatum map, and bridge metadata of every depth at the ICS address.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated PostgreSQL 17"]
async fn deep_and_repeated_key_metadata_never_stops_the_follower() -> Result {
    let scenario = Scenario::new(31_000).await?;
    let ordinary = scenario.ordinary(0);
    // Label 674 holding 1,000 nested lists, and an Alonzo-form auxiliary map
    // whose label-674 metadatum is an indefinite map with a repeated key.
    let mut deep = vec![0xa1, 0x19, 0x02, 0xa2];
    deep.extend(nested_lists(1000).0);
    let repeated =
        hex::decode("d90103a100a11902a2bf616b01616b02ff").expect("tagged auxiliary data");
    let u1 = unrelated(&scenario, 1).auxiliary_data(deep.clone()).build();
    let u2 = unrelated(&scenario, 2).auxiliary_data(repeated).build();
    // Payments to the ICS address whose bridge metadatum is a recipient
    // beside the deep label, nested 1,000 levels, and nested past the limit.
    let ics = scenario.address(&scenario.start.profile.addresses.ics);
    let recipient = vec![0xca, 0xfe, 0x01];
    let mut both = vec![0xa2, 0x19, 0x02, 0xa2];
    both.extend(nested_lists(1000).0);
    both.extend([0x1a, 0x00, 0x63, 0x32, 0x6d]);
    both.extend(tx::recipient_metadatum(&recipient));
    let b1 = Tx::new()
        .input(&tx::hash(b"bridge one"), 0)
        .output(Output::new(ics.clone(), 3_000_000))
        .auxiliary_data(both)
        .build();
    let b2 = Tx::new()
        .input(&tx::hash(b"bridge two"), 0)
        .output(Output::new(ics.clone(), 3_000_000))
        .metadata(6_500_973, nested_lists(1000).0)
        .build();
    let b3 = Tx::new()
        .input(&tx::hash(b"bridge three"), 0)
        .output(Output::new(ics.clone(), 3_000_000))
        .metadata(6_500_973, nested_lists(2001).0)
        .build();
    // The provider renders every label of a transaction; one stored past
    // 2,000 levels is null.
    scenario.answer(
        &format!("/txs/{}/metadata", b1.hash),
        format!(
            "[{{\"label\":\"674\",\"json_metadata\":{}}},{{\"label\":\"6500973\",\"json_metadata\":[\"0xcafe01\"]}}]",
            nested_json(1000)
        ),
    );
    scenario.answer(
        &format!("/txs/{}/metadata", b2.hash),
        format!(
            "[{{\"label\":\"6500973\",\"json_metadata\":{}}}]",
            nested_json(1000)
        ),
    );
    scenario.answer(
        &format!("/txs/{}/metadata", b3.hash),
        "[{\"label\":\"6500973\",\"json_metadata\":null}]".to_owned(),
    );
    let block = scenario.serve(vec![
        ordinary.built.clone(),
        u1.clone(),
        u2.clone(),
        b1.clone(),
        b2.clone(),
        b3.clone(),
    ]);
    scenario.follow(&block, &ordinary).await?;
    let config = scenario.config();
    for unrelated in [&u1, &u2] {
        assert_eq!(served(config, &unrelated.hash).await?, Value::Null);
    }
    let ics_text = &scenario.start.profile.addresses.ics;
    for (bridge, json) in [
        (&b1, Some("[\"0xcafe01\"]".to_owned())),
        (&b2, Some(nested_json(1000))),
        (&b3, None),
    ] {
        let mut row = served(config, &bridge.hash).await?;
        row["block"] = Value::Null;
        assert_eq!(
            row,
            json!({"block": null, "index": row["index"], "valid": true, "inputs": [],
                "outputs": [output(0, ics_text, 3_000_000, None, json!([]))],
                "metadata": ["6500973"]})
        );
        assert_eq!(
            metadata_matches(config, &bridge.hash, json.as_deref()).await?,
            Some(true),
            "bridge metadata of {}",
            bridge.hash
        );
    }
    scenario.start.cleanup().await
}

/// Witness sets with a supplemental datum nested 1,000 levels deep, a datum
/// that is an indefinite map with a repeated key, and a redeemer of 1,000
/// nested constructors; relevant outputs whose datums come from such sets.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated PostgreSQL 17"]
async fn deep_and_repeated_key_witness_data_never_stops_the_follower() -> Result {
    let scenario = Scenario::new(32_000).await?;
    let ordinary = scenario.ordinary(0);
    let (deep, _) = nested_lists(1000);
    let repeated = hex::decode("bf01000100ff").unwrap();
    let mut deep_set = vec![0xa1, 0x04, 0x81];
    deep_set.extend(&deep);
    let mut repeated_set = vec![0xa1, 0x04, 0xd9, 0x01, 0x02, 0x81];
    repeated_set.extend(&repeated);
    let mut redeemer = vec![0xa1, 0x05, 0xa1, 0x82, 0x00, 0x00, 0x82];
    redeemer.extend([0xd8, 0x79, 0x81].repeat(1000));
    redeemer.extend([0xd8, 0x79, 0x80, 0x82, 0x01, 0x01]);
    let u1 = unrelated(&scenario, 1)
        .witness_set(deep_set.clone())
        .build();
    let u2 = unrelated(&scenario, 2)
        .witness_set(repeated_set.clone())
        .build();
    let u3 = unrelated(&scenario, 3).witness_set(redeemer).build();
    // The same witness sets supply the datums of outputs the node reads.
    let reserve = scenario.address(&scenario.start.profile.addresses.reserve);
    let mapping = scenario.address(&scenario.start.profile.addresses.mapping);
    let (deep_hash, repeated_hash) = (tx::hash(&deep), tx::hash(&repeated));
    let r1 = Tx::new()
        .input(&tx::hash(b"witness one"), 0)
        .output(Output::new(reserve, 2_000_000).datum_hash(&deep_hash))
        .witness_set(deep_set)
        .build();
    let r2 = Tx::new()
        .input(&tx::hash(b"witness two"), 0)
        .output(Output::new(mapping, 2_000_000).datum_hash(&repeated_hash))
        .witness_set(repeated_set)
        .build();
    let block = scenario.serve(vec![
        ordinary.built.clone(),
        u1.clone(),
        u2.clone(),
        u3.clone(),
        r1.clone(),
        r2.clone(),
    ]);
    scenario.follow(&block, &ordinary).await?;
    let config = scenario.config();
    for unrelated in [&u1, &u2, &u3] {
        assert_eq!(served(config, &unrelated.hash).await?, Value::Null);
    }
    for (relevant, hash) in [(&r1, &deep_hash), (&r2, &repeated_hash)] {
        let row = served(config, &relevant.hash).await?;
        assert_eq!(row["outputs"][0]["data_hash"], json!(hash));
    }
    // Past 2,000 JSON levels the value is NULL; both pairs of the map stay.
    assert_eq!(datum_matches(config, &deep_hash, None).await?, Some(true));
    assert_eq!(
        datum_matches(
            config,
            &repeated_hash,
            Some(r#"{"map":[{"k":{"int":1},"v":{"int":0}},{"k":{"int":1},"v":{"int":0}}]}"#)
        )
        .await?,
        Some(true)
    );
    assert_eq!(
        scenario.fake.requests("/scripts/datum/"),
        0,
        "witness datums need no request"
    );
    scenario.start.cleanup().await
}

/// Inline datums nested past and just below db-sync's limit, a repeated map
/// key, and datum hashes whose preimage is not on chain, at the ICS, reserve,
/// mapping and governance addresses.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated PostgreSQL 17"]
async fn deep_and_unresolvable_datums_at_profile_addresses_never_stop_the_follower() -> Result {
    let scenario = Scenario::new(33_000).await?;
    let ordinary = scenario.ordinary(0);
    let addresses = &scenario.start.profile.addresses;
    let (ics, reserve, mapping, council) = (
        addresses.ics.clone(),
        addresses.reserve.clone(),
        addresses.mapping.clone(),
        addresses.council.clone(),
    );
    let repeated = (
        hex::decode("a201000102").unwrap(),
        Some(r#"{"map":[{"k":{"int":1},"v":{"int":0}},{"k":{"int":1},"v":{"int":2}}]}"#.to_owned()),
    );
    let governance = (
        hex::decode("d87a814102").unwrap(),
        Some(r#"{"constructor":1,"fields":[{"bytes":"02"}]}"#.to_owned()),
    );
    // (address, datum, db-sync value, inline, preimage on chain)
    let payments = vec![
        (ics.clone(), nested_lists(1000), true, true),
        (ics.clone(), nested_lists(999), true, true),
        (ics.clone(), (cbor_bytes(b"ics"), None), false, false),
        (reserve.clone(), nested_constructors(1000), true, true),
        (reserve.clone(), repeated, true, true),
        (reserve.clone(), (cbor_bytes(b"res"), None), false, false),
        (
            mapping.clone(),
            (
                hex::decode(REGISTRATION).unwrap(),
                Some(REGISTRATION_VALUE.to_owned()),
            ),
            true,
            true,
        ),
        (council.clone(), governance, false, true),
    ];
    let mut transactions = vec![ordinary.built.clone()];
    let mut expected = Vec::new();
    for (n, (address, (raw, value), inline, on_chain)) in payments.into_iter().enumerate() {
        let hash = tx::hash(&raw);
        let output = Output::new(scenario.address(&address), 2_000_000 + n as u64);
        let output = if inline {
            output.inline_datum(raw.clone())
        } else {
            output.datum_hash(&hash)
        };
        let built = Tx::new()
            .input(&tx::hash(&[b'p', n as u8]), 0)
            .output(output)
            .build();
        if !inline && on_chain {
            scenario
                .fake
                .chain
                .lock()
                .unwrap()
                .datums
                .insert(hash.clone(), raw.clone());
        }
        transactions.push(built.clone());
        expected.push((built, address, n, hash, value, on_chain));
    }
    let block = scenario.serve(transactions);
    scenario.follow(&block, &ordinary).await?;
    let config = scenario.config();
    for (built, address, n, hash, value, on_chain) in &expected {
        let row = served(config, &built.hash).await?;
        assert_eq!(
            row["outputs"],
            json!([output(
                0,
                address,
                2_000_000 + *n as u64,
                Some(hash),
                json!([])
            )]),
            "payment {n}"
        );
        let stored = datum_matches(config, hash, value.as_deref()).await?;
        if *on_chain {
            assert_eq!(stored, Some(true), "datum of payment {n}");
        } else {
            assert_eq!(stored, None, "no datum row without a preimage: payment {n}");
            assert_eq!(
                scenario
                    .fake
                    .requests(&format!("/scripts/datum/{hash}/cbor")),
                1,
                "a missing preimage is asked for once"
            );
        }
    }
    scenario.start.cleanup().await
}

/// Payments to the registered-candidates address, with a datum and with
/// cNIGHT: the address is not a profile address.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated PostgreSQL 17"]
async fn payments_to_the_registered_candidates_address_never_stop_the_follower() -> Result {
    let scenario = Scenario::new(34_000).await?;
    let ordinary = scenario.ordinary(0);
    let release = match scenario.start.profile.network.as_str() {
        "mainnet" => "mainnet-node-1.0.0",
        _ => "preprod-node-1.0.400",
    };
    let file: Value = serde_json::from_slice(&std::fs::read(format!(
        "{}/../../profiles/{release}/registered-candidates-addresses.json",
        env!("CARGO_MANIFEST_DIR")
    ))?)?;
    let candidates = file["committee_candidates_address"]
        .as_str()
        .unwrap()
        .to_owned();
    let at_candidates = scenario.address(&candidates);
    let datum = hex::decode(REGISTRATION).unwrap();
    let plain = Tx::new()
        .input(&tx::hash(b"candidate"), 0)
        .output(Output::new(at_candidates.clone(), 1_200_000).inline_datum(datum.clone()))
        .build();
    let (point, held) = &scenario.night[1];
    let mut with_night = Output::new(at_candidates, 1_300_000).inline_datum(datum.clone());
    for asset in &held.assets {
        with_night = with_night.token(&asset.policy, &asset.name, asset.quantity);
    }
    let holding = Tx::new()
        .input(&point.hash, point.index)
        .output(with_night)
        .build();
    let block = scenario.serve(vec![ordinary.built.clone(), plain.clone(), holding.clone()]);
    scenario.follow(&block, &ordinary).await?;
    let config = scenario.config();
    assert_eq!(served(config, &plain.hash).await?, Value::Null);
    let row = served(config, &holding.hash).await?;
    assert_eq!(row["inputs"], json!([[held.transaction_id, point.index]]));
    assert_eq!(
        row["outputs"],
        json!([output(
            0,
            &candidates,
            1_300_000,
            Some(&tx::hash(&datum)),
            assets_of(held)
        )])
    );
    scenario.start.cleanup().await
}

/// Datums with integers beyond 2^53, bignums, a tag-102 constructor index
/// beyond 2^53, a 2,000-byte chunked bignum and a repeated map key, at the
/// mapping address (stored exactly) and at holders of cNIGHT (not read).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated PostgreSQL 17"]
async fn exact_integers_and_repeated_keys_in_datums_never_stop_the_follower() -> Result {
    let scenario = Scenario::new(35_000).await?;
    let ordinary = scenario.ordinary(0);
    let mapping_text = scenario.start.profile.addresses.mapping.clone();
    let mapping = scenario.address(&mapping_text);
    let mut transactions = vec![ordinary.built.clone()];
    let mut at_mapping = Vec::new();
    for (n, (raw, value)) in integers(1).into_iter().enumerate() {
        let built = Tx::new()
            .input(&tx::hash(&[b'i', n as u8]), 0)
            .output(Output::new(mapping.clone(), 2_000_000).inline_datum(raw.clone()))
            .build();
        transactions.push(built.clone());
        at_mapping.push((built, tx::hash(&raw), value));
    }
    // A hashed datum at the mapping address: the provider's CBOR answer is
    // exact, whatever its JSON rendering rounds.
    let (word, word_value) = integers(2).remove(0);
    let word_hash = tx::hash(&word);
    scenario
        .fake
        .chain
        .lock()
        .unwrap()
        .datums
        .insert(word_hash.clone(), word.clone());
    let hashed = Tx::new()
        .input(&tx::hash(b"hashed integer"), 0)
        .output(Output::new(mapping.clone(), 2_000_000).datum_hash(&word_hash))
        .build();
    transactions.push(hashed.clone());
    at_mapping.push((hashed, word_hash, word_value));
    // One holder output per datum, each with one cNIGHT unit of a held
    // output, the rest of it back to its holder.
    let (point, held) = &scenario.night[1];
    let night = held
        .assets
        .iter()
        .find(|asset| asset.unit() == scenario.start.profile.assets.cnight.unit())
        .unwrap()
        .clone();
    let datums = integers(3);
    assert!(night.quantity > datums.len() as i128, "enough cNIGHT units");
    let mut split = Tx::new().input(&point.hash, point.index);
    let mut rest = Output::new(scenario.stranger(0x51), 2_000_000);
    for asset in &held.assets {
        let quantity = if asset.unit() == night.unit() {
            asset.quantity - datums.len() as i128
        } else {
            asset.quantity
        };
        rest = rest.token(&asset.policy, &asset.name, quantity);
    }
    split = split.output(rest);
    let mut holders = Vec::new();
    for (n, (raw, _)) in datums.iter().enumerate() {
        let holder = scenario.stranger(0x60 + n as u8);
        split = split.output(
            Output::new(holder.clone(), 1_500_000)
                .token(&night.policy, &night.name, 1)
                .inline_datum(raw.clone()),
        );
        holders.push((address_text(&holder).unwrap(), tx::hash(raw)));
    }
    let split = split.build();
    transactions.push(split.clone());
    let block = scenario.serve(transactions);
    scenario.follow(&block, &ordinary).await?;
    let config = scenario.config();
    for (built, hash, value) in &at_mapping {
        let row = served(config, &built.hash).await?;
        assert_eq!(row["outputs"][0]["data_hash"], json!(hash));
        assert_eq!(
            datum_matches(config, hash, Some(value)).await?,
            Some(true),
            "exact value of {hash}"
        );
    }
    let row = served(config, &split.hash).await?;
    let outputs = row["outputs"].as_array().unwrap();
    assert_eq!(outputs.len(), 1 + holders.len());
    for (n, (address, hash)) in holders.iter().enumerate() {
        assert_eq!(outputs[n + 1]["address"], json!(address));
        assert_eq!(outputs[n + 1]["data_hash"], json!(hash));
        assert_eq!(
            outputs[n + 1]["assets"],
            json!([[night.policy, night.name, "1"]])
        );
        assert_eq!(
            datum_value(config, hash).await?,
            None,
            "the node reads no datum of a holder output"
        );
    }
    scenario.start.cleanup().await
}

/// The policy of the self-minted tokens that fill reference-input outputs.
const JUNK: [u8; 28] = [0x4a; 28];

/// 1,304 one-policy assets fill one output value of at most 5,000 bytes.
fn junk_units() -> Vec<String> {
    let mut names: Vec<Vec<u8>> = vec![Vec::new()];
    names.extend((0..=255u8).map(|byte| vec![byte]));
    for first in 0..=255u8 {
        for second in 0..=255u8 {
            names.push(vec![first, second]);
        }
    }
    names.truncate(1304);
    names
        .into_iter()
        .map(|name| format!("{}{}", hex::encode(JUNK), hex::encode(name)))
        .collect()
}

/// A transaction with 310 reference inputs to outputs that hold 1,304
/// tokens each, beside a relevant transaction, in a block of exactly 100
/// transactions. One of them has an output the decoder cannot classify, so
/// its outputs answer is read: above 32 MiB because the provider renders
/// every reference input with every token it holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated PostgreSQL 17"]
async fn an_oversized_outputs_answer_never_stops_the_follower() -> Result {
    let scenario = Scenario::new(36_000).await?;
    let ordinary = scenario.ordinary(0);
    let references = (0..310u32)
        .map(|n| tx::hash(&[b"ref".as_slice(), &n.to_be_bytes()].concat()))
        .collect::<Vec<_>>();
    let big = |seed: &[u8], unknown: bool| {
        let mut built = Tx::new()
            .input(&tx::hash(seed), 0)
            .output(Output::new(scenario.stranger(0x70), 2_000_000));
        if unknown {
            // Header type 9 is no address form the decoder knows.
            let mut address = vec![0x90_u8];
            address.extend([5_u8; 28]);
            built = built.output(Output::new(address, 4_000_000));
        }
        for reference in &references {
            built = built.reference(reference, 0);
        }
        built.fee(777_777).build()
    };
    let read = big(b"spent by the decoded one", false);
    let resolved = big(b"spent by the resolved one", true);
    let stranger = address_text(&scenario.stranger(0x70)).unwrap();
    let elsewhere = address_text(&scenario.stranger(0x71)).unwrap();
    let units = junk_units();
    let mut fat = String::from("[{\"unit\":\"lovelace\",\"quantity\":\"23000000\"}");
    for unit in &units {
        fat.push_str(&format!(",{{\"unit\":\"{unit}\",\"quantity\":\"1\"}}"));
    }
    fat.push(']');
    let row = |hash: &str, amount: &str, reference: bool| {
        format!(
            "{{\"address\":\"{elsewhere}\",\"amount\":{amount},\"tx_hash\":\"{hash}\",\"output_index\":0,\"data_hash\":null,\"inline_datum\":null,\"reference_script_hash\":null,\"collateral\":false,\"reference\":{reference}}}"
        )
    };
    let mut inputs = vec![row(
        &tx::hash(b"spent by the resolved one"),
        "[{\"unit\":\"lovelace\",\"quantity\":\"23000000\"}]",
        false,
    )];
    inputs.extend(references.iter().map(|hash| row(hash, &fat, true)));
    let answer = format!(
        "{{\"hash\":\"{}\",\"inputs\":[{}],\"outputs\":[{{\"address\":\"{stranger}\",\"amount\":[{{\"unit\":\"lovelace\",\"quantity\":\"2000000\"}}],\"output_index\":0,\"data_hash\":null,\"inline_datum\":null,\"collateral\":false,\"reference_script_hash\":null}},{{\"address\":\"{elsewhere}\",\"amount\":[{{\"unit\":\"lovelace\",\"quantity\":\"4000000\"}}],\"output_index\":1,\"data_hash\":null,\"inline_datum\":null,\"collateral\":false,\"reference_script_hash\":null}}]}}",
        resolved.hash,
        inputs.join(",")
    );
    assert!(
        answer.len() > 32 * 1024 * 1024,
        "{} bytes of outputs answer",
        answer.len()
    );
    scenario.answer(&format!("/txs/{}/utxos", resolved.hash), answer);
    let mut transactions = vec![ordinary.built.clone(), read.clone(), resolved.clone()];
    transactions.extend((0..97u64).map(|n| scripted::filler(36_000_000 + n)));
    assert_eq!(transactions.len(), scripted::PAGE);
    let block = scenario.serve(transactions);
    scenario.follow(&block, &ordinary).await?;
    let config = scenario.config();
    assert_eq!(served(config, &read.hash).await?, Value::Null);
    assert_eq!(served(config, &resolved.hash).await?, Value::Null);
    assert_eq!(
        scenario.fake.requests(&format!("/txs/{}/utxos", read.hash)),
        0,
        "a decoded transaction needs no outputs answer"
    );
    assert_eq!(
        scenario
            .fake
            .requests(&format!("/txs/{}/utxos", resolved.hash)),
        1,
        "the oversized answer is read once"
    );
    scenario.start.cleanup().await
}

/// Recorded phase-2-invalid transactions whose collateral returns hold
/// tokens; one that spends a held cNIGHT output as collateral and returns it;
/// one whose collateral return the decoder cannot classify, resolved through
/// the outputs answer the provider renders for an invalid transaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated PostgreSQL 17"]
async fn phase_two_invalid_transactions_never_stop_the_follower() -> Result {
    let scenario = Scenario::new(37_000).await?;
    let ordinary = scenario.ordinary(0);
    let recorded = recorded()
        .into_iter()
        .filter(|(shape, _)| shape.starts_with("phase-2-invalid"))
        .map(|(_, built)| built)
        .collect::<Vec<_>>();
    assert_eq!(recorded.len(), 3);
    // The collateral is a held cNIGHT output; the body's own output would
    // move it too, but an invalid transaction only takes its collateral.
    let (point, held) = &scenario.night[1];
    let holder = scenario.stranger(0x31);
    let mut returned = Output::new(holder.clone(), 4_000_000);
    let mut ignored = Output::new(scenario.stranger(0x32), 5_000_000);
    for asset in &held.assets {
        returned = returned.token(&asset.policy, &asset.name, asset.quantity);
        ignored = ignored.token(&asset.policy, &asset.name, asset.quantity);
    }
    let relevant = Tx::new()
        .input(&tx::hash(b"ordinary input of the invalid one"), 0)
        .collateral(&point.hash, point.index)
        .output(ignored)
        .collateral_return(returned)
        .invalid()
        .build();
    // An unclassifiable collateral return: the provider lists the collateral
    // inputs as ordinary rows and the return as an ordinary output at the
    // body's output count.
    let mut unknown = vec![0x90_u8];
    unknown.extend([6_u8; 28]);
    let collateral = tx::hash(b"collateral of the resolved one");
    let resolved = Tx::new()
        .input(&tx::hash(b"input of the resolved one"), 0)
        .collateral(&collateral, 0)
        .output(Output::new(scenario.stranger(0x33), 1_000_000))
        .output(Output::new(scenario.stranger(0x34), 1_000_000))
        .collateral_return(Output::new(unknown, 3_000_000))
        .invalid()
        .build();
    let elsewhere = address_text(&scenario.stranger(0x35)).unwrap();
    scenario.answer(
        &format!("/txs/{}/utxos", resolved.hash),
        json!({"hash": resolved.hash,
            "inputs": [{"address": elsewhere, "amount": [{"unit": "lovelace", "quantity": "5000000"}],
                "tx_hash": collateral, "output_index": 0, "data_hash": null, "inline_datum": null,
                "reference_script_hash": null, "collateral": false, "reference": false}],
            "outputs": [{"address": elsewhere, "amount": [{"unit": "lovelace", "quantity": "3000000"}],
                "output_index": 2, "data_hash": null, "inline_datum": null, "collateral": false,
                "reference_script_hash": null}]})
        .to_string(),
    );
    let mut transactions = vec![ordinary.built.clone()];
    transactions.extend(recorded.iter().cloned());
    transactions.extend([relevant.clone(), resolved.clone()]);
    let block = scenario.serve(transactions);
    scenario.follow(&block, &ordinary).await?;
    let config = scenario.config();
    for built in &recorded {
        assert_eq!(
            served(config, &built.hash).await?,
            Value::Null,
            "recorded {}",
            built.hash
        );
    }
    let mut row = served(config, &relevant.hash).await?;
    row["block"] = Value::Null;
    assert_eq!(
        row,
        json!({"block": null, "index": 4, "valid": false,
            "inputs": [[held.transaction_id, point.index]],
            "outputs": [output(1, &address_text(&holder).unwrap(), 4_000_000, None, assets_of(held))],
            "metadata": []})
    );
    assert_eq!(served(config, &resolved.hash).await?, Value::Null);
    assert_eq!(
        scenario
            .fake
            .requests(&format!("/txs/{}/utxos", resolved.hash)),
        1
    );
    scenario.start.cleanup().await
}

/// Recorded valid transactions whose collateral returns hold tokens; a valid
/// transaction with an output to a pointer address in a non-minimal encoding
/// carrying cNIGHT, an output the decoder cannot classify, and a collateral
/// return holding a token, whose outputs answer renders the pointer address
/// canonically and the collateral return as lovelace only.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated PostgreSQL 17"]
async fn provider_renderings_of_collateral_returns_and_pointers_never_stop_the_follower() -> Result
{
    let scenario = Scenario::new(38_000).await?;
    let ordinary = scenario.ordinary(0);
    let recorded = recorded()
        .into_iter()
        .filter(|(shape, _)| shape.starts_with("valid"))
        .map(|(_, built)| built)
        .collect::<Vec<_>>();
    assert_eq!(recorded.len(), 2);
    // Slot 133 as 80 81 05 and transaction index 2 as 80 02: the ledger
    // accepts redundant leading groups and serves the minimal form.
    let key = [0x77_u8; 28];
    let mut pointer = vec![0x40 | scenario.network];
    pointer.extend(key);
    pointer.extend([0x80, 0x81, 0x05, 0x80, 0x02, 0x00]);
    let canonical = address_text(&tx::pointer_address(scenario.network, key, (133, 2, 0))).unwrap();
    assert_ne!(
        tx::pointer_address(scenario.network, key, (133, 2, 0)),
        pointer
    );
    let (point, held) = &scenario.night[1];
    let mut at_pointer = Output::new(pointer, 2_500_000);
    for asset in &held.assets {
        at_pointer = at_pointer.token(&asset.policy, &asset.name, asset.quantity);
    }
    let mut unknown = vec![0x90_u8];
    unknown.extend([7_u8; 28]);
    let collateral = tx::hash(b"token collateral");
    let returned =
        Output::new(scenario.stranger(0x41), 4_000_000).token(&hex::encode(JUNK), "01", 5);
    let valid = Tx::new()
        .input(&point.hash, point.index)
        .collateral(&collateral, 0)
        .output(at_pointer)
        .output(Output::new(unknown, 1_000_000))
        .collateral_return(returned)
        .build();
    let elsewhere = address_text(&scenario.stranger(0x42)).unwrap();
    let returned_text = address_text(&scenario.stranger(0x41)).unwrap();
    let mut amount = vec![json!({"unit": "lovelace", "quantity": "2500000"})];
    for asset in &held.assets {
        amount.push(json!({"unit": asset.unit(), "quantity": asset.quantity.to_string()}));
    }
    scenario.answer(
        &format!("/txs/{}/utxos", valid.hash),
        json!({"hash": valid.hash,
            "inputs": [
                {"address": elsewhere, "amount": [{"unit": "lovelace", "quantity": "9000000"}],
                 "tx_hash": point.hash, "output_index": point.index, "data_hash": null,
                 "inline_datum": null, "reference_script_hash": null, "collateral": false,
                 "reference": false},
                {"address": returned_text, "amount": [{"unit": "lovelace", "quantity": "4500000"}],
                 "tx_hash": collateral, "output_index": 0, "data_hash": null,
                 "inline_datum": null, "reference_script_hash": null, "collateral": true,
                 "reference": false}],
            "outputs": [
                {"address": canonical, "amount": amount, "output_index": 0, "data_hash": null,
                 "inline_datum": null, "collateral": false, "reference_script_hash": null},
                {"address": elsewhere, "amount": [{"unit": "lovelace", "quantity": "1000000"}],
                 "output_index": 1, "data_hash": null, "inline_datum": null, "collateral": false,
                 "reference_script_hash": null},
                {"address": returned_text, "amount": [{"unit": "lovelace", "quantity": "4000000"}],
                 "output_index": 2, "data_hash": null, "inline_datum": null, "collateral": true,
                 "reference_script_hash": null}]})
        .to_string(),
    );
    let mut transactions = vec![ordinary.built.clone()];
    transactions.extend(recorded.iter().cloned());
    transactions.push(valid.clone());
    let block = scenario.serve(transactions);
    scenario.follow(&block, &ordinary).await?;
    let config = scenario.config();
    for built in &recorded {
        assert_eq!(
            served(config, &built.hash).await?,
            Value::Null,
            "recorded {}",
            built.hash
        );
    }
    let row = served(config, &valid.hash).await?;
    assert_eq!(row["inputs"], json!([[held.transaction_id, point.index]]));
    assert_eq!(
        row["outputs"],
        json!([output(0, &canonical, 2_500_000, None, assets_of(held))]),
        "the pointer output in its canonical form, nothing else"
    );
    assert_eq!(
        scenario
            .fake
            .requests(&format!("/txs/{}/utxos", valid.hash)),
        1
    );
    scenario.start.cleanup().await
}
