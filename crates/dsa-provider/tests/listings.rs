//! The listing calls read the backend's recorded answers
//! (`dsa-testkit/tests/fixtures/listing-answers.json`, Preprod, rows cut)
//! from a loopback server: every member the build uses is parsed.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use dsa_provider::{
    Clock, Config, Provider,
    types::{AddressText, Hash32, Heights, Page, Unit},
};
use dsa_testkit::server::{Behavior, KEY, Server};
use serde_json::Value;

struct Fixed(AtomicU64);
impl Clock for Fixed {
    fn unix_seconds(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

#[tokio::test]
async fn the_listing_calls_read_the_backends_answers() {
    let recorded: Value = serde_json::from_str(include_str!(
        "../../dsa-testkit/tests/fixtures/listing-answers.json"
    ))
    .unwrap();
    let answers: BTreeMap<String, String> = recorded
        .as_object()
        .unwrap()
        .values()
        .filter_map(|entry| {
            Some((
                entry.get("path")?.as_str()?.to_owned(),
                entry.get("body")?.to_string(),
            ))
        })
        .collect();
    let server = Server::start(move |path, _| {
        let path = path.strip_prefix("/api/v0").unwrap_or(path);
        answers
            .get(path)
            .map_or_else(|| Behavior::status(404), |body| Behavior::raw(body.clone()))
    })
    .await;
    let key = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(key.path(), KEY).unwrap();
    let mut config = Config::new(server.url.clone(), key.path());
    config.requests_per_second = 10.0;
    let provider =
        Provider::with_clock(config, Arc::new(Fixed(AtomicU64::new(1_795_000_000)))).unwrap();
    let hash = |text: &str| text.parse::<Hash32>().unwrap();
    let cnight: Unit = "d2dbff622e509dda256fedbd31ef6e9fd98ed49ad91d5c0e07f68af1"
        .parse()
        .unwrap();
    let rows = provider
        .asset_outputs(&cnight, Heights::default(), Page::new(4).unwrap())
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].block_height, Some(5_183_219));
    assert_eq!(rows[0].amount.len(), 2);
    let reserve: AddressText = "addr_test1wraevj382xaeeww85mg5whg75j3evzpq5re33zpkgagzgfsgag5wv"
        .parse()
        .unwrap();
    let rows = provider
        .address_outputs(&reserve, Page::first())
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].inline_datum.is_some() && rows[0].block_height.is_none());
    let auth: Unit = "7e69087d98fac5869eac14e13dfb6f98228c41e638aa2a59d1f85e9c"
        .parse()
        .unwrap();
    let rows = provider
        .asset_transactions(
            &auth,
            Heights {
                from: Some(5_414_627),
                to: None,
            },
            Page::first(),
            false,
        )
        .await
        .unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| (row.block_height, row.tx_index))
            .collect::<Vec<_>>(),
        [(5_416_762, 0), (5_433_739, 0)]
    );
    let moved = provider
        .transaction_utxos(&hash(
            "24db5990d78fc8c273854fcf4e0e290a419529680d6e6a837104c91f3a3c2f3a",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(moved.inputs.len(), 2);
    assert!(
        moved
            .inputs
            .iter()
            .all(|input| !input.collateral && !input.reference)
    );
    assert!(!moved.outputs.is_empty());
    let info = provider
        .transaction(&hash(
            "f74215250265a6d5be00d8e3bf1ecb9dc40e5a85f6c9309c8160be77fea7950f",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (info.block_height, info.index, info.valid_contract),
        (5_283_062, 13, true)
    );
    let asset = provider.asset(&cnight).await.unwrap().unwrap();
    assert_eq!(
        (asset.quantity.as_str(), asset.mint_or_burn_count),
        ("109638605718619", 319)
    );
    let policy = provider
        .policy_assets(
            "bdace6f5897301896f7debbf5b386d6997e4558c12377be3ecb22ce8",
            Page::first(),
        )
        .await
        .unwrap();
    assert_eq!(policy.len(), 1);
    let last = provider.epoch_last_block(324).await.unwrap().unwrap();
    let header = provider.block_by_hash(&last).await.unwrap().unwrap();
    assert_eq!(
        (header.height, header.epoch, header.tx_count),
        (5_401_535, 324, 1)
    );
}

/// A provider over a loopback server answering `answer(path)`, the path
/// without `/api/v0`.
async fn provider_of(
    answer: impl Fn(&str) -> Behavior + Send + Sync + 'static,
) -> (Server, tempfile::NamedTempFile, Provider) {
    let server =
        Server::start(move |path, _| answer(path.strip_prefix("/api/v0").unwrap_or(path))).await;
    let key = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(key.path(), KEY).unwrap();
    let mut config = Config::new(server.url.clone(), key.path());
    config.requests_per_second = 10.0;
    let provider =
        Provider::with_clock(config, Arc::new(Fixed(AtomicU64::new(1_795_000_000)))).unwrap();
    (server, key, provider)
}

/// `/assets/{unit}/history` as the backend's query renders it (`tx_hash`,
/// `amount` as signed text, `action`): the call reads the signed amounts,
/// a unit the provider does not know has no history, and a row whose action
/// does not name the amount's sign is refused.
#[tokio::test]
async fn the_history_call_reads_mints_and_burns() {
    let cnight = "d2dbff622e509dda256fedbd31ef6e9fd98ed49ad91d5c0e07f68af1";
    let unit: Unit = cnight.parse().unwrap();
    let mint = "24db5990d78fc8c273854fcf4e0e290a419529680d6e6a837104c91f3a3c2f3a";
    let burn = "f74215250265a6d5be00d8e3bf1ecb9dc40e5a85f6c9309c8160be77fea7950f";
    let good = serde_json::json!([
        {"tx_hash": mint, "amount": "1000003000", "action": "minted"},
        {"tx_hash": burn, "amount": "-3000", "action": "burned"}
    ])
    .to_string();
    let wanted = format!("/assets/{cnight}/history?count=100&page=1&order=asc");
    let (_server, _key, provider) = provider_of(move |path| {
        if path == wanted {
            Behavior::raw(good.clone())
        } else {
            Behavior::status(404)
        }
    })
    .await;
    let rows = provider.asset_history(&unit, Page::first()).await.unwrap();
    assert_eq!(
        rows.iter().map(|row| row.quantity()).collect::<Vec<_>>(),
        [Some(1_000_003_000), Some(-3_000)]
    );
    assert_eq!(rows[1].tx_hash, burn.parse::<Hash32>().unwrap());
    let other: Unit = "7e69087d98fac5869eac14e13dfb6f98228c41e638aa2a59d1f85e9c"
        .parse()
        .unwrap();
    assert!(
        provider
            .asset_history(&other, Page::first())
            .await
            .unwrap()
            .is_empty()
    );
    let wrong =
        serde_json::json!([{"tx_hash": burn, "amount": "3000", "action": "burned"}]).to_string();
    let (_server, _key, provider) = provider_of(move |_| Behavior::raw(wrong.clone())).await;
    assert!(provider.asset_history(&unit, Page::first()).await.is_err());
}

/// The transaction that spent an output is read by the build's
/// `/txs/{hash}/utxos` only, where every output row must name it (null while
/// the output is unspent); the follower's reading of the same answer leaves
/// the member unread, whatever it holds.
#[tokio::test]
async fn only_a_build_reads_the_spending_transaction_of_an_output() {
    let transaction = "24db5990d78fc8c273854fcf4e0e290a419529680d6e6a837104c91f3a3c2f3a";
    let spender = "f74215250265a6d5be00d8e3bf1ecb9dc40e5a85f6c9309c8160be77fea7950f";
    let answer = |consumed: Option<Value>| {
        let mut row = serde_json::json!({
            "address": "addr_test1wraevj382xaeeww85mg5whg75j3evzpq5re33zpkgagzgfsgag5wv",
            "amount": [{"unit": "lovelace", "quantity": "1000000"}], "output_index": 0,
            "data_hash": null, "inline_datum": null, "collateral": false,
            "reference_script_hash": null});
        if let Some(consumed) = consumed {
            row["consumed_by_tx"] = consumed;
        }
        serde_json::json!({"hash": transaction, "inputs": [], "outputs": [row]}).to_string()
    };
    let hash: Hash32 = transaction.parse().unwrap();
    for (name, body) in [
        (
            "unreadable",
            answer(Some(Value::from("not a transaction hash"))),
        ),
        ("absent", answer(None)),
        ("spent", answer(Some(Value::from(spender)))),
        ("unspent", answer(Some(Value::Null))),
    ] {
        let (_server, _key, provider) = provider_of(move |_| Behavior::raw(body.clone())).await;
        let follower = provider.transaction_outputs(&hash).await;
        assert!(
            follower.is_ok(),
            "{name}: the follower refused {follower:?}"
        );
        let build = provider.transaction_utxos(&hash).await;
        match name {
            "spent" => assert_eq!(
                build.unwrap().unwrap().outputs[0].consumed_by_tx,
                Some(spender.parse().unwrap())
            ),
            "unspent" => assert_eq!(build.unwrap().unwrap().outputs[0].consumed_by_tx, None),
            _ => assert!(build.is_err(), "{name}: the build read {build:?}"),
        }
    }
}
