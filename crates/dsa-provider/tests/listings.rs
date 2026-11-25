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
    // A valid transaction with collateral: the collateral input repeats an
    // input, flagged and lovelace only; the collateral return is flagged,
    // lovelace only and names no spending transaction.
    let flagged = provider
        .transaction_utxos(&hash(
            "35c1ecdf3d0c1e55a70b5dea93b99aa1c93e64f52131790f0ed8f44a1abbb7a9",
        ))
        .await
        .unwrap()
        .unwrap();
    let collateral: Vec<_> = flagged.inputs.iter().filter(|row| row.collateral).collect();
    assert_eq!(collateral.len(), 1);
    assert_eq!(collateral[0].amount.len(), 1);
    assert!(flagged.inputs.iter().any(|row| !row.collateral
        && (&row.tx_hash, row.output_index)
            == (&collateral[0].tx_hash, collateral[0].output_index)));
    let unspent_return = flagged
        .outputs
        .iter()
        .find(|row| row.collateral)
        .expect("a collateral return");
    assert_eq!(
        (unspent_return.amount.len(), &unspent_return.consumed_by_tx),
        (1, &None)
    );
    // The unit listing in a height range (Mainnet).
    let mainnet_cnight: Unit = "0691b2fecca1ac4f53cb6dfb00b7013e561d1f34403b957cbb5af1fa4e49474854"
        .parse()
        .unwrap();
    let ranged = provider
        .asset_outputs(
            &mainnet_cnight,
            Heights {
                from: Some(13_211_938),
                to: Some(14_211_937),
            },
            Page::first(),
        )
        .await
        .unwrap();
    assert_eq!(
        ranged
            .iter()
            .map(|row| row.block_height)
            .collect::<Vec<_>>(),
        [Some(13_211_957), Some(13_211_961), Some(13_212_002)]
    );
    // An address's list from a height on.
    let mapping: AddressText = "addr_test1wplxjzranravtp574s2wz00md7vz9rzpucu252je68u9a8qzjheng"
        .parse()
        .unwrap();
    let listed = provider
        .address_transactions(
            &mapping,
            Heights {
                from: Some(5_414_627),
                to: None,
            },
            Page::first(),
            false,
        )
        .await
        .unwrap();
    assert_eq!(listed.len(), 4);
    assert_eq!(listed[0].block_height, 5_416_735);
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

/// Answers the backend's queries never give are refused by the calls a build
/// makes, before any row is used: an amount whose unit is too short, an
/// output index a positional ID cannot hold, a row given twice, and a page
/// that is not in the order asked.
#[tokio::test]
async fn the_build_calls_refuse_answers_out_of_shape() {
    let transaction = "24db5990d78fc8c273854fcf4e0e290a419529680d6e6a837104c91f3a3c2f3a";
    let other = "f74215250265a6d5be00d8e3bf1ecb9dc40e5a85f6c9309c8160be77fea7950f";
    let address = "addr_test1wraevj382xaeeww85mg5whg75j3evzpq5re33zpkgagzgfsgag5wv";
    let cnight = "d2dbff622e509dda256fedbd31ef6e9fd98ed49ad91d5c0e07f68af1";
    let lovelace = serde_json::json!([{"unit": "lovelace", "quantity": "1000000"}]);
    let short = serde_json::json!([{"unit": "lovelace", "quantity": "1000000"},
        {"unit": "abcd", "quantity": "1"}]);
    let output = |index: u32, amount: &Value| {
        serde_json::json!({"address": address, "amount": amount, "output_index": index,
            "data_hash": null, "inline_datum": null, "collateral": false,
            "reference_script_hash": null, "consumed_by_tx": null})
    };
    let input = |amount: &Value| {
        serde_json::json!({"address": address, "amount": amount, "tx_hash": other,
            "output_index": 0, "data_hash": null, "inline_datum": null,
            "reference_script_hash": null, "collateral": false, "reference": false})
    };
    let utxos = |inputs: Vec<Value>, outputs: Vec<Value>| {
        serde_json::json!({"hash": transaction, "inputs": inputs, "outputs": outputs}).to_string()
    };
    let listed = |height: u32, index: u32, amount: &Value| {
        serde_json::json!({"address": address, "tx_hash": transaction, "output_index": index,
            "amount": amount, "block": other, "block_height": height, "block_time": 1_700_000_000,
            "data_hash": null, "inline_datum": null, "inline_datum_json": null,
            "reference_script_hash": null})
    };
    let row = |height: u32, index: u32, hash: &str| {
        serde_json::json!({"tx_hash": hash, "tx_index": index, "block_height": height,
            "block_time": 1_700_000_000})
    };
    let hash: Hash32 = transaction.parse().unwrap();
    let unit: Unit = cnight.parse().unwrap();
    let at: AddressText = address.parse().unwrap();
    let range = Heights {
        from: Some(1),
        to: Some(100),
    };
    let mut refused = Vec::new();
    // `/txs/{hash}/utxos`
    for (name, body) in [
        (
            "output unit too short",
            utxos(vec![], vec![output(0, &short)]),
        ),
        (
            "input unit too short",
            utxos(vec![input(&short)], vec![output(0, &lovelace)]),
        ),
        (
            "output index above the positional limit",
            utxos(vec![], vec![output(5_000, &lovelace)]),
        ),
        (
            "output given twice",
            utxos(vec![], vec![output(0, &lovelace), output(0, &lovelace)]),
        ),
        (
            "input given twice",
            utxos(vec![input(&lovelace), input(&lovelace)], vec![]),
        ),
    ] {
        let (_server, _key, provider) = provider_of(move |_| Behavior::raw(body.clone())).await;
        refused.push((name, provider.transaction_utxos(&hash).await.err()));
    }
    // `/assets/{unit}/utxos` and `/addresses/{address}/utxos`
    for (name, body) in [
        (
            "unit listing not in height order",
            serde_json::json!([listed(20, 0, &lovelace), listed(10, 1, &lovelace)]),
        ),
        (
            "unit listing with a row given twice",
            serde_json::json!([listed(10, 0, &lovelace), listed(10, 0, &lovelace)]),
        ),
        (
            "unit listing with a unit too short",
            serde_json::json!([listed(10, 0, &short)]),
        ),
    ] {
        let body = body.to_string();
        let (_server, _key, provider) = provider_of(move |_| Behavior::raw(body.clone())).await;
        refused.push((
            name,
            provider
                .asset_outputs(&unit, range, Page::first())
                .await
                .err(),
        ));
    }
    let twice = serde_json::json!([listed(10, 0, &lovelace), listed(10, 0, &lovelace)]).to_string();
    let (_server, _key, provider) = provider_of(move |_| Behavior::raw(twice.clone())).await;
    refused.push((
        "address listing with a row given twice",
        provider.address_outputs(&at, Page::first()).await.err(),
    ));
    // transaction lists, ascending and descending
    let ascending = serde_json::json!([row(10, 0, transaction), row(11, 2, other)]).to_string();
    let descending = serde_json::json!([row(11, 2, other), row(10, 0, transaction)]).to_string();
    for (name, body, descend) in [
        (
            "ascending list in descending order",
            descending.clone(),
            false,
        ),
        (
            "descending list in ascending order",
            ascending.clone(),
            true,
        ),
        (
            "list with a place given twice",
            serde_json::json!([row(10, 0, transaction), row(10, 0, other)]).to_string(),
            false,
        ),
    ] {
        let (_server, _key, provider) = provider_of(move |_| Behavior::raw(body.clone())).await;
        refused.push((
            name,
            provider
                .asset_transactions(&unit, range, Page::first(), descend)
                .await
                .err(),
        ));
        let (_server, _key, provider) = provider_of({
            let body = if descend {
                ascending.clone()
            } else {
                descending.clone()
            };
            move |_| Behavior::raw(body.clone())
        })
        .await;
        if name != "list with a place given twice" {
            refused.push((
                name,
                provider
                    .address_transactions(&at, range, Page::first(), descend)
                    .await
                    .err(),
            ));
        }
    }
    let accepted: Vec<_> = refused
        .iter()
        .filter(|(_, error)| {
            error
                .as_ref()
                .is_none_or(|e| e.code != "provider_invalid_response")
        })
        .map(|(name, error)| format!("{name}: {error:?}"))
        .collect();
    assert!(accepted.is_empty(), "not refused: {accepted:#?}");
    // The answers in shape are read.
    let (_server, _key, provider) = provider_of(move |_| Behavior::raw(ascending.clone())).await;
    assert_eq!(
        provider
            .asset_transactions(&unit, range, Page::first(), false)
            .await
            .unwrap()
            .len(),
        2
    );
}
