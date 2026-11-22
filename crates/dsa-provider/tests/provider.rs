mod common;

use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use common::*;
use dsa_provider::{
    Action, Config, Endpoint, Provider, Reply,
    types::{Hash32, Page, PoolId},
};
use futures_util::StreamExt;
use serde_json::json;

fn point() -> Hash32 {
    hash('1').parse().unwrap()
}
fn page() -> Page {
    Page::new(1).unwrap()
}
fn pool() -> PoolId {
    bech32::encode::<bech32::Bech32>(bech32::Hrp::parse("pool").unwrap(), &[7; 28])
        .unwrap()
        .parse()
        .unwrap()
}

#[tokio::test]
async fn paced_overlap_is_bounded_and_answers_keep_request_order() {
    let server = Server::start(|path, _| {
        let height = path.rsplit('/').next().unwrap().parse::<u32>().unwrap();
        Behavior::json(header(height)).delayed(if height == 1 { 450 } else { 250 })
    })
    .await;
    let (provider, _, recorder) = client(&server.url, |config| config.max_in_flight = 3);
    let results: Vec<_> = provider
        .ordered((1..=8).map(Endpoint::BlockAtHeight).collect())
        .collect()
        .await;
    let heights: Vec<_> = results
        .into_iter()
        .map(|result| match result.unwrap() {
            Reply::Header(header) => header.height,
            _ => panic!("header expected"),
        })
        .collect();
    assert_eq!(heights, (1..=8).collect::<Vec<_>>());
    let requests = server.state.requests.lock().unwrap();
    assert_eq!(requests.len(), 8);
    let sent_heights: Vec<u32> = requests
        .iter()
        .map(|request| request.path.rsplit('/').next().unwrap().parse().unwrap())
        .collect();
    assert_eq!(sent_heights, (1..=8).collect::<Vec<_>>());
    assert!(requests.iter().all(|request| request.had_key));
    for pair in requests.windows(2) {
        assert!(pair[1].at.duration_since(pair[0].at) >= Duration::from_millis(90));
    }
    let reservations = recorder.reserved_at.lock().unwrap();
    for pair in reservations.windows(2) {
        assert!(pair[1].duration_since(pair[0]) >= Duration::from_millis(100));
    }
    for (reserved, received) in reservations.iter().zip(requests.iter()) {
        assert!(
            *reserved <= received.at,
            "attempt persistence must precede the send"
        );
    }
    let maximum = server.state.maximum_active.load(Ordering::SeqCst);
    assert!((2..=3).contains(&maximum));
    assert_eq!(provider.status().unwrap().requests_in_flight, 0);
    assert_eq!(provider.status().unwrap().calls_today, 8);
    assert_eq!(recorder.counts.lock().unwrap().last().unwrap().requests, 8);
}

#[tokio::test]
async fn sequential_requests_reuse_a_connection_and_persist_before_send() {
    let server = Server::start(|_, _| Behavior::json(genesis())).await;
    let (provider, _, recorder) = client(&server.url, |_| {});
    for _ in 0..3 {
        assert!(matches!(
            provider.fetch(Endpoint::Genesis).await.unwrap(),
            Reply::Genesis(_)
        ));
    }
    assert_eq!(server.state.connections.load(Ordering::SeqCst), 1);
    assert_eq!(recorder.counts.lock().unwrap().len(), 3);
    assert_eq!(provider.status().unwrap().calls_today, 3);
}

#[tokio::test]
async fn outcome_classes_have_explicit_actions_and_no_hidden_retry() {
    for (status, code, action) in [
        (400, "provider_request_rejected", Action::Retry),
        (401, "provider_key_rejected", Action::Fatal),
        (402, "quota_exhausted", Action::Retry),
        (403, "provider_key_rejected", Action::Fatal),
        (404, "provider_block_not_ready", Action::Retry),
        (418, "provider_banned", Action::Retry),
        (429, "provider_rate_limited", Action::Retry),
        (500, "provider_unavailable", Action::Retry),
        (503, "provider_unavailable", Action::Retry),
    ] {
        let server = Server::start(move |_, _| Behavior::status(status)).await;
        let (provider, _, recorder) = client(&server.url, |_| {});
        let error = provider.fetch(Endpoint::LatestBlock).await.unwrap_err();
        assert_eq!(error.code, code);
        assert_eq!(error.action, action);
        assert_eq!(error.http_status, Some(status));
        if action == Action::Fatal {
            assert_eq!(error.field, Some("provider.key_file"));
        }
        let again = provider.fetch(Endpoint::LatestBlock).await.unwrap_err();
        assert_eq!(again, error);
        assert_eq!(server.state.requests.lock().unwrap().len(), 1);
        assert_eq!(recorder.counts.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn quota_wait_survives_a_concurrent_success_and_resumes_at_utc_midnight() {
    let server = Server::start(|_, index| {
        if index == 0 {
            Behavior::status(402).delayed(150)
        } else {
            Behavior::json(header(2)).delayed(200)
        }
    })
    .await;
    let (provider, clock, _) = client(&server.url, |config| config.max_in_flight = 2);
    clock.set(1795046399);
    let results: Vec<_> = provider
        .ordered(vec![Endpoint::LatestBlock, Endpoint::LatestBlock])
        .collect()
        .await;
    assert_eq!(results[0].as_ref().unwrap_err().retry_at, Some(1795046400));
    assert!(results[1].is_ok());
    assert_eq!(
        provider
            .fetch(Endpoint::LatestBlock)
            .await
            .unwrap_err()
            .code,
        "quota_exhausted"
    );
    assert_eq!(server.state.requests.lock().unwrap().len(), 2);
    clock.set(1795046400);
    assert!(provider.fetch(Endpoint::LatestBlock).await.is_ok());
    let status = provider.status().unwrap();
    assert_eq!(status.calls_today, 1);
    assert_eq!(status.calls_previous_days[0].requests, 2);
    assert_eq!(status.last_refusal.unwrap().http_status, 402);
}

#[tokio::test]
async fn backoff_progression_and_retry_after_are_honored() {
    let server = Server::start(|_, _| Behavior::status(503)).await;
    let (provider, clock, _) = client(&server.url, |_| {});
    let mut now = NOW;
    for delay in [1, 2, 4, 8, 16, 30, 30] {
        let error = provider.fetch(Endpoint::Genesis).await.unwrap_err();
        assert_eq!(error.retry_at, Some(now + delay));
        now += delay;
        clock.set(now);
    }
    for (http, header, expected) in [
        (429, "7", 7),
        (418, "7", 3600),
        (429, "Wed, 18 Nov 2026 23:00:00 GMT", 1795042800 - NOW),
    ] {
        let server = Server::start(move |_, _| {
            let mut answer = Behavior::status(http);
            answer.retry_after = Some(header.to_owned());
            answer
        })
        .await;
        let (provider, _, _) = client(&server.url, |_| {});
        assert_eq!(
            provider
                .fetch(Endpoint::Genesis)
                .await
                .unwrap_err()
                .retry_at,
            Some(NOW + expected)
        );
    }
}

#[tokio::test]
async fn transport_timeout_truncation_and_response_limits_are_counted_retries() {
    for kind in ["disconnect", "timeout", "truncated", "oversized", "chunked"] {
        let server = Server::start(move |_, index| {
            if index > 0 {
                return Behavior::json(header(2));
            }
            match kind {
                "disconnect" => {
                    let mut reply = Behavior::raw(String::new());
                    reply.disconnect = true;
                    reply
                }
                "timeout" => Behavior::json(header(1)).delayed(300),
                "truncated" => Behavior::raw("[{\"tx_hash\":".to_owned()),
                "oversized" => Behavior::raw(" ".repeat(65536)),
                _ => {
                    let mut reply = Behavior::raw(" ".repeat(65536));
                    reply.chunked = true;
                    reply
                }
            }
        })
        .await;
        let (provider, clock, _) = client(&server.url, |config| {
            config.limits.headers = 1024;
            if kind == "timeout" {
                config.request_timeout = Duration::from_millis(50);
            }
        });
        let error = provider.fetch(Endpoint::LatestBlock).await.unwrap_err();
        assert_eq!(error.action, Action::Retry);
        assert_eq!(
            error.code,
            match kind {
                "disconnect" => "provider_unavailable",
                "timeout" => "provider_timeout",
                "truncated" => "provider_invalid_response",
                _ => "provider_response_size_limit",
            }
        );
        assert_eq!(provider.status().unwrap().calls_today, 1);
        assert_eq!(provider.status().unwrap().requests_in_flight, 0);
        clock.set(error.retry_at.unwrap());
        assert!(provider.fetch(Endpoint::LatestBlock).await.is_ok());
        assert_eq!(provider.status().unwrap().calls_today, 2);
        assert_eq!(server.state.requests.lock().unwrap().len(), 2);
    }
}

#[tokio::test]
async fn absent_datums_and_unready_epochs_have_distinct_meanings() {
    let server = Server::start(|_, _| Behavior::status(404)).await;
    let (provider, _, _) = client(&server.url, |_| {});
    assert!(matches!(
        provider.fetch(Endpoint::Datum(point())).await.unwrap(),
        Reply::Datum(None)
    ));
    assert_eq!(
        provider.fetch(Endpoint::Epoch(667)).await.unwrap_err().code,
        "provider_epoch_not_ready"
    );
    assert_eq!(provider.status().unwrap().calls_today, 2);
    for value in [
        json!({"epoch":667,"active_stake":null}),
        json!({"epoch":667,"nonce":null,"max_tx_size":16384,"max_block_size":90112}),
    ] {
        let parameters = value.get("nonce").is_some();
        let server = Server::start(move |_, _| Behavior::json(value.clone())).await;
        let (provider, _, _) = client(&server.url, |_| {});
        let endpoint = if parameters {
            Endpoint::EpochParameters(667)
        } else {
            Endpoint::Epoch(667)
        };
        assert_eq!(
            provider.fetch(endpoint).await.unwrap_err().code,
            "provider_epoch_not_ready"
        );
    }
}

#[tokio::test]
async fn accounting_failure_prevents_send_and_restored_counts_continue() {
    let server = Server::start(|_, _| Behavior::json(genesis())).await;
    let (provider, clock, recorder) = client(&server.url, |_| {});
    recorder.fail.store(true, Ordering::SeqCst);
    let error = provider.fetch(Endpoint::Genesis).await.unwrap_err();
    assert_eq!(error.code, "provider_accounting_unavailable");
    assert_eq!(server.state.requests.lock().unwrap().len(), 0);
    recorder.fail.store(false, Ordering::SeqCst);
    clock.set(error.retry_at.unwrap());
    provider.fetch(Endpoint::Genesis).await.unwrap();
    let seed = recorder.counts.lock().unwrap().clone();
    let key = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(key.path(), KEY).unwrap();
    let restored = Provider::with_clock(
        Config::new(&server.url, key.path()),
        Arc::new(Recorder::default()),
        seed,
        clock,
    )
    .unwrap();
    restored.fetch(Endpoint::Genesis).await.unwrap();
    assert_eq!(restored.status().unwrap().calls_today, 2);
}

#[tokio::test]
async fn credential_reflection_and_redirects_are_refused() {
    let target = Server::start(|_, _| Behavior::json(genesis())).await;
    let destination = target.url.clone();
    let redirect = Server::start(move |_, _| {
        let mut reply = Behavior::status(302);
        reply.location = Some(destination.clone());
        reply
    })
    .await;
    let (provider, _, _) = client(&redirect.url, |_| {});
    assert_eq!(
        provider.fetch(Endpoint::Genesis).await.unwrap_err().code,
        "provider_request_rejected"
    );
    assert!(target.state.requests.lock().unwrap().is_empty());
    for key in [
        KEY.to_owned(),
        "synthetic-blockfrost-project-\\u0069d".to_owned(),
    ] {
        let server=Server::start(move |_,_|Behavior::raw(format!("{{\"network_magic\":764824073,\"security_param\":2160,\"epoch_length\":432000,\"system_start\":1506203091,\"slot_length\":1,\"unknown\":\"{key}\"}}"))).await;
        let (provider, _, _) = client(&server.url, |_| {});
        let error = provider.fetch(Endpoint::Genesis).await.unwrap_err();
        assert_eq!(error.code, "unsafe_provider_response");
        assert!(!format!("{error:?}").contains(KEY));
    }
}

#[tokio::test]
async fn https_checks_certificate_trust_and_hostname() {
    for names in [vec!["127.0.0.1".to_owned()], vec!["localhost".to_owned()]] {
        let matching = names[0] == "127.0.0.1";
        let server = Server::tls(|_, _| Behavior::json(genesis()), names).await;
        let certificate = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(certificate.path(), server.certificate.as_ref().unwrap()).unwrap();
        let (provider, _, _) = client(&server.url, |config| {
            config.root_certificate = Some(certificate.path().to_owned())
        });
        let result = provider.fetch(Endpoint::Genesis).await;
        assert_eq!(result.is_ok(), matching);
        if !matching {
            assert_eq!(result.unwrap_err().code, "provider_unavailable");
        }
        assert_eq!(provider.status().unwrap().calls_today, 1);
    }
    let server = Server::tls(
        |_, _| Behavior::json(genesis()),
        vec!["127.0.0.1".to_owned()],
    )
    .await;
    let (provider, _, _) = client(&server.url, |_| {});
    assert_eq!(
        provider.fetch(Endpoint::Genesis).await.unwrap_err().code,
        "provider_unavailable"
    );
}

#[tokio::test]
async fn cbor_exact_page_boundary_and_synthetic_forks_are_available_to_checks() {
    let server = Server::start(|path, index| {
        if path.contains("/txs/cbor") {
            if path.contains("page=2") {
                return Behavior::json(json!([]));
            }
            return Behavior::json(json!(
                (0..100)
                    .map(|number| json!({"tx_hash":format!("{number:064x}"),"cbor":"84a000f5f6"}))
                    .collect::<Vec<_>>()
            ));
        }
        let mut answer = header(10);
        answer["hash"] = json!(if index.is_multiple_of(2) {
            hash('a')
        } else {
            hash('b')
        });
        answer["tx_count"] = json!(2);
        Behavior::json(answer)
    })
    .await;
    let (provider, _, _) = client(&server.url, |_| {});
    let first = provider
        .fetch(Endpoint::BlockCbor {
            block: point(),
            page: page(),
        })
        .await
        .unwrap();
    assert!(matches!(first,Reply::Transactions(rows) if rows.len()==100));
    let second = provider
        .fetch(Endpoint::BlockCbor {
            block: point(),
            page: Page::new(2).unwrap(),
        })
        .await
        .unwrap();
    assert!(matches!(second,Reply::Transactions(rows) if rows.is_empty()));
    let first = provider.fetch(Endpoint::LatestBlock).await.unwrap();
    let second = provider.fetch(Endpoint::LatestBlock).await.unwrap();
    match (first, second) {
        (Reply::Header(a), Reply::Header(b)) => {
            assert_eq!(a.height, b.height);
            assert_ne!(a.hash, b.hash);
        }
        _ => panic!("headers expected"),
    }
}

#[tokio::test]
async fn every_typed_endpoint_accepts_the_primary_response_shape() {
    let pool = pool();
    let pool_id = pool.to_string();
    let block = point();
    let block_hash = block.to_string();
    let mut by_hash = header(1);
    by_hash["hash"] = json!(block_hash);
    let cases = vec![
        (
            Endpoint::LatestBlock,
            "/blocks/latest".to_owned(),
            header(1),
        ),
        (
            Endpoint::BlockAtHeight(1),
            "/blocks/1".to_owned(),
            header(1),
        ),
        (
            Endpoint::Block(block.clone()),
            format!("/blocks/{block_hash}"),
            by_hash,
        ),
        (
            Endpoint::NextBlocks {
                block: block.clone(),
                page: page(),
            },
            format!("/blocks/{block_hash}/next?count=100&page=1"),
            json!([header(2)]),
        ),
        (
            Endpoint::BlockCbor {
                block: block.clone(),
                page: page(),
            },
            format!("/blocks/{block_hash}/txs/cbor?count=100&page=1&order=asc"),
            json!([{"tx_hash": hash('2'), "cbor": "84a0a0f5f6"}]),
        ),
        (Endpoint::Genesis, "/genesis".to_owned(), genesis()),
        (
            Endpoint::Epoch(667),
            "/epochs/667".to_owned(),
            json!({"epoch": 667, "active_stake": "1000"}),
        ),
        (
            Endpoint::EpochParameters(667),
            "/epochs/667/parameters".to_owned(),
            json!({"epoch": 667, "nonce": hash('1'), "max_tx_size": 16384, "max_block_size": 90112, "unknown": {}}),
        ),
        (
            Endpoint::EpochBlocks {
                epoch: 667,
                page: page(),
            },
            "/epochs/667/blocks?count=100&page=1&order=desc".to_owned(),
            json!([block_hash]),
        ),
        (
            Endpoint::EpochPoolStake {
                epoch: 667,
                pool: pool.clone(),
            },
            format!("/epochs/667/stakes/{pool_id}?count=1&page=1"),
            json!([{"stake_address": "stake1synthetic", "amount": "0"}]),
        ),
        (
            Endpoint::Pools(page()),
            "/pools?count=100&page=1&order=asc".to_owned(),
            json!([pool_id]),
        ),
        (
            Endpoint::ExtendedPools(page()),
            "/pools/extended?count=100&page=1&order=asc".to_owned(),
            json!([{"pool_id": pool_id, "active_stake": "1000", "metadata": {"anything": true}}]),
        ),
        (
            Endpoint::RetiredPools(page()),
            "/pools/retired?count=100&page=1&order=desc".to_owned(),
            json!([{"pool_id": pool_id, "epoch": 666}]),
        ),
        (
            Endpoint::Pool(pool.clone()),
            format!("/pools/{pool_id}"),
            json!({"pool_id": pool_id, "active_stake": "1000"}),
        ),
        (
            Endpoint::PoolHistory { pool, page: page() },
            format!("/pools/{pool_id}/history?count=100&page=1&order=desc"),
            json!([{"epoch": 667, "active_stake": "1000", "delegators_count": 1}]),
        ),
        (
            Endpoint::Datum(block.clone()),
            format!("/scripts/datum/{block_hash}/cbor"),
            json!({"cbor": "19a6aa"}),
        ),
        (
            Endpoint::Metadata(block.clone()),
            format!("/txs/{block_hash}/metadata"),
            json!([{"label": "6500973", "json_metadata": ["0x0123"]}]),
        ),
        (
            Endpoint::TransactionOutputs(block),
            format!("/txs/{block_hash}/utxos"),
            json!({"hash": block_hash, "inputs": [{"ignored": "input rendering"}], "outputs": [{
                "address": "addr1synthetic", "output_index": 0,
                "amount": [{"unit": "lovelace", "quantity": "2"}],
                "data_hash": null, "inline_datum": null, "collateral": false,
                "reference_script_hash": null, "consumed_by_tx": null
            }]}),
        ),
    ];
    let responses: Vec<_> = cases
        .iter()
        .map(|(_, path, body)| (path.clone(), body.clone()))
        .collect();
    let server = Server::start(move |path, index| {
        let (expected, body) = &responses[index];
        assert_eq!(path, format!("/api/v0{expected}"));
        Behavior::json(body.clone())
    })
    .await;
    let (provider, _, _) = client(&server.url, |_| {});
    let count = cases.len();
    for (endpoint, _, _) in cases {
        provider.fetch(endpoint).await.unwrap();
    }
    assert_eq!(server.state.requests.lock().unwrap().len(), count);
}

#[tokio::test]
async fn utxo_fallback_discards_large_inputs_and_limits_retained_outputs() {
    let output = json!({"address":"addr1synthetic","output_index":0,"amount":[{"unit":"lovelace","quantity":"2"}],
        "data_hash":null,"inline_datum":null,"collateral":false,"consumed_by_tx":null});
    let normal = output.clone();
    let server=Server::start(move |_,_|Behavior::json(json!({"hash":hash('1'),
        "inputs":[{"address":"historical input","amount":[{"unit":"lovelace","quantity":"1"}],"ignored":"x".repeat(2*1024*1024)}],
        "outputs":[normal.clone()]}))).await;
    let (provider, _, _) = client(&server.url, |config| {
        config.limits.transaction_outputs = 4 * 1024 * 1024;
        config.limits.transaction_output_values = 1024;
    });
    let row = provider
        .fetch(Endpoint::TransactionOutputs(point()))
        .await
        .unwrap();
    assert!(matches!(row,Reply::TransactionOutputs(row) if row.outputs.len()==1));
    let server = Server::start(move |_, _| {
        Behavior::json(json!({"hash":hash('1'),"inputs":[],"outputs":(0..100).map(|index|{let mut row=output.clone();row["output_index"]=json!(index);row}).collect::<Vec<_>>()}))
    })
    .await;
    let (provider, _, _) = client(&server.url, |config| {
        config.limits.transaction_output_values = 1024
    });
    assert_eq!(
        provider
            .fetch(Endpoint::TransactionOutputs(point()))
            .await
            .unwrap_err()
            .code,
        "provider_response_size_limit"
    );
}

#[tokio::test]
async fn raw_fallback_evidence_tees_once_with_bounded_retained_values() {
    use dsa_chain::observations::Shape;
    use sha2::{Digest, Sha256};
    use std::collections::BTreeSet;
    use std::io::Read;
    let raw = format!(
        " \n{}\n",
        json!({"hash":hash('1'),"inputs":[{"ignored":"x".repeat(2*1024*1024)}],"outputs":[{"address":"addr1synthetic","output_index":0,"amount":[{"unit":"lovelace","quantity":"2"}],"data_hash":null,"inline_datum":null,"collateral":false,"consumed_by_tx":null}]})
    );
    let body = raw.clone();
    let server = Server::start(move |_, _| Behavior::raw(body.clone())).await;
    let (provider, _, recorder) = client(&server.url, |config| {
        config.limits.transaction_outputs = 4 * 1024 * 1024;
        config.limits.transaction_output_values = 1024;
    });
    let directory = tempfile::tempdir().unwrap();
    let captured = provider
        .fetch_with_evidence(
            Endpoint::TransactionOutputs(point()),
            &BTreeSet::from([Shape::StoredPointerAddress]),
            directory.path(),
        )
        .await
        .unwrap();
    let (_, reply, raw_response) = captured.into_parts();
    assert!(matches!(reply,Reply::TransactionOutputs(row) if row.outputs.len()==1));
    let mut evidence = raw_response.unwrap();
    assert_eq!(
        evidence.sha256(),
        format!("{:x}", Sha256::digest(raw.as_bytes()))
    );
    assert_eq!(evidence.length(), raw.len() as u64);
    let mut recovered = Vec::new();
    evidence
        .reader()
        .unwrap()
        .read_to_end(&mut recovered)
        .unwrap();
    assert_eq!(recovered, raw.as_bytes());
    assert_eq!(recorder.counts.lock().unwrap().len(), 1);
    assert_eq!(server.state.requests.lock().unwrap().len(), 1);
    drop(evidence);
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    let missing = directory.path().join("not-created");
    let (_, _, raw) = provider
        .fetch_with_evidence(
            Endpoint::TransactionOutputs(point()),
            &BTreeSet::new(),
            &missing,
        )
        .await
        .unwrap()
        .into_parts();
    assert!(raw.is_none());
}
#[tokio::test]
async fn raw_fallback_rejects_escaped_reflected_key_in_discarded_input() {
    use dsa_chain::observations::Shape;
    use std::collections::BTreeSet;
    let key = "synthetic-blockfrost-project-\\u0069d";
    let body = format!(
        "{{\"hash\":\"{}\",\"inputs\":[{{\"ignored\":\"{key}\"}}],\"outputs\":[]}}",
        hash('1')
    );
    let server = Server::start(move |_, _| Behavior::raw(body.clone())).await;
    let (provider, _, recorder) = client(&server.url, |_| {});
    let directory = tempfile::tempdir().unwrap();
    let result = provider
        .fetch_with_evidence(
            Endpoint::TransactionOutputs(point()),
            &BTreeSet::from([Shape::StoredLargeDatum]),
            directory.path(),
        )
        .await;
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("reflected key accepted"),
    };
    assert_eq!(error.code, "unsafe_provider_response");
    assert!(!format!("{error:?}").contains(KEY));
    assert_eq!(recorder.counts.lock().unwrap().len(), 1);
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn local_evidence_capacity_does_not_backoff_ordinary_provider_requests() {
    use dsa_chain::observations::Shape;
    use std::collections::BTreeSet;
    let server = Server::start(|_, _| Behavior::raw("[]".into())).await;
    let (provider, _, recorder) = client(&server.url, |config| {
        config.limits.evidence_memory = 1;
        config.limits.evidence_spool = 1;
        config.limits.transaction_outputs = dsa_provider::evidence::MAX_RESPONSE_BYTES + 1;
    });
    let directory = tempfile::tempdir().unwrap();
    let result = provider
        .fetch_with_evidence(
            Endpoint::Metadata(point()),
            &BTreeSet::from([Shape::StoredBridgeMetadata]),
            directory.path(),
        )
        .await;
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("capacity limit ignored"),
    };
    assert_eq!(error.code, "evidence_capture_budget");
    assert!(provider.status().unwrap().retry_at.is_none());
    assert!(
        matches!(provider.fetch(Endpoint::Metadata(point())).await.unwrap(),Reply::Metadata(rows) if rows.is_empty())
    );
    assert_eq!(recorder.counts.lock().unwrap().len(), 2);
    let missing = directory.path().join("missing");
    let result = provider
        .fetch_with_evidence(
            Endpoint::TransactionOutputs(point()),
            &BTreeSet::from([Shape::StoredPointerAddress]),
            &missing,
        )
        .await;
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("missing spool directory accepted"),
    };
    assert_eq!(error.code, "evidence_storage_unavailable");
    assert!(provider.status().unwrap().retry_at.is_none());
    assert!(
        matches!(provider.fetch(Endpoint::Metadata(point())).await.unwrap(),Reply::Metadata(rows) if rows.is_empty())
    );
    assert_eq!(recorder.counts.lock().unwrap().len(), 4);
}
