//! The client against loopback fakes: outcome classes, isolation of one
//! endpoint's answer, pacing, waits, timeouts, hostile answers and counts.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use chrono::NaiveDate;
use dsa_provider::{
    Clock, Config, Error, Kind, Provider,
    pages::BlockPages,
    types::{AddressText, Hash32, Heights, Page, PoolId, Unit},
};
use dsa_testkit::{
    chain::Chain,
    server::{Behavior, KEY, Server},
};
use serde_json::{Value, json};
use tokio::time::Instant;

const NOW: u64 = 1_795_000_000;
const MIDNIGHT: u64 = 1_795_046_400;

struct TestClock(AtomicU64);
impl Clock for TestClock {
    fn unix_seconds(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}
impl TestClock {
    fn set(&self, time: u64) {
        self.0.store(time, Ordering::SeqCst);
    }
}

fn client(url: &str, configure: impl FnOnce(&mut Config)) -> (Provider, Arc<TestClock>) {
    let key = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(key.path(), KEY).unwrap();
    let mut config = Config::new(url, key.path());
    config.requests_per_second = 10.0;
    configure(&mut config);
    let clock = Arc::new(TestClock(AtomicU64::new(NOW)));
    (Provider::with_clock(config, clock.clone()).unwrap(), clock)
}

fn hash(character: char) -> Hash32 {
    character.to_string().repeat(64).parse().unwrap()
}

fn header(height: u32) -> Value {
    json!({"hash":format!("{height:064x}"),"height":height,"slot":height as u64*20,
        "epoch":667,"time":NOW,"tx_count":1,"size":400,
        "previous_block":format!("{:064x}",height.saturating_sub(1)),
        "next_block":null,"confirmations":1,"unknown":{"nested":[1,2,3]}})
}

fn genesis() -> Value {
    json!({"network_magic":764824073,"security_param":2160,"epoch_length":432000,
        "system_start":1506203091,"slot_length":1,"active_slots_coefficient":0.05})
}

fn pool() -> PoolId {
    bech32::encode::<bech32::Bech32>(bech32::Hrp::parse("pool").unwrap(), &[7; 28])
        .unwrap()
        .parse()
        .unwrap()
}

fn day(unix: u64) -> NaiveDate {
    chrono::DateTime::from_timestamp(unix as i64, 0)
        .unwrap()
        .date_naive()
}

#[tokio::test]
async fn pace_and_in_flight_never_exceed_the_configuration() {
    let server = Server::start(|path, _| {
        let height = path.rsplit('/').next().unwrap().parse::<u32>().unwrap();
        Behavior::json(header(height)).delayed(if height == 1 { 450 } else { 250 })
    })
    .await;
    let (provider, _) = client(&server.url, |config| config.max_in_flight = 3);
    let calls = (1..=8).map(|height| provider.block_at(height));
    let heights: Vec<_> = futures_util::future::join_all(calls)
        .await
        .into_iter()
        .map(|header| header.unwrap().unwrap().height)
        .collect();
    assert_eq!(heights, (1..=8).collect::<Vec<_>>());
    let requests = server.state.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 8);
    assert!(requests.iter().all(|request| request.had_key));
    for pair in requests.windows(2) {
        assert!(pair[1].at.duration_since(pair[0].at) >= Duration::from_millis(90));
    }
    let maximum = server.state.maximum_active.load(Ordering::SeqCst);
    assert!((2..=3).contains(&maximum), "{maximum} answered at once");
    assert_eq!(provider.status().requests_in_flight, 0);
    assert_eq!(provider.counts(), vec![(day(NOW), 8)]);
}

#[tokio::test]
async fn sequential_requests_reuse_one_connection() {
    let server = Server::start(|_, _| Behavior::json(genesis())).await;
    let (provider, _) = client(&server.url, |_| {});
    for _ in 0..3 {
        provider.genesis().await.unwrap();
    }
    assert_eq!(server.state.connections.load(Ordering::SeqCst), 1);
    assert_eq!(provider.counts(), vec![(day(NOW), 3)]);
}

#[tokio::test]
async fn each_outcome_class_is_typed_and_only_global_refusals_gate() {
    let redirect = |mut behavior: Behavior| {
        behavior.location = Some("http://127.0.0.1:9/elsewhere".to_owned());
        behavior
    };
    let mut chunked = Behavior::raw(" ".repeat(65_536));
    chunked.chunked = true;
    // Noon UTC: the 402 class waits for the next midnight.
    const T: u64 = MIDNIGHT - 43_200;
    let retry = |delay| Kind::Retry { until: T + delay };
    let wait = |until| Kind::Wait { until };
    let fatal = Kind::Fatal {
        field: "provider.key_file",
    };
    /// Name, first answer, code, kind, HTTP status, whether it gates.
    type Class = (
        &'static str,
        Behavior,
        &'static str,
        Kind,
        Option<u16>,
        bool,
    );
    let classes: Vec<Class> = vec![
        (
            "404 without meaning",
            Behavior::status(404),
            "provider_not_found",
            retry(0),
            None,
            false,
        ),
        (
            "400",
            Behavior::status(400),
            "provider_request_rejected",
            retry(0),
            Some(400),
            false,
        ),
        (
            "redirect",
            redirect(Behavior::status(302)),
            "provider_request_rejected",
            retry(0),
            Some(302),
            false,
        ),
        (
            "401",
            Behavior::status(401),
            "provider_key_rejected",
            fatal,
            Some(401),
            true,
        ),
        (
            "403",
            Behavior::status(403),
            "provider_key_rejected",
            fatal,
            Some(403),
            true,
        ),
        (
            "402",
            Behavior::status(402),
            "quota_exhausted",
            wait(MIDNIGHT + 120),
            Some(402),
            true,
        ),
        (
            "418",
            Behavior::status(418),
            "provider_banned",
            wait(T + 3600),
            Some(418),
            true,
        ),
        (
            "429",
            Behavior::status(429),
            "provider_rate_limited",
            wait(T + 60),
            Some(429),
            true,
        ),
        (
            "500",
            Behavior::status(500),
            "provider_unavailable",
            retry(1),
            Some(500),
            true,
        ),
        (
            "503",
            Behavior::status(503),
            "provider_unavailable",
            retry(1),
            Some(503),
            true,
        ),
        (
            "dropped connection",
            Behavior::dropped(),
            "provider_unavailable",
            retry(1),
            None,
            true,
        ),
        (
            "broken body",
            Behavior::json(header(1)).broken_after(10),
            "provider_unavailable",
            retry(1),
            None,
            true,
        ),
        (
            "truncated body",
            Behavior::json(header(1)).truncated(40),
            "provider_invalid_response",
            retry(0),
            None,
            false,
        ),
        (
            "wrong shape",
            Behavior::json(json!({"x":1})),
            "provider_invalid_response",
            retry(0),
            None,
            false,
        ),
        (
            "oversized",
            Behavior::raw(" ".repeat(65_536)),
            "provider_response_size_limit",
            retry(0),
            None,
            false,
        ),
        (
            "oversized without length",
            chunked,
            "provider_response_size_limit",
            retry(0),
            None,
            false,
        ),
    ];
    for (name, first, code, kind, http_status, gates) in classes {
        let server = Server::start(move |_, index| {
            if index == 0 {
                first.clone()
            } else {
                Behavior::json(genesis())
            }
        })
        .await;
        let (provider, clock) = client(&server.url, |config| config.limits.small = 1024);
        clock.set(T);
        let error = provider.latest_block().await.unwrap_err();
        assert_eq!(
            (error.code, error.kind, error.http_status),
            (code, kind, http_status),
            "{name}"
        );
        assert_eq!(provider.counts()[0].1, 1, "{name}: the attempt is counted");
        let next = provider.genesis().await;
        if gates {
            let blocked = next.unwrap_err();
            assert_eq!((blocked.code, blocked.kind), (code, kind), "{name}");
            assert_eq!(blocked.http_status, None, "{name}: not sent");
            assert_eq!(server.state.count(), 1, "{name}: every request waits");
        } else {
            next.unwrap_or_else(|error| panic!("{name}: another request failed with {error}"));
            assert_eq!(server.state.count(), 2, "{name}: nothing shared changed");
        }
    }
}

#[tokio::test]
async fn a_404_with_a_meaning_is_a_value() {
    let server = Server::start(|_, _| Behavior::status(404)).await;
    let (provider, _) = client(&server.url, |_| {});
    assert_eq!(provider.block_at(5).await.unwrap(), None);
    assert!(
        provider
            .next_blocks(&hash('1'), Page::first())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        provider
            .block_transactions(&hash('1'), Page::first())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(provider.datum(&hash('1')).await.unwrap(), None);
    assert_eq!(provider.epoch_parameters(667).await.unwrap(), None);
    assert_eq!(provider.epoch(667).await.unwrap(), None);
    assert_eq!(provider.pool(&pool()).await.unwrap(), None);
    assert!(provider.status().blocked.is_none());
    for (path, body) in [
        (
            "/epochs/667/parameters",
            json!({"epoch":667,"nonce":null,"max_tx_size":16384}),
        ),
        ("/epochs/667", json!({"epoch":667,"active_stake":null})),
    ] {
        let server = Server::start(move |_, _| Behavior::json(body.clone())).await;
        let (provider, _) = client(&server.url, |_| {});
        let ready = if path.ends_with("parameters") {
            provider.epoch_parameters(667).await.unwrap().is_some()
        } else {
            provider.epoch(667).await.unwrap().is_some()
        };
        assert!(!ready, "{path}: an epoch without its value is not ready");
    }
}

#[tokio::test]
async fn a_404_on_one_endpoint_does_not_delay_or_change_another_request() {
    let server = Server::start(|path, _| {
        if path.contains("/utxos") {
            Behavior::status(404).delayed(100)
        } else if path.contains("/next") {
            Behavior::status(404).delayed(150)
        } else {
            Behavior::json(header(9)).delayed(300)
        }
    })
    .await;
    let (provider, _) = client(&server.url, |config| config.max_in_flight = 3);
    let (spent, stored) = (hash('2'), hash('3'));
    let (outputs, tip, fork) = tokio::join!(
        provider.transaction_outputs(&spent),
        provider.latest_block(),
        provider.next_blocks(&stored, Page::first()),
    );
    assert_eq!(outputs.unwrap_err().code, "provider_not_found");
    assert_eq!(tip.unwrap().height, 9);
    assert!(
        fork.unwrap().is_none(),
        "the tip's own 404 still means a fork"
    );
    let started = Instant::now();
    assert_eq!(provider.latest_block().await.unwrap().height, 9);
    assert!(
        started.elapsed() < Duration::from_millis(900),
        "not delayed"
    );
    assert_eq!(server.state.count(), 4, "every request was sent");
    assert!(provider.status().blocked.is_none());
}

#[tokio::test]
async fn backoff_progression_and_retry_after_are_honored() {
    let server = Server::start(|_, _| Behavior::status(503)).await;
    let (provider, clock) = client(&server.url, |_| {});
    let mut now = NOW;
    for delay in [1, 2, 4, 8, 16, 30, 30] {
        let error = provider.genesis().await.unwrap_err();
        assert_eq!(error.kind, Kind::Retry { until: now + delay });
        now += delay;
        clock.set(now);
    }
    for (status, header, expected) in [
        (429, "7", 7),
        (418, "7", 3600),
        (429, "Wed, 18 Nov 2026 23:00:00 GMT", 1_795_042_800 - NOW),
    ] {
        let server = Server::start(move |_, _| Behavior::status(status).retry_after(header)).await;
        let (provider, _) = client(&server.url, |_| {});
        assert_eq!(
            provider.genesis().await.unwrap_err().kind,
            Kind::Wait {
                until: NOW + expected
            }
        );
    }
}

#[tokio::test]
async fn a_spent_allowance_waits_for_the_next_day_and_an_early_refusal_retries_within_ten_minutes()
{
    for (refused_at, expected) in [
        (MIDNIGHT - 1, MIDNIGHT + 120),
        (MIDNIGHT + 1, MIDNIGHT + 301),
    ] {
        let server = Server::start(|_, index| {
            if index == 0 {
                Behavior::status(402)
            } else {
                Behavior::json(genesis())
            }
        })
        .await;
        let (provider, clock) = client(&server.url, |_| {});
        clock.set(refused_at);
        let error = provider.genesis().await.unwrap_err();
        assert_eq!(error.code, "quota_exhausted");
        let until = error.until().unwrap();
        assert_eq!(until, expected);
        if refused_at > MIDNIGHT {
            assert!(until - refused_at <= 600, "retried within ten minutes");
        }
        assert_eq!(provider.status().last_refusal.unwrap().http_status, 402);
        clock.set(until - 1);
        assert_eq!(
            provider.genesis().await.unwrap_err().code,
            "quota_exhausted"
        );
        assert_eq!(server.state.count(), 1, "nothing is sent while waiting");
        clock.set(until);
        provider.genesis().await.unwrap();
        assert_eq!(server.state.count(), 2);
    }
}

#[tokio::test]
async fn truncated_and_hostile_answers_return_an_error_or_a_value() {
    let deep_metadata = format!(
        "[{{\"label\":\"6500973\",\"json_metadata\":{}1{}}}]",
        "[".repeat(5_000),
        "]".repeat(5_000)
    );
    let deep_inputs = format!(
        "{{\"hash\":\"{}\",\"inputs\":{}0{},\"outputs\":[]}}",
        hash('2'),
        "[".repeat(5_000),
        "]".repeat(5_000)
    );
    let page = json!([{"tx_hash": hash('2'), "cbor": "84a300d90102800180020af5f6a0"}]).to_string();
    let answers = Arc::new(Mutex::new(vec![
        "{\"hash\":\"abc\\".to_owned(),
        deep_metadata.clone(),
        deep_inputs,
    ]));
    let offsets: Vec<_> = (0..page.len()).step_by(7).collect();
    for offset in &offsets {
        answers.lock().unwrap().push(page[..*offset].to_owned());
    }
    let served = answers.clone();
    let server =
        Server::start(move |_, index| Behavior::raw(served.lock().unwrap()[index].clone())).await;
    let (provider, _) = client(&server.url, |_| {});
    assert_eq!(
        provider.latest_block().await.unwrap_err().code,
        "provider_invalid_response",
        "a body ending in a backslash"
    );
    let metadata = provider.transaction_metadata(&hash('1')).await.unwrap();
    let raw = metadata.label(6_500_973).unwrap();
    assert_eq!(raw.len(), 10_001);
    assert_eq!(
        dsa_chain::metadata::normalize(raw).unwrap(),
        None,
        "deeper than the database renders: stored as NULL"
    );
    assert_eq!(&metadata.body[..], deep_metadata.as_bytes());
    assert_eq!(
        provider
            .transaction_outputs(&hash('2'))
            .await
            .unwrap_err()
            .code,
        "provider_json_depth_limit",
        "the streamed reader bounds the depth it skips"
    );
    for offset in offsets {
        let result = provider.block_transactions(&hash('1'), Page::first()).await;
        assert!(result.is_err(), "a page cut at byte {offset} is refused");
    }
    assert!(provider.status().blocked.is_none());
}

#[tokio::test]
async fn a_stalled_answer_ends_at_the_idle_timeout() {
    // The total time (10 s) is far above the stall (3 s): only the idle
    // timeout can end this answer early.
    let server = Server::start(|_, index| match index {
        0 => Behavior::json(header(1)).stalled(10, Duration::from_secs(3)),
        _ => Behavior::json(header(1)),
    })
    .await;
    let (provider, _) = client(&server.url, |config| {
        config.idle_timeout = Duration::from_millis(300);
        config.request_timeout = Duration::from_secs(10);
    });
    let started = Instant::now();
    let error = provider.latest_block().await.unwrap_err();
    assert_eq!(error.code, "provider_timeout");
    assert!(started.elapsed() < Duration::from_millis(1_500));
    assert!(
        provider.status().blocked.is_none(),
        "a slow answer is not an outage"
    );
    provider.latest_block().await.unwrap();
    let server = Server::start(|_, index| match index {
        0 => Behavior::json(header(1)).delayed(2_000),
        _ => Behavior::json(header(1)),
    })
    .await;
    let (provider, clock) = client(&server.url, |config| {
        config.request_timeout = Duration::from_secs(1);
    });
    let error = provider.latest_block().await.unwrap_err();
    assert_eq!(
        error.code, "provider_timeout",
        "no answer within the total time"
    );
    clock.set(error.until().unwrap());
    provider.latest_block().await.unwrap();
}

#[tokio::test]
async fn call_counts_are_kept_per_utc_day_and_seeded_at_start() {
    let server = Server::start(|_, index| {
        if index == 1 {
            Behavior::status(400)
        } else {
            Behavior::json(genesis())
        }
    })
    .await;
    let (provider, clock) = client(&server.url, |_| {});
    clock.set(MIDNIGHT - 10);
    provider.genesis().await.unwrap();
    provider.genesis().await.unwrap_err();
    provider.genesis().await.unwrap();
    clock.set(MIDNIGHT);
    provider.genesis().await.unwrap();
    let counts = provider.counts();
    assert_eq!(counts, vec![(day(MIDNIGHT - 10), 3), (day(MIDNIGHT), 1)]);
    let (restarted, restarted_clock) = client(&server.url, |_| {});
    restarted_clock.set(MIDNIGHT + 5);
    restarted.seed_counts(&counts);
    restarted.genesis().await.unwrap();
    restarted.seed_counts(&[(day(MIDNIGHT), 1)]);
    assert_eq!(
        restarted.counts(),
        vec![(day(MIDNIGHT - 10), 3), (day(MIDNIGHT), 2)]
    );
}

/// Each endpoint counts in its family. A request counts when it is sent: a
/// call dropped while it waits for the pace and an attempt the gate refuses
/// are counted neither in a family nor in the day.
#[tokio::test]
async fn requests_count_by_family_when_they_are_sent() {
    let unknown = Server::start(|_, _| Behavior::status(404)).await;
    let (provider, _) = client(&unknown.url, |_| {});
    let unit: Unit = "d2dbff622e509dda256fedbd31ef6e9fd98ed49ad91d5c0e07f68af1"
        .parse()
        .unwrap();
    let address: AddressText = "addr_test1synthetic".parse().unwrap();
    let (p, all) = (Page::first(), Heights::default());
    let policy = unit.policy().to_owned();
    assert!(
        provider
            .asset_outputs(&unit, all, p)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        provider
            .address_outputs(&address, p)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        provider
            .asset_transactions(&unit, all, p, false)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        provider
            .address_transactions(&address, all, p, true)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(provider.policy_assets(&policy, p).await.unwrap().is_empty());
    assert!(provider.asset(&unit).await.unwrap().is_none());
    assert!(provider.asset_history(&unit, p).await.unwrap().is_empty());
    assert!(provider.block_by_hash(&hash('1')).await.unwrap().is_none());
    assert!(provider.block_at(7).await.unwrap().is_none());
    assert!(provider.transaction(&hash('2')).await.unwrap().is_none());
    assert!(
        provider
            .transaction_utxos(&hash('3'))
            .await
            .unwrap()
            .is_none()
    );
    assert!(provider.epoch_last_block(667).await.unwrap().is_none());
    let expected = [
        ("assets", 2),
        ("epochs", 1),
        ("headers", 2),
        ("listings", 5),
    ]
    .into_iter()
    .chain([("outputs", 1), ("transactions", 1)])
    .map(|(family, count)| (family.to_owned(), count))
    .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(provider.sent_by_family(), expected);
    assert_eq!(unknown.state.count(), 12);

    let server = Server::start(|path, _| {
        if path.ends_with("/blocks/latest") {
            Behavior::status(429).retry_after("60")
        } else {
            Behavior::json(genesis())
        }
    })
    .await;
    let (provider, _) = client(&server.url, |config| config.requests_per_second = 0.5);
    provider.genesis().await.unwrap();
    let waiting = tokio::time::timeout(Duration::from_millis(300), provider.genesis()).await;
    assert!(
        waiting.is_err(),
        "the second call did not wait for the pace"
    );
    let refused = provider.latest_block().await.unwrap_err();
    assert_eq!(refused.http_status, Some(429));
    let gated = provider.genesis().await.unwrap_err();
    assert_eq!(gated.http_status, None, "the gated attempt was sent");
    assert_eq!(server.state.count(), 2);
    let sent = provider.sent_by_family();
    assert_eq!(
        sent,
        [("genesis".to_owned(), 1), ("headers".to_owned(), 1)]
            .into_iter()
            .collect()
    );
    assert_eq!(provider.counts(), vec![(day(NOW), 2)]);
}

#[tokio::test]
async fn https_checks_certificate_trust_and_hostname() {
    for names in [vec!["127.0.0.1".to_owned()], vec!["localhost".to_owned()]] {
        let matching = names[0] == "127.0.0.1";
        let server = Server::tls(|_, _| Behavior::json(genesis()), names).await;
        let certificate = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(certificate.path(), server.certificate.as_ref().unwrap()).unwrap();
        let (provider, _) = client(&server.url, |config| {
            config.root_certificate = Some(certificate.path().to_owned())
        });
        let result = provider.genesis().await;
        assert_eq!(result.is_ok(), matching);
        if !matching {
            assert_eq!(result.unwrap_err().code, "provider_unavailable");
        }
    }
    let server = Server::tls(
        |_, _| Behavior::json(genesis()),
        vec!["127.0.0.1".to_owned()],
    )
    .await;
    let (provider, _) = client(&server.url, |_| {});
    assert_eq!(
        provider.genesis().await.unwrap_err().code,
        "provider_unavailable"
    );
}

#[tokio::test]
async fn every_endpoint_reads_its_primary_shape_at_its_path() {
    let pool = pool();
    let block = hash('1');
    let mut by_height = header(1);
    by_height["hash"] = json!(block.to_string());
    let output = json!({"address":"addr1synthetic","output_index":0,
        "amount":[{"unit":"lovelace","quantity":"2"}],"data_hash":null,"inline_datum":null,
        "collateral":false,"reference_script_hash":null,"consumed_by_tx":null});
    let answers: Vec<(String, Value)> = vec![
        ("/blocks/latest".into(), header(1)),
        ("/blocks/1".into(), by_height),
        (
            format!("/blocks/{block}/next?count=100&page=1"),
            json!([header(2)]),
        ),
        (
            format!("/blocks/{block}/txs/cbor?count=100&page=2&order=asc"),
            json!([{"tx_hash": hash('2'), "cbor": "84a0a0f5f6"}]),
        ),
        ("/genesis".into(), genesis()),
        (
            "/epochs/667".into(),
            json!({"epoch": 667, "active_stake": "1000"}),
        ),
        (
            "/epochs/667/parameters".into(),
            json!({"epoch": 667, "nonce": hash('1'), "max_tx_size": 16384, "max_block_size": 90112}),
        ),
        (
            format!("/epochs/667/stakes/{pool}?count=1&page=1"),
            json!([{"stake_address": "stake1synthetic", "amount": "0"}]),
        ),
        (
            "/pools?count=100&page=1&order=asc".into(),
            json!([pool.to_string()]),
        ),
        (
            "/pools/extended?count=100&page=1&order=asc".into(),
            json!([{"pool_id": pool.to_string(), "active_stake": "1000", "metadata": {"anything": true}}]),
        ),
        (
            "/pools/retired?count=100&page=1&order=desc".into(),
            json!([{"pool_id": pool.to_string(), "epoch": 666}]),
        ),
        (
            format!("/pools/{pool}"),
            json!({"pool_id": pool.to_string(), "active_stake": "1000"}),
        ),
        (
            format!("/pools/{pool}/history?count=100&page=1&order=desc"),
            json!([{"epoch": 667, "active_stake": "1000", "delegators_count": 1}]),
        ),
        (
            format!("/scripts/datum/{block}/cbor"),
            json!({"cbor": "19a6aa"}),
        ),
        (
            format!("/txs/{block}/metadata"),
            json!([{"label": "6500973", "json_metadata": ["0x0123"]}]),
        ),
        (
            format!("/txs/{block}/utxos"),
            json!({"hash": block.to_string(), "inputs": [{"ignored": true}], "outputs": [output]}),
        ),
    ];
    let expected = answers.clone();
    let server = Server::start(move |path, index| {
        let (want, body) = &expected[index];
        assert_eq!(path, format!("/api/v0{want}"));
        Behavior::json(body.clone())
    })
    .await;
    let (provider, _) = client(&server.url, |_| {});
    let p = Page::first();
    assert_eq!(provider.latest_block().await.unwrap().height, 1);
    assert_eq!(provider.block_at(1).await.unwrap().unwrap().hash, block);
    assert_eq!(
        provider.next_blocks(&block, p).await.unwrap().unwrap()[0].height,
        2
    );
    let page = provider
        .block_transactions(&block, Page::new(2).unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        page.transactions[0].cbor.0,
        vec![0x84, 0xa0, 0xa0, 0xf5, 0xf6]
    );
    assert_eq!(provider.genesis().await.unwrap().security_param, 2160);
    assert!(provider.epoch(667).await.unwrap().is_some());
    assert_eq!(
        provider
            .epoch_parameters(667)
            .await
            .unwrap()
            .unwrap()
            .max_tx_size,
        16384
    );
    assert_eq!(
        provider.epoch_pool_stake(667, &pool).await.unwrap().len(),
        1
    );
    assert_eq!(provider.pools(p).await.unwrap(), vec![pool.clone()]);
    assert_eq!(
        provider.extended_pools(p).await.unwrap()[0].active_stake.0,
        1000
    );
    assert_eq!(provider.retired_pools(p).await.unwrap()[0].epoch, 666);
    assert!(provider.pool(&pool).await.unwrap().is_some());
    assert_eq!(provider.pool_history(&pool, p).await.unwrap()[0].epoch, 667);
    assert_eq!(
        provider.datum(&block).await.unwrap().unwrap().cbor,
        vec![0x19, 0xa6, 0xaa]
    );
    assert_eq!(
        provider
            .transaction_metadata(&block)
            .await
            .unwrap()
            .label(6_500_973),
        Some("[\"0x0123\"]")
    );
    let outputs = provider.transaction_outputs(&block).await.unwrap();
    assert_eq!(outputs.outputs.len(), 1);
    assert!(outputs.body.is_some());
    assert_eq!(server.state.count(), answers.len());
    let families = [("datums", 1), ("epochs", 3), ("genesis", 1), ("headers", 3)]
        .into_iter()
        .chain([
            ("metadata", 1),
            ("outputs", 1),
            ("pools", 5),
            ("transactions", 1),
        ])
        .map(|(family, count)| (family.to_owned(), count))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(provider.sent_by_family(), families);
}

#[tokio::test]
async fn utxo_answers_stream_past_large_inputs_and_keep_bounded_bodies() {
    let output = json!({"address":"addr1synthetic","output_index":0,"amount":[{"unit":"lovelace","quantity":"2"}],
        "data_hash":null,"inline_datum":null,"collateral":false});
    let answer = |padding: usize, outputs: usize| {
        json!({"hash": hash('1'), "inputs": [{"ignored": "x".repeat(padding)}],
            "outputs": (0..outputs).map(|index| {let mut row = output.clone(); row["output_index"] = json!(index); row}).collect::<Vec<_>>()})
        .to_string()
    };
    let bodies = [answer(2 << 20, 1), answer(9 << 20, 1), answer(0, 100)];
    let served = bodies.clone();
    let server = Server::start(move |_, index| Behavior::raw(served[index].clone())).await;
    let (provider, _) = client(&server.url, |config| {
        config.limits.outputs = 16 << 20;
        config.limits.output_values = 1024;
    });
    let kept = provider.transaction_outputs(&hash('1')).await.unwrap();
    assert_eq!(kept.outputs.len(), 1);
    assert_eq!(kept.body.as_deref(), Some(bodies[0].as_bytes()));
    let large = provider.transaction_outputs(&hash('1')).await.unwrap();
    assert_eq!(large.outputs.len(), 1);
    assert!(large.body.is_none(), "an answer above 8 MiB is not kept");
    assert_eq!(
        provider
            .transaction_outputs(&hash('1'))
            .await
            .unwrap_err()
            .code,
        "provider_response_size_limit"
    );
}

#[tokio::test]
async fn a_scripted_chain_reads_through_the_client() {
    let chain = Arc::new(Mutex::new(Chain::new(100, &[0, 100, 201, 1, 1])));
    let server = Server::with_handler(Chain::handler(chain.clone())).await;
    let (provider, _) = client(&server.url, |_| {});
    let tip = provider.latest_block().await.unwrap();
    assert_eq!(tip.height, 104);
    for (height, pages) in [(101, 1), (102, 3)] {
        let header = provider.block_at(height).await.unwrap().unwrap();
        let before = server.state.count();
        let mut collected = BlockPages::new(header.hash.clone(), header.tx_count).unwrap();
        while let Some(page) = collected.next_page() {
            let page = provider
                .block_transactions(&header.hash, page)
                .await
                .unwrap()
                .unwrap();
            collected.push(page).unwrap();
        }
        let transactions = collected.finish().unwrap();
        assert_eq!(
            server.state.count() - before,
            pages,
            "exactly 100 needs no extra page"
        );
        assert_eq!(transactions.pages.len(), pages);
        assert_eq!(
            dsa_chain::transaction::decode_all(transactions.records())
                .unwrap()
                .len(),
            header.tx_count as usize
        );
    }
    let old_tip = tip.hash.clone();
    let stored_101 = provider.block_at(101).await.unwrap().unwrap().hash;
    chain.lock().unwrap().fork(2);
    assert!(
        provider
            .next_blocks(&old_tip, Page::first())
            .await
            .unwrap()
            .is_none(),
        "the stored tip left the chain"
    );
    let below: Hash32 = provider.block_at(102).await.unwrap().unwrap().hash;
    let replaced = provider
        .next_blocks(&below, Page::first())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replaced.len(), 2);
    assert_ne!(replaced[1].hash, old_tip);
    chain.lock().unwrap().contradict(101);
    assert_ne!(
        provider.block_at(101).await.unwrap().unwrap().hash,
        stored_101,
        "a final block the provider contradicts"
    );
    chain
        .lock()
        .unwrap()
        .fault("/blocks/latest", Behavior::status(429).retry_after("5"), 1);
    let error: Error = provider.latest_block().await.unwrap_err();
    assert_eq!(error.kind, Kind::Wait { until: NOW + 5 });
}
