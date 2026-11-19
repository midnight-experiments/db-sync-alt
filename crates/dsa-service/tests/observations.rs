//! Own benign transport/projection controls, not finalized field witnesses.
#[allow(dead_code)]
#[path = "../../dsa-provider/tests/common/mod.rs"]
mod common;
use common::{Behavior, Server};
use dsa_chain::{checks::Header, observations::Shape, profiles::Profile, transaction};
use dsa_provider::{Endpoint, types::Page};
use dsa_service::observations::{self, ObservedBlock};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

fn bytes(value: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let n = value.len();
    if n < 24 {
        out.push(0x40 + n as u8);
    } else if n < 256 {
        out.extend([0x58, n as u8]);
    } else {
        out.push(0x59);
        out.extend((n as u16).to_be_bytes());
    }
    out.extend(value);
    out
}
struct Fixture {
    profile: Profile,
    hash: String,
    page: String,
    metadata: String,
    raw_datum: Vec<u8>,
    header: Header,
}
fn fixture(size: usize, stored: bool, valid: bool, bridge: bool) -> Fixture {
    let mut address = vec![0x41];
    address.extend([7; 28]);
    address.extend([0, 0, 0]);
    let address_text = transaction::address_text(&address).unwrap();
    let mut profile = Profile::load("preprod").unwrap();
    if stored {
        profile.addresses.insert("ics".into(), address_text);
    }
    // Native Plutus list of zero integers: three-byte array head + N zeros.
    let mut raw_datum = vec![0x99];
    raw_datum.extend(((size - 3) as u16).to_be_bytes());
    raw_datum.resize(size, 0);
    let mut auxiliary = vec![0xa1, 0x1a];
    auxiliary.extend((if bridge { 6500973u32 } else { 2001 }).to_be_bytes());
    auxiliary.extend([0x81, 0x42, 1, 2]);
    let mut output = vec![0xa3, 0];
    output.extend(bytes(&address));
    output.extend([1, 1, 2, 0x82, 1]);
    output.extend(&raw_datum);
    let mut body = vec![0xa4, 0, 0x80, 1, 0x81];
    body.extend(output);
    body.extend([2, 1, 7]);
    body.extend(bytes(&hex::decode(transaction::hash(&auxiliary)).unwrap()));
    let hash = transaction::hash(&body);
    let mut envelope = vec![0x84];
    envelope.extend(body);
    envelope.extend([0xa0, if valid { 0xf5 } else { 0xf4 }]);
    envelope.extend(auxiliary);
    let page = format!(
        " \n{}\n",
        json!([{"tx_hash":hash,"cbor":hex::encode(envelope),"ignored":"preserve me"}])
    );
    let metadata = format!(
        " \n{}\n",
        json!([{"label":if bridge{"6500973"}else{"2001"},"json_metadata":["0x0102"],"ignored":"preserve me too"}])
    );
    Fixture {
        profile,
        hash,
        page,
        metadata,
        raw_datum,
        header: Header {
            height: 10,
            hash: "01".repeat(32),
            previous_hash: Some("02".repeat(32)),
            transaction_count: 1,
        },
    }
}
fn previous() -> Header {
    Header {
        height: 9,
        hash: "02".repeat(32),
        previous_hash: None,
        transaction_count: 0,
    }
}
async fn capture(fixture: &Fixture, pending: &BTreeSet<Shape>) -> ObservedBlock {
    let page = fixture.page.clone();
    let metadata = fixture.metadata.clone();
    let txhash = fixture.hash.clone();
    let server = Server::start(move |path, _| {
        if path.ends_with("/metadata") {
            assert!(path.contains(&txhash));
            Behavior::raw(metadata.clone())
        } else {
            Behavior::raw(page.clone())
        }
    })
    .await;
    let (provider, _, recorder) = common::client(&server.url, |_| {});
    let directory = tempfile::tempdir().unwrap();
    let mut block = ObservedBlock::new(fixture.header.clone()).unwrap();
    block
        .push(
            provider
                .fetch_with_evidence(
                    Endpoint::BlockCbor {
                        block: fixture.header.hash.parse().unwrap(),
                        page: Page::new(1).unwrap(),
                    },
                    pending,
                    directory.path(),
                )
                .await
                .unwrap(),
        )
        .unwrap();
    block
        .push(
            provider
                .fetch_with_evidence(
                    Endpoint::Metadata(fixture.hash.parse().unwrap()),
                    pending,
                    directory.path(),
                )
                .await
                .unwrap(),
        )
        .unwrap();
    assert_eq!(
        recorder.counts.lock().unwrap().len(),
        2,
        "evidence must reuse the two ordinary requests"
    );
    assert!(
        server
            .state
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|request| request.had_key)
    );
    block
}
#[tokio::test]
async fn actual_decoder_and_projection_enforce_boundaries_and_excluded_facts() {
    let pending = Shape::ALL.into_iter().collect();
    for (size, stored, valid, bridge, expected) in [
        (10000, true, true, true, 2),
        (10001, true, true, true, 3),
        (10001, false, true, true, 0),
        (10001, true, false, true, 0),
        (10001, true, true, false, 2),
    ] {
        let fixture = fixture(size, stored, valid, bridge);
        let block = capture(&fixture, &pending).await;
        let mut held = BTreeMap::new();
        let publication = block
            .prepare(&fixture.profile, &previous(), 9, &mut held, &pending)
            .unwrap();
        assert_eq!(
            publication.observation_count(),
            expected,
            "size{size}/stored{stored}/valid{valid}/bridge{bridge}"
        );
        assert!(publication.evidence_limitations().is_empty());
        if size == 10000 {
            assert!(
                publication
                    .observation_shapes()
                    .iter()
                    .all(|shape| *shape != Shape::StoredLargeDatum)
            );
        }
        if !stored || !valid {
            assert!(publication.rows().tx_out.is_empty());
        }
        drop(publication);
        assert!(
            held.is_empty(),
            "uncommitted candidates must not mutate held state"
        );
    }
}

async fn connect(config: &tokio_postgres::Config) -> tokio_postgres::Client {
    let (client, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}
async fn insert_selected(
    transaction: &tokio_postgres::Transaction<'_>,
    rows: &dsa_chain::rows::Rows,
) {
    for row in &rows.tx {
        transaction.execute("INSERT INTO serve.tx(id,hash,block_id,block_index,valid_contract) VALUES($1,decode($2,'hex'),$3,$4,$5)",&[&row.id,&row.hash,&row.block_id,&i32::from(row.block_index),&row.valid_contract]).await.unwrap();
    }
    for row in &rows.tx_out {
        transaction.execute("INSERT INTO serve.tx_out(id,tx_id,index,address,value,data_hash) VALUES($1,$2,$3,$4,$5::text::numeric,decode($6,'hex'))",&[&row.id,&row.tx_id,&(row.index as i16),&row.address,&row.value,&row.data_hash]).await.unwrap();
    }
    for (index, row) in rows.datum.iter().enumerate() {
        let json = row
            .value
            .as_ref()
            .map(|s| serde_json::from_str::<serde_json::Value>(s).unwrap());
        transaction.execute("INSERT INTO serve.datum(id,hash,value) VALUES($1,decode($2,'hex'),$3) ON CONFLICT DO NOTHING",&[&(index as i64+1),&row.hash,&json]).await.unwrap();
    }
    for row in &rows.tx_metadata {
        let json = row
            .json
            .as_ref()
            .map(|s| serde_json::from_str::<serde_json::Value>(s).unwrap());
        transaction
            .execute(
                "INSERT INTO serve.tx_metadata(id,tx_id,key,json) VALUES($1,$2,6500973,$3)",
                &[&row.id, &row.tx_id, &json],
            )
            .await
            .unwrap();
    }
}
#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn publication_raw_witness_status_rollback_retry_restart_are_atomic() {
    let root: tokio_postgres::Config = std::env::var("DSA_TEST_DATABASE").unwrap().parse().unwrap();
    let admin = connect(&root).await;
    let name = format!("observations_{}", std::process::id());
    admin
        .batch_execute(&format!("CREATE DATABASE {name}"))
        .await
        .unwrap();
    let mut config = root.clone();
    config.dbname(&name);
    let mut client = connect(&config).await;
    // The real A2 table definitions are used. No production stack or archive.
    client
        .batch_execute(include_str!("../../dsa-store/src/layout.sql"))
        .await
        .unwrap();
    client.batch_execute("CREATE TABLE dsa.identity(id integer PRIMARY KEY,network text NOT NULL); INSERT INTO dsa.identity VALUES(1,'preprod')").await.unwrap();
    let transaction = client.transaction().await.unwrap();
    observations::migrate(&transaction).await.unwrap();
    transaction.commit().await.unwrap();
    client.batch_execute("CREATE TABLE dsa_follower.commit_failure(id integer PRIMARY KEY,parent integer REFERENCES dsa_follower.commit_failure(id) DEFERRABLE INITIALLY DEFERRED)").await.unwrap();
    let observer = connect(&config).await;
    let fixture = fixture(10001, true, true, true);
    assert_eq!(fixture.raw_datum.len(), 10001);
    let pending = observations::pending(&observations::status(&client).await.unwrap());
    assert_eq!(pending.len(), 3);
    let mut held = BTreeMap::new();
    let mut never_stored = capture(&fixture, &pending)
        .await
        .prepare(&fixture.profile, &previous(), 9, &mut held, &pending)
        .unwrap();
    let transaction = client.transaction().await.unwrap();
    assert_eq!(never_stored.stage_witnesses(&transaction).await.unwrap(), 0);
    transaction.commit().await.unwrap();
    drop(never_stored);
    assert_eq!(
        observations::pending(&observations::status(&client).await.unwrap()).len(),
        3
    );
    let mut failed = capture(&fixture, &pending)
        .await
        .prepare(&fixture.profile, &previous(), 9, &mut held, &pending)
        .unwrap();
    let transaction = client.transaction().await.unwrap();
    insert_selected(&transaction, failed.rows()).await;
    assert_eq!(failed.stage_witnesses(&transaction).await.unwrap(), 3);
    assert_eq!(
        observations::pending(&observations::status(&observer).await.unwrap()).len(),
        3,
        "uncommitted witnesses must be invisible"
    );
    transaction
        .batch_execute("INSERT INTO dsa_follower.commit_failure VALUES(1,2)")
        .await
        .unwrap();
    assert!(transaction.commit().await.is_err());
    drop(failed);
    assert!(held.is_empty());
    let empty = observations::status(&client).await.unwrap();
    assert_eq!(observations::pending(&empty).len(), 3);
    assert_eq!(
        client
            .query_one("SELECT count(*) FROM serve.tx_out", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    assert_eq!(
        client
            .query_one("SELECT count(*) FROM dsa_follower.raw_chunk", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    let mut retry = capture(&fixture, &pending)
        .await
        .prepare(&fixture.profile, &previous(), 9, &mut held, &pending)
        .unwrap();
    let transaction = client.transaction().await.unwrap();
    insert_selected(&transaction, retry.rows()).await;
    assert_eq!(retry.stage_witnesses(&transaction).await.unwrap(), 3);
    transaction.commit().await.unwrap();
    retry.apply_held();
    assert_eq!(held.len(), 1);
    let first = observations::status(&client).await.unwrap();
    assert!(observations::pending(&first).is_empty());
    drop(client);
    client = connect(&config).await;
    assert_eq!(observations::status(&client).await.unwrap(), first);
    let reference = &first[&Shape::StoredLargeDatum]
        .first_stored_observation
        .as_ref()
        .unwrap()
        .sources[0];
    assert_eq!(
        reference.response_sha256,
        format!("{:x}", Sha256::digest(fixture.page.as_bytes()))
    );
    let mut extracted = Vec::new();
    observations::extract_raw(&client, &reference.response_sha256, &mut extracted)
        .await
        .unwrap();
    assert_eq!(extracted, fixture.page.as_bytes());
    assert!(!String::from_utf8(extracted).unwrap().contains(common::KEY));
    let mut duplicate_held = BTreeMap::new();
    let mut duplicate = capture(&fixture, &pending)
        .await
        .prepare(
            &fixture.profile,
            &previous(),
            9,
            &mut duplicate_held,
            &pending,
        )
        .unwrap();
    let transaction = client.transaction().await.unwrap();
    assert_eq!(duplicate.stage_witnesses(&transaction).await.unwrap(), 0);
    transaction.commit().await.unwrap();
    drop(duplicate);
    assert_eq!(observations::status(&client).await.unwrap(), first);
    let no_pending = BTreeSet::new();
    let ignored_directory = tempfile::tempdir().unwrap();
    let bad_path = ignored_directory.path().join("does-not-exist");
    let raw = fixture.page.clone();
    let server = Server::start(move |_, _| Behavior::raw(raw.clone())).await;
    let (provider, _, _) = common::client(&server.url, |_| {});
    let (_, _, raw) = provider
        .fetch_with_evidence(
            Endpoint::BlockCbor {
                block: fixture.header.hash.parse().unwrap(),
                page: Page::new(1).unwrap(),
            },
            &no_pending,
            &bad_path,
        )
        .await
        .unwrap()
        .into_parts();
    assert!(
        raw.is_none(),
        "already witnessed shapes must not open spools or retain raw bodies"
    );
    drop(observer);
    drop(client);
    admin
        .batch_execute(&format!("DROP DATABASE {name} WITH (FORCE)"))
        .await
        .unwrap();
}
