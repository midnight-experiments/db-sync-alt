//! Own benign transport/projection controls, not finalized field witnesses.
#[allow(dead_code)]
#[path = "../../dsa-provider/tests/common/mod.rs"]
mod common;
use common::{Behavior, Server};
use dsa_chain::{checks::Header, observations::Shape, profiles::Profile, transaction};
use dsa_provider::{Endpoint, types::Page};
use dsa_service::observations::{self, InsertedRows, ObservedBlock};
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
    datum_response: Option<Option<String>>,
    outputs_response: Option<String>,
    header: Header,
}
fn fixture(size: usize, stored: bool, valid: bool, bridge: bool) -> Fixture {
    fixture_options(vec![0; size], stored, valid, bridge, "recipient", "inline")
}
fn fixture_options(
    mut raw_datum: Vec<u8>,
    stored: bool,
    valid: bool,
    bridge: bool,
    metadata_kind: &str,
    datum_source: &str,
) -> Fixture {
    let mut address = vec![0x41];
    address.extend([7; 28]);
    address.extend([0, 0, 0]);
    let address_text = transaction::address_text(&address).unwrap();
    let mut profile = Profile::load("preprod").unwrap();
    if stored {
        profile.addresses.insert("ics".into(), address_text);
    }
    // Native Plutus list of zero integers: three-byte array head + N zeros.
    let size = raw_datum.len();
    if raw_datum.iter().all(|b| *b == 0) {
        raw_datum[0] = 0x99;
        raw_datum[1..3].copy_from_slice(&((size - 3) as u16).to_be_bytes());
    }
    let mut auxiliary = vec![0xa1, 0x1a];
    auxiliary.extend((if bridge { 6500973u32 } else { 2001 }).to_be_bytes());
    auxiliary.extend(if metadata_kind == "recipient" {
        vec![0x81, 0x42, 1, 2]
    } else {
        vec![0]
    });
    let mut output = vec![0xa3, 0];
    output.extend(bytes(&address));
    output.extend([1, 1, 2, 0x82]);
    if matches!(datum_source, "endpoint" | "hash_only" | "witness") {
        output.push(0);
        output.extend(bytes(&hex::decode(transaction::hash(&raw_datum)).unwrap()));
    } else {
        output.extend([1, 0xd8, 0x18]);
        output.extend(bytes(&raw_datum));
    }
    if datum_source == "fallback" {
        output[0] = 0xa4;
        output.extend([4, 0]);
    } // benign own unsupported-output control only

    let mut body = if datum_source == "collateral" {
        vec![0xa5, 0, 0x80, 1, 0x80, 2, 1, 7]
    } else {
        let mut body = vec![0xa4, 0, 0x80, 1, 0x81];
        body.extend(output.clone());
        body.extend([2, 1, 7]);
        body
    };
    body.extend(bytes(&hex::decode(transaction::hash(&auxiliary)).unwrap()));
    if datum_source == "collateral" {
        body.push(16);
        body.extend(output);
    }
    let hash = transaction::hash(&body);
    let mut envelope = vec![0x84];
    envelope.extend(body);
    if datum_source == "witness" {
        envelope.extend([0xa1, 4, 0x81]);
        envelope.extend(&raw_datum);
    } else {
        envelope.push(0xa0);
    }
    envelope.push(if valid { 0xf5 } else { 0xf4 });
    envelope.extend(auxiliary);
    let page = format!(
        " \n{}\n",
        json!([{"tx_hash":hash,"cbor":hex::encode(envelope),"ignored":"preserve me"}])
    );
    let value: serde_json::Value = match metadata_kind {
        "recipient" => json!(["0x0102"]),
        "zero" => json!(0),
        "null" => serde_json::Value::Null,
        "nul" => json!("\0"),
        _ => unreachable!(),
    };
    let metadata = format!(
        " \n{}\n",
        json!([{"label":if bridge{"6500973"}else{"2001"},"json_metadata":value,"ignored":"preserve me too"}])
    );
    let datum_response = match datum_source {
        "endpoint" => Some(Some(format!(
            " \n{}\n",
            json!({"cbor":hex::encode(&raw_datum),"ignored":"raw datum response"})
        ))),
        "hash_only" => Some(None),
        _ => None,
    };
    let outputs_response=(datum_source=="fallback").then(||format!(" \n{}\n",json!({"hash":hash,"inputs":[{"ignored":"x".repeat(100_000)}],"outputs":[{"address":transaction::address_text(&address).unwrap(),"output_index":0,"amount":[{"unit":"lovelace","quantity":"1"}],"data_hash":transaction::hash(&raw_datum),"inline_datum":hex::encode(&raw_datum),"collateral":false,"consumed_by_tx":null}]})));
    Fixture {
        profile,
        hash,
        page,
        metadata,
        raw_datum,
        datum_response,
        outputs_response,
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
    let datum_response = fixture.datum_response.clone();
    let outputs_response = fixture.outputs_response.clone();
    let server = Server::start(move |path, _| {
        if path.ends_with("/utxos") {
            Behavior::raw(outputs_response.clone().unwrap())
        } else if path.contains("/scripts/datum/") {
            match &datum_response {
                Some(Some(raw)) => Behavior::raw(raw.clone()),
                Some(None) => Behavior::status(404),
                _ => panic!("unexpected datum request"),
            }
        } else if path.ends_with("/metadata") {
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
    if fixture.datum_response.is_some() {
        block
            .push(
                provider
                    .fetch_with_evidence(
                        Endpoint::Datum(transaction::hash(&fixture.raw_datum).parse().unwrap()),
                        pending,
                        directory.path(),
                    )
                    .await
                    .unwrap(),
            )
            .unwrap();
    }
    if fixture.outputs_response.is_some() {
        block
            .push(
                provider
                    .fetch_with_evidence(
                        Endpoint::TransactionOutputs(fixture.hash.parse().unwrap()),
                        pending,
                        directory.path(),
                    )
                    .await
                    .unwrap(),
            )
            .unwrap();
    }
    assert_eq!(
        recorder.counts.lock().unwrap().len(),
        2 + usize::from(fixture.datum_response.is_some())
            + usize::from(fixture.outputs_response.is_some()),
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
        let mut block = capture(&fixture, &pending).await;
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
) -> InsertedRows {
    let mut inserted = InsertedRows::default();
    for row in &rows.tx {
        transaction.execute("INSERT INTO serve.tx(id,hash,block_id,block_index,valid_contract) VALUES($1,decode($2,'hex'),$3,$4::integer,$5)",&[&row.id,&row.hash,&row.block_id,&i32::from(row.block_index),&row.valid_contract]).await.unwrap();
    }
    for row in &rows.tx_out {
        let returned=transaction.query_one("INSERT INTO serve.tx_out(id,tx_id,index,address,value,data_hash) VALUES($1,$2,$3::smallint,$4,$5::text::numeric,decode($6,'hex')) RETURNING id",&[&row.id,&row.tx_id,&(row.index as i16),&row.address,&row.value,&row.data_hash]).await.unwrap();
        inserted.output_returned(&returned).unwrap();
    }
    for row in &rows.tx_in {
        transaction
            .execute(
                "INSERT INTO serve.tx_in(id,tx_in_id,tx_out_id,tx_out_index) VALUES($1,$2,$3,$4::smallint)",
                &[
                    &row.id,
                    &row.tx_in_id,
                    &row.tx_out_id,
                    &(row.tx_out_index as i16),
                ],
            )
            .await
            .unwrap();
    }
    for (index, row) in rows.datum.iter().enumerate() {
        let json = row
            .value
            .as_ref()
            .map(|s| serde_json::from_str::<serde_json::Value>(s).unwrap());
        if let Some(returned)=transaction.query_opt("INSERT INTO serve.datum(id,hash,value) VALUES($1,decode($2,'hex'),$3) ON CONFLICT DO NOTHING RETURNING hash::bytea AS hash",&[&(index as i64+1),&row.hash,&json]).await.unwrap(){inserted.datum_returned(&returned).unwrap();}
    }
    for row in &rows.tx_metadata {
        let json = row
            .json
            .as_ref()
            .map(|s| serde_json::from_str::<serde_json::Value>(s).unwrap());
        let returned=transaction.query_one("INSERT INTO serve.tx_metadata(id,tx_id,key,json) VALUES($1,$2,6500973,$3) RETURNING id",&[&row.id,&row.tx_id,&json]).await.unwrap();
        inserted.metadata_returned(&returned).unwrap();
    }
    inserted
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
    let mut fixture = fixture_options(vec![0; 10001], true, true, true, "zero", "endpoint");
    let mut page: serde_json::Value = serde_json::from_str(&fixture.page).unwrap();
    page[0]["extra_unknown"] = json!("z".repeat(150_000));
    fixture.page = format!(" \n{}\n", page);
    assert_eq!(fixture.raw_datum.len(), 10001);
    let pending = observations::pending(&observations::status(&client).await.unwrap());
    assert_eq!(pending.len(), 3);
    let mut held = BTreeMap::new();
    let mut captured_block = capture(&fixture, &pending).await;
    let mut never_stored = captured_block
        .prepare(&fixture.profile, &previous(), 9, &mut held, &pending)
        .unwrap();
    let transaction = client.transaction().await.unwrap();
    assert_eq!(
        never_stored
            .stage_witnesses(&transaction, &InsertedRows::default())
            .await
            .unwrap(),
        0
    );
    transaction.commit().await.unwrap();
    drop(never_stored);
    assert_eq!(
        observations::pending(&observations::status(&client).await.unwrap()).len(),
        3
    );
    let mut captured_block = capture(&fixture, &pending).await;
    let mut failed = captured_block
        .prepare(&fixture.profile, &previous(), 9, &mut held, &pending)
        .unwrap();
    let transaction = client.transaction().await.unwrap();
    let inserted = insert_selected(&transaction, failed.rows()).await;
    assert_eq!(
        failed
            .stage_witnesses(&transaction, &inserted)
            .await
            .unwrap(),
        3
    );
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
    // Retry the preserved original capture after COMMIT failure: no re-fetch.
    let retry = captured_block
        .prepare(&fixture.profile, &previous(), 9, &mut held, &pending)
        .unwrap();
    let transaction = client.transaction().await.unwrap();
    let mut transaction = transaction;
    let savepoint = transaction.transaction().await.unwrap();
    let inserted = insert_selected(&savepoint, retry.rows()).await;
    savepoint.commit().await.unwrap();
    assert_eq!(retry.observation_count(), 3);
    retry.commit(transaction, inserted).await.unwrap();
    assert_eq!(held.len(), 1);
    assert_eq!(
        client
            .query_one("SELECT json::text FROM serve.tx_metadata", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "0"
    );
    let first = observations::status(&client).await.unwrap();
    assert!(observations::pending(&first).is_empty());
    drop(client);
    client = connect(&config).await;
    assert_eq!(observations::status(&client).await.unwrap(), first);
    let mut documents: serde_json::Value = serde_json::from_str(include_str!(
        "../../db-sync-alt/schema/status.examples.json"
    ))
    .unwrap();
    documents["following"]["capabilities"] =
        serde_json::to_value(dsa_chain::capabilities::NodeCapabilities::pinned()).unwrap();
    documents["following"]["real_data_evidence"] =
        serde_json::to_value(observations::status(&client).await.unwrap()).unwrap();
    let schema: serde_json::Value =
        serde_json::from_str(include_str!("../../db-sync-alt/schema/status.schema.json")).unwrap();
    jsonschema::validator_for(&schema)
        .unwrap()
        .validate(&documents["following"])
        .unwrap();

    let reference = &first[&Shape::StoredLargeDatum]
        .first_stored_observation
        .as_ref()
        .unwrap()
        .sources[0];
    assert_eq!(
        reference.response_sha256,
        format!("{:x}", Sha256::digest(fixture.page.as_bytes()))
    );
    assert!(
        client
            .query_one(
                "SELECT count(*) FROM dsa_follower.raw_chunk WHERE sha256=$1",
                &[&reference.response_sha256]
            )
            .await
            .unwrap()
            .get::<_, i64>(0)
            >= 3
    );
    let datum_reference = &first[&Shape::StoredLargeDatum]
        .first_stored_observation
        .as_ref()
        .unwrap()
        .sources[1];
    assert_eq!(datum_reference.source, "datum");
    let mut datum_bytes = Vec::new();
    observations::extract_raw(&client, &datum_reference.response_sha256, &mut datum_bytes)
        .await
        .unwrap();
    assert_eq!(
        datum_bytes,
        fixture
            .datum_response
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap()
            .as_bytes()
    );
    let mut extracted = Vec::new();
    observations::extract_raw(&client, &reference.response_sha256, &mut extracted)
        .await
        .unwrap();
    assert_eq!(extracted, fixture.page.as_bytes());
    assert!(!String::from_utf8(extracted).unwrap().contains(common::KEY));
    let mut duplicate_held = BTreeMap::new();
    let mut captured_block = capture(&fixture, &pending).await;
    let mut duplicate = captured_block
        .prepare(
            &fixture.profile,
            &previous(),
            9,
            &mut duplicate_held,
            &pending,
        )
        .unwrap();
    let transaction = client.transaction().await.unwrap();
    assert_eq!(
        duplicate
            .stage_witnesses(&transaction, &InsertedRows::default())
            .await
            .unwrap(),
        0
    );
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

    let mut no_facts_held = BTreeMap::new();
    let mut no_fact_capture = capture(&fixture, &BTreeSet::new()).await;
    let mut no_facts = no_fact_capture
        .prepare(
            &fixture.profile,
            &previous(),
            9,
            &mut no_facts_held,
            &BTreeSet::new(),
        )
        .unwrap();
    let transaction = client.transaction().await.unwrap();
    assert!(transaction.batch_execute("SELECT 1/0").await.is_err());
    assert_eq!(
        no_facts
            .stage_witnesses(&transaction, &InsertedRows::default())
            .await
            .unwrap(),
        0,
        "no candidate means zero SQL even on an aborted transaction"
    );
    drop(transaction);
    drop(no_facts);
    client.execute("UPDATE dsa_follower.raw_chunk SET body=decode('00','hex') WHERE sha256=$1 AND ordinal=0",&[&reference.response_sha256]).await.unwrap();
    assert!(
        observations::extract_raw(&client, &reference.response_sha256, &mut Vec::new())
            .await
            .is_err(),
        "corrupt raw bytes must not pass extraction"
    );
    drop(observer);
    drop(client);
    admin
        .batch_execute(&format!("DROP DATABASE {name} WITH (FORCE)"))
        .await
        .unwrap();
}

fn spend_fixture(original: &Fixture) -> Fixture {
    let mut body = vec![0xa3, 0, 0x81, 0x82];
    body.extend(bytes(&hex::decode(&original.hash).unwrap()));
    body.extend([0, 1, 0x80, 2, 1]);
    let hash = transaction::hash(&body);
    let mut envelope = vec![0x84];
    envelope.extend(body);
    envelope.extend([0xa0, 0xf5, 0xf6]);
    Fixture {
        profile: original.profile.clone(),
        hash: hash.clone(),
        page: json!([{"tx_hash":hash,"cbor":hex::encode(envelope)}]).to_string(),
        metadata: "[]".into(),
        raw_datum: Vec::new(),
        datum_response: None,
        outputs_response: None,
        header: Header {
            height: 11,
            hash: "03".repeat(32),
            previous_hash: Some(original.header.hash.clone()),
            transaction_count: 1,
        },
    }
}
#[tokio::test]
async fn old_inline_missing_origin_is_explicit_without_false_spending_capture() {
    let fixture = fixture(10001, true, true, true);
    let pending = Shape::ALL.into_iter().collect();
    let mut held = BTreeMap::new();
    capture(&fixture, &pending)
        .await
        .prepare(&fixture.profile, &previous(), 9, &mut held, &pending)
        .unwrap()
        .apply_held();
    let spend = spend_fixture(&fixture);
    let mut captured_block = capture(&spend, &pending).await;
    let prepared = captured_block
        .prepare(&spend.profile, &fixture.header, 10, &mut held, &pending)
        .unwrap();
    assert_eq!(
        prepared.evidence_limitations(),
        ["held_inline_origin_unavailable"]
    );
    assert_eq!(prepared.observation_count(), 0);
    assert_eq!(prepared.rows().datum.len(), 1);
    drop(prepared);
    assert_eq!(held.len(), 1, "an uncommitted spend remains staged");
    // A conversion carries the actual stored output fields/hash, not an
    // invented provider preimage. Its verified datum lookup avoids a fetch.
    for output in held.values_mut() {
        output.output.inline_datum = None;
    }
    let mut block = capture(&spend, &pending).await;
    block.known_stored_datum(transaction::hash(&fixture.raw_datum));
    let prepared = block
        .prepare(&spend.profile, &fixture.header, 10, &mut held, &pending)
        .unwrap();
    assert!(prepared.rows().datum.is_empty());
    assert!(prepared.evidence_limitations().is_empty());
    assert_eq!(prepared.observation_count(), 0);
}
#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn already_stored_datum_does_not_claim_new_real_evidence() {
    let root: tokio_postgres::Config = std::env::var("DSA_TEST_DATABASE").unwrap().parse().unwrap();
    let admin = connect(&root).await;
    let name = format!("observations_old_{}", std::process::id());
    admin
        .batch_execute(&format!("CREATE DATABASE {name}"))
        .await
        .unwrap();
    let mut config = root.clone();
    config.dbname(&name);
    let mut client = connect(&config).await;
    client
        .batch_execute(include_str!("../../dsa-store/src/layout.sql"))
        .await
        .unwrap();
    client.batch_execute("CREATE TABLE dsa.identity(id integer PRIMARY KEY,network text NOT NULL); INSERT INTO dsa.identity VALUES(1,'preprod')").await.unwrap();
    let transaction = client.transaction().await.unwrap();
    observations::migrate(&transaction).await.unwrap();
    transaction.commit().await.unwrap();
    let fixture = fixture_options(vec![0; 10001], true, true, true, "recipient", "fallback");
    let pending = Shape::ALL.into_iter().collect();
    let mut held = BTreeMap::new();
    let mut captured_block = capture(&fixture, &pending).await;
    let mut publication = captured_block
        .prepare(&fixture.profile, &previous(), 9, &mut held, &pending)
        .unwrap();
    let datum = &publication.rows().datum[0];
    let value: serde_json::Value = serde_json::from_str(datum.value.as_ref().unwrap()).unwrap();
    client
        .execute(
            "INSERT INTO serve.datum(id,hash,value) VALUES(1,decode($1,'hex'),$2)",
            &[&datum.hash, &value],
        )
        .await
        .unwrap();
    let transaction = client.transaction().await.unwrap();
    let inserted = insert_selected(&transaction, publication.rows()).await;
    assert_eq!(
        publication
            .stage_witnesses(&transaction, &inserted)
            .await
            .unwrap(),
        2
    );
    transaction.commit().await.unwrap();
    publication.apply_held();
    let state = observations::status(&client).await.unwrap();
    assert!(
        state[&Shape::StoredLargeDatum]
            .first_stored_observation
            .is_none()
    );
    assert_eq!(
        client
            .query_one("SELECT count(*) FROM serve.datum", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    let reference = state[&Shape::StoredPointerAddress]
        .first_stored_observation
        .as_ref()
        .unwrap()
        .sources
        .iter()
        .find(|source| source.source == "output_fallback")
        .unwrap();
    let mut raw = Vec::new();
    observations::extract_raw(&client, &reference.response_sha256, &mut raw)
        .await
        .unwrap();
    assert_eq!(raw, fixture.outputs_response.as_ref().unwrap().as_bytes());
    assert_eq!(
        client
            .query_one(
                "SELECT count(*) FROM serve.tx_out o JOIN serve.datum d ON d.hash=o.data_hash",
                &[]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    drop(client);
    admin
        .batch_execute(&format!("DROP DATABASE {name} WITH (FORCE)"))
        .await
        .unwrap();
}

#[tokio::test]
async fn positive_zero_null_placeholders_hash_only_and_distinct_sources() {
    let pending = Shape::ALL.into_iter().collect();
    for kind in ["recipient", "zero", "null", "nul"] {
        let fixture = fixture_options(vec![0; 10001], true, true, true, kind, "inline");
        let mut held = BTreeMap::new();
        let mut captured_block = capture(&fixture, &pending).await;
        let publication = captured_block
            .prepare(&fixture.profile, &previous(), 9, &mut held, &pending)
            .unwrap();
        let positive = matches!(kind, "recipient" | "zero");
        assert_eq!(
            publication
                .observation_shapes()
                .contains(&Shape::StoredBridgeMetadata),
            positive,
            "metadata{kind}"
        );
        assert_eq!(publication.rows().tx_metadata[0].json.is_some(), positive);
        if kind == "zero" {
            assert_eq!(publication.rows().tx_metadata[0].json.as_deref(), Some("0"));
        }
    }
    for source in ["inline", "witness", "endpoint", "fallback", "hash_only"] {
        let fixture = fixture_options(vec![0; 10001], true, true, true, "recipient", source);
        let mut held = BTreeMap::new();
        let mut captured_block = capture(&fixture, &pending).await;
        let publication = captured_block
            .prepare(&fixture.profile, &previous(), 9, &mut held, &pending)
            .unwrap();
        let large = publication
            .observation_candidates()
            .find(|(shape, _)| *shape == Shape::StoredLargeDatum);
        if source == "hash_only" {
            assert!(large.is_none());
            assert!(publication.rows().datum.is_empty());
            assert!(publication.rows().tx_out[0].data_hash.is_some());
        } else {
            let (_, observation) = large.unwrap();
            assert_eq!(observation.raw_datum_bytes, Some(10001));
            let expected = match source {
                "endpoint" => "datum",
                "fallback" => "output_fallback",
                _ => "block_cbor",
            };
            assert_eq!(observation.sources.last().unwrap().source, expected);
        }
    }
    // Raw bytes define the accepted criterion, even when the valid Plutus
    // preimage's rendering exceeds the node-facing JSON depth limit.
    let mut raw = vec![0x81; 10001];
    raw.push(0);
    let fixture = fixture_options(raw, true, true, true, "recipient", "inline");
    let mut held = BTreeMap::new();
    let mut captured_block = capture(&fixture, &pending).await;
    let publication = captured_block
        .prepare(&fixture.profile, &previous(), 9, &mut held, &pending)
        .unwrap();
    assert!(publication.rows().datum[0].value.is_none());
    assert!(
        publication
            .observation_shapes()
            .contains(&Shape::StoredLargeDatum)
    );
}

#[tokio::test]
async fn causal_supplement_retries_preserve_the_one_original_page_capture() {
    let fixture = fixture_options(vec![0; 10001], true, true, true, "recipient", "endpoint");
    let pending = Shape::ALL.into_iter().collect();
    let page = fixture.page.clone();
    let metadata = fixture.metadata.clone();
    let datum = fixture
        .datum_response
        .as_ref()
        .unwrap()
        .as_ref()
        .unwrap()
        .clone();
    let server = Server::start(move |path, _| {
        Behavior::raw(if path.ends_with("/metadata") {
            metadata.clone()
        } else if path.contains("/scripts/datum/") {
            datum.clone()
        } else {
            page.clone()
        })
    })
    .await;
    let (provider, _, recorder) = common::client(&server.url, |_| {});
    let directory = tempfile::tempdir().unwrap();
    let mut block = ObservedBlock::new(fixture.header.clone()).unwrap();
    let mut held = BTreeMap::new();
    block
        .push(
            provider
                .fetch_with_evidence(
                    Endpoint::BlockCbor {
                        block: fixture.header.hash.parse().unwrap(),
                        page: Page::new(1).unwrap(),
                    },
                    &pending,
                    directory.path(),
                )
                .await
                .unwrap(),
        )
        .unwrap();
    match block.prepare(&fixture.profile, &previous(), 9, &mut held, &pending) {
        Err(observations::Error::Build(dsa_chain::rows::BuildError::Required(
            dsa_chain::rows::SupplementRequest::Datum(_),
        ))) => {}
        Err(error) => panic!("expected causal required datum, got {error:?}"),
        Ok(_) => panic!("missing datum accepted"),
    }
    assert!(held.is_empty());
    block
        .push(
            provider
                .fetch_with_evidence(
                    Endpoint::Datum(transaction::hash(&fixture.raw_datum).parse().unwrap()),
                    &pending,
                    directory.path(),
                )
                .await
                .unwrap(),
        )
        .unwrap();
    assert!(matches!(
        block.prepare(&fixture.profile, &previous(), 9, &mut held, &pending),
        Err(observations::Error::Build(
            dsa_chain::rows::BuildError::Required(dsa_chain::rows::SupplementRequest::Metadata(_))
        ))
    ));
    assert!(held.is_empty());
    block
        .push(
            provider
                .fetch_with_evidence(
                    Endpoint::Metadata(fixture.hash.parse().unwrap()),
                    &pending,
                    directory.path(),
                )
                .await
                .unwrap(),
        )
        .unwrap();
    let first = block
        .prepare(&fixture.profile, &previous(), 9, &mut held, &pending)
        .unwrap();
    assert_eq!(first.observation_count(), 3);
    let original = first.rows().clone();
    drop(first);
    let retry = block
        .prepare(&fixture.profile, &previous(), 9, &mut held, &pending)
        .unwrap();
    assert_eq!(retry.rows(), &original);
    assert_eq!(retry.observation_count(), 3);
    drop(retry);
    assert_eq!(recorder.counts.lock().unwrap().len(), 3);
    assert_eq!(
        server
            .state
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request.path.contains("/txs/cbor"))
            .count(),
        1
    );
    assert!(held.is_empty());
}

#[tokio::test]
async fn invalid_collateral_return_stores_pointer_and_raw_datum_only() {
    let fixture = fixture_options(vec![0; 10001], true, false, true, "recipient", "collateral");
    let pending = Shape::ALL.into_iter().collect();
    let mut input = capture(&fixture, &pending).await;
    let mut held = BTreeMap::new();
    let publication = input
        .prepare(&fixture.profile, &previous(), 9, &mut held, &pending)
        .unwrap();
    assert_eq!(publication.rows().tx_out.len(), 1);
    assert_eq!(publication.rows().tx_out[0].index, 0);
    assert!(!publication.rows().tx[0].valid_contract);
    assert!(publication.rows().tx_metadata.is_empty());
    assert_eq!(
        publication.observation_shapes(),
        [Shape::StoredPointerAddress, Shape::StoredLargeDatum]
    );
}
