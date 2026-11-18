#[path = "keep_rule/fixtures.rs"]
mod fixtures;
#[path = "keep_rule/queries.rs"]
mod queries;

use std::{collections::BTreeMap, error::Error as StdError, time::Instant};

use dsa_chain::keep::{Bounds, RetainedRows, TABLES, keep};
use dsa_store::{
    Store,
    keep::{Selection, SourceSchema},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio_postgres::{Client, Config, NoTls, config::SslMode};

use fixtures::{Fixture, Rows, selected};
use queries::{IdBounds, Queries};

type Result<T = ()> = std::result::Result<T, Box<dyn StdError>>;

async fn connect(mut config: Config, schema: &str, readonly: bool) -> Result<Client> {
    config.ssl_mode(SslMode::Disable);
    let (client, connection) = config.connect(NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client.batch_execute(&format!("SET search_path={schema}; SET jit=off; SET statement_timeout='60s'; SET default_transaction_read_only={}",if readonly {"on"}else{"off"})).await?;
    Ok(client)
}

struct Database {
    admin: Client,
    store: Store,
    full: Client,
    kept: Client,
    full_queries: Queries,
    kept_queries: Queries,
    database: String,
    owner: String,
}
impl Database {
    async fn new(profile: &dsa_chain::profiles::Profile) -> Result<Self> {
        let root: Config = std::env::var("DSA_TEST_DATABASE")?.parse()?;
        let admin = connect(root.clone(), "public", false).await?;
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let owner = format!("keep_owner_{}_{nonce:x}", std::process::id());
        let database = format!("keep_database_{}_{nonce:x}", std::process::id());
        admin.batch_execute(&format!("CREATE ROLE {owner} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS")).await?;
        admin
            .batch_execute(&format!("CREATE DATABASE {database} OWNER {owner}"))
            .await?;
        let mut config = root;
        config
            .user(&owner)
            .dbname(&database)
            .ssl_mode(SslMode::Disable);
        let mut store = Store::connect(config.clone(), None).await?;
        store.create(profile).await?;
        store
            .client()
            .batch_execute("SET jit=off; SET statement_timeout='60s'; CREATE SCHEMA kept")
            .await?;
        for table in TABLES {
            store
                .client()
                .batch_execute(&format!(
                    "CREATE TABLE kept.{table} (LIKE serve.{table} INCLUDING ALL)"
                ))
                .await?;
        }
        store.client().batch_execute("UPDATE dsa.position SET serving=true,reason='test',target_height=8,target_hash=decode(repeat('00',32),'hex'),target_time=now(),tip_height=10,tip_hash=decode(repeat('00',32),'hex'),tip_time=now(),horizon_height=6,horizon_hash=decode(repeat('00',32),'hex'),horizon_time=now() WHERE id=1").await?;
        let full = connect(config.clone(), "public", true).await?;
        let kept = connect(config, "kept", true).await?;
        let full_queries = Queries::prepare(&full).await?;
        let kept_queries = Queries::prepare(&kept).await?;
        Ok(Self {
            admin,
            store,
            full,
            kept,
            full_queries,
            kept_queries,
            database,
            owner,
        })
    }

    async fn replace(&self, fixture: &Fixture) -> Result<(Rows, RetainedRows)> {
        let tables = TABLES
            .iter()
            .flat_map(|table| [format!("serve.{table}"), format!("kept.{table}")])
            .collect::<Vec<_>>()
            .join(",");
        self.store
            .client()
            .batch_execute(&format!("TRUNCATE {tables}"))
            .await?;
        self.store
            .client()
            .execute(
                "UPDATE dsa.identity SET network=$1,profile_hash=$2 WHERE id=1",
                &[&fixture.profile.network, &fixture.profile.hash()?],
            )
            .await?;
        self.store
            .client()
            .execute(
                "UPDATE dsa.position SET horizon_height=$1::integer,updated_at=now() WHERE id=1",
                &[&(fixture.bounds.horizon as i32)],
            )
            .await?;
        let rows = fixture.rows();
        load(self.store.client(), "serve", &rows).await?;
        let selection = Selection::new(&fixture.profile, fixture.bounds)?;
        // This adapter projects all eleven row families from the production CTEs.
        let entries=TABLES.iter().map(|table| {
            let key=if *table=="epoch_param" {"epoch_no"}else{"id"};
            format!("'{table}',(SELECT coalesce(jsonb_agg(to_jsonb(s) ORDER BY s.{key}),'[]'::jsonb) FROM serve.{table} s JOIN keep_{table} k ON k.id=s.{key})")
        }).collect::<Vec<_>>().join(",");
        let sql = format!(
            "{} SELECT jsonb_build_object({entries})",
            Selection::ctes_sql(SourceSchema::Serve)
        );
        selection
            .validate_source(self.store.client(), SourceSchema::Serve)
            .await?;
        let row = self
            .store
            .client()
            .query_one(&sql, &selection.parameters())
            .await?;
        let actual: Rows = serde_json::from_value(row.get(0))?;
        load(self.store.client(), "kept", &actual).await?;
        let actual_ids = row_ids(&actual);
        Ok((actual, actual_ids))
    }

    async fn cleanup(self) -> Result {
        drop(self.full);
        drop(self.kept);
        drop(self.store);
        self.admin
            .batch_execute(&format!("DROP DATABASE {} WITH (FORCE)", self.database))
            .await?;
        self.admin
            .batch_execute(&format!("DROP ROLE {}", self.owner))
            .await?;
        Ok(())
    }
}

async fn load(client: &Client, schema: &str, rows: &Rows) -> Result {
    let statements=TABLES.iter().enumerate().map(|(index,table)|format!("insert_{index} AS (INSERT INTO {schema}.{table} SELECT * FROM jsonb_populate_recordset(NULL::{schema}.{table},$1::jsonb->'{table}') RETURNING 1)")).collect::<Vec<_>>().join(",");
    let sql = format!("WITH {statements} SELECT 1");
    let value = serde_json::to_value(rows)?;
    client.query_one(&sql, &[&value]).await?;
    Ok(())
}

fn assert_canonical_ids(fixture: &Fixture) {
    let c = &fixture.chain;
    let tx_key = |id: i64| {
        let tx = &c.transactions[&id];
        (tx.block, tx.index)
    };
    let mut transactions = c.transactions.keys().copied().collect::<Vec<_>>();
    transactions.sort_by_key(|id| tx_key(*id));
    let mut outputs = c.outputs.keys().copied().collect::<Vec<_>>();
    outputs.sort_by_key(|id| {
        let o = &c.outputs[id];
        (tx_key(o.transaction), o.index)
    });
    let tx_hashes: BTreeMap<_, _> = fixture.rows()["tx"]
        .iter()
        .map(|row| {
            (
                row["id"].as_i64().unwrap(),
                row["hash"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    let mut inputs = c.inputs.keys().copied().collect::<Vec<_>>();
    inputs.sort_by_key(|id| {
        let i = &c.inputs[id];
        {
            let origin = &c.outputs[&i.output];
            (
                tx_key(i.transaction),
                tx_hashes[&origin.transaction].clone(),
                origin.index,
            )
        }
    });
    let mut assets = c.output_assets.keys().copied().collect::<Vec<_>>();
    assets.sort_by_key(|id| {
        let a = &c.output_assets[id];
        let o = &c.outputs[&a.output];
        (tx_key(o.transaction), o.index, c.assets[&a.asset].unit())
    });
    for ids in [transactions, outputs, inputs, assets] {
        assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
    }
}

fn row_ids(rows: &Rows) -> RetainedRows {
    rows.iter()
        .map(|(table, rows)| {
            let key = if table == "epoch_param" {
                "epoch_no"
            } else {
                "id"
            };
            (
                table.clone(),
                rows.iter().map(|row| row[key].as_i64().unwrap()).collect(),
            )
        })
        .collect()
}
fn bound_value(value: IdBounds) -> Value {
    json!({"tx":value.tx,"output":value.output,"asset":value.asset,"input":value.input})
}

fn named_cases() -> BTreeMap<&'static str, Fixture> {
    let mut result = BTreeMap::new();
    let mut empty_window = Fixture::generated(101);
    let h = i64::from(empty_window.bounds.horizon);
    empty_window.chain.transactions.retain(|_, tx| tx.block < h);
    empty_window.chain.outputs.retain(|_, output| {
        empty_window
            .chain
            .transactions
            .contains_key(&output.transaction)
    });
    empty_window.chain.inputs.retain(|_, input| {
        empty_window
            .chain
            .transactions
            .contains_key(&input.transaction)
            && empty_window.chain.outputs.contains_key(&input.output)
    });
    empty_window
        .chain
        .output_assets
        .retain(|_, asset| empty_window.chain.outputs.contains_key(&asset.output));
    empty_window
        .chain
        .metadata
        .retain(|_, tx| empty_window.chain.transactions.contains_key(tx));
    result.insert("empty_event_window", empty_window);
    let mut shared = Fixture::generated(3);
    let removed = shared.tags["mapping_0"];
    let retained = shared.tags["mapping_1"];
    let shared_hash = shared.chain.datums[&301].hash.clone();
    shared.chain.outputs.get_mut(&removed).unwrap().datum_hash = Some(shared_hash.clone());
    shared.chain.outputs.get_mut(&retained).unwrap().datum_hash = Some(shared_hash);
    shared
        .chain
        .inputs
        .retain(|_, input| input.output != removed && input.output != retained);
    shared.spend(2, 4, 0, removed);
    shared.reindex_ordinals();
    result.insert("shared_datum_and_creating_transaction", shared);
    let mut fallback = Fixture::generated(1);
    fallback.no_window_spend();
    result.insert("no_spend_in_window", fallback);
    result.insert("spend_of_output_below_horizon", Fixture::generated(2));
    let mut first = Fixture::generated(4);
    first.bounds.horizon = 6;
    result.insert("horizon_first_block_of_epoch", first);
    let mut permissioned = Fixture::generated(5);
    permissioned.bounds.horizon = 6;
    result.insert("permissioned_across_four_epochs", permissioned);
    let mut governance = Fixture::generated(6);
    governance.bounds.horizon = 6;
    governance.missing_governance_datum();
    governance.reindex_ordinals();
    result.insert("missing_governance_datum_preimage", governance);
    let mut empty = Fixture::generated(7);
    empty.empty_epoch();
    result.insert("empty_preceding_epoch", empty);
    let mut null_slot = Fixture::generated(8);
    null_slot.chain.blocks.get_mut(&3).unwrap().slot = None;
    result.insert("null_epoch_end_slot", null_slot);
    result.insert(
        "same_permissioned_policy_other_asset_name",
        Fixture::generated(9),
    );
    let mut advanced = Fixture::generated(10);
    advanced.bounds.horizon = 7;
    result.insert("new_governance_record_below_horizon", advanced);
    let mut null_value = Fixture::generated(12);
    null_value.bounds.horizon = 7;
    let out = null_value.tags["council_6"];
    null_value.chain.datums.insert(
        105,
        dsa_chain::keep::Datum {
            hash: "12".repeat(32),
        },
    );
    null_value.chain.outputs.get_mut(&out).unwrap().datum_hash = Some("12".repeat(32));
    result.insert("governance_null_datum_value", null_value);
    result
}

#[test]
fn named_keep_rule_edges() -> Result {
    for (name, fixture) in named_cases() {
        let kept = keep(&fixture.chain, &fixture.profile, fixture.bounds)?;
        assert_canonical_ids(&fixture);
        let h = i64::from(fixture.bounds.horizon);
        assert!(
            kept["tx_out"].contains(&fixture.tags["old_spent_in_window"]),
            "{name}"
        );
        assert!(!kept["tx_in"].is_empty(), "{name}");
        if name == "no_spend_in_window" || name == "empty_event_window" {
            assert_eq!(kept["tx_in"].len(), 1);
        }
        if name == "missing_governance_datum_preimage" {
            assert!(kept["tx_out"].contains(&fixture.tags["council_4"]));
        }
        if name == "empty_preceding_epoch" {
            assert!(kept["block"].contains(&3));
        }
        if name == "null_epoch_end_slot" {
            assert!(!kept["block"].contains(&3));
        }
        if name == "permissioned_across_four_epochs" {
            assert!(kept["tx_out"].contains(&fixture.tags["permissioned_2"]));
            assert!(kept["tx_out"].contains(&fixture.tags["permissioned_4"]));
        }
        if name == "same_permissioned_policy_other_asset_name" {
            assert!(!kept["tx_out"].contains(&fixture.tags["wrong_permissioned_name"]));
            assert!(!kept["datum"].contains(&505));
            assert!(kept["tx_out"].contains(&fixture.tags["permissioned_2"]));
        }
        if name == "shared_datum_and_creating_transaction" {
            assert!(!kept["tx_out"].contains(&fixture.tags["mapping_0"]));
            assert!(kept["tx_out"].contains(&fixture.tags["mapping_1"]));
            assert!(kept["datum"].contains(&301));
            assert!(kept["tx"].contains(&0));
            assert!(!kept["tx_out"].contains(&fixture.tags["council_0"]));
        }
        if name == "new_governance_record_below_horizon" {
            assert!(kept["tx_out"].contains(&fixture.tags["council_6"]));
            assert!(!kept["tx_out"].contains(&fixture.tags["council_4"]));
        }
        if name == "governance_null_datum_value" {
            assert!(kept["tx_out"].contains(&fixture.tags["council_6"]));
            assert!(kept["datum"].contains(&105));
        }
        assert!(kept["block"].iter().any(|height| *height >= h));
    }
    let fixture = Fixture::generated(1);
    assert!(
        Selection::new(
            &fixture.profile,
            Bounds {
                horizon: 9,
                target: 8,
                tip: 10
            }
        )
        .is_err()
    );
    Ok(())
}

#[derive(Default)]
struct Counts {
    positions: u64,
    helper_differences: u64,
    node_queries: u64,
    helper_examples: Vec<Value>,
}

async fn verify_observations(
    db: &Database,
    fixture: &Fixture,
    kept_ids: &RetainedRows,
) -> Result<Counts> {
    let full_ids = row_ids(&fixture.rows());
    let mut counts = Counts::default();
    let mut low_full = BTreeMap::new();
    let mut low_kept = BTreeMap::new();
    let mut high_full = BTreeMap::new();
    let mut high_kept = BTreeMap::new();
    for block in fixture.bounds.horizon as i32..=fixture.bounds.target as i32 {
        let (lf, lk, hf, hk) = tokio::try_join!(
            queries::bounds(&db.full_queries, &db.full, fixture, &full_ids, block, false),
            queries::bounds(&db.kept_queries, &db.kept, fixture, kept_ids, block, false),
            queries::bounds(&db.full_queries, &db.full, fixture, &full_ids, block, true),
            queries::bounds(&db.kept_queries, &db.kept, fixture, kept_ids, block, true)
        )?;
        if lf != lk {
            counts.helper_examples.push(json!({"seed":fixture.seed,"block":block,"kind":"low","full":bound_value(lf),"kept":bound_value(lk),"sql":queries::node_sql("get_low_bounds")}));
            counts.helper_differences += 1;
        }
        if hf != hk {
            counts.helper_examples.push(json!({"seed":fixture.seed,"block":block,"kind":"high","full":bound_value(hf),"kept":bound_value(hk)}));
            counts.helper_differences += 1;
        }
        low_full.insert(block, lf);
        low_kept.insert(block, lk);
        high_full.insert(block, hf);
        high_kept.insert(block, hk);
        let (full, kept) = tokio::try_join!(
            queries::states(&db.full_queries, &db.full, fixture, block),
            queries::states(&db.kept_queries, &db.kept, fixture, block)
        )?;
        assert_eq!(full, kept, "seed {} state at {block}", fixture.seed);
        counts.node_queries += full.len() as u64 * 2 + 4;
    }
    let wide_low = IdBounds {
        tx: 0,
        output: 0,
        asset: 0,
        input: 0,
    };
    let wide_high = IdBounds {
        tx: i64::MAX,
        output: i64::MAX,
        asset: i64::MAX,
        input: i64::MAX,
    };
    for start in fixture.bounds.horizon as i32..=fixture.bounds.target as i32 {
        for end in start..=fixture.bounds.target as i32 {
            for index in 0..=8_i32 {
                let (full, kept, unbounded_full, unbounded_kept) = tokio::try_join!(
                    queries::events(
                        &db.full_queries,
                        &db.full,
                        fixture,
                        start,
                        index,
                        end,
                        (low_full[&start], high_full[&end])
                    ),
                    queries::events(
                        &db.kept_queries,
                        &db.kept,
                        fixture,
                        start,
                        index,
                        end,
                        (low_kept[&start], high_kept[&end])
                    ),
                    queries::events(
                        &db.full_queries,
                        &db.full,
                        fixture,
                        start,
                        index,
                        end,
                        (wide_low, wide_high)
                    ),
                    queries::events(
                        &db.kept_queries,
                        &db.kept,
                        fixture,
                        start,
                        index,
                        end,
                        (wide_low, wide_high)
                    )
                )?;
                assert_eq!(
                    full, kept,
                    "seed {} cursor({start},{index}) reference {end}",
                    fixture.seed
                );
                assert_eq!(
                    full, unbounded_full,
                    "seed {} full bound guarantee",
                    fixture.seed
                );
                assert_eq!(
                    kept, unbounded_kept,
                    "seed {} kept bound guarantee",
                    fixture.seed
                );
                counts.positions += 1;
                counts.node_queries += 16;
            }
        }
    }
    Ok(counts)
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL 17"]
async fn keep_rule_ten_thousand_chains() -> Result {
    let cases: u64 = std::env::var("DSA_KEEP_CASES")
        .unwrap_or_else(|_| "10000".to_owned())
        .parse()?;
    let start: u64 = std::env::var("DSA_KEEP_SEED_START")
        .unwrap_or_else(|_| "0".to_owned())
        .parse()?;
    let fixture = Fixture::generated(start);
    let db = Database::new(&fixture.profile).await?;
    let begin = Instant::now();
    let mut digest = Sha256::new();
    let mut total = Counts::default();
    for seed in start..start + cases {
        let fixture = Fixture::generated(seed);
        assert_canonical_ids(&fixture);
        let (rows, kept_ids) = db.replace(&fixture).await?;
        assert_eq!(
            rows,
            selected(
                &fixture.rows(),
                &keep(&fixture.chain, &fixture.profile, fixture.bounds)?
            ),
            "seed {seed} SQL/model row mismatch"
        );
        digest.update(seed.to_le_bytes());
        digest.update(serde_json::to_vec(&rows)?);
        let count = verify_observations(&db, &fixture, &kept_ids).await?;
        if total.helper_examples.len() < 3 {
            total.helper_examples.extend(
                count
                    .helper_examples
                    .into_iter()
                    .take(3 - total.helper_examples.len()),
            );
        }
        total.positions += count.positions;
        total.helper_differences += count.helper_differences;
        total.node_queries += count.node_queries;
        if (seed - start + 1).is_multiple_of(100) {
            println!(
                "{}",
                json!({"completed":seed-start+1,"last_seed":seed,"seconds":begin.elapsed().as_secs_f64(),"node_queries":total.node_queries})
            );
        }
    }
    println!(
        "{}",
        json!({"cases":cases,"seed_start":start,"seconds":begin.elapsed().as_secs_f64(),"positions":total.positions,"node_queries":total.node_queries,"helper_id_differences":total.helper_differences,"helper_examples":total.helper_examples,"selected_rows_sha256":format!("{:x}",digest.finalize())})
    );
    db.cleanup().await
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL 17"]
async fn keep_rule_node_regressions() -> Result {
    let db = Database::new(&Fixture::generated(1).profile).await?;
    for (name, fixture) in named_cases() {
        let (_, ids) = db.replace(&fixture).await?;
        // Deliberately compare node observations independently of the model.
        // The required SQL mutation controls must fail here at a node answer.
        verify_observations(&db, &fixture, &ids).await?;
        let full_ids = row_ids(&fixture.rows());
        for block in [0, fixture.bounds.tip as i32] {
            for high in [false, true] {
                queries::bounds(&db.full_queries, &db.full, &fixture, &full_ids, block, high)
                    .await?;
                if ids["block"].contains(&i64::from(block)) {
                    queries::bounds(&db.kept_queries, &db.kept, &fixture, &ids, block, high)
                        .await?;
                } else if high {
                    assert!(
                        db.kept_queries
                            .rows(&db.kept, "get_high_bounds", &[&block])
                            .await?
                            .is_empty()
                    );
                }
            }
        }
        println!("node regression: {name}");
    }
    db.cleanup().await
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL 17"]
async fn keep_rule_selection_entry_points() -> Result {
    let fixture = Fixture::generated(18);
    let db = Database::new(&fixture.profile).await?;
    for (name, fixture) in named_cases() {
        let (rows, _) = db.replace(&fixture).await?;
        assert_eq!(
            rows,
            selected(
                &fixture.rows(),
                &keep(&fixture.chain, &fixture.profile, fixture.bounds)?
            ),
            "{name} SQL/model rows"
        );
    }
    let (expected, ids) = db.replace(&fixture).await?;
    let selection = Selection::new(&fixture.profile, fixture.bounds)?;
    assert_eq!(ids, keep(&fixture.chain, &fixture.profile, fixture.bounds)?);
    for schema in [SourceSchema::Public, SourceSchema::Serve] {
        assert_eq!(selection.collect_ids(db.store.client(), schema).await?, ids);
        for table in TABLES {
            let sql = format!(
                "SELECT to_jsonb(r) FROM ({}) r",
                selection.rows_sql(schema, table)?
            );
            let middle = ids[table]
                .iter()
                .nth(ids[table].len() / 2)
                .copied()
                .unwrap_or(0);
            let mut actual = Vec::new();
            for (lower, upper) in [(0, middle), (middle + 1, i64::MAX)] {
                actual.extend(
                    db.store
                        .client()
                        .query(&sql, &selection.row_parameters(&lower, &upper))
                        .await?
                        .iter()
                        .map(|row| row.get::<_, Value>(0)),
                );
            }
            assert_eq!(
                actual,
                expected[table],
                "{table} {} streamed ranges",
                schema.name()
            );
        }
    }
    assert!(
        selection
            .rows_sql(SourceSchema::Serve, "tx; DROP SCHEMA serve")
            .is_err()
    );
    let absent = Selection::new(
        &fixture.profile,
        Bounds {
            horizon: 12,
            target: 12,
            tip: 14,
        },
    )?;
    assert!(
        absent
            .validate_source(db.store.client(), SourceSchema::Serve)
            .await
            .unwrap_err()
            .to_string()
            .contains("missing_horizon_epoch")
    );
    db.store
        .client()
        .batch_execute("UPDATE dsa.position SET serving=false WHERE id=1")
        .await?;
    assert!(
        selection
            .collect_ids(&db.full, SourceSchema::Public)
            .await
            .unwrap_err()
            .to_string()
            .contains("database")
    );
    db.cleanup().await
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL 17"]
async fn keep_rule_node_spend_regression() -> Result {
    let fixture = Fixture::generated(2);
    let db = Database::new(&fixture.profile).await?;
    db.replace(&fixture).await?;
    let wide = (
        IdBounds {
            tx: 0,
            output: 0,
            asset: 0,
            input: 0,
        },
        IdBounds {
            tx: i64::MAX,
            output: i64::MAX,
            asset: i64::MAX,
            input: i64::MAX,
        },
    );
    let (full, kept) = tokio::try_join!(
        queries::events(
            &db.full_queries,
            &db.full,
            &fixture,
            fixture.bounds.horizon as i32,
            0,
            fixture.bounds.target as i32,
            wide
        ),
        queries::events(
            &db.kept_queries,
            &db.kept,
            &fixture,
            fixture.bounds.horizon as i32,
            0,
            fixture.bounds.target as i32,
            wide
        )
    )?;
    assert!(!full["asset_spends"].is_empty());
    assert_eq!(
        full["asset_spends"], kept["asset_spends"],
        "get_asset_spends of an output below H"
    );
    db.cleanup().await
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL 17"]
async fn keep_rule_partial_order_witnesses() -> Result {
    // This is a schema-valid ambiguity probe, not a claim that the native
    // policies permit two matching outputs in a real Cardano transaction.
    let mut fixture = Fixture::generated(24);
    fixture.bounds.horizon = 6;
    let governance = fixture.output(4, 0, 7, "council", "council", Some(707));
    let old_governance = fixture.tags["council_4"];
    fixture
        .chain
        .inputs
        .retain(|_, input| input.output != old_governance);
    fixture.spend(5, 0, 0, old_governance);
    fixture.spend(5, 0, 1, governance);
    let permissioned = fixture.output(2, 2, 8, "other", "permissioned_candidates", Some(708));
    fixture.spend(4, 2, 5, permissioned);
    fixture.reindex_ordinals();
    assert_canonical_ids(&fixture);
    let db = Database::new(&fixture.profile).await?;
    let (rows, ids) = db.replace(&fixture).await?;
    assert_eq!(
        rows,
        selected(
            &fixture.rows(),
            &keep(&fixture.chain, &fixture.profile, fixture.bounds)?
        )
    );
    for id in [
        governance,
        old_governance,
        permissioned,
        fixture.tags["permissioned_2"],
    ] {
        assert!(ids["tx_out"].contains(&id));
    }
    db.full.batch_execute("SET enable_indexscan=off; SET enable_bitmapscan=off; SET enable_hashjoin=off; SET enable_mergejoin=off; SET join_collapse_limit=1").await?;
    let address = &fixture.profile.addresses["council"];
    let policy = hex::decode(&fixture.profile.assets["council"].policy)?;
    let ident = fixture.asset_id("permissioned_candidates");
    let reference = 5_i32;
    let epoch = 1_i32;
    let full_ids = row_ids(&fixture.rows());
    let mut observations = Vec::new();
    for reversed in [false, true] {
        if reversed {
            let tables = TABLES
                .iter()
                .map(|table| format!("serve.{table}"))
                .collect::<Vec<_>>()
                .join(",");
            db.store
                .client()
                .batch_execute(&format!("TRUNCATE {tables}"))
                .await?;
            let mut rows = fixture.rows();
            for values in rows.values_mut() {
                values.reverse();
            }
            load(db.store.client(), "serve", &rows).await?;
        }
        let low =
            queries::bounds(&db.full_queries, &db.full, &fixture, &full_ids, 6, false).await?;
        let governance = db
            .full_queries
            .rows(
                &db.full,
                "get_governance_body_utxo",
                &[address, &policy, &reference],
            )
            .await?;
        let permissioned = db
            .full_queries
            .rows(&db.full, "get_token_utxo_for_epoch", &[&ident, &epoch])
            .await?;
        assert!([0, 7].contains(&governance[0]["utxo_index"].as_i64().unwrap()));
        assert!([0, 8].contains(&permissioned[0]["utxo_index"].as_i64().unwrap()));
        observations.push(json!({"reversed":reversed,"low":bound_value(low),"governance":governance,"permissioned":permissioned}));
    }
    println!(
        "{}",
        json!({"identical_logical_data_partial_order":observations})
    );
    db.cleanup().await
}
