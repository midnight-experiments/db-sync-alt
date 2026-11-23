//! PostgreSQL proofs use the dedicated Docker fixture in DSA_TEST_DATABASE.
//! The on-demand realistic plan test defaults to 5,000-height setup batches and
//! a recorded 300-second session timeout. DSA_PLAN_DATABASE resumes the exact
//! owned fixture printed at startup, after validating all five row counts.
//! DSA_PLAN_KEEP_DATABASE=1 retains it for another scale; the variant transaction
//! rolls back its input change. DSA_PLAN_TIP/HORIZON select a smaller proof;
//! unset them to finish the full fixture. Batch/timeout overrides are bounded.

#[path = "keep_rule/fixtures.rs"]
mod fixtures;
#[path = "keep_rule/queries.rs"]
mod queries;

use std::{collections::BTreeMap, error::Error as StdError, time::Instant};

use dsa_chain::keep::{Bounds, RetainedRows, TABLES};
use dsa_store::{
    Store,
    keep::{Form, Selection, SourceSchema},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio_postgres::{Client, Config, NoTls, config::SslMode};

use fixtures::{Fixture, Rows, model::keep, selected};
use futures_util::TryStreamExt;
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
    reader: Option<String>,
}
impl Database {
    async fn new(profile: &dsa_chain::profiles::Profile) -> Result<Self> {
        let root: Config = std::env::var("DSA_TEST_DATABASE")?.parse()?;
        let admin = connect(root.clone(), "public", false).await?;
        // Tests of one process run in parallel and may read the same clock
        // value, so a counter makes the names distinct.
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let nonce = format!(
            "{:x}_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        let owner = format!("keep_owner_{}_{nonce}", std::process::id());
        let database = format!("keep_database_{}_{nonce}", std::process::id());
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
            reader: None,
        })
    }

    async fn resume(database: String, profile: &dsa_chain::profiles::Profile) -> Result<Self> {
        if !database.starts_with("keep_database_")
            || !database
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'_')
        {
            return Err("invalid_scratch_database".into());
        }
        let root: Config = std::env::var("DSA_TEST_DATABASE")?.parse()?;
        let admin = connect(root.clone(), "public", false).await?;
        let owner: String = admin
            .query_one(
                "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname=$1",
                &[&database],
            )
            .await?
            .get(0);
        if !owner.starts_with("keep_owner_") {
            return Err("invalid_scratch_owner".into());
        }
        let mut config = root;
        config
            .user(&owner)
            .dbname(&database)
            .ssl_mode(SslMode::Disable);
        let store = Store::connect(config.clone(), None).await?;
        store.open(profile).await?;
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
            reader: None,
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
                "UPDATE dsa.position SET horizon_height=$1::integer,target_height=$2::integer,tip_height=$3::integer,updated_at=now() WHERE id=1",
                &[&(fixture.bounds.horizon as i32),&(fixture.bounds.target as i32),&(fixture.bounds.tip as i32)],
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
            Selection::ctes(Form::Source(SourceSchema::Serve))
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
        if let Some(reader) = self.reader {
            self.admin
                .batch_execute(&format!("DROP ROLE {reader}"))
                .await?;
        }
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
    let mut empty_window = Fixture::base(101);
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
    let mut shared = Fixture::base(3);
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
    let mut fallback = Fixture::base(1);
    fallback.no_window_spend();
    result.insert("no_spend_in_window", fallback);
    result.insert("spend_of_output_below_horizon", Fixture::base(2));
    result.insert("v2_mixed_assets_and_node_read_datums", Fixture::base(42));
    let mut expanded = Fixture::base(42);
    expanded.bounds.horizon = 7;
    expanded.expanded_token_rule_edges();
    result.insert("v2_all_unit_policies_governance_and_datums", expanded);
    let mut first = Fixture::base(4);
    first.bounds.horizon = 6;
    result.insert("horizon_first_block_of_epoch", first);
    let mut permissioned = Fixture::base(5);
    permissioned.bounds.horizon = 6;
    result.insert("permissioned_across_four_epochs", permissioned);
    let mut governance = Fixture::base(6);
    governance.bounds.horizon = 6;
    governance.missing_governance_datum();
    governance.reindex_ordinals();
    result.insert("missing_governance_datum_preimage", governance);
    let mut empty = Fixture::base(7);
    empty.empty_epoch();
    result.insert("empty_preceding_epoch", empty);
    let mut null_slot = Fixture::base(8);
    null_slot.chain.blocks.get_mut(&3).unwrap().slot = None;
    result.insert("null_epoch_end_slot", null_slot);
    result.insert(
        "same_permissioned_policy_other_asset_name",
        Fixture::base(9),
    );
    let mut advanced = Fixture::base(10);
    advanced.bounds.horizon = 7;
    result.insert("new_governance_record_below_horizon", advanced);
    let mut null_value = Fixture::base(12);
    null_value.bounds.horizon = 7;
    let out = null_value.tags["council_6"];
    null_value.chain.datums.insert(
        105,
        fixtures::model::Datum {
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
    let fixture = Fixture::base(1);
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

#[test]
fn version_two_projection_and_datum_boundaries() -> Result {
    for seed in [1, 2] {
        let f = Fixture::base(seed);
        let kept = keep(&f.chain, &f.profile, f.bounds)?;
        let rule = dsa_chain::assets::AssetRule::from_profile(&f.profile);
        for role in ["ics", "reserve", "other"] {
            let output = f.tags[&format!("v2_mixed_{role}")];
            let full = f
                .chain
                .output_assets
                .iter()
                .filter(|(_, a)| a.output == output)
                .map(|(id, a)| {
                    (
                        *id,
                        rule.allows(
                            &f.chain.assets[&a.asset].policy,
                            &f.chain.assets[&a.asset].name,
                        ),
                    )
                })
                .collect::<Vec<_>>();
            assert!(kept["tx_out"].contains(&output));
            assert!(full.iter().any(|(_, allowed)| !*allowed));
            assert!(full.iter().any(|(_, allowed)| *allowed));
            for (id, allowed) in full {
                assert_eq!(kept["ma_tx_out"].contains(&id), allowed);
            }
        }
        let shell = f.tags["v2_zero_eligible_shell"];
        assert!(kept["tx_out"].contains(&shell));
        assert!(
            !kept["ma_tx_out"]
                .iter()
                .any(|id| f.chain.output_assets[id].output == shell)
        );
        let unique = f.tags["v2_plain_governance_unique_datum"];
        assert!(f.chain.outputs[&unique].datum_hash.is_some());
        assert!(kept["tx_out"].contains(&unique));
        assert!(!kept["datum"].contains(&90111));
        assert!(kept["datum"].contains(&90112));
        assert!(kept["multi_asset"].contains(&9012));
        assert!(!kept["multi_asset"].contains(&9010));
        for unit in dsa_chain::assets::fixed_units(&f.profile) {
            let id = f
                .chain
                .assets
                .iter()
                .find(|(_, asset)| asset.unit() == unit)
                .unwrap()
                .0;
            assert!(kept["multi_asset"].contains(id));
        }
        // The old v1 per-output projection kept all these foreign rows. It is
        // observably different and must never be relabelled as version two.
        let v1 = f
            .chain
            .output_assets
            .iter()
            .filter(|(_, a)| kept["tx_out"].contains(&a.output))
            .map(|(id, _)| *id)
            .collect::<std::collections::BTreeSet<_>>();
        assert!(kept["ma_tx_out"].is_subset(&v1));
        assert_ne!(v1, kept["ma_tx_out"]);
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL17"]
async fn keep_rule_v2_literal_governance_and_datum_edges() -> Result {
    for seed in [41, 42] {
        let mut f = Fixture::base(seed);
        f.bounds.horizon = 7;
        f.expanded_token_rule_edges();
        let kept = keep(&f.chain, &f.profile, f.bounds)?;
        for role in [
            "mapping",
            "council",
            "technical_committee",
            "ics",
            "reserve",
        ] {
            let out = f.tags[&format!("v2_datum_only_{role}")];
            assert!(kept["tx_out"].contains(&out));
            let hash = f.chain.outputs[&out].datum_hash.as_ref().unwrap();
            let datum = f
                .chain
                .datums
                .iter()
                .find(|(_, d)| &d.hash == hash)
                .unwrap()
                .0;
            assert!(kept["datum"].contains(datum));
            assert!(
                !kept["ma_tx_out"]
                    .iter()
                    .any(|id| f.chain.output_assets[id].output == out)
            );
        }
        assert!(kept["datum"].contains(&90310));
        for (role, out) in f
            .tags
            .iter()
            .filter(|(tag, _)| tag.starts_with("v2_wrong_name_"))
        {
            assert!(kept["tx_out"].contains(out), "{role}");
            let hash = f.chain.outputs[out].datum_hash.as_ref().unwrap();
            let datum = f
                .chain
                .datums
                .iter()
                .find(|(_, d)| &d.hash == hash)
                .unwrap()
                .0;
            assert!(
                !kept["datum"].contains(datum),
                "wrong unit cannot grant datum retention: {role}"
            );
            assert!(
                !kept["ma_tx_out"]
                    .iter()
                    .any(|id| f.chain.output_assets[id].output == *out),
                "{role}"
            );
        }
        let db = Database::new(&f.profile).await?;
        db.replace(&f).await?;
        for (body_index, body) in ["council", "technical_committee"].into_iter().enumerate() {
            for (kind, reference) in [("carry", 7_i32), ("live", 8_i32)] {
                let output = f.tags[&format!("v2_{body}_{kind}_alt")];
                assert!(kept["tx_out"].contains(&output));
                let tx = f.chain.outputs[&output].transaction;
                let height = f.chain.transactions[&tx].block;
                let rows = f.rows();
                let expected = json!({"full_datum":{"constructor":0,"fields":[{"int":90200+body_index as i64*10+if kind=="live" {1}else{0}}]},"block_number":height,"block_hash":rows["block"].iter().find(|r|r["id"]==height).unwrap()["hash"],"tx_index_in_block":12+body_index*2,"tx_hash":rows["tx"].iter().find(|r|r["id"]==tx).unwrap()["hash"],"utxo_index":20});
                let policy = hex::decode(&fixtures::role_asset(&f.profile, body).policy)?;
                for (queries, client) in
                    [(&db.full_queries, &db.full), (&db.kept_queries, &db.kept)]
                {
                    let answer = queries
                        .rows(
                            client,
                            "get_governance_body_utxo",
                            &[
                                &fixtures::role_address(&f.profile, body).unwrap(),
                                &policy,
                                &reference,
                            ],
                        )
                        .await?;
                    assert_eq!(
                        answer,
                        vec![expected.clone()],
                        "{body}/{kind} literal policy-only answer"
                    );
                }
            }
        }
        verify_observations(&db, &f, &kept).await?;
        db.cleanup().await?;
    }
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
            let max_index = fixture
                .chain
                .transactions
                .values()
                .filter(|tx| tx.block == i64::from(start))
                .map(|tx| i32::from(tx.index) + 1)
                .max()
                .unwrap_or(0);
            for index in 0..=max_index {
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
async fn keep_rule_indexed_stages_match_independent_model() -> Result {
    let initial = Fixture::base(1);
    let db = Database::new(&initial.profile).await?;
    let mut cases = named_cases().into_values().collect::<Vec<_>>();
    cases.extend((0..32).map(Fixture::generated));
    let mut empty_epochs = Fixture::base(17);
    for (height, block) in &mut empty_epochs.chain.blocks {
        if *height < i64::from(empty_epochs.bounds.horizon) {
            block.epoch = Some(0);
        }
    }
    cases.push(empty_epochs);
    // Supported datum-hash outputs may carry a hash without an available preimage.
    // Eligibility must not invent a datum for either address or exact-unit arm.
    for (seed, address, unit) in [
        (18, "mapping", "cnight"),
        (19, "other", "permissioned_candidates"),
    ] {
        let mut missing = Fixture::base(seed);
        missing.output(8, 12, 30, address, unit, Some(987654));
        missing.chain.datums.remove(&987654);
        cases.push(missing);
    }
    let plan = Selection::staging_plan();
    let mut duplicate_epoch_cases = 0;
    for fixture in &cases {
        let (_, full_ids) = db.replace(fixture).await?;
        let model = keep(&fixture.chain, &fixture.profile, fixture.bounds)?;
        assert_eq!(full_ids, model);
        let selection = Selection::new(&fixture.profile, fixture.bounds)?;
        db.store.client().batch_execute("BEGIN").await?;
        for table in TABLES {
            db.store.client().batch_execute(&format!("CREATE TEMP TABLE {table}(LIKE serve.{table} INCLUDING ALL) ON COMMIT DROP;INSERT INTO pg_temp.{table} SELECT * FROM serve.{table}")).await?;
        }
        assert_eq!(plan.len(), 20);
        for stage in &plan {
            db.store
                .client()
                .execute(&stage.create_sql, &selection.parameters())
                .await?;
            db.store.client().batch_execute(&stage.prepare_sql).await?;
        }
        let epoch_counts = db
            .store
            .client()
            .query_one(
                "SELECT count(*),count(DISTINCT id) FROM pg_temp.conversion_keys_epochs",
                &[],
            )
            .await?;
        duplicate_epoch_cases +=
            u64::from(epoch_counts.get::<_, i64>(0) > epoch_counts.get::<_, i64>(1));
        let mut staged = TABLES
            .iter()
            .map(|table| (table.to_string(), std::collections::BTreeSet::new()))
            .collect::<RetainedRows>();
        for row in db
            .store
            .client()
            .query(
                "SELECT family,id FROM pg_temp.conversion_keep_keys ORDER BY family,id",
                &[],
            )
            .await?
        {
            staged
                .get_mut(row.get::<_, &str>(0))
                .unwrap()
                .insert(row.get(1));
        }
        assert_eq!(staged, model, "indexed stages seed{}", fixture.seed);
        db.store.client().batch_execute("ROLLBACK").await?;
        let temporary_names = plan
            .iter()
            .map(|stage| format!("pg_temp.{}", stage.table))
            .chain(TABLES.iter().map(|table| format!("pg_temp.{table}")))
            .collect::<Vec<_>>();
        assert_eq!(db.store.client().query_one("SELECT count(*) FROM unnest($1::text[]) name WHERE to_regclass(name) IS NOT NULL",&[&temporary_names]).await?.get::<_,i64>(0),0);
    }
    assert!(duplicate_epoch_cases > 0);
    println!(
        "{}",
        json!({"indexed_stage_cases":cases.len(),"stages":20,"family_comparisons":cases.len()*TABLES.len(),"duplicate_epoch_cases":duplicate_epoch_cases,"oracle":"independent pure model and original all11 CTEs","rollback_cleanup_verified":true,"temporary_tables_checked":plan.len()+TABLES.len(),"ordered_staging_sql":plan.iter().map(|stage|json!({"table":stage.table,"create_sha256":format!("{:x}",Sha256::digest(stage.create_sql.as_bytes())),"prepare_sha256":format!("{:x}",Sha256::digest(stage.prepare_sql.as_bytes()))})).collect::<Vec<_>>()})
    );
    db.cleanup().await
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
    let mut topologies = std::collections::BTreeSet::new();
    let mut empty_windows = 0_u64;
    let mut null_epoch_cases = 0_u64;
    let mut post_target_cases = 0_u64;
    let mut varied_quantities = 0_u64;
    for seed in start..start + cases {
        let fixture = Fixture::generated(seed);
        assert_canonical_ids(&fixture);
        let retained = keep(&fixture.chain, &fixture.profile, fixture.bounds)?;
        let shared = dsa_chain::assets::AssetRule::from_profile(&fixture.profile);
        for (id, row) in &fixture.chain.output_assets {
            let asset = &fixture.chain.assets[&row.asset];
            assert_eq!(
                retained["ma_tx_out"].contains(id),
                retained["tx_out"].contains(&row.output)
                    && shared.allows(&asset.policy, &asset.name),
                "shared predicate seed{seed} asset{id}"
            );
        }
        let b = fixture.bounds;
        topologies.insert((
            b.horizon,
            b.target,
            b.tip,
            fixture.chain.blocks[&i64::from(b.horizon)].epoch.unwrap() - 2,
        ));
        empty_windows += u64::from(
            !fixture
                .chain
                .transactions
                .values()
                .any(|tx| tx.block >= i64::from(b.horizon) && tx.block <= i64::from(b.target)),
        );
        null_epoch_cases += u64::from(
            fixture
                .chain
                .blocks
                .values()
                .any(|header| header.epoch.is_none()),
        );
        post_target_cases += u64::from(
            fixture
                .chain
                .transactions
                .values()
                .any(|tx| tx.block > i64::from(b.target)),
        );
        varied_quantities += u64::from(fixture.quantities.values().any(|quantity| *quantity != 1));
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
        json!({"generator_coverage":{"distinct_bounds_epoch_cutoffs":topologies.len(),"minimum_tip":topologies.iter().map(|x|x.2).min(),"maximum_tip":topologies.iter().map(|x|x.2).max(),"minimum_epoch_cutoff":topologies.iter().map(|x|x.3).min(),"maximum_epoch_cutoff":topologies.iter().map(|x|x.3).max(),"empty_event_windows":empty_windows,"null_epoch_cases":null_epoch_cases,"post_target_cases":post_target_cases,"non_unit_quantities":varied_quantities}})
    );
    println!(
        "{}",
        json!({"cases":cases,"seed_start":start,"seconds":begin.elapsed().as_secs_f64(),"positions":total.positions,"node_queries":total.node_queries,"helper_id_differences":total.helper_differences,"helper_examples":total.helper_examples,"selected_rows_sha256":format!("{:x}",digest.finalize())})
    );
    db.cleanup().await
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL 17"]
async fn keep_rule_node_regressions() -> Result {
    let db = Database::new(&Fixture::base(1).profile).await?;
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
    let fixture = Fixture::base(18);
    let mut db = Database::new(&fixture.profile).await?;
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
        assert_eq!(
            selection
                .collect_ids(db.store.client(), Form::Source(schema))
                .await?,
            ids
        );
        for table in TABLES {
            let sql = format!(
                "SELECT to_jsonb(r) FROM ({}) r",
                selection.rows_sql(Form::Source(schema), table)?
            );
            let actual = db
                .store
                .client()
                .query(&sql, &selection.parameters())
                .await?
                .iter()
                .map(|row| row.get::<_, Value>(0))
                .collect::<Vec<_>>();
            assert_eq!(
                actual,
                expected[table],
                "{table} {} one source query",
                schema.name()
            );
            let stream = selection
                .stream_rows(db.store.client(), Form::Source(schema), table)
                .await?;
            tokio::pin!(stream);
            let mut count = 0;
            while let Some(row) = stream.try_next().await? {
                assert_eq!(
                    row.columns().len(),
                    expected[table][0].as_object().unwrap().len()
                );
                count += 1;
            }
            assert_eq!(count, expected[table].len());
        }
    }
    let reader = format!("{}_reader", db.owner);
    db.admin.batch_execute(&format!("CREATE ROLE {reader} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS; ALTER ROLE {reader} SET default_transaction_read_only=on")).await?;
    db.store.grant_reader(&fixture.profile, &reader).await?;
    let mut config: Config = std::env::var("DSA_TEST_DATABASE")?.parse()?;
    config.user(&reader).dbname(&db.database);
    let reader_client = connect(config, "public", true).await?;
    assert!(
        !reader_client
            .query_one(
                "SELECT has_database_privilege(current_user,current_database(),'TEMP')",
                &[]
            )
            .await?
            .get::<_, bool>(0)
    );
    queries::reader_probes(&reader_client).await?;
    for table in TABLES {
        let stream = selection
            .stream_rows(&reader_client, Form::Source(SourceSchema::Public), table)
            .await?;
        tokio::pin!(stream);
        let mut count = 0;
        while stream.try_next().await?.is_some() {
            count += 1;
        }
        assert_eq!(count, expected[table].len());
    }
    drop(reader_client);
    db.reader = Some(reader);
    assert!(
        selection
            .rows_sql(Form::Source(SourceSchema::Serve), "tx; DROP SCHEMA serve")
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
            .collect_ids(&db.full, Form::Source(SourceSchema::Public))
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
    let fixture = Fixture::base(2);
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
    // A schema-only ambiguity control. The deployed singleton-token policies
    // exclude these ties on a valid chain; selection still preserves all ties.
    let mut fixture = Fixture::base(24);
    fixture.bounds.horizon = 6;
    fixture.chain.blocks.get_mut(&3).unwrap().slot = fixture.chain.blocks[&2].slot;
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
    let address = &fixture.profile.addresses.council;
    let policy = hex::decode(&fixture.profile.assets.council.policy)?;
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

#[tokio::test]
#[ignore = "requires isolated PostgreSQL 17"]
async fn keep_rule_height_limit_and_collector_guard() -> Result {
    let mut fixture = Fixture::base(42);
    fixture.translate_heights(dsa_chain::ids::HEIGHT_LIMIT - 11);
    for height in [fixture.bounds.target, fixture.bounds.tip] {
        let output = fixture.output(height, 4095, 4095, "other", "cnight", None);
        fixture
            .chain
            .output_assets
            .retain(|_, row| row.output != output);
        fixture.chain.output_assets.insert(
            dsa_chain::ids::asset_id(height, 4095, 4095, 4095)?,
            fixtures::model::OutputAsset {
                output,
                asset: fixture.asset_id("cnight"),
            },
        );
    }
    assert_canonical_ids(&fixture);
    let db = Database::new(&fixture.profile).await?;
    let (rows, _) = db.replace(&fixture).await?;
    assert_eq!(
        rows,
        selected(
            &fixture.rows(),
            &keep(&fixture.chain, &fixture.profile, fixture.bounds)?
        )
    );
    let selection = Selection::new(&fixture.profile, fixture.bounds)?;
    for table in TABLES {
        let stream = selection
            .stream_rows(db.store.client(), Form::Source(SourceSchema::Serve), table)
            .await?;
        tokio::pin!(stream);
        let mut count = 0;
        while stream.try_next().await?.is_some() {
            count += 1;
        }
        assert_eq!(count, rows[table].len());
    }
    assert!(
        Selection::new(
            &fixture.profile,
            Bounds {
                horizon: 1,
                target: 2,
                tip: 5
            }
        )
        .unwrap_err()
        .to_string()
        .contains("invalid_keep_target")
    );
    db.replace(&Fixture::base(42)).await?;
    db.store.client().batch_execute("INSERT INTO serve.block SELECT i,decode(md5(i::text)||md5(i::text),'hex'),0,i,i,timestamp '2026-01-01',0 FROM generate_series(11,100002) i").await?;
    let selection = Selection::new(
        &fixture.profile,
        Bounds {
            horizon: 0,
            target: 100000,
            tip: 100002,
        },
    )?;
    assert!(
        selection
            .collect_ids(db.store.client(), Form::Source(SourceSchema::Serve))
            .await
            .unwrap_err()
            .to_string()
            .contains("keep_validation_too_large")
    );
    let stream = selection
        .stream_rows(
            db.store.client(),
            Form::Source(SourceSchema::Serve),
            "block",
        )
        .await?;
    tokio::pin!(stream);
    let mut count = 0;
    while stream.try_next().await?.is_some() {
        count += 1;
    }
    assert_eq!(count, 100003);
    db.cleanup().await
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL 17"]
async fn keep_rule_page_and_bridge_checkpoints() -> Result {
    let db = Database::new(&Fixture::base(2).profile).await?;
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
    for fixture in [
        Fixture::base(2),
        Fixture::generated(3),
        Fixture::generated(4),
    ] {
        db.replace(&fixture).await?;
        let h = fixture.bounds.horizon as i32;
        let t = fixture.bounds.target as i32;
        for end in h..=t {
            let max = fixture
                .chain
                .transactions
                .values()
                .filter(|tx| tx.block == i64::from(end))
                .map(|tx| i32::from(tx.index) + 1)
                .max()
                .unwrap_or(0);
            for end_index in 0..=max {
                let range = (h, 0, end, end_index);
                let all = queries::paged_events(
                    &db.full_queries,
                    &db.full,
                    &fixture,
                    range,
                    (1000, 0),
                    wide,
                )
                .await?;
                for (limit, offset) in [(1, 0), (1, 1), (2, 1), (1, 1000)] {
                    let (full, kept) = tokio::try_join!(
                        queries::paged_events(
                            &db.full_queries,
                            &db.full,
                            &fixture,
                            range,
                            (limit, offset),
                            wide
                        ),
                        queries::paged_events(
                            &db.kept_queries,
                            &db.kept,
                            &fixture,
                            range,
                            (limit, offset),
                            wide
                        )
                    )?;
                    assert_eq!(full, kept, "end/page cut {range:?} {limit}/{offset}");
                    for (name, rows) in &full {
                        assert_eq!(
                            *rows,
                            all[name]
                                .iter()
                                .skip(offset as usize)
                                .take(limit as usize)
                                .cloned()
                                .collect::<Vec<_>>(),
                            "{name} pagination"
                        );
                    }
                }
            }
        }
        let ics = &fixture.profile.addresses.ics;
        let reserve = &fixture.profile.addresses.reserve;
        let policy = hex::decode(&fixture.profile.assets.bridge.policy)?;
        let name = hex::decode(&fixture.profile.assets.bridge.name)?;
        let metadata = 6500973_i64;
        for block in h..=t {
            let max = fixture
                .chain
                .transactions
                .values()
                .filter(|tx| tx.block == i64::from(block))
                .map(|tx| i32::from(tx.index) + 1)
                .max()
                .unwrap_or(0);
            let mut checkpoints = vec![
                queries::Checkpoint::Block(block - 1),
                queries::Checkpoint::Block(block),
            ];
            for index in 0..=max {
                checkpoints.extend([
                    queries::Checkpoint::Tx(block, index),
                    queries::Checkpoint::TxReserveTransfer(block, index),
                ]);
            }
            for checkpoint in checkpoints {
                let params: [&(dyn tokio_postgres::types::ToSql + Sync); 6] =
                    [ics, reserve, &policy, &name, &metadata, &t];
                let all = db
                    .full_queries
                    .bridge_rows(&db.full, checkpoint, None, &params)
                    .await?;
                for limit in [None, Some(1), Some(2)] {
                    let (full, kept) = tokio::try_join!(
                        db.full_queries
                            .bridge_rows(&db.full, checkpoint, limit, &params),
                        db.kept_queries
                            .bridge_rows(&db.kept, checkpoint, limit, &params)
                    )?;
                    assert_eq!(full, kept, "bridge {checkpoint:?} {limit:?}");
                    assert_eq!(
                        full,
                        all.iter()
                            .take(limit.map_or(usize::MAX, |n| n as usize))
                            .cloned()
                            .collect::<Vec<_>>()
                    );
                }
            }
        }
    }
    db.cleanup().await
}

#[tokio::test]
#[ignore = "on-demand realistic PostgreSQL plan check"]
async fn keep_rule_realistic_plans() -> Result {
    let profile = dsa_chain::profiles::Profile::load("mainnet")?;
    let mut db = if let Ok(database) = std::env::var("DSA_PLAN_DATABASE") {
        Database::resume(database, &profile).await?
    } else {
        Database::new(&profile).await?
    };
    let horizon: u32 = std::env::var("DSA_PLAN_HORIZON")
        .unwrap_or_else(|_| "1200000".to_owned())
        .parse()?;
    let tip: u32 = std::env::var("DSA_PLAN_TIP")
        .unwrap_or_else(|_| (1_200_000 + 30_009 + profile.cardano.security_parameter).to_string())
        .parse()?;
    let target = tip
        .checked_sub(profile.cardano.security_parameter)
        .ok_or("invalid_plan_tip")?;
    let timeout: u32 = std::env::var("DSA_PLAN_TIMEOUT_SECONDS")
        .unwrap_or_else(|_| "300".to_owned())
        .parse()?;
    let batch: u32 = std::env::var("DSA_PLAN_BATCH_HEIGHTS")
        .unwrap_or_else(|_| "5000".to_owned())
        .parse()?;
    if !(1..=600).contains(&timeout) || !(1..=20000).contains(&batch) {
        return Err("invalid_plan_resource_bound".into());
    }
    let selection = Selection::new(
        &profile,
        Bounds {
            horizon,
            target,
            tip,
        },
    )?;
    // Setup is synthetic and owned. Its checkpoint-paced writes can take longer
    // than a small fixture; this session-only timeout is recorded explicitly.
    db.store
        .client()
        .batch_execute(&format!("SET statement_timeout='{timeout}s'; SET jit=off"))
        .await?;
    let last: i64 = db
        .store
        .client()
        .query_one(
            "SELECT coalesce(max(id),-1) FROM serve.block WHERE id BETWEEN 0 AND $1",
            &[&i64::from(tip)],
        )
        .await?
        .get(0);
    let verify_prefix = |last: i64| {
        let client = db.store.client();
        async move {
            let counts=client.query_one("SELECT (SELECT count(*) FROM serve.block),(SELECT count(*) FROM serve.tx),(SELECT count(*) FROM serve.tx_out),(SELECT count(*) FROM serve.ma_tx_out),(SELECT count(*) FROM serve.tx_in)",&[]).await?;
            let count = last + 1;
            let spends = last.clamp(0, 1_199_999);
            let expected = [
                count,
                count * 2,
                count * 2,
                count * 2,
                2 * (spends - spends / 10),
            ];
            for (index, expected) in expected.into_iter().enumerate() {
                assert_eq!(
                    counts.get::<_, i64>(index),
                    expected,
                    "resume family {index}"
                );
            }
            Ok::<_, Box<dyn StdError>>(())
        }
    };
    verify_prefix(last).await?;
    println!(
        "{}",
        json!({"realistic_database":db.database,"owner":db.owner,"resume_last_height":last,"horizon":horizon,"target":target,"tip":tip,"batch_heights":batch,"statement_timeout_seconds":timeout})
    );
    let started = Instant::now();
    for first in ((last + 1) as u32..=tip).step_by(batch as usize) {
        let last = (first + batch - 1).min(tip);
        db.store.client().batch_execute(&format!("INSERT INTO serve.block SELECT i,decode(md5(i::text)||md5(i::text),'hex'),i/21600,i*20,i,timestamp '2026-01-01'+i*interval '20 seconds',3 FROM generate_series({first}::bigint,{last}::bigint) i; INSERT INTO serve.tx SELECT i*4096+j,decode(md5((i*4096+j)::text)||md5((i*4096+j)::text),'hex'),i,j,true FROM generate_series({first}::bigint,{last}::bigint) i CROSS JOIN (VALUES(0),(2)) ordinal(j); INSERT INTO serve.tx_out SELECT (i*4096+j)*4096,i*4096+j,0,'external',1,NULL,NULL FROM generate_series({first}::bigint,{last}::bigint) i CROSS JOIN (VALUES(0),(2)) ordinal(j); INSERT INTO serve.ma_tx_out SELECT ((i*4096+j)*4096)*4096,1,(i*4096+j)*4096,1 FROM generate_series({first}::bigint,{last}::bigint) i CROSS JOIN (VALUES(0),(2)) ordinal(j); INSERT INTO serve.tx_in SELECT (i*4096)*4096+j/2,i*4096,(i-1)*4096+j,0 FROM generate_series({first}::bigint,{last}::bigint) i CROSS JOIN (VALUES(0),(2)) ordinal(j) WHERE i>0 AND i<1200000 AND i%10<>0")).await?;
        println!(
            "{}",
            json!({"realistic_setup_last_height":last,"seconds":started.elapsed().as_secs_f64()})
        );
    }
    verify_prefix(i64::from(tip)).await?;
    let asset = &profile.assets.cnight;
    db.store
        .client()
        .execute(
            "INSERT INTO serve.multi_asset VALUES(1,$1::bytea,$2::bytea) ON CONFLICT(id) DO NOTHING",
            &[&hex::decode(&asset.policy)?, &hex::decode(&asset.name)?],
        )
        .await?;
    db.store.client().batch_execute("ANALYZE serve.block; ANALYZE serve.tx; ANALYZE serve.tx_out; ANALYZE serve.tx_in; ANALYZE serve.ma_tx_out; ANALYZE serve.multi_asset").await?;
    let settings=db.store.client().query_one("SELECT current_setting('statement_timeout'),current_setting('work_mem'),current_setting('max_wal_size'),current_setting('max_parallel_workers_per_gather'),current_setting('jit'),current_setting('server_version'),current_setting('block_size')",&[]).await?;
    println!(
        "{}",
        json!({"realistic_setup_seconds":started.elapsed().as_secs_f64(),"source_headers":tip+1,"source_outputs":(tip+1)*2,"source_inputs":2*((tip.min(1_199_999))-(tip.min(1_199_999))/10),"statement_timeout":settings.get::<_,String>(0),"work_mem":settings.get::<_,String>(1),"max_wal_size":settings.get::<_,String>(2),"parallel_workers_per_gather":settings.get::<_,String>(3),"jit":settings.get::<_,String>(4),"server_version":settings.get::<_,String>(5),"block_size":settings.get::<_,String>(6)})
    );
    let lower = i64::from(horizon) * 16_777_216;
    let upper = i64::from(target) * 16_777_216 + 16_777_215;
    let window: bool = db
        .store
        .client()
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM serve.tx_in WHERE id BETWEEN $1 AND $2)",
            &[&lower, &upper],
        )
        .await?
        .get(0);
    async fn plans<C: tokio_postgres::GenericClient>(
        client: &C,
        selection: &Selection,
        window: bool,
        outputs: u32,
    ) -> Result {
        for table in ["block", "tx_out", "tx_in"] {
            for forced in [false, true] {
                client
                    .batch_execute(if forced {
                        "SET LOCAL enable_seqscan=off"
                    } else {
                        "SET LOCAL enable_seqscan=on"
                    })
                    .await?;
                let sql = format!(
                    "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT count(*) FROM ({}) rows",
                    selection.rows_sql(Form::Source(SourceSchema::Serve), table)?
                );
                let begin = Instant::now();
                let plan: Value = client
                    .query_one(&sql, &selection.parameters())
                    .await?
                    .get(0);
                fn scan(value: &Value, max_loops: &mut f64, fallback: &mut Option<Value>) {
                    if let Some(object) = value.as_object() {
                        assert_ne!(
                            object.get("Scan Direction").and_then(Value::as_str),
                            Some("Backward")
                        );
                        *max_loops = (*max_loops).max(
                            object
                                .get("Actual Loops")
                                .and_then(Value::as_f64)
                                .unwrap_or(0.0),
                        );
                        if object.get("Subplan Name").and_then(Value::as_str)
                            == Some("CTE fallback_candidates")
                        {
                            *fallback = Some(value.clone());
                        }
                        for child in object.values() {
                            scan(child, max_loops, fallback);
                        }
                    } else if let Some(array) = value.as_array() {
                        for child in array {
                            scan(child, max_loops, fallback);
                        }
                    }
                }
                let mut max_loops = 0.0;
                let mut fallback = None;
                scan(&plan, &mut max_loops, &mut fallback);
                assert!(
                    max_loops <= f64::from(outputs) * 3.0 + 1000.0,
                    "superlinear inner loops"
                );
                fn fallback_scans(value: &Value, rows: &mut Vec<Value>) {
                    if value.get("Relation Name").and_then(Value::as_str) == Some("tx_in") {
                        rows.push(
                            json!({"rows":value["Actual Rows"],"loops":value["Actual Loops"]}),
                        );
                    }
                    if let Some(object) = value.as_object() {
                        for child in object.values() {
                            fallback_scans(child, rows);
                        }
                    } else if let Some(array) = value.as_array() {
                        for child in array {
                            fallback_scans(child, rows);
                        }
                    }
                }
                let mut reads = Vec::new();
                if let Some(node) = &fallback {
                    fallback_scans(node, &mut reads);
                }
                if table != "block" {
                    assert!(!reads.is_empty());
                    for read in &reads {
                        assert_eq!(
                            read["loops"].as_f64().unwrap() > 0.0,
                            !window,
                            "fallback source branch"
                        );
                    }
                }
                fn work(value: &Value, rows: &mut Vec<Value>) {
                    if let Some(object) = value.as_object() {
                        if object.contains_key("Node Type") {
                            let loops = object
                                .get("Actual Loops")
                                .and_then(Value::as_f64)
                                .unwrap_or(0.0);
                            let actual = object
                                .get("Actual Rows")
                                .and_then(Value::as_f64)
                                .unwrap_or(0.0);
                            let filtered = [
                                "Rows Removed by Filter",
                                "Rows Removed by Join Filter",
                                "Rows Removed by Index Recheck",
                            ]
                            .iter()
                            .filter_map(|key| object.get(*key).and_then(Value::as_f64))
                            .sum::<f64>();
                            rows.push(json!({"node":object.get("Node Type"),"relation":object.get("Relation Name"),"alias":object.get("Alias"),"rows_times_loops":actual*loops,"filtered_rows_times_loops":filtered*loops,"approximate_row_work":(actual+filtered)*loops}));
                        }
                        for child in object.values() {
                            work(child, rows);
                        }
                    } else if let Some(array) = value.as_array() {
                        for child in array {
                            work(child, rows);
                        }
                    }
                }
                let mut row_work = Vec::new();
                work(&plan, &mut row_work);
                let mut totals: BTreeMap<String, (f64, f64)> = BTreeMap::new();
                for node in &row_work {
                    if let Some(relation) = node["relation"].as_str() {
                        let total = totals.entry(relation.to_owned()).or_default();
                        total.0 += node["rows_times_loops"].as_f64().unwrap();
                        total.1 += node["filtered_rows_times_loops"].as_f64().unwrap();
                    }
                }
                let relation_work:BTreeMap<_,_>=totals.into_iter().map(|(name,(rows,filtered))|(name,json!({"rows_times_loops":rows,"filtered_rows_times_loops":filtered,"approximate_row_work":rows+filtered}))).collect();
                println!(
                    "{}",
                    json!({"realistic_plan_family":table,"window_has_spend":window,"forced_index_plan":forced,"elapsed_seconds":begin.elapsed().as_secs_f64(),"max_inner_loops":max_loops,"fallback_source_scans":reads,"execution_ms":plan[0]["Execution Time"],"temp_read_blocks":plan[0]["Plan"]["Temp Read Blocks"],"temp_written_blocks":plan[0]["Plan"]["Temp Written Blocks"],"shared_hit_blocks":plan[0]["Plan"]["Shared Hit Blocks"],"shared_read_blocks":plan[0]["Plan"]["Shared Read Blocks"],"per_node_row_work":row_work,"per_relation_row_work":relation_work,"statistics":"ANALYZE before fixture variant; unchanged in the rolled-back variant","explain":plan})
                );
            }
        }
        Ok(())
    }
    let tx = db.store.writer_transaction().await?;
    tx.batch_execute("SET TRANSACTION READ ONLY").await?;
    plans(&tx, &selection, window, (tip + 1) * 2).await?;
    tx.commit().await?;
    // Change only the owned fixture. Rollback preserves a byte-consistent
    // resume prefix for a later same-scale run, including after an error.
    let variant = db.store.writer_transaction().await?;
    let affected = if window {
        variant
            .execute(
                "DELETE FROM serve.tx_in WHERE id BETWEEN $1 AND $2",
                &[&lower, &upper],
            )
            .await?
    } else {
        variant
            .execute(
                "INSERT INTO serve.tx_in VALUES($1,$2,36864,0)",
                &[&(lower + 4095), &(i64::from(horizon) * 4096)],
            )
            .await?
    };
    println!(
        "{}",
        json!({"fixture_variant":if window {"delete_all_window_spends"}else{"insert_one_window_spend"},"affected_rows":affected,"statistics":"pre-variant ANALYZE; no re-analyze","rolled_back_after_plans":true})
    );
    plans(&variant, &selection, !window, (tip + 1) * 2).await?;
    variant.rollback().await?;
    if std::env::var("DSA_PLAN_KEEP_DATABASE").as_deref() == Ok("1") {
        println!(
            "{}",
            json!({"retained_plan_database":db.database,"owner":db.owner})
        );
        Ok(())
    } else {
        db.cleanup().await
    }
}
