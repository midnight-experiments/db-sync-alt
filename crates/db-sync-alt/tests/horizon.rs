//! Horizon advance on generated chains, against PostgreSQL 17 in
//! DSA_TEST_DATABASE. DSA_KEEP_CASES sets the number of chains (default 2,000).

#[path = "keep_rule/fixtures.rs"]
#[allow(dead_code)]
mod fixtures;

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error as StdError,
    time::Instant,
};

use dsa_chain::keep::{Bounds, TABLES};
use dsa_store::Store;
use fixtures::{Fixture, Rows, model::keep, selected};
use serde_json::{Value, json};
use tokio_postgres::{Client, Config, NoTls, config::SslMode};

type Result<T = ()> = std::result::Result<T, Box<dyn StdError + Send + Sync>>;

async fn connect(config: &Config) -> Result<Client> {
    let (client, connection) = config.connect(NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

struct Scratch {
    admin: Client,
    config: Config,
    client: Client,
    owner: String,
    name: String,
}
impl Scratch {
    async fn new(fixture: &Fixture) -> Result<Self> {
        let root: Config = std::env::var("DSA_TEST_DATABASE")?.parse()?;
        let admin = connect(&root).await?;
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let owner = format!("horizon_owner_{}_{nonce:x}", std::process::id());
        let name = format!("horizon_{}_{nonce:x}", std::process::id());
        admin.batch_execute(&format!("CREATE ROLE {owner} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS")).await?;
        admin
            .batch_execute(&format!("CREATE DATABASE {name} OWNER {owner}"))
            .await?;
        let mut config = root;
        config.user(&owner).dbname(&name).ssl_mode(SslMode::Disable);
        Store::connect(config.clone(), None)
            .await?
            .create(&fixture.profile)
            .await?;
        let client = connect(&config).await?;
        client.batch_execute("SET jit=off").await?;
        Ok(Self {
            admin,
            config,
            client,
            owner,
            name,
        })
    }

    /// The store a conversion of the complete chain at `horizon` produces:
    /// the model's rows, the outputs kept although spent before H, the supply
    /// at T and a first-served time for every height H…T.
    async fn load(&self, f: &Fixture, horizon: u32) -> Result<Rows> {
        let bounds = Bounds {
            horizon,
            ..f.bounds
        };
        let kept = selected(&f.rows(), &keep(&f.chain, &f.profile, bounds)?);
        let tables = TABLES
            .iter()
            .map(|table| format!("serve.{table}"))
            .collect::<Vec<_>>()
            .join(",");
        self.client
            .batch_execute(&format!(
                "TRUNCATE {tables},dsa.consumed_before_horizon,dsa.supply,dsa.first_served"
            ))
            .await?;
        self.client
            .execute(
                "UPDATE dsa.identity SET network=$1,profile_hash=$2 WHERE id=1",
                &[&f.profile.network, &f.profile.hash()?],
            )
            .await?;
        let inserts = TABLES.iter().enumerate().map(|(index, table)| format!("insert_{index} AS (INSERT INTO serve.{table} SELECT * FROM jsonb_populate_recordset(NULL::serve.{table},$1::jsonb->'{table}') RETURNING 1)")).collect::<Vec<_>>().join(",");
        self.client
            .query_one(
                &format!("WITH {inserts} SELECT 1"),
                &[&serde_json::to_value(&kept)?],
            )
            .await?;
        let markers = markers(f, &kept, horizon).into_iter().collect::<Vec<_>>();
        self.client
            .execute(
                "INSERT INTO dsa.consumed_before_horizon SELECT unnest($1::bigint[])",
                &[&markers],
            )
            .await?;
        let target = i64::from(f.bounds.target);
        let spent = f
            .chain
            .inputs
            .values()
            .filter(|input| f.chain.transactions[&input.transaction].block <= target)
            .map(|input| input.output)
            .collect::<BTreeSet<_>>();
        for unit in dsa_chain::assets::fixed_units(&f.profile) {
            let quantity: u64 = f
                .chain
                .output_assets
                .iter()
                .filter(|(_, row)| {
                    f.chain.assets[&row.asset].unit() == unit
                        && !spent.contains(&row.output)
                        && f.chain.transactions[&f.chain.outputs[&row.output].transaction].block
                            <= target
                })
                .map(|(id, _)| f.quantities.get(id).copied().unwrap_or(1))
                .sum();
            self.client
                .execute(
                    "INSERT INTO dsa.supply VALUES($1,$2::text::numeric)",
                    &[&unit, &quantity.to_string()],
                )
                .await?;
        }
        self.client.execute("UPDATE dsa.position p SET horizon_height=$1::integer,horizon_hash=h.hash,horizon_time=h.time AT TIME ZONE 'UTC',target_height=$2::integer,target_hash=t.hash,target_time=t.time AT TIME ZONE 'UTC',tip_height=$3::integer,tip_hash=e.hash,tip_time=e.time AT TIME ZONE 'UTC',serving=true,reason='serving',updated_at=now() FROM serve.block h,serve.block t,serve.block e WHERE p.id=1 AND h.id=$1::bigint AND t.id=$2::bigint AND e.id=$3::bigint",&[&i64::from(horizon),&target,&i64::from(f.bounds.tip)]).await?;
        self.client
            .execute(
                "INSERT INTO dsa.first_served SELECT id,now() FROM serve.block WHERE id BETWEEN $1 AND $2",
                &[&i64::from(horizon), &target],
            )
            .await?;
        Ok(kept)
    }

    async fn rows(&self) -> Result<(Rows, BTreeSet<i64>)> {
        let mut rows = BTreeMap::new();
        for table in TABLES {
            let key = if table == "epoch_param" {
                "epoch_no"
            } else {
                "id"
            };
            let value:Value=self.client.query_one(&format!("SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY {key}),'[]'::jsonb) FROM serve.{table} t"),&[]).await?.get(0);
            rows.insert(table.to_owned(), serde_json::from_value(value)?);
        }
        let markers = self
            .client
            .query("SELECT tx_out_id FROM dsa.consumed_before_horizon", &[])
            .await?
            .iter()
            .map(|row| row.get(0))
            .collect();
        Ok((rows, markers))
    }

    async fn cleanup(self) -> Result {
        drop(self.client);
        self.admin
            .batch_execute(&format!("DROP DATABASE {} WITH (FORCE)", self.name))
            .await?;
        self.admin
            .batch_execute(&format!("DROP ROLE {}", self.owner))
            .await?;
        Ok(())
    }
}

/// Kept outputs whose spend lies before `horizon`.
fn markers(f: &Fixture, kept: &Rows, horizon: u32) -> BTreeSet<i64> {
    let outputs = kept["tx_out"]
        .iter()
        .map(|row| row["id"].as_i64().unwrap())
        .collect::<BTreeSet<_>>();
    f.chain
        .inputs
        .values()
        .filter(|input| {
            f.chain.transactions[&input.transaction].block < i64::from(horizon)
                && outputs.contains(&input.output)
        })
        .map(|input| input.output)
        .collect()
}

struct Random(u64);
impl Random {
    fn below(&mut self, limit: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % limit as u64) as usize
    }
}

/// Three increasing horizons H0 < H1 < H2 ≤ T such that every header from H0
/// on has an epoch and a slot, as in every store (`advance_horizon` refuses
/// otherwise).
fn horizons(f: &Fixture, seed: u64) -> Option<(u32, u32, u32)> {
    let complete = |height: u32| {
        f.chain
            .blocks
            .get(&i64::from(height))
            .is_some_and(|header| header.epoch.is_some() && header.slot.is_some())
    };
    let heights = (1..=f.bounds.target)
        .filter(|height| (*height..=f.bounds.tip).all(complete))
        .collect::<Vec<_>>();
    if heights.len() < 3 {
        return None;
    }
    let mut random = Random(seed.wrapping_mul(0x9e3779b97f4a7c15) | 1);
    let first = random.below(heights.len() - 2);
    let second = first + 1 + random.below(heights.len() - first - 2);
    let third = second + 1 + random.below(heights.len() - second - 1);
    Some((heights[first], heights[second], heights[third]))
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL 17"]
async fn horizon_advance_equals_one_step_the_model_and_a_conversion() -> Result {
    let cases: u64 = std::env::var("DSA_KEEP_CASES")
        .unwrap_or_else(|_| "2000".to_owned())
        .parse()?;
    let start: u64 = std::env::var("DSA_KEEP_SEED_START")
        .unwrap_or_else(|_| "0".to_owned())
        .parse()?;
    let first = Fixture::generated(start);
    let scratch = Scratch::new(&first).await?;
    let began = Instant::now();
    let (mut checked, mut skipped, mut empty_windows, mut spent_governance, mut deleted) =
        (0_u64, 0_u64, 0_u64, 0_u64, 0_u64);
    for seed in start..start + cases {
        let f = Fixture::generated(seed);
        let Some((h0, h1, h2)) = horizons(&f, seed) else {
            skipped += 1;
            continue;
        };
        let expected = selected(
            &f.rows(),
            &keep(
                &f.chain,
                &f.profile,
                Bounds {
                    horizon: h2,
                    ..f.bounds
                },
            )?,
        );
        let expected_markers = markers(&f, &expected, h2);
        // Stepwise, with a restart between the steps.
        scratch.load(&f, h0).await?;
        let mut store = Store::connect(scratch.config.clone(), None).await?;
        let advance = store.advance_horizon(&f.profile, h1).await?;
        drop(store);
        let mut store = Store::connect(scratch.config.clone(), None).await?;
        let second = store.advance_horizon(&f.profile, h2).await?;
        deleted += advance.deleted.values().sum::<u64>() + second.deleted.values().sum::<u64>();
        let stepwise = scratch.rows().await?;
        assert_eq!(
            stepwise.0, expected,
            "seed {seed}: stepwise {h0}->{h1}->{h2} against the model"
        );
        assert_eq!(stepwise.1, expected_markers, "seed {seed}: markers");
        let stepwise_digest = store.verify(&f.profile).await?;
        assert_eq!(store.horizon_candidate().await?, h2, "seed {seed}");
        // One step.
        scratch.load(&f, h0).await?;
        store.advance_horizon(&f.profile, h2).await?;
        assert_eq!(
            scratch.rows().await?,
            stepwise,
            "seed {seed}: one step {h0}->{h2}"
        );
        // The store a conversion of the complete chain at H2 produces.
        scratch.load(&f, h2).await?;
        let converted = store.verify(&f.profile).await?;
        assert_eq!(
            converted.content_digest, stepwise_digest.content_digest,
            "seed {seed}"
        );
        assert_eq!(
            converted.state_digest, stepwise_digest.state_digest,
            "seed {seed}"
        );
        assert_eq!(converted.families, stepwise_digest.families, "seed {seed}");
        drop(store);
        let h = i64::from(h2);
        let t = i64::from(f.bounds.target);
        empty_windows += u64::from(
            !f.chain
                .inputs
                .keys()
                .any(|id| *id >= h * 16777216 && *id <= t * 16777216 + 16777215),
        );
        spent_governance += u64::from(expected_markers.iter().any(|id| {
            let address = &f.chain.outputs[id].address;
            [
                &f.profile.addresses["council"],
                &f.profile.addresses["technical_committee"],
            ]
            .contains(&address)
        }));
        checked += 1;
        if checked.is_multiple_of(100) {
            println!(
                "{}",
                json!({"checked":checked,"last_seed":seed,"seconds":began.elapsed().as_secs_f64()})
            );
        }
    }
    println!(
        "{}",
        json!({"cases":cases,"checked":checked,"skipped":skipped,"empty_windows_at_h2":empty_windows,"spent_governance_records_kept":spent_governance,"rows_deleted":deleted,"seconds":began.elapsed().as_secs_f64()})
    );
    assert!(checked > 0 && deleted > 0);
    if cases >= 500 {
        assert!(empty_windows > 0, "no chain with an empty window at H2");
        assert!(spent_governance > 0, "no spent governance record kept");
    }
    scratch.cleanup().await
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL 17"]
async fn horizon_advance_refuses_invalid_targets_and_candidate_follows_retention() -> Result {
    let f = Fixture::generated(5);
    let scratch = Scratch::new(&f).await?;
    let (h0, _, h2) = horizons(&f, 5).ok_or("too few horizons")?;
    scratch.load(&f, h0).await?;
    let mut store = Store::connect(scratch.config.clone(), None).await?;
    for invalid in [h0, h0.saturating_sub(1), f.bounds.target + 1] {
        let error = store
            .advance_horizon(&f.profile, invalid)
            .await
            .unwrap_err();
        assert_eq!(error.report()["error"], "invalid_horizon_advance");
    }
    assert_eq!(store.horizon_candidate().await?, h0);
    // A header without a slot that would become old stops the advance.
    scratch
        .client
        .execute(
            "UPDATE serve.block SET slot_no=NULL WHERE id=$1",
            &[&i64::from(h0)],
        )
        .await?;
    let error = store.advance_horizon(&f.profile, h2).await.unwrap_err();
    assert_eq!(error.report()["error"], "horizon_header_incomplete");
    scratch.load(&f, h0).await?;
    // Heights first served more than the retention ago may go.
    scratch
        .client
        .execute(
            "UPDATE dsa.first_served SET first_served_at=now()-interval '8 days' WHERE height<$1",
            &[&(h2 as i32)],
        )
        .await?;
    assert_eq!(store.horizon_candidate().await?, h2);
    scratch
        .client
        .batch_execute("UPDATE dsa.first_served SET first_served_at=now()-interval '8 days'")
        .await?;
    assert_eq!(store.horizon_candidate().await?, f.bounds.target);
    drop(store);
    scratch.cleanup().await
}
