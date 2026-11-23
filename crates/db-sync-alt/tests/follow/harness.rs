//! Generated chains followed into a store through the store's update API,
//! the same chains written as a schema-5 source, and conversions of that
//! source to compare with.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error as StdError,
    ops::RangeInclusive,
};

use chrono::{DateTime, NaiveDate, NaiveDateTime};
use dsa_chain::{
    assets::AssetRule,
    block::{
        BlockOutput, DatumAvailability, Header, Held, Outcome, Selection, SupplementRequest,
        Supplements, inputs, process_block,
    },
    keep::TABLES,
    observation::{Candidate, Source as Answer},
    profiles::Profile,
    transaction::{Outpoint, Transaction, decode_all},
};
use dsa_store::{
    Store,
    convert::{ConvertOptions, LegacySource},
    follow::{BlockRow, EpochParamRow, FinalBlock, HeaderRef, Observation, Update, UpdateReport},
    verify::Verification,
};
use dsa_testkit::{
    chain::Block,
    generate::{self, Generated, Roles, Settings},
};
use serde_json::{Value, json};
use tokio_postgres::Config;

use crate::common::{Database, connect, fixtures::Rows};

pub type Result<T = ()> = std::result::Result<T, Box<dyn StdError + Send + Sync>>;

/// The security parameter of the test profiles.
pub const K: u32 = 12;
pub const WEEK: i64 = 604_800;

/// A store error with its named cause.
pub fn named(error: dsa_store::Error) -> Box<dyn StdError + Send + Sync> {
    error.report().to_string().into()
}

pub struct Random(pub u64);
impl Random {
    pub fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1)
    }
    pub fn below(&mut self, limit: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % limit.max(1)
    }
    pub fn chance(&mut self, one_in: u64) -> bool {
        self.below(one_in) == 0
    }
}

/// A real profile with a small security parameter.
pub fn profile(seed: u64) -> Profile {
    let mut profile = Profile::load(if seed.is_multiple_of(2) {
        "preprod"
    } else {
        "mainnet"
    })
    .unwrap();
    profile.cardano.security_parameter = K;
    profile
}

pub fn roles(profile: &Profile) -> Roles {
    let rule = AssetRule::from_profile(profile);
    Roles {
        network: u8::from(profile.network == "mainnet"),
        addresses: profile
            .addresses
            .iter()
            .map(|(role, address)| (role.name().to_owned(), address.to_owned()))
            .collect(),
        units: profile
            .assets
            .iter()
            .map(|(role, asset)| {
                (
                    role.name().to_owned(),
                    (asset.policy.clone(), asset.name.clone()),
                )
            })
            .collect(),
        eligible_units: rule.exact_units().clone(),
        governance_policies: rule.governance_policies().clone(),
    }
}

pub fn chain(
    profile: &Profile,
    seed: u64,
    first_height: u32,
    blocks: u32,
    large: bool,
) -> Generated {
    generate::generate(
        &roles(profile),
        &Settings {
            seed,
            first_height,
            blocks,
            transactions: 5,
            large_datums: large,
        },
    )
}

pub fn time(block: &Block) -> NaiveDateTime {
    DateTime::from_timestamp(block.time as i64, 0)
        .unwrap()
        .naive_utc()
}

pub fn header(block: &Block) -> BlockRow {
    BlockRow {
        height: block.height,
        hash: block.hash.clone(),
        epoch: block.epoch,
        slot: block.slot,
        time: time(block),
        transaction_count: block.transactions.len() as u32,
    }
}

pub fn day() -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 11, 23).unwrap()
}

fn decode(block: &Block) -> Result<Vec<Transaction<'_>>> {
    Ok(decode_all(
        block
            .transactions
            .iter()
            .map(|tx| (tx.hash.as_str(), tx.cbor.as_slice())),
    )?)
}

/// Which datums a block's caller already holds.
pub enum Datums<'a> {
    Store(&'a Store),
    Known(&'a BTreeSet<String>),
}

/// One block through `process_block`, with every supplement it names
/// answered as the provider would at the block's height.
pub async fn process(
    selection: &Selection<'_>,
    g: &Generated,
    block: &Block,
    held: &BTreeMap<Outpoint, Held>,
    datums: &Datums<'_>,
    emitted: &BTreeSet<String>,
) -> Result<BlockOutput> {
    let transactions = decode(block)?;
    let header = Header {
        height: block.height,
        hash: block.hash.clone(),
        previous_hash: block.previous.clone(),
        transaction_count: transactions.len(),
    };
    let mut supplements = Supplements::default();
    for _ in 0..4 {
        let requests = match process_block(selection, &header, &transactions, held, &supplements)? {
            Outcome::Ready(output) => return Ok(*output),
            Outcome::Needs(requests) => requests,
        };
        let wanted = requests
            .iter()
            .filter_map(|request| match request {
                SupplementRequest::Datum(hash) => Some(hash.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        let stored = match datums {
            Datums::Store(store) => store.stored_datums(&wanted).await.map_err(named)?,
            Datums::Known(known) => wanted
                .iter()
                .filter(|hash| known.contains(*hash))
                .cloned()
                .collect(),
        };
        for request in requests {
            match request {
                SupplementRequest::Datum(hash) => {
                    let availability = if emitted.contains(&hash) || stored.contains(&hash) {
                        DatumAvailability::Stored
                    } else {
                        match g.datum(&hash, block.height) {
                            Some(bytes) => DatumAvailability::Present(bytes.to_vec()),
                            None => DatumAvailability::NotFound,
                        }
                    };
                    supplements.datums.insert(hash, availability);
                }
                SupplementRequest::Metadata(transaction) => {
                    let rendering = g
                        .metadata
                        .get(&transaction)
                        .cloned()
                        .ok_or("no metadata for a transaction with an ICS output")?;
                    supplements.metadata.insert(transaction, rendering);
                }
                SupplementRequest::Output { .. } => {
                    return Err("unexpected output supplement".into());
                }
            }
        }
    }
    Err("supplements did not converge".into())
}

pub fn apply(held: &mut BTreeMap<Outpoint, Held>, output: &BlockOutput) {
    for (point, entry) in &output.created {
        held.insert(point.clone(), entry.clone());
    }
    for point in &output.spent {
        held.remove(point);
    }
}

/// Every block of the chain processed in order over an in-memory held map:
/// the full history a schema-5 store holds.
pub async fn derive(profile: &Profile, g: &Generated) -> Result<Vec<BlockOutput>> {
    let selection = Selection::new(profile);
    let mut held = BTreeMap::new();
    let mut stored = BTreeSet::new();
    let mut outputs = Vec::new();
    for block in &g.blocks {
        let output = process(
            &selection,
            g,
            block,
            &held,
            &Datums::Known(&stored),
            &BTreeSet::new(),
        )
        .await?;
        apply(&mut held, &output);
        stored.extend(output.rows.datum.iter().map(|row| row.hash.clone()));
        outputs.push(output);
    }
    Ok(outputs)
}

/// The provider answers a candidate's sources name.
pub fn answers(g: &Generated, block: &Block, candidate: &Candidate) -> BTreeMap<Answer, Vec<u8>> {
    candidate
        .sources
        .iter()
        .map(|source| {
            let body = match source {
                Answer::BlockPage(page) => block.page(*page as usize),
                Answer::Datum(hash) => {
                    json!({"cbor": hex::encode(g.datums.get(hash).map(|datum| datum.1.clone()).unwrap_or_default())})
                }
                Answer::TransactionMetadata(hash) => {
                    let value = g
                        .metadata
                        .get(hash)
                        .cloned()
                        .flatten()
                        .map(|text| serde_json::from_str::<Value>(&text).unwrap());
                    json!([{"label":"6500973","json_metadata":value}])
                }
                Answer::TransactionOutputs(hash) => json!({"hash": hash}),
            };
            (source.clone(), body.to_string().into_bytes())
        })
        .collect()
}

pub fn epoch_params(g: &Generated, heights: RangeInclusive<u32>) -> Vec<EpochParamRow> {
    heights
        .map(|height| g.block(height).unwrap().epoch)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|epoch| EpochParamRow {
            epoch,
            nonce: g.epochs[&epoch].0.clone(),
            max_tx_size: g.epochs[&epoch].1,
        })
        .collect()
}

pub fn pools(g: &Generated, epoch: u32) -> Vec<(Vec<u8>, String)> {
    g.stake[&epoch]
        .iter()
        .map(|(pool, amount)| (hex::decode(pool).unwrap(), amount.to_string()))
        .collect()
}

/// The final blocks of an update as the follower builds them: one held
/// lookup for the inputs of all blocks, each block processed in order, its
/// created and spent outputs applied before the next one.
pub struct Built {
    pub blocks: Vec<FinalBlock>,
    pub mint: BTreeMap<String, i128>,
    pub observations: Vec<Observation>,
}

pub async fn build(
    store: &Store,
    profile: &Profile,
    g: &Generated,
    heights: RangeInclusive<u32>,
) -> Result<Built> {
    let selection = Selection::new(profile);
    let blocks = heights
        .map(|height| g.block(height).ok_or("height outside the chain"))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut points = BTreeSet::new();
    for block in &blocks {
        points.extend(inputs(&decode(block)?));
    }
    let points = points.into_iter().collect::<Vec<_>>();
    let mut held = store.held_outputs(&points).await.map_err(named)?;
    let mut emitted = BTreeSet::new();
    let mut built = Built {
        blocks: Vec::new(),
        mint: BTreeMap::new(),
        observations: Vec::new(),
    };
    for block in blocks {
        let output = process(&selection, g, block, &held, &Datums::Store(store), &emitted).await?;
        apply(&mut held, &output);
        emitted.extend(output.rows.datum.iter().map(|row| row.hash.clone()));
        for (unit, delta) in &output.mint_delta {
            *built.mint.entry(unit.clone()).or_default() += delta;
        }
        for candidate in &output.observations {
            built.observations.push(Observation {
                height: block.height,
                candidate: candidate.clone(),
                answers: answers(g, block, candidate),
            });
        }
        built.blocks.push(FinalBlock {
            height: block.height,
            hash: block.hash.clone(),
            transaction_count: block.transactions.len() as u32,
            rows: output.rows,
        });
    }
    Ok(built)
}

/// The same chain as a schema-5 source, written up to a final height.
pub struct Source {
    pub database: Database,
    assets: BTreeMap<(String, String), i64>,
    datums: BTreeSet<String>,
    pools: BTreeMap<String, i64>,
    stake: i64,
    epochs: BTreeSet<u32>,
    pub through: u32,
}

fn binary(hex: &str) -> String {
    format!("\\x{hex}")
}

fn json_text(text: &Option<String>) -> Result<Value> {
    Ok(text
        .as_deref()
        .map(serde_json::from_str::<Value>)
        .transpose()?
        .unwrap_or(Value::Null))
}

impl Source {
    pub async fn new() -> Result<Self> {
        let database = Database::new().await?;
        let empty: Rows = TABLES.iter().map(|t| (t.to_string(), Vec::new())).collect();
        database.legacy_rows(&empty, 0, 0).await?;
        Ok(Self {
            database,
            assets: BTreeMap::new(),
            datums: BTreeSet::new(),
            pools: BTreeMap::new(),
            stake: 0,
            epochs: BTreeSet::new(),
            through: 0,
        })
    }

    /// Starts over: the final blocks through `target` and `headers`.
    pub async fn start(
        &mut self,
        g: &Generated,
        history: &[BlockOutput],
        target: u32,
        headers: &[BlockRow],
    ) -> Result {
        self.assets.clear();
        self.datums.clear();
        self.pools.clear();
        self.stake = 0;
        self.epochs.clear();
        self.through = g.blocks[0].height - 1;
        let rows = self.rows(g, history, target, headers)?;
        self.database.write_legacy(true, None, &rows).await
    }

    /// Adds the final blocks up to `target` and replaces every header above
    /// the previous target with `headers`.
    pub async fn extend(
        &mut self,
        g: &Generated,
        history: &[BlockOutput],
        target: u32,
        headers: &[BlockRow],
    ) -> Result {
        let above = self.through;
        let rows = self.rows(g, history, target, headers)?;
        self.database.write_legacy(false, Some(above), &rows).await
    }

    fn rows(
        &mut self,
        g: &Generated,
        history: &[BlockOutput],
        target: u32,
        headers: &[BlockRow],
    ) -> Result<Rows> {
        let first = g.blocks[0].height;
        let mut rows: Rows = TABLES.iter().map(|t| (t.to_string(), Vec::new())).collect();
        let mut push = |table: &str, value: Value| rows.get_mut(table).unwrap().push(value);
        for h in headers {
            push(
                "block",
                json!({"id":h.height,"hash":binary(&h.hash),"epoch_no":h.epoch,"slot_no":h.slot,"block_no":h.height,"time":h.time.format("%Y-%m-%dT%H:%M:%S").to_string(),"tx_count":h.transaction_count}),
            );
        }
        for height in self.through + 1..=target {
            let rows = &history[(height - first) as usize].rows;
            for r in &rows.tx {
                push(
                    "tx",
                    json!({"id":r.id,"hash":binary(&r.hash),"block_id":r.block_id,"block_index":r.block_index,"valid_contract":r.valid_contract}),
                );
            }
            for r in &rows.tx_out {
                push(
                    "tx_out",
                    json!({"id":r.id,"tx_id":r.tx_id,"index":r.index,"address":r.address,"value":r.value,"data_hash":r.data_hash.as_deref().map(binary),"consumed_by_tx_id":null}),
                );
            }
            for r in &rows.tx_in {
                push(
                    "tx_in",
                    json!({"id":r.id,"tx_in_id":r.tx_in_id,"tx_out_id":r.tx_out_id,"tx_out_index":r.tx_out_index}),
                );
            }
            for r in &rows.ma_tx_out {
                let unit = (r.policy.clone(), r.name.clone());
                let next = self.assets.len() as i64 + 1;
                let ident = *self.assets.entry(unit).or_insert_with(|| {
                    push(
                        "multi_asset",
                        json!({"id":next,"policy":binary(&r.policy),"name":binary(&r.name)}),
                    );
                    next
                });
                push(
                    "ma_tx_out",
                    json!({"id":r.id,"quantity":r.quantity,"tx_out_id":r.tx_out_id,"ident":ident}),
                );
            }
            for r in &rows.datum {
                if self.datums.insert(r.hash.clone()) {
                    push(
                        "datum",
                        json!({"id":self.datums.len(),"hash":binary(&r.hash),"value":json_text(&r.value)?}),
                    );
                }
            }
            for r in &rows.tx_metadata {
                push(
                    "tx_metadata",
                    json!({"id":r.id,"key":r.key,"tx_id":r.tx_id,"json":json_text(&r.json)?}),
                );
            }
            let epoch = g.block(height).unwrap().epoch;
            if self.epochs.insert(epoch) {
                let (nonce, size) = &g.epochs[&epoch];
                push(
                    "epoch_param",
                    json!({"epoch_no":epoch,"nonce":binary(nonce),"max_tx_size":size}),
                );
                for (pool, amount) in &g.stake[&epoch] {
                    let next = self.pools.len() as i64 + 1;
                    let id = *self.pools.entry(pool.clone()).or_insert_with(|| {
                        push("pool_hash", json!({"id":next,"hash_raw":binary(pool)}));
                        next
                    });
                    self.stake += 1;
                    push(
                        "epoch_stake",
                        json!({"id":self.stake,"pool_id":id,"amount":amount,"epoch_no":epoch}),
                    );
                }
            }
        }
        self.through = target;
        Ok(rows)
    }
}

/// Empties a store database to the closed layout a load accepts.
pub async fn reset(database: &Database, profile: &Profile) -> Result {
    let client = connect(&database.config).await?;
    let layout: bool = client
        .query_one("SELECT to_regclass('dsa.identity') IS NOT NULL", &[])
        .await?
        .get(0);
    if !layout {
        return Ok(());
    }
    let tables = TABLES
        .iter()
        .map(|table| format!("serve.{table}"))
        .collect::<Vec<_>>()
        .join(",");
    client.batch_execute(&format!("TRUNCATE {tables},dsa.supply,dsa.consumed_before_horizon,dsa.first_served,dsa.calls,dsa.service_state,dsa.unspent,dsa.first_observation;\
        UPDATE dsa.position SET serving=false,reason='empty',target_height=NULL,target_hash=NULL,target_time=NULL,tip_height=NULL,tip_hash=NULL,tip_time=NULL,\
        horizon_height=NULL,horizon_hash=NULL,horizon_time=NULL,retention_seconds=604800 WHERE id=1")).await?;
    client
        .execute(
            "UPDATE dsa.identity SET network=$1,profile_hash=$2,origin='{\"kind\":\"empty\"}'::jsonb WHERE id=1",
            &[&profile.network, &profile.hash()?],
        )
        .await?;
    Ok(())
}

/// Converts the source at `horizon` into `destination` through the library.
pub async fn convert(
    destination: &Database,
    source: &Database,
    profile: &Profile,
    horizon: u32,
    retention_seconds: i64,
) -> Result<Verification> {
    reset(destination, profile).await?;
    let mut legacy = LegacySource::connect(source.reader_config(), None).await?;
    let mut store = Store::connect(destination.config.clone(), None).await?;
    let converted = store
        .convert(
            profile,
            &mut legacy,
            &ConvertOptions {
                horizon_block: Some(horizon),
                retention_seconds,
                ..ConvertOptions::default()
            },
        )
        .await
        .map_err(named)?;
    Ok(converted.verified)
}

/// Content and state digests, families, supply and positions are equal.
pub fn same(follow: &Verification, converted: &Verification, context: &str) -> Result {
    let differing = follow
        .families
        .iter()
        .filter(|(family, digest)| converted.families.get(*family) != Some(digest))
        .map(|(family, _)| family.clone())
        .collect::<Vec<_>>();
    if follow.content_digest != converted.content_digest
        || follow.state_digest != converted.state_digest
        || follow.supply != converted.supply
        || follow.state != converted.state
    {
        return Err(format!(
            "{context}: differs from the conversion: families {differing:?}, markers {}, supply {:?} / {:?}, state {:?} / {:?}",
            follow.state_digest != converted.state_digest,
            follow.supply,
            converted.supply,
            follow.state,
            converted.state
        )
        .into());
    }
    Ok(())
}

/// The rows of one table in natural-key form, for a diagnostic diff.
pub async fn natural(config: &Config, table: &str) -> Result<Vec<String>> {
    let sql = match table {
        "ma_tx_out" => "SELECT jsonb_build_array(a.id,a.quantity,a.tx_out_id,encode(m.policy,'hex'),encode(m.name,'hex'))::text FROM serve.ma_tx_out a JOIN serve.multi_asset m ON m.id=a.ident ORDER BY 1".to_owned(),
        "multi_asset" => "SELECT jsonb_build_array(encode(policy,'hex'),encode(name,'hex'))::text FROM serve.multi_asset ORDER BY 1".to_owned(),
        "datum" => "SELECT jsonb_build_array(encode(hash,'hex'),value)::text FROM serve.datum ORDER BY 1".to_owned(),
        "pool_hash" => "SELECT encode(hash_raw,'hex') FROM serve.pool_hash ORDER BY 1".to_owned(),
        "epoch_stake" => "SELECT jsonb_build_array(s.epoch_no,encode(p.hash_raw,'hex'),s.amount)::text FROM serve.epoch_stake s JOIN serve.pool_hash p ON p.id=s.pool_id ORDER BY 1".to_owned(),
        "consumed_before_horizon" => "SELECT tx_out_id::text FROM dsa.consumed_before_horizon ORDER BY 1".to_owned(),
        table => format!("SELECT to_jsonb(t)::text FROM serve.{table} t ORDER BY 1"),
    };
    Ok(connect(config)
        .await?
        .query(&sql, &[])
        .await?
        .iter()
        .map(|row| row.get(0))
        .collect())
}

/// The rows only one side holds, per table.
pub async fn diff(left: &Config, right: &Config) -> Result<Value> {
    let mut found = serde_json::Map::new();
    for table in TABLES.iter().copied().chain(["consumed_before_horizon"]) {
        let (a, b) = (natural(left, table).await?, natural(right, table).await?);
        let (a, b): (BTreeSet<_>, BTreeSet<_>) = (a.into_iter().collect(), b.into_iter().collect());
        let only_left = a.difference(&b).take(5).cloned().collect::<Vec<_>>();
        let only_right = b.difference(&a).take(5).cloned().collect::<Vec<_>>();
        if !only_left.is_empty() || !only_right.is_empty() {
            found.insert(
                table.to_owned(),
                json!({"follower":only_left,"conversion":only_right}),
            );
        }
    }
    Ok(Value::Object(found))
}

/// One step of the follower as the test plans it.
#[derive(Clone, Debug)]
pub struct Plan {
    pub fork_point: u32,
    pub headers: Vec<BlockRow>,
    pub blocks: u32,
    pub prune_to: Option<u32>,
}

/// A store followed through updates, and what it holds.
pub struct Follower {
    pub config: Config,
    pub store: Store,
    pub prune: bool,
    pub target: HeaderRef,
    pub tip: HeaderRef,
    pub horizon: u32,
    /// Stored headers above the target.
    pub headers: BTreeMap<u32, BlockRow>,
    pub staked: BTreeSet<u32>,
    /// First height at which each observation shape was handed to the store.
    pub seen: BTreeMap<String, u32>,
    pub calls: i64,
}

impl Follower {
    pub async fn open(
        config: &Config,
        profile: &Profile,
        prune: bool,
        staked: BTreeSet<u32>,
    ) -> Result<Self> {
        let store = Store::connect(config.clone(), None).await?;
        let state = store.follow_state(profile).await.map_err(named)?;
        let headers = store
            .headers_above(state.target.height)
            .await
            .map_err(named)?
            .into_iter()
            .map(|row| (row.height, row))
            .collect();
        Ok(Self {
            config: config.clone(),
            store,
            prune,
            target: HeaderRef {
                height: state.target.height,
                hash: state.target.hash,
            },
            tip: HeaderRef {
                height: state.tip.height,
                hash: state.tip.hash,
            },
            horizon: state.horizon.height,
            headers,
            staked,
            seen: BTreeMap::new(),
            calls: 0,
        })
    }

    /// Closes the connection and opens a new one; the stored state is the
    /// one the test tracks.
    pub async fn restart(&mut self, profile: &Profile) -> Result {
        let next = Self::open(&self.config, profile, self.prune, BTreeSet::new()).await?;
        if (&next.target, &next.tip, next.horizon, &next.headers)
            != (&self.target, &self.tip, self.horizon, &self.headers)
        {
            return Err("the stored position changed across a restart".into());
        }
        self.store = next.store;
        Ok(())
    }

    /// Builds the update of `plan` against this store and applies it.
    pub async fn update(
        &mut self,
        profile: &Profile,
        g: &Generated,
        plan: &Plan,
    ) -> Result<UpdateReport> {
        let heights = self.target.height + 1..=self.target.height + plan.blocks;
        let built = build(&self.store, profile, g, heights.clone()).await?;
        let params = epoch_params(g, heights.clone());
        self.calls += 1 + i64::from(plan.blocks) * 2;
        let update = Update {
            target: &self.target,
            tip: &self.tip,
            fork_point: plan.fork_point,
            headers: &plan.headers,
            blocks: &built.blocks,
            epoch_params: &params,
            mint_delta: &built.mint,
            calls: &[(day(), self.calls)],
            observations: &built.observations,
            prune_to: plan.prune_to.filter(|_| self.prune),
        };
        let report = self
            .store
            .apply_update(profile, &update)
            .await
            .map_err(named)?;
        for observation in &built.observations {
            self.seen
                .entry(observation.candidate.shape.as_str().to_owned())
                .or_insert(observation.height);
        }
        self.headers.retain(|height, _| *height <= plan.fork_point);
        self.headers
            .extend(plan.headers.iter().map(|row| (row.height, row.clone())));
        if plan.blocks > 0 {
            let target = *heights.end();
            self.target = HeaderRef {
                height: target,
                hash: g.block(target).unwrap().hash.clone(),
            };
            self.headers.retain(|height, _| *height > target);
        }
        // The position the plan leads to, checked against the store's own.
        let tip = self.target.height + K;
        self.tip = HeaderRef {
            height: tip,
            hash: self.headers[&tip].hash.clone(),
        };
        if let Some(horizon) = plan.prune_to.filter(|_| self.prune) {
            self.horizon = horizon;
        }
        let state = self.store.follow_state(profile).await.map_err(named)?;
        if (report.target, report.tip, report.horizon) != (self.target.height, tip, self.horizon)
            || (
                state.target.height,
                &state.target.hash,
                state.tip.height,
                &state.tip.hash,
                state.horizon.height,
            ) != (
                self.target.height,
                &self.target.hash,
                tip,
                &self.tip.hash,
                self.horizon,
            )
        {
            return Err(format!(
                "the store is at target {} tip {} horizon {}, the plan leads to {} {} {}",
                state.target.height,
                state.tip.height,
                state.horizon.height,
                self.target.height,
                tip,
                self.horizon
            )
            .into());
        }
        for param in &params {
            if self.staked.insert(param.epoch) {
                self.store
                    .put_epoch_stake(param.epoch, &pools(g, param.epoch))
                    .await
                    .map_err(named)?;
            }
        }
        Ok(report)
    }

    /// The headers a source must hold above `through` to equal this store.
    pub fn headers_from(&self, g: &Generated, through: u32) -> Vec<BlockRow> {
        (through + 1..=self.target.height)
            .map(|height| header(g.block(height).unwrap()))
            .chain(self.headers.values().cloned())
            .collect()
    }
}

/// Appends `blocks` canonical blocks with their headers.
pub fn advance(g: &Generated, f: &Follower, blocks: u32, prune_to: Option<u32>) -> Plan {
    let tip = f.tip.height;
    Plan {
        fork_point: tip,
        headers: (tip + 1..=tip + blocks)
            .map(|height| header(g.block(height).unwrap()))
            .collect(),
        blocks,
        prune_to,
    }
}

/// Replaces the top `depth` headers by another branch.
pub fn replace(g: &Generated, f: &Follower, depth: u32, salt: u64) -> Plan {
    let tip = f.tip.height;
    Plan {
        fork_point: tip - depth,
        headers: (tip - depth + 1..=tip)
            .map(|height| header(&generate::alternative(g.block(height).unwrap(), salt, 2)))
            .collect(),
        blocks: 0,
        prune_to: None,
    }
}

/// A random step: up to six final blocks, sometimes none; sometimes a fork
/// point below the tip; sometimes a new branch above the new target; and
/// sometimes a horizon advance.
pub fn plan(random: &mut Random, g: &Generated, f: &Follower, last_target: u32, salt: u64) -> Plan {
    let (target, tip) = (f.target.height, f.tip.height);
    let remaining = last_target - target;
    let blocks = if remaining == 0 || random.chance(5) {
        0
    } else {
        1 + random.below(u64::from(remaining.min(6))) as u32
    };
    let mut fork_point = if random.chance(4) {
        target + random.below(u64::from(tip - target)) as u32
    } else {
        tip
    };
    // Headers that become final must be the canonical ones.
    if let Some(abandoned) = f
        .headers
        .iter()
        .find(|(height, row)| row.hash != g.block(**height).unwrap().hash)
        .map(|(height, _)| *height)
        && abandoned <= target + blocks
        && abandoned <= fork_point
    {
        fork_point = abandoned - 1;
    }
    let new_target = target + blocks;
    let new_tip = new_target + K;
    let low = new_target.max(fork_point) + 1;
    let branch = (low <= new_tip && random.chance(3))
        .then(|| low + random.below(u64::from(new_tip - low + 1)) as u32);
    let headers = (fork_point + 1..=new_tip)
        .map(|height| {
            let block = g.block(height).unwrap();
            if branch.is_some_and(|first| height >= first) {
                header(&generate::alternative(
                    block,
                    salt,
                    random.below(3) as usize,
                ))
            } else {
                header(block)
            }
        })
        .collect();
    let prune_to = (new_target > f.horizon && random.chance(3)).then(|| {
        if random.chance(4) {
            new_target
        } else {
            f.horizon + 1 + random.below(u64::from(new_target - f.horizon)) as u32
        }
    });
    Plan {
        fork_point,
        headers,
        blocks,
        prune_to,
    }
}

/// A converted store and its follower on a generated chain.
pub struct Setup {
    pub profile: Profile,
    pub g: Generated,
    pub history: Vec<BlockOutput>,
    pub source: Source,
    pub database: Database,
    pub follower: Follower,
    pub last_target: u32,
}

impl Setup {
    pub async fn new(
        seed: u64,
        first_height: u32,
        blocks: u32,
        large: bool,
        retention_seconds: i64,
    ) -> Result<Self> {
        let profile = profile(seed);
        let g = chain(&profile, seed, first_height, blocks, large);
        let history = derive(&profile, &g).await?;
        let target = first_height + 10;
        let horizon = target - 2;
        let tip = target + K;
        let mut source = Source::new().await?;
        let headers = (first_height..=tip)
            .map(|height| header(g.block(height).unwrap()))
            .collect::<Vec<_>>();
        source.start(&g, &history, target, &headers).await?;
        let database = Database::new().await?;
        convert(
            &database,
            &source.database,
            &profile,
            horizon,
            retention_seconds,
        )
        .await?;
        let staked = (first_height..=target)
            .map(|height| g.block(height).unwrap().epoch)
            .collect();
        let follower = Follower::open(&database.config, &profile, true, staked).await?;
        Ok(Self {
            last_target: first_height + blocks - 1 - K,
            profile,
            g,
            history,
            source,
            database,
            follower,
        })
    }

    /// Applies a plan and returns its report.
    pub async fn step(&mut self, plan: &Plan) -> Result<UpdateReport> {
        self.follower.update(&self.profile, &self.g, plan).await
    }

    pub async fn cleanup(self) -> Result {
        drop(self.follower);
        self.database.cleanup().await?;
        self.source.database.cleanup().await
    }
}

/// Every row of every served and state table, identity columns included.
pub async fn dump(config: &Config) -> Result<Value> {
    let client = connect(config).await?;
    let mut all = serde_json::Map::new();
    for table in TABLES.iter().map(|table| format!("serve.{table}")).chain(
        [
            "supply",
            "consumed_before_horizon",
            "first_served",
            "calls",
            "position",
            "service_state",
            "unspent",
            "first_observation",
        ]
        .map(|table| format!("dsa.{table}")),
    ) {
        let value: Value = client
            .query_one(
                &format!("SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]'::jsonb) FROM {table} t"),
                &[],
            )
            .await?
            .get(0);
        all.insert(table, value);
    }
    Ok(Value::Object(all))
}

/// The state tables without times and the content as digests: what two
/// stores that followed the same chain share.
pub async fn logical(store: &mut Store, profile: &Profile, config: &Config) -> Result<Value> {
    let verified = store.verify(profile).await.map_err(named)?;
    let client = connect(config).await?;
    let state: Value = client
        .query_one(
            "SELECT jsonb_build_object(
              'position',(SELECT to_jsonb(p)-'updated_at' FROM dsa.position p),
              'first_served',(SELECT jsonb_agg(height ORDER BY height) FROM dsa.first_served),
              'supply',(SELECT jsonb_object_agg(unit,quantity) FROM dsa.supply),
              'unspent',(SELECT jsonb_object_agg(address,outputs) FROM dsa.unspent),
              'calls',(SELECT jsonb_object_agg(day,requests) FROM dsa.calls),
              'observations',(SELECT coalesce(jsonb_agg(to_jsonb(o)-'observed_at' ORDER BY shape),'[]') FROM dsa.first_observation o),
              'service',(SELECT jsonb_build_object('state',state,'reason',reason,'target',last_update->'target','inserted',last_update->'inserted','deleted',last_update->'deleted') FROM dsa.service_state))",
            &[],
        )
        .await?
        .get(0);
    Ok(
        json!({"content_digest":verified.content_digest,"state_digest":verified.state_digest,"families":verified.families,"state":state}),
    )
}

/// Rows outside every update's range that a store keeps: old unspent outputs
/// with their transactions and cNIGHT rows (every tenth at the mapping
/// address, with its datum), and every fourth of them spent by transactions
/// at the `window` height.
pub async fn fill(
    config: &tokio_postgres::Config,
    profile: &dsa_chain::profiles::Profile,
    first: i32,
    blocks: i32,
    window: u32,
) -> Result {
    let cnight = &profile.assets.cnight;
    let last = first + blocks - 1;
    let client = connect(config).await?;
    client
        .execute(
            "INSERT INTO serve.tx(id,hash,block_id,block_index,valid_contract)
             SELECT b::bigint*4096+i,decode(md5('f'||b||'/'||i)||md5('g'||b||'/'||i),'hex'),b,i,true
             FROM generate_series($1::integer,$2::integer) b,generate_series(0,99) i",
            &[&first, &last],
        )
        .await?;
    client
        .execute(
            "INSERT INTO serve.tx_out(id,tx_id,index,address,value,data_hash,consumed_by_tx_id)
             SELECT t.id*4096+j,t.id,j,CASE WHEN t.id%10=0 THEN $3 ELSE 'addr_filler_'||(t.id%997) END,1000000,
              CASE WHEN t.id%10=0 THEN decode(md5('d'||t.id||'/'||j)||md5('e'||t.id||'/'||j),'hex') END,NULL
             FROM serve.tx t,generate_series(0,1) j WHERE t.block_id BETWEEN $1 AND $2 AND (j=0 OR t.id%2=0)",
            &[&i64::from(first), &i64::from(last), &profile.addresses.mapping],
        )
        .await?;
    client
        .execute(
            "INSERT INTO serve.datum(id,hash,value)
             SELECT nextval('dsa.datum_id_seq'),o.data_hash,jsonb_build_object('int',o.id)
             FROM serve.tx_out o WHERE o.tx_id BETWEEN $1::bigint*4096 AND $2::bigint*4096+4095 AND o.data_hash IS NOT NULL",
            &[&i64::from(first), &i64::from(last)],
        )
        .await?;
    client
        .execute(
            "INSERT INTO serve.ma_tx_out(id,quantity,tx_out_id,ident)
             SELECT o.id*4096,5,o.id,m.id FROM serve.tx_out o
             JOIN serve.multi_asset m ON m.policy=decode($3,'hex') AND m.name=decode($4,'hex')
             WHERE o.tx_id BETWEEN $1::bigint*4096 AND $2::bigint*4096+4095",
            &[
                &i64::from(first),
                &i64::from(last),
                &cnight.policy,
                &cnight.name,
            ],
        )
        .await?;
    // Spending transactions at the window height, 4,000 inputs each.
    let base = i64::from(window) * 4096 + 1000 + i64::from(first);
    client
        .execute(
            "WITH spent AS (SELECT row_number() OVER (ORDER BY o.id)-1 AS n,o.tx_id,o.index FROM serve.tx_out o
              WHERE o.tx_id BETWEEN $1::bigint*4096 AND $2::bigint*4096+4095 AND o.index=0 AND o.tx_id%4=1),
             spenders AS (INSERT INTO serve.tx(id,hash,block_id,block_index,valid_contract)
              SELECT DISTINCT $3::bigint+n/4000,decode(md5('s'||($3::bigint+n/4000))||md5('t'||($3::bigint+n/4000)),'hex'),
               $4::bigint,$3::bigint+n/4000-$4::bigint*4096,true FROM spent)
             INSERT INTO serve.tx_in(id,tx_in_id,tx_out_id,tx_out_index)
             SELECT ($3::bigint+n/4000)*4096+n%4000,$3::bigint+n/4000,tx_id,index FROM spent",
            &[&i64::from(first), &i64::from(last), &base, &i64::from(window)],
        )
        .await?;
    client.batch_execute("ANALYZE").await?;
    Ok(())
}
