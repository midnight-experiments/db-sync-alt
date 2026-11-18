use std::collections::{BTreeMap, BTreeSet};

use dsa_chain::{
    ids,
    keep::{Bounds, Chain, Datum, Header, Input, Output, OutputAsset, Stake, Transaction},
    profiles::{Asset, Profile},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub type Rows = BTreeMap<String, Vec<Value>>;

pub struct Fixture {
    pub chain: Chain,
    pub profile: Profile,
    pub bounds: Bounds,
    pub tags: BTreeMap<String, i64>,
    pub seed: u64,
    pub quantities: BTreeMap<i64, u64>,
}
struct Random(u64);
impl Random {
    fn next(&mut self, limit: u64) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(2685821657736338717) % limit
    }
}

fn hash(seed: u64, id: i64) -> String {
    format!("{:x}", Sha256::digest(format!("{seed}/{id}").as_bytes()))
}
fn binary(value: &str) -> String {
    format!("\\x{value}")
}

impl Fixture {
    pub fn base(seed: u64) -> Self {
        let mut random = Random(
            seed.wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407),
        );
        let mut profile = Profile::load(if seed.is_multiple_of(2) {
            "preprod"
        } else {
            "mainnet"
        })
        .unwrap();
        profile.cardano.security_parameter = 2;
        let bounds = Bounds {
            horizon: 6 + random.next(2) as u32,
            target: 8,
            tip: 10,
        };
        let mut fixture = Self {
            chain: Chain::default(),
            profile,
            bounds,
            tags: BTreeMap::new(),
            seed,
            quantities: BTreeMap::new(),
        };
        let units: BTreeMap<_, _> = fixture
            .profile
            .assets
            .values()
            .map(|asset| (asset.unit(), asset.clone()))
            .collect();
        for (index, asset) in units.into_values().enumerate() {
            fixture
                .chain
                .assets
                .insert(11 + index as i64 * 17 + (seed % 7) as i64, asset);
        }
        fixture.chain.assets.insert(
            9001,
            Asset {
                policy: "ab".repeat(28),
                name: "00".to_owned(),
            },
        );
        fixture.chain.assets.insert(
            9002,
            Asset {
                policy: fixture.profile.assets["permissioned_candidates"]
                    .policy
                    .clone(),
                name: "01".to_owned(),
            },
        );
        for height in 0..=bounds.tip {
            fixture.chain.blocks.insert(
                i64::from(height),
                Header {
                    epoch: Some((height / 2) as i32),
                    slot: Some(i64::from(height) * 100 + 1),
                },
            );
            fixture.chain.epochs.insert((height / 2) as i32);
        }
        fixture.chain.pools.extend([13, 29, 47]);
        for epoch in 0..=5 {
            for (ordinal, pool) in [13, 29, 47].into_iter().enumerate() {
                fixture.chain.stakes.insert(
                    101 + epoch as i64 * 10 + ordinal as i64,
                    Stake { epoch, pool },
                );
            }
        }
        let mut previous = BTreeMap::new();
        for height in 0..=bounds.target {
            for index in [0, 2, 4, 6] {
                fixture.ensure_transaction(height, index);
            }
            // Each selected transaction retains its full-block index, with gaps.
            for index in [0, 2] {
                let id = fixture.output(height, index, 9, "other", "cnight", None);
                fixture
                    .tags
                    .insert(format!("ordinary_{height}_{index}"), id);
            }
            if height % 2 == 0 {
                for (ordinal, body) in ["council", "technical_committee"].into_iter().enumerate() {
                    let id = fixture.output(
                        height,
                        0,
                        ordinal as u16,
                        body,
                        body,
                        Some(100 + height as i64 + ordinal as i64 * 20),
                    );
                    if let Some(old) = previous.insert(body, id) {
                        fixture.spend(height, 0, ordinal as u16, old);
                    }
                    fixture.tags.insert(format!("{body}_{height}"), id);
                }
                let id = fixture.output(
                    height,
                    2,
                    0,
                    "other",
                    "permissioned_candidates",
                    Some(200 + height as i64),
                );
                if let Some(old) = previous.insert("permissioned_candidates", id) {
                    fixture.spend(height, 2, 0, old);
                }
                fixture.tags.insert(format!("permissioned_{height}"), id);
            }
            let mapping = fixture.output(
                height,
                4,
                0,
                "mapping",
                "auth",
                Some(if random.next(3) == 0 {
                    301
                } else {
                    300 + height as i64
                }),
            );
            if height > 0 && random.next(2) == 0 {
                let old = fixture.tags[&format!("mapping_{}", height - 1)];
                fixture.spend(height, 4, 2, old);
            }
            fixture.tags.insert(format!("mapping_{height}"), mapping);
            let bridge = fixture.output(
                height,
                6,
                0,
                if height % 2 == 0 { "ics" } else { "reserve" },
                "bridge",
                Some(400 + height as i64),
            );
            if let Some(old) = previous.insert("bridge", bridge) {
                fixture.spend(height, 6, 3, old);
            }
            fixture.tags.insert(format!("bridge_{height}"), bridge);
            if height % 2 == 0 {
                fixture.chain.metadata.insert(
                    ids::transaction_id(height, 6).unwrap(),
                    ids::transaction_id(height, 6).unwrap(),
                );
            }
            if random.next(3) == 0 {
                let out = fixture.output(height, 6, 1, "ics", "cnight", Some(301));
                fixture.add_asset(out, 9001, 1);
            }
            if random.next(2) == 0 {
                let old = fixture.tags[&format!("ordinary_{height}_0")];
                let out = fixture.chain.outputs.get_mut(&old).unwrap();
                out.datum_hash = Some(hash(seed, 777));
                fixture.chain.datums.entry(777).or_insert(Datum {
                    hash: hash(seed, 777),
                });
            }
        }
        // Same policy, distinct name must never become the permissioned token.
        let out = fixture.output(3, 6, 2, "other", "cnight", Some(505));
        fixture.add_asset(out, 9002, 1);
        fixture.spend(5, 6, 2, out);
        fixture
            .tags
            .insert("wrong_permissioned_name".to_owned(), out);
        // Sparse, explicit source input ordinals are preserved in tx_in.id.
        let old = fixture.tags["ordinary_1_0"];
        fixture.spend(7, 4, 5, old);
        fixture.tags.insert("old_spent_in_window".to_owned(), old);
        if seed.is_multiple_of(11) {
            fixture.missing_governance_datum();
        }
        if seed.is_multiple_of(13) {
            fixture.chain.blocks.get_mut(&3).unwrap().slot = None;
        }
        if seed.is_multiple_of(17) {
            fixture.empty_epoch();
        }
        fixture.token_rule_edges();
        fixture.reindex_ordinals();
        fixture
    }

    /// Random topology with a small complete observation window. Old history,
    /// gaps, epoch lengths, source extent and payloads vary independently.
    pub fn generated(seed: u64) -> Self {
        let mut f = Self::base(seed);
        let mut random = Random(seed.wrapping_add(0x9e3779b97f4a7c15));
        let offset = random.next(9) as u32;
        let mut positions = BTreeMap::new();
        let mut height = offset;
        for old in 0..=10_i64 {
            positions.insert(old, height);
            height += 1 + random.next(3) as u32;
        }
        let target = positions[&8];
        let tip = target + 2;
        let horizon = target - random.next(3) as u32;
        let epoch_length = 1 + random.next(u64::from(tip + 5)) as u32;
        let mut transactions = BTreeMap::new();
        let mut tx_ids = BTreeMap::new();
        let mut tx_positions = BTreeMap::new();
        for (old, tx) in &f.chain.transactions {
            let index = tx.index + random.next(2) as u16;
            let block = positions[&tx.block];
            let id = ids::transaction_id(block, index).unwrap();
            tx_ids.insert(*old, id);
            tx_positions.insert(*old, (block, index));
            transactions.insert(
                id,
                Transaction {
                    block: i64::from(block),
                    index,
                },
            );
        }
        let mut outputs = BTreeMap::new();
        let mut out_ids = BTreeMap::new();
        for (old, output) in &f.chain.outputs {
            let (height, tx) = tx_positions[&output.transaction];
            let index = output.index + random.next(2) as u16 * 32;
            let id = ids::output_id(height, tx, index).unwrap();
            let mut output = output.clone();
            output.transaction = tx_ids[&output.transaction];
            output.index = index;
            out_ids.insert(*old, id);
            outputs.insert(id, output);
        }
        for input in f.chain.inputs.values_mut() {
            input.transaction = tx_ids[&input.transaction];
            input.output = out_ids[&input.output];
        }
        for row in f.chain.output_assets.values_mut() {
            row.output = out_ids[&row.output];
        }
        f.chain.metadata = f
            .chain
            .metadata
            .values()
            .map(|tx| (tx_ids[tx], tx_ids[tx]))
            .collect();
        for id in f.tags.values_mut() {
            *id = out_ids[id];
        }
        f.chain.transactions = transactions;
        f.chain.outputs = outputs;
        f.chain.blocks.clear();
        for block in 0..=tip {
            let mut epoch = Some((block / epoch_length) as i32);
            let mut slot = Some(i64::from(block) * 100 + 1);
            if block < horizon && random.next(19) == 0 {
                epoch = None;
            }
            if block < horizon && random.next(11) == 0 {
                slot = None;
            }
            f.chain
                .blocks
                .insert(i64::from(block), Header { epoch, slot });
        }
        f.bounds = Bounds {
            horizon,
            target,
            tip,
        };
        // Real source shapes can contain post-target event rows. K1 keeps these
        // headers while K2 must still end at T, including for future spends.
        let post = f.output(target + 1, 2, 0, "ics", "bridge", Some(600));
        let old = f.tags["ordinary_0_0"];
        f.spend(target + 1, 2, 0, old);
        f.tags.insert("post_target_output".to_owned(), post);
        if seed.is_multiple_of(7) {
            f.chain
                .transactions
                .retain(|_, tx| tx.block < i64::from(horizon) || tx.block > i64::from(target));
            f.chain
                .outputs
                .retain(|_, output| f.chain.transactions.contains_key(&output.transaction));
            f.chain.inputs.retain(|_, input| {
                f.chain.transactions.contains_key(&input.transaction)
                    && f.chain.outputs.contains_key(&input.output)
            });
            f.chain
                .output_assets
                .retain(|_, row| f.chain.outputs.contains_key(&row.output));
            f.chain
                .metadata
                .retain(|_, tx| f.chain.transactions.contains_key(tx));
        }
        f.chain.epochs = f
            .chain
            .blocks
            .values()
            .filter_map(|header| header.epoch)
            .collect();
        f.chain.stakes.clear();
        for epoch in &f.chain.epochs {
            for pool in &f.chain.pools {
                if random.next(5) != 0 {
                    f.chain.stakes.insert(
                        i64::from(*epoch) * 100 + pool,
                        Stake {
                            epoch: *epoch,
                            pool: *pool,
                        },
                    );
                }
            }
        }
        f.reindex_ordinals();
        f.quantities = f
            .chain
            .output_assets
            .keys()
            .map(|id| {
                let asset = &f.chain.assets[&f.chain.output_assets[id].asset];
                let singleton = ["council", "technical_committee", "permissioned_candidates"]
                    .iter()
                    .any(|role| f.profile.assets[*role] == *asset);
                (*id, if singleton { 1 } else { 1 + random.next(3) })
            })
            .collect();
        f
    }

    /// Every generated proof includes foreign units around eligible original
    /// ordinals at all address classes, plus the distinct Q4 datum boundary.
    pub fn token_rule_edges(&mut self) {
        for (id, policy, name) in [
            (9010, "00".repeat(28), "".to_owned()),
            (9011, "ff".repeat(28), "".to_owned()),
            (
                9012,
                self.profile.assets["council"].policy.clone(),
                "01".to_owned(),
            ),
        ] {
            self.chain.assets.insert(id, Asset { policy, name });
        }
        for (index, role) in ["ics", "reserve", "other"].into_iter().enumerate() {
            let out = self.output(8, 8, 20 + index as u16, role, "cnight", None);
            for (ordinal, role) in ["auth", "bridge"].into_iter().enumerate() {
                let asset = self.asset_id(role);
                if !self
                    .chain
                    .output_assets
                    .values()
                    .any(|a| a.output == out && a.asset == asset)
                {
                    self.add_asset(out, asset, 1 + ordinal as u16);
                }
            }
            for (ordinal, asset) in [9010, 9001, 9011].into_iter().enumerate() {
                self.add_asset(out, asset, 3 + ordinal as u16);
            }
            self.tags.insert(format!("v2_mixed_{role}"), out);
        }
        let ordinary = self.output(8, 10, 20, "other", "cnight", Some(90111));
        self.add_asset(ordinary, 9012, 1);
        self.tags
            .insert("v2_plain_governance_unique_datum".to_owned(), ordinary);
        let shared = self.output(8, 10, 21, "other", "cnight", Some(90112));
        self.add_asset(shared, 9012, 1);
        self.output(8, 10, 22, "mapping", "auth", Some(90112));
        self.tags
            .insert("v2_plain_governance_shared_datum".to_owned(), shared);
        let shell = self.output(8, 10, 23, "reserve", "cnight", None);
        self.chain.output_assets.retain(|_, a| a.output != shell);
        self.add_asset(shell, 9010, 0);
        self.tags.insert("v2_zero_eligible_shell".to_owned(), shell);
    }

    pub fn translate_heights(&mut self, offset: u32) {
        let tx_ids: BTreeMap<_, _> = self
            .chain
            .transactions
            .iter()
            .map(|(id, tx)| {
                (
                    *id,
                    ids::transaction_id(tx.block as u32 + offset, tx.index).unwrap(),
                )
            })
            .collect();
        let out_ids: BTreeMap<_, _> = self
            .chain
            .outputs
            .iter()
            .map(|(id, out)| (*id, tx_ids[&out.transaction] * 4096 + i64::from(out.index)))
            .collect();
        self.chain.blocks = std::mem::take(&mut self.chain.blocks)
            .into_iter()
            .map(|(id, header)| (id + i64::from(offset), header))
            .collect();
        self.chain.transactions = std::mem::take(&mut self.chain.transactions)
            .into_iter()
            .map(|(id, mut tx)| {
                tx.block += i64::from(offset);
                (tx_ids[&id], tx)
            })
            .collect();
        self.chain.outputs = std::mem::take(&mut self.chain.outputs)
            .into_iter()
            .map(|(id, mut output)| {
                output.transaction = tx_ids[&output.transaction];
                (out_ids[&id], output)
            })
            .collect();
        for input in self.chain.inputs.values_mut() {
            input.transaction = tx_ids[&input.transaction];
            input.output = out_ids[&input.output];
        }
        for row in self.chain.output_assets.values_mut() {
            row.output = out_ids[&row.output];
        }
        self.chain.metadata = self
            .chain
            .metadata
            .values()
            .map(|tx| (tx_ids[tx], tx_ids[tx]))
            .collect();
        for id in self.tags.values_mut() {
            *id = out_ids[id];
        }
        self.bounds.horizon += offset;
        self.bounds.target += offset;
        self.bounds.tip += offset;
        self.reindex_ordinals();
    }

    pub fn ensure_transaction(&mut self, height: u32, index: u16) -> i64 {
        let id = ids::transaction_id(height, index).unwrap();
        self.chain.transactions.entry(id).or_insert(Transaction {
            block: i64::from(height),
            index,
        });
        id
    }

    pub fn asset_id(&self, role: &str) -> i64 {
        let asset = &self.profile.assets[role];
        *self
            .chain
            .assets
            .iter()
            .find(|(_, candidate)| *candidate == asset)
            .unwrap()
            .0
    }

    pub fn output(
        &mut self,
        height: u32,
        tx_index: u16,
        index: u16,
        address_role: &str,
        asset_role: &str,
        datum: Option<i64>,
    ) -> i64 {
        let transaction = self.ensure_transaction(height, tx_index);
        let id = ids::output_id(height, tx_index, index).unwrap();
        let address = self
            .profile
            .addresses
            .get(address_role)
            .cloned()
            .unwrap_or_else(|| "addr_external_holder".to_owned());
        let datum_hash = datum.map(|datum| {
            let hash = hash(self.seed, datum);
            self.chain
                .datums
                .entry(datum)
                .or_insert(Datum { hash: hash.clone() });
            hash
        });
        self.chain.outputs.insert(
            id,
            Output {
                transaction,
                index,
                address,
                datum_hash,
            },
        );
        self.add_asset(id, self.asset_id(asset_role), 0);
        id
    }

    pub fn add_asset(&mut self, output: i64, asset: i64, ordinal: u16) {
        let (height, tx, index) = ids::output_position(output).unwrap();
        self.chain.output_assets.insert(
            ids::asset_id(height, tx, index, ordinal).unwrap(),
            OutputAsset { output, asset },
        );
    }

    pub fn spend(&mut self, height: u32, tx_index: u16, ordinal: u16, output: i64) {
        let transaction = self.ensure_transaction(height, tx_index);
        self.chain.inputs.insert(
            ids::input_id(height, tx_index, ordinal).unwrap(),
            Input {
                transaction,
                ordinal,
                output,
            },
        );
    }

    pub fn no_window_spend(&mut self) {
        let h = i64::from(self.bounds.horizon);
        self.chain
            .inputs
            .retain(|_, input| self.chain.transactions[&input.transaction].block < h);
    }

    pub fn missing_governance_datum(&mut self) {
        let previous = self.tags["council_4"];
        let current = self.output(5, 0, 0, "council", "council", None);
        self.chain.outputs.get_mut(&current).unwrap().datum_hash = Some(hash(self.seed, 999999));
        for input in self.chain.inputs.values_mut() {
            if input.output == previous {
                input.output = current;
            }
        }
        self.spend(5, 0, 0, previous);
        self.tags
            .insert("missing_governance_datum".to_owned(), current);
    }

    pub fn empty_epoch(&mut self) {
        for height in [2, 3] {
            self.chain.blocks.get_mut(&height).unwrap().epoch = Some(0);
        }
    }

    pub fn reindex_ordinals(&mut self) {
        let mut grouped: BTreeMap<i64, Vec<OutputAsset>> = BTreeMap::new();
        for row in self.chain.output_assets.values() {
            grouped.entry(row.output).or_default().push(row.clone());
        }
        self.chain.output_assets.clear();
        for (output, mut assets) in grouped {
            assets.sort_by_key(|row| self.chain.assets[&row.asset].unit());
            let (height, tx, index) = ids::output_position(output).unwrap();
            for (ordinal, row) in assets.into_iter().enumerate() {
                self.chain.output_assets.insert(
                    ids::asset_id(height, tx, index, ordinal as u16).unwrap(),
                    row,
                );
            }
        }
        let mut grouped: BTreeMap<i64, Vec<Input>> = BTreeMap::new();
        for row in self.chain.inputs.values() {
            grouped
                .entry(row.transaction)
                .or_default()
                .push(row.clone());
        }
        self.chain.inputs.clear();
        for (tx, mut inputs) in grouped {
            inputs.sort_by_key(|input| {
                let output = &self.chain.outputs[&input.output];
                (hash(self.seed, output.transaction + 20000), output.index)
            });
            let (height, tx_index) = ids::transaction_position(tx).unwrap();
            for (index, mut input) in inputs.into_iter().enumerate() {
                input.ordinal = (index * 2 + 1) as u16;
                self.chain.inputs.insert(
                    ids::input_id(height, tx_index, input.ordinal).unwrap(),
                    input,
                );
            }
        }
    }

    pub fn rows(&self) -> Rows {
        let c = &self.chain;
        let mut rows = BTreeMap::new();
        rows.insert("block".to_owned(),c.blocks.iter().map(|(id,b)|json!({"id":id,"hash":binary(&hash(self.seed,*id+10000)),"epoch_no":b.epoch,"slot_no":b.slot,"block_no":id,"time":format!("2026-01-{:02}T{:02}:{:02}:{:02}",1+(id/86400)%28,(id/3600)%24,(id/60)%60,id%60),"tx_count":c.transactions.values().filter(|tx|tx.block==*id).map(|tx|i64::from(tx.index)+1).max().unwrap_or(0)})).collect());
        rows.insert("tx".to_owned(),c.transactions.iter().map(|(id,t)|json!({"id":id,"hash":binary(&hash(self.seed,*id+20000)),"block_id":t.block,"block_index":t.index,"valid_contract":id%3!=0})).collect());
        rows.insert("tx_out".to_owned(),c.outputs.iter().map(|(id,o)|json!({"id":id,"tx_id":o.transaction,"index":o.index,"address":o.address,"value":id%1000+1,"data_hash":o.datum_hash.as_ref().map(|hash|binary(hash)),"consumed_by_tx_id":Value::Null})).collect());
        rows.insert("tx_in".to_owned(),c.inputs.iter().map(|(id,i)| {
            let out=&c.outputs[&i.output];json!({"id":id,"tx_in_id":i.transaction,"tx_out_id":out.transaction,"tx_out_index":out.index})
        }).collect());
        rows.insert(
            "ma_tx_out".to_owned(),
            c.output_assets
                .iter()
                .map(|(id, a)| json!({"id":id,"quantity":self.quantities.get(id).copied().unwrap_or(1),"tx_out_id":a.output,"ident":a.asset}))
                .collect(),
        );
        rows.insert(
            "multi_asset".to_owned(),
            c.assets
                .iter()
                .map(|(id, a)| json!({"id":id,"policy":binary(&a.policy),"name":binary(&a.name)}))
                .collect(),
        );
        rows.insert("datum".to_owned(),c.datums.iter().map(|(id,d)|json!({"id":id,"hash":binary(&d.hash),"value":if id%7==0 {Value::Null}else{json!({"constructor":0,"fields":[{"int":id}]})}})).collect());
        rows.insert("epoch_param".to_owned(),c.epochs.iter().map(|epoch|json!({"epoch_no":epoch,"nonce":binary(&hash(self.seed,i64::from(*epoch)+30000)),"max_tx_size":16384})).collect());
        rows.insert(
            "pool_hash".to_owned(),
            c.pools
                .iter()
                .map(|id| json!({"id":id,"hash_raw":binary(&hash(self.seed,*id+40000)[..56])}))
                .collect(),
        );
        rows.insert("epoch_stake".to_owned(),c.stakes.iter().map(|(id,s)|json!({"id":id,"pool_id":s.pool,"epoch_no":s.epoch,"amount":id+100000})).collect());
        rows.insert("tx_metadata".to_owned(),c.metadata.iter().map(|(id,tx)|json!({"id":id,"tx_id":tx,"key":6500973,"json":[{"msg":"generated bridge metadata"}]})).collect());
        rows
    }
}

pub fn selected(rows: &Rows, ids: &BTreeMap<String, BTreeSet<i64>>) -> Rows {
    rows.iter()
        .map(|(table, values)| {
            let key = if table == "epoch_param" {
                "epoch_no"
            } else {
                "id"
            };
            (
                table.clone(),
                values
                    .iter()
                    .filter(|row| ids[table].contains(&row[key].as_i64().unwrap()))
                    .cloned()
                    .collect(),
            )
        })
        .collect()
}
