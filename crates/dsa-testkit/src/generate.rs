//! Seeded chains of built transactions for the follower's tests. Every token
//! comes from a mint and every transaction conserves all of its tokens, so a
//! correct block function never refuses a block. A chain spans several
//! epochs and contains ordinary and unrelated transactions, spends of outputs
//! created in the same block, phase-2-invalid transactions with a collateral
//! return, mints and burns, governance records that are replaced or spent,
//! permissioned-candidate outputs in several epochs, hashed datums revealed
//! only when the output is spent, bridge metadata at the ICS address, quiet
//! stretches without a spend of a selected output, empty blocks, outputs at
//! pointer addresses and datums above 10,000 bytes.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    chain::Block,
    tx::{
        self, Built, Output, Tx, enterprise_address, hash, int_datum, large_datum, pointer_address,
        recipient_metadatum,
    },
};

/// What a profile selects, as plain values.
#[derive(Clone, Debug)]
pub struct Roles {
    /// The network nibble of generated addresses (1 Mainnet, 0 test networks).
    pub network: u8,
    /// Profile role -> Bech32 address.
    pub addresses: BTreeMap<String, String>,
    /// Profile role -> (policy, name), lowercase hexadecimal.
    pub units: BTreeMap<String, (String, String)>,
    /// Units kept on outputs, as policy followed by name.
    pub eligible_units: BTreeSet<String>,
    /// Policies whose every unit is kept on outputs.
    pub governance_policies: BTreeSet<String>,
}

#[derive(Clone, Debug)]
pub struct Settings {
    pub seed: u64,
    pub first_height: u32,
    pub blocks: u32,
    /// The most transactions in one block.
    pub transactions: usize,
    pub large_datums: bool,
}

/// How often each shape occurs in a chain.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Counts {
    pub transactions: usize,
    pub invalid: usize,
    pub same_block_spends: usize,
    pub mints: usize,
    pub burns: usize,
    pub governance_replaced: usize,
    pub governance_spent: usize,
    pub permissioned_moves: usize,
    pub revealed_at_spend: usize,
    pub bridge_metadata: usize,
    pub empty_blocks: usize,
    pub quiet_blocks: usize,
    pub pointer_outputs: usize,
    pub large_datums: usize,
}

pub struct Generated {
    pub blocks: Vec<Block>,
    /// Datum bytes by hash and the height at which they first appear on chain.
    pub datums: BTreeMap<String, (u32, Vec<u8>)>,
    /// For each transaction with an output at the ICS address, the provider's
    /// rendering of its bridge metadatum (`None`: it has none).
    pub metadata: BTreeMap<String, Option<String>>,
    /// Epoch -> (nonce, max_tx_size).
    pub epochs: BTreeMap<u32, (String, u32)>,
    /// Epoch -> pools (hash of 28 bytes, amount).
    pub stake: BTreeMap<u32, Vec<(String, u64)>>,
    pub counts: Counts,
}

impl Generated {
    /// The datum the provider returns while the block at `height` is read.
    pub fn datum(&self, hash: &str, height: u32) -> Option<&[u8]> {
        self.datums
            .get(hash)
            .filter(|(first, _)| *first <= height)
            .map(|(_, bytes)| bytes.as_slice())
    }

    pub fn block(&self, height: u32) -> Option<&Block> {
        let first = self.blocks.first()?.height;
        self.blocks.get(height.checked_sub(first)? as usize)
    }
}

/// Another header at the same height, epoch and slot, with another hash and
/// other transactions: a branch the provider abandons later.
pub fn alternative(block: &Block, salt: u64, transactions: usize) -> Block {
    let transactions: Vec<Built> = (0..transactions)
        .map(|index| {
            crate::chain::filler((salt << 32) ^ (u64::from(block.height) << 8) ^ index as u64)
        })
        .collect();
    let mut seed = format!("alternative {salt} {}", block.hash).into_bytes();
    for tx in &transactions {
        seed.extend(tx.hash.as_bytes());
    }
    Block {
        hash: hash(&seed),
        transactions,
        ..block.clone()
    }
}

struct Random(u64);
impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
    fn below(&mut self, limit: u64) -> u64 {
        self.next() % limit.max(1)
    }
    fn chance(&mut self, one_in: u64) -> bool {
        self.below(one_in) == 0
    }
}

type Point = (String, u16);
type Unit = (String, String);

#[derive(Clone, Debug, PartialEq, Eq)]
enum Place {
    Role(String),
    Ordinary(u8),
    Pointer(u8),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Special {
    Plain,
    Governance,
    Permissioned,
}

#[derive(Clone, Debug)]
enum Datum {
    None,
    Inline(Vec<u8>),
    /// Hash in the output, bytes in the creating transaction's witness set.
    Witness(Vec<u8>),
    /// Hash only; the bytes appear in the transaction that spends it.
    Hidden(Vec<u8>),
}

#[derive(Clone, Debug)]
struct Utxo {
    place: Place,
    coin: u64,
    tokens: BTreeMap<Unit, i128>,
    datum: Datum,
    special: Special,
}

struct Generator<'r> {
    roles: &'r Roles,
    random: Random,
    seed: u64,
    counter: u64,
    height: u32,
    utxos: BTreeMap<Point, Utxo>,
    created_here: BTreeSet<Point>,
    datums: BTreeMap<String, (u32, Vec<u8>)>,
    metadata: BTreeMap<String, Option<String>>,
    governance: BTreeMap<String, (bool, Option<Point>)>,
    permissioned: (bool, Option<Point>, u32),
    epoch: u32,
    counts: Counts,
}

/// A transaction under construction and the outputs it creates.
struct Draft {
    tx: Tx,
    spent: Vec<Point>,
    outputs: Vec<Utxo>,
    collateral_return: Option<Utxo>,
    witness: Vec<Vec<u8>>,
    ics_metadata: Option<Vec<u8>>,
    /// Net mint per unit: a mint map holds each unit once.
    mint: BTreeMap<Unit, i128>,
}

impl Draft {
    fn new(fee: u64) -> Self {
        Self {
            tx: Tx::new().fee(fee),
            spent: Vec::new(),
            outputs: Vec::new(),
            collateral_return: None,
            witness: Vec::new(),
            ics_metadata: None,
            mint: BTreeMap::new(),
        }
    }
}

const MINTED: [&str; 3] = ["cnight", "auth", "bridge"];

impl Generator<'_> {
    fn unit(&self, role: &str) -> Unit {
        self.roles.units[role].clone()
    }

    fn eligible(&self, unit: &Unit) -> bool {
        self.roles.governance_policies.contains(&unit.0)
            || self
                .roles
                .eligible_units
                .contains(&format!("{}{}", unit.0, unit.1))
    }

    fn relevant(&self, utxo: &Utxo) -> bool {
        matches!(utxo.place, Place::Role(_)) || utxo.tokens.keys().any(|unit| self.eligible(unit))
    }

    fn unique(&mut self) -> u64 {
        self.counter += 1;
        self.counter
    }

    fn funding(&mut self) -> String {
        let n = self.unique();
        hash(format!("funding {} {n}", self.seed).as_bytes())
    }

    fn foreign(&mut self) -> Unit {
        ("ab".repeat(28), format!("{:02x}", self.random.below(3)))
    }

    fn address(&self, place: &Place) -> Vec<u8> {
        let key = |index: u8| {
            let mut key = [0x5a; 28];
            key[0] = index;
            key
        };
        match place {
            Place::Role(role) => {
                let address: pallas_addresses::Address =
                    self.roles.addresses[role].parse().expect("profile address");
                address.to_vec()
            }
            Place::Ordinary(index) => enterprise_address(self.roles.network, key(*index)),
            Place::Pointer(index) => pointer_address(
                self.roles.network,
                key(*index),
                (u64::from(self.height), u64::from(*index), 0),
            ),
        }
    }

    fn output(&self, utxo: &Utxo) -> Output {
        let mut output = Output::new(self.address(&utxo.place), utxo.coin);
        for ((policy, name), quantity) in &utxo.tokens {
            output = output.token(policy, name, *quantity);
        }
        match &utxo.datum {
            Datum::None => output,
            Datum::Inline(bytes) => output.inline_datum(bytes.clone()),
            Datum::Witness(bytes) | Datum::Hidden(bytes) => output.datum_hash(&hash(bytes)),
        }
    }

    /// A random place for a new output: a profile role or an ordinary address.
    fn place(&mut self) -> Place {
        match self.random.below(6) {
            0 => Place::Role("mapping".to_owned()),
            1 => Place::Role("ics".to_owned()),
            2 => Place::Role("reserve".to_owned()),
            _ => Place::Ordinary(self.random.below(4) as u8),
        }
    }

    /// A datum for an output at `place`: shared small values are common.
    fn datum(&mut self, place: &Place) -> Datum {
        if !matches!(place, Place::Role(_)) {
            return Datum::None;
        }
        let bytes = int_datum(self.random.below(40) as i64);
        match self.random.below(4) {
            0 => Datum::None,
            1 => Datum::Inline(bytes),
            _ => Datum::Witness(bytes),
        }
    }

    fn utxo(&mut self, place: Place, tokens: BTreeMap<Unit, i128>) -> Utxo {
        let datum = self.datum(&place);
        Utxo {
            place,
            coin: 1_000_000 + self.random.below(5_000_000),
            tokens,
            datum,
            special: Special::Plain,
        }
    }

    fn spend(&mut self, draft: &mut Draft, point: &Point, collateral: bool) -> Utxo {
        let utxo = self.utxos.remove(point).expect("spent output exists");
        if self.created_here.contains(point) {
            self.counts.same_block_spends += 1;
        }
        if let Datum::Hidden(bytes) = &utxo.datum {
            draft.witness.push(bytes.clone());
            self.counts.revealed_at_spend += 1;
        }
        draft.tx = if collateral {
            std::mem::take(&mut draft.tx).collateral(&point.0, point.1)
        } else {
            std::mem::take(&mut draft.tx).input(&point.0, point.1)
        };
        draft.spent.push(point.clone());
        utxo
    }

    fn mint(&mut self, draft: &mut Draft, unit: &Unit, quantity: i128) {
        *draft.mint.entry(unit.clone()).or_default() += quantity;
    }

    /// Relevant plain outputs, optionally only those created in this block.
    fn candidates(&self) -> Vec<Point> {
        self.utxos
            .iter()
            .filter(|(_, utxo)| utxo.special == Special::Plain && self.relevant(utxo))
            .map(|(point, _)| point.clone())
            .collect()
    }

    fn pick(&mut self, count: usize) -> Vec<Point> {
        let all = self.candidates();
        let here: Vec<_> = all
            .iter()
            .filter(|point| self.created_here.contains(*point))
            .cloned()
            .collect();
        let mut picked = Vec::new();
        for _ in 0..count {
            let pool = if !here.is_empty() && self.random.chance(3) {
                &here
            } else {
                &all
            };
            if pool.is_empty() {
                break;
            }
            let point = pool[self.random.below(pool.len() as u64) as usize].clone();
            if !picked.contains(&point) {
                picked.push(point);
            }
        }
        picked
    }

    /// Splits `tokens` over `count` outputs at random places.
    fn split(&mut self, tokens: &BTreeMap<Unit, i128>, count: usize) -> Vec<Utxo> {
        let mut parts = vec![BTreeMap::new(); count];
        for (unit, quantity) in tokens {
            let mut left = *quantity;
            for (index, part) in parts.iter_mut().enumerate() {
                let share = if index + 1 == count {
                    left
                } else {
                    (self.random.below(left as u64 + 1)) as i128
                };
                if share > 0 {
                    part.insert(unit.clone(), share);
                }
                left -= share;
            }
        }
        parts
            .into_iter()
            .map(|tokens| {
                let place = self.place();
                self.utxo(place, tokens)
            })
            .collect()
    }

    fn fund(&mut self, draft: &mut Draft) {
        let funding = self.funding();
        draft.tx = std::mem::take(&mut draft.tx).input(&funding, 0);
        let mut tokens = BTreeMap::new();
        for role in MINTED {
            if self.random.chance(2) || role == "auth" {
                let unit = self.unit(role);
                let quantity = 1 + self.random.below(20) as i128;
                self.mint(draft, &unit, quantity);
                *tokens.entry(unit).or_default() += quantity;
            }
        }
        let count = 1 + self.random.below(2) as usize;
        draft.outputs.extend(self.split(&tokens, count));
        if self.random.chance(4) {
            // A plain output at a profile address: selected, token-less.
            draft.outputs.push(Utxo {
                place: Place::Role("reserve".to_owned()),
                coin: 1_200_000,
                tokens: BTreeMap::new(),
                datum: Datum::None,
                special: Special::Plain,
            });
        }
    }

    fn transfer(&mut self, draft: &mut Draft) {
        let count = 1 + self.random.below(2) as usize;
        let inputs = self.pick(count);
        if inputs.is_empty() {
            return self.fund(draft);
        }
        let mut tokens: BTreeMap<Unit, i128> = BTreeMap::new();
        for point in &inputs {
            let utxo = self.spend(draft, point, false);
            for (unit, quantity) in utxo.tokens {
                *tokens.entry(unit).or_default() += quantity;
            }
        }
        if self.random.chance(4) {
            let burnable: Vec<Unit> = MINTED
                .iter()
                .map(|role| self.unit(role))
                .filter(|unit| tokens.get(unit).is_some_and(|q| *q > 0))
                .collect();
            if let Some(unit) = burnable.first().cloned() {
                let held = tokens[&unit];
                let burn = 1 + self.random.below(held as u64) as i128;
                self.mint(draft, &unit, -burn);
                *tokens.get_mut(&unit).unwrap() -= burn;
                tokens.retain(|_, quantity| *quantity > 0);
            }
        }
        if self.random.chance(5) {
            let unit = self.unit("cnight");
            let quantity = 1 + self.random.below(5) as i128;
            self.mint(draft, &unit, quantity);
            *tokens.entry(unit).or_default() += quantity;
        }
        if self.random.chance(4) {
            let unit = self.foreign();
            self.mint(draft, &unit, 7);
            *tokens.entry(unit).or_default() += 7;
        }
        let count = 1 + self.random.below(3) as usize;
        draft.outputs.extend(self.split(&tokens, count));
    }

    fn invalid(&mut self, draft: &mut Draft) {
        let collateral = self.pick(1);
        let Some(point) = collateral.first() else {
            return self.fund(draft);
        };
        self.counts.invalid += 1;
        let funding = self.funding();
        draft.tx = std::mem::take(&mut draft.tx).input(&funding, 0).invalid();
        let utxo = self.spend(draft, point, true);
        // Body outputs and the mint take no effect.
        let ignored = self.place();
        draft.outputs.push(Utxo {
            place: ignored,
            coin: 2_000_000,
            tokens: BTreeMap::from([(self.unit("cnight"), 3)]),
            datum: Datum::None,
            special: Special::Plain,
        });
        let unit = self.unit("auth");
        self.mint(draft, &unit, 5);
        let place = self.place();
        draft.collateral_return = Some(self.utxo(place, utxo.tokens));
    }

    fn governance(&mut self, draft: &mut Draft) {
        let body = if self.random.chance(2) {
            "council"
        } else {
            "technical_committee"
        };
        let (minted, record) = self.governance[body].clone();
        let datum = Datum::Inline(int_datum(1_000 + self.random.below(20) as i64));
        let at_body = |tokens| Utxo {
            place: Place::Role(body.to_owned()),
            coin: 2_000_000,
            tokens,
            datum: datum.clone(),
            special: Special::Governance,
        };
        if !minted {
            let funding = self.funding();
            draft.tx = std::mem::take(&mut draft.tx).input(&funding, 0);
            let unit = self.unit(body);
            let extra = (unit.0.clone(), "01".to_owned());
            self.mint(draft, &unit, 1);
            self.mint(draft, &extra, 1);
            draft
                .outputs
                .push(at_body(BTreeMap::from([(unit, 1), (extra, 1)])));
            self.governance.insert(body.to_owned(), (true, None));
            return;
        }
        let point = record.expect("a minted governance token has a holder");
        let at_address = matches!(&self.utxos[&point].place, Place::Role(role) if role == body);
        let utxo = self.spend(draft, &point, false);
        if at_address && self.random.chance(3) {
            // The record is spent: the tokens leave the governance address.
            self.counts.governance_spent += 1;
            draft.outputs.push(Utxo {
                place: Place::Ordinary(9),
                coin: 2_000_000,
                tokens: utxo.tokens,
                datum: Datum::None,
                special: Special::Governance,
            });
        } else {
            if at_address {
                self.counts.governance_replaced += 1;
            }
            draft.outputs.push(at_body(utxo.tokens));
        }
    }

    fn permissioned(&mut self, draft: &mut Draft) {
        let (minted, holder, epoch) = self.permissioned.clone();
        if minted && epoch == self.epoch {
            return self.transfer(draft);
        }
        let unit = self.unit("permissioned_candidates");
        let mut tokens = BTreeMap::from([(unit.clone(), 1)]);
        if let Some(point) = holder.filter(|_| minted) {
            tokens = self.spend(draft, &point, false).tokens;
            self.counts.permissioned_moves += 1;
        } else {
            let funding = self.funding();
            draft.tx = std::mem::take(&mut draft.tx).input(&funding, 0);
            self.mint(draft, &unit, 1);
        }
        let place = if self.random.chance(2) {
            Place::Role("mapping".to_owned())
        } else {
            Place::Ordinary(7)
        };
        draft.outputs.push(Utxo {
            place,
            coin: 2_000_000,
            tokens,
            datum: Datum::Inline(int_datum(5_000 + i64::from(self.epoch))),
            special: Special::Permissioned,
        });
        self.permissioned = (true, None, self.epoch);
    }

    fn reveal_later(&mut self, draft: &mut Draft) {
        let funding = self.funding();
        draft.tx = std::mem::take(&mut draft.tx).input(&funding, 0);
        let unit = self.unit("auth");
        self.mint(draft, &unit, 2);
        let n = self.unique();
        draft.outputs.push(Utxo {
            place: Place::Role("mapping".to_owned()),
            coin: 1_500_000,
            tokens: BTreeMap::from([(unit, 2)]),
            datum: Datum::Hidden(int_datum(1_000_000_000 + n as i64)),
            special: Special::Plain,
        });
    }

    fn bridge(&mut self, draft: &mut Draft) {
        let funding = self.funding();
        draft.tx = std::mem::take(&mut draft.tx).input(&funding, 0);
        let role = if self.random.chance(2) {
            "cnight"
        } else {
            "bridge"
        };
        let unit = self.unit(role);
        let quantity = 1 + self.random.below(9) as i128;
        self.mint(draft, &unit, quantity);
        let mut utxo = self.utxo(
            Place::Role("ics".to_owned()),
            BTreeMap::from([(unit, quantity)]),
        );
        if matches!(utxo.datum, Datum::Witness(_)) {
            utxo.datum = Datum::Inline(int_datum(77));
        }
        draft.outputs.push(utxo);
        if self.random.chance(4) {
            return;
        }
        let length = 1 + self.random.below(32) as usize;
        let recipient: Vec<u8> = (0..length).map(|_| self.random.below(256) as u8).collect();
        draft.ics_metadata = Some(recipient);
    }

    fn unrelated(&mut self, draft: &mut Draft) {
        let funding = self.funding();
        draft.tx = std::mem::take(&mut draft.tx).input(&funding, 0);
        let mut tokens = BTreeMap::new();
        if self.random.chance(2) {
            let unit = self.foreign();
            self.mint(draft, &unit, 3);
            tokens.insert(unit, 3);
        }
        let place = Place::Ordinary(self.random.below(4) as u8);
        draft.outputs.push(self.utxo(place, tokens));
    }

    fn pointer(&mut self, draft: &mut Draft) {
        let funding = self.funding();
        draft.tx = std::mem::take(&mut draft.tx).input(&funding, 0);
        let unit = self.unit("cnight");
        self.mint(draft, &unit, 4);
        self.counts.pointer_outputs += 1;
        let place = Place::Pointer(self.random.below(3) as u8);
        draft
            .outputs
            .push(self.utxo(place, BTreeMap::from([(unit, 4)])));
    }

    fn large(&mut self, draft: &mut Draft) {
        let funding = self.funding();
        draft.tx = std::mem::take(&mut draft.tx).input(&funding, 0);
        let n = self.unique() as usize;
        self.counts.large_datums += 1;
        draft.outputs.push(Utxo {
            place: Place::Role("mapping".to_owned()),
            coin: 3_000_000,
            tokens: BTreeMap::new(),
            datum: Datum::Inline(large_datum(10_100 + 66 * (n % 50))),
            special: Special::Plain,
        });
    }

    fn reveal(&mut self, bytes: &[u8]) {
        let height = self.height;
        self.datums
            .entry(hash(bytes))
            .or_insert_with(|| (height, bytes.to_vec()));
    }

    /// Builds the draft and applies it to the outputs.
    fn finish(&mut self, mut draft: Draft) -> Built {
        let invalid = draft.collateral_return.is_some();
        let mut tx = std::mem::take(&mut draft.tx);
        for (unit, quantity) in std::mem::take(&mut draft.mint) {
            if quantity == 0 {
                continue;
            }
            tx = tx.mint(&unit.0, &unit.1, quantity);
            if !invalid && self.eligible(&unit) {
                if quantity > 0 {
                    self.counts.mints += 1;
                } else {
                    self.counts.burns += 1;
                }
            }
        }
        let mut ics = false;
        for utxo in &draft.outputs {
            tx = tx.output(self.output(utxo));
            ics |= !invalid && utxo.place == Place::Role("ics".to_owned());
            match &utxo.datum {
                Datum::Inline(bytes) => self.reveal(bytes),
                Datum::Witness(bytes) => {
                    draft.witness.push(bytes.clone());
                }
                _ => {}
            }
        }
        if let Some(utxo) = &draft.collateral_return {
            tx = tx.collateral_return(self.output(utxo));
            match &utxo.datum {
                Datum::Inline(bytes) => self.reveal(bytes),
                Datum::Witness(bytes) => draft.witness.push(bytes.clone()),
                _ => {}
            }
        }
        draft.witness.sort();
        draft.witness.dedup();
        for bytes in &draft.witness {
            tx = tx.witness_datum(bytes.clone());
            self.reveal(bytes);
        }
        if let Some(recipient) = &draft.ics_metadata {
            tx = tx.metadata(6_500_973, recipient_metadatum(recipient));
        }
        let built = tx.build();
        if ics {
            let rendering = draft
                .ics_metadata
                .as_ref()
                .map(|recipient| format!("[\"0x{}\"]", hex::encode(recipient)));
            self.counts.bridge_metadata += usize::from(rendering.is_some());
            self.metadata.insert(built.hash.clone(), rendering);
        }
        let created: Vec<(u16, Utxo)> = if invalid {
            vec![(
                draft.outputs.len() as u16,
                draft.collateral_return.take().expect("return"),
            )]
        } else {
            draft
                .outputs
                .into_iter()
                .enumerate()
                .map(|(index, utxo)| (index as u16, utxo))
                .collect()
        };
        for (index, utxo) in created {
            let point = (built.hash.clone(), index);
            match utxo.special {
                Special::Governance => {
                    let body = match &utxo.place {
                        Place::Role(role) => role.clone(),
                        _ => self
                            .governance
                            .iter()
                            .find(|(_, (_, record))| {
                                record.as_ref().is_some_and(|r| draft.spent.contains(r))
                            })
                            .map(|(body, _)| body.clone())
                            .unwrap_or_else(|| {
                                let unit = utxo.tokens.keys().next().expect("governance token");
                                self.roles
                                    .units
                                    .iter()
                                    .find(|(role, u)| {
                                        u.0 == unit.0
                                            && (*role == "council"
                                                || *role == "technical_committee")
                                    })
                                    .map(|(role, _)| role.clone())
                                    .expect("governance body")
                            }),
                    };
                    self.governance.insert(body, (true, Some(point.clone())));
                }
                Special::Permissioned => self.permissioned.1 = Some(point.clone()),
                Special::Plain => {}
            }
            self.created_here.insert(point.clone());
            self.utxos.insert(point, utxo);
        }
        self.counts.transactions += 1;
        built
    }
}

/// Generates a chain. Heights run from `first_height`; every block has an
/// epoch (6 to 12 blocks each) and a slot; the first two blocks hold a mint
/// and a spend of it.
pub fn generate(roles: &Roles, settings: &Settings) -> Generated {
    let mut g = Generator {
        roles,
        random: Random(settings.seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1),
        seed: settings.seed,
        counter: 0,
        height: settings.first_height,
        utxos: BTreeMap::new(),
        created_here: BTreeSet::new(),
        datums: BTreeMap::new(),
        metadata: BTreeMap::new(),
        governance: ["council", "technical_committee"]
            .into_iter()
            .map(|body| (body.to_owned(), (false, None)))
            .collect(),
        permissioned: (false, None, 0),
        epoch: 100 + (settings.seed % 50) as u32,
        counts: Counts::default(),
    };
    let mut blocks: Vec<Block> = Vec::new();
    let mut epochs = BTreeMap::new();
    let mut stake = BTreeMap::new();
    let mut epoch_left = 6 + g.random.below(7) as u32;
    let mut slot = 1_000 * u64::from(settings.first_height) + g.random.below(1_000);
    let mut quiet = 0;
    for offset in 0..settings.blocks {
        let height = settings.first_height + offset;
        g.height = height;
        g.created_here.clear();
        if epoch_left == 0 {
            g.epoch += 1;
            epoch_left = 6 + g.random.below(7) as u32;
        }
        epoch_left -= 1;
        epochs.entry(g.epoch).or_insert_with(|| {
            (
                hash(format!("nonce {} {}", settings.seed, g.epoch).as_bytes()),
                16_384 + g.epoch,
            )
        });
        let pools = 2 + g.random.below(3) as usize;
        let first_pool = g.random.below(3);
        stake.entry(g.epoch).or_insert_with(|| {
            (0..pools as u64)
                .map(|n| {
                    let pool = hash(format!("pool {}", first_pool + n).as_bytes())[..56].to_owned();
                    (
                        pool,
                        1_000_000 + (first_pool + n) * 1_000 + u64::from(height),
                    )
                })
                .collect::<Vec<_>>()
        });
        slot += 1 + g.random.below(40);
        if quiet > 0 {
            quiet -= 1;
        } else if offset > 3 && g.random.chance(10) {
            quiet = 3 + g.random.below(8);
        }
        let count = match offset {
            0 | 1 => 1,
            _ if g.random.chance(7) => 0,
            _ => 1 + g.random.below(settings.transactions as u64) as usize,
        };
        if count == 0 {
            g.counts.empty_blocks += 1;
        }
        if quiet > 0 {
            g.counts.quiet_blocks += 1;
        }
        let mut transactions = Vec::new();
        for _ in 0..count {
            let fee = 170_000 + g.unique();
            let mut draft = Draft::new(fee);
            match offset {
                0 => g.fund(&mut draft),
                1 => g.transfer(&mut draft),
                _ if quiet > 0 => match g.random.below(5) {
                    0 => g.unrelated(&mut draft),
                    1 => g.pointer(&mut draft),
                    2 => g.bridge(&mut draft),
                    3 if settings.large_datums && g.random.chance(6) => g.large(&mut draft),
                    _ => g.fund(&mut draft),
                },
                _ => match g.random.below(24) {
                    0..=2 => g.fund(&mut draft),
                    3..=9 => g.transfer(&mut draft),
                    10..=11 => g.invalid(&mut draft),
                    12..=14 => g.governance(&mut draft),
                    15 => g.permissioned(&mut draft),
                    16 => g.reveal_later(&mut draft),
                    17..=18 => g.bridge(&mut draft),
                    19..=20 => g.unrelated(&mut draft),
                    21 => g.pointer(&mut draft),
                    22 if settings.large_datums && g.random.chance(4) => g.large(&mut draft),
                    _ => g.transfer(&mut draft),
                },
            }
            transactions.push(g.finish(draft));
        }
        let previous = blocks.last().map(|block| block.hash.clone());
        let mut seed = format!("block {} {height} {:?}", settings.seed, previous).into_bytes();
        for tx in &transactions {
            seed.extend(tx.hash.as_bytes());
        }
        blocks.push(Block {
            hash: hash(&seed),
            height,
            slot,
            epoch: g.epoch,
            time: 1_596_059_091 + slot,
            previous,
            transactions,
        });
    }
    Generated {
        blocks,
        datums: g.datums,
        metadata: g.metadata,
        epochs,
        stake,
        counts: g.counts,
    }
}

/// The hex Blake2b-256 of a datum's bytes, as an output's datum hash.
pub fn datum_hash(bytes: &[u8]) -> String {
    tx::hash(bytes)
}
