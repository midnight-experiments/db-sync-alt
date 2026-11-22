//! Conway transactions encoded as CBOR. The hash is the Blake2b-256 of the
//! encoded body, so a built transaction passes the product decoder's check.

use std::collections::BTreeMap;

use pallas_crypto::hash::Hasher;

/// Hex Blake2b-256 of `bytes`.
pub fn hash(bytes: &[u8]) -> String {
    Hasher::<256>::hash(bytes).to_string()
}

fn head(out: &mut Vec<u8>, major: u8, value: u64) {
    let major = major << 5;
    match value {
        0..=23 => out.push(major | value as u8),
        24..=0xff => out.extend([major | 24, value as u8]),
        0x100..=0xffff => {
            out.push(major | 25);
            out.extend((value as u16).to_be_bytes());
        }
        0x1_0000..=0xffff_ffff => {
            out.push(major | 26);
            out.extend((value as u32).to_be_bytes());
        }
        _ => {
            out.push(major | 27);
            out.extend(value.to_be_bytes());
        }
    }
}

fn integer(out: &mut Vec<u8>, value: i128) {
    if value >= 0 {
        head(out, 0, u64::try_from(value).expect("unsigned CBOR integer"));
    } else {
        head(
            out,
            1,
            u64::try_from(-1 - value).expect("negative CBOR integer"),
        );
    }
}

fn bytes(out: &mut Vec<u8>, value: &[u8]) {
    head(out, 2, value.len() as u64);
    out.extend_from_slice(value);
}

fn decode_hex(value: &str, length: Option<usize>) -> Vec<u8> {
    let decoded = hex::decode(value).expect("hex test value");
    if let Some(length) = length {
        assert_eq!(decoded.len(), length, "hex test value length");
    }
    decoded
}

/// A token quantity: positive in outputs, either sign in a mint.
#[derive(Clone, Debug)]
pub struct Token {
    pub policy: String,
    pub name: String,
    pub quantity: i128,
}

impl Token {
    pub fn new(policy: &str, name: &str, quantity: i128) -> Self {
        Self {
            policy: policy.to_owned(),
            name: name.to_owned(),
            quantity,
        }
    }
}

fn multiasset(out: &mut Vec<u8>, tokens: &[Token]) {
    let mut policies: BTreeMap<Vec<u8>, Vec<(Vec<u8>, i128)>> = BTreeMap::new();
    for token in tokens {
        policies
            .entry(decode_hex(&token.policy, Some(28)))
            .or_default()
            .push((decode_hex(&token.name, None), token.quantity));
    }
    head(out, 5, policies.len() as u64);
    for (policy, names) in policies {
        bytes(out, &policy);
        head(out, 5, names.len() as u64);
        for (name, quantity) in names {
            bytes(out, &name);
            integer(out, quantity);
        }
    }
}

#[derive(Clone, Debug)]
pub enum Datum {
    None,
    Hash(String),
    Inline(Vec<u8>),
}

#[derive(Clone, Debug)]
pub struct Output {
    pub address: Vec<u8>,
    pub coin: u64,
    pub tokens: Vec<Token>,
    pub datum: Datum,
    /// Encode as the pre-Babbage array instead of the map form.
    pub legacy: bool,
}

impl Output {
    /// An output to raw address bytes.
    pub fn new(address: Vec<u8>, coin: u64) -> Self {
        Self {
            address,
            coin,
            tokens: Vec::new(),
            datum: Datum::None,
            legacy: false,
        }
    }

    /// An output to a Bech32 or Base58 address.
    pub fn to(address: &str, coin: u64) -> Self {
        let address: pallas_addresses::Address = address.parse().expect("test address");
        Self::new(address.to_vec(), coin)
    }

    pub fn token(mut self, policy: &str, name: &str, quantity: i128) -> Self {
        self.tokens.push(Token::new(policy, name, quantity));
        self
    }

    pub fn datum_hash(mut self, hash: &str) -> Self {
        self.datum = Datum::Hash(hash.to_owned());
        self
    }

    pub fn inline_datum(mut self, datum: Vec<u8>) -> Self {
        self.datum = Datum::Inline(datum);
        self
    }

    pub fn legacy(mut self) -> Self {
        self.legacy = true;
        self
    }

    fn encode(&self, out: &mut Vec<u8>) {
        let value = |out: &mut Vec<u8>| {
            if self.tokens.is_empty() {
                head(out, 0, self.coin);
            } else {
                head(out, 4, 2);
                head(out, 0, self.coin);
                multiasset(out, &self.tokens);
            }
        };
        if self.legacy {
            let hash = match &self.datum {
                Datum::None => None,
                Datum::Hash(hash) => Some(hash),
                Datum::Inline(_) => panic!("a legacy output has no inline datum"),
            };
            head(out, 4, if hash.is_some() { 3 } else { 2 });
            bytes(out, &self.address);
            value(out);
            if let Some(hash) = hash {
                bytes(out, &decode_hex(hash, Some(32)));
            }
            return;
        }
        head(
            out,
            5,
            if matches!(self.datum, Datum::None) {
                2
            } else {
                3
            },
        );
        head(out, 0, 0);
        bytes(out, &self.address);
        head(out, 0, 1);
        value(out);
        match &self.datum {
            Datum::None => {}
            Datum::Hash(hash) => {
                head(out, 0, 2);
                head(out, 4, 2);
                head(out, 0, 0);
                bytes(out, &decode_hex(hash, Some(32)));
            }
            Datum::Inline(datum) => {
                head(out, 0, 2);
                head(out, 4, 2);
                head(out, 0, 1);
                head(out, 6, 24);
                bytes(out, datum);
            }
        }
    }
}

/// A transaction input as hex transaction hash and output index.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Input {
    pub hash: String,
    pub index: u16,
}

fn inputs(out: &mut Vec<u8>, inputs: &[Input]) {
    head(out, 6, 258);
    head(out, 4, inputs.len() as u64);
    for input in inputs {
        head(out, 4, 2);
        bytes(out, &decode_hex(&input.hash, Some(32)));
        head(out, 0, u64::from(input.index));
    }
}

/// A built transaction: its hash and the complete CBOR envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Built {
    pub hash: String,
    pub cbor: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct Tx {
    inputs: Vec<Input>,
    collateral: Vec<Input>,
    outputs: Vec<Output>,
    collateral_return: Option<Output>,
    fee: u64,
    mint: Vec<Token>,
    datums: Vec<Vec<u8>>,
    metadata: Vec<(u64, Vec<u8>)>,
    valid: bool,
}

impl Default for Tx {
    fn default() -> Self {
        Self::new()
    }
}

impl Tx {
    pub fn new() -> Self {
        Self {
            inputs: Vec::new(),
            collateral: Vec::new(),
            outputs: Vec::new(),
            collateral_return: None,
            fee: 170_000,
            mint: Vec::new(),
            datums: Vec::new(),
            metadata: Vec::new(),
            valid: true,
        }
    }

    pub fn input(mut self, hash: &str, index: u16) -> Self {
        self.inputs.push(Input {
            hash: hash.to_owned(),
            index,
        });
        self
    }

    pub fn collateral(mut self, hash: &str, index: u16) -> Self {
        self.collateral.push(Input {
            hash: hash.to_owned(),
            index,
        });
        self
    }

    pub fn output(mut self, output: Output) -> Self {
        self.outputs.push(output);
        self
    }

    pub fn collateral_return(mut self, output: Output) -> Self {
        self.collateral_return = Some(output);
        self
    }

    /// A distinct fee also makes otherwise identical transactions distinct.
    pub fn fee(mut self, fee: u64) -> Self {
        self.fee = fee;
        self
    }

    pub fn mint(mut self, policy: &str, name: &str, quantity: i128) -> Self {
        self.mint.push(Token::new(policy, name, quantity));
        self
    }

    /// A Plutus datum in the witness set, as its CBOR bytes.
    pub fn witness_datum(mut self, datum: Vec<u8>) -> Self {
        self.datums.push(datum);
        self
    }

    /// One auxiliary metadatum, as the CBOR bytes of its value.
    pub fn metadata(mut self, label: u64, value: Vec<u8>) -> Self {
        self.metadata.push((label, value));
        self
    }

    /// Mark the transaction as failing phase 2: collateral is spent instead.
    pub fn invalid(mut self) -> Self {
        self.valid = false;
        self
    }

    pub fn body(&self) -> Vec<u8> {
        let auxiliary = self.auxiliary();
        let mut fields: Vec<(u64, Vec<u8>)> = Vec::new();
        let mut field = |key: u64, encode: &dyn Fn(&mut Vec<u8>)| {
            let mut value = Vec::new();
            encode(&mut value);
            fields.push((key, value));
        };
        field(0, &|out| inputs(out, &self.inputs));
        field(1, &|out| {
            head(out, 4, self.outputs.len() as u64);
            for output in &self.outputs {
                output.encode(out);
            }
        });
        field(2, &|out| head(out, 0, self.fee));
        if let Some(auxiliary) = &auxiliary {
            field(7, &|out| {
                bytes(out, &decode_hex(&hash(auxiliary), Some(32)))
            });
        }
        if !self.mint.is_empty() {
            field(9, &|out| multiasset(out, &self.mint));
        }
        if !self.collateral.is_empty() {
            field(13, &|out| inputs(out, &self.collateral));
        }
        if let Some(output) = &self.collateral_return {
            field(16, &|out| output.encode(out));
        }
        let mut body = Vec::new();
        head(&mut body, 5, fields.len() as u64);
        for (key, value) in fields {
            head(&mut body, 0, key);
            body.extend(value);
        }
        body
    }

    fn auxiliary(&self) -> Option<Vec<u8>> {
        if self.metadata.is_empty() {
            return None;
        }
        let mut out = Vec::new();
        head(&mut out, 5, self.metadata.len() as u64);
        for (label, value) in &self.metadata {
            head(&mut out, 0, *label);
            out.extend_from_slice(value);
        }
        Some(out)
    }

    pub fn build(&self) -> Built {
        let body = self.body();
        let mut cbor = Vec::with_capacity(body.len() + 64);
        head(&mut cbor, 4, 4);
        cbor.extend_from_slice(&body);
        if self.datums.is_empty() {
            head(&mut cbor, 5, 0);
        } else {
            head(&mut cbor, 5, 1);
            head(&mut cbor, 0, 4);
            head(&mut cbor, 4, self.datums.len() as u64);
            for datum in &self.datums {
                cbor.extend_from_slice(datum);
            }
        }
        cbor.push(if self.valid { 0xf5 } else { 0xf4 });
        match self.auxiliary() {
            Some(auxiliary) => cbor.extend(auxiliary),
            None => cbor.push(0xf6),
        }
        Built {
            hash: hash(&body),
            cbor,
        }
    }
}

/// Plutus data: an integer.
pub fn int_datum(value: i64) -> Vec<u8> {
    let mut out = Vec::new();
    integer(&mut out, i128::from(value));
    out
}

/// Plutus data: a list of 64-byte strings whose encoding is at least
/// `length` bytes long.
pub fn large_datum(length: usize) -> Vec<u8> {
    let count = length.div_ceil(66);
    let mut out = Vec::new();
    head(&mut out, 4, count as u64);
    for index in 0..count {
        bytes(&mut out, &[index as u8; 64]);
    }
    out
}

/// CBOR of a metadatum list holding one byte string, the bridge recipient form.
pub fn recipient_metadatum(recipient: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    head(&mut out, 4, 1);
    bytes(&mut out, recipient);
    out
}

fn variable_length(out: &mut Vec<u8>, mut value: u64) {
    let mut groups = vec![(value & 0x7f) as u8];
    value >>= 7;
    while value > 0 {
        groups.push((value & 0x7f) as u8 | 0x80);
        value >>= 7;
    }
    out.extend(groups.into_iter().rev());
}

/// A Shelley pointer address (header type 4) on the given network.
pub fn pointer_address(network: u8, payment_key: [u8; 28], pointer: (u64, u64, u64)) -> Vec<u8> {
    let mut out = vec![0x40 | network];
    out.extend(payment_key);
    variable_length(&mut out, pointer.0);
    variable_length(&mut out, pointer.1);
    variable_length(&mut out, pointer.2);
    out
}

/// A Shelley enterprise address (header type 6) on the given network.
pub fn enterprise_address(network: u8, payment_key: [u8; 28]) -> Vec<u8> {
    let mut out = vec![0x60 | network];
    out.extend(payment_key);
    out
}
