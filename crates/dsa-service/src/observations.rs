//! First stored real-shape captures. Database-local uniqueness is the store
//! boundary; selected rows, raw bytes and provenance share one transaction.
use dsa_chain::{
    checks::{self, Header},
    ids,
    observations::{Fact, Shape, Source},
    profiles::Profile,
    rows::{self, HeldOutput, PreparedBlock, Rows, Supplements},
    transaction::Outpoint,
};
use dsa_provider::{
    Endpoint, Reply,
    evidence::{CapturedReply, RawResponse},
    pages::CapturedBlock,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{Read, Write},
};
use tokio_postgres::{Client, Transaction};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Invalid(&'static str),
    #[error(transparent)]
    Build(#[from] rows::BuildError),
    #[error(transparent)]
    Check(#[from] checks::CheckError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Sql(#[from] tokio_postgres::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseRef {
    pub source: String,
    pub endpoint: String,
    pub response_sha256: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Observation {
    pub block_height: u32,
    pub block_hash: String,
    pub transaction_hash: String,
    pub transaction_index: u16,
    pub observed_at: u64,
    pub output_index: Option<u16>,
    pub datum_hash: Option<String>,
    pub raw_datum_bytes: Option<usize>,
    pub metadata_key: Option<u64>,
    pub sources: Vec<ResponseRef>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct FieldEvidence {
    pub historical_validation: &'static str,
    pub first_stored_observation: Option<Observation>,
}
/// Load afresh after a successful commit or restart. Pending candidates never
/// enter this status representation. Historical proof is not self-certified.
pub async fn status(client: &Client) -> Result<BTreeMap<Shape, FieldEvidence>, Error> {
    let mut result = Shape::ALL
        .into_iter()
        .map(|shape| {
            (
                shape,
                FieldEvidence {
                    historical_validation: "not_proven_on_real_data",
                    first_stored_observation: None,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    for row in client
        .query(
            "SELECT shape,observation FROM dsa_follower.first_witness ORDER BY shape",
            &[],
        )
        .await?
    {
        let name: String = row.get(0);
        let shape = Shape::ALL
            .into_iter()
            .find(|shape| shape.as_str() == name)
            .ok_or(Error::Invalid("invalid_stored_shape"))?;
        result.get_mut(&shape).unwrap().first_stored_observation =
            Some(serde_json::from_value(row.get(1))?);
    }
    Ok(result)
}
pub fn pending(status: &BTreeMap<Shape, FieldEvidence>) -> BTreeSet<Shape> {
    status
        .iter()
        .filter_map(|(shape, value)| value.first_stored_observation.is_none().then_some(*shape))
        .collect()
}
/// Follower-owned migration, called under the normal store writer lock. It does
/// not alter A's node content, format, identity, snapshot or digest.
pub async fn migrate(transaction: &Transaction<'_>) -> Result<(), Error> {
    transaction
        .batch_execute(include_str!("observations.sql"))
        .await?;
    Ok(())
}

/// Accept only immutable replies from the normal provider decoder. This keeps
/// raw sources tied to the same pages/supplements used by row construction.
pub struct ObservedBlock {
    header: Header,
    inventory: CapturedBlock,
    supplements: Supplements,
    sources: Vec<RawResponse>,
    outputs: Vec<dsa_provider::types::TransactionOutputs>,
}
impl ObservedBlock {
    pub fn new(header: Header) -> Result<Self, Error> {
        let block = header
            .hash
            .parse()
            .map_err(|_| Error::Invalid("invalid_block_hash"))?;
        let inventory = CapturedBlock::new(
            block,
            u32::try_from(header.transaction_count)
                .map_err(|_| Error::Invalid("provider_transaction_count_limit"))?,
        )
        .map_err(Error::Invalid)?;
        Ok(Self {
            header,
            inventory,
            supplements: Supplements::default(),
            sources: Vec::new(),
            outputs: Vec::new(),
        })
    }
    pub fn push(&mut self, captured: CapturedReply) -> Result<(), Error> {
        let (endpoint, reply, raw) = captured.into_parts();
        match (endpoint, reply) {
            (Endpoint::BlockCbor { .. }, Reply::Transactions(page)) => {
                self.inventory.push(page).map_err(Error::Invalid)?
            }
            (Endpoint::Datum(hash), Reply::Datum(value)) => {
                let availability = if let Some(value) = value {
                    let bytes = hex_bytes(value.as_str())?;
                    if dsa_chain::transaction::hash(&bytes) != hash.as_str() {
                        return Err(Error::Invalid("provider_datum_hash_mismatch"));
                    }
                    rows::DatumAvailability::Present(bytes)
                } else {
                    rows::DatumAvailability::NotFound
                };
                if self
                    .supplements
                    .datums
                    .insert(hash.to_string(), availability)
                    .is_some()
                {
                    return Err(Error::Invalid("duplicate_datum_supplement"));
                }
            }
            (Endpoint::Metadata(hash), Reply::Metadata(values)) => {
                let mut selected = values.into_iter().filter(|row| row.label.0 == 6500973);
                let json = selected
                    .next()
                    .map(|row| row.json_metadata.get().to_owned());
                if selected.next().is_some() {
                    return Err(Error::Invalid("duplicate_bridge_metadata"));
                }
                if self
                    .supplements
                    .metadata
                    .insert(hash.to_string(), json)
                    .is_some()
                {
                    return Err(Error::Invalid("duplicate_metadata_supplement"));
                }
            }
            (Endpoint::TransactionOutputs(_), Reply::TransactionOutputs(outputs)) => {
                self.outputs.push(outputs)
            }
            _ => return Err(Error::Invalid("unexpected_observation_reply")),
        }
        if let Some(raw) = raw {
            self.sources.push(raw);
        }
        Ok(())
    }
    /// Verified stored facts supplied by the normal lookup, not a hash oracle.
    pub fn known_stored_datum(&mut self, hash: String) {
        self.supplements
            .datums
            .entry(hash)
            .or_insert(rows::DatumAvailability::Stored);
    }
    pub fn prepare<'a>(
        mut self,
        profile: &Profile,
        previous: &Header,
        previous_target: u32,
        held: &'a mut BTreeMap<Outpoint, HeldOutput>,
        pending: &BTreeSet<Shape>,
    ) -> Result<Publication<'a>, Error> {
        let captured = self.inventory.finish().map_err(Error::Invalid)?;
        let decoded = captured
            .decode()
            .map_err(|_| Error::Invalid("provider_transaction_decode"))?;
        for outputs in self.outputs {
            let tx = decoded
                .transactions()
                .iter()
                .find(|tx| tx.hash == outputs.hash.as_str())
                .ok_or(Error::Invalid("unrequested_output_supplement"))?;
            let resolved =
                dsa_provider::supplements::resolve_outputs(tx, &outputs).map_err(Error::Invalid)?;
            if self
                .supplements
                .outputs
                .insert(tx.hash.clone(), resolved)
                .is_some()
            {
                return Err(Error::Invalid("duplicate_output_supplement"));
            }
        }
        checks::check_block(
            profile,
            previous,
            &self.header,
            previous_target,
            &decoded,
            held,
            &self.supplements,
        )?;
        let prepared = rows::prepare_block(
            profile,
            self.header.height,
            &decoded,
            held,
            &self.supplements,
            pending,
        )?;
        let mut observations = Vec::new();
        let mut limitations = Vec::new();
        for fact in prepared.facts() {
            match bind(fact, &self.header, &self.sources) {
                Ok(observation) => observations.push((fact.shape, observation)),
                Err(Error::Invalid("held_inline_origin_unavailable")) => {
                    limitations.push("held_inline_origin_unavailable")
                }
                Err(error) => return Err(error),
            }
        }
        Ok(Publication {
            network: profile.network.clone(),
            prepared,
            observations,
            sources: self.sources,
            limitations,
        })
    }
}
fn hex_bytes(text: &str) -> Result<Vec<u8>, Error> {
    text.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let digit = |b: u8| {
                (b as char)
                    .to_digit(16)
                    .map(|v| v as u8)
                    .ok_or(Error::Invalid("invalid_cbor_hex"))
            };
            Ok(digit(pair[0])? * 16 + digit(pair[1])?)
        })
        .collect()
}
fn bind(fact: &Fact, header: &Header, sources: &[RawResponse]) -> Result<Observation, Error> {
    if fact.source == Source::HeldInline {
        return Err(Error::Invalid("held_inline_origin_unavailable"));
    }
    let mut required = vec![(
        "block_cbor",
        format!(
            "/blocks/{}/txs/cbor?count=100&page={}&order=asc",
            header.hash,
            u32::from(fact.transaction_index) / 100 + 1
        ),
    )];
    match &fact.source {
        Source::Datum(hash) => required.push(("datum", format!("/scripts/datum/{hash}/cbor"))),
        Source::OutputFallback(hash) => {
            required.push(("output_fallback", format!("/txs/{hash}/utxos")))
        }
        _ => {}
    }
    if fact.shape == Shape::StoredBridgeMetadata {
        required.push((
            "metadata",
            format!("/txs/{}/metadata", fact.transaction_hash),
        ));
    }
    let mut refs = Vec::new();
    let mut observed_at = 0;
    for (source, endpoint) in required {
        let raw = sources
            .iter()
            .find(|raw| raw.path().ok().as_deref() == Some(&endpoint))
            .ok_or(Error::Invalid("stored_shape_response_unavailable"))?;
        observed_at = observed_at.max(raw.received_at());
        refs.push(ResponseRef {
            source: source.to_owned(),
            endpoint,
            response_sha256: raw.sha256().to_owned(),
        });
    }
    Ok(Observation {
        block_height: header.height,
        block_hash: header.hash.clone(),
        transaction_hash: fact.transaction_hash.clone(),
        transaction_index: fact.transaction_index,
        observed_at,
        output_index: fact.output_index,
        datum_hash: fact.datum_hash.clone(),
        raw_datum_bytes: fact.raw_datum_bytes,
        metadata_key: fact.metadata_key,
        sources: refs,
    })
}

pub struct Publication<'a> {
    network: String,
    prepared: PreparedBlock<'a>,
    observations: Vec<(Shape, Observation)>,
    sources: Vec<RawResponse>,
    limitations: Vec<&'static str>,
}
impl Publication<'_> {
    pub fn observation_count(&self) -> usize {
        self.observations.len()
    }
    pub fn observation_shapes(&self) -> Vec<Shape> {
        self.observations.iter().map(|(shape, _)| *shape).collect()
    }

    pub fn rows(&self) -> &Rows {
        self.prepared.rows()
    }
    pub fn evidence_limitations(&self) -> &[&'static str] {
        &self.limitations
    }
    /// Call AFTER inserting the selected rows in this SAME transaction. Each
    /// candidate must name a freshly inserted row, not an older idempotent row.
    /// Commit SQL first; only then apply_held. A failed SQL update drops this.
    pub async fn stage_witnesses(&mut self, transaction: &Transaction<'_>) -> Result<usize, Error> {
        let network: String = transaction
            .query_one("SELECT network FROM dsa.identity WHERE id=1", &[])
            .await?
            .get(0);
        if network != self.network {
            return Err(Error::Invalid("observation_network_mismatch"));
        }
        let mut count = 0;
        for (shape, observation) in &self.observations {
            if transaction
                .query_opt(
                    "SELECT 1 FROM dsa_follower.first_witness WHERE shape=$1",
                    &[&shape.as_str()],
                )
                .await?
                .is_some()
            {
                continue;
            }
            if !fresh_row(transaction, *shape, observation).await? {
                continue;
            }
            // Raw rows have no side effects outside this transaction. Exactly
            // one metadata row is authoritative, including after a restart.
            for reference in &observation.sources {
                let raw = self
                    .sources
                    .iter_mut()
                    .find(|raw| raw.sha256() == reference.response_sha256)
                    .ok_or(Error::Invalid("observation_response_missing"))?;
                store_raw(transaction, raw).await?;
            }
            let value = serde_json::to_value(observation)?;
            count+=transaction.execute("INSERT INTO dsa_follower.first_witness(shape,network,observation) VALUES($1,$2,$3) ON CONFLICT(shape) DO NOTHING",&[&shape.as_str(),&network,&value]).await? as usize;
        }
        Ok(count)
    }
    pub fn apply_held(self) -> Rows {
        self.prepared.commit()
    }
}
async fn fresh_row(
    transaction: &Transaction<'_>,
    shape: Shape,
    observation: &Observation,
) -> Result<bool, Error> {
    let txid = ids::transaction_id(observation.block_height, observation.transaction_index)
        .map_err(|_| Error::Invalid("invalid_observation_position"))?;
    let row=match shape {
        Shape::StoredPointerAddress=>{let index=observation.output_index.ok_or(Error::Invalid("missing_output_index"))?;let id=ids::output_id(observation.block_height,observation.transaction_index,index).map_err(|_|Error::Invalid("invalid_observation_position"))?;transaction.query_opt("SELECT 1 FROM serve.tx_out WHERE id=$1 AND xmin::text::bigint=(pg_current_xact_id()::text::numeric % 4294967296)::bigint",&[&id]).await?},
        Shape::StoredLargeDatum=>{let hash=observation.datum_hash.as_ref().ok_or(Error::Invalid("missing_datum_hash"))?;transaction.query_opt("SELECT 1 FROM serve.datum WHERE hash=decode($1,'hex') AND xmin::text::bigint=(pg_current_xact_id()::text::numeric % 4294967296)::bigint",&[hash]).await?},
        Shape::StoredBridgeMetadata=>transaction.query_opt("SELECT 1 FROM serve.tx_metadata WHERE id=$1 AND tx_id=$1 AND key=6500973 AND xmin::text::bigint=(pg_current_xact_id()::text::numeric % 4294967296)::bigint",&[&txid]).await?,
    };
    Ok(row.is_some())
}
async fn store_raw(transaction: &Transaction<'_>, raw: &mut RawResponse) -> Result<(), Error> {
    let hash = raw.sha256().to_owned();
    let length =
        i64::try_from(raw.length()).map_err(|_| Error::Invalid("response_size_overflow"))?;
    let inserted=transaction.execute("INSERT INTO dsa_follower.raw_response(sha256,length) VALUES($1,$2) ON CONFLICT DO NOTHING",&[&hash,&length]).await?;
    if inserted == 0 {
        let existing: i64 = transaction
            .query_one(
                "SELECT length FROM dsa_follower.raw_response WHERE sha256=$1",
                &[&hash],
            )
            .await?
            .get(0);
        if existing != length {
            return Err(Error::Invalid("stored_response_length_mismatch"));
        }
        return Ok(());
    }
    let mut reader = raw.reader()?;
    let mut buffer = vec![0; 65536];
    let mut ordinal = 0i32;
    let mut total = 0i64;
    let mut digest = Sha256::new();
    loop {
        let n = reader.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        let chunk = &buffer[..n];
        digest.update(chunk);
        total += n as i64;
        transaction
            .execute(
                "INSERT INTO dsa_follower.raw_chunk(sha256,ordinal,body) VALUES($1,$2,$3)",
                &[&hash, &ordinal, &chunk],
            )
            .await?;
        ordinal += 1;
    }
    if total != length || format!("{:x}", digest.finalize()) != hash {
        return Err(Error::Invalid("response_integrity"));
    }
    Ok(())
}
/// Read-only extraction for a status response_sha256 reference. Writes the
/// exact body in <=64KiB chunks and verifies count, order, size and SHA256.
/// Use a temporary destination and rename it only after this function succeeds.
pub async fn extract_raw(
    client: &Client,
    hash: &str,
    output: &mut impl Write,
) -> Result<u64, Error> {
    if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(Error::Invalid("invalid_response_reference"));
    }
    let expected: i64 = client
        .query_one(
            "SELECT length FROM dsa_follower.raw_response WHERE sha256=$1",
            &[&hash],
        )
        .await?
        .get(0);
    let mut digest = Sha256::new();
    let mut total = 0u64;
    let mut ordinal = 0i32;
    loop {
        let row = client
            .query_opt(
                "SELECT body FROM dsa_follower.raw_chunk WHERE sha256=$1 AND ordinal=$2",
                &[&hash, &ordinal],
            )
            .await?;
        let Some(row) = row else {
            break;
        };
        let bytes: Vec<u8> = row.get(0);
        digest.update(&bytes);
        output.write_all(&bytes)?;
        total += bytes.len() as u64;
        ordinal += 1;
    }
    if total != expected as u64 || format!("{:x}", digest.finalize()) != hash {
        return Err(Error::Invalid("stored_response_integrity"));
    }
    Ok(total)
}
