//! Resumable recorder of finalized blocks and the supplements they need,
//! for the replay corpus. It has its own request budget, checked before
//! every request; the counts are written after every request.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use chrono::NaiveDate;
use clap::Parser;
use dsa_chain::{
    block::{DatumAvailability, Outcome, Selection, SupplementRequest, Supplements, process_block},
    profiles::Profile,
};
#[path = "../../../../crates/dsa-provider/tests/support/origins.rs"]
mod origins;
#[path = "../../../../crates/dsa-provider/tests/support/recorded.rs"]
mod recorded;
use dsa_provider::{
    Config, Provider,
    pages::BlockPages,
    types::{Hash32, Header, Hex, TransactionCbor},
};
use serde::{Deserialize, Serialize};
use sha2::Digest;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    plan: PathBuf,
    #[arg(long)]
    origins: PathBuf,
    #[arg(long)]
    key_file: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    send_lock_file: PathBuf,
    #[arg(long, default_value_t = 6000)]
    total_budget: i64,
    #[arg(long, default_value_t = 50000)]
    daily_budget: i64,
}

#[derive(Deserialize)]
struct Plan {
    network: String,
    blocks: Vec<Block>,
}
#[derive(Deserialize)]
struct Block {
    hash: Hash32,
    height: u32,
    tx_count: u32,
}

type Failure = Box<dyn std::error::Error>;

fn atomic_json(path: &Path, value: &impl Serialize) -> Result<(), Failure> {
    let tmp = path.with_extension("tmp");
    let mut file = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
    serde_json::to_writer(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    std::fs::rename(&tmp, path)?;
    File::open(path.parent().ok_or("missing output parent")?)?.sync_all()?;
    Ok(())
}

fn unix_now() -> Result<u64, Failure> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
}

struct Recorder<'a> {
    provider: Provider,
    args: &'a Args,
    counts_path: PathBuf,
}

impl Recorder<'_> {
    /// Refuse a request beyond the budget; called before every request.
    fn reserve(&self) -> Result<(), Failure> {
        let counts = self.provider.counts();
        let total: i64 = counts.iter().map(|(_, count)| count).sum();
        let today = chrono::DateTime::from_timestamp(unix_now()? as i64, 0)
            .ok_or("clock outside the calendar")?
            .date_naive();
        let daily = counts
            .iter()
            .find(|(day, _)| *day == today)
            .map_or(0, |(_, count)| *count);
        if total >= self.args.total_budget || daily >= self.args.daily_budget {
            return Err("recording budget spent".into());
        }
        Ok(())
    }

    /// Persist the counts, and a refusal, after every request.
    fn settle<T>(&self, result: Result<T, dsa_provider::Error>) -> Result<T, Failure> {
        atomic_json(&self.counts_path, &self.provider.counts())?;
        result.map_err(|error| {
            let _ = atomic_json(&self.args.output.join("last-refusal.json"), &error);
            eprintln!("provider_refusal code={} kind={:?}", error.code, error.kind);
            Box::new(error) as Failure
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Failure> {
    let args = Args::parse();
    if !(1..=6000).contains(&args.total_budget) || !(1..=50000).contains(&args.daily_budget) {
        return Err("invalid recorder budget".into());
    }
    let plan: Plan = serde_json::from_reader(File::open(&args.plan)?)?;
    let base = match plan.network.as_str() {
        "mainnet" => "https://cardano-mainnet.blockfrost.io/api/v0",
        "preprod" => "https://cardano-preprod.blockfrost.io/api/v0",
        _ => return Err("unsupported recording network".into()),
    };
    if plan.blocks.is_empty() || plan.blocks.windows(2).any(|w| w[0].height >= w[1].height) {
        return Err("recording blocks must be unique and sorted".into());
    }
    std::fs::create_dir_all(&args.output)?;
    for subdir in ["headers", "transactions", "datums", "metadata", "outputs"] {
        std::fs::create_dir_all(args.output.join(subdir))?;
    }
    // One recorder per corpus: an exclusive lock held until exit.
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&args.send_lock_file)?;
    lock.try_lock()?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let refusal = args.output.join("last-refusal.json");
    if refusal.exists() {
        let previous: serde_json::Value = serde_json::from_reader(File::open(refusal)?)?;
        if previous["kind"]["kind"] == "fatal" {
            return Err("recording has an unresolved fatal provider refusal".into());
        }
        let until = previous["kind"]["until"].as_u64().unwrap_or(0);
        if unix_now()? < until {
            return Err("recording provider wait remains active".into());
        }
    }
    let mut config = Config::new(base, &args.key_file);
    config.requests_per_second = 2.0;
    config.max_in_flight = 1;
    let provider = Provider::new(config)?;
    let counts_path = args.output.join("counts.json");
    if counts_path.exists() {
        let counts: Vec<(NaiveDate, i64)> = serde_json::from_reader(File::open(&counts_path)?)?;
        provider.seed_counts(&counts);
    }
    let recorder = Recorder {
        provider,
        args: &args,
        counts_path,
    };
    record(&recorder, &plan).await
}

async fn record(recorder: &Recorder<'_>, plan: &Plan) -> Result<(), Failure> {
    let (provider, args) = (&recorder.provider, recorder.args);
    let profile = Profile::load(&plan.network)?;
    let selection = Selection::new(&profile);
    let projected = plan.blocks.iter().map(|block| block.height).collect();
    let mut held = origins::load(&args.origins, &projected)?;
    let mut known_datums = BTreeSet::<String>::new();
    for block in &plan.blocks {
        let header_path = args.output.join(format!("headers/{}.json", block.height));
        let header: Header = if header_path.exists() {
            serde_json::from_reader(File::open(&header_path)?)?
        } else {
            recorder.reserve()?;
            recorder
                .settle(provider.block_at(block.height).await)?
                .ok_or("provider has no block at a recorded height")?
        };
        if header.hash != block.hash || header.tx_count != block.tx_count {
            return Err("provider header differs from historical SQL".into());
        }
        if !header_path.exists() {
            atomic_json(&header_path, &header)?;
        }
        let tx_path = args
            .output
            .join(format!("transactions/{}.json", block.height));
        let transactions: Vec<TransactionCbor> = if tx_path.exists() {
            serde_json::from_reader(File::open(&tx_path)?)?
        } else {
            let mut pages = BlockPages::new(block.hash.clone(), block.tx_count)?;
            while let Some(page) = pages.next_page() {
                recorder.reserve()?;
                let received = recorder
                    .settle(provider.block_transactions(&block.hash, page).await)?
                    .ok_or("provider lost a recorded block")?;
                pages.push(received)?;
            }
            let transactions = pages.finish()?.transactions;
            atomic_json(&tx_path, &transactions)?;
            transactions
        };
        if transactions.len() != block.tx_count as usize {
            return Err("cached transaction count mismatch".into());
        }
        let decoded = dsa_chain::transaction::decode_all(
            transactions
                .iter()
                .map(|tx| (tx.tx_hash.as_str(), tx.cbor.0.as_slice())),
        )?;
        let block_header = dsa_chain::block::Header::from(&header);
        let mut supplements = Supplements::default();
        supplements.datums.extend(
            known_datums
                .iter()
                .map(|hash| (hash.clone(), DatumAvailability::Stored)),
        );
        let mut requested = BTreeSet::new();
        let output = loop {
            let needs =
                match process_block(&selection, &block_header, &decoded, &held, &supplements)? {
                    Outcome::Ready(output) => break output,
                    Outcome::Needs(needs) => needs,
                };
            for request in needs {
                if !requested.insert(serde_json::to_string(&request)?) {
                    return Err("provider supplement failed to resolve requested fact".into());
                }
                let load = |supplements: &mut Supplements| {
                    recorded::load(&args.output, &request, block.height, &decoded, supplements)
                };
                if load(&mut supplements)?.is_some() {
                    continue;
                }
                recorder.reserve()?;
                match &request {
                    SupplementRequest::Datum(hash) => {
                        let started = unix_now()?;
                        let datum = recorder.settle(provider.datum(&hash.parse()?).await)?;
                        let path = args
                            .output
                            .join(format!("datums/{}-{hash}.json", block.height));
                        atomic_json(&path, &datum.map(|datum| Hex(datum.cbor)))?;
                        let capture = serde_json::json!({"format":"context-datum-capture-v1","block":block.height,"datum_hash":hash,"request_started_unix":started,"response_completed_unix":unix_now()?,"reply_sha256":format!("{:x}",sha2::Sha256::digest(std::fs::read(&path)?))});
                        atomic_json(&path.with_extension("capture.json"), &capture)?;
                    }
                    SupplementRequest::Metadata(hash) => {
                        let metadata =
                            recorder.settle(provider.transaction_metadata(&hash.parse()?).await)?;
                        atomic_json(
                            &args.output.join(format!("metadata/{hash}.json")),
                            &metadata.entries,
                        )?;
                    }
                    SupplementRequest::Output { transaction, .. } => {
                        let outputs = recorder
                            .settle(provider.transaction_outputs(&transaction.parse()?).await)?;
                        atomic_json(
                            &args.output.join(format!("outputs/{transaction}.json")),
                            &outputs,
                        )?;
                    }
                }
                if load(&mut supplements)?.is_none() {
                    return Err("recorded provider supplement missing".into());
                }
            }
        };
        for (point, entry) in &output.created {
            held.insert(point.clone(), entry.clone());
        }
        for point in &output.spent {
            held.remove(point);
        }
        known_datums.extend(output.rows.datum.iter().map(|datum| datum.hash.clone()));
        let total: i64 = provider.counts().iter().map(|(_, count)| count).sum();
        println!(
            "recorded network={} height={} requests={total}",
            plan.network, block.height
        );
    }
    let counts: BTreeMap<_, _> = provider.counts().into_iter().collect();
    atomic_json(
        &args.output.join("summary.json"),
        &serde_json::json!({"network":plan.network,"counts":counts,"finished_unix":unix_now()?}),
    )?;
    Ok(())
}
