//! Resumable finalized-block corpus recorder. This tool has a separate explicit
//! research budget; the production provider client has no daily allowance.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use clap::Parser;
use dsa_chain::{
    profiles::Profile,
    rows::{self, BuildError, DatumAvailability, SupplementRequest, Supplements},
};
#[path = "support/origins.rs"]
mod origins;
#[path = "support/recorded.rs"]
mod recorded;
use dsa_provider::{
    AttemptRecorder, Config, DailyCount, Provider, RecordFuture, RecorderError,
    pages::CapturedBlock,
    types::{Endpoint, Hash32, Header, Page, Reply, TransactionCbor},
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
    total_budget: u64,
    #[arg(long, default_value_t = 50000)]
    daily_budget: u64,
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
#[derive(Deserialize, Serialize)]
struct Reservation {
    utc_day: String,
    requests: u64,
}

struct Ledger {
    file: Mutex<File>,
    total: Mutex<u64>,
    total_budget: u64,
    daily_budget: u64,
}
impl AttemptRecorder for Ledger {
    fn record(&self, count: DailyCount) -> RecordFuture<'_> {
        Box::pin(async move {
            let mut total = self.total.lock().map_err(|_| RecorderError)?;
            if *total >= self.total_budget || count.requests > self.daily_budget {
                return Err(RecorderError);
            }
            let mut file = self.file.lock().map_err(|_| RecorderError)?;
            let row = Reservation {
                utc_day: count.utc_day,
                requests: count.requests,
            };
            // One fsynced append precedes each send. An interrupted reservation
            // stays spent; a torn final line causes a refusal on restart.
            let mut bytes = serde_json::to_vec(&row).map_err(|_| RecorderError)?;
            bytes.push(b'\n');
            file.write_all(&bytes)
                .and_then(|_| file.sync_all())
                .map_err(|_| RecorderError)?;
            *total += 1;
            Ok(())
        })
    }
}

fn atomic_json(path: &Path, value: &impl Serialize) -> Result<(), Box<dyn std::error::Error>> {
    let tmp = path.with_extension("tmp");
    let mut file = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
    serde_json::to_writer(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    std::fs::rename(&tmp, path)?;
    File::open(path.parent().ok_or("missing output parent")?)?.sync_all()?;
    Ok(())
}

async fn fetch(
    provider: &Provider,
    output: &Path,
    endpoint: Endpoint,
) -> Result<Reply, Box<dyn std::error::Error>> {
    // Refusals are persisted as evidence by the caller. The client decides the
    // cooldown; neither a retry nor a resumed process bypasses its wait.
    match provider.fetch(endpoint).await {
        Ok(reply) => Ok(reply),
        Err(error) => {
            atomic_json(&output.join("last-refusal.json"), &error)?;
            eprintln!(
                "provider_refusal code={} http={:?} retry_at={:?}",
                error.code, error.http_status, error.retry_at
            );
            Err(Box::new(error))
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if args.total_budget == 0
        || args.total_budget > 6000
        || args.daily_budget == 0
        || args.daily_budget > 50000
    {
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
    // The outer launch also holds one host-wide recorder flock. This exclusive
    // file prevents accidental overlapping processes on the same corpus.
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&args.send_lock_file)?;
    lock.try_lock()?;
    // The preceding process held this same lock through its final send. Delay
    // before our first send too, preserving spacing across network switches or
    // a resumed recorder instead of giving each process an initial burst.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let refusal = args.output.join("last-refusal.json");
    if refusal.exists() {
        let previous: serde_json::Value = serde_json::from_reader(File::open(refusal)?)?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        if previous
            .get("retry_at")
            .and_then(|v| v.as_u64())
            .is_some_and(|until| now < until)
        {
            return Err("recording provider cooldown remains active".into());
        }
        if previous.get("action").and_then(|v| v.as_str()) == Some("fatal") {
            return Err("recording has an unresolved fatal provider refusal".into());
        }
    }
    record(&args, &plan, base).await
}

async fn record(args: &Args, plan: &Plan, base: &str) -> Result<(), Box<dyn std::error::Error>> {
    let ledger_path = args.output.join("attempts.jsonl");
    let mut days = BTreeMap::new();
    let mut total = 0u64;
    if ledger_path.exists() {
        for line in BufReader::new(File::open(&ledger_path)?).lines() {
            let row: Reservation = serde_json::from_str(&line?)?;
            if row.requests != days.get(&row.utc_day).copied().unwrap_or(0) + 1 {
                return Err("attempt ledger sequence invalid".into());
            }
            days.insert(row.utc_day, row.requests);
            total += 1;
        }
    }
    let counts = days
        .iter()
        .map(|(utc_day, requests)| DailyCount {
            utc_day: utc_day.clone(),
            requests: *requests,
        })
        .collect();
    let ledger = Arc::new(Ledger {
        file: Mutex::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&ledger_path)?,
        ),
        total: Mutex::new(total),
        total_budget: args.total_budget,
        daily_budget: args.daily_budget,
    });
    let mut config = Config::new(base, &args.key_file);
    config.requests_per_second = 2.0;
    config.max_in_flight = 1;
    let provider = Provider::new(config, ledger.clone(), counts)?;
    let profile = Profile::load(&plan.network)?;
    let projected = plan.blocks.iter().map(|block| block.height).collect();
    let mut held = origins::load(&args.origins, &projected)?;
    let mut known_datums = BTreeSet::<String>::new();
    let mut previous: Option<Header> = None;
    let mut header_queue = BTreeMap::new();
    for block in &plan.blocks {
        let header_path = args.output.join(format!("headers/{}.json", block.height));
        let header: Header = if header_path.exists() {
            serde_json::from_reader(File::open(&header_path)?)?
        } else if let Some(header) = header_queue.remove(&block.height) {
            header
        } else if let Some(previous) = previous.as_ref().filter(|p| p.height + 1 == block.height) {
            match fetch(
                &provider,
                &args.output,
                Endpoint::NextBlocks {
                    block: previous.hash.clone(),
                    page: Page::new(1)?,
                },
            )
            .await?
            {
                Reply::Headers(headers) => {
                    let mut parent = previous.clone();
                    for h in headers {
                        if h.height != parent.height + 1
                            || h.previous_block.as_ref() != Some(&parent.hash)
                        {
                            return Err("provider header linkage mismatch".into());
                        }
                        parent = h.clone();
                        header_queue.insert(h.height, h);
                    }
                    header_queue
                        .remove(&block.height)
                        .ok_or("provider header range incomplete")?
                }
                _ => return Err("unexpected provider header reply".into()),
            }
        } else {
            match fetch(&provider, &args.output, Endpoint::Block(block.hash.clone())).await? {
                Reply::Header(h) => h,
                _ => return Err("unexpected provider header reply".into()),
            }
        };
        if header.hash != block.hash
            || header.height != block.height
            || header.tx_count != block.tx_count
        {
            return Err("provider header differs from historical SQL".into());
        }
        if !header_path.exists() {
            atomic_json(&header_path, &header)?;
        }
        previous = Some(header);
        let tx_path = args
            .output
            .join(format!("transactions/{}.json", block.height));
        let (transactions, source) = if tx_path.exists() {
            let rows: Vec<TransactionCbor> = serde_json::from_reader(File::open(&tx_path)?)?;
            let source = dsa_chain::inventory::CapturedTransactions::from_records(
                rows.iter()
                    .map(|record| {
                        Ok((
                            record.tx_hash.to_string(),
                            hex::decode(record.cbor.as_str())?,
                        ))
                    })
                    .collect::<Result<Vec<_>, hex::FromHexError>>()?,
            );
            (rows, source)
        } else {
            let mut rows = Vec::new();
            let mut captured = CapturedBlock::new(block.hash.clone(), block.tx_count)?;
            if block.tx_count > 0 {
                for page in 1..=10000 {
                    let reply = fetch(
                        &provider,
                        &args.output,
                        Endpoint::BlockCbor {
                            block: block.hash.clone(),
                            page: Page::new(page)?,
                        },
                    )
                    .await?;
                    let Reply::Transactions(batch) = reply else {
                        return Err("unexpected provider CBOR reply".into());
                    };
                    rows.extend(batch.records().iter().cloned());
                    captured.push(batch)?;
                    if captured.is_complete() {
                        break;
                    }
                }
            }
            if rows.len() != block.tx_count as usize {
                return Err("provider transaction count mismatch".into());
            }
            let source = captured.finish()?;
            atomic_json(&tx_path, &rows)?;
            (rows, source)
        };
        if transactions.len() != block.tx_count as usize {
            return Err("cached transaction count mismatch".into());
        }
        let decoded = source.decode()?;
        let mut supplements = Supplements::default();
        supplements.datums.extend(
            known_datums
                .iter()
                .map(|hash| (hash.clone(), DatumAvailability::Stored)),
        );
        let mut requests = BTreeSet::new();
        let built = loop {
            match rows::build_block(&profile, block.height, &decoded, &mut held, &supplements) {
                Ok(rows) => break rows,
                Err(BuildError::Required(request)) => {
                    if !requests.insert(serde_json::to_string(&request)?) {
                        return Err("provider supplement failed to resolve requested fact".into());
                    }
                    if recorded::load(
                        &args.output,
                        &request,
                        block.height,
                        decoded.transactions(),
                        &mut supplements,
                    )?
                    .is_some()
                    {
                        continue;
                    }
                    match &request {
                        SupplementRequest::Datum(hash) => {
                            let request_started_unix =
                                SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
                            let Reply::Datum(reply) =
                                fetch(&provider, &args.output, Endpoint::Datum(hash.parse()?))
                                    .await?
                            else {
                                return Err("unexpected provider datum reply".into());
                            };
                            atomic_json(
                                &args
                                    .output
                                    .join(format!("datums/{}-{hash}.json", block.height)),
                                &reply,
                            )?;
                            let response_completed_unix =
                                SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
                            atomic_json(
                                &args
                                    .output
                                    .join(format!("datums/{}-{hash}.capture.json", block.height)),
                                &serde_json::json!({"format":"context-datum-capture-v1","block":block.height,"datum_hash":hash,"request_started_unix":request_started_unix,"response_completed_unix":response_completed_unix,"reply_sha256":format!("{:x}",sha2::Sha256::digest(std::fs::read(args.output.join(format!("datums/{}-{hash}.json",block.height)))?))}),
                            )?;
                        }
                        SupplementRequest::Metadata(hash) => {
                            let Reply::Metadata(reply) =
                                fetch(&provider, &args.output, Endpoint::Metadata(hash.parse()?))
                                    .await?
                            else {
                                return Err("unexpected provider metadata reply".into());
                            };
                            atomic_json(
                                &args.output.join(format!("metadata/{hash}.json")),
                                &reply,
                            )?;
                        }
                        SupplementRequest::Output { transaction, .. } => {
                            let Reply::TransactionOutputs(reply) = fetch(
                                &provider,
                                &args.output,
                                Endpoint::TransactionOutputs(transaction.parse()?),
                            )
                            .await?
                            else {
                                return Err("unexpected provider output reply".into());
                            };
                            atomic_json(
                                &args.output.join(format!("outputs/{transaction}.json")),
                                &reply,
                            )?;
                        }
                    }
                    if !recorded::load(
                        &args.output,
                        &request,
                        block.height,
                        decoded.transactions(),
                        &mut supplements,
                    )?
                    .is_some()
                    {
                        return Err("recorded provider supplement missing".into());
                    }
                }
                Err(error) => return Err(error.into()),
            }
        };
        known_datums.extend(built.datum.into_iter().map(|datum| datum.hash));
        println!(
            "recorded network={} height={} attempts={}",
            plan.network,
            block.height,
            *ledger.total.lock().map_err(|_| "ledger lock")?
        );
    }
    let finished = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    atomic_json(
        &args.output.join("summary.json"),
        &serde_json::json!({"network":plan.network,"attempts_total":*ledger.total.lock().map_err(|_| "ledger lock")?,"status":provider.status()?,"finished_unix":finished}),
    )?;
    tokio::time::sleep(Duration::from_millis(1)).await;
    Ok(())
}
