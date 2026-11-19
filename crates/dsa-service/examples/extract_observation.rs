//! Local-only extraction. DSN and optional CA arrive through the environment;
//! neither is logged. Destination is created only after SHA verification.
use dsa_service::observations::extract_raw;
use std::{io::Write, path::PathBuf};

#[tokio::main]
async fn main() {
    if run().await.is_err() {
        eprintln!("observation extraction failed");
        std::process::exit(1);
    }
}
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args().skip(1);
    let hash = arguments.next().ok_or("missing_response_sha256")?;
    let destination = PathBuf::from(arguments.next().ok_or("missing_destination")?);
    if arguments.next().is_some() {
        return Err("unexpected_argument".into());
    }
    let mut config: tokio_postgres::Config = std::env::var("DSA_DATABASE")?.parse()?;
    config.options("-c default_transaction_read_only=on -c statement_timeout=60000");
    let ca = std::env::var_os("DSA_TLS_ROOT_CA").map(PathBuf::from);
    let store = dsa_store::Store::connect(config, ca.as_deref()).await?;
    let parent = destination
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(std::path::Path::new("."));
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    let length = extract_raw(store.client(), &hash, &mut temporary).await?;
    temporary.flush()?;
    temporary.as_file().sync_all()?;
    temporary.persist_noclobber(destination)?;
    eprintln!("verified {length} response bytes");
    Ok(())
}
