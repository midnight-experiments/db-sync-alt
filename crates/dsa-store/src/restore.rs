//! Restore a snapshot file into an empty database.
//!
//! 1. The file is opened and checked completely (description, framing and
//!    every section hash) before the database changes.
//! 2. Under the writer lock the destination becomes an empty closed layout:
//!    a blank database gets one; the build of an interrupted restore is undone.
//! 3. One build transaction moves the empty tables to `dsa_retired`, renames
//!    their indexes, creates and loads new tables in `serve`, indexes and
//!    verifies them, and commits with the gate closed. The public views still
//!    read the empty tables, so readers keep receiving SQLSTATE 55000 and the
//!    node finds exactly one index per name.
//! 4. A short publication transaction takes the view locks without waiting
//!    (and retries while a reader holds one), points the views at the new
//!    tables, drops the empty ones, records the first-served times and opens
//!    the gate. A query that arrives during it waits for it.

use std::{
    fs::File,
    io::Read,
    path::Path,
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

use bytes::Bytes;
use dsa_chain::profiles::Profile;
use futures_util::{SinkExt, pin_mut};
use tokio_postgres::{Transaction, error::SqlState};

use crate::{
    Error, NODE_RELATIONS, Store,
    load::{self, RETIRED},
    snapshot::{self, CheckedReader, Description, Section},
    verify::{self, Checks, Verification},
};

/// How long publication retries while readers hold the view locks.
const PUBLICATION_WAIT: Duration = Duration::from_secs(60);

impl Store {
    pub async fn restore(
        &mut self,
        profile: &Profile,
        path: &Path,
        expected_content_digest: Option<&str>,
    ) -> Result<Verification, Error> {
        let result = self
            .restore_file(profile, path, expected_content_digest)
            .await;
        if result.is_err() {
            self.release_writer().await;
        }
        result
    }

    async fn restore_file(
        &mut self,
        profile: &Profile,
        path: &Path,
        expected_content_digest: Option<&str>,
    ) -> Result<Verification, Error> {
        let mut file = File::open(path)?;
        let description = snapshot::read_description(&mut file, profile)?;
        if let Some(expected) = expected_content_digest
            && expected != description.verified.content_digest
        {
            return Err(Error::Mismatch {
                error: "snapshot_content_digest_unexpected",
                item: "content_digest".to_owned(),
                expected: expected.to_owned(),
                actual: description.verified.content_digest.clone(),
            });
        }
        snapshot::preflight(&mut file, &description)?;
        self.prepare_load(profile, "restore_failed").await?;
        let verified = self.build(profile, &mut file, &description).await?;
        self.publish(profile, &description, &verified).await?;
        Ok(verified)
    }

    async fn build(
        &mut self,
        profile: &Profile,
        file: &mut File,
        description: &Description,
    ) -> Result<Verification, Error> {
        let transaction = self.writer_transaction().await?;
        load::lock_empty(&transaction, "SHARE", "restore_failed").await?;
        transaction
            .batch_execute(&format!("ALTER SCHEMA serve RENAME TO {RETIRED}"))
            .await?;
        load::rename_indexes(&transaction, RETIRED, "", "retired_").await?;
        transaction.batch_execute(load::served_tables_ddl()).await?;
        for section in &description.sections {
            load_section(&transaction, file, section)
                .await
                .map_err(|error| Error::InFamily {
                    family: section.table.clone(),
                    error: Box::new(error),
                })?;
        }
        if file.read(&mut [0])? != 0 {
            return Err(Error::Refusal("snapshot_trailing_bytes"));
        }
        let expected = &description.verified;
        load::write_supply(&transaction, &expected.supply).await?;
        load::write_position(&transaction, &expected.state).await?;
        transaction
            .batch_execute(include_str!("indexes.sql"))
            .await?;
        load::analyze_and_sequences(&transaction).await?;
        let verified = verify::verify(&transaction, profile, Checks::LOAD).await?;
        verify::same(expected, &verified, "snapshot_content_digest_mismatch")?;
        transaction.commit().await?;
        Ok(verified)
    }

    async fn publish(
        &mut self,
        profile: &Profile,
        description: &Description,
        verified: &Verification,
    ) -> Result<(), Error> {
        let origin = serde_json::to_value(description.origin.restored(verified))?;
        let commit_sent = self.cancellation.commit_sent.clone();
        let views = NODE_RELATIONS
            .iter()
            .map(|table| format!("public.{table}"))
            .collect::<Vec<_>>()
            .join(",");
        let retired = NODE_RELATIONS
            .iter()
            .map(|table| format!("{RETIRED}.{table}"))
            .collect::<Vec<_>>()
            .join(",");
        let began = Instant::now();
        loop {
            let transaction = self.writer_transaction().await?;
            if let Err(error) = transaction
                .batch_execute(&format!(
                    "LOCK TABLE {views} IN ACCESS EXCLUSIVE MODE NOWAIT"
                ))
                .await
            {
                if error.code() != Some(&SqlState::LOCK_NOT_AVAILABLE) {
                    return Err(error.into());
                }
                transaction.rollback().await?;
                if began.elapsed() >= PUBLICATION_WAIT {
                    return Err(Error::Refusal("publication_lock_unavailable"));
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            transaction.batch_execute(include_str!("views.sql")).await?;
            transaction
                .batch_execute(&format!("DROP TABLE {retired};DROP SCHEMA {RETIRED}"))
                .await?;
            load::publish_state(&transaction, profile, &verified.state, &origin).await?;
            commit_sent.store(true, Ordering::SeqCst);
            transaction.commit().await?;
            return Ok(());
        }
    }
}

/// Streams one section into its table, re-hashing the bytes actually read.
async fn load_section(
    transaction: &Transaction<'_>,
    file: &mut File,
    section: &Section,
) -> Result<(), Error> {
    let mut decoder = zstd::stream::read::Decoder::new(CheckedReader::new(
        Read::by_ref(file).take(section.compressed_bytes),
    ))?;
    decoder.window_log_max(27)?;
    let sink = transaction
        .copy_in::<_, Bytes>(&format!(
            "COPY {} FROM STDIN (FORMAT binary)",
            snapshot::qualified(&section.table)?
        ))
        .await?;
    pin_mut!(sink);
    let mut bytes = 0;
    let mut buffer = [0; 65536];
    loop {
        let n = decoder.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        bytes += n as u64;
        if bytes > section.plain_bytes {
            return Err(Error::Refusal("snapshot_plain_size_mismatch"));
        }
        sink.as_mut()
            .send(Bytes::copy_from_slice(&buffer[..n]))
            .await?;
    }
    let (size, hash) = decoder.finish().into_inner().finish();
    if size != section.compressed_bytes || hash != section.sha256 {
        return Err(Error::Refusal("snapshot_section_hash_mismatch"));
    }
    let rows = sink.as_mut().finish().await?;
    if bytes != section.plain_bytes || rows != section.rows {
        return Err(Error::Refusal("snapshot_row_count_mismatch"));
    }
    Ok(())
}
