//! Stable-reader cold restore prototype. Internal staging is not a release claim.
use super::*;
use crate::storage;
use serde_json::{Value, json};
use tokio_postgres::Transaction;

const JOURNAL: &str = r#"
CREATE TABLE dsa.restore_build (
 id boolean PRIMARY KEY CHECK(id), protocol integer NOT NULL CHECK(protocol=1),
 database_oid oid NOT NULL,database_name text NOT NULL,owner_oid oid NOT NULL,
 identity_created_at timestamptz NOT NULL,input_sha256 text NOT NULL CHECK(length(input_sha256)=64),
 storage_kind text NOT NULL CHECK(storage_kind IN ('direct','inherited')),
 phase text NOT NULL CHECK(phase IN ('ready','published','complete')),
 parents jsonb NOT NULL,children jsonb NOT NULL,verified jsonb NOT NULL,origin jsonb NOT NULL,
 initial_horizon bigint NOT NULL,initial_target bigint NOT NULL,
 retention_anchor_at timestamptz,
 CHECK(initial_horizon<=initial_target),
 CHECK((phase='complete')=(retention_anchor_at IS NOT NULL))
);
REVOKE ALL ON dsa.restore_build FROM PUBLIC;
"#;

fn event(name: &str, value: Value) {
    eprintln!("{}", json!({"event":name,"details":value}));
}

// New private objects must not inherit an owner's permissive default ACLs.
// Only objects created by this transaction are passed here; existing/public
// grants and reader memberships are never rewritten.
async fn private_acl<C: GenericClient>(client: &C, kind: &str, name: &str) -> Result<(), Error> {
    let (source, oid, acl, owner, default) = match kind {
        "SCHEMA" => (
            "pg_namespace",
            "to_regnamespace($1)",
            "nspacl",
            "nspowner",
            "n",
        ),
        "TABLE" => ("pg_class", "to_regclass($1)", "relacl", "relowner", "r"),
        "FUNCTION" => ("pg_proc", "to_regprocedure($1)", "proacl", "proowner", "f"),
        _ => return Err(Error::Refusal("invalid_private_object_kind")),
    };
    let sql = format!(
        "SELECT DISTINCT CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(a.grantee)) END FROM {source} o CROSS JOIN LATERAL aclexplode(coalesce(o.{acl},acldefault('{default}',o.{owner}))) a WHERE o.oid={oid} AND a.grantee<>o.{owner}"
    );
    for row in client.query(&sql, &[&name]).await? {
        let grantee: String = row.get(0);
        client
            .batch_execute(&format!("REVOKE ALL ON {kind} {name} FROM {grantee}"))
            .await?;
    }
    Ok(())
}

pub(super) fn input_hash(file: &mut File) -> Result<String, Error> {
    let offset = file.stream_position()?;
    file.seek(SeekFrom::Start(0))?;
    let (_, hash) = hash_reader(&mut *file)?;
    file.seek(SeekFrom::Start(offset))?;
    Ok(hash)
}

async fn record<C: GenericClient>(
    client: &C,
    hash: &str,
    description: &Description,
    verified: &Verification,
    kind: &str,
    phase: &str,
) -> Result<(), Error> {
    client.batch_execute(JOURNAL).await?;
    private_acl(client, "TABLE", "dsa.restore_build").await?;
    let mut parents = serde_json::Map::new();
    let mut children = serde_json::Map::new();
    for family in NODE_RELATIONS {
        let parent: String = client
            .query_one(
                "SELECT (to_regclass($1)::oid)::text",
                &[&format!("serve.{family}")],
            )
            .await?
            .get(0);
        parents.insert(family.into(), json!(parent));
        if kind == "inherited" {
            let child: String = client
                .query_one(
                    "SELECT (to_regclass($1)::oid)::text",
                    &[&format!("dsa_restore.{family}")],
                )
                .await?
                .get(0);
            children.insert(family.into(), json!(child));
        }
    }
    let origin = serde_json::to_value(description.origin.restored(verified))?;
    client.execute("INSERT INTO dsa.restore_build SELECT true,1,d.oid,current_database(),r.oid,i.created_at,$1,$2,$3,$4,$5,$6,$7,$8,$9,NULL FROM pg_database d CROSS JOIN pg_roles r CROSS JOIN dsa.identity i WHERE d.datname=current_database() AND r.rolname=current_user AND i.id=1",&[&hash,&kind,&phase,&Value::Object(parents),&Value::Object(children),&serde_json::to_value(verified)?,&origin,&i64::from(verified.state.horizon.height),&i64::from(verified.state.target.height)]).await?;
    Ok(())
}

pub(super) async fn record_direct<C: GenericClient>(
    client: &C,
    hash: &str,
    description: &Description,
    verified: &Verification,
) -> Result<(), Error> {
    record(client, hash, description, verified, "direct", "published").await
}

/// This is a post-publication transaction. A lost acknowledgement is resolved by
/// the durable phase and immutable anchor, never by setting clocks a second time.
pub(super) async fn settle(store: &mut Store) -> Result<(), Error> {
    let stage = RestoreStage::start("restore_retention_anchor", "database");
    let tx = store.writer_transaction().await?;
    storage::recover_retention_anchor(&tx).await?;
    tx.commit().await?;
    stage.finish();
    event(
        "restore_complete",
        json!({"retention_anchor":"post_publication_once_only"}),
    );
    Ok(())
}

async fn publish(store: &mut Store) -> Result<(), Error> {
    let stage = RestoreStage::start("restore_publish", "database");
    let tx = store.writer_transaction().await?;
    storage::validate_routes(&tx).await?;
    let phase: String = tx
        .query_one(
            "SELECT phase FROM dsa.restore_build WHERE id FOR UPDATE",
            &[],
        )
        .await?
        .get(0);
    if phase != "ready" {
        return Err(Error::Refusal("restore_phase_mismatch"));
    }
    // No table/view/index DDL or N-row timestamp update here. Even a stalled
    // row update does not prevent a reader's MVCC gate SELECT seeing false.
    event(
        "restore_publication_started",
        json!({"binding_changes":false}),
    );
    tx.execute("UPDATE dsa.identity SET origin=(SELECT origin FROM dsa.restore_build WHERE id),tool_version=$1 WHERE id=1",&[&VERSION]).await?;
    tx.execute(
        "UPDATE dsa.restore_build SET phase='published' WHERE id AND phase='ready'",
        &[],
    )
    .await?;
    tx.batch_execute("UPDATE dsa.position SET serving=true,reason='serving',updated_at=clock_timestamp() WHERE id=1").await?;
    stage.finish();
    let stage = RestoreStage::start("restore_commit", "database");
    tx.commit().await?;
    stage.finish();
    event(
        "restore_publication_committed",
        json!({"retention_anchor_pending":true}),
    );
    settle(store).await
}

pub(super) async fn resume(
    store: &mut Store,
    profile: &Profile,
    path: &Path,
) -> Result<Option<Verification>, Error> {
    if !storage::has_journal(store.client()).await? {
        return Ok(None);
    }
    store.open(profile).await?;
    // Acquire the normal session writer lock before inspecting mutable phase.
    let tx = store.writer_transaction().await?;
    storage::validate_routes(&tx).await?;
    let row=tx.query_one("SELECT phase,input_sha256,verified,(SELECT serving FROM dsa.position WHERE id=1) FROM dsa.restore_build WHERE id",&[]).await?;
    let phase: String = row.get(0);
    if phase == "complete" {
        return Err(Error::Refusal("destination_not_empty"));
    }
    let serving: bool = row.get(3);
    if (phase == "ready" && serving) || (phase == "published" && !serving) {
        // A gate that closed after publication is not a new empty destination.
        return Err(Error::Refusal("restore_phase_mismatch"));
    }
    tx.commit().await?;
    let mut file = File::open(path)?;
    let description = read_description(&mut file, profile)?;
    preflight(&mut file, &description)?;
    if input_hash(&mut file)? != row.get::<_, String>(1) {
        return Err(Error::Refusal("restore_input_binding_mismatch"));
    }
    let saved: Verification = serde_json::from_value(row.get(2))?;
    if !verify::logical_equal(&description.verified, &saved) {
        return Err(Error::Refusal("restore_verification_binding_mismatch"));
    }
    let stage = RestoreStage::start("restore_resume_verify", "database");
    let tx = store.writer_transaction().await?;
    // Owned READY layouts from the earlier prototype can lack inherited stats.
    // Repair only missing nonempty-family stats without another payload load.
    let kind: String = tx
        .query_one("SELECT storage_kind FROM dsa.restore_build WHERE id", &[])
        .await?
        .get(0);
    if kind == "inherited" {
        for family in NODE_RELATIONS {
            let have:bool=tx.query_one("SELECT EXISTS(SELECT 1 FROM pg_stats WHERE schemaname='serve' AND tablename=$1 AND inherited)",&[&family]).await?.get(0);
            if !have
                && saved
                    .families
                    .get(family)
                    .is_some_and(|digest| digest.rows > 0)
            {
                let analyze = RestoreStage::start("restore_inherited_analyze", family);
                tx.batch_execute(&format!("ANALYZE serve.{family}"))
                    .await
                    .map_err(|error| analyze.error(error))?;
                analyze.finish();
            }
        }
    }
    let actual = verify::verify(&tx, profile, phase == "published").await?;
    if !verify::logical_equal(&saved, &actual) {
        return Err(Error::Refusal("snapshot_content_digest_mismatch"));
    }
    tx.commit().await?;
    stage.finish();
    event(
        "restore_resume_verified",
        json!({"phase":phase,"copy_required":false}),
    );
    if phase == "ready" {
        publish(store).await?;
    } else {
        settle(store).await?;
    }
    Ok(Some(actual))
}

pub(super) async fn restore_preflighted(
    store: &mut Store,
    profile: &Profile,
    mut file: File,
    description: Description,
) -> Result<Verification, Error> {
    let hash = input_hash(&mut file)?;
    let stage = RestoreStage::start("restore_recheck_empty", "database");
    let tx = store.writer_transaction().await?;
    let (_, locked) = restore_tables()?;
    lock_empty_restore(&tx, &locked, false, "restore_failed").await?;
    let collision:bool=tx.query_one("SELECT to_regnamespace('dsa_restore') IS NOT NULL OR to_regclass('dsa.restore_build') IS NOT NULL OR to_regprocedure('dsa.restore_parent_insert_guard()') IS NOT NULL OR EXISTS(SELECT 1 FROM pg_inherits WHERE inhparent=ANY(ARRAY(SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='serve')))",&[]).await?.get(0);
    if collision {
        return Err(Error::Refusal("restore_private_storage_conflict"));
    }
    let indexes = restore_indexes(&tx).await?;
    // Any missing startup index is created over the empty stable parent, using
    // compatible locks. Canonical names never disappear or become duplicates.
    tx.batch_execute(&indexes.missing.join("\n")).await?;
    tx.batch_execute(&format!("CREATE SCHEMA dsa_restore;REVOKE ALL ON SCHEMA dsa_restore FROM PUBLIC;CREATE FUNCTION dsa.restore_parent_insert_guard() RETURNS trigger LANGUAGE plpgsql AS $${}$$;REVOKE ALL ON FUNCTION dsa.restore_parent_insert_guard() FROM PUBLIC",storage::PARENT_GUARD_BODY)).await?;
    private_acl(&tx, "SCHEMA", "dsa_restore").await?;
    private_acl(&tx, "FUNCTION", "dsa.restore_parent_insert_guard()").await?;
    for family in NODE_RELATIONS {
        let key = if family == "epoch_param" {
            "epoch_no"
        } else {
            "id"
        };
        tx.batch_execute(&format!("CREATE TABLE dsa_restore.{family}(LIKE serve.{family} INCLUDING ALL EXCLUDING INDEXES,CONSTRAINT restore_{family}_pkey PRIMARY KEY({key}));ALTER TABLE dsa_restore.{family} SET(autovacuum_analyze_scale_factor=0.01,autovacuum_vacuum_scale_factor=0.05);REVOKE ALL ON dsa_restore.{family} FROM PUBLIC;CREATE TRIGGER restore_empty_parent BEFORE INSERT ON serve.{family} FOR EACH ROW EXECUTE FUNCTION dsa.restore_parent_insert_guard()")).await?;
        private_acl(&tx, "TABLE", &format!("dsa_restore.{family}")).await?;
    }
    // Marker storage is private to the tool. Recreate its proven-empty table
    // without CASCADE; an unexpected dependent object safely refuses the load.
    tx.batch_execute("DROP TABLE dsa.consumed_before_horizon;CREATE TABLE dsa.consumed_before_horizon(tx_out_id bigint PRIMARY KEY CHECK(tx_out_id>=0));REVOKE ALL ON dsa.consumed_before_horizon FROM PUBLIC").await?;
    private_acl(&tx, "TABLE", "dsa.consumed_before_horizon").await?;
    stage.finish();
    load_sections(&tx, &mut file, &description).await?;
    let stage = RestoreStage::start("restore_load_state", "database");
    let mut extra = [0];
    if file.read(&mut extra)? != 0 {
        return Err(Error::Refusal("snapshot_trailing_bytes"));
    }
    for (unit, quantity) in &description.verified.supply {
        tx.execute(
            "INSERT INTO dsa.supply VALUES($1,$2::text::numeric)",
            &[unit, quantity],
        )
        .await?;
    }
    let s = &description.verified.state;
    tx.execute("UPDATE dsa.position SET horizon_height=$1::integer,horizon_hash=decode($2,'hex'),horizon_time=$3::text::timestamptz,target_height=$4::integer,target_hash=decode($5,'hex'),target_time=$6::text::timestamptz,tip_height=$7::integer,tip_hash=decode($8,'hex'),tip_time=$9::text::timestamptz,retention_seconds=$10,updated_at=now() WHERE id=1",&[&(s.horizon.height as i32),&s.horizon.hash,&s.horizon.time,&(s.target.height as i32),&s.target.hash,&s.target.time,&(s.tip.height as i32),&s.tip.hash,&s.tip.time,&s.retention_seconds]).await?;
    stage.finish();
    let stage = RestoreStage::start("restore_indexes", "database");
    for sql in include_str!("../indexes.sql")
        .lines()
        .filter(|s| s.starts_with("CREATE"))
    {
        let name = sql
            .split("INDEX ")
            .nth(1)
            .and_then(|s| s.split_whitespace().next())
            .ok_or(Error::Refusal("invalid_index_catalogue"))?;
        tx.batch_execute(
            &sql.replace(&format!("INDEX {name} "), &format!("INDEX restore_{name} "))
                .replace(" ON serve.", " ON dsa_restore."),
        )
        .await?;
    }
    stage.finish();
    for family in NODE_RELATIONS {
        let stage = RestoreStage::start("restore_analyze", family);
        tx.batch_execute(&format!(
            "ANALYZE dsa_restore.{family};ALTER TABLE dsa_restore.{family} INHERIT serve.{family}"
        ))
        .await?;
        stage.finish();
        // Per-child stats do not populate the parent's inherited join stats.
        // ANALYZE's locks stay compatible with node AccessShare readers.
        let stage = RestoreStage::start("restore_inherited_analyze", family);
        tx.batch_execute(&format!("ANALYZE serve.{family}"))
            .await
            .map_err(|error| stage.error(error))?;
        stage.finish();
    }
    tx.batch_execute("ANALYZE dsa.consumed_before_horizon")
        .await?;
    for table in ["multi_asset", "datum", "pool_hash", "epoch_stake"] {
        let stage = RestoreStage::start("restore_sequences", table);
        tx.query_one(&format!("SELECT setval('dsa.{table}_id_seq',greatest(coalesce(max(id),0)+1,1),false) FROM serve.{table}"),&[]).await?;
        stage.finish();
    }
    let stage = RestoreStage::start("restore_verify", "database");
    let verified = verify::verify(&tx, profile, false).await?;
    if !verify::logical_equal(&description.verified, &verified) {
        return Err(Error::Refusal("snapshot_content_digest_mismatch"));
    }
    stage.finish();
    record(&tx, &hash, &description, &verified, "inherited", "ready").await?;
    storage::validate_routes(&tx).await?;
    let stage = RestoreStage::start("restore_build_commit", "database");
    tx.commit().await?;
    stage.finish();
    event(
        "restore_build_committed",
        json!({"verified":true,"gate_open":false,"copy_required_on_resume":false,"storage_mode":"stable_parent_inheritance"}),
    );
    publish(store).await?;
    // The returned report is the actual in-transaction verification of the
    // imported bytes; it is not a trusted copy of the file's verification.
    Ok(verified)
}

async fn load_sections(
    tx: &Transaction<'_>,
    file: &mut File,
    description: &Description,
) -> Result<(), Error> {
    for section in &description.sections {
        {
            let table = if section.table == MARKERS {
                qualified(MARKERS)?
            } else {
                format!("dsa_restore.{}", section.table)
            };
            let began = Instant::now();
            let database_error =
                |stage, error: tokio_postgres::Error, wait_seconds, queued_plain_bytes| {
                    Error::AtStage {
                        stage,
                        family: section.table.clone(),
                        elapsed_seconds: began.elapsed().as_secs_f64(),
                        wait_seconds,
                        queued_plain_bytes,
                        error: Box::new(Error::Postgres(error)),
                    }
                };
            eprintln!(
                "{}",
                serde_json::json!({"event":"restore_section_started","family":section.table,"plain_bytes":section.plain_bytes,"compressed_bytes":section.compressed_bytes,"expected_rows":section.rows})
            );
            let mut decoder = zstd::stream::read::Decoder::new(CheckedReader::new(
                Read::by_ref(file).take(section.compressed_bytes),
            ))?;
            decoder.window_log_max(27)?;
            let query = format!("COPY {table} FROM STDIN (FORMAT binary)");
            let sink = tx
                .copy_in::<_, Bytes>(&query)
                .await
                .map_err(|error| database_error("restore_copy_open", error, None, None))?;
            eprintln!(
                "{}",
                serde_json::json!({"event":"restore_copy_opened","family":section.table,"elapsed_seconds":began.elapsed().as_secs_f64()})
            );
            pin_mut!(sink);
            let mut bytes = 0;
            let mut buf = [0; 65536];
            loop {
                let n = decoder.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                bytes += n as u64;
                if bytes > section.plain_bytes {
                    return Err(Error::Refusal("snapshot_plain_size_mismatch"));
                }
                let send = Instant::now();
                sink.as_mut()
                    .send(Bytes::copy_from_slice(&buf[..n]))
                    .await
                    .map_err(|error| {
                        database_error(
                            "restore_copy_send",
                            error,
                            Some(send.elapsed().as_secs_f64()),
                            Some(bytes - n as u64),
                        )
                    })?;
                if send.elapsed().as_secs() >= 1 {
                    eprintln!(
                        "{}",
                        serde_json::json!({"event":"restore_copy_backpressure","family":section.table,"queued_plain_bytes":bytes,"send_seconds":send.elapsed().as_secs_f64(),"elapsed_seconds":began.elapsed().as_secs_f64(),"note":"queued bytes are not server row acknowledgements"})
                    );
                }
            }
            let (size, hash) = decoder.finish().into_inner().finish();
            if size != section.compressed_bytes || hash != section.sha256 {
                return Err(Error::Refusal("snapshot_section_hash_mismatch"));
            }
            let finish = Instant::now();
            let rows = sink.as_mut().finish().await.map_err(|error| {
                database_error(
                    "restore_copy_finish",
                    error,
                    Some(finish.elapsed().as_secs_f64()),
                    Some(bytes),
                )
            })?;
            if bytes != section.plain_bytes || rows != section.rows {
                return Err(Error::Refusal("snapshot_row_count_mismatch"));
            }
            eprintln!(
                "{}",
                serde_json::json!({"event":"restore_section_finished","family":section.table,"plain_bytes":bytes,"acknowledged_rows":rows,"elapsed_seconds":began.elapsed().as_secs_f64()})
            );
        }
    }
    Ok(())
}
