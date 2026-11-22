//! Internal write routing for the cold-restore prototype.
//!
//! Node queries keep their stable served parents. INSERT/COPY writers must use
//! `write_relation`: inherited PostgreSQL parent indexes do not enforce child
//! uniqueness, and parent ON CONFLICT is not a child conflict arbiter. Empty
//! parents therefore reject direct inserts. UPDATE/DELETE through parents keep
//! PostgreSQL's normal inherited semantics. B4 must consume this boundary; this
//! module does not claim qualification of an unimplemented follower.

use tokio_postgres::GenericClient;

use crate::{Error, NODE_RELATIONS};

pub(crate) const SCHEMA: &str = "dsa_restore";
pub(crate) const PARENT_GUARD_BODY: &str =
    "BEGIN RAISE EXCEPTION 'restore_write_route_required' USING ERRCODE='55000';END";

pub(crate) async fn has_journal<C: GenericClient>(client: &C) -> Result<bool, Error> {
    Ok(client
        .query_one("SELECT to_regclass('dsa.restore_build') IS NOT NULL", &[])
        .await?
        .get(0))
}

/// A post-publication clock is conservative: it cannot age restored events
/// before they became visible. NULL protects the cohort until a writer repairs
/// interrupted settlement. This runs under the existing session writer lock.
pub(crate) async fn recover_retention_anchor<C: GenericClient>(client: &C) -> Result<(), Error> {
    if has_journal(client).await? {
        validate_routes(client).await?;
        client.execute("UPDATE dsa.restore_build SET retention_anchor_at=clock_timestamp(),phase='complete' WHERE id AND phase='published' AND retention_anchor_at IS NULL",&[]).await?;
    }
    Ok(())
}

/// Resolve one validated physical insertion target. No arbitrary identifier or
/// search_path is accepted. Existing non-inherited layouts keep their old route.
pub async fn write_relation<C: GenericClient>(client: &C, family: &str) -> Result<String, Error> {
    if !NODE_RELATIONS.contains(&family) {
        return Err(Error::Refusal("unknown_write_family"));
    }
    client.query_one("SELECT dsa.gate()", &[]).await?;
    let inherited: i64 = client
        .query_one(
            "SELECT count(*) FROM pg_inherits WHERE inhparent=to_regclass($1)",
            &[&format!("serve.{family}")],
        )
        .await?
        .get(0);
    if !has_journal(client).await? {
        if inherited != 0 {
            return Err(Error::Refusal("unowned_storage_inheritance"));
        }
        return Ok(format!("serve.{family}"));
    }
    validate_routes(client).await?;
    recover_retention_anchor(client).await?;
    let ready: bool = client.query_one("SELECT phase='complete' AND retention_anchor_at IS NOT NULL FROM dsa.restore_build WHERE id",&[]).await?.get(0);
    if !ready {
        return Err(Error::Refusal("restore_not_published"));
    }
    let kind: String = client
        .query_one("SELECT storage_kind FROM dsa.restore_build WHERE id", &[])
        .await?
        .get(0);
    Ok(format!(
        "{}.{family}",
        if kind == "direct" { "serve" } else { SCHEMA }
    ))
}

pub(crate) async fn validate_routes<C: GenericClient>(client: &C) -> Result<(), Error> {
    let binding: bool = client.query_one("SELECT b.database_oid=(SELECT oid FROM pg_database WHERE datname=current_database()) AND b.database_name=current_database() AND b.owner_oid=(SELECT datdba FROM pg_database WHERE datname=current_database()) AND b.identity_created_at=i.created_at FROM dsa.restore_build b CROSS JOIN dsa.identity i WHERE b.id AND i.id=1",&[]).await?.get(0);
    if !binding {
        return Err(Error::Refusal("restore_destination_binding_mismatch"));
    }
    if !crate::snapshot::restore_indexes(client)
        .await?
        .missing
        .is_empty()
    {
        return Err(Error::Refusal("restore_public_index_missing"));
    }
    let kind: String = client
        .query_one(
            "SELECT storage_kind FROM dsa.restore_build WHERE id AND protocol=1",
            &[],
        )
        .await?
        .get(0);
    if kind == "inherited" {
        let guard:bool=client.query_one("SELECT EXISTS(SELECT 1 FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang CROSS JOIN dsa.restore_build b WHERE b.id AND p.oid=to_regprocedure('dsa.restore_parent_insert_guard()') AND p.proowner=b.owner_oid AND p.prosrc=$1 AND NOT p.prosecdef AND NOT p.proretset AND p.pronargs=0 AND p.prorettype='trigger'::regtype AND l.lanname='plpgsql')",&[&PARENT_GUARD_BODY]).await?.get(0);
        if !guard {
            return Err(Error::Refusal("restore_parent_guard_mismatch"));
        }
    }
    for family in NODE_RELATIONS {
        if kind == "direct" {
            let valid: bool=client.query_one("SELECT b.parents->>($1::text)=(to_regclass('serve.'||$1)::oid)::text AND NOT EXISTS(SELECT 1 FROM pg_inherits WHERE inhparent=to_regclass('serve.'||$1)) FROM dsa.restore_build b WHERE b.id",&[&family]).await?.get(0);
            if !valid {
                return Err(Error::Refusal("restore_storage_binding_mismatch"));
            }
            continue;
        }
        let valid: bool = client.query_one("SELECT b.parents->>($1::text)=(to_regclass('serve.'||$1)::oid)::text AND b.children->>($1::text)=(to_regclass('dsa_restore.'||$1)::oid)::text AND (SELECT count(*)=1 AND bool_and(inhrelid=to_regclass('dsa_restore.'||$1)) FROM pg_inherits WHERE inhparent=to_regclass('serve.'||$1)) AND NOT EXISTS(SELECT 1 FROM pg_inherits WHERE inhparent=to_regclass('dsa_restore.'||$1)) AND (SELECT count(*)=1 FROM pg_inherits WHERE inhrelid=to_regclass('dsa_restore.'||$1)) AND EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=to_regclass('serve.'||$1) AND tgname='restore_empty_parent' AND tgenabled='O' AND tgtype=7 AND tgnargs=0 AND tgqual IS NULL AND NOT tgisinternal AND tgfoid=to_regprocedure('dsa.restore_parent_insert_guard()')) FROM dsa.restore_build b WHERE b.id",&[&family]).await?.get(0);
        if !valid {
            return Err(Error::Refusal("restore_storage_binding_mismatch"));
        }
        let occupied: bool = client
            .query_one(
                &format!("SELECT EXISTS(SELECT 1 FROM ONLY serve.{family})"),
                &[],
            )
            .await?
            .get(0);
        if occupied {
            return Err(Error::Refusal("restore_parent_not_empty"));
        }
        let key = if family == "epoch_param" {
            "epoch_no"
        } else {
            "id"
        };
        let primary: bool=client.query_one("SELECT count(*)=1 AND bool_and(i.indisprimary AND i.indisunique AND i.indisvalid AND i.indisready AND i.indislive AND i.indnkeyatts=1 AND i.indnatts=1 AND i.indpred IS NULL AND i.indexprs IS NULL AND i.indkey[0]=(SELECT attnum FROM pg_attribute WHERE attrelid=i.indrelid AND attname=$2)) FROM pg_index i WHERE i.indrelid=to_regclass($1) AND i.indisprimary",&[&format!("dsa_restore.{family}"),&key]).await?.get(0);
        if !primary {
            return Err(Error::Refusal("restore_private_index_mismatch"));
        }
    }
    if kind == "inherited" {
        for sql in include_str!("indexes.sql")
            .lines()
            .filter(|s| s.starts_with("CREATE"))
        {
            let name = sql
                .split("INDEX ")
                .nth(1)
                .and_then(|s| s.split_whitespace().next())
                .ok_or(Error::Refusal("invalid_index_catalogue"))?;
            let table = sql
                .split(" ON serve.")
                .nth(1)
                .and_then(|s| s.split(['(', ' ']).next())
                .ok_or(Error::Refusal("invalid_index_catalogue"))?;
            let row=client.query_opt("SELECT pg_get_indexdef(c.oid),i.indisvalid AND i.indisready AND i.indislive AND i.indrelid=to_regclass($2) FROM pg_class c JOIN pg_index i ON i.indexrelid=c.oid WHERE c.oid=to_regclass($1)",&[&format!("dsa_restore.restore_{name}"),&format!("dsa_restore.{table}")]).await?;
            let Some(row) = row else {
                return Err(Error::Refusal("restore_private_index_mismatch"));
            };
            let actual = row
                .get::<_, String>(0)
                .replace(&format!("INDEX restore_{name} "), &format!("INDEX {name} "))
                .replace(" ON dsa_restore.", " ON serve.");
            if !row.get::<_, bool>(1)
                || crate::snapshot::index_shape(&actual) != crate::snapshot::index_shape(sql)
            {
                return Err(Error::Refusal("restore_private_index_mismatch"));
            }
        }
    }
    Ok(())
}

/// Physical bytes include the one owned child, unlike PostgreSQL's parent-only
/// pg_total_relation_size. Route validation rejects extra/grandchild relations.
pub(crate) async fn relation_bytes<C: GenericClient>(
    client: &C,
    table: &str,
) -> Result<i64, Error> {
    Ok(client.query_one("SELECT pg_total_relation_size($1::text::regclass)+coalesce((SELECT sum(pg_total_relation_size(inhrelid))::bigint FROM pg_inherits WHERE inhparent=$1::text::regclass),0)",&[&table]).await?.get(0))
}

/// Effective retention time. None means protected/unanchored or not yet served;
/// pruning must not treat it as an old event. Only the immutable initial cohort
/// uses the restore anchor; later heights keep their ordinary per-height clock.
pub async fn first_served_at<C: GenericClient>(
    client: &C,
    height: i64,
) -> Result<Option<String>, Error> {
    if has_journal(client).await? {
        return Ok(client.query_one("SELECT to_char(CASE WHEN $1::bigint BETWEEN b.initial_horizon AND b.initial_target THEN b.retention_anchor_at ELSE (SELECT first_served_at FROM dsa.first_served WHERE height=$1::bigint) END AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') FROM dsa.restore_build b WHERE b.id",&[&height]).await?.get(0));
    }
    Ok(client.query_one("SELECT to_char((SELECT first_served_at FROM dsa.first_served WHERE height=$1::bigint) AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')",&[&height]).await?.get(0))
}
