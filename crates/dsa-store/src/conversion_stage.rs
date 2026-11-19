//! Private, temporary conversion copy. Every source COPY is fully drained into
//! one bounded page before any destination write. Binary COPY preserves SQL
//! NULL/JSON null and all supported PostgreSQL values without a Rust decoder.
//! The unmodified A3 predicate is evaluated on the verified private copy only.

use crate::{
    Error, NODE_RELATIONS, canonical,
    convert::{ConvertOptions, FamilyCost, columns},
    keep::{Selection, SourceSchema},
    verify::{MARKERS, ServingState},
};
use bytes::{Bytes, BytesMut};
use dsa_chain::digest::{FamilyDigest, FamilyHasher};
use futures_util::{SinkExt, StreamExt, pin_mut};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{collections::BTreeMap, time::Instant};
use tokio_postgres::Transaction;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RawFamilyCost {
    pub rows: u64,
    pub wire_bytes: u64,
    pub pages: u64,
    pub source_seconds: f64,
    pub destination_seconds: f64,
    pub longest_source_page_seconds: f64,
    pub largest_row_bytes: u64,
    pub temporary_relation_bytes: i64,
}
#[derive(Debug, Default)]
pub(crate) struct RawState {
    pub digests: BTreeMap<String, FamilyDigest>,
    pub costs: BTreeMap<String, RawFamilyCost>,
    pub temporary_bytes: i64,
}
pub(crate) struct Selected {
    pub families: BTreeMap<String, FamilyDigest>,
    pub markers: FamilyDigest,
    pub costs: BTreeMap<String, FamilyCost>,
    pub temporary_bytes: i64,
}

fn key(table: &str) -> &str {
    if table == "epoch_param" {
        "epoch_no"
    } else {
        "id"
    }
}
fn bounds(table: &str, state: &ServingState) -> (i64, i64) {
    let t = i64::from(state.target.height);
    match table {
        "block" => (0, i64::from(state.tip.height)),
        "tx" => (0, t * 4096 + 4095),
        "tx_metadata" => (i64::from(state.horizon.height) * 4096, t * 4096 + 4095),
        "tx_out" | "tx_in" => (0, t * 16777216 + 16777215),
        "ma_tx_out" => (0, t * 68719476736 + 68719476735),
        _ => (i64::MIN, i64::MAX),
    }
}
fn names(table: &str, alias: &str) -> String {
    columns(table)
        .iter()
        .map(|(n, _, _)| format!("{alias}.\"{n}\""))
        .collect::<Vec<_>>()
        .join(",")
}
/// Recursion asks for one indexed successor at a time and stops at the byte or
/// row budget. At most one lookahead is excluded and read on the next page.
/// A single oversized first row is accepted alone. JSON is used ONLY to obtain
/// a conservative wire-size bound; the actual transfer is PostgreSQL binary.
fn page_sql(schema: &str, table: &str) -> String {
    let k = key(table);
    let overhead = 64 + 4 * columns(table).len();
    let size = format!("octet_length(to_jsonb(s)::text)::bigint+{overhead}");
    let fields = columns(table)
        .iter()
        .map(|(n, _, _)| format!("(fact).\"{n}\""))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "COPY (WITH RECURSIVE page AS ((SELECT s AS fact,s.{k}::bigint AS id,{size} AS bytes,1::bigint AS n FROM {schema}.{table} s WHERE ($1::bigint IS NULL OR s.{k}>$1) AND s.{k} BETWEEN $2::bigint AND $3::bigint ORDER BY s.{k} LIMIT 1) UNION ALL SELECT successor.fact,successor.id,p.bytes+successor.bytes,p.n+1 FROM page p CROSS JOIN LATERAL (SELECT s AS fact,s.{k}::bigint AS id,{size} AS bytes FROM {schema}.{table} s WHERE s.{k}>p.id AND s.{k} BETWEEN $2::bigint AND $3::bigint ORDER BY s.{k} LIMIT 1) successor WHERE p.bytes<$4::bigint AND p.n<$5::bigint) SELECT {fields} FROM page WHERE bytes<=$4::bigint OR n=1 ORDER BY page.id) TO STDOUT(FORMAT binary)"
    )
}
// COPY does not accept bound protocol parameters. Substitute only validated
// integer values into a closed catalogue, never user text/DSNs/identifiers.
fn concrete_page(
    schema: &str,
    table: &str,
    state: &ServingState,
    after: Option<i64>,
    options: &ConvertOptions,
) -> String {
    let (low, high) = bounds(table, state);
    page_sql(schema, table)
        .replace(
            "$1",
            &after.map_or_else(|| "NULL".to_owned(), |n| format!("({n})")),
        )
        .replace("$2", &format!("({low})"))
        .replace("$3", &format!("({high})"))
        .replace("$4", &options.batch_bytes.to_string())
        .replace("$5", &options.batch_rows.min(4096).to_string())
}
const SET_PLANNER: &str = "SELECT set_config('enable_seqscan',$1,true),set_config('enable_bitmapscan',$2,true),set_config('enable_indexscan',$3,true),set_config('enable_indexonlyscan',$4,true)";
async fn ordered(tx: &Transaction<'_>) -> Result<Vec<String>, Error> {
    let r=tx.query_one("SELECT current_setting('enable_seqscan'),current_setting('enable_bitmapscan'),current_setting('enable_indexscan'),current_setting('enable_indexonlyscan')",&[]).await?;
    let old = (0..4).map(|n| r.get::<_, String>(n)).collect();
    tx.query_one(SET_PLANNER, &[&"off", &"off", &"on", &"on"])
        .await?;
    Ok(old)
}
async fn restore(tx: &Transaction<'_>, old: &[String]) -> Result<(), Error> {
    let r = tx
        .query_one(SET_PLANNER, &[&old[0], &old[1], &old[2], &old[3]])
        .await?;
    if (0..4).any(|n| r.get::<_, String>(n) != old[n]) {
        return Err(Error::Refusal("conversion_planner_restore_failed"));
    }
    Ok(())
}
struct Page {
    wire: Bytes,
    rows: u64,
    last: Option<i64>,
    largest: u64,
    seconds: f64,
}
fn invalid() -> Error {
    Error::Refusal("invalid_conversion_binary_page")
}
fn take<'a>(wire: &'a [u8], pos: &mut usize, n: usize) -> Result<&'a [u8], Error> {
    let end = pos.checked_add(n).ok_or_else(invalid)?;
    let out = wire.get(*pos..end).ok_or_else(invalid)?;
    *pos = end;
    Ok(out)
}
fn parse(
    wire: &[u8],
    table: &str,
    after: Option<i64>,
    hash: &mut FamilyHasher,
) -> Result<(u64, Option<i64>, u64), Error> {
    if wire.get(..11) != Some(b"PGCOPY\n\xff\r\n\0".as_slice()) {
        return Err(invalid());
    }
    let mut pos = 11;
    let flags = i32::from_be_bytes(take(wire, &mut pos, 4)?.try_into().map_err(|_| invalid())?);
    let extra = i32::from_be_bytes(take(wire, &mut pos, 4)?.try_into().map_err(|_| invalid())?);
    if flags != 0 || extra != 0 {
        return Err(invalid());
    }
    let (mut count, mut last, mut largest) = (0, after, 0);
    let expected_fields = columns(table).len();
    loop {
        let start = pos;
        let fields =
            i16::from_be_bytes(take(wire, &mut pos, 2)?.try_into().map_err(|_| invalid())?);
        if fields == -1 {
            break;
        }
        if usize::try_from(fields).ok() != Some(expected_fields) {
            return Err(invalid());
        }
        let mut id = None;
        for field in 0..fields {
            let len =
                i32::from_be_bytes(take(wire, &mut pos, 4)?.try_into().map_err(|_| invalid())?);
            if len < -1 {
                return Err(invalid());
            }
            let data = if len == -1 {
                None
            } else {
                Some(take(wire, &mut pos, len as usize)?)
            };
            if field == 0 {
                let data = data.ok_or_else(invalid)?;
                id = Some(if table == "epoch_param" {
                    i64::from(i32::from_be_bytes(data.try_into().map_err(|_| invalid())?))
                } else {
                    i64::from_be_bytes(data.try_into().map_err(|_| invalid())?)
                });
            }
        }
        let id = id.ok_or_else(invalid)?;
        if last.is_some_and(|previous| id <= previous) {
            return Err(invalid());
        }
        last = Some(id);
        count += 1;
        largest = largest.max((pos - start) as u64);
        hash.row(&wire[start..pos]);
    }
    if pos != wire.len() {
        return Err(invalid());
    }
    Ok((count, if count == 0 { None } else { last }, largest))
}
async fn read_page(
    tx: &Transaction<'_>,
    schema: &str,
    table: &'static str,
    state: &ServingState,
    after: Option<i64>,
    options: &ConvertOptions,
    hash: &mut FamilyHasher,
) -> Result<Page, Error> {
    let sql = concrete_page(schema, table, state, after, options);
    let began = Instant::now();
    eprintln!(
        "{}",
        json!({"event":"source_page_started","family":table,"schema":schema,"after_id":after,"row_limit":options.batch_rows.min(4096),"byte_budget":options.batch_bytes})
    );
    let stream = tx.copy_out(&sql).await.map_err(|error| Error::SourceRead {
        family: table,
        elapsed_seconds: began.elapsed().as_secs_f64(),
        error,
    })?;
    pin_mut!(stream);
    let mut wire = BytesMut::new();
    let mut first = true;
    while let Some(chunk) = stream.next().await {
        let chunk=chunk.map_err(|error|{
            eprintln!("{}",json!({"event":"source_page_failed","family":table,"schema":schema,"after_id":after,"wire_bytes_received":wire.len(),"elapsed_seconds":began.elapsed().as_secs_f64()}));
            Error::SourceRead{family:table,elapsed_seconds:began.elapsed().as_secs_f64(),error}
        })?;
        if first {
            eprintln!(
                "{}",
                json!({"event":"source_page_first_copy_chunk","family":table,"schema":schema,"after_id":after,"seconds":began.elapsed().as_secs_f64(),"note":"COPY header may precede first row"})
            );
            first = false;
        }
        wire.extend_from_slice(&chunk);
    }
    let (rows, last, largest) = parse(&wire, table, after, hash)?;
    if rows > options.batch_rows.min(4096) as u64
        || (rows > 1 && wire.len() > options.batch_bytes + 21)
    {
        return Err(invalid());
    }
    Ok(Page {
        wire: wire.freeze(),
        rows,
        last,
        largest,
        seconds: began.elapsed().as_secs_f64(),
    })
}

pub(crate) async fn stage(
    source: &Transaction<'_>,
    destination: &Transaction<'_>,
    state: &ServingState,
    options: &ConvertOptions,
) -> Result<RawState, Error> {
    let before = ordered(source).await?;
    let mut result = RawState::default();
    for table in NODE_RELATIONS {
        destination
            .batch_execute(&format!(
                "CREATE TEMP TABLE {table}(LIKE serve.{table} INCLUDING ALL) ON COMMIT DROP"
            ))
            .await?;
        let began = Instant::now();
        let mut cost = RawFamilyCost::default();
        let mut hash = FamilyHasher::new(table);
        let mut after = None;
        eprintln!(
            "{}",
            json!({"event":"source_family_started","family":table,"mode":"bounded_binary_raw_staging"})
        );
        loop {
            let page = read_page(source, "public", table, state, after, options, &mut hash).await?;
            cost.pages += 1;
            cost.source_seconds += page.seconds;
            cost.longest_source_page_seconds = cost.longest_source_page_seconds.max(page.seconds);
            cost.largest_row_bytes = cost.largest_row_bytes.max(page.largest);
            let write = Instant::now();
            if page.rows > 0 {
                let cols = columns(table)
                    .iter()
                    .map(|(n, _, _)| format!("\"{n}\""))
                    .collect::<Vec<_>>()
                    .join(",");
                let sink = destination
                    .copy_in::<_, Bytes>(&format!(
                        "COPY pg_temp.{table}({cols}) FROM STDIN(FORMAT binary)"
                    ))
                    .await?;
                pin_mut!(sink);
                sink.as_mut().send(page.wire.clone()).await?;
                if sink.as_mut().finish().await? != page.rows {
                    return Err(Error::Refusal("conversion_copy_row_mismatch"));
                }
            }
            let destination_seconds = write.elapsed().as_secs_f64();
            cost.destination_seconds += destination_seconds;
            cost.rows += page.rows;
            cost.wire_bytes += page.wire.len() as u64;
            eprintln!(
                "{}",
                json!({"event":"source_page_finished","family":table,"after_id":after,"last_id":page.last,"rows":page.rows,"wire_bytes":page.wire.len(),"source_seconds":page.seconds,"destination_write_seconds":destination_seconds,"completed_rows":cost.rows,"pages":cost.pages})
            );
            if let Some(id) = page.last {
                after = Some(id);
            } else {
                break;
            }
        }
        cost.temporary_relation_bytes = destination
            .query_one(
                "SELECT pg_total_relation_size($1::text::regclass)",
                &[&format!("pg_temp.{table}")],
            )
            .await?
            .get(0);
        result.temporary_bytes += cost.temporary_relation_bytes;
        eprintln!(
            "{}",
            json!({"event":"source_family_finished","family":table,"mode":"raw_staging","elapsed_seconds":began.elapsed().as_secs_f64(),"cost":cost})
        );
        result.digests.insert(table.to_owned(), hash.finish());
        result.costs.insert(table.to_owned(), cost);
    }
    restore(source, &before).await?;
    eprintln!(
        "{}",
        json!({"event":"conversion_raw_staging_complete","temporary_relation_bytes":result.temporary_bytes,"planner_restored":before})
    );
    Ok(result)
}

pub(crate) async fn select(
    destination: &Transaction<'_>,
    state: &ServingState,
    options: &ConvertOptions,
    raw: &RawState,
    selection: &Selection,
) -> Result<Selected, Error> {
    let before = ordered(destination).await?;
    // Independent byte-for-byte typed transport verification, itself paged.
    for table in NODE_RELATIONS {
        let mut hash = FamilyHasher::new(table);
        let mut after = None;
        loop {
            let page = read_page(
                destination,
                "pg_temp",
                table,
                state,
                after,
                options,
                &mut hash,
            )
            .await?;
            if let Some(id) = page.last {
                after = Some(id);
            } else {
                break;
            }
        }
        if raw.digests.get(table) != Some(&hash.finish()) {
            return Err(Error::Refusal("conversion_raw_digest_mismatch"));
        }
    }
    restore(destination, &before).await?;
    for line in include_str!("indexes.sql")
        .lines()
        .filter(|line| line.starts_with("CREATE"))
    {
        let sql = line
            .replace("CREATE UNIQUE INDEX ", "CREATE UNIQUE INDEX conversion_")
            .replace("CREATE INDEX ", "CREATE INDEX conversion_")
            .replace("ON serve.", "ON pg_temp.");
        destination.batch_execute(&sql).await?;
    }
    let mut indexed_bytes = 0_i64;
    for table in NODE_RELATIONS {
        destination
            .batch_execute(&format!("ANALYZE pg_temp.{table}"))
            .await?;
        indexed_bytes += destination
            .query_one(
                "SELECT pg_total_relation_size($1::text::regclass)",
                &[&format!("pg_temp.{table}")],
            )
            .await?
            .get::<_, i64>(0);
    }
    let unions=NODE_RELATIONS.iter().map(|table|{
        if *table=="ma_tx_out" {format!("SELECT 'ma_tx_out'::text AS family,id FROM (SELECT id FROM keep_ma_tx_out UNION SELECT a.id FROM pg_temp.ma_tx_out a WHERE a.id BETWEEN $1::bigint*68719476736 AND $2::bigint*68719476736+68719476735 AND ({})) selected",Selection::asset_predicate(SourceSchema::Temporary,"a"))}
        else {format!("SELECT '{table}'::text AS family,id FROM keep_{table}")}
    }).collect::<Vec<_>>().join(" UNION ALL ");
    eprintln!(
        "{}",
        json!({"event":"conversion_local_selection_started","source_reads_required":false,"temporary_relation_bytes":raw.temporary_bytes})
    );
    destination
        .execute(
            &format!(
                "CREATE TEMP TABLE conversion_keep_keys ON COMMIT DROP AS {} {unions}",
                Selection::ctes_sql(SourceSchema::Temporary)
            ),
            &selection.parameters(),
        )
        .await?;
    destination.batch_execute("ALTER TABLE pg_temp.conversion_keep_keys ADD PRIMARY KEY(family,id);ANALYZE pg_temp.conversion_keep_keys").await?;
    let temporary_bytes: i64 = destination
        .query_one(
            "SELECT pg_total_relation_size('pg_temp.conversion_keep_keys')",
            &[],
        )
        .await?
        .get(0);
    let mut families = BTreeMap::new();
    let mut costs = BTreeMap::new();
    let mut markers = FamilyHasher::new(MARKERS);
    for table in NODE_RELATIONS {
        let sql = canonical::selected_staging(table)?;
        let began = Instant::now();
        let rows = destination.query_raw(&sql, selection.parameters()).await?;
        pin_mut!(rows);
        let mut digest = FamilyHasher::new(table);
        let (mut count, mut bytes) = (0, 0);
        while let Some(row) = rows.next().await {
            let row = row?;
            let original: &str = row.try_get(0)?;
            let canonical: &str = row.try_get(1)?;
            let marker: Option<&str> = row.try_get(3)?;
            digest.row(canonical.as_bytes());
            count += 1;
            bytes += original.len() as u64;
            if let Some(marker) = marker {
                markers.row(marker.as_bytes());
            }
        }
        let k = key(table);
        let fields = names(table, "s");
        let inserted=destination.execute(&format!("INSERT INTO serve.{table} SELECT {fields} FROM pg_temp.{table} s JOIN pg_temp.conversion_keep_keys k ON k.id=s.{k} WHERE k.family='{table}'"),&[]).await?;
        if inserted != count {
            return Err(Error::Refusal("conversion_selected_row_mismatch"));
        }
        costs.insert(
            table.to_owned(),
            FamilyCost {
                rows: count,
                jsonb_utf8_bytes: bytes,
                batches: 1,
                elapsed_seconds: began.elapsed().as_secs_f64(),
                sql_sha256: format!(
                    "{:x}",
                    <sha2::Sha256 as sha2::Digest>::digest(sql.as_bytes())
                ),
            },
        );
        families.insert(table.to_owned(), digest.finish());
    }
    destination.execute("INSERT INTO dsa.consumed_before_horizon SELECT o.id FROM pg_temp.tx_out o JOIN pg_temp.conversion_keep_keys k ON k.id=o.id AND k.family='tx_out' WHERE o.id<$1::bigint*16777216 AND EXISTS(SELECT 1 FROM pg_temp.tx_in i WHERE i.tx_out_id=o.tx_id AND i.tx_out_index=o.index AND i.id BETWEEN 0 AND $1::bigint*16777216-1)",&[&i64::from(state.horizon.height)]).await?;
    eprintln!(
        "{}",
        json!({"event":"conversion_local_selection_finished","temporary_relation_bytes":indexed_bytes+temporary_bytes})
    );
    Ok(Selected {
        families,
        markers: markers.finish(),
        costs,
        temporary_bytes: indexed_bytes + temporary_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn frame(id: i64, value: Option<&[u8]>) -> Vec<u8> {
        let mut out = b"PGCOPY\n\xff\r\n\0".to_vec();
        out.extend(0_i32.to_be_bytes());
        out.extend(0_i32.to_be_bytes());
        out.extend(3_i16.to_be_bytes());
        out.extend(8_i32.to_be_bytes());
        out.extend(id.to_be_bytes());
        out.extend(32_i32.to_be_bytes());
        out.extend([7; 32]);
        if let Some(value) = value {
            out.extend((value.len() as i32).to_be_bytes());
            out.extend(value);
        } else {
            out.extend((-1_i32).to_be_bytes());
        }
        out.extend((-1_i16).to_be_bytes());
        out
    }
    #[test]
    fn binary_transport_nulls_limits_and_corruption() {
        let sql_null = frame(i64::MIN, None);
        let json_null = frame(i64::MIN, Some(b"\x01null"));
        let mut a = FamilyHasher::new("datum");
        let mut b = FamilyHasher::new("datum");
        assert_eq!(parse(&sql_null, "datum", None, &mut a).unwrap().0, 1);
        assert_eq!(
            parse(&json_null, "datum", None, &mut b).unwrap().1,
            Some(i64::MIN)
        );
        assert_ne!(a.finish(), b.finish());
        for broken in [
            sql_null[..sql_null.len() - 1].to_vec(),
            [sql_null.as_slice(), b"extra"].concat(),
        ] {
            assert!(parse(&broken, "datum", None, &mut FamilyHasher::new("datum")).is_err());
        }
        assert!(
            parse(
                &json_null,
                "datum",
                Some(i64::MIN),
                &mut FamilyHasher::new("datum")
            )
            .is_err()
        );
        let huge = frame(i64::MAX, Some(&vec![0; 100_000]));
        let (count, last, largest) =
            parse(&huge, "datum", None, &mut FamilyHasher::new("datum")).unwrap();
        assert_eq!((count, last), (1, Some(i64::MAX)));
        assert!(largest > 100_000);
    }
}
