//! Closed SQL catalogue for natural-key payload streams; no source projection.

use crate::Error;

pub(crate) fn query(table: &str) -> Result<String, Error> {
    let (payload, from, order) = match table {
        "block" => (
            "b.block_no,encode(b.hash,'hex'),b.epoch_no,b.slot_no,(extract(epoch FROM (b.time-timestamp '2000-01-01'))*1000000)::numeric(30,0)::text,b.tx_count",
            "serve.block b",
            "b.block_no,b.hash",
        ),
        "tx" => (
            "encode(t.hash,'hex'),t.block_id,t.block_index,t.valid_contract",
            "serve.tx t",
            "t.hash",
        ),
        "tx_out" => (
            "encode(t.hash,'hex'),o.index,o.address,o.value::text,encode(o.data_hash,'hex'),o.consumed_by_tx_id",
            "serve.tx_out o JOIN serve.tx t ON t.id=o.tx_id",
            "t.hash,o.index",
        ),
        "tx_in" => (
            "encode(t.hash,'hex'),encode(origin.hash,'hex'),i.tx_out_index,CASE WHEN i.id>=h.height*16777216 THEN i.id-i.tx_in_id*4096 ELSE NULL END",
            "serve.tx_in i JOIN serve.tx t ON t.id=i.tx_in_id JOIN serve.tx origin ON origin.id=i.tx_out_id CROSS JOIN h",
            "t.hash,origin.hash,i.tx_out_index",
        ),
        "ma_tx_out" => (
            "encode(t.hash,'hex'),o.index,encode(m.policy,'hex'),encode(m.name,'hex'),a.quantity::text,CASE WHEN a.id>=h.height*68719476736 THEN a.id-a.tx_out_id*4096 ELSE NULL END",
            "serve.ma_tx_out a JOIN serve.tx_out o ON o.id=a.tx_out_id JOIN serve.tx t ON t.id=o.tx_id JOIN serve.multi_asset m ON m.id=a.ident CROSS JOIN h",
            "t.hash,o.index,m.policy,m.name",
        ),
        "multi_asset" => (
            "encode(m.policy,'hex'),encode(m.name,'hex')",
            "serve.multi_asset m",
            "m.policy,m.name",
        ),
        "datum" => ("encode(d.hash,'hex'),d.value", "serve.datum d", "d.hash"),
        "epoch_param" => (
            "e.epoch_no,encode(e.nonce,'hex'),e.max_tx_size",
            "serve.epoch_param e",
            "e.epoch_no",
        ),
        "pool_hash" => (
            "encode(p.hash_raw,'hex')",
            "serve.pool_hash p",
            "p.hash_raw",
        ),
        "epoch_stake" => (
            "s.epoch_no,encode(p.hash_raw,'hex'),s.amount::text",
            "serve.epoch_stake s JOIN serve.pool_hash p ON p.id=s.pool_id",
            "s.epoch_no,p.hash_raw,s.amount",
        ),
        "tx_metadata" => (
            "encode(t.hash,'hex'),m.key::text,m.json",
            "serve.tx_metadata m JOIN serve.tx t ON t.id=m.tx_id",
            "t.hash,m.key",
        ),
        "consumed_before_horizon" => (
            "encode(t.hash,'hex'),o.index",
            "dsa.consumed_before_horizon c JOIN serve.tx_out o ON o.id=c.tx_out_id JOIN serve.tx t ON t.id=o.tx_id",
            "t.hash,o.index",
        ),
        _ => return Err(Error::Refusal("invalid_snapshot_table")),
    };
    Ok(format!(
        "WITH h AS (SELECT $1::bigint AS height) SELECT jsonb_build_array({payload}) FROM {from} ORDER BY {order}"
    ))
}
