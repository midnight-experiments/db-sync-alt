-- Parameters are H, T, P, profile addresses, governance addresses/policies,
-- permissioned policy/name, and fixed policies/names. __SOURCE__ is a closed
-- schema choice. Positional ranges use inclusive upper bounds to fit bigint.
WITH
horizon AS MATERIALIZED (
 SELECT epoch_no::integer AS epoch FROM __SOURCE__.block WHERE id=$1::bigint
),
window_blocks AS MATERIALIZED (
 SELECT id FROM __SOURCE__.block WHERE id BETWEEN $1::bigint AND $3::bigint
),
window_transactions AS MATERIALIZED (
 SELECT id FROM __SOURCE__.tx WHERE id BETWEEN $1::bigint*4096 AND $2::bigint*4096+4095
),
window_outputs AS MATERIALIZED (
 SELECT id FROM __SOURCE__.tx_out WHERE id BETWEEN $1::bigint*16777216 AND $2::bigint*16777216+16777215
),
window_inputs AS MATERIALIZED (
 SELECT id FROM __SOURCE__.tx_in WHERE id BETWEEN $1::bigint*16777216 AND $2::bigint*16777216+16777215
),
old_unspent AS MATERIALIZED (
 SELECT o.id FROM __SOURCE__.tx_out o
 WHERE o.id>=0 AND o.id<$1::bigint*16777216
 AND NOT EXISTS(SELECT 1 FROM __SOURCE__.tx_in i
  WHERE i.tx_out_id=o.tx_id AND i.tx_out_index=o.index AND i.id>=0 AND i.id<$1::bigint*16777216)
),
governance_candidates AS MATERIALIZED (
 SELECT o.id,o.tx_id,t.block_id,g.body FROM unnest($5::text[],$6::bytea[]) WITH ORDINALITY g(address,policy,body)
 JOIN __SOURCE__.tx_out o ON o.address=g.address AND o.id>=0 AND o.id<$1::bigint*16777216
 JOIN __SOURCE__.datum d ON d.hash=o.data_hash
 JOIN __SOURCE__.tx t ON t.id=o.tx_id AND t.id>=0 AND t.id<$1::bigint*4096
 JOIN __SOURCE__.block b ON b.id=t.block_id AND b.id>=0 AND b.id<$1::bigint
 WHERE EXISTS(SELECT 1 FROM __SOURCE__.ma_tx_out a JOIN __SOURCE__.multi_asset m ON m.id=a.ident
  WHERE a.tx_out_id=o.id AND m.policy=g.policy AND a.id>=0 AND a.id<$1::bigint*68719476736)
),
governance_last_transactions AS MATERIALIZED (
 SELECT body,max(tx_id) AS tx_id FROM governance_candidates GROUP BY body
),
governance_carry AS MATERIALIZED (
 SELECT c.id,c.tx_id,c.block_id FROM governance_candidates c
 JOIN governance_last_transactions last ON last.body=c.body AND last.tx_id=c.tx_id
),
permissioned_candidates AS MATERIALIZED (
 SELECT o.id,o.tx_id,t.block_id,b.epoch_no FROM __SOURCE__.multi_asset m
 JOIN __SOURCE__.ma_tx_out a ON a.ident=m.id AND a.id>=0 AND a.id<$1::bigint*68719476736
 JOIN __SOURCE__.tx_out o ON o.id=a.tx_out_id AND o.id>=0 AND o.id<$1::bigint*16777216
 JOIN __SOURCE__.tx t ON t.id=o.tx_id AND t.id>=0 AND t.id<$1::bigint*4096
 JOIN __SOURCE__.block b ON b.id=t.block_id AND b.id>=0 AND b.id<$1::bigint
 WHERE m.policy=$7::bytea AND m.name=$8::bytea
),
permissioned_last_transaction AS MATERIALIZED (
 SELECT max(c.tx_id) AS tx_id FROM permissioned_candidates c CROSS JOIN horizon h WHERE c.epoch_no<=h.epoch-2
),
permissioned_carry AS MATERIALIZED (
 SELECT c.id,c.tx_id,c.block_id FROM permissioned_candidates c CROSS JOIN horizon h
 WHERE c.epoch_no>h.epoch-2
 UNION SELECT c.id,c.tx_id,c.block_id FROM permissioned_candidates c JOIN permissioned_last_transaction last ON last.tx_id=c.tx_id
),
epoch_cutoffs AS (SELECT epoch-2 AS epoch FROM horizon UNION ALL SELECT epoch-1 FROM horizon),
old_headers AS MATERIALIZED (
 SELECT id,epoch_no,slot_no FROM __SOURCE__.block WHERE id>=0 AND id<$1::bigint
),
epoch_last_slots AS MATERIALIZED (
 SELECT e.epoch,max(b.slot_no) AS slot_no FROM epoch_cutoffs e
 JOIN old_headers b ON b.epoch_no<=e.epoch AND b.slot_no IS NOT NULL GROUP BY e.epoch
),
epoch_headers AS MATERIALIZED (
 SELECT b.id FROM old_headers b JOIN epoch_last_slots e ON b.epoch_no<=e.epoch AND b.slot_no=e.slot_no
),
fallback_candidates AS MATERIALIZED (
 SELECT id,tx_in_id,tx_out_id,tx_out_index FROM __SOURCE__.tx_in
 WHERE id>=0 AND id<$1::bigint*16777216 AND NOT EXISTS(SELECT 1 FROM window_inputs)
),
fallback_last_id AS MATERIALIZED (
 SELECT max(id) AS id FROM fallback_candidates
),
fallback_input AS MATERIALIZED (
 SELECT i.* FROM fallback_candidates i JOIN fallback_last_id last ON i.id=last.id
),
keep_tx_out AS MATERIALIZED (
 SELECT id FROM window_outputs UNION SELECT id FROM old_unspent
 UNION SELECT id FROM governance_carry UNION SELECT id FROM permissioned_carry
 UNION SELECT tx_out_id*4096+tx_out_index FROM fallback_input
),
keep_block AS MATERIALIZED (
 SELECT id FROM window_blocks UNION SELECT block_id FROM governance_carry
 UNION SELECT block_id FROM permissioned_carry UNION SELECT id FROM epoch_headers
),
keep_tx AS MATERIALIZED (
 SELECT id FROM window_transactions UNION SELECT tx_in_id FROM fallback_input
 UNION SELECT o.tx_id FROM __SOURCE__.tx_out o JOIN keep_tx_out k ON k.id=o.id
 WHERE o.id BETWEEN 0 AND $2::bigint*16777216+16777215
),
keep_tx_in AS MATERIALIZED (SELECT id FROM window_inputs UNION SELECT id FROM fallback_input),
keep_ma_tx_out AS MATERIALIZED (
 SELECT a.id FROM __SOURCE__.ma_tx_out a JOIN keep_tx_out o ON o.id=a.tx_out_id
 WHERE a.id BETWEEN 0 AND $2::bigint*68719476736+68719476735
),
keep_multi_asset AS MATERIALIZED (
 SELECT m.id FROM unnest($9::bytea[],$10::bytea[]) f(policy,name)
 JOIN __SOURCE__.multi_asset m ON m.policy=f.policy AND m.name=f.name
 UNION SELECT a.ident FROM __SOURCE__.ma_tx_out a JOIN keep_ma_tx_out k ON k.id=a.id
 WHERE a.id BETWEEN 0 AND $2::bigint*68719476736+68719476735
),
keep_datum AS MATERIALIZED (
 SELECT DISTINCT d.id FROM __SOURCE__.tx_out o JOIN keep_tx_out k ON k.id=o.id
 JOIN __SOURCE__.datum d ON d.hash=o.data_hash
 WHERE o.id BETWEEN 0 AND $2::bigint*16777216+16777215 AND (o.address=ANY($4::text[]) OR EXISTS(
  SELECT 1 FROM __SOURCE__.ma_tx_out a JOIN __SOURCE__.multi_asset m ON m.id=a.ident
  WHERE a.tx_out_id=o.id AND a.id BETWEEN 0 AND $2::bigint*68719476736+68719476735
  AND (m.policy=ANY($6::bytea[]) OR (m.policy=$7::bytea AND m.name=$8::bytea))))
),
keep_epoch_param AS MATERIALIZED (
 SELECT e.epoch_no::bigint AS id FROM __SOURCE__.epoch_param e CROSS JOIN horizon h WHERE e.epoch_no>=h.epoch-2
),
keep_epoch_stake AS MATERIALIZED (
 SELECT s.id FROM __SOURCE__.epoch_stake s CROSS JOIN horizon h WHERE s.epoch_no>=h.epoch-2
),
keep_pool_hash AS MATERIALIZED (
 SELECT DISTINCT s.pool_id AS id FROM __SOURCE__.epoch_stake s JOIN keep_epoch_stake k ON k.id=s.id
),
keep_tx_metadata AS MATERIALIZED (
 SELECT id FROM __SOURCE__.tx_metadata WHERE id BETWEEN $1::bigint*4096 AND $2::bigint*4096+4095
)
