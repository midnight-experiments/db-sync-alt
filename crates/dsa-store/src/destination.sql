-- What an existing layout holds before a load: whether it serves, the kind of
-- its origin, whether any served or state table has a row, whether the build
-- of an interrupted restore exists, and whether `serve` holds an object that
-- is not one of the eleven tables or their indexes. $1 lists the tables, $2
-- the index names.
SELECT p.serving,i.origin->>'kind',
 EXISTS(SELECT 1 FROM serve.block) OR EXISTS(SELECT 1 FROM serve.tx) OR EXISTS(SELECT 1 FROM serve.tx_out)
 OR EXISTS(SELECT 1 FROM serve.tx_in) OR EXISTS(SELECT 1 FROM serve.ma_tx_out) OR EXISTS(SELECT 1 FROM serve.multi_asset)
 OR EXISTS(SELECT 1 FROM serve.datum) OR EXISTS(SELECT 1 FROM serve.epoch_param) OR EXISTS(SELECT 1 FROM serve.pool_hash)
 OR EXISTS(SELECT 1 FROM serve.epoch_stake) OR EXISTS(SELECT 1 FROM serve.tx_metadata)
 OR EXISTS(SELECT 1 FROM dsa.supply) OR EXISTS(SELECT 1 FROM dsa.consumed_before_horizon)
 OR EXISTS(SELECT 1 FROM dsa.first_served) OR EXISTS(SELECT 1 FROM dsa.calls)
 OR EXISTS(SELECT 1 FROM dsa.service_state) OR EXISTS(SELECT 1 FROM dsa.unspent)
 OR EXISTS(SELECT 1 FROM dsa.first_observation),
 to_regnamespace('dsa_retired') IS NOT NULL,
 EXISTS(SELECT 1 FROM pg_class c WHERE c.relnamespace='serve'::regnamespace
  AND NOT (c.relkind='r' AND c.relname=ANY($1::text[])) AND NOT (c.relkind='i' AND c.relname=ANY($2::text[])))
 OR EXISTS(SELECT 1 FROM pg_proc WHERE pronamespace='serve'::regnamespace)
 OR EXISTS(SELECT 1 FROM pg_type t WHERE t.typnamespace='serve'::regnamespace AND t.typrelid=0
  AND NOT EXISTS(SELECT 1 FROM pg_type e WHERE e.oid=t.typelem AND e.typrelid<>0))
FROM dsa.position p CROSS JOIN dsa.identity i WHERE p.id=1 AND i.id=1
