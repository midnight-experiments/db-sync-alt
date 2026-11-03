-- Schema 3: midnight-node reads this database directly with a reader role.
--
-- serve.* holds the published generation in db-sync's shape (names, base types, the node's
-- index names) with positional IDs; the public views add only the serving gate. The upgrade
-- (store/upgrade.py) fills serve.* from the current projection and creates its guards after
-- the load; publications keep it current (store/serve.py).
ALTER TABLE chain.integrity_state ADD COLUMN serve_generation bigint REFERENCES chain.generations(id);
CREATE SCHEMA serve;
REVOKE ALL ON SCHEMA serve FROM PUBLIC;
CREATE TABLE serve.block(id bigint PRIMARY KEY,hash public.hash32type NOT NULL,epoch_no public.word31type,slot_no public.word63type,block_no public.word31type NOT NULL,time timestamp NOT NULL,tx_count public.word63type NOT NULL,
 CONSTRAINT block_position CHECK(id=block_no AND id<134217728));
CREATE TABLE serve.tx(id bigint PRIMARY KEY,hash public.hash32type NOT NULL,block_id bigint NOT NULL,block_index public.word31type NOT NULL,valid_contract boolean NOT NULL,
 CONSTRAINT tx_position CHECK(block_id BETWEEN 0 AND 134217727 AND block_index BETWEEN 0 AND 4095 AND id=block_id*4096+block_index));
CREATE TABLE serve.tx_out(id bigint PRIMARY KEY,tx_id bigint NOT NULL,index public.txindex NOT NULL,address text NOT NULL,value public.lovelace NOT NULL,data_hash public.hash32type,consumed_by_tx_id bigint,
 CONSTRAINT tx_out_position CHECK(tx_id BETWEEN 0 AND 549755813887 AND index BETWEEN 0 AND 4095 AND id=tx_id*4096+index));
CREATE TABLE serve.tx_in(id bigint PRIMARY KEY,tx_in_id bigint NOT NULL,tx_out_id bigint NOT NULL,tx_out_index public.txindex NOT NULL,
 CONSTRAINT tx_in_position CHECK(tx_in_id BETWEEN 0 AND 549755813887 AND tx_out_id BETWEEN 0 AND 549755813887 AND id-tx_in_id*4096 BETWEEN 0 AND 4095));
CREATE TABLE serve.ma_tx_out(id bigint PRIMARY KEY,quantity public.word64type NOT NULL,tx_out_id bigint NOT NULL,ident bigint NOT NULL,
 CONSTRAINT ma_tx_out_position CHECK(tx_out_id BETWEEN 0 AND 2251799813685247 AND id-tx_out_id*4096 BETWEEN 0 AND 4095));
CREATE TABLE serve.multi_asset(id bigint PRIMARY KEY,policy public.hash28type NOT NULL,name public.asset32type NOT NULL);
CREATE TABLE serve.datum(id bigint PRIMARY KEY,hash public.hash32type NOT NULL,value jsonb NOT NULL);
CREATE TABLE serve.epoch_param(epoch_no public.word31type PRIMARY KEY,nonce public.hash32type NOT NULL);
CREATE TABLE serve.pool_hash(id bigint PRIMARY KEY,hash_raw public.hash28type NOT NULL);
CREATE TABLE serve.epoch_stake(id bigint PRIMARY KEY,pool_id bigint NOT NULL,amount public.lovelace NOT NULL,epoch_no public.word31type NOT NULL);
REVOKE ALL ON ALL TABLES IN SCHEMA serve FROM PUBLIC;
-- The node checks these eleven index names in pg_indexes; each must resolve to exactly one row,
-- so the stored facts' indexes of the same names are renamed.
ALTER INDEX chain.idx_block_block_no RENAME TO chain_idx_block_block_no;
ALTER INDEX chain.idx_tx_block_id RENAME TO chain_idx_tx_block_id;
ALTER INDEX chain.idx_tx_out_tx_id RENAME TO chain_idx_tx_out_tx_id;
ALTER INDEX chain.idx_tx_out_data_hash RENAME TO chain_idx_tx_out_data_hash;
ALTER INDEX chain.idx_tx_out_address RENAME TO chain_idx_tx_out_address;
ALTER INDEX chain.idx_tx_in_tx_in_id RENAME TO chain_idx_tx_in_tx_in_id;
ALTER INDEX chain.idx_tx_in_tx_out_id_tx_out_index RENAME TO chain_idx_tx_in_tx_out_id_tx_out_index;
ALTER INDEX chain.idx_multi_asset_policy_name RENAME TO chain_idx_multi_asset_policy_name;
ALTER INDEX chain.idx_multi_asset_policy_name_hex RENAME TO chain_idx_multi_asset_policy_name_hex;
ALTER INDEX chain.idx_ma_tx_out_ident RENAME TO chain_idx_ma_tx_out_ident;
ALTER INDEX chain.idx_ma_tx_out_id_ident RENAME TO chain_idx_ma_tx_out_id_ident;
-- chain.gate() is the only serving check: every node-facing view ends in a call to it, so a
-- query raises SQLSTATE 55000 instead of answering whenever the database does not serve a ready
-- canonical generation whose projection and served tables are current. The call has no column
-- references, so the planner evaluates it once per execution as a one-time filter before any
-- scan, also in generic plans cached while the database served.
CREATE FUNCTION chain.gate() RETURNS boolean LANGUAGE plpgsql STABLE SECURITY DEFINER PARALLEL UNSAFE
SET search_path=pg_catalog,chain AS $$
DECLARE s chain.service_state%ROWTYPE;
BEGIN
 SELECT * INTO s FROM chain.service_state WHERE id=1;
 IF NOT FOUND OR s.owner IS DISTINCT FROM 'db-sync-alt' OR s.schema_version IS DISTINCT FROM 3 THEN
   RAISE EXCEPTION 'db_sync_alt_not_serving' USING ERRCODE='55000',DETAIL='identity';
 END IF;
 IF s.revoked THEN RAISE EXCEPTION 'db_sync_alt_not_serving' USING ERRCODE='55000',DETAIL='revoked'; END IF;
 IF s.migration_state<>'ready' OR s.canonical_ready IS NULL
    OR NOT EXISTS(SELECT 1 FROM chain.generations g WHERE g.id=s.canonical_ready AND g.status='ready') THEN
   RAISE EXCEPTION 'db_sync_alt_not_serving' USING ERRCODE='55000',DETAIL='unready';
 END IF;
 IF NOT EXISTS(SELECT 1 FROM chain.integrity_state i WHERE i.id=1 AND i.projection_generation=s.canonical_ready AND i.serve_generation=s.canonical_ready) THEN
   RAISE EXCEPTION 'db_sync_alt_not_serving' USING ERRCODE='55000',DETAIL='projection';
 END IF;
 RETURN true;
END $$;
REVOKE EXECUTE ON FUNCTION chain.gate() FROM PUBLIC;
CREATE OR REPLACE VIEW chain.ready_generation AS SELECT g.* FROM chain.service_state s JOIN chain.generations g ON g.id=s.canonical_ready WHERE s.id=1 AND s.owner='db-sync-alt' AND s.schema_version=3 AND NOT s.revoked AND s.migration_state='ready' AND g.status='ready';
-- Node-facing relations: the served tables, each gated. block_parent and ma_tx_mint are not read
-- by the node and are removed so public holds only node-facing relations (and tx_metadata,
-- which is never granted).
DROP VIEW public.block_parent,public.ma_tx_mint,public.tx,public.tx_out,public.tx_in,public.datum,public.multi_asset,public.ma_tx_out,public.epoch_param,public.pool_hash,public.epoch_stake,public.block;
CREATE VIEW public.block AS SELECT id,hash,epoch_no,slot_no,block_no,time,tx_count FROM serve.block WHERE chain.gate();
CREATE VIEW public.tx AS SELECT id,hash,block_id,block_index,valid_contract FROM serve.tx WHERE chain.gate();
CREATE VIEW public.tx_out AS SELECT id,tx_id,index,address,value,data_hash,consumed_by_tx_id FROM serve.tx_out WHERE chain.gate();
CREATE VIEW public.tx_in AS SELECT id,tx_in_id,tx_out_id,tx_out_index FROM serve.tx_in WHERE chain.gate();
CREATE VIEW public.ma_tx_out AS SELECT id,quantity,tx_out_id,ident FROM serve.ma_tx_out WHERE chain.gate();
CREATE VIEW public.multi_asset AS SELECT id,policy,name FROM serve.multi_asset WHERE chain.gate();
CREATE VIEW public.datum AS SELECT id,hash,value FROM serve.datum WHERE chain.gate();
CREATE VIEW public.epoch_param AS SELECT epoch_no,nonce FROM serve.epoch_param WHERE chain.gate();
CREATE VIEW public.pool_hash AS SELECT id,hash_raw FROM serve.pool_hash WHERE chain.gate();
CREATE VIEW public.epoch_stake AS SELECT id,pool_id,amount,epoch_no FROM serve.epoch_stake WHERE chain.gate();
