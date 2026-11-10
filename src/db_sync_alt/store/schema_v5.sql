-- Schema 5: epoch parameters and bridge metadata for midnight-node 2.1.101, which reads
-- MAX(epoch_param.max_tx_size) of the cursor block's epoch and tx_metadata in its bridge query.
--
-- chain.epoch_nonces already holds every epoch's max_tx_size: a store created with this release
-- stores it with each epoch, and the upgrade of a nonce-only store adds it (from an authoritative
-- source) in the same transaction, before this file runs. The served table gains the column and
-- the node-facing view adds it in place, so the reader's grant on the view is kept. The serving
-- gate and the ready view require schema 5, replaced in the same transaction as the version.
ALTER TABLE serve.epoch_param ADD COLUMN max_tx_size public.word31type;
UPDATE serve.epoch_param s SET max_tx_size=c.max_tx_size FROM chain.epoch_nonces c WHERE c.epoch=s.epoch_no;
ALTER TABLE serve.epoch_param ALTER COLUMN max_tx_size SET NOT NULL;
CREATE OR REPLACE VIEW public.epoch_param AS SELECT epoch_no,nonce,max_tx_size FROM serve.epoch_param WHERE chain.gate();
-- Bridge metadata for midnight-node 2.1.101, whose bridge query LEFT JOINs tx_metadata on key 6500973
-- and decodes its json as jsonb. chain.tx_metadata exists before this file runs (created with the store,
-- or by the upgrade of a nonce-only store). The served rows are those with a metadatum (present=1);
-- their ID is the transaction's positional ID. The placeholder view is replaced in place with the same
-- columns and types (bigint, word64type, bigint, jsonb), so a grant on it is kept.
CREATE TABLE serve.tx_metadata(id bigint PRIMARY KEY,key public.word64type NOT NULL,tx_id bigint NOT NULL,json jsonb,
 CONSTRAINT tx_metadata_key CHECK(key=6500973),
 CONSTRAINT tx_metadata_position CHECK(tx_id BETWEEN 0 AND 549755813887 AND id=tx_id));
INSERT INTO serve.tx_metadata(id,key,tx_id,json)
 SELECT b.height::bigint*4096+t.tx_index,6500973,b.height::bigint*4096+t.tx_index,m.json FROM chain.tx_metadata m
 JOIN chain.transactions t ON t.hash=m.tx_hash JOIN chain.blocks b ON b.hash=t.block_hash
 WHERE m.present=1 AND m.first_generation<=(SELECT canonical_ready FROM chain.service_state WHERE id=1) ORDER BY 1;
CREATE INDEX serve_tx_metadata_tx_id ON serve.tx_metadata(tx_id);
REVOKE ALL ON serve.tx_metadata FROM PUBLIC;
CREATE TRIGGER projection_guard BEFORE INSERT OR UPDATE OR DELETE ON serve.tx_metadata FOR EACH STATEMENT EXECUTE FUNCTION chain.guard_projection();
CREATE TRIGGER no_truncate BEFORE TRUNCATE ON serve.tx_metadata EXECUTE FUNCTION chain.guard_truncate();
CREATE OR REPLACE VIEW public.tx_metadata AS SELECT id,key,tx_id,json FROM serve.tx_metadata WHERE chain.gate();
-- Phase-2-invalid transactions, stored as cardano-db-sync stores them: valid 0, served as valid_contract
-- false. A store written before this schema holds valid 1 only; its check is replaced in place.
ALTER TABLE chain.transactions DROP CONSTRAINT transactions_valid_check,
 ADD CONSTRAINT transactions_valid_check CHECK(valid IN (0,1));
-- The generations acquired with bridge metadata: verification finds them without reading every manifest.
CREATE INDEX generations_bridge_metadata ON chain.generations(id) WHERE (manifest->'capabilities'->'bridge_metadata_v1')='true'::jsonb;
CREATE OR REPLACE FUNCTION chain.gate() RETURNS boolean LANGUAGE plpgsql STABLE SECURITY DEFINER PARALLEL UNSAFE
SET search_path=pg_catalog,chain AS $$
DECLARE s chain.service_state%ROWTYPE;
BEGIN
 SELECT * INTO s FROM chain.service_state WHERE id=1;
 IF NOT FOUND OR s.owner IS DISTINCT FROM 'db-sync-alt' OR s.schema_version IS DISTINCT FROM 5 THEN
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
CREATE OR REPLACE VIEW chain.ready_generation AS SELECT g.* FROM chain.service_state s JOIN chain.generations g ON g.id=s.canonical_ready WHERE s.id=1 AND s.owner='db-sync-alt' AND s.schema_version=5 AND NOT s.revoked AND s.migration_state='ready' AND g.status='ready';
