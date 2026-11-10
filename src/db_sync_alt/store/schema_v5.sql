-- Schema 5: epoch parameters for midnight-node 2.1.101, which reads
-- MAX(epoch_param.max_tx_size) of the cursor block's epoch.
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
