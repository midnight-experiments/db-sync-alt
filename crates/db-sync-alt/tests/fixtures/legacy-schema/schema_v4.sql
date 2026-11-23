-- Schema 4: integrity checkpoints.
--
-- A checkpoint records, for one verified generation, the full digests of every fact family and the
-- generation's integrity root. Writing it moves the integrity base to that generation in the same
-- gated transaction. Every later verification recomputes the base's digests, compares them with the
-- checkpoint and walks only the transitions published after it. Checkpoints are never changed or
-- removed and are not readable by the node's reader.
CREATE TABLE chain.integrity_checkpoints(
  generation bigint PRIMARY KEY REFERENCES chain.generations(id),
  previous_base bigint NOT NULL REFERENCES chain.generations(id) CHECK (previous_base < generation),
  root text NOT NULL,
  digests jsonb NOT NULL,
  service_version text NOT NULL,
  checkpointed_at timestamptz NOT NULL DEFAULT now());
-- Insert only inside a publication-gated transaction: the projection-write flag, the writer lock and
-- the serving lock. Rows are never updated or deleted once the database is ready.
CREATE FUNCTION chain.guard_integrity_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM chain.service_state WHERE migration_state='ready') THEN IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF; END IF;
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'immutable_integrity_checkpoint' USING ERRCODE='23514'; END IF;
 IF current_setting('db_sync_alt.projection_write',true) IS DISTINCT FROM 'on'
    OR NOT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid() AND classid=36 AND objid=1 AND objsubid=2 AND granted)
    OR NOT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid() AND classid=36 AND objid=2 AND objsubid=2 AND granted) THEN
   RAISE EXCEPTION 'immutable_projection_or_pointer' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER checkpoint_guard BEFORE INSERT OR UPDATE OR DELETE ON chain.integrity_checkpoints FOR EACH ROW EXECUTE FUNCTION chain.guard_integrity_checkpoint();
CREATE TRIGGER no_truncate BEFORE TRUNCATE ON chain.integrity_checkpoints EXECUTE FUNCTION chain.guard_truncate();
-- The serving gate and the ready view require schema 4. The upgrade replaces both and sets the
-- schema version in one transaction: a node query sees serving schema 3 or serving schema 4.
CREATE OR REPLACE FUNCTION chain.gate() RETURNS boolean LANGUAGE plpgsql STABLE SECURITY DEFINER PARALLEL UNSAFE
SET search_path=pg_catalog,chain AS $$
DECLARE s chain.service_state%ROWTYPE;
BEGIN
 SELECT * INTO s FROM chain.service_state WHERE id=1;
 IF NOT FOUND OR s.owner IS DISTINCT FROM 'db-sync-alt' OR s.schema_version IS DISTINCT FROM 4 THEN
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
CREATE OR REPLACE VIEW chain.ready_generation AS SELECT g.* FROM chain.service_state s JOIN chain.generations g ON g.id=s.canonical_ready WHERE s.id=1 AND s.owner='db-sync-alt' AND s.schema_version=4 AND NOT s.revoked AND s.migration_state='ready' AND g.status='ready';
