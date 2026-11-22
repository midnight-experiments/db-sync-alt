-- Explicit schema2 upgrade. Applied only after a fully verified schema-1 base.
CREATE TABLE chain.integrity_state(id integer PRIMARY KEY CHECK(id=1),base_generation bigint NOT NULL REFERENCES chain.generations(id),revision bigint NOT NULL DEFAULT 0,projection_generation bigint REFERENCES chain.generations(id));
-- The upgrade loads the verified base, then adds/validates this table's FK
-- within the same transaction before schema2 or any guards become visible.
CREATE TABLE chain.current_membership(height public.word31type PRIMARY KEY,hash public.hash32type NOT NULL);
CREATE INDEX current_membership_hash ON chain.current_membership(hash);
CREATE TABLE chain.generation_reuse(generation bigint PRIMARY KEY REFERENCES chain.generations(id),through_height public.word31type NOT NULL,receipt jsonb NOT NULL);
CREATE TABLE chain.membership_delta(generation bigint REFERENCES chain.generations(id),height public.word31type,hash public.hash32type NOT NULL REFERENCES chain.blocks(hash),PRIMARY KEY(generation,height));
CREATE INDEX checkpoint_phase_unit_generation ON chain.checkpoints(phase,unit,generation);
CREATE FUNCTION chain.guard_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE g bigint; rowstatus text; sealed boolean;
BEGIN
 IF NOT EXISTS(SELECT 1 FROM chain.service_state WHERE migration_state='ready') THEN
   IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
 END IF;
 IF TG_TABLE_NAME='metadata' THEN
   IF TG_OP='INSERT' AND (NEW.key='sql_completion_required_from' OR NEW.key ~ '^poll-budget-policy:[0-9]{4}-[0-9]{2}-[0-9]{2}$') THEN RETURN NEW; END IF;
   RAISE EXCEPTION 'immutable_metadata' USING ERRCODE='23514';
 ELSIF TG_TABLE_NAME='pools' THEN
   IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'immutable_pool' USING ERRCODE='23514'; END IF;
 ELSIF TG_TABLE_NAME='generations' THEN
   IF TG_OP='DELETE' OR (TG_OP='UPDATE' AND OLD.status<>'pending') THEN RAISE EXCEPTION 'immutable_generation' USING ERRCODE='23514'; END IF;
   IF TG_OP='UPDATE' AND (NEW.id<>OLD.id OR NEW.parent IS DISTINCT FROM OLD.parent OR NEW.source<>OLD.source OR NEW.start_height<>OLD.start_height OR NEW.start_index<>OLD.start_index OR NEW.target_height<>OLD.target_height OR NEW.witness_height<>OLD.witness_height) THEN RAISE EXCEPTION 'immutable_generation_identity' USING ERRCODE='23514'; END IF;
   IF TG_OP='UPDATE' AND OLD.manifest->'integrity'->>'version'='2' AND (NEW.manifest->'integrity'->>'version' IS DISTINCT FROM '2' OR NEW.manifest->'parent_ref' IS DISTINCT FROM OLD.manifest->'parent_ref') THEN RAISE EXCEPTION 'immutable_proof_contract' USING ERRCODE='23514'; END IF;
   IF TG_OP='UPDATE' AND OLD.manifest ? 'completion' AND NEW.manifest IS DISTINCT FROM OLD.manifest THEN RAISE EXCEPTION 'immutable_seal' USING ERRCODE='23514'; END IF;
 ELSE
   IF TG_TABLE_NAME IN ('generation_blocks','membership_delta','generation_reuse','checkpoints','provenance') THEN g=COALESCE(NEW.generation,OLD.generation); ELSE g=COALESCE(NEW.first_generation,OLD.first_generation); END IF;
   SELECT status,manifest ? 'completion' INTO rowstatus,sealed FROM chain.generations WHERE id=g;
   IF rowstatus IS DISTINCT FROM 'pending' THEN RAISE EXCEPTION 'immutable_fact' USING ERRCODE='23514'; END IF;
   IF TG_TABLE_NAME='checkpoints' THEN
     IF NEW.phase='completion-revalidation' AND NEW.unit='latest' AND TG_OP IN ('INSERT','UPDATE') THEN RETURN NEW; END IF;
   END IF;
   IF sealed THEN RAISE EXCEPTION 'immutable_sealed_pending' USING ERRCODE='23514'; END IF;
   IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'immutable_fact' USING ERRCODE='23514'; END IF;
 END IF;
 UPDATE chain.integrity_state SET revision=revision+1 WHERE id=1;
 RETURN NEW;
END $$;
CREATE OR REPLACE FUNCTION chain.membership(generation_id bigint) RETURNS TABLE(height integer,hash bytea) LANGUAGE sql STABLE AS $$
 WITH RECURSIVE lineage(id,ceiling) AS (
 SELECT id,witness_height FROM chain.generations WHERE id=generation_id
 UNION ALL SELECT p.id,LEAST(l.ceiling,COALESCE(r.through_height,p.target_height)) FROM lineage l JOIN chain.generations c ON c.id=l.id JOIN chain.generations p ON p.id=c.parent LEFT JOIN chain.generation_reuse r ON r.generation=c.id
 ), bindings AS (SELECT generation,height,hash FROM chain.generation_blocks UNION ALL SELECT generation,height,hash FROM chain.membership_delta)
 SELECT b.height::integer,b.hash::bytea FROM lineage l JOIN bindings b ON b.generation=l.id AND b.height<=l.ceiling
$$;
CREATE OR REPLACE VIEW chain.ready_generation AS SELECT g.* FROM chain.service_state s JOIN chain.generations g ON g.id=s.canonical_ready WHERE s.id=1 AND s.owner='db-sync-alt' AND s.schema_version=2 AND NOT s.revoked AND s.migration_state='ready' AND g.status='ready';
CREATE OR REPLACE VIEW public.block AS SELECT b.id,b.hash,b.epoch AS epoch_no,b.slot AS slot_no,b.height AS block_no,b.time,b.tx_count FROM chain.ready_generation g JOIN chain.integrity_state i ON i.projection_generation=g.id JOIN chain.current_membership m ON true JOIN chain.blocks b ON b.hash=m.hash;
-- Accidental direct owner DML cannot change the served index/pointer. The
-- transaction-local gate is internal to the checked canonical publication API;
-- deliberately disabling triggers/forging that API as a superuser is out of scope.
CREATE FUNCTION chain.guard_projection() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM chain.service_state WHERE migration_state='ready') THEN IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF; END IF;
 IF TG_TABLE_NAME='integrity_state' THEN
   IF pg_trigger_depth()>1 AND TG_OP='UPDATE' AND NEW.base_generation=OLD.base_generation AND NEW.projection_generation IS NOT DISTINCT FROM OLD.projection_generation AND NEW.revision=OLD.revision+1 THEN RETURN NEW; END IF;
 END IF;
 IF TG_TABLE_NAME='service_state' THEN
   IF TG_OP='UPDATE' AND NEW.revoked AND NEW.canonical_ready IS NULL AND NEW.publication_serial=OLD.publication_serial+1 AND NEW.schema_version=OLD.schema_version AND (to_jsonb(NEW)-'revoked'-'canonical_ready'-'publication_serial')=(to_jsonb(OLD)-'revoked'-'canonical_ready'-'publication_serial') THEN RETURN NEW; END IF;
 END IF;
 IF current_setting('db_sync_alt.projection_write',true) IS DISTINCT FROM 'on' OR NOT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid() AND classid=36 AND objid=1 AND objsubid=2 AND granted) THEN RAISE EXCEPTION 'immutable_projection_or_pointer' USING ERRCODE='23514'; END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
END $$;

CREATE FUNCTION chain.guard_truncate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'immutable_storage' USING ERRCODE='23514'; END $$;
