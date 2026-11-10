-- Narrow ready-generation metadata gate; installed only by the checked anchor API.
CREATE OR REPLACE FUNCTION chain.guard_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
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
   IF TG_TABLE_NAME='checkpoints' THEN
    IF NEW.phase='relevance' AND NEW.unit='anchor' THEN
     IF TG_OP<>'INSERT' OR rowstatus IS DISTINCT FROM 'ready'
        OR NOT EXISTS(SELECT 1 FROM chain.service_state WHERE id=1 AND schema_version=5 AND migration_state='ready' AND NOT revoked AND canonical_ready=g)
        OR EXISTS(SELECT 1 FROM chain.generations WHERE status='pending')
        OR current_setting('db_sync_alt.projection_write',true) IS DISTINCT FROM 'on'
        OR current_setting('db_sync_alt.relevance_anchor_write',true) IS NULL
        OR current_setting('db_sync_alt.relevance_anchor_write',true)::jsonb IS DISTINCT FROM NEW.receipt
        OR NOT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid() AND classid=36 AND objid=1 AND objsubid=2 AND granted)
        OR NOT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid() AND classid=36 AND objid=2 AND objsubid=2 AND granted) THEN
       RAISE EXCEPTION 'immutable_relevance_anchor' USING ERRCODE='23514';
     END IF;
     UPDATE chain.integrity_state SET revision=revision+1 WHERE id=1;
     RETURN NEW;
    END IF;
   END IF;
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
