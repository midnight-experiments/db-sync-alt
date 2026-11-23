-- Database identity and the mutable serving position.
CREATE TABLE dsa.identity (
 id smallint PRIMARY KEY CHECK(id=1),
 database_format integer NOT NULL,
 network text NOT NULL,
 profile_hash text NOT NULL CHECK(length(profile_hash)=64),
 keep_rule_version integer NOT NULL,
 tool_version text NOT NULL,
 node_release text NOT NULL,
 origin jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE dsa.position (
 id smallint PRIMARY KEY CHECK(id=1),
 serving boolean NOT NULL DEFAULT false,
 reason text NOT NULL DEFAULT 'empty',
 target_height public.word31type, target_hash public.hash32type, target_time timestamptz,
 tip_height public.word31type, tip_hash public.hash32type, tip_time timestamptz,
 horizon_height public.word31type, horizon_hash public.hash32type, horizon_time timestamptz,
 retention_seconds bigint NOT NULL DEFAULT 604800 CHECK(retention_seconds>=86400),
 updated_at timestamptz NOT NULL DEFAULT now(),
 CHECK((target_height IS NULL AND target_hash IS NULL AND target_time IS NULL) OR
       (target_height IS NOT NULL AND target_hash IS NOT NULL AND target_time IS NOT NULL)),
 CHECK((tip_height IS NULL AND tip_hash IS NULL AND tip_time IS NULL) OR
       (tip_height IS NOT NULL AND tip_hash IS NOT NULL AND tip_time IS NOT NULL)),
 CHECK((horizon_height IS NULL AND horizon_hash IS NULL AND horizon_time IS NULL) OR
       (horizon_height IS NOT NULL AND horizon_hash IS NOT NULL AND horizon_time IS NOT NULL)),
 CHECK(NOT serving OR (target_height IS NOT NULL AND tip_height IS NOT NULL AND horizon_height IS NOT NULL AND tip_height>=target_height AND horizon_height<=target_height))
);
CREATE TABLE dsa.first_served (
 height public.word31type PRIMARY KEY,
 first_served_at timestamptz NOT NULL
);
CREATE TABLE dsa.supply (
 unit text PRIMARY KEY,
 quantity public.word64type NOT NULL
);
CREATE TABLE dsa.calls (
 day date PRIMARY KEY,
 requests bigint NOT NULL CHECK(requests>=0)
);
-- Only exceptional retained old outputs need this bit after their spend expires.
-- Public consumed_by_tx_id remains NULL; presence means consumed before H.
CREATE TABLE dsa.consumed_before_horizon (
 tx_out_id bigint PRIMARY KEY CHECK(tx_out_id>=0)
);
-- Served identity columns deliberately retain the legacy no-default shape.
-- Writers allocate identities explicitly using these private sequences.
CREATE SEQUENCE dsa.multi_asset_id_seq MINVALUE 1;
CREATE SEQUENCE dsa.datum_id_seq MINVALUE 1;
CREATE SEQUENCE dsa.pool_hash_id_seq MINVALUE 1;
CREATE SEQUENCE dsa.epoch_stake_id_seq MINVALUE 1;
-- The follower's state, outside the snapshot file and both digests: the
-- service's state with its last update and last error (one row, written at
-- publication), the unspent outputs at each profile address, and the first
-- stored observation of each rare shape with the provider answers that hold it.
CREATE TABLE dsa.service_state (
 id smallint PRIMARY KEY CHECK(id=1),
 state text NOT NULL,
 reason text NOT NULL,
 detail jsonb NOT NULL DEFAULT '{}'::jsonb,
 last_update jsonb,
 last_error jsonb,
 updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE dsa.unspent (
 address text PRIMARY KEY,
 outputs bigint NOT NULL CHECK(outputs>=0)
);
CREATE TABLE dsa.first_observation (
 shape text PRIMARY KEY,
 block_height public.word31type NOT NULL,
 transaction_hash public.hash32type,
 output_index public.txindex,
 datum_hash public.hash32type,
 raw_datum_bytes integer CHECK(raw_datum_bytes>=0),
 sources text[] NOT NULL,
 answers bytea[] NOT NULL CHECK(cardinality(answers)=cardinality(sources)),
 observed_at timestamptz NOT NULL DEFAULT now()
);
REVOKE ALL ON ALL TABLES IN SCHEMA dsa FROM PUBLIC;
CREATE FUNCTION dsa.gate() RETURNS boolean LANGUAGE plpgsql STABLE SECURITY DEFINER PARALLEL UNSAFE
SET search_path=pg_catalog,dsa AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM dsa.identity WHERE id=1 AND database_format=1 AND keep_rule_version=__KEEP_RULE_VERSION__) THEN
   RAISE EXCEPTION 'db_sync_alt_not_serving' USING ERRCODE='55000',DETAIL='identity';
 END IF;
 IF NOT EXISTS(SELECT 1 FROM dsa.position WHERE id=1 AND serving) THEN
   RAISE EXCEPTION 'db_sync_alt_not_serving' USING ERRCODE='55000',DETAIL='unready';
 END IF;
 RETURN true;
END $$;
COMMENT ON FUNCTION dsa.gate() IS 'db-sync-alt keep-rule-version=__KEEP_RULE_VERSION__';
REVOKE EXECUTE ON FUNCTION dsa.gate() FROM PUBLIC;
