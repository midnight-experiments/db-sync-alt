CREATE SCHEMA IF NOT EXISTS dsa_follower;
REVOKE ALL ON SCHEMA dsa_follower FROM PUBLIC;
CREATE TABLE IF NOT EXISTS dsa_follower.first_witness(
 shape text PRIMARY KEY CHECK(shape IN ('stored_pointer_address','stored_large_datum','stored_bridge_metadata')),
 network text NOT NULL,
 observation jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS dsa_follower.raw_response(
 sha256 text PRIMARY KEY CHECK(sha256 ~ '^[0-9a-f]{64}$'),
 length bigint NOT NULL CHECK(length>=0 AND length<=134217728)
);
CREATE TABLE IF NOT EXISTS dsa_follower.raw_chunk(
 sha256 text NOT NULL REFERENCES dsa_follower.raw_response(sha256),
 ordinal integer NOT NULL CHECK(ordinal>=0),
 body bytea NOT NULL CHECK(octet_length(body) BETWEEN 1 AND 65536),
 PRIMARY KEY(sha256,ordinal)
);
REVOKE ALL ON ALL TABLES IN SCHEMA dsa_follower FROM PUBLIC;
