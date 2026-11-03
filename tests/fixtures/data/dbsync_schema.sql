-- This file is part of midnight-node.
-- Copyright (C) Midnight Foundation
-- SPDX-License-Identifier: Apache-2.0
-- Query-compatible experimental subset, not a general db-sync replacement.
CREATE DOMAIN hash28type AS bytea CHECK(octet_length(VALUE)=28);
CREATE DOMAIN hash32type AS bytea CHECK(octet_length(VALUE)=32);
CREATE DOMAIN word31type AS integer CHECK(VALUE>=0);
CREATE DOMAIN word63type AS bigint CHECK(VALUE>=0);
CREATE DOMAIN uinteger AS integer CHECK(VALUE>=0);
CREATE DOMAIN txindex AS smallint CHECK(VALUE>=0);
CREATE DOMAIN word64type AS numeric(20,0) CHECK(VALUE>=0 AND VALUE<=18446744073709551615);
CREATE DOMAIN lovelace AS numeric(20,0) CHECK(VALUE>=0 AND VALUE<=18446744073709551615);
CREATE DOMAIN asset32type AS bytea CHECK(octet_length(VALUE)<=32);
CREATE TABLE block(id bigint PRIMARY KEY,hash hash32type UNIQUE NOT NULL,epoch_no word31type,slot_no word63type,block_no word31type UNIQUE,time timestamp NOT NULL,tx_count bigint NOT NULL);
CREATE TABLE tx(id bigint PRIMARY KEY,hash hash32type UNIQUE NOT NULL,block_id bigint REFERENCES block(id),block_index word31type,valid_contract boolean,UNIQUE(block_id,block_index));
CREATE TABLE tx_out(id bigint PRIMARY KEY,tx_id bigint REFERENCES tx(id),index txindex,address varchar NOT NULL,value lovelace NOT NULL,data_hash hash32type,consumed_by_tx_id bigint,UNIQUE(tx_id,index));
CREATE TABLE tx_in(id bigint PRIMARY KEY,tx_in_id bigint REFERENCES tx(id),tx_out_id bigint REFERENCES tx(id),tx_out_index txindex,UNIQUE(tx_in_id,tx_out_id,tx_out_index));
CREATE TABLE datum(id bigint PRIMARY KEY,hash hash32type UNIQUE NOT NULL,value jsonb);
CREATE TABLE multi_asset(id bigint PRIMARY KEY,policy hash28type NOT NULL,name asset32type NOT NULL,UNIQUE(policy,name));
CREATE TABLE ma_tx_out(id bigint PRIMARY KEY,quantity word64type NOT NULL,tx_out_id bigint REFERENCES tx_out(id),ident bigint REFERENCES multi_asset(id),UNIQUE(tx_out_id,ident));
CREATE TABLE ma_tx_mint(id bigint PRIMARY KEY,quantity numeric(20,0),tx_id bigint REFERENCES tx(id),ident bigint REFERENCES multi_asset(id));
CREATE TABLE epoch_param(epoch_no uinteger PRIMARY KEY,nonce hash32type);
CREATE TABLE pool_hash(id bigint PRIMARY KEY,hash_raw hash28type UNIQUE NOT NULL);
CREATE TABLE epoch_stake(id bigint PRIMARY KEY,pool_id bigint REFERENCES pool_hash(id),amount lovelace,epoch_no uinteger);
CREATE TABLE tx_metadata(id bigint PRIMARY KEY,key word64type,tx_id bigint REFERENCES tx(id),json jsonb);
CREATE TABLE experiment_coverage(id integer PRIMARY KEY,manifest jsonb NOT NULL);
CREATE INDEX ON tx(block_id); CREATE INDEX ON tx_out(tx_id); CREATE INDEX ON tx_in(tx_in_id); CREATE INDEX ON tx_in(tx_out_id,tx_out_index); CREATE INDEX ON ma_tx_out(tx_out_id); CREATE INDEX ON ma_tx_out(ident); CREATE INDEX ON block(slot_no);

CREATE FUNCTION experiment_assert_equal(ok boolean) RETURNS boolean LANGUAGE plpgsql AS $$ BEGIN IF NOT ok THEN RAISE EXCEPTION 'Conflicting immutable fact'; END IF; RETURN true; END $$;
