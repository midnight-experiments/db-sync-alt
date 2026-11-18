-- Node-facing relations and positional constraints.
REVOKE ALL ON SCHEMA public FROM PUBLIC;
CREATE SCHEMA serve;
CREATE SCHEMA dsa;
REVOKE ALL ON SCHEMA serve,dsa FROM PUBLIC;
CREATE DOMAIN public.hash28type AS bytea CHECK(octet_length(VALUE)=28);
CREATE DOMAIN public.hash32type AS bytea CHECK(octet_length(VALUE)=32);
CREATE DOMAIN public.word31type AS integer CHECK(VALUE>=0);
CREATE DOMAIN public.word63type AS bigint CHECK(VALUE>=0);
CREATE DOMAIN public.uinteger AS integer CHECK(VALUE>=0);
CREATE DOMAIN public.txindex AS smallint CHECK(VALUE>=0);
CREATE DOMAIN public.word64type AS numeric CHECK(VALUE=trunc(VALUE) AND VALUE>=0 AND VALUE<=18446744073709551615);
CREATE DOMAIN public.lovelace AS numeric CHECK(VALUE=trunc(VALUE) AND VALUE>=0 AND VALUE<=18446744073709551615);
CREATE DOMAIN public.asset32type AS bytea CHECK(octet_length(VALUE)<=32);
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
CREATE TABLE serve.datum(id bigint PRIMARY KEY,hash public.hash32type NOT NULL,value jsonb);
CREATE TABLE serve.epoch_param(epoch_no public.word31type PRIMARY KEY,nonce public.hash32type NOT NULL,max_tx_size public.word31type NOT NULL);
CREATE TABLE serve.pool_hash(id bigint PRIMARY KEY,hash_raw public.hash28type NOT NULL);
CREATE TABLE serve.epoch_stake(id bigint PRIMARY KEY,pool_id bigint NOT NULL,amount public.lovelace NOT NULL,epoch_no public.word31type NOT NULL);
CREATE TABLE serve.tx_metadata(id bigint PRIMARY KEY,key public.word64type NOT NULL,tx_id bigint NOT NULL,json jsonb,
 CONSTRAINT tx_metadata_key CHECK(key=6500973),
 CONSTRAINT tx_metadata_position CHECK(tx_id BETWEEN 0 AND 549755813887 AND id=tx_id));
ALTER TABLE serve.block SET (autovacuum_analyze_scale_factor=0.01, autovacuum_vacuum_scale_factor=0.05);
ALTER TABLE serve.tx SET (autovacuum_analyze_scale_factor=0.01, autovacuum_vacuum_scale_factor=0.05);
ALTER TABLE serve.tx_out SET (autovacuum_analyze_scale_factor=0.01, autovacuum_vacuum_scale_factor=0.05);
ALTER TABLE serve.tx_in SET (autovacuum_analyze_scale_factor=0.01, autovacuum_vacuum_scale_factor=0.05);
ALTER TABLE serve.ma_tx_out SET (autovacuum_analyze_scale_factor=0.01, autovacuum_vacuum_scale_factor=0.05);
ALTER TABLE serve.multi_asset SET (autovacuum_analyze_scale_factor=0.01, autovacuum_vacuum_scale_factor=0.05);
ALTER TABLE serve.datum SET (autovacuum_analyze_scale_factor=0.01, autovacuum_vacuum_scale_factor=0.05);
ALTER TABLE serve.epoch_param SET (autovacuum_analyze_scale_factor=0.01, autovacuum_vacuum_scale_factor=0.05);
ALTER TABLE serve.pool_hash SET (autovacuum_analyze_scale_factor=0.01, autovacuum_vacuum_scale_factor=0.05);
ALTER TABLE serve.epoch_stake SET (autovacuum_analyze_scale_factor=0.01, autovacuum_vacuum_scale_factor=0.05);
ALTER TABLE serve.tx_metadata SET (autovacuum_analyze_scale_factor=0.01, autovacuum_vacuum_scale_factor=0.05);
REVOKE ALL ON ALL TABLES IN SCHEMA serve FROM PUBLIC;
