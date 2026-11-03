-- Schema 3, after the first load of serve.*: indexes (db-sync's plus the node's eleven names),
-- guards and statistics. Built after the load because building an index once is faster than
-- maintaining it row by row.
CREATE UNIQUE INDEX serve_block_hash ON serve.block(hash);
CREATE INDEX idx_block_block_no ON serve.block(block_no);
CREATE INDEX serve_block_epoch_no ON serve.block(epoch_no);
CREATE INDEX serve_block_slot_no ON serve.block(slot_no);
CREATE UNIQUE INDEX serve_tx_hash ON serve.tx(hash);
CREATE INDEX idx_tx_block_id ON serve.tx(block_id);
CREATE UNIQUE INDEX serve_tx_out_tx_index ON serve.tx_out(tx_id,index);
CREATE INDEX idx_tx_out_tx_id ON serve.tx_out(tx_id);
CREATE INDEX idx_tx_out_address ON serve.tx_out USING hash(address);
CREATE INDEX idx_tx_out_data_hash ON serve.tx_out(data_hash);
CREATE INDEX idx_tx_in_tx_in_id ON serve.tx_in(tx_in_id);
CREATE INDEX idx_tx_in_tx_out_id_tx_out_index ON serve.tx_in(tx_out_id,tx_out_index);
CREATE INDEX idx_ma_tx_out_id_ident ON serve.ma_tx_out(tx_out_id,ident);
CREATE INDEX idx_ma_tx_out_ident ON serve.ma_tx_out(ident);
CREATE UNIQUE INDEX idx_multi_asset_policy_name ON serve.multi_asset(policy,name);
CREATE INDEX idx_multi_asset_policy_name_hex ON serve.multi_asset(encode(policy,'hex'),encode(name,'hex'));
CREATE UNIQUE INDEX serve_datum_hash ON serve.datum(hash);
CREATE UNIQUE INDEX serve_pool_hash_raw ON serve.pool_hash(hash_raw);
CREATE INDEX serve_epoch_stake_epoch_no ON serve.epoch_stake(epoch_no);
-- Only a publication (projection-write flag + serving lock) may change the served tables.
CREATE TRIGGER projection_guard BEFORE INSERT OR UPDATE OR DELETE ON serve.block FOR EACH STATEMENT EXECUTE FUNCTION chain.guard_projection();
CREATE TRIGGER projection_guard BEFORE INSERT OR UPDATE OR DELETE ON serve.tx FOR EACH STATEMENT EXECUTE FUNCTION chain.guard_projection();
CREATE TRIGGER projection_guard BEFORE INSERT OR UPDATE OR DELETE ON serve.tx_out FOR EACH STATEMENT EXECUTE FUNCTION chain.guard_projection();
CREATE TRIGGER projection_guard BEFORE INSERT OR UPDATE OR DELETE ON serve.tx_in FOR EACH STATEMENT EXECUTE FUNCTION chain.guard_projection();
CREATE TRIGGER projection_guard BEFORE INSERT OR UPDATE OR DELETE ON serve.ma_tx_out FOR EACH STATEMENT EXECUTE FUNCTION chain.guard_projection();
CREATE TRIGGER projection_guard BEFORE INSERT OR UPDATE OR DELETE ON serve.multi_asset FOR EACH STATEMENT EXECUTE FUNCTION chain.guard_projection();
CREATE TRIGGER projection_guard BEFORE INSERT OR UPDATE OR DELETE ON serve.datum FOR EACH STATEMENT EXECUTE FUNCTION chain.guard_projection();
CREATE TRIGGER projection_guard BEFORE INSERT OR UPDATE OR DELETE ON serve.epoch_param FOR EACH STATEMENT EXECUTE FUNCTION chain.guard_projection();
CREATE TRIGGER projection_guard BEFORE INSERT OR UPDATE OR DELETE ON serve.pool_hash FOR EACH STATEMENT EXECUTE FUNCTION chain.guard_projection();
CREATE TRIGGER projection_guard BEFORE INSERT OR UPDATE OR DELETE ON serve.epoch_stake FOR EACH STATEMENT EXECUTE FUNCTION chain.guard_projection();
CREATE TRIGGER no_truncate BEFORE TRUNCATE ON serve.block EXECUTE FUNCTION chain.guard_truncate();
CREATE TRIGGER no_truncate BEFORE TRUNCATE ON serve.tx EXECUTE FUNCTION chain.guard_truncate();
CREATE TRIGGER no_truncate BEFORE TRUNCATE ON serve.tx_out EXECUTE FUNCTION chain.guard_truncate();
CREATE TRIGGER no_truncate BEFORE TRUNCATE ON serve.tx_in EXECUTE FUNCTION chain.guard_truncate();
CREATE TRIGGER no_truncate BEFORE TRUNCATE ON serve.ma_tx_out EXECUTE FUNCTION chain.guard_truncate();
CREATE TRIGGER no_truncate BEFORE TRUNCATE ON serve.multi_asset EXECUTE FUNCTION chain.guard_truncate();
CREATE TRIGGER no_truncate BEFORE TRUNCATE ON serve.datum EXECUTE FUNCTION chain.guard_truncate();
CREATE TRIGGER no_truncate BEFORE TRUNCATE ON serve.epoch_param EXECUTE FUNCTION chain.guard_truncate();
CREATE TRIGGER no_truncate BEFORE TRUNCATE ON serve.pool_hash EXECUTE FUNCTION chain.guard_truncate();
CREATE TRIGGER no_truncate BEFORE TRUNCATE ON serve.epoch_stake EXECUTE FUNCTION chain.guard_truncate();
ANALYZE serve.block,serve.tx,serve.tx_out,serve.tx_in,serve.ma_tx_out,serve.multi_asset,serve.datum,serve.epoch_param,serve.pool_hash,serve.epoch_stake;
