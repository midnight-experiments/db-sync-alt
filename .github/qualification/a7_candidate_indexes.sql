-- Experiment only: destination-private lookup indexes, no served DDL change.
-- Existing raw id/epoch_no primary keys remain. Not applied to product yet.
CREATE INDEX conversion_idx_tx_out_address ON pg_temp.tx_out(address);
CREATE INDEX conversion_idx_tx_in_outpoint ON pg_temp.tx_in(tx_out_id,tx_out_index);
CREATE INDEX conversion_idx_ma_tx_out_output_unit ON pg_temp.ma_tx_out(tx_out_id,ident);
CREATE INDEX conversion_idx_ma_tx_out_ident ON pg_temp.ma_tx_out(ident);
CREATE UNIQUE INDEX conversion_multi_asset_unit ON pg_temp.multi_asset(policy,name);
CREATE UNIQUE INDEX conversion_datum_hash ON pg_temp.datum(hash);
CREATE INDEX conversion_epoch_stake_epoch_no ON pg_temp.epoch_stake(epoch_no);
