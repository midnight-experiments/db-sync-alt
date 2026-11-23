-- Every view evaluates the serving gate once per statement. A restore runs
-- this again to point the views at the tables it built.
CREATE OR REPLACE VIEW public.block AS SELECT * FROM serve.block WHERE dsa.gate();
CREATE OR REPLACE VIEW public.tx AS SELECT * FROM serve.tx WHERE dsa.gate();
CREATE OR REPLACE VIEW public.tx_out AS SELECT * FROM serve.tx_out WHERE dsa.gate();
CREATE OR REPLACE VIEW public.tx_in AS SELECT * FROM serve.tx_in WHERE dsa.gate();
CREATE OR REPLACE VIEW public.ma_tx_out AS SELECT * FROM serve.ma_tx_out WHERE dsa.gate();
CREATE OR REPLACE VIEW public.multi_asset AS SELECT * FROM serve.multi_asset WHERE dsa.gate();
CREATE OR REPLACE VIEW public.datum AS SELECT * FROM serve.datum WHERE dsa.gate();
CREATE OR REPLACE VIEW public.epoch_param AS SELECT * FROM serve.epoch_param WHERE dsa.gate();
CREATE OR REPLACE VIEW public.pool_hash AS SELECT * FROM serve.pool_hash WHERE dsa.gate();
CREATE OR REPLACE VIEW public.epoch_stake AS SELECT * FROM serve.epoch_stake WHERE dsa.gate();
CREATE OR REPLACE VIEW public.tx_metadata AS SELECT * FROM serve.tx_metadata WHERE dsa.gate();
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC;
