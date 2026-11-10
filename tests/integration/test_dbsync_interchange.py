"""SQL and provider interchange preserve facts, ordinals and epoch transitions."""
import copy
import json
import os
from pathlib import Path
import tempfile
import unittest

import psycopg
from psycopg.types.json import Jsonb

from db_sync_alt.contract import digest
from db_sync_alt.sources.dbsync.acquire import CountedSQLSource, acquire
from db_sync_alt.store.database import PgStore
from db_sync_alt.verify.store import verify_store
from integration.test_dbsync_acquire import ATTEST

ROOT = Path(__file__).resolve().parents[2]

# The db-sync source below is built from a fixture store by test code; each
# acquisition itself runs only the service's PostgreSQL code.
class InterchangeTests(unittest.TestCase):
    incremental = False
    @classmethod
    def setUpClass(cls):
        import fixtures.database as integration
        cls.integration=integration;integration.SyncFixture.setUpClass()
    @classmethod
    def tearDownClass(cls):cls.integration.SyncFixture.tearDownClass()
    def test_sql_blockfrost_sql_epoch_transition_and_old_namespace_ids(self):
        it=self.integration;fixture=it.SyncFixture();fixture.setUp()
        old=os.environ.get('PGSERVICEFILE');tmp=tempfile.TemporaryDirectory()
        try:
            seed=fixture.make(transition=True);routes,_=it.gen_fixture.fixtures(transition=True)
            with PgStore(conninfo=seed) as prepared:
                fixture.run_driver(prepared,routes)
                source_dsn=fixture.database();dest=fixture.database()
                with psycopg.connect(source_dsn,autocommit=True) as db:
                    db.execute((ROOT/'tests/fixtures/data/dbsync_schema.sql').read_text())
                    db.execute("ALTER TABLE block ADD COLUMN previous_id bigint;ALTER TABLE tx_out ADD COLUMN inline_datum_id bigint;CREATE TABLE schema_version(stage_one bigint NOT NULL,stage_two bigint NOT NULL,stage_three bigint NOT NULL);INSERT INTO schema_version VALUES(15,48,6);CREATE TABLE meta(network_name varchar NOT NULL);INSERT INTO meta VALUES('preprod');CREATE TABLE epoch_stake_progress(epoch_no word31type NOT NULL,completed boolean NOT NULL)")
                    # Distinct source IDs demonstrate namespace independence.
                    blocks=list(prepared.rows('blocks',1));block_ids={r['hash']:r['height']+123 for r in blocks}
                    original_blocks=copy.deepcopy(blocks)
                    # Full db-sync retains predecessors of sparse selected headers.
                    missing={r['parent_hash'] for r in blocks if r['parent_hash'] and r['parent_hash'] not in block_ids}
                    self.assertEqual(missing,{bytes.fromhex(it.gen_fixture.block(7)['hash'])})
                    predecessor=it.gen_fixture.block_row(it.gen_fixture.block(7,transition=True))
                    blocks.append(predecessor);block_ids[predecessor['hash']]=predecessor['height']+123
                    txs=list(prepared.rows('transactions',1));tx_ids={r['hash']:i+77 for i,r in enumerate(txs)}
                    assets=list(prepared.rows('assets',1));asset_ids={(r['policy'],r['name']):i+500 for i,r in enumerate(assets)}
                    outs=list(prepared.rows('outputs',1));out_ids={(r['tx_hash'],r['output_index']):i+800 for i,r in enumerate(outs)}
                    with db.transaction():
                        for r in blocks:db.execute('INSERT INTO block(id,hash,epoch_no,slot_no,block_no,time,tx_count,previous_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)',(block_ids[r['hash']],r['hash'],r['epoch'],r['slot'],r['height'],r['time'],r['tx_count'],block_ids.get(r['parent_hash'])))
                        for r in txs:db.execute('INSERT INTO tx VALUES(%s,%s,%s,%s,true)',(tx_ids[r['hash']],r['hash'],block_ids[r['block_hash']],r['tx_index']))
                        for i,r in enumerate(prepared.rows('datums',1)):db.execute('INSERT INTO datum VALUES(%s,%s,%s)',(i+1,r['hash'],Jsonb(json.loads(r['value']))))
                        for r in outs:db.execute('INSERT INTO tx_out(id,tx_id,index,address,value,data_hash) VALUES(%s,%s,%s,%s,%s,%s)',(out_ids[(r['tx_hash'],r['output_index'])],tx_ids[r['tx_hash']],r['output_index'],r['address'],r['value'],r['datum_hash']))
                        for r in assets:db.execute('INSERT INTO multi_asset VALUES(%s,%s,%s)',(asset_ids[(r['policy'],r['name'])],r['policy'],r['name']))
                        for i,r in enumerate(prepared.rows('output_assets',1)):db.execute('INSERT INTO ma_tx_out VALUES(%s,%s,%s,%s)',(i+1,r['quantity'],out_ids[(r['tx_hash'],r['output_index'])],asset_ids[(r['policy'],r['name'])]))
                        for i,r in enumerate(prepared.rows('inputs',1)):db.execute('INSERT INTO tx_in VALUES(%s,%s,%s,%s)',(i+1,tx_ids[r['tx_hash']],tx_ids[r['origin_hash']],r['origin_index']))
                        for i,r in enumerate(prepared.rows('mints',1)):db.execute('INSERT INTO ma_tx_mint VALUES(%s,%s,%s,%s)',(i+1,r['quantity'],tx_ids[r['tx_hash']],asset_ids[(r['policy'],r['name'])]))
                        pools={r['pool_hash'] for r in prepared.rows('epoch_stakes',1)};pool_ids={p:i+1 for i,p in enumerate(sorted(pools))}
                        for pool,pid in pool_ids.items():db.execute('INSERT INTO pool_hash VALUES(%s,%s)',(pid,pool))
                        for r in prepared.rows('epoch_nonces',1):db.execute('INSERT INTO epoch_param VALUES(%s,%s,%s)',(r['epoch'],r['nonce'],r['max_tx_size']));db.execute('INSERT INTO epoch_stake_progress VALUES(%s,true)',(r['epoch'],))
                        for i,r in enumerate(prepared.rows('epoch_stakes',1)):db.execute('INSERT INTO epoch_stake VALUES(%s,%s,%s,%s)',(i+1,pool_ids[r['pool_hash']],r['amount'],r['epoch']))
                expected=prepared.digests(1)
            cfg=Path(tmp.name)/'pg_service.conf';cfg.write_text('[source]\n'+''.join(k+'='+v+'\n' for k,v in psycopg.conninfo.conninfo_to_dict(source_dsn).items()));os.environ['PGSERVICEFILE']=str(cfg)
            source=lambda:CountedSQLSource('source',fixture.profile,ATTEST)
            from db_sync_alt.cli.acquire import blockfrost_once
            check=source()
            with check.snapshot():
                observed={r['hash']:r for r in check.blocks(min(r['height'] for r in original_blocks),max(r['height'] for r in original_blocks))}
            self.assertEqual({r['hash']:r for r in original_blocks},observed)
            with PgStore(conninfo=dest,create=True,network='preprod') as store:
                acquire(source(),store,fixture.profile,target_height=it.gen_fixture.OFFSET+10)
                self.assertEqual(store.generation()['manifest']['acquisition']['version'],2)
                self.assertEqual(store.generation()['manifest']['acquisition']['window_blocks'],1000)
                if self.incremental:
                    from db_sync_alt.store.upgrade import upgrade_store
                    self.assertEqual(upgrade_store(store)['schema_version'],5)
                    self.assertTrue(store.supports_checkpoints)
                    store.certify_ready_full()
                old_ids=store.db.execute('SELECT hash,id FROM chain.transactions ORDER BY id').fetchall();old_digest=store.digests(0)
                # Too-high fixed target must not create lower acquisition job.
                with self.assertRaisesRegex(Exception,'unconfirmed_target'):
                    blockfrost_once(store,fixture.profile,target_height=it.gen_fixture.OFFSET+13,client=it.AccountedFake(routes))
                self.assertIsNone(store.sync_job('provider-active'));self.assertIsNone(store.pending());self.assertEqual(store.meta('canonical_ready'),0)
                blockfrost_once(store,fixture.profile,target_height=it.gen_fixture.OFFSET+11,client=it.AccountedFake(routes))
                self.assertEqual(store.generation()['source'],'blockfrost-cardano')
                if self.incremental:
                    # Released 0.2 uses a compact profile digest for provider
                    # deltas; this is separate from SQL acquisition receipt v2.
                    from db_sync_alt.store.incremental import profile_digest,KIND_V2
                    child=store.generation()
                    self.assertEqual(child['manifest']['integrity']['kind'],KIND_V2)
                    self.assertNotIn('identity',child['manifest'])
                    self.assertEqual(profile_digest(child),digest(fixture.profile.identity))
                    self.assertEqual(store.write_checkpoint(store.certify_checkpoint()),
                                     {'generation':1,'previous_base':0})
                    verify_store(store)
                    # Bridge metadata coverage starts with the Blockfrost update.
                    self.assertEqual(store.status()['bridge']['coverage_start'],it.gen_fixture.OFFSET+11)
                caught_up=acquire(source(),store,fixture.profile,target_height=it.gen_fixture.OFFSET+12)
                self.assertEqual(store.digests(2),expected);self.assertEqual(store.digests(0),old_digest)
                self.assertEqual(store.db.execute('SELECT hash,id FROM chain.transactions WHERE first_generation=0 ORDER BY id').fetchall(),old_ids)
                self.assertEqual(store.generation()['manifest']['required_epochs'],[0,1,2,3])
                self.assertTrue(any(r['receipt'].get('epoch_end',{}).get('height')==it.gen_fixture.OFFSET+11 for r in store.checkpoint_rows(phase='epochs',unit='2') if r['receipt'].get('epoch_end')))
                verify_store(store)
                if not self.incremental:
                    # Schema 1 serves no bridge metadata: nothing to report.
                    self.assertNotIn('bridge',caught_up)
                if self.incremental:
                    # A full SQL receipt remains full-profile-bound when it is
                    # made the schema-4 integrity base after a compact delta.
                    self.assertEqual(store.generation()['manifest']['acquisition']['version'],2)
                    self.assertEqual(profile_digest(store.generation()),digest(fixture.profile.identity))
                    self.assertEqual(store.write_checkpoint(store.certify_checkpoint()),
                                     {'generation':2,'previous_base':1})
                    self.assertEqual(store.db.execute('SELECT generation,previous_base FROM chain.integrity_checkpoints ORDER BY generation').fetchall(),
                                     [(1,0),(2,1)])
                    verify_store(store)
                    # The cardano-db-sync update holds no bridge metadata: coverage starts again after it, and a
                    # checkpoint inside it is reported unsupported.
                    from db_sync_alt.bridge import parse_checkpoint
                    bridge=store.status(parse_checkpoint('block:%d'%(it.gen_fixture.OFFSET+10)))['bridge']
                    self.assertEqual((bridge['coverage_start'],bridge['checkpoint_supported'],bridge['reason']),(it.gen_fixture.OFFSET+13,False,'before_coverage'))
                    # The catch-up says so when it publishes.
                    self.assertEqual((caught_up['bridge']['coverage_start'],caught_up['bridge_coverage_ended'],caught_up['warnings']),
                                     (it.gen_fixture.OFFSET+13,{'previous_coverage_start':it.gen_fixture.OFFSET+11,'coverage_start':it.gen_fixture.OFFSET+13},
                                      ['bridge_metadata_coverage_restarted']))
                    # Both publication paths kept the served tables exact; db-sync reports the stored ordinals.
                    from db_sync_alt.store import serve
                    serve.verify(store,store.generation()['id'])
                    inputs={tuple(bytes(v) if isinstance(v,memoryview) else v for v in r[:3]):r[3] for r in store.db.execute('SELECT tx_hash,origin_hash,origin_index,ordinal FROM chain.inputs')}
                    assets={tuple(bytes(v) if isinstance(v,memoryview) else v for v in r[:4]):r[4] for r in store.db.execute('SELECT tx_hash,output_index,policy,name,ordinal FROM chain.output_assets')}
                    self.assertTrue(inputs and assets)
                    anchors={k:store.generation()['manifest']['anchors'][k] for k in ('start','target')}
                    self.assertEqual(source().ordinals(sorted(inputs),sorted(assets),anchors),{'inputs':inputs,'output_assets':assets})
                    with self.assertRaisesRegex(Exception,'source_anchor_conflict'):
                        source().ordinals([],[],{'start':dict(anchors['start'],hash='00'*32),'target':anchors['target']})
        finally:
            if old is None:os.environ.pop('PGSERVICEFILE',None)
            else:os.environ['PGSERVICEFILE']=old
            tmp.cleanup();fixture.tearDown()

class IncrementalInterchangeTests(InterchangeTests):
    incremental = True
