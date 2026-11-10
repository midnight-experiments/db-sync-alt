"""Store, backup/restore, access and prior-answer controls on PostgreSQL with the synthetic three-generation store."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from unittest.mock import patch

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.types.json import Jsonb
from db_sync_alt.contract import Refusal
from db_sync_alt.store.database import PgStore, RESTORE_GUARD
from db_sync_alt.backup import backup, restore
from db_sync_alt.verify.store import verify_store, validate_prior_answers, physical_inventory
from fixtures.generations import make_store, PROFILE, START, h, block


class PgCoreTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp=tempfile.TemporaryDirectory();cls.admin=psycopg.connect(os.environ['DB_SYNC_ALT_TEST_DSN'],autocommit=True)
    @classmethod
    def tearDownClass(cls):
        cls.admin.close();cls.tmp.cleanup()
    def setUp(self):self.databases=[]
    def tearDown(self):
        for name in self.databases:
            self.admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(name)))
    def db(self):
        name='db_sync_alt_test_'+uuid.uuid4().hex[:12];self.databases.append(name)
        self.admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
        return make_conninfo(os.environ['DB_SYNC_ALT_TEST_DSN'],dbname=name)
    def stored(self):
        """A new database holding the ready three-generation store; returns its DSN and verification reports."""
        conninfo=self.db();reports=make_store(conninfo)
        with PgStore(conninfo=conninfo,readonly=True) as store:self.assertEqual(store.meta('canonical_ready'),2)
        return conninfo,reports
    def assert_code(self,code,fn):
        with self.assertRaises(Refusal) as cm:fn()
        self.assertEqual(cm.exception.code,code)
    def ready_generation(self,store):
        row=store.db.execute('SELECT id FROM chain.ready_generation').fetchone()
        return None if row is None else row[0]
    def assert_closed_to_others(self,store):
        """No role but the owner may connect to, create in or use the internals of a db-sync-alt database."""
        self.assertEqual(store.db.execute("""SELECT has_database_privilege('public',current_database(),'CONNECT'),
            has_database_privilege('public',current_database(),'TEMPORARY'),has_database_privilege('public',current_database(),'CREATE'),
            has_schema_privilege('public','chain','USAGE'),has_schema_privilege('public','public','CREATE')""").fetchone(),(False,)*5)
        self.assertEqual(store.db.execute("""SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
            WHERE n.nspname='chain' AND has_function_privilege('public',p.oid,'EXECUTE')""").fetchone()[0],0)
    def pending(self,store):
        old=store.generation();return store.begin_generation(source='blockfrost-cardano',start=PROFILE.start,target=old['target_height']+1,witness=old['witness_height']+1,manifest={},parent=old['id'])

    def test_three_generation_store_and_role_boundary(self):
        conninfo,reports=self.stored()
        with PgStore(conninfo=conninfo) as store:
            self.assertEqual([g['id'] for g in store.status()['generations']],[0,1,2])
            self.assertEqual(store.db.execute('SELECT count(*) FROM chain.provenance').fetchone()[0],3)
            self.assertGreater(store.db.execute('SELECT count(*) FROM chain.blocks').fetchone()[0],len(list(store.rows('blocks',2))))
            for gen in range(3):self.assertEqual(verify_store(store,gen),reports[gen])
            self.assertEqual(store.fact('outputs',(h('origin'),0))['value'],str(2**64-1))
            # Schema 1 until the explicit upgrade; a reader is granted only on the current schema.
            self.assertEqual((store.state()['schema_version'],self.ready_generation(store)),(1,2))
            self.assert_closed_to_others(store)
            other='not_granted_'+uuid.uuid4().hex[:8]
            self.admin.execute(sql.SQL('CREATE ROLE {} LOGIN').format(sql.Identifier(other)))
            try:
                with self.assertRaises(psycopg.OperationalError):psycopg.connect(make_conninfo(conninfo,user=other)).close()
                with psycopg.connect(conninfo,autocommit=True) as db:
                    db.execute(sql.SQL('SET ROLE {}').format(sql.Identifier(other)))
                    for query in ('SELECT * FROM chain.outputs','SELECT * FROM chain.generation_blocks','SELECT * FROM chain.membership(0)'):
                        with self.assertRaises(psycopg.errors.InsufficientPrivilege):db.execute(query)
            finally:
                self.admin.execute(sql.SQL('DROP ROLE {}').format(sql.Identifier(other)))
            self.assert_code('writer_busy',lambda:PgStore(conninfo=conninfo))
            gen=self.pending(store)
            self.assertEqual(self.ready_generation(store),2)
            self.assert_code('immutable_fact_conflict',lambda:store.insert('outputs',dict(store.fact('outputs',(h('origin'),0)),value='1'),gen))
            self.assert_code('publication_state_requires_fenced_transition',lambda:store.set_meta('canonical_ready',gen))

    def test_nonempty_destination_refused(self):
        conninfo=self.db()
        with psycopg.connect(conninfo,autocommit=True) as db:db.execute('CREATE TABLE unrelated(id int)')
        self.assert_code('destination_not_empty',lambda:make_store(conninfo))
        with psycopg.connect(conninfo,autocommit=True) as db:self.assertIsNotNone(db.execute("SELECT to_regclass('unrelated')").fetchone()[0])

    def test_function_only_destination_is_unrelated(self):
        conninfo=self.db()
        with psycopg.connect(conninfo,autocommit=True) as db:
            db.execute('CREATE FUNCTION public.unrelated() RETURNS integer LANGUAGE sql AS \'SELECT 42\'')
        self.assert_code('destination_not_empty',lambda:make_store(conninfo))
        with psycopg.connect(conninfo,autocommit=True) as db:
            self.assertEqual(db.execute('SELECT public.unrelated()').fetchone()[0],42)
            self.assertIsNone(db.execute("SELECT to_regnamespace('chain')").fetchone()[0])
            self.assertIsNone(db.execute('SELECT to_regnamespace(%s)',(RESTORE_GUARD,)).fetchone()[0])

    def test_interrupted_build_stays_unready_and_retry_refused(self):
        conninfo=self.db()
        def interrupt(store,generation):raise Refusal('injected_interruption')
        self.assert_code('injected_interruption',lambda:make_store(conninfo,before_seal=interrupt))
        with PgStore(conninfo=conninfo,readonly=True,allow_incomplete=True) as store:
            self.assertEqual(store.state()['migration_state'],'importing')
            self.assertIsNone(store.meta('canonical_ready'))
            self.assertIsNone(self.ready_generation(store))
            self.assertEqual(store.db.execute('SELECT count(*) FROM public.block').fetchone()[0],0)
        self.assert_code('incomplete_owned_destination',lambda:PgStore(conninfo=conninfo))
        self.assert_code('destination_not_empty',lambda:make_store(conninfo))

    def test_backup_restore_all_physical_rows_and_ids_without_test_fixtures(self):
        conninfo,_=self.stored();folder=Path(self.tmp.name)/uuid.uuid4().hex;folder.mkdir()
        archive=folder/'state.dump';manifest=folder/'state.json'
        with PgStore(conninfo=conninfo,readonly=True) as store:
            before=physical_inventory(store);ids=store.db.execute('SELECT hash,id FROM chain.blocks ORDER BY hash').fetchall()
        receipt=backup(archive,manifest,conninfo=conninfo)
        unrelated=self.db()
        with psycopg.connect(unrelated,autocommit=True) as db:db.execute("CREATE FUNCTION public.unrelated() RETURNS integer LANGUAGE sql AS 'SELECT 42'")
        self.assert_code('destination_not_empty',lambda:restore(archive,manifest,conninfo=unrelated))
        with psycopg.connect(unrelated,autocommit=True) as db:
            self.assertEqual(db.execute('SELECT public.unrelated()').fetchone()[0],42)
            self.assertIsNone(db.execute("SELECT to_regnamespace('chain')").fetchone()[0])
            self.assertIsNone(db.execute('SELECT to_regnamespace(%s)',(RESTORE_GUARD,)).fetchone()[0])
        recovered=self.db();result=restore(archive,manifest,conninfo=recovered,expected_manifest_sha256=receipt['manifest_sha256'])
        self.assertEqual(result['canonical_ready'],2)
        with PgStore(conninfo=recovered) as store:
            self.assertEqual(physical_inventory(store),before)
            self.assertEqual(store.db.execute('SELECT hash,id FROM chain.blocks ORDER BY hash').fetchall(),ids)
            self.assertEqual(self.ready_generation(store),2)
            self.assert_closed_to_others(store)
            # Native sequence state resumes beyond every retained surrogate ID.
            gen=self.pending(store);row=block(START-20);store.insert('blocks',row,gen)
            nextid=store.db.execute('SELECT id FROM chain.blocks WHERE hash=%s',(row['hash'],)).fetchone()[0]
            self.assertGreater(nextid,max(v for _,v in ids))
        env=dict(os.environ);env['RUNTIME_TEST_DSN']=conninfo
        code="""import builtins,os,sys
original=builtins.__import__
def deny(name,*args,**kwargs):
 if name.split('.')[0]=='fixtures':raise AssertionError(name)
 return original(name,*args,**kwargs)
builtins.__import__=deny
from db_sync_alt.store.database import PgStore
from db_sync_alt.verify.store import verify_store
import db_sync_alt.backup as pg_backup
with PgStore(conninfo=os.environ['RUNTIME_TEST_DSN'],readonly=True) as s:
 assert verify_store(s)['generation']==2
 assert s.db.execute('SELECT id FROM chain.ready_generation').fetchone()[0]==2
print('postgres-only-runtime-ok')
"""
        completed=subprocess.run([sys.executable,'-c',code],env=env,capture_output=True,text=True)
        self.assertEqual(completed.returncode,0,completed.stderr);self.assertIn('postgres-only-runtime-ok',completed.stdout)
        self.assert_code('destination_not_empty',lambda:restore(archive,manifest,conninfo=conninfo))
        archive.write_bytes(archive.read_bytes()+b'corrupt')
        self.assert_code('backup_checksum_mismatch',lambda:restore(archive,manifest,conninfo=self.db()))

    def test_restore_interruption_marker_and_wrong_profile(self):
        conninfo,_=self.stored();folder=Path(self.tmp.name)/uuid.uuid4().hex;folder.mkdir()
        archive=folder/'state.dump';manifest=folder/'state.json';backup(archive,manifest,conninfo=conninfo)
        destination=self.db()
        with patch('db_sync_alt.backup.run_cli',side_effect=Refusal('postgres_backup_tool_failure')):
            self.assert_code('postgres_backup_tool_failure',lambda:restore(archive,manifest,conninfo=destination))
        self.assert_code('incomplete_owned_destination',lambda:PgStore(conninfo=destination,readonly=True,allow_incomplete=True))
        self.assert_code('destination_not_empty',lambda:restore(archive,manifest,conninfo=destination))
        body=json.loads(manifest.read_text());body['profile']['network']='mainnet';manifest.write_text(json.dumps(body))
        self.assert_code('backup_profile_mismatch',lambda:restore(archive,manifest,conninfo=self.db()))

    def test_numeric_domains_and_write_rollback(self):
        conninfo,_=self.stored()
        with PgStore(conninfo=conninfo) as store:
            gen=self.pending(store)
            for value in ('1.2','-1','18446744073709551616','NaN','Infinity'):
                with self.assertRaises(psycopg.errors.CheckViolation):store.db.execute('INSERT INTO chain.outputs(tx_hash,output_index,address,value,first_generation) VALUES(%s,1,\'bad\',%s,%s)',(h('spend'),value,gen))
            for value in ('01','1.2','+1','-0'):
                self.assert_code('invalid_numeric_domain',lambda:store.insert('outputs',{'tx_hash':h('spend'),'output_index':1,'address':'holder','value':value,'datum_hash':None},gen))
            with self.assertRaises(RuntimeError):
                with store.atomic():store.checkpoint(gen,'events','failed',{'lo':1,'hi':2});raise RuntimeError('rollback')
            self.assertFalse(store.completed(gen,'events','failed'))
            self.assertEqual(store.meta('canonical_ready'),2)

    def test_prior_answer_guards_allow_pre_s_but_refuse_new_contributor(self):
        conninfo,_=self.stored()
        with PgStore(conninfo=conninfo) as store:
            gen=self.pending(store);policy=bytes.fromhex(PROFILE.night[:56]);name=bytes.fromhex(PROFILE.night[56:])
            # New old origin, discovered only for future closure, does not rewrite covered events.
            row=block(START-30);store.insert('blocks',row,gen);store.bind_block(gen,row['height'],row['hash'])
            store.insert('transactions',{'hash':h('late'),'block_hash':row['hash'],'tx_index':0,'valid':1},gen)
            store.insert('outputs',{'tx_hash':h('late'),'output_index':0,'address':'holder','value':'1','datum_hash':None},gen)
            store.insert('output_assets',{'tx_hash':h('late'),'output_index':0,'policy':policy,'name':name,'quantity':'1','ordinal':0},gen)
            validate_prior_answers(store,gen)
            # Missing asset on an already spent pre-S output changes the OLD spend answer.
            # Prepare the prior snapshot fixture under explicit admin mutation for regression.
            store.db.execute('DELETE FROM chain.output_assets WHERE tx_hash=%s',(h('origin'),))
            store.insert('output_assets',{'tx_hash':h('origin'),'output_index':0,'policy':policy,'name':name,'quantity':'3','ordinal':0},gen)
            self.assert_code('historical_selected_event_changed',lambda:validate_prior_answers(store,gen))
            store.db.execute('DELETE FROM chain.output_assets WHERE tx_hash=%s',(h('origin'),))
            store.insert('outputs',{'tx_hash':h('spend'),'output_index':1,'address':'holder','value':'1','datum_hash':None},gen)
            store.insert('output_assets',{'tx_hash':h('spend'),'output_index':1,'policy':policy,'name':name,'quantity':'1','ordinal':0},gen)
            self.assert_code('historical_selected_event_changed',lambda:validate_prior_answers(store,gen))

    def test_sync_job_reservation(self):
        conninfo,_=self.stored()
        with PgStore(conninfo=conninfo) as store:
            body={'job_id':'fixture-job','attempts':1,'budget':10,'state':'active'}
            store.save_sync_job('provider-active',body)
            with PgStore(conninfo=conninfo,readonly=True) as reader:self.assertEqual(reader.sync_job('provider-active'),body)
            self.assert_code('sync_job_budget_reset',lambda:store.save_sync_job('provider-active',dict(body,attempts=0)))
            self.assert_code('sync_job_budget_reset',lambda:store.save_sync_job('provider-active',dict(body,budget=11)))
            self.assert_code('sync_job_rollover_refused',lambda:store.save_sync_job('provider-active',dict(body,job_id='new-job')))
            self.assert_code('invalid_sync_job',lambda:store.save_sync_job('provider-active',dict(body,attempts=True)))
            self.assert_code('invalid_sync_job',lambda:store.save_sync_job('provider-active',dict(body,budget=10001)))
            with store.atomic():self.assert_code('sync_job_requires_durable_autocommit',lambda:store.save_sync_job('provider-active',dict(body,attempts=2)))
        with PgStore(conninfo=conninfo) as store:self.assertEqual(store.sync_job('provider-active')['attempts'],1)

    def test_bulk_header_insert_keeps_single_insert_rules(self):
        conninfo,_=self.stored()
        with PgStore(conninfo=conninfo) as store:
            gen=self.pending(store)
            def bulk(rows):
                with store.atomic():return store.insert_blocks(gen,rows)
            self.assertEqual(bulk([block(START)]),0)  # identical header, already inherited at its height
            self.assert_code('immutable_fact_conflict',lambda:bulk([dict(block(START),tx_count=99)]))
            self.assert_code('canonical_block_conflict',lambda:bulk([dict(block(START),hash=h('other-at-start'))]))
            late=[block(START-40),block(START-39)]
            self.assert_code('canonical_block_conflict',lambda:bulk([*late,dict(block(START-39),hash=h('twin-at-height'))]))
            self.assertIsNone(store.block_at(gen,START-40))
            self.assertEqual(bulk(late),2)
            self.assertEqual(bulk(late),0)
            for row in late:self.assertEqual(store.block_at(gen,row['height'])['hash'],row['hash'])
            self.assertEqual(store.fact('blocks',(late[0]['hash'],)),late[0])

    def test_epoch_and_state_history_refusal(self):
        conninfo,_=self.stored()
        with PgStore(conninfo=conninfo) as store:
            gen=self.pending(store)
            store.insert('epoch_stakes',{'epoch':0,'pool_hash':b'q'*28,'amount':'1'},gen)
            self.assert_code('historical_epoch_changed',lambda:validate_prior_answers(store,gen))
            store.db.execute('DELETE FROM chain.epoch_stakes WHERE pool_hash=%s',(b'q'*28,))
            policy=bytes.fromhex(PROFILE.permissioned)
            store.insert('assets',{'policy':policy,'name':b''},gen)
            store.insert('output_assets',{'tx_hash':h('origin'),'output_index':0,'policy':policy,'name':b'','quantity':'1','ordinal':1},gen)
            self.assert_code('historical_state_changed',lambda:validate_prior_answers(store,gen))

    def test_malformed_serving_admission_receipts_and_assets(self):
        def anchor(store,generation):
            if generation==0:
                receipt=store.checkpoint_rows(generation=0,phase='epochs',unit='0')[0]['receipt'];receipt['epoch_end']['hash']='ff'*32
                store.db.execute("UPDATE chain.checkpoints SET receipt=%s WHERE generation=0 AND phase='epochs' AND unit='0'",(Jsonb(receipt),))
        def asset(store,generation):
            if generation==2:store.insert('assets',{'policy':b'z'*28,'name':b''},2)
        for variant,expected in [(anchor,'epoch_anchor_mismatch'),(asset,'selected_asset_outside_profile')]:
            destination=self.db()
            self.assert_code(expected,lambda:make_store(destination,before_seal=variant))
            with PgStore(conninfo=destination,readonly=True,allow_incomplete=True) as store:
                self.assertIsNone(self.ready_generation(store));self.assertIsNone(store.meta('canonical_ready'))

    def membership_reference_views(self,store):
        store.db.execute('CREATE SCHEMA reference_views')
        store.db.execute('CREATE VIEW reference_views.tx AS SELECT t.id,t.hash,b.id AS block_id,t.tx_index AS block_index,(t.valid=1) AS valid_contract FROM chain.transactions t JOIN public.block b ON b.hash=t.block_hash WHERE t.first_generation<=(SELECT id FROM chain.ready_generation);')
        store.db.execute('CREATE VIEW reference_views.tx_out AS SELECT o.id,t.id AS tx_id,o.output_index AS index,o.address,o.value,o.datum_hash AS data_hash,NULL::bigint AS consumed_by_tx_id FROM chain.outputs o JOIN reference_views.tx t ON t.hash=o.tx_hash WHERE o.first_generation<=(SELECT id FROM chain.ready_generation);')
        store.db.execute('CREATE VIEW reference_views.tx_in AS SELECT i.id,t.id AS tx_in_id,origin.id AS tx_out_id,i.origin_index AS tx_out_index FROM chain.inputs i JOIN reference_views.tx t ON t.hash=i.tx_hash JOIN reference_views.tx origin ON origin.hash=i.origin_hash WHERE i.first_generation<=(SELECT id FROM chain.ready_generation);')
        store.db.execute('CREATE VIEW reference_views.ma_tx_mint AS SELECT a.id,a.quantity,t.id AS tx_id,m.id AS ident FROM chain.mints a JOIN reference_views.tx t ON t.hash=a.tx_hash JOIN public.multi_asset m ON m.policy=a.policy AND m.name=a.name WHERE a.first_generation<=(SELECT id FROM chain.ready_generation);')

    def assert_view_equality(self,store):
        result={}
        for name in ('tx','tx_out','tx_in','ma_tx_mint'):
            current=store.db.execute('SELECT * FROM public.'+name+' ORDER BY id')
            types=[(c.name,c.type_code) for c in current.description];rows=current.fetchall()
            reference=store.db.execute('SELECT * FROM reference_views.'+name+' ORDER BY id')
            self.assertEqual(types,[(c.name,c.type_code) for c in reference.description])
            self.assertEqual(rows,reference.fetchall());result[name]=rows
        return result

    def test_fact_view_prefix_pending_and_revocation_equality(self):
        conninfo,_=self.stored()
        with PgStore(conninfo=conninfo) as store:
            self.membership_reference_views(store)
            for generation in (0,1,2):
                store.db.execute('UPDATE chain.service_state SET canonical_ready=%s',(generation,))
                self.assert_view_equality(store)
            before=self.assert_view_equality(store);gen=self.pending(store)
            row=block(START-30);store.insert('blocks',row,gen);store.bind_block(gen,row['height'],row['hash'])
            store.insert('transactions',{'hash':h('late-view'),'block_hash':row['hash'],'tx_index':0,'valid':1},gen)
            store.insert('outputs',{'tx_hash':h('late-view'),'output_index':0,'address':'holder','value':'1','datum_hash':None},gen)
            self.assertEqual(self.assert_view_equality(store),before)
            store.mark_conflict()
            self.assertTrue(all(not rows for rows in self.assert_view_equality(store).values()))

    def test_valid_empty_publication_and_incomplete_publication_refusal(self):
        from db_sync_alt.contract import digest
        conninfo,_=self.stored()
        with PgStore(conninfo=conninfo) as store:
            self.membership_reference_views(store);before_views=self.assert_view_equality(store)
            previous=store.generation();target=previous['target_height']+1;witness=target+2160
            manifest={'identity':PROFILE.identity,'contracts':digest(PROFILE.contract),'required_epochs':[0,1,2],'capabilities':{'bridge_qualified':False,'candidates_empty':True,'epoch_max_tx_size_v1':True},'anchors':{n:{'height':height,'hash':block(height,True)['hash'].hex()} for n,height in [('start',START),('target',target),('witness',witness)]}}
            manifest.update(source_identity={'adapter':'db-sync-direct-pg-v1'},parent_digest=digest(previous['manifest']['digests']),acquisition={'version':1,'window_blocks':1000})
            gen=store.begin_generation(source='cardano-db-sync-postgresql',start=PROFILE.start,target=target,witness=witness,manifest=manifest,parent=2)
            with self.assertRaises(Refusal):store.publish(gen,manifest)
            self.assertEqual(store.meta('canonical_ready'),2)
            with store.atomic():
                store.db.execute('INSERT INTO chain.generation_blocks SELECT %s,height,hash FROM chain.membership(2) WHERE height>%s',(gen,previous['target_height']))
                row=block(witness,True);store.insert('blocks',row,gen);store.bind_block(gen,witness,row['hash'])
                store.checkpoint(gen,'metadata','inherited',{'lo':START,'hi':previous['target_height'],'parent':2,'parent_digest':digest(previous['manifest']['digests']),'source_kind':'generation-inheritance','inherited_source_kind':previous['source']})
                store.checkpoint(gen,'metadata','new',{'lo':target,'hi':witness,'source_kind':'cardano-db-sync-postgresql'})
                store.checkpoint(gen,'events','new',{'lo':target,'hi':target,'source_kind':'cardano-db-sync-postgresql'})
            late=block(START-30);store.insert('blocks',late,gen);store.bind_block(gen,late['height'],late['hash'])
            store.insert('transactions',{'hash':h('published-late'),'block_hash':late['hash'],'tx_index':0,'valid':1},gen)
            store.insert('outputs',{'tx_hash':h('published-late'),'output_index':0,'address':'holder','value':'1','datum_hash':None},gen)
            self.assertEqual(self.assert_view_equality(store),before_views)
            verified=verify_store(store,gen,require_ready=False)
            # Synthetic source inventory for this isolated view-only fixture.
            independent={'version':1,'scope':'independent-new-facts-and-membership-with-certified-parent','parent_digest':manifest['parent_digest'],'ranges':[['metadata',lo,min(witness,lo+999)] for lo in range(target,witness+1,1000)]+[['events',target,target]],'epochs':[],'inventory':verified['digests']}
            manifest.update(digests=verified['digests'],validation={'kind':'independent-sql-delta-inventory-v2','digests':verified['digests'],'independent':independent})
            manifest=store.finalize_pending(gen,manifest,{'independent':independent},verified['digests'])
            store.publish(gen,manifest);self.assertEqual(store.meta('canonical_ready'),gen)
            self.assertEqual(self.ready_generation(store),gen)
            after_views=self.assert_view_equality(store)
            self.assertEqual(len(after_views['tx']),len(before_views['tx'])+1)
            self.assertEqual(len(after_views['tx_out']),len(before_views['tx_out'])+1)
            for name,rows in before_views.items():self.assertTrue(set(rows).issubset(set(after_views[name])))
            self.assertEqual(verify_store(store,2)['digests'],previous['manifest']['digests'])

    def test_verification_reports_progress(self):
        conninfo,_=self.stored();events=[]
        with PgStore(conninfo=conninfo,progress_callback=lambda phase,**fields:events.append((phase,fields))) as store:
            self.assertEqual(store.db.execute('SHOW statement_timeout').fetchone()[0],'30s')
            before=store.generation(2)['manifest']['digests'];events.clear()
            self.assertEqual(verify_store(store,2)['digests'],before)
            self.assertEqual(sum(phase=='digest_family_complete' for phase,_ in events),10)
            self.assertIn('verification_complete',[phase for phase,_ in events])
            self.assertTrue(all(fields.get('rows',0)>=0 for _,fields in events))

    def test_importing_store_with_ready_rows_never_serves(self):
        conninfo=self.db()
        with PgStore(conninfo=conninfo,create=True,network='preprod') as store:
            store.db.execute("INSERT INTO chain.generations VALUES(0,NULL,'ready','blockfrost-cardano',4500197,4,4500197,4502357,'{}')")
            store.db.execute("INSERT INTO chain.blocks(hash,height,parent_hash,epoch,slot,time,tx_count,first_generation) SELECT decode(md5(n::text)||md5((n+1)::text),'hex'),n,NULL,0,n,'2026-01-01',0,0 FROM generate_series(100000,120000) n")
            store.db.execute('INSERT INTO chain.generation_blocks SELECT 0,height,hash FROM chain.blocks')
            self.assertEqual(store.db.execute('SELECT count(*) FROM chain.generation_blocks').fetchone()[0],20001)
            self.assertEqual(store.state()['migration_state'],'importing')
            self.assertIsNone(store.state()['canonical_ready'])
            self.assertEqual(store.db.execute('SELECT count(*) FROM public.block').fetchone()[0],0)
            self.assertIsNone(self.ready_generation(store))
        self.assert_code('destination_not_empty',lambda:PgStore(conninfo=conninfo,create=True,network='preprod'))

    def test_point_reads_use_indexed_membership_at_scale(self):
        conninfo,_=self.stored()
        with PgStore(conninfo=conninfo) as store:
            # Additional pre-S metadata is legal dependency metadata for this perf fixture.
            store.db.execute("INSERT INTO chain.blocks(hash,height,parent_hash,epoch,slot,time,tx_count,first_generation) SELECT decode(md5(n::text)||md5((n+1)::text),'hex'),n,NULL,0,n,'2026-01-01',0,0 FROM generate_series(100000,199999) n")
            store.db.execute('INSERT INTO chain.generation_blocks SELECT 0,height,hash FROM chain.blocks WHERE height BETWEEN 100000 AND 199999')
            store.db.execute('ANALYZE')
            samples=[]
            for _ in range(8):
                before=time.monotonic();self.assertEqual(store.block_at(2,START)['height'],START);samples.append((time.monotonic()-before)*1000)
            self.assertLess(max(samples),250)
            explained=store.db.execute('EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT * FROM public.block WHERE block_no=%s',(START,)).fetchone()[0]
            plan=explained[0]['Plan'];self.assertLess(plan.get('Shared Hit Blocks',0)+plan.get('Shared Read Blocks',0),1000)

if __name__=='__main__':unittest.main(verbosity=2)
