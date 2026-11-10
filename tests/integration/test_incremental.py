"""Isolated PostgreSQL schema-2 proof admission and full-publication controls."""
import copy
import unittest
import psycopg
from unittest.mock import patch
from db_sync_alt.store.database import PgStore
from db_sync_alt.store.upgrade import upgrade_store
from db_sync_alt.verify.store import verify_store
from db_sync_alt.contract import Refusal, TABLES
from db_sync_alt.sources.blockfrost.adapter import update
import fixtures.blockfrost_generation as gen_fixture
from fixtures.database import SyncFixture

class IncrementalTests(SyncFixture):
    def test_projection_stream_exact_corruption_and_cancel(self):
        from db_sync_alt.verify.store import verify_projection
        from db_sync_alt.errors import CooperativeStop
        dsn=self.make()
        class Rollback(Exception):pass
        with PgStore(conninfo=dsn) as store:
            upgrade_store(store);verify_projection(store,0)
            rows=store.db.execute('SELECT height,hash FROM chain.current_membership ORDER BY height').fetchall()
            for statement,args in [
                ('DELETE FROM chain.current_membership WHERE height=%s',(rows[1][0],)),
                ('INSERT INTO chain.current_membership VALUES(%s,%s)',(rows[-1][0]+1,rows[-1][1])),
                ('UPDATE chain.current_membership SET hash=%s WHERE height=%s',(rows[0][1],rows[1][0]))]:
                with self.assertRaises(Rollback):
                    with store.atomic():
                        store.db.execute("SET LOCAL db_sync_alt.projection_write='on'")
                        store.db.execute(statement,args)
                        with self.assertRaisesRegex(Refusal,'membership_projection_mismatch'):verify_projection(store,0)
                        raise Rollback()
                self.assertEqual(store.db.execute('SELECT height,hash FROM chain.current_membership ORDER BY height').fetchall(),rows)
            def stop(phase,**values):
                if phase=='projection_verification' and values['rows']==1000:raise CooperativeStop()
            store.progress_callback=stop
            with self.assertRaises(CooperativeStop):verify_projection(store,0)
            store.progress_callback=None
            self.assertEqual(store.db.info.transaction_status,psycopg.pq.TransactionStatus.IDLE)
            self.assertEqual(store.db.execute("SELECT count(*) FROM pg_cursors WHERE name LIKE 'projection_%'").fetchone()[0],0)
            self.assertEqual(store.db.execute('SHOW statement_timeout').fetchone()[0],'30s')
            verify_projection(store,0)
            store.db.execute('ALTER FUNCTION chain.membership(bigint) RENAME TO membership_fixture_original')
            store.db.execute('''CREATE FUNCTION chain.membership(generation_id bigint) RETURNS TABLE(height integer,hash bytea) LANGUAGE sql STABLE AS $$
                SELECT * FROM chain.membership_fixture_original(generation_id)
                UNION ALL (SELECT * FROM chain.membership_fixture_original(generation_id) LIMIT 1) $$''')
            with self.assertRaisesRegex(Refusal,'membership_projection_mismatch'):verify_projection(store,0)

    def test_projection_stream_one_snapshot_during_concurrent_change(self):
        from db_sync_alt.verify.store import verify_projection
        dsn=self.make()
        with PgStore(conninfo=dsn) as writer:
            upgrade_store(writer)
            with PgStore(conninfo=dsn,readonly=True) as reader:
                row=writer.db.execute('SELECT height,hash FROM chain.current_membership ORDER BY height DESC LIMIT 1').fetchone()
                changed=[]
                def concurrent(phase,**values):
                    if phase=='projection_verification' and values['rows']==1000 and not changed:
                        with writer.atomic():
                            writer.db.execute("SET LOCAL db_sync_alt.projection_write='on'")
                            writer.db.execute('DELETE FROM chain.current_membership WHERE height=%s',(row[0],))
                        changed.append(True)
                reader.progress_callback=concurrent
                # Deliberate privileged projection change after the cursor's
                # snapshot must not mix old expected/new actual streams.
                verify_projection(reader,0);self.assertEqual(changed,[True])
                reader.progress_callback=None
                with self.assertRaisesRegex(Refusal,'membership_projection_mismatch'):verify_projection(reader,0)
                # Full certification rejects the missing witness even earlier
                # in its anchor check; it must not issue a parent capability.
                with self.assertRaisesRegex(Refusal,'anchor_mismatch'):writer.certify_ready_full()
                with self.assertRaisesRegex(Refusal,'parent_uncertified'):writer.current_verified_parent()
                with writer.atomic():
                    writer.db.execute("SET LOCAL db_sync_alt.projection_write='on'")
                    writer.db.execute('INSERT INTO chain.current_membership VALUES(%s,%s)',row)
                writer.certify_ready_full()

    def prepared_transition(self,store,*,transition=False):
        """Copy generation 1 of a full-update reference into `store` as an incremental continuation in the written form."""
        from db_sync_alt.contract import digest
        routes,_=gen_fixture.fixtures(transition=transition)
        with gen_fixture.baseline(self.database(),transition=transition) as reference:
            expected=gen_fixture.advance(reference,routes,budget=500)
            m=copy.deepcopy(reference.generation()['manifest'])
            for key in ('digests','validation','completion'):m.pop(key,None)
            m['identity_sha256']=digest(m.pop('identity'))
            gen=store.begin_continuation(source='blockfrost-cardano',start=self.profile.start,target=gen_fixture.OFFSET+12,witness=gen_fixture.OFFSET+2172,manifest=m,parent_capability=store.current_verified_parent())
            cutoff=store.generation(0)['witness_height']
            store.stage_metadata_reuse(gen,cutoff,{'height':cutoff,'hash':store.block_at(0,cutoff)['hash'].hex()})
            for table in TABLES:
                for row in reference.rows(table,1):
                    if table in ('inputs','output_assets'):row['ordinal']=reference.ordinal(table,row)
                    store.insert(table,row,gen)
                    if table=='blocks':store.bind_block(gen,row['height'],row['hash'])
            for row in reference.checkpoint_rows(generation=1):
                if row['phase'] not in ('metadata','final-anchors','completion-revalidation'):
                    store.checkpoint(gen,row['phase'],row['unit'],row['receipt'])
            store.checkpoint(gen,'metadata','suffix',{'lo':cutoff+1,'hi':gen_fixture.OFFSET+2172,'source_kind':'blockfrost-cardano'})
            return gen,expected,reference.checkpoint_rows(generation=1,phase='final-anchors')[0]['receipt']

    def test_delta_ticket_seal_publish_oracle_and_backup_restore(self):
        from db_sync_alt.backup import backup,restore,file_hash
        from pathlib import Path
        dsn=self.make(transition=True)
        with PgStore(conninfo=dsn) as store:
            upgrade_store(store);store.certify_ready_full()
            gen,expected,receipt=self.prepared_transition(store,transition=True)
            with patch.object(store,'digests',side_effect=AssertionError('full history on fast path')):
                ticket=store.verify_pending_incremental(gen)
                original=store.generation(gen)['manifest']
                final=store.finalize_pending_incremental(gen,original,receipt,ticket)
                self.assertNotIn('digests',final)
                from db_sync_alt.verify.completion import validate_completion
                from db_sync_alt.contract import digest
                malformed=copy.deepcopy(store.generation(gen));malformed['manifest']['integrity']['semantic']['counts']['blocks']=True
                bad=malformed['manifest'];bad['integrity']['root']=digest(bad['integrity']['semantic'])
                bad['completion']['sha256']=digest({'integrity':bad['integrity'],'original_receipt_sha256':bad['completion']['original_receipt_sha256']})
                with self.assertRaisesRegex(Refusal,'completion_invalid'):validate_completion(store,malformed,require_seal=True)
                malformed=copy.deepcopy(store.generation(gen));malformed['manifest']['integrity']['version']=3
                with self.assertRaisesRegex(Refusal,'completion_invalid'):validate_completion(store,malformed,require_seal=True)
                with self.assertRaises(Refusal):store.publish(gen,final)
                store.publish_incremental(gen,ticket)
            self.assertEqual(verify_store(store)['digests'],expected['digests'])
            self.assertEqual(store.db.execute('SELECT max(block_no) FROM public.block').fetchone()[0],gen_fixture.OFFSET+2172)
            self.assertEqual(store.current_verified_parent().descriptor['generation'],1)
        archive=Path(self.tmp.name)/'v2.dump';manifest=Path(self.tmp.name)/'v2.json'
        backup(archive,manifest,conninfo=dsn)
        destination=self.database();restore(archive,manifest,conninfo=destination,expected_manifest_sha256=file_hash(manifest)['sha256'])
        with PgStore(conninfo=destination) as restored:
            self.assertTrue(restored.supports_incremental)
            self.assertEqual(verify_store(restored)['digests'],expected['digests'])
            restored.certify_ready_full()

    def test_full_metadata_verification_hash_joins_materialized_membership_and_blocks(self):
        """Full verification materializes the lineage membership and the block headers and hash-joins the
        two scans, with the rows of the direct join against chain.blocks; a broken parent link or a member
        missing from chain.blocks still refuses; the delta verification query keeps its text and arguments."""
        class Rollback(Exception):pass
        executed=[];execute=psycopg.ServerCursor.execute
        def recording(cursor,query,params=None,**kwargs):
            if cursor.name=='verify_metadata':executed.append((query,params))
            return execute(cursor,query,params,**kwargs)
        def nodes(node,*,ctes=True):
            return [node]+[n for child in node.get('Plans',[]) if ctes or child.get('Parent Relationship')!='InitPlan' for n in nodes(child,ctes=ctes)]
        joins=('Hash Join','Merge Join','Nested Loop')
        direct='WITH m AS MATERIALIZED (SELECT height,hash FROM chain.membership(%s)) SELECT m.height,b.hash,b.parent_hash,b.height FROM m LEFT JOIN chain.blocks b ON b.hash=m.hash WHERE m.height BETWEEN %s AND %s ORDER BY m.height'
        dsn=self.make(transition=True)
        with PgStore(conninfo=dsn) as store,patch.object(psycopg.ServerCursor,'execute',recording):
            upgrade_store(store);store.certify_ready_full()
            gen,expected,receipt=self.prepared_transition(store,transition=True)
            executed.clear();ticket=store.verify_pending_incremental(gen)
            membership,args=store.membership_sql(gen)
            reuse=store.db.execute('SELECT through_height FROM chain.generation_reuse WHERE generation=%s',(gen,)).fetchone()[0]
            self.assertEqual(executed,[('SELECT m.height,b.hash,b.parent_hash,b.height FROM ('+membership+') m LEFT JOIN chain.blocks b ON b.hash=m.hash WHERE m.height BETWEEN %s AND %s ORDER BY m.height',
                                        (*args,reuse+1,store.generation(gen)['witness_height']))])
            store.finalize_pending_incremental(gen,store.generation(gen)['manifest'],receipt,ticket);store.publish_incremental(gen,ticket)
            executed.clear();self.assertEqual(verify_store(store)['digests'],expected['digests'])
            # Startup verifies the published generation and its integrity base on the full path.
            self.assertEqual(sorted(params[0] for _,params in executed),[0,gen])
            full=list(executed)
            member=store.block_at(gen,store.generation(gen)['start_height']+1)['hash']
            for statement in ('UPDATE chain.blocks SET parent_hash=hash WHERE hash=%s','DELETE FROM chain.blocks WHERE hash=%s'):
                with self.assertRaises(Rollback):
                    with store.atomic():
                        store.db.execute("SET LOCAL session_replication_role='replica'")
                        store.db.execute(statement,(member,))
                        with self.assertRaisesRegex(Refusal,'metadata_gap_or_parent_conflict'):verify_store(store)
                        raise Rollback()
            self.assertEqual(verify_store(store)['digests'],expected['digests'])
            store.db.execute('ANALYZE')
            for query,params in full:
                plan=store.db.execute('EXPLAIN (FORMAT JSON) '+query,params).fetchone()[0][0]['Plan']
                main=nodes(plan,ctes=False)
                self.assertEqual([n['Node Type'] for n in main if n['Node Type'] in joins],['Hash Join'])
                self.assertEqual(sorted(n['CTE Name'] for n in main if n['Node Type']=='CTE Scan'),['b','m'])
                # chain.blocks is read once, by the materialized CTE; no lookup per membership row.
                self.assertEqual([n.get('Subplan Name') for n in nodes(plan) if n.get('Relation Name')=='blocks'],['CTE b'])
                rows=store.db.execute(query,params).fetchall()
                self.assertEqual(len(rows),params[-1]-params[-2]+1)
                self.assertEqual(rows,store.db.execute(direct,params).fetchall())

    def test_ticket_raw_delta_change_sealed_resume_and_publication_rollback(self):
        from dataclasses import replace
        dsn=self.make()
        with PgStore(conninfo=dsn) as store:
            upgrade_store(store);store.certify_ready_full()
            gen,expected,receipt=self.prepared_transition(store)
            ticket=store.verify_pending_incremental(gen)
            with self.assertRaisesRegex(Refusal,'ticket_invalid'):
                store.finalize_pending_incremental(gen,store.generation(gen)['manifest'],receipt,replace(ticket))
            original_proof=copy.deepcopy(ticket.proof)
            ticket.proof['counts']['transactions']+=1
            with self.assertRaisesRegex(Refusal,'ticket_invalid'):
                store.finalize_pending_incremental(gen,store.generation(gen)['manifest'],receipt,ticket)
            ticket.proof.clear();ticket.proof.update(original_proof)
            store.db.execute("INSERT INTO chain.checkpoints VALUES(%s,'diagnostic','changed','{}')",(gen,))
            with self.assertRaisesRegex(Refusal,'ticket_invalid'):
                store.finalize_pending_incremental(gen,store.generation(gen)['manifest'],receipt,ticket)
            ticket=store.verify_pending_incremental(gen)
            final=store.finalize_pending_incremental(gen,store.generation(gen)['manifest'],receipt,ticket)
            saved=copy.deepcopy(store.generation(gen));oldstate=store.state()
            with self.assertRaises(psycopg.errors.CheckViolation):
                store.db.execute("INSERT INTO chain.checkpoints VALUES(%s,'diagnostic','afterseal','{}')",(gen,))
            with self.assertRaisesRegex(Refusal,'interrupted'):
                store.publish_incremental(gen,ticket,stop_requested=lambda:True)
            self.assertEqual(store.generation(gen),saved);self.assertEqual(store.state(),oldstate)
            ticket.proof['counts']['transactions']+=1
            with self.assertRaisesRegex(Refusal,'ticket_invalid'):store.publish_incremental(gen,ticket)
            ticket.proof['counts']['transactions']-=1
        with PgStore(conninfo=dsn) as store:
            with self.assertRaises(Refusal):store.publish_incremental(gen,ticket)
            store.certify_ready_full();fresh=store.verify_pending_incremental(gen)
            self.assertEqual(store.finalize_pending_incremental(gen,final,{'fresh_source_check':True},fresh),final)
            self.assertEqual(store.generation(gen),saved)
            self.assertEqual(len(store.checkpoint_rows(generation=gen,phase='completion-revalidation')),1)
            execute=store.db.execute
            with psycopg.connect(dsn,autocommit=True) as reader:
                oldrows=reader.execute('SELECT * FROM public.block ORDER BY id').fetchall()
                def fail_commit(query,*args,**kw):
                    if isinstance(query,str) and query.startswith('UPDATE chain.service_state SET canonical_ready='):
                        self.assertEqual(reader.execute('SELECT * FROM public.block ORDER BY id').fetchall(),oldrows)
                        raise RuntimeError('injected publication interruption')
                    return execute(query,*args,**kw)
                with patch.object(store.db,'execute',side_effect=fail_commit),self.assertRaisesRegex(RuntimeError,'interruption'):
                    store.publish_incremental(gen,fresh)
                self.assertEqual(store.state(),oldstate);self.assertEqual(store.generation(gen),saved)
                self.assertEqual(reader.execute('SELECT * FROM public.block ORDER BY id').fetchall(),oldrows)
                store.publish_incremental(gen,fresh)
            self.assertEqual(verify_store(store)['digests'],expected['digests'])

    def test_duplicate_raw_membership_and_old_first_generation_refuse(self):
        dsn=self.make()
        with PgStore(conninfo=dsn) as store:
            upgrade_store(store);store.certify_ready_full()
            gen,_,_=self.prepared_transition(store)
            with self.assertRaises(psycopg.errors.CheckViolation):
                store.db.execute("INSERT INTO chain.datums(hash,value,first_generation) VALUES(decode(repeat('ef',32),'hex'),'{}',0)")
            start=store.generation()['start_height'];h=store.block_at(0,start)['hash']
            store.db.execute('INSERT INTO chain.membership_delta VALUES(%s,%s,%s)',(gen,start,h))
            with self.assertRaisesRegex(Refusal,'conflicting_inherited_membership'):store.verify_pending_incremental(gen)
            self.assertEqual(store.state()['canonical_ready'],0)

    def test_schema1_archive_restore_and_partial_pending_upgrade_resume(self):
        from db_sync_alt.backup import backup,restore
        from pathlib import Path
        from db_sync_alt.errors import TransientSyncFailure
        dsn=self.make()
        archive=Path(self.tmp.name)/'gen_fixture.dump';manifest=Path(self.tmp.name)/'gen_fixture.json'
        backup(archive,manifest,conninfo=dsn)
        target=self.database();restore(archive,manifest,conninfo=target)
        routes,_=gen_fixture.fixtures();broken=copy.deepcopy(routes)
        def unavailable():raise RuntimeError('HTTP 503; request counted, no automatic retry')
        broken['/blocks/'+gen_fixture.block(12)['hash']+'/txs/cbor?count=100&page=1&order=asc']=unavailable
        now=[1700000000.0]
        with PgStore(conninfo=target) as store:
            self.assertFalse(store.supports_incremental)
            with self.assertRaises(TransientSyncFailure):self.run_driver(store,broken,clock=lambda:now[0])
            pending=copy.deepcopy(store.pending());job=copy.deepcopy(store.sync_job('provider-active'))
            upgrade_store(store)
            self.assertEqual(store.pending(),pending);self.assertEqual(store.sync_job('provider-active'),job)
        now[0]+=86400
        with PgStore(conninfo=target) as store:
            self.run_driver(store,routes,clock=lambda:now[0])
            self.assertEqual(store.sync_job('provider-active')['job_id'],job['job_id'])
            self.assertGreater(store.sync_job('provider-active')['attempts'],job['attempts'])
            self.assertEqual(store.generation()['target_height'],pending['target_height'])
            self.assertNotIn('integrity',store.generation()['manifest'])
            self.compare_reference(routes,store)
            self.assertTrue(store.db.execute('SELECT chain.gate()').fetchone()[0])

    def test_full_oracle_reconstructs_projection_and_old_runtime_refuses(self):
        dsn=self.make()
        with PgStore(conninfo=dsn) as store:
            upgrade_store(store);store.certify_ready_full()
            # Simulate an erroneous privileged internal publication, not normal
            # owner DML (which the other test proves is denied).
            with store.atomic():
                store.db.execute("SET LOCAL db_sync_alt.projection_write='on'")
                store.db.execute('DELETE FROM chain.current_membership WHERE height=%s',(gen_fixture.OFFSET+100,))
        with patch('db_sync_alt.store.database.SUPPORTED_SCHEMA_VERSIONS',(1,)):
            with self.assertRaisesRegex(Refusal,'unsupported_database_identity'):PgStore(conninfo=dsn)
        with PgStore(conninfo=dsn) as store:
            with self.assertRaisesRegex(Refusal,'membership_projection_mismatch'):store.certify_ready_full()
            with self.assertRaisesRegex(Refusal,'parent_uncertified'):store.current_verified_parent()

    def test_full_publication_projection_preserves_prefix_and_rolls_back(self):
        dsn=self.make();routes,_=gen_fixture.fixtures()
        slow=lambda *a,**kw:update(*a,**kw)
        with PgStore(conninfo=dsn) as store:
            with patch.object(store,'publish',side_effect=Refusal('fixture_stop')),self.assertRaises(Exception):
                self.run_driver(store,routes,update_fn=slow)
            pending=store.pending();self.assertIsNotNone(pending)
            upgrade_store(store);before=store.state();old=store.db.execute('SELECT * FROM chain.current_membership ORDER BY height').fetchall()
            gen=pending['id'];manifest=pending['manifest'];execute=store.db.execute
            # A fully verified pending generation cannot gain reuse/delta rows or a new parent.
            with self.assertRaises(psycopg.errors.CheckViolation):
                execute('UPDATE chain.generations SET parent=NULL WHERE id=%s',(gen,))
            with self.assertRaises(psycopg.errors.CheckViolation):
                execute('UPDATE chain.generations SET parent=999 WHERE id=%s',(gen,))
            with store.atomic():
                execute("SET LOCAL db_sync_alt.projection_write='on'")
                execute('DELETE FROM chain.current_membership WHERE height=%s',(old[0][0],))
            with self.assertRaisesRegex(Refusal,'membership_projection_mismatch'):store.publish(gen,manifest)
            with store.atomic():
                execute("SET LOCAL db_sync_alt.projection_write='on'")
                execute('INSERT INTO chain.current_membership VALUES(%s,%s)',old[0])
            def fail_insert(query,*a,**kw):
                if isinstance(query,str) and query.startswith('INSERT INTO chain.current_membership SELECT height,hash'):
                    raise RuntimeError('fixture publication rollback')
                return execute(query,*a,**kw)
            with patch.object(store.db,'execute',side_effect=fail_insert),self.assertRaisesRegex(RuntimeError,'rollback'):
                store.publish(gen,manifest)
            self.assertEqual(store.state(),before);self.assertEqual(store.pending(),pending)
            self.assertEqual(execute('SELECT * FROM chain.current_membership ORDER BY height').fetchall(),old)
            meta=store.meta
            with patch.object(store,'meta',side_effect=lambda key:999 if key=='canonical_ready' else meta(key)),self.assertRaisesRegex(Refusal,'publication_parent_changed'):
                store.publish(gen,manifest)
            store.publish(gen,manifest)
            self.assertEqual(execute('SELECT * FROM chain.current_membership WHERE height<=%s ORDER BY height',(store.generation(0)['target_height'],)).fetchall(),[r for r in old if r[0]<=store.generation(0)['target_height']])
            self.assertEqual(execute('SHOW statement_timeout').fetchone()[0],'30s')
            verify_store(store)

    def test_full_publication_projection_adds_new_pre_start_origin_binding(self):
        from fixtures.cbor import tx
        dsn=self.make();routes,_=gen_fixture.fixtures()
        origin=tx({0:[],1:[[b'a',100]],2:4})
        spend=tx({0:[[bytes.fromhex(origin[1]),0]],1:[],2:3})
        origin_block=gen_fixture.block(7);origin_block.update(tx_count=1,epoch=0)
        routes['/blocks/'+origin_block['hash']]=origin_block
        routes['/blocks/'+str(origin_block['height'])]=origin_block
        for height,(raw,hash_) in ((7,origin),(11,spend)):
            block=origin_block if height==7 else gen_fixture.block(height)
            routes['/blocks/'+block['hash']+'/txs/cbor?count=100&page=1&order=asc']=[{'tx_hash':hash_,'cbor':raw}]
            routes['/txs/'+hash_+'/cbor']={'cbor':raw}
            routes['/txs/'+hash_]={'hash':hash_,'block':block['hash'],'block_height':block['height'],'index':0,'valid_contract':True}
            routes['/txs/'+hash_+'/utxos']={'hash':hash_,'inputs':[] if height==7 else [dict(gen_fixture.out(),tx_hash=origin[1],reference=False,collateral=False)],'outputs':[gen_fixture.out()] if height==7 else []}
        with PgStore(conninfo=dsn) as store:
            upgrade_store(store);old=store.generation();self.assertIsNone(store.block_at(0,gen_fixture.OFFSET+7))
            self.run_driver(store,routes,update_fn=lambda *a,**kw:update(*a,**kw))
            self.assertEqual(store.generation(0),old)
            self.assertEqual(store.db.execute('SELECT hash FROM chain.current_membership WHERE height=%s',(gen_fixture.OFFSET+7,)).fetchone()[0],bytes.fromhex(origin_block['hash']))
            self.compare_reference(routes,store);verify_store(store)

    def test_upgrade_bulk_fk_validation_is_atomic_and_required(self):
        dsn=self.make()
        with PgStore(conninfo=dsn) as store,psycopg.connect(dsn,autocommit=True) as observer:
            state=store.state();execute=store.db.execute
            def corrupt_before_validation(query,*args,**kwargs):
                if isinstance(query,str) and query.startswith('ALTER TABLE chain.current_membership ADD CONSTRAINT'):
                    self.assertIsNone(observer.execute("SELECT to_regclass('chain.current_membership')").fetchone()[0])
                    execute("INSERT INTO chain.current_membership VALUES(1,decode(repeat('ff',32),'hex'))")
                return execute(query,*args,**kwargs)
            with patch.object(store.db,'execute',side_effect=corrupt_before_validation),self.assertRaises(psycopg.errors.ForeignKeyViolation):
                upgrade_store(store)
            self.assertEqual(store.state(),state)
            self.assertIsNone(execute("SELECT to_regclass('chain.current_membership')").fetchone()[0])
            self.assertEqual(execute('SHOW statement_timeout').fetchone()[0],'30s')
            upgrade_store(store)
            self.assertEqual(execute("SELECT convalidated,confrelid='chain.blocks'::regclass FROM pg_constraint WHERE conrelid='chain.current_membership'::regclass AND conname='current_membership_hash_fkey'").fetchone(),(True,True))
            with self.assertRaises(psycopg.errors.ForeignKeyViolation):
                with store.atomic():
                    execute("SET LOCAL db_sync_alt.projection_write='on'")
                    execute("INSERT INTO chain.current_membership VALUES(1,decode(repeat('ff',32),'hex'))")
            store.certify_ready_full()

    def test_upgrade_timeout_is_local_and_cancellation_rolls_back(self):
        dsn=self.make()
        with PgStore(conninfo=dsn) as store:
            before=store.state();generation=store.generation();execute=store.db.execute
            self.assertEqual(execute('SHOW statement_timeout').fetchone()[0],'30s')
            def cancel_projection(query,*args,**kwargs):
                if isinstance(query,str) and query.startswith('INSERT INTO chain.current_membership SELECT'):
                    self.assertEqual(execute('SHOW statement_timeout').fetchone()[0],'2min')
                    execute("SET LOCAL statement_timeout='1ms'")
                    execute('SELECT pg_sleep(0.05)')
                return execute(query,*args,**kwargs)
            with patch.object(store.db,'execute',side_effect=cancel_projection),self.assertRaises(psycopg.errors.QueryCanceled):
                upgrade_store(store)
            self.assertEqual(store.state(),before);self.assertEqual(store.generation(),generation)
            self.assertIsNone(execute("SELECT to_regclass('chain.current_membership')").fetchone()[0])
            self.assertEqual(execute('SHOW statement_timeout').fetchone()[0],'30s')
            upgrade_store(store)
            self.assertEqual(execute('SHOW statement_timeout').fetchone()[0],'30s')
            store.certify_ready_full()

    def test_upgrade_preserves_schema1_data_and_guards_raw_owner_mutations(self):
        dsn=self.make()
        with PgStore(conninfo=dsn) as store:
            before=verify_store(store);old=store.generation();state=store.state()
            result=upgrade_store(store)
            self.assertEqual(result['schema_version'],4)
            self.assertEqual(verify_store(store)['digests'],before['digests'])
            self.assertEqual(store.generation(),old)
            self.assertEqual(store.db.execute('SELECT count(*) FROM public.block').fetchone()[0],before['digests']['blocks']['count'])
            self.assertTrue(store.db.execute('SELECT chain.gate()').fetchone()[0])
            self.assertEqual(store.state()['publication_serial'],state['publication_serial'])
            cap=store.certify_ready_full()
            self.assertIs(store.current_verified_parent(),cap)
            statements=["UPDATE chain.blocks SET tx_count=tx_count+1 WHERE id=(SELECT min(id) FROM chain.blocks)",
                "DELETE FROM chain.current_membership WHERE height=(SELECT min(height) FROM chain.current_membership)",
                "UPDATE chain.service_state SET canonical_ready=NULL WHERE id=1",
                "UPDATE chain.metadata SET value='null'::jsonb WHERE key='profile'",
                "UPDATE chain.generations SET manifest='{}'::jsonb WHERE id=0",
                "UPDATE chain.integrity_state SET revision=revision+1 WHERE id=1",
                "TRUNCATE chain.current_membership", "UPDATE chain.pools SET pool_hash=decode(repeat('ab',28),'hex')"]
            for query in statements:
                with self.subTest(query=query):
                    with self.assertRaises(psycopg.errors.CheckViolation):store.db.execute(query)
            self.assertEqual(verify_store(store)['digests'],before['digests'])
            self.assertIs(store.current_verified_parent(),cap)
            revision=store.db.execute('SELECT revision FROM chain.integrity_state').fetchone()[0]
            store.db.execute("INSERT INTO chain.sync_jobs VALUES('fixture-account','{\"attempts\":3}'::jsonb)")
            self.assertEqual(store.db.execute('SELECT revision FROM chain.integrity_state').fetchone()[0],revision)
            self.assertIs(store.current_verified_parent(),cap)
        with PgStore(conninfo=dsn) as fresh:
            with self.assertRaises(Refusal):fresh.current_verified_parent()
            fresh.certify_ready_full()

if __name__=='__main__':unittest.main(verbosity=2)
