"""Upstream migration catalogs and representative rows for both supported sources."""
import copy
import contextlib
import io
import hashlib
import json
import os
from pathlib import Path
import unittest
import uuid

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb
from db_sync_alt.contract import Profile, Refusal
from db_sync_alt.sources.dbsync.acquire import CountedSQLSource, acquire
from db_sync_alt.store.database import PgStore
from db_sync_alt.verify.store import verify_store
from db_sync_alt.verify.completion import validate_completion
from integration.test_network_profiles import Databases

ROOT=Path(__file__).resolve().parents[2]
H=lambda n:hashlib.sha256(str(n).encode()).digest()

def apply_upstream(db,version):
    fixture=json.loads((ROOT/'tests/fixtures/data/upstream-dbsync-migrations.json').read_text())[version]
    for item in fixture['files']:
        assert hashlib.sha256(item['sql'].encode()).hexdigest()==item['sha256']
        db.execute(item['sql'])
    expected=(16,52,6) if version=='13.8.0.0' else (15,48,6)
    assert tuple(db.execute('SELECT stage_one,stage_two,stage_three FROM schema_version').fetchone())==expected
    if version=='13.8.0.0':
        assert db.execute("SELECT relkind FROM pg_class WHERE oid='public.epoch'::regclass").fetchone()[0]=='v'
        assert db.execute("SELECT to_regclass('public.epoch_finalized') IS NOT NULL").fetchone()[0]

_INSERT_COLUMNS={}

def insert(db,table,values):
    """Supply valid defaults for unrelated upstream non-null fields; source-query values explicit."""
    row=dict(values)
    key=(db.info.dbname,table)
    if key not in _INSERT_COLUMNS:
        _INSERT_COLUMNS[key]=db.execute("SELECT column_name,udt_name,column_default FROM information_schema.columns "
                       "WHERE table_schema='public' AND table_name=%s AND is_nullable='NO'",(table,)).fetchall()
    cols=_INSERT_COLUMNS[key]
    for name,typ,default in cols:
        if name in row or default is not None:continue
        if typ in ('int2','int4','int8','numeric','float4','float8'):row[name]=0
        elif typ=='bool':row[name]=False
        elif typ in ('text','varchar'):row[name]='fixture'
        elif typ=='timestamp':row[name]='2026-01-01'
        else:raise AssertionError((table,name,typ))
    db.execute(sql.SQL('INSERT INTO {} ({}) VALUES ({})').format(sql.Identifier(table),
               sql.SQL(',').join(map(sql.Identifier,row)),sql.SQL(',').join(sql.Placeholder() for _ in row)),list(row.values()))

def seed(db,profile,creating_version='13.6.0.5'):
    s=profile.start[0];target=s+3;w=target+profile.security;epoch=617
    insert(db,'meta',dict(network_name=profile.network.dbsync_network_name,version=creating_version,start_time='2020-01-01'))
    insert(db,'slot_leader',dict(id=1,hash=b'l'*28,description='fixture'))
    with db.transaction():
        for h in range(s-100,w+1):
            e=epoch-2 if h<s-50 else epoch-1 if h<s else epoch
            insert(db,'block',dict(id=h,hash=H(h),block_no=h,previous_id=h-1 if h>s-100 else None,
                                  epoch_no=e,slot_no=h,time='2026-01-01',tx_count=10,slot_leader_id=1))
        for n,h,i in [(1,s-1,0),(2,s-1,1),(3,s,profile.start[1]),(4,s+1,0),(5,s+2,0),(6,s+3,0),(7,s+4,0)]:
            insert(db,'tx',dict(id=n,hash=H('tx'+str(n)),block_id=h,block_index=i,valid_contract=True))
        insert(db,'multi_asset',dict(id=1,policy=bytes.fromhex(profile.night[:56]),name=bytes.fromhex(profile.night[56:]),fingerprint='asset_fixture'))
        insert(db,'datum',dict(id=1,hash=H('datum'),value=Jsonb({'int':7}),bytes=b'\x07',tx_id=2))
        for oid,tx,address,datum in [(1,1,'old-token',None),(2,2,profile.mapping,H('datum')),(3,3,'new-token',None)]:
            insert(db,'tx_out',dict(id=oid,tx_id=tx,index=0,address=address,value=2000000,data_hash=datum,address_has_script=False))
        for n,o in [(1,1),(2,3)]:insert(db,'ma_tx_out',dict(id=n,quantity=1000,tx_out_id=o,ident=1))
        for n,t,o in [(1,4,1),(2,5,2),(3,7,3)]:insert(db,'tx_in',dict(id=n,tx_in_id=t,tx_out_id=o,tx_out_index=0))
        insert(db,'ma_tx_mint',dict(id=1,quantity=-7,tx_id=6,ident=1))
        insert(db,'pool_hash',dict(id=1,hash_raw=b'p'*28,view='pool_fixture'))
        insert(db,'stake_address',dict(id=1,hash_raw=b's'*29,view='stake1_fixture'))
        for e in range(epoch-2,epoch+1):
            insert(db,'epoch_param',dict(epoch_no=e,nonce=H('epoch'+str(e)),block_id=s))
            insert(db,'epoch_stake_progress',dict(epoch_no=e,completed=True))
            insert(db,'epoch_stake',dict(id=e,epoch_no=e,pool_id=1,addr_id=1,amount=123456))
        insert(db,'extra_migrations',dict(token='StakeDistrEnded'))
    return target

class SourceVersions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dbs=Databases();cls.profile=Profile(ROOT/'profiles/mainnet-node-1.0.0');cls.templates={}
        for version in ['13.7.1.0','13.8.0.0']:
            dsn,alias=cls.dbs.create()
            with psycopg.connect(dsn,autocommit=True) as db:apply_upstream(db,version);cls.target=seed(db,cls.profile)
            cls.templates[version]=(dsn,alias)
    @classmethod
    def tearDownClass(cls):cls.dbs.close()
    def source(self,version='13.8.0.0',alias=None,attestation=None):
        a={'dbsync_version':version,'tx_in_retained':True,'epoch_stake_retained':True,'operator_attested':True}
        if attestation is not None:a=attestation
        return CountedSQLSource(alias or self.templates[version][1],self.profile,a)
    def cloned_source(self):
        _,template=self.templates['13.8.0.0'];name='db_sync_alt_test_net_'+uuid.uuid4().hex[:10]
        self.dbs.names.append(name);self.dbs.admin.execute(sql.SQL('CREATE DATABASE {} TEMPLATE {}').format(sql.Identifier(name),sql.Identifier(template)))
        dsn=psycopg.conninfo.make_conninfo(os.environ['DB_SYNC_ALT_TEST_DSN'],dbname=name)
        with self.dbs.servicefile.open('a') as out:out.write('['+name+']\n'+''.join(k+'='+v+'\n' for k,v in psycopg.conninfo.conninfo_to_dict(dsn).items())+'\n')
        return dsn,name
    def test_upstream_13_8_acquire_independent_verification_ordinals_and_real_provenance(self):
        results=[]
        for version,stages in [('13.7.1.0',[15,48,6]),('13.8.0.0',[16,52,6])]:
            dsn,_=self.dbs.create();source=self.source(version)
            with PgStore(conninfo=dsn,create=True,network='mainnet') as store:
                result=acquire(source,store,self.profile,target_height=self.target,window_blocks=10000)
                verify_store(store);g=store.generation();validate_completion(store,g,result['digests'],require_seal=True)
                identity=g['manifest']['source_identity'];self.assertEqual(identity['schema'],stages);self.assertEqual(identity['dbsync_version'],version)
                anchors={k:g['manifest']['anchors'][k] for k in ['start','target']}
                inputs={(r['tx_hash'],r['origin_hash'],r['origin_index']):store.ordinal('inputs',r) for r in store.rows('inputs',g['id'])}
                assets={(r['tx_hash'],r['output_index'],r['policy'],r['name']):store.ordinal('output_assets',r) for r in store.rows('output_assets',g['id'])}
                self.assertEqual(source.ordinals(sorted(inputs),sorted(assets),anchors),{'inputs':inputs,'output_assets':assets})
                self.assertGreater(result['digests']['inputs']['count'],0);self.assertGreater(result['digests']['epoch_stakes']['count'],0)
                results.append(result['digests'])
        self.assertEqual(results[0],results[1])
    def test_modes_schema_types_network_and_epoch_gaps_fail_before_publication(self):
        cases=[
            ('unsupported_source_schema','UPDATE schema_version SET stage_one=15,stage_two=44'),
            ('unsupported_source_schema','UPDATE schema_version SET stage_three=0'),
            ('unsupported_source_schema','UPDATE schema_version SET stage_two=999'),
            ('source_network_mismatch',"UPDATE meta SET network_name='preprod'"),
            ('source_columns_missing','ALTER TABLE tx_out DROP COLUMN address CASCADE'),
            ('unsupported_source_column_type','ALTER TABLE tx_in ALTER COLUMN tx_out_index TYPE integer'),
            ('unsupported_source_column_type','ALTER TABLE tx ALTER COLUMN block_id DROP NOT NULL'),
            ('epoch_stake_not_complete','UPDATE epoch_stake_progress SET completed=false WHERE epoch_no=615'),
            ('epoch_stake_unavailable','DELETE FROM epoch_stake WHERE epoch_no=615'),
            ('epoch_nonce_unavailable','UPDATE epoch_param SET nonce=NULL WHERE epoch_no=615'),
        ]
        for token in ['ConsumeTxOutPreviouslySet','PruneTxOutFlagPreviouslySet','BootstrapStarted','BootstrapFinished','TxOutAddressPreviouslySet','UnknownFutureMode']:
            cases.append(('unsupported_source_mode',"INSERT INTO extra_migrations(token) VALUES('"+token+"')"))
        for expected,mutation in cases:
            with self.subTest(mutation=mutation):
                existing=tuple(self.dbs.names)
                try:
                    dsn,alias=self.cloned_source()
                    with psycopg.connect(dsn,autocommit=True) as db:db.execute(mutation)
                    dest,_=self.dbs.create()
                    with PgStore(conninfo=dest,create=True,network='mainnet') as store:
                        with self.assertRaisesRegex(Refusal,expected):acquire(self.source(alias=alias),store,self.profile,target_height=self.target)
                        self.assertIsNone(store.meta('canonical_ready'))
                finally:
                    # Connections above have closed. Preserve both pristine
                    # version templates; retire only this subcase's owned DBs
                    # rather than accumulating 32 migration-catalog clones.
                    for name in reversed(self.dbs.names[len(existing):]):
                        self.assertTrue(name.startswith('db_sync_alt_test_net_'))
                        self.assertNotIn(name,{alias for _,alias in self.templates.values()})
                        self.dbs.admin.execute(sql.SQL('DROP DATABASE IF EXISTS {} WITH (FORCE)').format(sql.Identifier(name)))
                        self.dbs.names.remove(name)
                    self.assertEqual(tuple(self.dbs.names),existing)
    def test_version_and_retention_attestations_cannot_mislabel_13_8(self):
        good={'dbsync_version':'13.8.0.0','tx_in_retained':True,'epoch_stake_retained':True,'operator_attested':True}
        for field,value,code in [('dbsync_version','13.7.1.0','source_version_attestation_mismatch'),('tx_in_retained',False,'retention_attestation_required'),('epoch_stake_retained',False,'retention_attestation_required'),('operator_attested',False,'retention_evidence_required')]:
            with self.subTest(field=field):
                a=copy.deepcopy(good);a[field]=value;source=self.source(attestation=a)
                with source.snapshot():
                    with self.assertRaisesRegex(Refusal,code):source.preflight(self.target)
    def test_ordinal_path_rejects_bad_schema_mode_or_attestation(self):
        for mutation,expected in [("INSERT INTO extra_migrations(token) VALUES('BootstrapFinished')",'unsupported_source_mode'),('UPDATE schema_version SET stage_three=0','unsupported_source_schema')]:
            dsn,alias=self.cloned_source()
            with psycopg.connect(dsn,autocommit=True) as db:db.execute(mutation)
            with self.assertRaisesRegex(Refusal,expected):self.source(alias=alias).ordinals([],[],{})
        with self.assertRaisesRegex(Refusal,'source_version_attestation_mismatch'):
            self.source(attestation={'dbsync_version':'13.7.1.0'}).ordinals([],[],{})

    def test_schema1_2_3_local_upgrades_preserve_facts_without_external_reads(self):
        from db_sync_alt.store import upgrade
        class UnusedSource:
            def ordinals(self,*args):raise AssertionError('local upgrade read external source')
            @property
            def validated_identity(self):raise AssertionError('unused source provenance was requested')
        for initial_schema in (1,2,3):
            with self.subTest(initial_schema=initial_schema):
                dsn,_=self.dbs.create()
                with PgStore(conninfo=dsn,create=True,network='mainnet') as store:
                    acquire(self.source(),store,self.profile,target_height=self.target,window_blocks=10000)
                    before=store.digests(store.generation()['id']);profile=store.meta('profile')
                    if initial_schema>=2:upgrade._upgrade_v2(store,store.state())
                    if initial_schema==3:upgrade._upgrade_v3(store,store.state(),None)
                    self.assertEqual(store.state()['schema_version'],initial_schema)
                    result=upgrade.upgrade_store(store,UnusedSource())
                    self.assertEqual(result['schema_version'],4)
                    self.assertEqual(result['base_generation'],store.integrity()['base'])
                    self.assertIn('checkpoint_generation',result)
                    self.assertNotIn('ordinal_source_identity',result)
                    self.assertEqual(store.digests(store.generation()['id']),before)
                    self.assertEqual(store.meta('profile'),profile)
                    verify_store(store)
                    self.assertEqual(upgrade.upgrade_store(store),{'schema_version':4,'changed':False})

    def test_upgrade_cli_recovers_missing_ordinals_from_attested_13_8_source(self):
        from db_sync_alt.store import upgrade
        dsn,alias=self.dbs.create()
        with PgStore(conninfo=dsn,create=True,network='mainnet') as store:
            acquire(self.source(),store,self.profile,target_height=self.target,window_blocks=10000)
            upgrade._upgrade_v2(store,store.state())
            for table in ('inputs','output_assets'):store.db.execute('ALTER TABLE chain.'+table+' DROP COLUMN ordinal')
        att=Path(self.dbs.tmp.name)/'upgrade-attestation.json'
        att.write_text(json.dumps(dict(dbsync_version='13.8.0.0',tx_in_retained=True,epoch_stake_retained=True,operator_attested=True)))
        out=io.StringIO()
        with contextlib.redirect_stdout(out):
            upgrade.main(['--service',alias,'--source-service',self.templates['13.8.0.0'][1],'--source-attestation',str(att)])
        result=json.loads(out.getvalue());self.assertEqual(result['schema_version'],4)
        self.assertIn('checkpoint_generation',result)
        self.assertGreater(result['ordinals_from_source']['inputs'],0)
        self.assertEqual(result['ordinal_source_identity']['schema'],[16,52,6])
        self.assertEqual(result['ordinal_source_identity']['dbsync_version'],'13.8.0.0')
        with PgStore(conninfo=dsn) as store:
            verify_store(store)
            self.assertEqual(store.generation()['manifest']['source_identity']['schema'],[16,52,6])
