"""Real SQL acquisition of a complete synthetic Mainnet prefix, then real CBOR."""
import copy
import datetime
import hashlib
import os
from pathlib import Path
import tempfile
import uuid
import psycopg
from psycopg import sql

from db_sync_alt.contract import Profile
from db_sync_alt.resources import profile_dir
from db_sync_alt.sources.blockfrost.relevance import address_bytes,address_text
from db_sync_alt.sources.blockfrost.adapter import OPENAPI
from db_sync_alt.sources.dbsync.acquire import CountedSQLSource,acquire
from db_sync_alt.store.database import PgStore
from db_sync_alt.store.upgrade import upgrade_store
from fixtures.cbor import tx
from fixtures.blockfrost import CountingProvider,Fake,FixtureHooks

PROFILE=Profile(profile_dir('mainnet'));S=PROFILE.start[0];K=PROFILE.security
FUTURE=18
H=lambda n:hashlib.sha256(str(n).encode()).digest()
P=bytes.fromhex(PROFILE.night[:56]);N=bytes.fromhex(PROFILE.night[56:]);ADDRESS=address_bytes(PROFILE.mapping,'mainnet')
OTHER=b'\x61'+b'o'*28

def make_tx(fee,*,inputs=(),relevant=False,mint=0,valid=True,collateral=(),reference=(),returned=None):
    output=[ADDRESS if relevant else OTHER,[100,{P:{N:3}}] if relevant else 100]
    body={0:[[h,i] for h,i in inputs],1:[output],2:fee}
    if mint:body[9]={P:{N:mint}}
    if collateral:body[13]=[[h,i] for h,i in collateral]
    if reference:body[18]=[[h,i] for h,i in reference]
    if returned is not None:body[16]=returned
    raw,h=tx(body,valid)
    return {'cbor':raw,'tx_hash':h,'body':body,'valid':valid}

PRE=make_tx(1,relevant=True);PRE_SPEND=make_tx(2,inputs=[(bytes.fromhex(PRE['tx_hash']),0)],relevant=True);ROOT=make_tx(3,relevant=True)

def output(transaction,index=0):
    from db_sync_alt.sources.blockfrost.cbor import decode_transaction
    d=decode_transaction(transaction['cbor'],transaction['tx_hash'])
    value=(list(d['outputs'])+[d['collateral_output']])[index]
    return {'output_index':index,'address':address_text(value['address'],'mainnet'),'amount':[{'unit':'lovelace','quantity':value['value']}]+[{'unit':p.hex()+n.hex(),'quantity':str(q)} for p,n,q in value['assets']],'data_hash':None,**({'collateral':True} if index==d['output_count'] else {})}

class Provider(CountingProvider):
    def get_optional(self,path):return self.get(path)
    def drain(self):
        value=super().drain();value.update(shared_profile='mainnet',adapter=PROFILE.network.adapter,openapi=OPENAPI);return value

class SQLFixture:
    @classmethod
    def setup(cls):
        cls.admin=psycopg.connect(os.environ['DB_SYNC_ALT_TEST_DSN'],autocommit=True);cls.names=[]
        cls.tmp=tempfile.TemporaryDirectory();cls.services=Path(cls.tmp.name)/'pg_service.conf';cls.old=os.environ.get('PGSERVICEFILE');os.environ['PGSERVICEFILE']=str(cls.services)
        cls.source_dsn,cls.source_name=cls.database();cls.source=psycopg.connect(cls.source_dsn,autocommit=True)
        c=cls.source;c.execute((Path(__file__).parent/'data/dbsync_schema.sql').read_text())
        c.execute("ALTER TABLE block ADD COLUMN previous_id bigint;ALTER TABLE tx_out ADD COLUMN inline_datum_id bigint;CREATE TABLE schema_version(stage_one bigint NOT NULL,stage_two bigint NOT NULL,stage_three bigint NOT NULL);INSERT INTO schema_version VALUES(15,48,6);CREATE TABLE epoch_stake_progress(epoch_no word31type NOT NULL,completed boolean NOT NULL);CREATE TABLE meta(network_name varchar NOT NULL);INSERT INTO meta VALUES('mainnet')")
        with c.transaction():
            for h,e in [(S-100,273),(S-50,274),(S-1,275)]+[(h,275) for h in range(S,S+K+FUTURE+1)]:
                c.execute("INSERT INTO block(id,hash,block_no,epoch_no,slot_no,time,tx_count,previous_id) VALUES(%s,%s,%s,%s,%s,'2026-01-01',%s,%s)",(h,H(h),h,e,h,10 if h<=S else 0,h-1))
            for ident,t,h,index in [(1,PRE,S-1,0),(2,PRE_SPEND,S-1,1),(3,ROOT,S,PROFILE.start[1])]:c.execute('INSERT INTO tx VALUES(%s,%s,%s,%s,true)',(ident,bytes.fromhex(t['tx_hash']),h,index))
            c.execute('INSERT INTO multi_asset VALUES(1,%s,%s)',(P,N))
            for ident,transaction in [(1,1),(2,2),(3,3)]:
                c.execute('INSERT INTO tx_out(id,tx_id,index,address,value,data_hash) VALUES(%s,%s,0,%s,100,NULL)',(ident,transaction,PROFILE.mapping))
                c.execute('INSERT INTO ma_tx_out VALUES(%s,3,%s,1)',(ident,ident))
            c.execute('INSERT INTO tx_in VALUES(1,2,1,0)');c.execute('INSERT INTO pool_hash VALUES(1,%s)',(b'p'*28,))
            for e,h in [(273,S-100),(274,S-50),(275,S-1)]:
                c.execute('INSERT INTO epoch_stake_progress VALUES(%s,true)',(e,));c.execute('INSERT INTO epoch_param VALUES(%s,%s)',(e,H('epoch'+str(e))));c.execute('INSERT INTO epoch_stake VALUES(%s,1,123,%s)',(e,e))
    @classmethod
    def database(cls):
        name='relevance_'+uuid.uuid4().hex[:12];cls.names.append(name);cls.admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
        dsn=psycopg.conninfo.make_conninfo(os.environ['DB_SYNC_ALT_TEST_DSN'],dbname=name)
        with cls.services.open('a') as out:out.write('['+name+']\n'+''.join(k+'='+v+'\n' for k,v in psycopg.conninfo.conninfo_to_dict(dsn).items())+'\n')
        return dsn,name
    @classmethod
    def teardown(cls):
        cls.source.close()
        for name in cls.names:cls.admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(name)))
        cls.admin.close()
        if cls.old is None:os.environ.pop('PGSERVICEFILE',None)
        else:os.environ['PGSERVICEFILE']=cls.old
        cls.tmp.cleanup()
    @classmethod
    def prepare(cls,transactions):
        by_height={S+i+1:rows for i,rows in enumerate(transactions)}
        cls.source.execute('UPDATE block SET tx_count=0 WHERE block_no>%s',(S,))
        for h,rows in by_height.items():cls.source.execute('UPDATE block SET tx_count=%s WHERE block_no=%s',(len(rows),h))
        dsn,_=cls.database();store=PgStore(conninfo=dsn,create=True,network='mainnet')
        acquire(CountedSQLSource(cls.source_name,PROFILE,{'dbsync_version':'13.7.1.0','tx_in_retained':True,'epoch_stake_retained':True,'operator_attested':True}),store,PROFILE,target_height=S,window_blocks=10000)
        from fixtures.sql_prerequisites import assert_v2_root
        assert_v2_root(store)
        upgrade_store(store);store.certify_ready_full()
        from db_sync_alt.sources.blockfrost.relevance import qualified_parent
        assert qualified_parent(store,PROFILE) is not None
        routes={'/metrics':[],'/genesis':{'network_magic':PROFILE.network.magic}}
        time=int(datetime.datetime(2026,1,1,tzinfo=datetime.timezone.utc).timestamp())
        def block(h):return {'hash':H(h).hex(),'height':h,'previous_block':H(h-1).hex(),'epoch':273 if h==S-100 else 274 if h==S-50 else 275,'slot':h,'time':time,'tx_count':10 if h<=S else len(by_height.get(h,[]))}
        for h in [S-100,S-50,S-1]+list(range(S,S+K+FUTURE+1)):
            row=block(h);routes['/blocks/'+str(h)]=row;routes['/blocks/'+row['hash']]=row
            routes['/blocks/'+row['hash']+'/next?count=100&page=1']=[block(n) for n in range(h+1,min(h+101,S+K+FUTURE+1))]
        routes['/blocks/latest']=block(S+K+len(transactions))
        all_tx={t['tx_hash']:t for t in [PRE,PRE_SPEND,ROOT]+[t for rows in transactions for t in rows]}
        positions=[(S-1,0,PRE),(S-1,1,PRE_SPEND),(S,PROFILE.start[1],ROOT)]+[(h,i,t) for h,rows in by_height.items() for i,t in enumerate(rows)]
        for h,index,t in positions:
            key=t['tx_hash'];routes['/txs/'+key+'/cbor']={'cbor':t['cbor']};routes['/txs/'+key]={'hash':key,'block':H(h).hex(),'block_height':h,'index':index,'valid_contract':t['valid']}
            inputs=[]
            for role,field in [('ordinary',0),('collateral',13),('reference',18)]:
                for origin,oi in t['body'].get(field,[]):
                    item=output(all_tx[origin.hex()],oi);item.update(tx_hash=origin.hex(),collateral=role=='collateral',reference=role=='reference');inputs.append(item)
            outputs=[output(t,i) for i in range(len(t['body'][1]))]
            if 16 in t['body']:outputs.append(output(t,len(outputs)))
            routes['/txs/'+key+'/utxos']={'hash':key,'inputs':inputs,'outputs':outputs}
        for h,rows in by_height.items():
            for page in range(1,len(rows)//100+2):routes['/blocks/'+H(h).hex()+'/txs/cbor?count=100&page='+str(page)+'&order=asc']=[{'tx_hash':t['tx_hash'],'cbor':t['cbor']} for t in rows[(page-1)*100:page*100]]
        return store,routes
    @staticmethod
    def follow(store,routes,target,hooks=None):
        from db_sync_alt.sources.blockfrost.follow import update_follow
        provider=Provider(50000,Fake(copy.deepcopy(routes)))
        result=update_follow(store,provider,PROFILE,target,hooks=hooks or FixtureHooks())
        return result,provider.client.calls
