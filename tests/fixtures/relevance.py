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
from db_sync_alt.sources.blockfrost.relevance import address_bytes,address_text,_encode_address
from db_sync_alt.sources.blockfrost.adapter import OPENAPI
from db_sync_alt.sources.dbsync.acquire import CountedSQLSource,acquire
from db_sync_alt.store.database import PgStore
from db_sync_alt.store.upgrade import upgrade_store
from fixtures.cbor import tx
from fixtures.blockfrost import CountingProvider,Fake,FixtureHooks

PROFILE=Profile(profile_dir('mainnet'));S=PROFILE.start[0];T=S or 1;K=PROFILE.security
FUTURE=18
H=lambda n:hashlib.sha256(str(n).encode()).digest()
P=bytes.fromhex(PROFILE.night[:56]);N=bytes.fromhex(PROFILE.night[56:]);ADDRESS=address_bytes(PROFILE.mapping,'mainnet')
OTHER=b'\x61'+b'o'*28

def make_tx(fee,*,inputs=(),relevant=False,mint=0,valid=True,collateral=(),reference=(),returned=None,outputs=None):
    output=[ADDRESS if relevant else OTHER,[100,{P:{N:3}}] if relevant else 100]
    body={0:[[h,i] for h,i in inputs],1:[output] if outputs is None else outputs,2:fee}
    if mint:body[9]={P:{N:mint}}
    if collateral:body[13]=[[h,i] for h,i in collateral]
    if reference:body[18]=[[h,i] for h,i in reference]
    if returned is not None:body[16]=returned
    raw,h=tx(body,valid)
    return {'cbor':raw,'tx_hash':h,'body':body,'valid':valid}

PRE=make_tx(1,relevant=True);PRE_SPEND=make_tx(2,inputs=[(bytes.fromhex(PRE['tx_hash']),0)],relevant=True);ROOT=make_tx(3,relevant=True);BASE_SPEND=make_tx(4,inputs=[(bytes.fromhex(ROOT['tx_hash']),0)],relevant=True)

def configure(network):
    global PROFILE,S,T,K,P,N,ADDRESS,OTHER,PRE,PRE_SPEND,ROOT,BASE_SPEND
    PROFILE=Profile(profile_dir(network));S=PROFILE.start[0];T=S or 1;K=PROFILE.security
    P=bytes.fromhex(PROFILE.night[:56]);N=bytes.fromhex(PROFILE.night[56:])
    ADDRESS=address_bytes(PROFILE.mapping,network);OTHER=bytes([0x61 if network=='mainnet' else 0x60])+b'o'*28
    PRE=make_tx(1,relevant=True);PRE_SPEND=make_tx(2,inputs=[(bytes.fromhex(PRE['tx_hash']),0)],relevant=True);ROOT=make_tx(3,relevant=True);BASE_SPEND=make_tx(4,inputs=[(bytes.fromhex(ROOT['tx_hash']),0)],relevant=True)

def ledger_address(raw):
    """The bytes the ledger keeps for a well-formed output address `raw` (cardano-ledger Address.hs, protocol
    version 9 and later): a pointer address (header 4 or 5) with its three variable-length integers
    re-serialised minimally. A test oracle, written apart from the code under test."""
    if raw[0]>>4 not in (4,5):return raw
    values=[];value=0
    for byte in raw[29:]:
        value=value<<7|byte&127
        if not byte&128:values.append(value);value=0
    assert len(values)==3 and not raw[-1]&128
    def minimal(v):
        groups=[v&127];v>>=7
        while v:groups.insert(0,v&127|128);v>>=7
        return bytes(groups)
    return raw[:29]+b''.join(minimal(v) for v in values)

def output(transaction,index=0):
    """Output `index` of `transaction` (its collateral return at the number of body outputs) as
    cardano-db-sync stores it in tx_out and Blockfrost renders it: the address the ledger keeps (a pointer
    re-serialised minimally), every asset."""
    from db_sync_alt.sources.blockfrost.cbor import decode_transaction
    d=decode_transaction(transaction['cbor'],transaction['tx_hash'])
    value=(list(d['outputs'])+[d['collateral_output']])[index]
    network=PROFILE.network.network;raw=ledger_address(value['address'])
    return {'output_index':index,'address':address_text(raw,network) or _encode_address(raw,network),
            'amount':[{'unit':'lovelace','quantity':value['value']}]+[{'unit':p.hex()+n.hex(),'quantity':str(q)} for p,n,q in value['assets']],
            'data_hash':None,'inline_datum':None,'reference_script_hash':None}

def utxos(transaction,known):
    """Blockfrost's `/txs/{hash}/utxos` answer, as blockfrost-backend-ryo renders cardano-db-sync 13.8's rows
    (confirmed for a valid transaction by a real Preprod answer). `known` maps hashes to transactions.

    A valid transaction: its ordinary and reference inputs with every asset, each collateral input again as a
    `collateral` row with lovelace only (collateral_tx_in), its body outputs, and its collateral return as a
    `collateral` row with lovelace only (collateral_tx_out). A phase-2-invalid transaction: cardano-db-sync
    stores only its collateral inputs (tx_in) and its collateral return (tx_out, at the number of body
    outputs), listed like ordinary rows with every asset."""
    body=transaction['body'];count=len(body[1])
    def lovelace(row):return {**row,'amount':row['amount'][:1]}
    def spent(origin,index,role):
        row=dict(output(known[origin.hex()],index),tx_hash=origin.hex(),collateral=role=='collateral',reference=role=='reference')
        return lovelace(row) if role=='collateral' else row
    if transaction['valid']:
        inputs=[spent(o,i,role) for role,field in (('ordinary',0),('reference',18),('collateral',13)) for o,i in body.get(field,[])]
        outputs=[dict(output(transaction,i),collateral=False) for i in range(count)]
        if 16 in body:outputs.append(dict(lovelace(output(transaction,count)),collateral=True))
    else:
        inputs=[spent(o,i,'ordinary') for o,i in body.get(13,[])]
        outputs=[dict(output(transaction,count),collateral=False)] if 16 in body else []
    return {'hash':transaction['tx_hash'],'inputs':inputs,'outputs':outputs}

class Provider(CountingProvider):
    def get_optional(self,path):return self.get(path)
    def drain(self):
        value=super().drain();value.update(shared_profile=PROFILE.network.network,adapter=PROFILE.network.adapter,openapi=OPENAPI);return value

class SQLFixture:
    @classmethod
    def setup(cls):
        cls.admin=psycopg.connect(os.environ['DB_SYNC_ALT_TEST_DSN'],autocommit=True);cls.names=[];cls.templates={}
        cls.tmp=tempfile.TemporaryDirectory();cls.services=Path(cls.tmp.name)/'pg_service.conf';cls.old=os.environ.get('PGSERVICEFILE');os.environ['PGSERVICEFILE']=str(cls.services)
        cls.source_dsn,cls.source_name=cls.database();cls.source=psycopg.connect(cls.source_dsn,autocommit=True)
        c=cls.source;c.execute((Path(__file__).parent/'data/dbsync_schema.sql').read_text())
        c.execute("ALTER TABLE block ADD COLUMN previous_id bigint;ALTER TABLE tx_out ADD COLUMN inline_datum_id bigint;CREATE TABLE schema_version(stage_one bigint NOT NULL,stage_two bigint NOT NULL,stage_three bigint NOT NULL);INSERT INTO schema_version VALUES(15,48,6);CREATE TABLE epoch_stake_progress(epoch_no word31type NOT NULL,completed boolean NOT NULL);CREATE TABLE meta(network_name varchar NOT NULL);INSERT INTO meta VALUES('"+PROFILE.network.network+"')")
        with c.transaction():
            for h,e in ([(S-100,273),(S-50,274),(S-1,275)] if S else [])+[(h,275 if S else 0) for h in range(S,T+K+FUTURE+1)]:
                c.execute("INSERT INTO block(id,hash,block_no,epoch_no,slot_no,time,tx_count,previous_id) VALUES(%s,%s,%s,%s,%s,'2026-01-01',%s,%s)",(h,H(h),h,e,h,10 if h<=T else 0,h-1 if h else None))
            for ident,t,h,index in ([(1,PRE,S-1,0),(2,PRE_SPEND,S-1,1)] if S else [])+[(3,ROOT,S,PROFILE.start[1])]+([] if S else [(4,BASE_SPEND,T,0)]):c.execute('INSERT INTO tx VALUES(%s,%s,%s,%s,true)',(ident,bytes.fromhex(t['tx_hash']),h,index))
            c.execute('INSERT INTO multi_asset VALUES(1,%s,%s)',(P,N))
            for ident,transaction in ([(1,1),(2,2)] if S else [])+[(3,3)]+([] if S else [(4,4)]):
                c.execute('INSERT INTO tx_out(id,tx_id,index,address,value,data_hash) VALUES(%s,%s,0,%s,100,NULL)',(ident,transaction,PROFILE.mapping))
                c.execute('INSERT INTO ma_tx_out VALUES(%s,3,%s,1)',(ident,ident))
            if S:c.execute('INSERT INTO tx_in VALUES(1,2,1,0)')
            else:c.execute('INSERT INTO tx_in VALUES(2,4,3,0)')
            c.execute('INSERT INTO pool_hash VALUES(1,%s)',(b'p'*28,))
            for e,h in ([(273,S-100),(274,S-50),(275,S-1)] if S else [(0,0)]):
                c.execute('INSERT INTO epoch_stake_progress VALUES(%s,true)',(e,));c.execute('INSERT INTO epoch_param VALUES(%s,%s,%s)',(e,H('epoch'+str(e)),12000+e));c.execute('INSERT INTO epoch_stake VALUES(%s,1,123,%s)',(e,e))
    @classmethod
    def database(cls,template=None):
        name='relevance_'+uuid.uuid4().hex[:12];cls.names.append(name);cls.admin.execute(sql.SQL('CREATE DATABASE {}'+(' TEMPLATE {}' if template else '')).format(sql.Identifier(name),*([sql.Identifier(template)] if template else [])))
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
    def prepare(cls,transactions,*,reuse_root=True,root_version=2,history=()):
        """A store holding the SQL root, and Blockfrost routes for `transactions` (one list per block after
        the root) and for the root's transactions; `history` adds (height, index, transaction) of other
        transactions the SQL source holds."""
        if type(root_version) is not int or root_version not in (1,2):raise ValueError("unsupported fixture root version")
        by_height={T+i+1:rows for i,rows in enumerate(transactions)}
        cls.source.execute('UPDATE block SET tx_count=0 WHERE block_no>%s',(T,))
        for h,rows in by_height.items():cls.source.execute('UPDATE block SET tx_count=%s WHERE block_no=%s',(len(rows),h))
        shape=(PROFILE.network.network,root_version,tuple((h,len(rows)) for h,rows in by_height.items()))
        template=cls.templates.get(shape) if reuse_root else None
        dsn,name=cls.database(template=template);store=PgStore(conninfo=dsn,create=not template,network=PROFILE.network.network)
        if not template:
            root_acquire=acquire
            if root_version==1:
                control=Path(__file__).parent/'sql_acquire_v1.py'
                assert hashlib.sha256(control.read_bytes()).hexdigest()=='95385711f0b82f44ee2aa493375cf7c5f6de947f26880a27ac6f67eb137265f8'
                from fixtures.sql_acquire_v1 import acquire as root_acquire
            root_acquire(CountedSQLSource(cls.source_name,PROFILE,{'dbsync_version':'13.7.1.0','tx_in_retained':True,'epoch_stake_retained':True,'operator_attested':True}),store,PROFILE,target_height=T,window_blocks=10000)
            from db_sync_alt.verify.completion import validate_completion,SQL_ADAPTER,SQL_VALIDATION
            root=store.generation()
            assert root['source']=='cardano-db-sync-postgresql' and root['parent'] is None
            assert root['manifest']['acquisition']['version']==root_version
            from db_sync_alt.verify.prerequisites import prerequisite_coverage
            coverage=prerequisite_coverage(store,root)
            assert coverage['complete'] is True
            if root_version==2:
                assert len(coverage['chunks'])==(S+9999)//10000
            assert root['manifest']['source_identity']['adapter']==SQL_ADAPTER and root['manifest']['validation']['kind']==SQL_VALIDATION
            assert validate_completion(store,root,require_seal=True) is not None
            upgrade_store(store);store.certify_ready_full()
            if reuse_root:
                assert store.pending() is None and store.generation()['id']==0
                before=store.digests(0);store.close()
                # PostgreSQL refuses TEMPLATE while any session is connected.
                _,template=cls.database(template=name);cls.templates[shape]=template
                cls.admin.execute(sql.SQL('ALTER DATABASE {} ALLOW_CONNECTIONS false').format(sql.Identifier(template)))
                print('SQL_COMPLETE_PRISTINE_TEMPLATE '+PROFILE.network.network,flush=True)
                store=PgStore(conninfo=dsn);assert store.digests(0)==before
        # Fresh sessions and copied roots undergo full certification, never a
        # copied in-memory capability or candidate-produced discovery state.
        store.certify_ready_full()
        assert store.pending() is None and store.generation()['id']==0
        from db_sync_alt.sources.blockfrost.relevance import qualified_parent
        assert qualified_parent(store,PROFILE) is not None
        routes={'/metrics':[],'/genesis':{'network_magic':PROFILE.network.magic}}
        time=int(datetime.datetime(2026,1,1,tzinfo=datetime.timezone.utc).timestamp())
        def block(h):return {'hash':H(h).hex(),'height':h,'previous_block':H(h-1).hex() if h else None,'epoch':0 if not S else 273 if h==S-100 else 274 if h==S-50 else 275,'slot':h,'time':time,'tx_count':10 if h<=T else len(by_height.get(h,[]))}
        for h in ([S-100,S-50,S-1] if S else [])+list(range(S,T+K+FUTURE+1)):
            row=block(h);routes['/blocks/'+str(h)]=row;routes['/blocks/'+row['hash']]=row
            routes['/blocks/'+row['hash']+'/next?count=100&page=1']=[block(n) for n in range(h+1,min(h+101,T+K+FUTURE+1))]
        routes['/blocks/latest']=block(T+K+len(transactions))
        all_tx={t['tx_hash']:t for t in [PRE,PRE_SPEND,ROOT,BASE_SPEND]+[t for _,_,t in history]+[t for rows in transactions for t in rows]}
        positions=([(S-1,0,PRE),(S-1,1,PRE_SPEND)] if S else [])+[(S,PROFILE.start[1],ROOT)]+([] if S else [(T,0,BASE_SPEND)])+list(history)+[(h,i,t) for h,rows in by_height.items() for i,t in enumerate(rows)]
        for h,index,t in positions:
            key=t['tx_hash'];routes['/txs/'+key+'/cbor']={'cbor':t['cbor']};routes['/txs/'+key]={'hash':key,'block':H(h).hex(),'block_height':h,'index':index,'valid_contract':t['valid']}
            routes['/txs/'+key+'/utxos']=utxos(t,all_tx)
        for h,rows in by_height.items():
            for page in range(1,len(rows)//100+2):routes['/blocks/'+H(h).hex()+'/txs/cbor?count=100&page='+str(page)+'&order=asc']=[{'tx_hash':t['tx_hash'],'cbor':t['cbor']} for t in rows[(page-1)*100:page*100]]
        return store,routes
    @staticmethod
    def follow(store,routes,target,hooks=None,*,relevance_enabled=True):
        from db_sync_alt.sources.blockfrost.follow import update_follow
        provider=Provider(50000,Fake(copy.deepcopy(routes)))
        result=update_follow(store,provider,PROFILE,target,hooks=hooks or FixtureHooks(),relevance_enabled=relevance_enabled)
        return result,provider.client.calls
