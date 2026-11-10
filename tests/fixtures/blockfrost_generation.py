"""Two-generation Blockfrost scenario: a synthetic generation 0 in PostgreSQL plus fake provider routes for the next update.

Heights are `OFFSET + h`; generation 0 targets h=10 (the Preprod start cursor) with its witness at h=2170.
Generation 0 also holds a spend before the start cursor (block h=10, transactions 0 and 1), so its
served `tx_in` is never empty.
"""
import contextlib
import io
from db_sync_alt.resources import profile_dir
from db_sync_alt.sources.blockfrost.adapter import block_row, update
from db_sync_alt.contract import Profile, digest
from fixtures import synthetic
from fixtures.blockfrost import CountingProvider, Fake
from fixtures.cbor import tx


OFFSET=4500187
PROFILE=Profile(profile_dir('preprod'))
POLICY=bytes.fromhex(PROFILE.night[:56]);NAME=bytes.fromhex(PROFILE.night[56:])

def block(h,fork=0,transition=False):
    # Fork is confined above committed height 10; old ready witness is immutable.
    bh=lambda n:(n+fork*10000 if n>10 else n).to_bytes(32,'big').hex()
    return {'hash':bh(h),'height':OFFSET+h,'previous_block':bh(h-1),'epoch':(0 if h==8 else 1 if h==9 else 3 if transition and h>=12 else 2),'slot':h*20,'time':1700000000+h,'tx_count':5 if h==10 else 1 if h in (11,12) else 0}

def out(index=0):return {'output_index':index,'address':'relevant','amount':[{'unit':'lovelace','quantity':'100'},{'unit':PROFILE.night,'quantity':'3'}],'data_hash':None}
# Prerequisite spend before the start cursor (block h=10, indexes 0 and 1; the cursor is index 4).
PRE_ORIGIN=tx({0:[],1:[[b'p',100]],2:3});PRE_SPEND=tx({0:[[bytes.fromhex(PRE_ORIGIN[1]),0]],1:[],2:3})

def fixtures(fork=0,transition=False):
    original=tx({0:[],1:[[b'a',100]],2:3});spend=tx({0:[[bytes.fromhex(original[1]),0]],1:[],2:3});mint=tx({0:[],1:[],2:3,9:{b'q'*28:{b'other':22},POLICY:{NAME:9,b'incidental':33}}})
    route={'/metrics':[],'/genesis':{'network_magic':1},'/blocks/latest':block(2172,fork,transition)}
    for h in range(8,2173):
        b=block(h,fork,transition);route['/blocks/'+str(OFFSET+h)]=b;route['/blocks/'+b['hash']]=b
        route['/blocks/'+b['hash']+'/next?count=100&page=1']=[block(n,fork,transition) for n in range(h+1,min(h+101,2173))]
    for h,(raw,th) in zip((10,11,12),(original,spend,mint)):
        b=block(h,fork,transition);route['/blocks/'+b['hash']+'/txs/cbor?count=100&page=1&order=asc']=[{'tx_hash':th,'cbor':raw}]
        route['/txs/'+th+'/cbor']={'cbor':raw}
        route['/txs/'+th]={'hash':th,'block':b['hash'],'block_height':OFFSET+h,'index':4 if h==10 else 0,'valid_contract':True}
        inp=[dict(out(),tx_hash=original[1],reference=False,collateral=False)] if h==11 else []
        route['/txs/'+th+'/utxos']={'hash':th,'inputs':inp,'outputs':[out()] if h==10 else []}
    b=block(10,fork,transition)
    for index,((raw,th),inputs,outputs) in enumerate([(PRE_ORIGIN,[],[out()]),(PRE_SPEND,[dict(out(),tx_hash=PRE_ORIGIN[1],reference=False,collateral=False)],[])]):
        route['/txs/'+th+'/cbor']={'cbor':raw};route['/txs/'+th+'/utxos']={'hash':th,'inputs':inputs,'outputs':outputs}
        route['/txs/'+th]={'hash':th,'block':b['hash'],'block_height':OFFSET+10,'index':index,'valid_contract':True}
    if transition:
        from fixtures.blockfrost import POOL
        for epoch,total,nonce in [(2,'123','6e'*32),(3,'19','cc'*32)]:
            route['/epochs/'+str(epoch)]={'epoch':epoch,'active_stake':total};route['/epochs/'+str(epoch)+'/parameters']={'epoch':epoch,'nonce':nonce,'max_tx_size':12000+epoch}
            route['/epochs/'+str(epoch)+'/blocks?count=1&page=1&order=desc']=[block(11 if epoch==2 else 2172,fork,transition)['hash']]
        route['/epochs/3/stakes?count=100&page=1&order=asc']=[{'stake_address':'stake_test1a','pool_id':POOL,'amount':'19'}]
    return route,original

def baseline(conninfo,transition=False):
    """Write generation 0 into a new store on the empty database `conninfo`; returns the open writer store."""
    routes,original=fixtures(transition=transition);s=synthetic.create(conninfo,PROFILE.identity)
    try:
        manifest={'identity':PROFILE.identity,'contracts':digest(PROFILE.contract),'anchors':{n:{'height':OFFSET+h,'hash':block(h,transition=transition)['hash']} for n,h in [('start',10),('target',10),('witness',2170)]},'required_epochs':[0,1,2],'capabilities':{'bridge_qualified':False,'candidates_empty':True,'epoch_max_tx_size_v1':True},'source_identity':{'fixture':'sql'}}
        g=synthetic.begin(s,start=PROFILE.start,target=OFFSET+10,witness=OFFSET+2170,manifest=manifest)
        with s.atomic():
            synthetic.add_blocks(s,g,[block_row(block(h,transition=transition)) for h in range(8,2171)])
            th=bytes.fromhex(original[1]);s.insert('transactions',{'hash':th,'block_hash':bytes.fromhex(block(10)['hash']),'tx_index':4,'valid':1},g)
            s.insert('outputs',{'tx_hash':th,'output_index':0,'address':'relevant','value':'100','datum_hash':None},g)
            s.insert('assets',{'policy':POLICY,'name':NAME},g);s.insert('output_assets',{'tx_hash':th,'output_index':0,'policy':POLICY,'name':NAME,'quantity':'3','ordinal':0},g)
            po,ps=bytes.fromhex(PRE_ORIGIN[1]),bytes.fromhex(PRE_SPEND[1])
            for index,key in enumerate((po,ps)):s.insert('transactions',{'hash':key,'block_hash':bytes.fromhex(block(10)['hash']),'tx_index':index,'valid':1},g)
            s.insert('outputs',{'tx_hash':po,'output_index':0,'address':'relevant','value':'100','datum_hash':None},g)
            s.insert('output_assets',{'tx_hash':po,'output_index':0,'policy':POLICY,'name':NAME,'quantity':'3','ordinal':0},g)
            s.insert('inputs',{'tx_hash':ps,'origin_hash':po,'origin_index':0,'ordinal':0},g)
            s.checkpoint(g,'metadata','all',{'lo':OFFSET+10,'hi':OFFSET+2170,'source_kind':'cardano-db-sync-postgresql','synthetic_fixture':True});s.checkpoint(g,'events','all',{'lo':OFFSET+10,'hi':OFFSET+10,'source_kind':'cardano-db-sync-postgresql','synthetic_fixture':True});s.checkpoint(g,'prerequisites','complete',{'source_kind':'cardano-db-sync-postgresql','synthetic_fixture':True})
            for epoch in range(3):
                s.insert('epoch_nonces',{'epoch':epoch,'nonce':b'n'*32,'max_tx_size':12000+epoch},g);s.insert('epoch_stakes',{'epoch':epoch,'pool_hash':b'p'*28,'amount':'123'},g);s.checkpoint(g,'epochs',epoch,{'epoch':epoch,'source_kind':'cardano-db-sync-postgresql','synthetic_fixture':True,'epoch_end':{'height':OFFSET+8+epoch,'hash':block(8+epoch)['hash']} if epoch<2 else None})
        synthetic.seal(s,g,manifest,synthetic_fixture=True)
        synthetic.open_serving(s,g)
    except BaseException:
        s.close();raise
    return s


def advance(s,routes,budget=200,hooks=None):
    """Run the full Blockfrost update (`adapter.update`) on store `s` up to OFFSET+12 with the fake `routes`."""
    from fixtures.blockfrost import FixtureHooks
    with contextlib.redirect_stdout(io.StringIO()):
        return update(s,CountingProvider(budget,Fake(routes)),PROFILE,OFFSET+12,hooks=hooks or FixtureHooks())
