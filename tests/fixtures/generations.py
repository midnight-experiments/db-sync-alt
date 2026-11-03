"""Small fully valid three-generation store (db-sync shaped, synthetic); no external acquisition."""
import hashlib
from db_sync_alt.contract import Profile, digest, canonical
from db_sync_alt.resources import profile_dir
from fixtures import synthetic

START=4500197
PROFILE=Profile(profile_dir('preprod'))
def h(value):return hashlib.sha256(str(value).encode()).digest()
def block(height,fork=False):
    key=lambda n:h(str(n)+('fork' if fork and n>START+1 else ''))
    return {'hash':key(height),'height':height,'parent_hash':key(height-1),'epoch':0 if height==START-2 else 1 if height==START-1 else 2,'slot':height*20,'time':'2026-01-01T00:00:00.000000Z','tx_count':5 if height<=START else 0}

def make_store(conninfo,*,before_seal=None,**store_options):
    """Write generations 0–2 (1 and 2 replace the provisional tail) into a new store and open it for serving.

    `before_seal(store, generation)` runs after a generation's facts are committed and before it is
    verified, so a test can alter or interrupt the build. Returns the verification report per generation.
    """
    reports={}
    with synthetic.create(conninfo,PROFILE.identity,**store_options) as store:
        for gen in range(3):
            target=START+gen;witness=target+2160;fork=gen>0
            manifest={'identity':PROFILE.identity,'contracts':digest(PROFILE.contract),'required_epochs':[0,1,2],'capabilities':{'bridge_qualified':False,'candidates_empty':True},'anchors':{name:{'height':n,'hash':block(n,fork)['hash'].hex()} for name,n in [('start',START),('target',target),('witness',witness)]}}
            parent=gen-1 if gen else None
            synthetic.begin(store,start=PROFILE.start,target=target,witness=witness,manifest=manifest,parent=parent)
            with store.atomic():
                if gen:
                    old=store.generation(parent)
                    store.checkpoint(gen,'metadata','inherited',{'lo':START,'hi':old['target_height'],'parent':parent,'parent_digest':digest(old['manifest']['digests']),'source_kind':'generation-inheritance','inherited_source_kind':old['source']})
                lo=START+gen if gen else START
                synthetic.add_blocks(store,gen,[block(height,fork) for height in range(lo,witness+1)])
                if gen==0:
                    synthetic.add_blocks(store,gen,[block(height) for height in (START-10,START-2,START-1)])
                    origin=h('origin');spend=h('spend');datum=h('datum')
                    for key,height,index in [(origin,START-10,0),(spend,START,4)]:store.insert('transactions',{'hash':key,'block_hash':block(height)['hash'],'tx_index':index,'valid':1},gen)
                    store.insert('datums',{'hash':datum,'value':canonical({'constructor':0,'fields':[]})},gen)
                    for key,value in ((origin,str(2**64-1)),(spend,'100')):store.insert('outputs',{'tx_hash':key,'output_index':0,'address':'holder','value':value,'datum_hash':datum},gen)
                    store.insert('inputs',{'tx_hash':spend,'origin_hash':origin,'origin_index':0,'ordinal':0},gen)
                    policy=bytes.fromhex(PROFILE.night[:56]);name=bytes.fromhex(PROFILE.night[56:])
                    store.insert('assets',{'policy':policy,'name':name},gen)
                    for key in (origin,spend):store.insert('output_assets',{'tx_hash':key,'output_index':0,'policy':policy,'name':name,'quantity':'3','ordinal':0},gen)
                    store.insert('mints',{'tx_hash':spend,'policy':policy,'name':name,'quantity':'-1'},gen)
                    for epoch in range(3):
                        store.insert('epoch_nonces',{'epoch':epoch,'nonce':h('nonce'+str(epoch))},gen)
                        store.insert('epoch_stakes',{'epoch':epoch,'pool_hash':b'p'*28,'amount':str(2**64-1)},gen)
                        receipt={'epoch':epoch,'source_kind':'cardano-db-sync-postgresql'}
                        if epoch<2:receipt['epoch_end']={'height':START-2+epoch,'hash':block(START-2+epoch)['hash'].hex()}
                        store.checkpoint(gen,'epochs',epoch,receipt)
                    store.checkpoint(gen,'prerequisites','complete',{'source_kind':'cardano-db-sync-postgresql'})
                store.checkpoint(gen,'metadata','own',{'lo':lo,'hi':witness,'source_kind':'cardano-db-sync-postgresql'})
                store.checkpoint(gen,'events','own',{'lo':lo,'hi':target,'source_kind':'cardano-db-sync-postgresql'})
                store.db.execute('INSERT INTO chain.provenance VALUES(%s,%s,%s,%s)',(gen,'fixture','generation',str(gen)))
            if before_seal:before_seal(store,gen)
            reports[gen]=synthetic.seal(store,gen,manifest)
        synthetic.open_serving(store,2)
    return reports
