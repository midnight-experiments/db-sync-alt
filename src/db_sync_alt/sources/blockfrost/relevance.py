"""Exact CBOR relevance with certified prior coverage and independent stream state.

This is an optimization of discovery, not a new fact contract. Unknown forms use
the original full provider path. Pending/candidate presence is never a negative
membership proof. Ready data is immutable and owned by the VerifiedParent session.
"""
import copy
import json
import re
import weakref
from db_sync_alt.contract import TABLES, KEYS, Refusal, canonical, digest, jsonable

KIND='independent-cbor-relevance-v1'
MAX_KEYS=200000
MAX_PROOF=2*1024*1024
_QUALIFIED=weakref.WeakKeyDictionary()
ALPHABET='qpzry9x8gf2tvdw0s3jn54khce6mua7l'

def _polymod(values):
    chk=1
    for value in values:
        top=chk>>25;chk=((chk&0x1ffffff)<<5)^value
        for index,generator in enumerate((0x3b6a57b2,0x26508e6d,0x1ea119fa,0x3d4233dd,0x2a1462b3)):
            if top>>index&1:chk^=generator
    return chk

def _expand(hrp):return [ord(x)>>5 for x in hrp]+[0]+[ord(x)&31 for x in hrp]

def address_bytes(text,network,*,require_supported=True):
    """Validated configured Shelley address; no checksum/padding shortcut."""
    if not isinstance(text,str) or text.lower()!=text or not 8<len(text)<=200:raise Refusal('relevance_profile_address_invalid')
    try:
        hrp,payload=text.rsplit('1',1);values=[ALPHABET.index(c) for c in payload]
    except (ValueError,TypeError):raise Refusal('relevance_profile_address_invalid') from None
    if hrp!=('addr' if network=='mainnet' else 'addr_test') or len(values)<6 or _polymod(_expand(hrp)+values)!=1:raise Refusal('relevance_profile_address_invalid')
    acc=bits=0;raw=[]
    for value in values[:-6]:
        acc=acc<<5|value;bits+=5
        while bits>=8:bits-=8;raw.append(acc>>bits&255)
    if bits>=5 or acc<<(8-bits)&255:raise Refusal('relevance_profile_address_invalid')
    result=bytes(raw)
    if not result or result[0]&15!=(1 if network=='mainnet' else 0) or require_supported and address_text(result,network) is None:raise Refusal('relevance_profile_address_invalid')
    return result

def address_text(raw,network):
    """Supported exact Shelley payment addresses; unknown forms return None."""
    if not isinstance(raw,bytes) or not raw:return None
    kind=raw[0]>>4;tag=raw[0]&15;expected=1 if network=='mainnet' else 0
    if kind not in (0,1,2,3,6,7) or len(raw)!=(57 if kind<4 else 29):return None
    if tag!=expected:raise Refusal('cbor_output_network_mismatch')
    return _encode_address(raw,network)

def _encode_address(raw,network):
    hrp='addr' if network=='mainnet' else 'addr_test';data=[];acc=bits=0
    for value in raw:
        acc=acc<<8|value;bits+=8
        while bits>=5:bits-=5;data.append(acc>>bits&31)
    if bits:data.append(acc<<(5-bits)&31)
    check=_polymod(_expand(hrp)+data+[0]*6)^1
    return hrp+'1'+''.join(ALPHABET[x] for x in data+[check>>(5*(5-i))&31 for i in range(6)])

def qualified_parent(store,profile):
    """Only a sealed SQL-complete root plus qualified provider transitions."""
    from db_sync_alt.store.incremental import integrity_reference
    from db_sync_alt.verify.completion import validate_completion, SQL_ADAPTER, SQL_VALIDATION
    try:
        cap=store.current_verified_parent();ready=store.generation()
    except (AttributeError,Refusal):return None
    if digest(cap.descriptor['manifest'])!=digest(ready['manifest']) or digest(cap.descriptor['profile'])!=digest(profile.identity):raise Refusal('relevance_parent_changed')
    cached=_QUALIFIED.get(store)
    if cached and cached[0]() is cap:return copy.deepcopy(cached[1])
    lineage=[];current=ready
    while current['source']=='blockfrost-cardano':
        certificate=current['manifest'].get('relevance_coverage')
        proofs=store.checkpoint_rows(generation=current['id'],phase='relevance',unit='coverage')
        if certificate is None:return None
        if not isinstance(certificate,dict) or certificate.get('kind')!=KIND or len(proofs)!=1 or proofs[0]['receipt']!=certificate:raise Refusal('relevance_coverage_invalid')
        from db_sync_alt.store.incremental import validate_completion_v2
        validate_completion_v2(store,current,require_seal=True)
        if current['parent'] is None:return None
        parent=store.generation(current['parent'])
        if certificate.get('parent_ref')!=integrity_reference(parent) or certificate.get('range')!=[parent['target_height']+1,current['target_height']] or certificate.get('profile')!=digest(profile.identity):raise Refusal('relevance_coverage_invalid')
        lineage.append(certificate);current=parent
        if cached and current['id']==cached[2]['generation'] and integrity_reference(current)==cached[2]['integrity_ref']:
            root=cached[1]['root']
            if any(c.get('root')!=root for c in lineage):raise Refusal('relevance_coverage_invalid')
            value={'kind':KIND,'version':1,'root':root,'profile':digest(profile.identity),'parent_ref':integrity_reference(ready)}
            _QUALIFIED[store]=(weakref.ref(cap),copy.deepcopy(value),cap.descriptor)
            return value
    if current['source']!='cardano-db-sync-postgresql' or current['manifest'].get('source_identity',{}).get('adapter')!=SQL_ADAPTER or current['manifest'].get('validation',{}).get('kind')!=SQL_VALIDATION:return None
    seal=validate_completion(store,current,require_seal=True)
    if seal is None:return None
    # SQL extension seals recursively bind their parent; the original root must
    # explicitly cover all prerequisites, never a historical experimental slice.
    while current['parent'] is not None:
        current=store.generation(current['parent'])
        if current['source']!='cardano-db-sync-postgresql':return None
        validate_completion(store,current,require_seal=True)
    root={'generation':current['id'],'completion':current['manifest']['completion']['sha256']}
    if any(c.get('root')!=root for c in lineage):raise Refusal('relevance_coverage_invalid')
    value={'kind':KIND,'version':1,'root':root,'profile':digest(profile.identity),'parent_ref':integrity_reference(ready)}
    _QUALIFIED[store]=(weakref.ref(cap),copy.deepcopy(value),cap.descriptor)
    return value

def certificate(store,profile,target):
    parent=qualified_parent(store,profile)
    if parent is None:return None
    return {**parent,'range':[store.generation()['target_height']+1,target]}

def pending_certificate(store,profile,generation):
    try:g=store.generation(generation);value=g.get('manifest',{}).get('relevance_coverage')
    except (AttributeError,KeyError,TypeError):return None
    if value is None:return None
    if value!=certificate(store,profile,g['target_height']):raise Refusal('relevance_coverage_invalid')
    return value

class Discovery:
    def __init__(self,store,profile,parent,*,independent=False):
        self.store=store;self.profile=profile;self.parent=parent;self.independent=independent
        self.network=profile.network.network
        self.addresses={address_bytes(a,self.network) for a in profile.addresses}
        self.created=set();self.block_created=set();self.parent_cache={}
    def begin(self):self.block_created=set();self.parent_cache={}
    def output(self,value):
        if value is None:return None
        if not value.get('classification_safe',True):return None
        if address_text(value['address'],self.network) is None:return None
        return value['address'] in self.addresses or any(self.profile.required(p,n) for p,n,_ in value['assets'])
    def contains(self,key):
        if key in self.created:return True
        if key not in self.parent_cache:
            self.parent_cache[key]=self.store.db.execute('SELECT 1 FROM chain.outputs WHERE tx_hash=%s AND output_index=%s AND first_generation<=%s',(*key,self.parent)).fetchone() is not None
        return self.parent_cache[key]
    def select(self,decoded):
        outputs=list(decoded.get('outputs',()))
        if decoded.get('collateral_return'):outputs.append(decoded.get('collateral_output'))
        if 'outputs' not in decoded:return True
        if self.independent:
            # Separate traversal and state, never the acquisition's selected list.
            if any(self.profile.required(p,n) for p,n,_ in decoded['mints']):return True
            for output in outputs:
                selected=self.output(output)
                if selected is not False:return True
            refs=decoded['ordinary']|(decoded['collateral'] if not decoded['valid'] else frozenset())
            return any(self.contains(key) for key in refs)
        decisions=[self.output(value) for value in outputs]
        if any(x is None or x for x in decisions):return True
        refs=decoded['ordinary']|(decoded['collateral'] if not decoded['valid'] else frozenset())
        return any(self.contains(key) for key in refs) or any(self.profile.required(p,n) for p,n,_ in decoded['mints'])
    def advance(self,decoded,outputs):
        if not decoded['valid']:return
        from db_sync_alt.sources.blockfrost.adapter import output_value
        for row in outputs:
            value,assets=output_value(row)
            if self.profile.relevant(value['address'],assets):
                key=(decoded['hash'],row['output_index']);self.created.add(key);self.block_created.add(key)
        if len(self.created)>MAX_KEYS:raise Refusal('relevance_work_unit_limit')

def compare_outputs(decoded,rows,network):
    """Hash-bound CBOR values must equal the selected full provider response."""
    from db_sync_alt.sources.blockfrost.adapter import output_value
    values=list(decoded.get('outputs',()))
    if decoded.get('collateral_return'):values.append(decoded.get('collateral_output'))
    for row in rows:
        index=row['output_index']
        if index>=len(values):raise Refusal('provider_output_count_mismatch')
        expected=values[index]
        if expected is None:raise Refusal('relevance_output_unqualified')
        address=address_text(expected['address'],network)
        actual,assets=output_value(row)
        if address is not None:
            if actual['address']!=address:raise Refusal('provider_output_cbor_mismatch')
        elif actual['address'].startswith(('addr1','addr_test1')):
            if address_bytes(actual['address'],network,require_supported=False)!=expected['address']:raise Refusal('provider_output_cbor_mismatch')
        # Opaque legacy encodings still use the original trusted provider path;
        # they cannot alias any of the checksum-validated Shelley profile addresses.
        if actual['value']!=expected['value'] or expected.get('datum_known',True) and actual['datum_hash']!=expected['datum_hash'] or assets!={(p,n):q for p,n,q in expected['assets']}:raise Refusal('provider_output_cbor_mismatch')

def inventory(facts):
    families={table:{} for table in TABLES}
    for table,row in facts:
        value={k:jsonable(row[k]) for k in TABLES[table]}
        if table in ('inputs','output_assets'):value['ordinal']=row['ordinal']
        key=canonical([value[k] for k in KEYS[table]])
        if key in families[table] and families[table][key]!=value:raise Refusal('independent_provider_conflict')
        families[table][key]=value
    return {table:{'keys':[json.loads(k) for k in sorted(rows)],'count':len(rows),'sha256':digest([rows[k] for k in sorted(rows)])} for table,rows in families.items()}

class CertifiedReadView:
    """Parent rows and independently discovered earlier work units only."""
    def __init__(self,store,parent,known):self.store=store;self.parent=parent;self.known=known
    def __getattr__(self,name):return getattr(self.store,name)
    def fact(self,table,key):
        if table=='blocks':return self.store.fact(table,key)
        if isinstance(key,dict):key=tuple(key[k] for k in KEYS[table])
        normalized=canonical(jsonable(key))
        row=self.store.db.execute('SELECT first_generation FROM chain.'+table+' WHERE '+' AND '.join(k+'=%s' for k in KEYS[table]),key).fetchone()
        if row is None or row[0]>self.parent and normalized not in self.known[table]:return None
        return self.store.fact(table,key)

def verify_inventory(store,generation,expected):
    if not isinstance(expected,dict) or set(expected)!=set(TABLES):raise Refusal('relevance_inventory_invalid')
    actual=[];known={table:set() for table in TABLES};count=0
    for table,entry in expected.items():
        if not isinstance(entry,dict) or set(entry)!={'keys','count','sha256'} or type(entry['count']) is not int or not isinstance(entry['keys'],list) or len(entry['keys'])!=entry['count']:raise Refusal('relevance_inventory_invalid')
        for rawkey in entry['keys']:
            count+=1
            if count>MAX_KEYS or not isinstance(rawkey,list) or len(rawkey)!=len(KEYS[table]):raise Refusal('relevance_inventory_invalid')
            key=tuple(bytes.fromhex(v) if name in ('hash','tx_hash','origin_hash','policy','name','pool_hash') else v for name,v in zip(KEYS[table],rawkey))
            row=store.fact(table,key)
            if row is None:raise Refusal('independent_provider_mismatch')
            if table in ('inputs','output_assets'):row={**row,'ordinal':store.ordinal(table,key)}
            actual.append((table,row));known[table].add(canonical(rawkey))
    if inventory(actual)!=expected:raise Refusal('independent_provider_mismatch')
    return known

def verify_generation(store,generation):
    """Candidate persistence must match independent key/hash inventories exactly."""
    g=store.generation(generation);cert=g['manifest'].get('relevance_coverage')
    if cert is None:return
    records=store.checkpoint_rows(generation=generation,phase='relevance',unit='coverage')
    if not isinstance(cert,dict) or set(cert)!={'kind','version','root','profile','parent_ref','range'} or type(cert['version']) is not int or cert['version']!=1 or len(records)!=1 or records[0]['receipt']!=cert or cert.get('kind')!=KIND:raise Refusal('relevance_coverage_invalid')
    from db_sync_alt.store.incremental import integrity_reference
    parent=store.generation(g['parent'])
    if cert['profile']!=digest(store.meta('profile')) or cert['parent_ref']!=integrity_reference(parent) or cert['range']!=[parent['target_height']+1,g['target_height']]:raise Refusal('relevance_coverage_invalid')
    root=store.generation(0)
    if root['source']!='cardano-db-sync-postgresql' or root['parent'] is not None or cert['root']!={'generation':0,'completion':root['manifest'].get('completion',{}).get('sha256')}:raise Refusal('relevance_coverage_invalid')
    known={table:set() for table in TABLES}
    for checkpoint in store.checkpoint_rows(generation=generation,phase='events'):
        proof=checkpoint['receipt'].get('relevance')
        required={'kind','version','parent_ref','inventory','transactions','transaction_hashes_sha256','created_outputs'}
        if not isinstance(proof,dict) or set(proof)!=required or type(proof.get('version')) is not int or proof.get('version')!=1 or proof.get('kind')!=KIND or proof.get('parent_ref')!=cert['parent_ref'] or len(canonical(proof).encode())>MAX_PROOF:raise Refusal('relevance_inventory_invalid')
        block=store.block_at(generation,checkpoint['receipt']['lo'])
        if type(proof['transactions']) is not int or proof['transactions']<0 or not isinstance(proof['transaction_hashes_sha256'],str) or re.fullmatch('[0-9a-f]{64}',proof['transaction_hashes_sha256']) is None or checkpoint['receipt']['lo']!=checkpoint['receipt']['hi'] or proof['transactions']!=block['tx_count'] or proof['transactions']!=checkpoint['receipt'].get('transactions_scanned'):raise Refusal('relevance_inventory_invalid')
        if not isinstance(proof['created_outputs'],list) or len(proof['created_outputs'])>MAX_KEYS or any(key not in proof['inventory']['outputs']['keys'] for key in proof['created_outputs']):raise Refusal('relevance_inventory_invalid')
        checked=verify_inventory(store,generation,proof['inventory'])
        for table,keys in checked.items():known[table].update(keys)
    if sum(map(len,known.values()))>MAX_KEYS:raise Refusal('relevance_work_unit_limit')
    from db_sync_alt.store.incremental import delta_rows
    for table in TABLES:
        for row in delta_rows(store,table,generation):
            if table in ('blocks','epoch_nonces','epoch_stakes'):continue  # Separate complete metadata/epoch checks.
            if canonical(jsonable([row[k] for k in KEYS[table]])) not in known[table]:raise Refusal('independent_provider_extra_fact')
