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
# Every inventory key takes at least 63 bytes of its block's proof (an asset key with an empty name), so one block
# adds at most MAX_PROOF // 63 keys: a generation of at most MAX_BLOCKS blocks can never pass MAX_KEYS.
MAX_BLOCKS=MAX_KEYS//(MAX_PROOF//63)
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

def _varint(value):
    groups=[value&127];value>>=7
    while value:groups.append(value&127|128);value>>=7
    return bytes(reversed(groups))

def canonical_address(raw):
    """The address bytes the ledger keeps and cardano-db-sync renders. A pointer address (header 4 or 5)
    holds its slot (at most 32 bits) and transaction and certificate indexes (at most 16 bits) as
    big-endian 7-bit groups; from protocol version 9 the ledger accepts redundant leading zero groups
    and re-serialises them minimally. Any other address, or one that is not a well-formed pointer, is
    returned unchanged."""
    if not isinstance(raw,bytes) or len(raw)<30 or raw[0]>>4 not in (4,5):return raw
    position=29;values=[]
    for width,bits in ((5,32),(3,16),(3,16)):
        value=0
        for _ in range(width):
            if position>=len(raw):return raw
            byte=raw[position];position+=1;value=value<<7|byte&127
            if not byte&128:break
        else:return raw
        if value>>bits:return raw
        values.append(value)
    if position!=len(raw):return raw
    return raw[:29]+b''.join(_varint(v) for v in values)

def _hashed(value):return isinstance(value,str) and re.fullmatch('[0-9a-f]{64}',value) is not None

def _reference(value):
    return (isinstance(value,dict) and set(value)=={'kind','sha256'}
            and value['kind'] in ('full-digests-v1','parent-delta-v2') and _hashed(value['sha256']))

def _coverage_bytes(value):
    """Legacy SQL-root and anchor-bound certificates retain strict distinct wire forms."""
    if not isinstance(value,dict) or type(value.get('version')) is not int or value['version'] not in (1,2):raise Refusal('relevance_coverage_invalid')
    version=value['version'];fields={'kind','version','root','profile','parent_ref','range'}
    if version==2:fields.add('origin')
    if set(value)!=fields or value.get('kind')!=KIND:raise Refusal('relevance_coverage_invalid')
    root=value['root'];bounds=value['range']
    if (not isinstance(root,dict) or set(root)!=({'generation','completion'} if version==1 else {'generation','anchor_sha256'})
            or type(root['generation']) is not int or root['generation']<0
            or not _hashed(root['completion' if version==1 else 'anchor_sha256'])
            or not _hashed(value['profile']) or not _reference(value['parent_ref'])
            or not isinstance(bounds,list) or len(bounds)!=2 or any(type(n) is not int or n<0 for n in bounds)
            or version==2 and value['origin'] not in ('certified','trusted-history')):raise Refusal('relevance_coverage_invalid')
    encoded=canonical(value)
    if len(encoded.encode())>MAX_PROOF:raise Refusal('relevance_coverage_invalid')
    return encoded

def _origin(value):return 'certified' if value['version']==1 else value['origin']

def validate_anchor(store,g,rows=None,*,profile_hash=None):
    """Validate this exact independently guarded metadata; never use a malformed row as absence."""
    from db_sync_alt.store.incremental import integrity_reference,profile_digest
    rows=store.checkpoint_rows(generation=g['id'],phase='relevance',unit='anchor') if rows is None else rows
    if not rows:return None
    if len(rows)!=1:raise Refusal('relevance_coverage_invalid')
    a=rows[0]['receipt'];expected=profile_hash if profile_hash is not None else digest(store.meta('profile'))
    if (not isinstance(a,dict) or set(a)!={'version','kind','generation','integrity_ref','profile','origin'}
            or type(a.get('version')) is not int or a['version']!=1
            or a['kind'] not in ('certified','trusted-history') or a['origin'] not in ('certified','trusted-history')
            or type(a['generation']) is not int or a['generation']!=g['id'] or a['generation']<0
            or not _reference(a['integrity_ref']) or a['integrity_ref']!=integrity_reference(g)
            or not _hashed(a['profile']) or a['profile']!=expected or profile_digest(g)!=expected
            or g['status']!='ready' or a['kind']=='certified' and 'completion' not in g['manifest']):raise Refusal('relevance_coverage_invalid')
    cert=g['manifest'].get('relevance_coverage')
    if a['origin']=='trusted-history' and (store.meta('profile').get('network')=='mainnet' or getattr(getattr(store,'network',None),'network',None)=='mainnet'):raise Refusal('relevance_coverage_invalid')
    if a['kind']=='trusted-history':
        identity=store.meta('profile')
        if a['origin']!='trusted-history' or identity.get('network')=='mainnet' or cert is not None:raise Refusal('relevance_coverage_invalid')
    elif cert is not None:
        _coverage_bytes(cert)
        if a['origin']!=_origin(cert):raise Refusal('relevance_coverage_invalid')
    else:
        from db_sync_alt.verify.completion import SQL_ADAPTER,SQL_VALIDATION
        if (a['origin']!='certified' or g['source']!='cardano-db-sync-postgresql'
                or g['manifest'].get('source_identity',{}).get('adapter')!=SQL_ADAPTER
                or g['manifest'].get('validation',{}).get('kind')!=SQL_VALIDATION):raise Refusal('relevance_coverage_invalid')
    return copy.deepcopy(a)

def _anchor_value(anchor,ready):
    from db_sync_alt.store.incremental import integrity_reference
    return {'kind':KIND,'version':2,'root':{'generation':anchor['generation'],'anchor_sha256':digest(anchor)},
            'profile':anchor['profile'],'parent_ref':integrity_reference(ready),'origin':anchor['origin']}

def _validate_root(store,cert,parent):
    """Check the referenced point and only its descendant suffix, without replaying anchor history."""
    _coverage_bytes(cert)
    from db_sync_alt.store.incremental import profile_digest,integrity_base
    cutoff=None
    if getattr(store,'supports_checkpoints',False):
        base,_=integrity_base(store)
        if cert['root']['generation']<base<=parent['id']:
            candidate=store.generation(base);a=validate_anchor(store,candidate,profile_hash=cert['profile'])
            covered=candidate['manifest'].get('relevance_coverage')
            if a is not None and covered is not None and cert['root']==covered['root'] and _origin(cert)==a['origin']:cutoff=candidate
    root=cutoff if cutoff is not None else store.generation(cert['root']['generation'])
    if cutoff is not None:
        found=store.db.execute('WITH RECURSIVE lineage(id,parent) AS (SELECT id,parent FROM chain.generations WHERE id=%s UNION ALL SELECT g.id,g.parent FROM lineage l JOIN chain.generations g ON g.id=l.parent WHERE l.id<>%s) SELECT 1 FROM lineage WHERE id=%s',(parent['id'],root['id'],root['id'])).fetchone()
        if not found:raise Refusal('relevance_coverage_invalid')
        return
    if profile_digest(root)!=cert['profile']:raise Refusal('relevance_coverage_invalid')
    if cert['version']==2:
        a=validate_anchor(store,root,profile_hash=cert['profile'])
        if a is None or digest(a)!=cert['root']['anchor_sha256'] or a['origin']!=cert['origin']:raise Refusal('relevance_coverage_invalid')
    else:
        from db_sync_alt.verify.completion import SQL_ADAPTER,SQL_VALIDATION
        if (root['source']!='cardano-db-sync-postgresql' or root['parent'] is not None or root['status']!='ready'
                or root['manifest'].get('source_identity',{}).get('adapter')!=SQL_ADAPTER
                or root['manifest'].get('validation',{}).get('kind')!=SQL_VALIDATION
                or root['manifest'].get('completion',{}).get('sha256')!=cert['root']['completion']):raise Refusal('relevance_coverage_invalid')
    found=store.db.execute('WITH RECURSIVE lineage(id,parent) AS (SELECT id,parent FROM chain.generations WHERE id=%s UNION ALL SELECT g.id,g.parent FROM lineage l JOIN chain.generations g ON g.id=l.parent WHERE l.id<>%s) SELECT 1 FROM lineage WHERE id=%s',(parent['id'],root['id'],root['id'])).fetchone()
    if not found:raise Refusal('relevance_coverage_invalid')


def _checkpoint_binding(store):
    """A checkpoint invalidates the cache; it never substitutes for relevance coverage."""
    if not getattr(store,'supports_checkpoints',False):return None
    from db_sync_alt.store.incremental import integrity_base
    base,checkpoint=integrity_base(store)
    if checkpoint is None:return (base,None)
    value=dict(checkpoint);stamp=value.get('checkpointed_at')
    if not hasattr(stamp,'isoformat'):raise Refusal('relevance_coverage_invalid')
    value['checkpointed_at']=stamp.isoformat()
    if len(canonical(value).encode())>MAX_PROOF:raise Refusal('relevance_coverage_invalid')
    return (base,digest(value))


def qualified_parent(store,profile):
    """Qualify only the suffix after the newest valid anchor; always validate the anchor itself."""
    from db_sync_alt.store.incremental import integrity_reference,profile_digest,revision
    from db_sync_alt.verify.completion import validate_completion,SQL_ADAPTER,SQL_VALIDATION
    try:cap=store.current_verified_parent()
    except (AttributeError,Refusal):return None
    profile_hash=digest(profile.identity);current_revision=revision(store)
    ready=store.generation();descriptor=cap.descriptor
    if (digest(descriptor['manifest'])!=digest(ready['manifest'])
            or digest(descriptor['profile'])!=profile_hash or profile_digest(ready)!=profile_hash):raise Refusal('relevance_parent_changed')
    # These constant-size bindings are checked even on a hot cache hit. The
    # current point can acquire a post-ready anchor without changing its manifest.
    ready_anchor=validate_anchor(store,ready,profile_hash=profile_hash)
    checkpoint=_checkpoint_binding(store);cached=_QUALIFIED.get(store)
    if ready_anchor is not None:
        value=_anchor_value(ready_anchor,ready)
        _QUALIFIED[store]=(weakref.ref(cap),copy.deepcopy(value),descriptor,profile_hash,current_revision,checkpoint)
        return value
    known_root=ready['manifest'].get('relevance_coverage',{}).get('root',{}).get('generation',-1)
    if type(known_root) is not int:known_root=-1
    if checkpoint is not None and known_root<checkpoint[0]<ready['id']:
        validate_anchor(store,store.generation(checkpoint[0]),profile_hash=profile_hash)
    if cached and cached[3]==profile_hash and cached[5]==checkpoint:
        if cached[1]['version']==2:
            a=validate_anchor(store,store.generation(cached[1]['root']['generation']),profile_hash=profile_hash)
            if a is None or digest(a)!=cached[1]['root']['anchor_sha256'] or a['origin']!=cached[1]['origin']:raise Refusal('relevance_coverage_invalid')
        if cached[0]() is cap and cached[4]==current_revision:return copy.deepcopy(cached[1])
    else:cached=None
    current=ready;lineage_root=None;lineage_origin=None
    while True:
        if profile_digest(current)!=profile_hash:raise Refusal('relevance_parent_changed')
        anchor=validate_anchor(store,current,profile_hash=profile_hash) if current['id']!=ready['id'] else None
        if anchor is not None:
            if lineage_root is not None:
                if lineage_origin!=anchor['origin']:raise Refusal('relevance_coverage_invalid')
                # A pending certificate can retain its older root across a later
                # checkpoint. Validate that exact root; never rewrite the pending mode.
                covered=current['manifest'].get('relevance_coverage')
                allowed=[{'generation':anchor['generation'],'anchor_sha256':digest(anchor)}]
                if covered is not None:allowed.append(covered['root'])
                if lineage_root not in allowed:raise Refusal('relevance_coverage_invalid')
            value=_anchor_value(anchor,ready);break
        if current['source']!='blockfrost-cardano':break
        certificate=current['manifest'].get('relevance_coverage')
        proofs=store.checkpoint_rows(generation=current['id'],phase='relevance',unit='coverage')
        if certificate is None:return None
        encoded=_coverage_bytes(certificate)
        if len(proofs)!=1 or _coverage_bytes(proofs[0]['receipt'])!=encoded:raise Refusal('relevance_coverage_invalid')
        root=certificate['root'];origin=_origin(certificate)
        if lineage_root is not None and (root!=lineage_root or origin!=lineage_origin):raise Refusal('relevance_coverage_invalid')
        lineage_root=root;lineage_origin=origin
        from db_sync_alt.store.incremental import validate_completion_v2
        validate_completion_v2(store,current,require_seal=True)
        if current['parent'] is None:return None
        parent=store.generation(current['parent'])
        if (profile_digest(parent)!=profile_hash or certificate['profile']!=profile_hash
                or certificate['parent_ref']!=integrity_reference(parent)
                or certificate['range']!=[parent['target_height']+1,current['target_height']]):raise Refusal('relevance_coverage_invalid')
        current=parent
        if cached and current['id']==cached[2]['generation'] and integrity_reference(current)==cached[2]['integrity_ref']:
            if lineage_root!=cached[1]['root'] or lineage_origin!=_origin(cached[1]):raise Refusal('relevance_coverage_invalid')
            value={**cached[1],'parent_ref':integrity_reference(ready)};break
    else:raise AssertionError('unreachable')
    if current['source']!='blockfrost-cardano' and anchor is None:
        if (current['source']!='cardano-db-sync-postgresql'
                or current['manifest'].get('source_identity',{}).get('adapter')!=SQL_ADAPTER
                or current['manifest'].get('validation',{}).get('kind')!=SQL_VALIDATION):return None
        while True:
            if profile_digest(current)!=profile_hash:raise Refusal('relevance_parent_changed')
            if validate_completion(store,current,require_seal=True) is None:return None
            if current['parent'] is None:break
            current=store.generation(current['parent'])
            if current['source']!='cardano-db-sync-postgresql':return None
        root={'generation':current['id'],'completion':current['manifest']['completion']['sha256']}
        if lineage_root is not None and (lineage_root!=root or lineage_origin!='certified'):raise Refusal('relevance_coverage_invalid')
        value={'kind':KIND,'version':1,'root':root,'profile':profile_hash,'parent_ref':integrity_reference(ready)}
    _QUALIFIED[store]=(weakref.ref(cap),copy.deepcopy(value),descriptor,profile_hash,current_revision,checkpoint)
    return value

def certificate(store,profile,target):
    parent=qualified_parent(store,profile)
    if parent is None:return None
    return {**parent,'range':[store.generation()['target_height']+1,target]}

def requested_coverage(store,profile,target,pending,enabled):
    """Select a mode before provider work; persisted coverage cannot change modes."""
    if type(enabled) is not bool:raise Refusal('invalid_relevance_configuration')
    value=None if pending is None else pending['manifest'].get('relevance_coverage')
    if pending is not None and enabled!=(value is not None):raise Refusal('relevance_mode_mismatch')
    if not enabled:return None
    if pending is not None:
        from db_sync_alt.store.incremental import profile_digest,validate_completion_v2
        if profile_digest(pending)!=digest(profile.identity):raise Refusal('relevance_coverage_invalid')
        validate_completion_v2(store,pending)
    expected=certificate(store,profile,target)
    if expected is None:raise Refusal('relevance_parent_unsupported')
    if pending is None:return expected
    encoded=_coverage_bytes(value)
    if encoded!=_coverage_bytes(expected):raise Refusal('relevance_coverage_invalid')
    records=store.checkpoint_rows(generation=pending['id'],phase='relevance',unit='coverage')
    if not records:
        # Recover only this exact previously declared certificate, against the
        # independently certified parent. Facts and request budgets are untouched.
        with store.atomic():
            current=store.generation(pending['id'])
            if current['status']!='pending' or canonical(current['manifest'])!=canonical(pending['manifest']) or canonical(certificate(store,profile,target))!=encoded:raise Refusal('relevance_coverage_invalid')
            records=store.checkpoint_rows(generation=current['id'],phase='relevance',unit='coverage')
            if not records:store.checkpoint(current['id'],'relevance','coverage',value)
            elif len(records)!=1 or _coverage_bytes(records[0]['receipt'])!=encoded:raise Refusal('relevance_coverage_invalid')
    elif len(records)!=1 or _coverage_bytes(records[0]['receipt'])!=encoded:raise Refusal('relevance_coverage_invalid')
    return value


def pending_certificate(store,profile,generation):
    g=store.generation(generation)
    return requested_coverage(store,profile,g['target_height'],g,True)

def _anchor_receipt(store,g,kind,origin):
    from db_sync_alt.store.incremental import integrity_reference
    value={'version':1,'kind':kind,'generation':g['id'],'integrity_ref':integrity_reference(g),
           'profile':digest(store.meta('profile')),'origin':origin}
    validate_anchor(store,g,[{'receipt':value}])
    return value

def checkpoint_anchor(store,cap):
    """An integrity capability can certify the parent without repeating its full audit.

    Anchor-free full-scan checkpoints do no relevance lineage work. A pending
    certificate keeps its original root: defer anchor creation until a checkpoint
    with no pending generation, leaving the ordinary integrity checkpoint intact.
    """
    from db_sync_alt.store.incremental import _require_checkpoint_capability,VerifiedParent,_descriptor,_session
    _require_checkpoint_capability(store,cap)
    g=store.generation(cap.generation)
    existing=validate_anchor(store,g)
    if existing is not None or store.pending() is not None or g['manifest'].get('relevance_coverage') is None:return None
    if store.generation()['id']!=g['id']:raise Refusal('checkpoint_uncertified')
    store._verified_parent=VerifiedParent(store,_session(store),_descriptor(store,g))
    from types import SimpleNamespace
    qualification=qualified_parent(store,SimpleNamespace(identity=store.meta('profile')))
    if qualification is None:raise Refusal('relevance_parent_unsupported')
    return _anchor_receipt(store,g,'certified',_origin(qualification))

def trust_history(store,profile):
    """One explicit non-Mainnet initialization under the existing exclusive writer lock."""
    from db_sync_alt.store.database import SERVING_LOCK,PROJECTION_WRITE
    from db_sync_alt.store.incremental import _session,integrity_reference
    _session(store)
    if profile.network.network=='mainnet' or store.network.network=='mainnet':raise Refusal('relevance_trust_mainnet_refused')
    if digest(profile.identity)!=digest(store.meta('profile')):raise Refusal('relevance_parent_changed')
    if not store.supports_checkpoints:raise Refusal('checkpoint_schema_required')
    if store.pending() is not None:raise Refusal('relevance_trust_pending')
    state=store.state();g=store.generation()
    if state['revoked'] or state['migration_state']!='ready':raise Refusal('relevance_parent_changed')
    if g['manifest'].get('relevance_coverage') is not None:raise Refusal('relevance_trust_already_initialized')
    def unused():
        if store.db.execute("SELECT 1 FROM chain.checkpoints WHERE phase='relevance' AND unit='anchor' AND (generation=%s) LIMIT 1",(g['id'],)).fetchone():raise Refusal('relevance_trust_already_initialized')
    unused()
    store.certify_ready_full()
    receipt=_anchor_receipt(store,g,'trusted-history','trusted-history')
    with store.atomic():
        store.db.execute('SELECT pg_advisory_xact_lock(%s,%s)',SERVING_LOCK)
        store.db.execute('SELECT set_config(%s,%s,true)',(PROJECTION_WRITE,'on'))
        if store.state()!=state or store.pending() is not None or store.generation()!=g or integrity_reference(store.generation())!=receipt['integrity_ref']:raise Refusal('relevance_parent_changed')
        unused();store.insert_relevance_anchor(receipt)
    _QUALIFIED.pop(store,None)
    return {'generation':g['id'],'relevance':'trusted-history'}

def history_status(store):
    """Observe stored provenance using summary columns/receipts, never full manifests.

    Like the rest of status, this is not a fresh integrity verification. The
    verifier/filter independently validate the selected anchor's exact reference.
    """
    ready=store.state()['canonical_ready'];profile=digest(store.meta('profile'))
    rows=store.db.execute("""WITH RECURSIVE lineage(id,parent,status) AS (
      SELECT id,parent,status FROM chain.generations WHERE id=%s
      UNION ALL SELECT g.id,g.parent,g.status FROM lineage l JOIN chain.generations g ON g.id=l.parent
      WHERE NOT EXISTS(SELECT 1 FROM chain.checkpoints c WHERE c.generation=l.id AND c.phase='relevance' AND c.unit='anchor'))
      SELECT l.id,l.status,c.receipt FROM lineage l JOIN chain.checkpoints c ON c.generation=l.id
      WHERE c.phase='relevance' AND c.unit='anchor'""",(ready,)).fetchall()
    if rows:
        if len(rows)!=1:raise Refusal('relevance_coverage_invalid')
        generation,status,a=rows[0]
        if (not isinstance(a,dict) or set(a)!={'version','kind','generation','integrity_ref','profile','origin'}
                or type(a.get('version')) is not int or a['version']!=1
                or a['kind'] not in ('certified','trusted-history') or a['origin'] not in ('certified','trusted-history')
                or a['kind']=='trusted-history' and a['origin']!='trusted-history'
                or type(a['generation']) is not int or a['generation']!=generation or generation<0 or status!='ready'
                or not _reference(a['integrity_ref']) or not _hashed(a['profile']) or a['profile']!=profile
                or a['origin']=='trusted-history' and store.network.network=='mainnet'):raise Refusal('relevance_coverage_invalid')
        return a['origin']
    records=store.checkpoint_rows(generation=ready,phase='relevance',unit='coverage')
    if not records:return 'not-certified'
    if len(records)!=1:raise Refusal('relevance_coverage_invalid')
    cert=records[0]['receipt'];_coverage_bytes(cert)
    if cert['profile']!=profile:raise Refusal('relevance_coverage_invalid')
    return _origin(cert)


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
        """Record the relevant outputs the transaction created (a phase-2-invalid one: its collateral return)."""
        from db_sync_alt.sources.blockfrost.adapter import output_value
        for row in outputs:
            value,assets=output_value(row)
            if self.profile.relevant(value['address'],assets):
                key=(decoded['hash'],row['output_index']);self.created.add(key);self.block_created.add(key)
        if len(self.created)>MAX_KEYS:raise Refusal('relevance_work_unit_limit')

def compare_outputs(decoded,rows,network):
    """Hash-bound CBOR values must equal the selected full provider response, as cardano-db-sync renders
    them: a pointer address in its canonical form, and a valid transaction's collateral return (the row
    marked collateral, from cardano-db-sync's collateral_tx_out) with its lovelace, whose assets Blockfrost
    does not list. That output is never created, so it is never stored."""
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
            if address_bytes(actual['address'],network,require_supported=False)!=canonical_address(expected['address']):raise Refusal('provider_output_cbor_mismatch')
        # Opaque legacy encodings still use the original trusted provider path;
        # they cannot alias any of the checksum-validated Shelley profile addresses.
        expected_assets={(p,n):q for p,n,q in expected['assets']}
        listed=(expected_assets,{}) if row.get('collateral') is True else (expected_assets,)
        if actual['value']!=expected['value'] or expected.get('datum_known',True) and actual['datum_hash']!=expected['datum_hash'] or assets not in listed:raise Refusal('provider_output_cbor_mismatch')

def inventory(facts,tables=TABLES):
    """Key/hash inventory of facts, one entry per family of `tables` (a generation's sealed families)."""
    families={table:{} for table in tables}
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
    families=store.sealed_tables(generation)
    if not isinstance(expected,dict) or set(expected)!=set(families):raise Refusal('relevance_inventory_invalid')
    actual=[];known={table:set() for table in families};count=0
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
    if inventory(actual,families)!=expected:raise Refusal('independent_provider_mismatch')
    return known

def verify_generation(store,generation):
    """Candidate persistence must match independent key/hash inventories exactly."""
    g=store.generation(generation);cert=g['manifest'].get('relevance_coverage')
    if cert is None:return
    records=store.checkpoint_rows(generation=generation,phase='relevance',unit='coverage')
    encoded=_coverage_bytes(cert)
    if len(records)!=1 or _coverage_bytes(records[0]['receipt'])!=encoded:raise Refusal('relevance_coverage_invalid')
    from db_sync_alt.store.incremental import integrity_reference
    parent=store.generation(g['parent'])
    from db_sync_alt.store.incremental import profile_digest
    expected_profile=digest(store.meta('profile'))
    if profile_digest(g)!=expected_profile or cert['profile']!=expected_profile or cert['parent_ref']!=integrity_reference(parent) or cert['range']!=[parent['target_height']+1,g['target_height']]:raise Refusal('relevance_coverage_invalid')
    if validate_anchor(store,g) is None:_validate_root(store,cert,parent)
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
    for table in store.sealed_tables(generation):
        for row in delta_rows(store,table,generation):
            if table in ('blocks','epoch_nonces','epoch_stakes'):continue  # Separate complete metadata/epoch checks.
            if canonical(jsonable([row[k] for k in KEYS[table]])) not in known[table]:raise Refusal('independent_provider_extra_fact')
