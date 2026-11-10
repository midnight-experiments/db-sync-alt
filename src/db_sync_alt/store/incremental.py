"""Schema2 certified-parent transitions. Roots are NOT full canonical family hashes."""
import copy
import hashlib
from dataclasses import dataclass,field
from psycopg.types.json import Jsonb
from db_sync_alt.contract import TABLES,KEYS,Refusal,canonical,digest,jsonable,validate_numeric

VERSION=2
KIND_V1='certified-parent-delta-v1'
KIND_V2='certified-parent-delta-v2'
# The manifest field that binds an incremental generation to the stored profile,
# per integrity kind: the whole profile (v1) or its digest (v2). The writer builds v2.
PROFILE_FIELD={KIND_V1:'identity',KIND_V2:'identity_sha256'}
FULL_DIGESTS='full-digests-v1'


def incremental(g):return isinstance(g['manifest'].get('integrity'),dict) and g['manifest']['integrity'].get('version')==VERSION

def form(m):
    """Integrity kind of an incremental manifest's profile field, or None unless it has exactly one."""
    kinds=[kind for kind,key in PROFILE_FIELD.items() if key in m]
    return kinds[0] if len(kinds)==1 else None

def profile_digest(g):
    """Digest of the profile a generation's manifest is bound to, or None for an invalid form."""
    m=g['manifest']
    if not incremental(g):return digest(m.get('identity'))
    kind=form(m)
    if kind is None:return None
    return m['identity_sha256'] if kind==KIND_V2 else digest(m['identity'])

def integrity_reference(g):
    if incremental(g):
        value=g['manifest'].get('integrity',{})
        if value.get('kind') not in PROFILE_FIELD or not isinstance(value.get('root'),str):raise Refusal('completion_invalid')
        return {'kind':'parent-delta-v2','sha256':value['root']}
    return {'kind':FULL_DIGESTS,'sha256':digest(g['manifest']['digests'])}

@dataclass(frozen=True)
class VerifiedParent:
    _owner: object
    _session: int
    _descriptor: dict
    _descriptor_sha256: str=field(init=False,repr=False)
    def __post_init__(self):object.__setattr__(self,'_descriptor_sha256',digest(self._descriptor))
    @property
    def descriptor(self):return copy.deepcopy(self._descriptor)

@dataclass(frozen=True)
class VerifiedTransition:
    _owner: object
    _session: int
    generation: int
    revision: int
    proof: dict
    manifest_sha256: str
    _proof_sha256: str=field(init=False,repr=False)
    def __post_init__(self):object.__setattr__(self,'_proof_sha256',digest(self.proof))

@dataclass(frozen=True)
class VerifiedCheckpoint:
    """A full verification of `generation` by this writer session at integrity revision `revision`."""
    _owner: object
    _session: int
    generation: int
    revision: int
    root: str
    _report: dict
    _sha256: str=field(init=False,repr=False)
    def __post_init__(self):object.__setattr__(self,'_sha256',digest([self.generation,self.revision,self.root,self._report]))
    @property
    def report(self):return copy.deepcopy(self._report)


def _session(store):
    store.require_writer()
    if not store.supports_incremental:raise Refusal('incremental_schema_required')
    pid=store.db.info.backend_pid
    if not store.db.execute("SELECT 1 FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid() AND classid=36 AND objid=1 AND objsubid=2 AND granted").fetchone():raise Refusal('writer_ownership_required')
    return pid

def revision(store):return store.db.execute('SELECT revision FROM chain.integrity_state WHERE id=1').fetchone()[0]

def _descriptor(store,g):
    return {'generation':g['id'],'serial':store.state()['publication_serial'],'integrity_ref':integrity_reference(g),
        'target':g['target_height'],'witness':g['witness_height'],'start':[g['start_height'],g['start_index']],
        'manifest':copy.deepcopy(g['manifest']),'source':g['source'],'profile':store.meta('profile')}

def current_verified_parent(store):
    pid=_session(store);cap=getattr(store,'_verified_parent',None);state=store.state()
    if not isinstance(cap,VerifiedParent) or cap._owner is not store or cap._session!=pid or state['revoked'] or state['canonical_ready']!=cap._descriptor['generation'] or state['publication_serial']!=cap._descriptor['serial']:
        raise Refusal('incremental_parent_uncertified')
    if digest(cap._descriptor)!=cap._descriptor_sha256:raise Refusal('incremental_parent_uncertified')
    if store.db.execute('SELECT projection_generation FROM chain.integrity_state WHERE id=1').fetchone()[0]!=cap._descriptor['generation']:raise Refusal('incremental_parent_uncertified')
    if integrity_reference(store.generation())!=cap._descriptor['integrity_ref']:raise Refusal('incremental_parent_uncertified')
    return cap

def _verify_canonical(store):
    """Full verification of the canonical generation, with a checkpoint capability when nothing
    changed the verified contents meanwhile (None otherwise)."""
    from db_sync_alt.verify.store import verify_store
    pid=_session(store);g=store.generation();before=revision(store);store._verified_checkpoint=None
    report=verify_store(store,g['id'])
    cap=None
    if revision(store)==before:
        cap=VerifiedCheckpoint(store,pid,g['id'],before,integrity_reference(g)['sha256'],report)
        store._verified_checkpoint=cap
    return g,pid,cap

def certify_ready_full(store):
    g,pid,_=_verify_canonical(store)
    # Full verification already compared every derived projection binding in a
    # single statement snapshot. This owner still binds the exact ready marker.
    if store.db.execute('SELECT projection_generation FROM chain.integrity_state WHERE id=1').fetchone()[0]!=g['id']:raise Refusal('membership_projection_mismatch')
    cap=VerifiedParent(store,pid,_descriptor(store,g));store._verified_parent=cap
    return cap

def certify_checkpoint(store):
    """Verify the canonical generation in full; the result may become the next integrity checkpoint."""
    cap=_verify_canonical(store)[2]
    if cap is None:raise Refusal('integrity_revision_changed')
    return cap

def verified_checkpoint(store):
    """The checkpoint capability of this writer session's last full verification."""
    cap=getattr(store,'_verified_checkpoint',None)
    _require_checkpoint_capability(store,cap)
    return cap

def _require_checkpoint_capability(store,cap):
    if (not isinstance(cap,VerifiedCheckpoint) or cap is not getattr(store,'_verified_checkpoint',None) or cap._owner is not store
            or cap._session!=_session(store) or cap.revision!=revision(store)
            or digest([cap.generation,cap.revision,cap.root,cap._report])!=cap._sha256
            or cap._report.get('generation')!=cap.generation):raise Refusal('checkpoint_uncertified')

def integrity_base(store):
    """The integrity base generation and its checkpoint row (None when the base has none)."""
    base=store.db.execute('SELECT base_generation FROM chain.integrity_state WHERE id=1').fetchone()[0]
    if not store.supports_checkpoints:return base,None
    row=store.db.execute('SELECT generation,previous_base,root,digests,service_version,checkpointed_at FROM chain.integrity_checkpoints WHERE generation=%s',(base,)).fetchone()
    return base,None if row is None else dict(zip(('generation','previous_base','root','digests','service_version','checkpointed_at'),row))

def insert_checkpoint(store,cap):
    """Insert the checkpoint `cap` certifies and move the integrity base to it.

    Runs inside the caller's transaction, which holds the serving lock with the
    projection-write flag on (the publication gate)."""
    from db_sync_alt import __version__
    _require_checkpoint_capability(store,cap)
    state=store.state()
    if state['revoked']:raise Refusal('checkpoint_database_revoked')
    if not store.supports_checkpoints:raise Refusal('checkpoint_schema_required')
    g=store.generation(cap.generation)
    if g['status']!='ready':raise Refusal('checkpoint_generation_not_ready')
    if state['canonical_ready'] is None or cap.generation!=state['canonical_ready'] and not store.db.execute('''WITH RECURSIVE lineage(id,parent) AS (
          SELECT id,parent FROM chain.generations WHERE id=%s UNION ALL
          SELECT g.id,g.parent FROM lineage l JOIN chain.generations g ON g.id=l.parent)
        SELECT 1 FROM lineage WHERE id=%s''',(state['canonical_ready'],cap.generation)).fetchone():raise Refusal('checkpoint_not_canonical')
    base,_=integrity_base(store)
    if cap.generation<=base:raise Refusal('checkpoint_not_after_base')
    if integrity_reference(g)['sha256']!=cap.root:raise Refusal('checkpoint_uncertified')
    store.db.execute('INSERT INTO chain.integrity_checkpoints(generation,previous_base,root,digests,service_version) VALUES(%s,%s,%s,%s,%s)',
                     (cap.generation,base,cap.root,Jsonb(cap._report['digests']),__version__))
    store.db.execute('UPDATE chain.integrity_state SET base_generation=%s WHERE id=1',(cap.generation,))
    return {'generation':cap.generation,'previous_base':base}

def write_checkpoint(store,cap):
    """One gated transaction: the checkpoint for `cap.generation` and the base move to it."""
    from db_sync_alt.store.database import SERVING_LOCK,PROJECTION_WRITE
    _require_checkpoint_capability(store,cap)
    from db_sync_alt.sources.blockfrost.relevance import checkpoint_anchor
    anchor=checkpoint_anchor(store,cap)
    with store.atomic():
        store.db.execute("SET LOCAL lock_timeout='5s'")
        store.db.execute('SELECT pg_advisory_xact_lock(%s,%s)',SERVING_LOCK)
        store.db.execute('SELECT set_config(%s,%s,true)',(PROJECTION_WRITE,'on'))
        result=insert_checkpoint(store,cap)
        if anchor is not None:store.insert_relevance_anchor(anchor)
        return result

def begin_continuation(store,*,source,start,target,witness,manifest,parent_capability):
    cap=current_verified_parent(store)
    if cap is not parent_capability:raise Refusal('incremental_parent_uncertified')
    if source!='blockfrost-cardano':raise Refusal('incremental_source_unsupported')
    m=copy.deepcopy(manifest)
    if 'completion' in m or 'integrity' in m or form(m) is None:raise Refusal('completion_invalid')
    parent=store.generation();m['parent_ref']=integrity_reference(parent)
    m['parent_digest']=m['parent_ref']['sha256']
    m['integrity']={'version':2,'kind':'pending-delta'}
    gen=store.begin_generation(source=source,start=start,target=target,witness=witness,manifest=m,parent=parent['id'])
    return gen

def resume_continuation(store,generation,parent_capability=None):
    cap=current_verified_parent(store)
    if parent_capability is not None and parent_capability is not cap:raise Refusal('incremental_parent_uncertified')
    g=store.generation(generation)
    if not incremental(g) or g['status']!='pending' or g['parent']!=cap._descriptor['generation']:raise Refusal('incremental_resume_mismatch')
    validate_completion_v2(store,g)
    return g

def stage_metadata_reuse(store,generation,through_height,source_receipt):
    current_verified_parent(store);g=resume_continuation(store,generation);parent=store.generation(g['parent'])
    if type(through_height) is not int or not parent['target_height']<=through_height<=min(parent['witness_height'],g['witness_height']):raise Refusal('metadata_reuse_bounds')
    if not isinstance(source_receipt,dict) or type(source_receipt.get('height')) is not int or source_receipt.get('height')!=through_height or source_receipt.get('hash')!=store.block_at(parent['id'],through_height)['hash'].hex():raise Refusal('metadata_reuse_anchor')
    old=store.db.execute('SELECT through_height,receipt FROM chain.generation_reuse WHERE generation=%s',(generation,)).fetchone()
    if old:
        if old!=(through_height,source_receipt):raise Refusal('metadata_reuse_changed')
        return
    if 'completion' in g['manifest'] or store.db.execute('SELECT 1 FROM chain.generation_blocks WHERE generation=%s',(generation,)).fetchone():raise Refusal('metadata_reuse_too_late')
    with store.atomic():
        store.db.execute('INSERT INTO chain.generation_reuse VALUES(%s,%s,%s)',(generation,through_height,Jsonb(source_receipt)))
        store.checkpoint(generation,'metadata','inherited',{'lo':g['start_height'],'hi':through_height,'parent':g['parent'],'parent_digest':g['manifest']['parent_digest'],'source_kind':'generation-inheritance','inherited_source_kind':parent['source'],'reuse_anchor':source_receipt})


def rebind_pending_witness(store,generation):
    """Bind an unsealed pending witness anchor to the generation's stored header.

    The newest Cardano block is unstable. The witness is the generation's own
    parent-linked stored header at witness_height, not an earlier provider
    observation. Only anchors.witness.hash can change; the verifier then checks
    the stored chain through the witness against it.
    """
    cap=current_verified_parent(store);g=resume_continuation(store,generation,cap)
    m=g['manifest'];anchor=m['anchors']['witness']
    if 'completion' in m or store.checkpoint_rows(generation=generation,phase='final-anchors'):raise Refusal('immutable_generation')
    if anchor.get('height')!=g['witness_height']:raise Refusal('anchor_bounds_mismatch')
    stored=store.block_at(generation,g['witness_height'])
    if stored is None:raise Refusal('metadata_incomplete')
    if anchor.get('hash')==stored['hash'].hex():return m
    new=copy.deepcopy(m);new['anchors']['witness']={'height':g['witness_height'],'hash':stored['hash'].hex()}
    with store.atomic():
        store.require_pending(generation)
        store.db.execute('UPDATE chain.generations SET manifest=%s WHERE id=%s',(Jsonb(new),generation))
    return store.generation(generation)['manifest']

def delta_rows(store,table,gen):
    from db_sync_alt.store.database import logical_row
    with store.db.transaction():
        with store.db.cursor(name='delta_'+table) as cur:
            cur.execute('SELECT '+','.join(TABLES[table])+' FROM chain.'+table+' WHERE first_generation=%s ORDER BY '+','.join(KEYS[table]),(gen,))
            for row in cur:yield logical_row(table,dict(zip(TABLES[table],row)))

def inventory(rows):
    h=hashlib.sha256();count=0
    for row in rows:h.update((canonical(jsonable(row))+'\n').encode());count+=1
    return {'count':count,'sha256':h.hexdigest()}

def _membership_inventory(store,gen,*,removed=False):
    g=store.generation(gen);reuse=store.db.execute('SELECT through_height FROM chain.generation_reuse WHERE generation=%s',(gen,)).fetchone()
    if not reuse:raise Refusal('metadata_reuse_missing')
    if removed:
        if reuse[0]>=store.generation(g['parent'])['witness_height']:return inventory(())
        query='SELECT m.height,encode(m.hash,\'hex\') FROM chain.membership(%s) m WHERE m.height>%s ORDER BY m.height';args=(g['parent'],reuse[0])
        # Normal path uses the verified current index; audit can reconstruct history.
        if store.state()['canonical_ready']==g['parent']:query="SELECT height,encode(hash,'hex') FROM chain.current_membership WHERE height>%s ORDER BY height";args=(reuse[0],)
    else:query="SELECT m.height,encode(m.hash,'hex'),to_jsonb(b)-'id'-'first_generation' FROM chain.membership_delta m JOIN chain.blocks b USING(hash) WHERE m.generation=%s ORDER BY m.height";args=(gen,)
    return inventory(store.db.execute(query,args))

def compute_proof(store,g):
    gen=g['id'];m=g['manifest'];parent=store.generation(g['parent']);reuse=store.db.execute('SELECT through_height,receipt FROM chain.generation_reuse WHERE generation=%s',(gen,)).fetchone()
    if not reuse:raise Refusal('metadata_reuse_missing')
    facts={}
    for table in TABLES:
        def validated():
            for row in delta_rows(store,table,gen):validate_numeric(table,row);yield row
        facts[table]=inventory(validated());store.progress('delta_family_complete',generation=gen,family=table,rows=facts[table]['count'])
    added=_membership_inventory(store,gen);removed=_membership_inventory(store,gen,removed=True)
    oldcounts=parent['manifest']['integrity']['semantic']['counts'] if incremental(parent) else {k:v['count'] for k,v in parent['manifest']['digests'].items()}
    counts={k:oldcounts[k]+facts[k]['count'] for k in TABLES};counts['blocks']=oldcounts['blocks']-removed['count']+added['count']
    checkpoints=[]
    for row in store.checkpoint_rows(generation=gen):
        if row['phase'] in ('final-anchors','completion-revalidation'):continue
        if row['phase']=='relevance' and row['unit']=='anchor':
            from db_sync_alt.sources.blockfrost.relevance import validate_anchor
            validate_anchor(store,g,[row])
        else:checkpoints.append(row)
    kind=form(m)
    if kind is None:raise Refusal('completion_invalid')
    key=PROFILE_FIELD[kind]
    return {'generation':gen,'parent':g['parent'],'parent_ref':integrity_reference(parent),'source':g['source'],'source_identity':m['source_identity'],key:m[key],'contracts':m['contracts'],'start':[g['start_height'],g['start_index']],'target':g['target_height'],'witness':g['witness_height'],'anchors':m['anchors'],'required_epochs':m['required_epochs'],'capabilities':m['capabilities'],'facts':facts,'membership':{'reuse_through':reuse[0],'reuse_receipt_sha256':digest(reuse[1]),'added':added,'removed':removed},'checkpoints_sha256':digest(checkpoints),'counts':counts}

def verify_pending_incremental(store,generation):
    from db_sync_alt.verify.store import verify_structure,validate_prior_answers
    cap=current_verified_parent(store);g=resume_continuation(store,generation,cap)
    before=revision(store);verify_structure(store,g,delta_parent=store.generation(g['parent']))
    try:validate_prior_answers(store,generation)
    except Refusal:store.mark_conflict();raise
    proof=compute_proof(store,g)
    if before!=revision(store):raise Refusal('incremental_revision_changed')
    if 'completion' in g['manifest']:validate_completion_v2(store,g,proof=proof,require_seal=True)
    ticket=VerifiedTransition(store,_session(store),generation,before,proof,digest(g['manifest']));store._transition_ticket=ticket
    return ticket

def _ticket(store,generation,ticket):
    current_verified_parent(store)
    if ticket is not getattr(store,'_transition_ticket',None) or not isinstance(ticket,VerifiedTransition) or ticket._owner is not store or ticket._session!=_session(store) or ticket.generation!=generation or ticket.revision!=revision(store):raise Refusal('incremental_ticket_invalid')
    if digest(ticket.proof)!=ticket._proof_sha256:raise Refusal('incremental_ticket_invalid')
    if digest(store.generation(generation)['manifest'])!=ticket.manifest_sha256:raise Refusal('incremental_ticket_invalid')

def validate_completion_v2(store,g,*,proof=None,require_seal=False,manifest=None):
    try:return _validate_completion_v2(store,g,proof=proof,require_seal=require_seal,manifest=manifest)
    except (KeyError,TypeError,ValueError,AttributeError,IndexError):raise Refusal('completion_invalid') from None

def _validate_completion_v2(store,g,*,proof=None,require_seal=False,manifest=None):
    from db_sync_alt.verify.completion import _bounded,_latest
    from db_sync_alt.networks import for_identity
    m=g['manifest'];integrity=m.get('integrity');original=store.checkpoint_rows(generation=g['id'],phase='final-anchors')
    if not incremental(g) or not isinstance(integrity,dict) or type(integrity.get('version')) is not int:raise Refusal('completion_invalid')
    parent=store.generation(g['parent']);network=for_identity(store.meta('profile'))
    if m.get('parent_ref')!=integrity_reference(parent) or m.get('parent_digest')!=m['parent_ref']['sha256'] or g['source']!='blockfrost-cardano' or network is None or m.get('source_identity')!=network.source_identity:raise Refusal('completion_mismatch')
    if 'completion' not in m:
        if require_seal or original or integrity!={'version':2,'kind':'pending-delta'} or form(m) is None:raise Refusal('completion_invalid')
        return None
    seal=m['completion'];kind=integrity.get('kind')
    if set(integrity)!={'version','kind','root','semantic'} or kind not in PROFILE_FIELD or kind!=form(m) or set(seal)!={'version','sha256','original_receipt_sha256'} or type(seal['version']) is not int or seal['version']!=2:raise Refusal('completion_invalid')
    if len(original)!=1 or original[0]['unit']!='complete' or seal['original_receipt_sha256']!=_bounded(original[0]['receipt']):raise Refusal('completion_invalid')
    if integrity['root']!=digest(integrity['semantic']) or seal['sha256']!=digest({'integrity':integrity,'original_receipt_sha256':seal['original_receipt_sha256']}):raise Refusal('completion_mismatch')
    semantic=integrity['semantic']
    _bounded(semantic)
    key=PROFILE_FIELD[kind]
    expected_keys={'generation','parent','parent_ref','source','source_identity',key,'contracts','start','target','witness','anchors','required_epochs','capabilities','facts','membership','checkpoints_sha256','counts'}
    from db_sync_alt.verify.completion import _hash
    if not isinstance(semantic,dict) or set(semantic)!=expected_keys or 'digests' in m:raise Refusal('completion_invalid')
    if kind==KIND_V2 and not _hash(m[key]):raise Refusal('completion_invalid')
    if set(semantic.get('facts',{}))!=set(TABLES) or set(semantic.get('counts',{}))!=set(TABLES):raise Refusal('completion_invalid')
    for value in semantic['facts'].values():
        if not isinstance(value,dict) or set(value)!={'count','sha256'} or type(value['count']) is not int or value['count']<0 or not _hash(value['sha256']):raise Refusal('completion_invalid')
    if any(type(n) is not int or n<0 for n in semantic['counts'].values()) or not _hash(semantic['checkpoints_sha256']):raise Refusal('completion_invalid')
    membership=semantic['membership']
    if not isinstance(membership,dict) or set(membership)!={'reuse_through','reuse_receipt_sha256','added','removed'} or type(membership['reuse_through']) is not int or not parent['target_height']<=membership['reuse_through']<=min(parent['witness_height'],g['witness_height']) or not _hash(membership['reuse_receipt_sha256']):raise Refusal('completion_invalid')
    for value in (membership['added'],membership['removed']):
        if not isinstance(value,dict) or set(value)!={'count','sha256'} or type(value['count']) is not int or value['count']<0 or not _hash(value['sha256']):raise Refusal('completion_invalid')
    for name,value in {'generation':g['id'],'parent':g['parent'],'parent_ref':m['parent_ref'],'source':g['source'],'source_identity':m['source_identity'],key:m[key],'contracts':m['contracts'],'start':[g['start_height'],g['start_index']],'target':g['target_height'],'witness':g['witness_height'],'anchors':m['anchors'],'required_epochs':m['required_epochs'],'capabilities':m['capabilities']}.items():
        if digest(semantic.get(name))!=digest(value):raise Refusal('completion_mismatch')
    if proof is not None and digest(proof)!=integrity['root']:raise Refusal('completion_mismatch')
    if manifest is not None and digest(manifest)!=digest(m):raise Refusal('completion_mismatch')
    if m.get('validation',{}).get('kind')!='provider-enumeration' or not store.completed(g['id'],'handoff','initial'):raise Refusal('provider_completeness_receipt_missing')
    _latest(store,g['id'],seal['original_receipt_sha256'])
    return seal

def finalize_pending_incremental(store,generation,manifest,receipt,ticket):
    from db_sync_alt.verify.completion import _bounded,_latest,MAX_REVALIDATIONS
    _ticket(store,generation,ticket);g=store.generation(generation)
    if g['status']!='pending':raise Refusal('immutable_generation')
    if 'completion' in g['manifest']:
        seal=validate_completion_v2(store,g,proof=ticket.proof,require_seal=True,manifest=manifest)
        previous=_latest(store,generation,seal['original_receipt_sha256']);count=previous['count']+1 if previous else 1
        if count>MAX_REVALIDATIONS:raise Refusal('completion_invalid')
        latest={'version':1,'count':count,'previous':previous['sha256'] if previous else seal['original_receipt_sha256'],'receipt_sha256':_bounded(receipt),'receipt':copy.deepcopy(receipt)}
        latest['sha256']=digest(latest)
        with store.atomic():
            _ticket(store,generation,ticket)
            store.db.execute("INSERT INTO chain.checkpoints VALUES(%s,'completion-revalidation','latest',%s) ON CONFLICT(generation,phase,unit) DO UPDATE SET receipt=excluded.receipt",(generation,Jsonb(latest)))
        return g['manifest']
    m=copy.deepcopy(manifest)
    # Only validation/provenance labels may be supplied at finalization.
    original={k:v for k,v in g['manifest'].items() if k not in ('validation','provider_attempts_this_invocation')}
    candidate={k:v for k,v in m.items() if k not in ('validation','provider_attempts_this_invocation')}
    if digest(original)!=digest(candidate):raise Refusal('completion_mismatch')
    # The kind follows the manifest's form, fixed when the generation began.
    kind=form(m);rh=_bounded(receipt);m['integrity']={'version':2,'kind':kind,'root':digest(ticket.proof),'semantic':ticket.proof}
    m['validation']={'kind':'provider-enumeration','integrity_kind':kind}
    m['completion']={'version':2,'original_receipt_sha256':rh,'sha256':digest({'integrity':m['integrity'],'original_receipt_sha256':rh})}
    with store.atomic():
        _ticket(store,generation,ticket)
        store.checkpoint(generation,'final-anchors','complete',receipt)
        store.db.execute('UPDATE chain.generations SET manifest=%s WHERE id=%s',(Jsonb(m),generation))
    new=VerifiedTransition(store,ticket._session,generation,revision(store),ticket.proof,digest(m));store._transition_ticket=new
    # Keep the originally issued object as the API handle, mapping to its sealed revision.
    store._sealed_ticket=(ticket,new)
    return m

def publish_incremental(store,generation,ticket,*,stop_requested=None):
    from db_sync_alt.store.database import SERVING_LOCK,PROJECTION_WRITE
    if (getattr(store,'_sealed_ticket',None) or (None,None))[0] is ticket:ticket=store._sealed_ticket[1]
    _ticket(store,generation,ticket);g=store.generation(generation);validate_completion_v2(store,g,proof=ticket.proof,require_seal=True)
    if stop_requested and stop_requested():raise Refusal('interrupted')
    with store.atomic():
        store.db.execute('SELECT pg_advisory_xact_lock(%s,%s)',SERVING_LOCK);_ticket(store,generation,ticket)
        if stop_requested and stop_requested():raise Refusal('interrupted')
        store.db.execute('SELECT set_config(%s,%s,true)',(PROJECTION_WRITE,'on'))
        cutoff=ticket.proof['membership']['reuse_through']
        store.db.execute('DELETE FROM chain.current_membership WHERE height>%s',(cutoff,))
        store.db.execute('INSERT INTO chain.current_membership SELECT height,hash FROM chain.membership_delta WHERE generation=%s',(generation,))
        store.db.execute("UPDATE chain.generations SET status='ready' WHERE id=%s",(generation,))
        store.db.execute('UPDATE chain.service_state SET canonical_ready=%s,publication_serial=publication_serial+1 WHERE id=1',(generation,))
        store.db.execute('UPDATE chain.integrity_state SET projection_generation=%s WHERE id=1',(generation,))
        if store.serves_directly:
            from db_sync_alt.store import serve
            serve.advance(store.db,generation,cutoff=cutoff,bindings='SELECT height,hash FROM chain.membership_delta WHERE generation=%s',bindings_args=(generation,))
    store._verified_parent=VerifiedParent(store,_session(store),_descriptor(store,store.generation()))
    store._transition_ticket=None;store._sealed_ticket=None
    return {'verification':'certified-parent-delta','generation':generation,'integrity':g['manifest']['integrity'],'source_completeness':'provider-enumeration'}
