"""PostgreSQL integrity, inherited completeness, and immutable answer-domain checks."""
from db_sync_alt.contract import FORMAT, LEGACY_FORMAT, Refusal, digest, required_epochs, generation_capabilities, epoch_size
from db_sync_alt.networks import for_identity


def verify_projection(store,generation):
    """Compare every reconstructed/indexed binding in one statement snapshot.

    Tagged rows share a single cursor/snapshot even for read-only callers while
    another session publishes. Pairing also rejects duplicate canonical heights;
    EXCEPT would silently remove identical duplicates. Memory stays bounded by
    the cursor batch, and the ordinary statement timeout remains unchanged.
    """
    import uuid
    store.progress('projection_verification',generation=generation,rows=0)
    count=0;previous=None
    with store.db.transaction():
        with store.db.cursor(name='projection_'+uuid.uuid4().hex) as cur:
            cur.itersize=1000
            cur.execute('''SELECT height::integer,hash::bytea,0 AS side FROM chain.membership(%s)
                           UNION ALL SELECT height::integer,hash::bytea,1 AS side FROM chain.current_membership
                           ORDER BY 1,3,2''',(generation,))
            rows=iter(cur)
            for expected in rows:
                actual=next(rows,None)
                if (expected[2]!=0 or actual is None or actual[2]!=1
                        or expected[:2]!=actual[:2]
                        or (previous is not None and expected[0]<=previous)):
                    raise Refusal('membership_projection_mismatch')
                previous=expected[0];count+=1
                if count%1000==0:store.progress('projection_verification',generation=generation,rows=count)
    store.progress('projection_verification_complete',generation=generation,rows=count)


def validate_publication(store,g,computed):
    from db_sync_alt.verify.completion import validate_completion,completion_required,SQL_VALIDATION
    validate_completion(store,g,computed,require_seal=completion_required(store,g))
    m=g['manifest'];validation=m.get('validation',{})
    if m.get('digests')!=computed:raise Refusal('semantic_digest_mismatch')
    if g['source']=='cardano-db-sync-postgresql':
        kinds=(SQL_VALIDATION,) if completion_required(store,g) else ('independent-sql-family-inventory-v1',)
        if validation.get('kind') not in kinds or digest(validation.get('digests'))!=digest(computed):raise Refusal('sql_completeness_receipt_missing')
        if validation.get('kind')==SQL_VALIDATION:
            original=store.checkpoint_rows(generation=g['id'],phase='final-anchors',unit='complete')
            if len(original)!=1 or digest(original[0]['receipt'].get('independent'))!=digest(validation.get('independent')):raise Refusal('sql_completeness_receipt_missing')
    elif g['source']=='blockfrost-cardano':
        if validation.get('kind')!='provider-enumeration' or not store.completed(g['id'],'handoff','initial') or not store.completed(g['id'],'final-anchors','complete'):raise Refusal('provider_completeness_receipt_missing')
    else:raise Refusal('unsupported_source_kind')


def mapping_policy(mapping):
    alphabet='qpzry9x8gf2tvdw0s3jn54khce6mua7l';acc=bits=0;raw=[]
    for char in mapping.rsplit('1',1)[1][:-6]:
        acc=(acc<<5)|alphabet.index(char);bits+=5
        while bits>=8:bits-=8;raw.append((acc>>bits)&255)
    return bytes(raw)[1:29]


def selected_asset_units(identity):
    configs=identity['configs'];cn=configs['cnight-config']['addresses']
    governance={v['policy_id'] for v in configs['federated-authority-config'].values()}
    units={cn['cnight_policy_id']+cn['cnight_asset_name'].encode().hex(),mapping_policy(cn['mapping_validator_address']).hex(),configs['permissioned-candidates-config']['permissioned_candidates_policy_id'].removeprefix('0x')}|governance
    for name in ('ics-config','reserve-config'):
        asset=configs[name]['asset'];units.add(asset['policy_id'].removeprefix('0x')+asset['asset_name'].encode().hex())
    return units,governance


def expected_capabilities(store,g):
    """The capabilities a generation is sealed with: those of its format, with bridge metadata exactly
    for a Blockfrost acquisition (cardano-db-sync acquisitions never read tx_metadata)."""
    return generation_capabilities(legacy=store.legacy_generation(g['id']),bridge_metadata=g['source']=='blockfrost-cardano')


def verify_structure(store,g,*,require_ready=False,delta_parent=None):
    gen=g['id'];m=g['manifest'];db=store.db
    delta=delta_parent is not None
    if require_ready and (g['status']!='ready' or store.meta('source_conflict')):raise Refusal('generation_not_canonical_ready')
    if db.execute("SELECT 1 FROM pg_constraint WHERE connamespace='chain'::regnamespace AND NOT convalidated LIMIT 1").fetchone():raise Refusal('storage_integrity_failed')
    identity=store.meta('profile');network=for_identity(identity)
    # The profile must be the packaged profile of the database's own network.
    if network is None or network!=store.network:raise Refusal('untrusted_profile_identity')
    units,governance=selected_asset_units(identity)
    from db_sync_alt.store.incremental import integrity_reference,profile_digest
    from db_sync_alt import bridge
    # Generations sealed in the nonce-only format keep its contract and capabilities.
    legacy=store.legacy_generation(gen)
    if m.get('contracts')!=digest(network.legacy_contract if legacy else network.contract) or profile_digest(g)!=digest(identity):raise Refusal('manifest_contract_mismatch')
    if any(type(v) is not bool for v in m.get('capabilities',{}).values()):raise Refusal('unsupported_capabilities')
    if m.get('capabilities')!=expected_capabilities(store,g):raise Refusal('unsupported_capabilities')
    bridge.verify_assets(store,g,units,governance,identity,delta=delta)
    if g['witness_height']!=g['target_height']+network.security:raise Refusal('invalid_witness')
    if (g['start_height'],g['start_index'])!=network.start or g['target_height']<g['start_height']:raise Refusal('invalid_start_cursor')
    anchors=m.get('anchors',{});expected_anchors={'start':g['start_height'],'target':g['target_height'],'witness':g['witness_height']}
    if set(anchors)!=set(expected_anchors):raise Refusal('invalid_anchor_set')
    for name,height in expected_anchors.items():
        if anchors[name].get('height')!=height or not isinstance(anchors[name].get('hash'),str) or len(anchors[name]['hash'])!=64:raise Refusal('anchor_bounds_mismatch')
    if not delta and db.execute("SELECT 1 FROM chain.generations WHERE id<%s AND status!='ready'",(gen,)).fetchone():raise Refusal('invalid_lineage')
    if not delta and db.execute('SELECT height FROM chain.membership(%s) GROUP BY height HAVING count(*)<>1 LIMIT 1',(gen,)).fetchone():raise Refusal('conflicting_inherited_membership')
    expected=g['start_height'];previous=None
    metadata_query='SELECT height,hash FROM chain.membership(%s)';metadata_args=(gen,)
    if delta:
        reuse=db.execute('SELECT through_height FROM chain.generation_reuse WHERE generation=%s',(gen,)).fetchone()
        if not reuse:raise Refusal('metadata_reuse_missing')
        if db.execute('SELECT 1 FROM chain.membership_delta d JOIN chain.current_membership c USING(height) WHERE d.generation=%s AND c.height<=%s LIMIT 1',(gen,reuse[0])).fetchone():raise Refusal('conflicting_inherited_membership')
        if db.execute('SELECT 1 FROM chain.membership_delta d JOIN chain.blocks b USING(hash) WHERE d.generation=%s AND d.height<>b.height LIMIT 1',(gen,)).fetchone():raise Refusal('metadata_gap_or_parent_conflict')
        expected=reuse[0]+1;previous=store.block_at(gen,reuse[0])['hash']
        metadata_query,metadata_args=store.membership_sql(gen)
    # The full path materializes the lineage membership and the block headers before the join. Inlined into the
    # range join, the recursive membership function can take seconds to plan on a long history. PostgreSQL also
    # estimates the materialized membership at a small fraction of its real size, so a join against chain.blocks
    # itself becomes one primary-key lookup per membership row and outruns the statement timeout. With both sides
    # materialized, PostgreSQL hash-joins the two scans.
    if delta:query='SELECT m.height,b.hash,b.parent_hash,b.height FROM ('+metadata_query+') m LEFT JOIN chain.blocks b ON b.hash=m.hash WHERE m.height BETWEEN %s AND %s ORDER BY m.height'
    else:query='WITH m AS MATERIALIZED ('+metadata_query+'), b AS MATERIALIZED (SELECT hash,parent_hash,height FROM chain.blocks) SELECT m.height,b.hash,b.parent_hash,b.height FROM m LEFT JOIN b ON b.hash=m.hash WHERE m.height BETWEEN %s AND %s ORDER BY m.height'
    # Bounded stream, not fetchall of full history.
    with db.transaction():
        with db.cursor(name='verify_metadata') as cur:
            cur.execute(query,(*metadata_args,expected,g['witness_height']))
            for height,h,parent,actual in cur:
                if height!=expected or actual!=height or h is None or (previous is not None and parent!=previous):raise Refusal('metadata_gap_or_parent_conflict')
                previous=h;expected+=1
                if (expected-g['start_height'])%10000==0:store.progress('verify_metadata',generation=gen,rows=expected-g['start_height'])
    if expected!=g['witness_height']+1:raise Refusal('metadata_incomplete')
    store.progress('verify_metadata_complete',generation=gen,rows=expected-g['start_height'])
    for name,anchor in anchors.items():
        row=store.block_at(gen,anchor['height'])
        if row is None or row['hash'].hex()!=anchor['hash']:raise Refusal('anchor_mismatch')
    if not isinstance(m.get('required_epochs'),list) or any(type(e) is not int for e in m['required_epochs']):raise Refusal('epoch_bounds_mismatch')
    first=store.block_at(gen,g['start_height']);target=store.block_at(gen,g['target_height'])
    if first is None or target is None or m.get('required_epochs')!=required_epochs(first['epoch'],target['epoch']):raise Refusal('epoch_bounds_mismatch')
    membership,args=store.membership_sql(gen) if delta else ('SELECT * FROM chain.membership(%s)',(gen,))
    scope='=' if delta else '<='
    if db.execute('SELECT 1 FROM chain.transactions t JOIN chain.blocks b ON b.hash=t.block_hash LEFT JOIN ('+membership+') m ON m.hash=b.hash WHERE t.first_generation'+scope+'%s AND (m.hash IS NULL OR b.height>%s OR t.tx_index>=b.tx_count OR t.valid NOT IN (0,1)) LIMIT 1',(*args,gen,g['target_height'])).fetchone():raise Refusal('transaction_coverage_invalid')
    # Physical FK validity alone does not guarantee referenced facts belong to this prefix.
    relationships=[('transactions','blocks','c.block_hash=p.hash'),('outputs','transactions','c.tx_hash=p.hash'),('outputs','datums','c.datum_hash=p.hash'),('inputs','transactions','c.tx_hash=p.hash'),('inputs','outputs','c.origin_hash=p.tx_hash AND c.origin_index=p.output_index'),('output_assets','outputs','c.tx_hash=p.tx_hash AND c.output_index=p.output_index'),('output_assets','assets','c.policy=p.policy AND c.name=p.name'),('mints','transactions','c.tx_hash=p.hash'),('mints','assets','c.policy=p.policy AND c.name=p.name')]
    for child,parent,join in relationships:
        if db.execute('SELECT 1 FROM chain.'+child+' c JOIN chain.'+parent+' p ON '+join+' WHERE c.first_generation'+scope+'%s AND p.first_generation>c.first_generation LIMIT 1',(gen,)).fetchone():raise Refusal('dependency_generation_invalid')
    if not delta and db.execute('SELECT 1 FROM chain.inputs WHERE first_generation<=%s GROUP BY origin_hash,origin_index HAVING count(*)>1 LIMIT 1',(gen,)).fetchone():raise Refusal('contradictory_ordinary_spend')
    for checkpoint in store.checkpoint_rows(generation=gen):
        phase=checkpoint['phase'];receipt=checkpoint['receipt']
        if phase=='relevance' and checkpoint['unit']=='anchor':
            from db_sync_alt.sources.blockfrost.relevance import validate_anchor
            validate_anchor(store,g,[checkpoint])
        if phase not in ('events','metadata','prerequisites','epochs'):continue
        kind=receipt.get('source_kind')
        if kind=='generation-inheritance':
            if phase!='metadata' or g['parent'] is None or receipt.get('parent')!=g['parent'] or receipt.get('inherited_source_kind')!=store.generation(g['parent'])['source'] or receipt.get('parent_digest')!=integrity_reference(store.generation(g['parent']))['sha256']:raise Refusal('invalid_inheritance_receipt')
        elif kind!=g['source']:raise Refusal('receipt_source_kind_mismatch')
    parent_target=store.generation(g['parent'])['target_height'] if g['parent'] is not None else g['start_height']-1
    for phase,start,end in [('events',parent_target+1,g['target_height']),('metadata',g['start_height'],g['witness_height'])]:
        intervals=[(c['receipt']['lo'],c['receipt']['hi']) for c in store.checkpoint_rows(generation=gen,phase=phase)]
        position=start
        for lo,hi in sorted(intervals):
            if hi<start:continue
            if lo!=position or hi<lo:raise Refusal('checkpoint_gap_or_overlap')
            position=hi+1
        if position!=end+1:raise Refusal('checkpoint_incomplete')
    # A start cursor at block 0 leaves no history before it to acquire.
    from db_sync_alt.verify.prerequisites import prerequisite_coverage
    prerequisite_coverage(store,g)
    for epoch in m['required_epochs']:
        # Every completed required epoch has its end block: the node's latest block of an epoch
        # it reads must be exact, never an older block.
        if epoch<target['epoch'] and not any(c['receipt'].get('epoch_end') for c in store.checkpoint_rows(phase='epochs',unit=epoch,before=gen+1)):raise Refusal('epoch_end_missing')
        if delta and epoch in delta_parent['manifest']['required_epochs']:continue
        if store.epoch_nonce(epoch,gen) is None or not db.execute('SELECT 1 FROM chain.epoch_stakes WHERE epoch=%s AND first_generation<=%s',(epoch,gen)).fetchone():raise Refusal('epoch_incomplete')
        if not store.checkpoint_rows(phase='epochs',unit=epoch,before=gen+1):raise Refusal('epoch_receipt_missing')
    # Preserve the old serving projection's admission check: an epoch-end
    # receipt only enables state queries when its retained canonical header
    # matches both epoch and hash. Pre-S endpoints need not include a successor.
    for checkpoint in store.checkpoint_rows(phase='epochs',**({'generation':gen} if delta else {'before':gen+1})):
        receipt=checkpoint['receipt'];anchor=receipt.get('epoch_end')
        # A provider epoch receipt of the current format names the size it read; the stored fact
        # must hold it. Receipts sealed in the nonce-only format stay as they were written.
        if receipt.get('source_kind')=='blockfrost-cardano' and not store.legacy_generation(checkpoint['generation']):
            fact=store.epoch_nonce(int(checkpoint['unit']),gen)
            try:size=epoch_size(receipt.get('max_tx_size'))
            except Refusal:raise Refusal('epoch_parameter_receipt_mismatch') from None
            if fact is None or fact.get('max_tx_size')!=size:raise Refusal('epoch_parameter_receipt_mismatch')
        if anchor:
            epoch=receipt.get('epoch')
            if type(epoch) is not int or checkpoint['unit']!=str(epoch) or not isinstance(anchor,dict) or type(anchor.get('height')) is not int or anchor['height']>g['target_height']:raise Refusal('epoch_anchor_mismatch')
            row=store.block_at(gen,anchor['height'])
            if row is None or row['epoch']!=epoch or row['hash'].hex()!=anchor.get('hash'):raise Refusal('epoch_anchor_mismatch')
    bridge.verify_family(store,g,identity,delta=delta)
    store.progress('verify_structure_complete',generation=gen)
    if g['manifest'].get('relevance_coverage') is not None:
        from db_sync_alt.sources.blockfrost.relevance import verify_generation
        verify_generation(store,gen)
    if delta:
        # A new binding of an existing pre-S physical header is still a delta;
        # it must belong to an actual retained transaction or epoch-end closure.
        for height,h in db.execute('SELECT height,hash FROM chain.membership_delta WHERE generation=%s AND (height<%s OR height>%s)',(gen,g['start_height'],g['witness_height'])):
            if height>g['witness_height']:raise Refusal('unexpected_membership')
            tx=db.execute('SELECT 1 FROM chain.transactions WHERE block_hash=%s AND first_generation<=%s LIMIT 1',(h,gen)).fetchone()
            epochs=store.checkpoint_rows(generation=gen,phase='epochs')
            if not tx and not any(c['receipt'].get('epoch_end',{}).get('hash')==h.hex() for c in epochs):raise Refusal('unexpected_membership')


def verify_checkpoint(store,g,computed,checkpoint,*,require_ready=True):
    """The integrity base against its checkpoint: the recomputed digests, the root, the counts
    and the chain of checkpoints back to the full-certified generation they started from."""
    from db_sync_alt.store.incremental import incremental,integrity_reference,validate_completion_v2
    if checkpoint['digests']!=computed:raise Refusal('checkpoint_digest_mismatch')
    if checkpoint['root']!=integrity_reference(g)['sha256']:raise Refusal('checkpoint_root_mismatch')
    if incremental(g):
        validate_completion_v2(store,g,require_seal=require_ready)
        if g['manifest']['integrity']['semantic']['counts']!={k:v['count'] for k,v in computed.items()}:raise Refusal('incremental_count_mismatch')
    else:
        from db_sync_alt.verify.completion import validate_completion
        validate_completion(store,g,computed)
        if require_ready:validate_publication(store,g,computed)
    chain=store.db.execute('SELECT generation,previous_base FROM chain.integrity_checkpoints ORDER BY generation').fetchall()
    first=store.generation(chain[0][1])
    if chain[-1][0]!=g['id'] or incremental(first) or any(previous!=earlier for (earlier,_),(_,previous) in zip(chain,chain[1:])):
        raise Refusal('checkpoint_chain_mismatch')


def verify_store(store,generation=None,*,require_ready=True):
    from db_sync_alt.store.incremental import incremental,compute_proof,validate_completion_v2,integrity_base
    g=store.generation(generation);gen=g['id'];m=g['manifest']
    record=getattr(store,'epoch_upgrade',None)
    if record is not None and gen>=record['generation']:
        # Sizes the epoch parameter upgrade added to epochs sealed before it.
        from db_sync_alt.store.epoch_parameters import verify_record
        verify_record(store)
    verify_structure(store,g,require_ready=require_ready)
    computed=store.digests(gen)
    base,checkpoint=integrity_base(store) if store.supports_incremental else (None,None)
    if checkpoint is not None and gen<base:raise Refusal('generation_before_integrity_base')
    if checkpoint is not None and gen==base:
        verify_checkpoint(store,g,computed,checkpoint,require_ready=require_ready)
    elif incremental(g):
        if base>=gen:raise Refusal('invalid_incremental_base')
        verify_store(store,base)
        validate_completion_v2(store,g,proof=compute_proof(store,g),require_seal=require_ready)
        if 'completion' in m and m['integrity']['semantic']['counts']!={k:v['count'] for k,v in computed.items()}:raise Refusal('incremental_count_mismatch')
        # Startup is a full audit from the integrity base: the base's digests are recomputed and
        # compared with its checkpoint (or its own manifest), never trusted, and every transition
        # published after the base is verified again.
        for (ancestor,) in store.db.execute('SELECT id FROM chain.generations WHERE id>%s AND id<%s ORDER BY id',(base,gen)):
            old=store.generation(ancestor)
            if incremental(old):validate_completion_v2(store,old,proof=compute_proof(store,old),require_seal=True)
            else:verify_store(store,ancestor)  # explicit SQL/source-switch full certificate
    else:
        from db_sync_alt.verify.completion import validate_completion
        validate_completion(store,g,computed)
        if require_ready:validate_publication(store,g,computed)
    if store.supports_incremental:
        projection=store.db.execute('SELECT projection_generation FROM chain.integrity_state WHERE id=1').fetchone()[0]
        if gen==store.state()['canonical_ready'] and projection!=gen:raise Refusal('membership_projection_mismatch')
    else:projection=None
    if gen==projection:
        verify_projection(store,gen)
        if store.serves_directly:
            from db_sync_alt.store import serve
            serve.verify(store,gen)
    store.progress('verification_complete',generation=gen)
    return {'verification':'offline-integrity','generation':gen,'format':LEGACY_FORMAT if store.legacy_generation(gen) else FORMAT,'digests':computed,'semantic_sha256':digest(computed),'source_completeness':m.get('validation',{}).get('kind','not-certified')}


def validate_prior_answers(store,generation):
    """Refuse additions that change already certified query domains.

    Exact event selectors mirror the pinned native query predicates. State
    selectors conservatively reject historical outputs/spends at supported
    governance/permissioned addresses/assets; no arbitrary pre-S ban.
    """
    g=store.generation(generation)
    if g['parent'] is None:return
    parent=store.generation(g['parent']);pid=parent['id'];db=store.db
    configs=store.meta('profile')['configs'];cn=configs['cnight-config'];mapping=cn['addresses']['mapping_validator_address']
    night_policy=bytes.fromhex(cn['addresses']['cnight_policy_id']);night_name=cn['addresses']['cnight_asset_name'].encode()
    auth_policy=mapping_policy(mapping)
    start=(parent['start_height'],parent['start_index']);end=parent['target_height']
    interval='(b.height,t.tx_index)>=(%s,%s) AND b.height<=%s'
    creation="""SELECT 1 FROM chain.output_assets a JOIN chain.outputs o ON o.tx_hash=a.tx_hash AND o.output_index=a.output_index JOIN chain.transactions t ON t.hash=o.tx_hash JOIN chain.blocks b ON b.hash=t.block_hash WHERE (a.first_generation>%s OR o.first_generation>%s OR t.first_generation>%s) AND a.first_generation<=%s AND """+interval+""" AND ((a.policy=%s AND a.name=%s) OR (o.address=%s AND o.datum_hash IS NOT NULL AND a.policy=%s AND a.name=''::bytea AND a.quantity=1)) LIMIT 1"""
    if db.execute(creation,(pid,pid,pid,generation,*start,end,night_policy,night_name,mapping,auth_policy)).fetchone():raise Refusal('historical_selected_event_changed')
    spending="""SELECT 1 FROM chain.inputs i JOIN chain.transactions t ON t.hash=i.tx_hash JOIN chain.blocks b ON b.hash=t.block_hash JOIN chain.outputs o ON o.tx_hash=i.origin_hash AND o.output_index=i.origin_index WHERE (i.first_generation>%s OR EXISTS(SELECT 1 FROM chain.output_assets changed WHERE changed.tx_hash=o.tx_hash AND changed.output_index=o.output_index AND changed.policy=%s AND changed.name=%s AND changed.first_generation>%s AND changed.first_generation<=%s)) AND i.first_generation<=%s AND """+interval+""" AND ((o.address=%s AND o.datum_hash IS NOT NULL) OR EXISTS(SELECT 1 FROM chain.output_assets a WHERE a.tx_hash=o.tx_hash AND a.output_index=o.output_index AND a.policy=%s AND a.name=%s)) LIMIT 1"""
    if db.execute(spending,(pid,night_policy,night_name,pid,generation,generation,*start,end,mapping,night_policy,night_name)).fetchone():raise Refusal('historical_selected_event_changed')
    permissioned=bytes.fromhex(configs['permissioned-candidates-config']['permissioned_candidates_policy_id'].removeprefix('0x'))
    gov=[(v['address'],bytes.fromhex(v['policy_id'])) for v in configs['federated-authority-config'].values()]
    for address,policy in [(None,permissioned),*gov]:
        selector='a.policy=%s'+(' AND o.address=%s' if address else '')
        params=(policy,address) if address else (policy,)
        base=' FROM chain.output_assets a JOIN chain.outputs o ON o.tx_hash=a.tx_hash AND o.output_index=a.output_index JOIN chain.transactions t ON t.hash=o.tx_hash JOIN chain.blocks b ON b.hash=t.block_hash WHERE '+selector
        if db.execute('SELECT 1'+base+' AND b.height<=%s AND (a.first_generation>%s OR o.first_generation>%s) AND a.first_generation<=%s LIMIT 1',(*params,end,pid,pid,generation)).fetchone():raise Refusal('historical_state_changed')
        if db.execute('SELECT 1 FROM chain.inputs i JOIN chain.transactions spending ON spending.hash=i.tx_hash JOIN chain.blocks sb ON sb.hash=spending.block_hash JOIN chain.outputs o ON o.tx_hash=i.origin_hash AND o.output_index=i.origin_index JOIN chain.output_assets a ON a.tx_hash=o.tx_hash AND a.output_index=o.output_index WHERE '+selector+' AND sb.height<=%s AND i.first_generation>%s AND i.first_generation<=%s LIMIT 1',(*params,end,pid,generation)).fetchone():raise Refusal('historical_state_changed')
    for table in ('epoch_nonces','epoch_stakes'):
        if db.execute('SELECT 1 FROM chain.'+table+' WHERE epoch=ANY(%s) AND first_generation>%s AND first_generation<=%s LIMIT 1',(parent['manifest']['required_epochs'],pid,generation)).fetchone():raise Refusal('historical_epoch_changed')


def physical_inventory(store):
    """Hash all durable rows/IDs, including obsolete witness facts and receipts."""
    import hashlib
    from db_sync_alt.contract import KEYS, canonical
    order={**{table:','.join(KEYS[table]) for table in store.physical_tables},'metadata':'key','generations':'id','generation_blocks':'generation,height','checkpoints':'generation,phase,unit','provenance':'generation,kind,key','pools':'pool_hash','sync_jobs':'id'}
    if store.supports_incremental:order.update(integrity_state='id',current_membership='height',generation_reuse='generation',membership_delta='generation,height')
    if store.supports_checkpoints:order.update(integrity_checkpoints='generation')
    if store.serves_directly:
        from db_sync_alt.store import serve
        order.update({'serve.'+table:serve.KEYS[table] for table in serve.tables(store.db)})
    result={}
    for table,key in order.items():
        hasher=hashlib.sha256();count=0
        with store.db.transaction():
            # Timestamps with a time zone serialize the same on every server.
            store.db.execute("SET LOCAL TimeZone='UTC'")
            with store.db.cursor(name='physical_'+table.replace('.','_')) as cur:
                cur.execute('SELECT to_jsonb(t) FROM '+(table if '.' in table else 'chain.'+table)+' t ORDER BY '+key)
                for row, in cur:
                    hasher.update((canonical(row)+'\n').encode());count+=1
                    if count%10000==0:store.progress('physical_inventory',family=table,rows=count)
        result[table]={'count':count,'sha256':hasher.hexdigest()}
        store.progress('physical_inventory_complete',family=table,rows=count)
    return result
