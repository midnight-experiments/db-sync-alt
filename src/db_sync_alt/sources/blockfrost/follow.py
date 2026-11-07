"""Session-qualified incremental provider continuation of a ready schema-2 store.

Event, CBOR and epoch acquisition use the same enumeration as the full update in
`adapter.update`. Only source continuity, provisional metadata reuse and the
certified-parent proof publication differ.
"""
import copy
import weakref
from db_sync_alt.sources.blockfrost.adapter import (OPENAPI, BlockfrostSource, block_row,
    check_anchor, handoff_overlap, diagnostic_context, complete_inherited_epoch,
    epoch_facts)
from db_sync_alt.contract import Refusal, digest, required_epochs

_QUALIFIED = weakref.WeakKeyDictionary()


def _observation_rows(observation, target, network, maximum=120):
    from db_sync_alt.sources.blockfrost.poller import HeadSnapshot
    if not isinstance(observation, HeadSnapshot) or not observation.fresh(maximum):
        return {}
    receipt = observation.receipt
    if (receipt.get('adapter') != network.adapter or receipt.get('openapi') != OPENAPI
            or receipt.get('shared_profile') != network.network):
        raise Refusal('provider_observation_mismatch')
    rows = {}
    for path, raw in [('/blocks/latest',observation.head),
                      ('/blocks/'+str(target),observation.confirmed)]:
        row=block_row(raw)
        if path != '/blocks/latest' and row['height'] != target:
            continue
        if not any(r.get('endpoint')==path and r.get('sha256')==digest(raw) for r in receipt['requests']):
            raise Refusal('provider_observation_mismatch')
        rows[row['height']]=row
    return rows


def _fresh(provider, height, cached=None):
    if cached is not None and height in cached:
        return cached[height]
    row=block_row(provider.get('/blocks/'+str(height)))
    if row['height']!=height:raise Refusal('provider_target_mismatch')
    return row


def _boundary(provider, store, parent, *, stopped):
    """Canonical prefix search bounded by the k-block provisional window."""
    lo, hi=parent['target_height'],parent['witness_height']
    while lo < hi:
        if stopped():raise Refusal('interrupted')
        mid=(lo+hi+1)//2
        actual=_fresh(provider,mid)
        if actual['hash']==store.block_at(parent['id'],mid)['hash']:
            lo=mid
        else:hi=mid-1
    return lo


def _target_anchor(provider, store, anchor):
    """The k-deep target is strict: any provider change refuses fail-closed."""
    try:return check_anchor(provider,store,anchor,committed=False)
    except Refusal as exc:
        if exc.code=='pending_anchor_changed':raise Refusal('pending_target_changed') from None
        raise


def _tip_row(provider, height):
    """Provider's current block at an unstable height above the target.

    A provider chain temporarily shorter than the height (HTTP 404) is an
    accounted transient wait, never a refusal.
    """
    raw=provider.get_optional('/blocks/'+str(height))
    if raw is None:provider._wait('provider_unavailable')
    row=block_row(raw)
    if row['height']!=height:raise Refusal('provider_target_mismatch')
    return row


def _observe(provider, store, gen, height, stage, observations):
    """Provider's current block at a provisional height above the target.

    Blocks above the target are unstable, as in cardano-db-sync: a provider
    hash that differs from the stored header is recorded, never refused.
    Returns (stored header or None, provider row).
    """
    stored=store.block_at(gen,height)
    stored_hash=None if stored is None else stored['hash'].hex()
    diagnostic_context(provider,'anchors',block_height=height,block_hash=stored_hash)
    row=_tip_row(provider,height)
    if stored is not None:
        observations.append({'stage':stage,'height':height,'stored':stored_hash,'provider':row['hash'].hex()})
    return stored,row


def _fetch_suffix(provider, store, base, witness, *, hooks):
    """Provider pages of the parent-linked header suffix (base, witness]; no storage.

    Each page keeps its own drained request receipt. A provider chain that
    does not (yet) reach the witness, or no longer knows the base (HTTP 404),
    is an accounted transient wait.
    """
    pages=[];last=base['height'];previous=base['hash']
    while last<witness:
        if hooks.stopped():raise Refusal('interrupted')
        hooks.check_space(store)
        hooks.update(phase='metadata',range_start=last+1,range_end=min(last+100,witness))
        diagnostic_context(provider,'metadata',block_height=last+1)
        batch=provider.get_optional('/blocks/'+bytes(previous).hex()+'/next?count=100&page=1')
        if batch is None or batch==[]:provider._wait('provider_unavailable')
        if not isinstance(batch,list):raise Refusal('provider_metadata_gap')
        rows=[]
        for raw in batch:
            row=block_row(raw)
            if row['height']!=last+len(rows)+1 or row['parent_hash']!=(rows[-1]['hash'] if rows else previous):
                raise Refusal('provider_metadata_parent_conflict')
            if row['height']>witness:break
            rows.append(row)
        if not rows:raise Refusal('provider_metadata_gap')
        pages.append((last+1,rows,provider.drain()))
        last=rows[-1]['height'];previous=rows[-1]['hash']
    return pages


def _store_suffix(store, provider, gen, pages, *, hooks):
    """Store a complete fetched suffix in one transaction, one checkpoint per page."""
    if not pages:return 0
    hooks.check_space(store)
    diagnostic_context(provider,'metadata.commit',block_height=pages[0][0])
    with store.atomic():
        for lo,rows,receipt in pages:
            for row in rows:store.insert('blocks',row,gen);store.bind_block(gen,row['height'],row['hash'])
            store.checkpoint(gen,'metadata',str(lo),{'lo':lo,'hi':rows[-1]['height'],
                             'source_kind':'blockfrost-cardano','provider':receipt})
    stored=sum(len(rows) for _,rows,_ in pages)
    hooks.sample(store,phase='metadata',committed=pages[-1][1][-1]['height'],rows=stored)
    return stored


def update_follow(store, provider, profile, target, *, hooks, observation=None):
    from db_sync_alt.store.incremental import integrity_reference
    parent_capability=store.current_verified_parent()
    ready=store.generation();pending=store.pending()
    if store.meta('profile') != profile.identity:raise Refusal('provider_profile_mismatch')
    if target<=ready['target_height']:raise Refusal('no_confirmed_progress')
    if pending and (pending['target_height']!=target or pending['source']!='blockfrost-cardano'):
        raise Refusal('resume_target_mismatch')
    source_identity=profile.network.source_identity
    token=_QUALIFIED.get(store)
    qualified=(token is not None and token['parent'] == ready['id']
               and token['capability']() is parent_capability and token['identity']==profile.identity)
    diagnostic_context(provider,'anchors',generation=(pending or ready)['id'])
    # Fresh canonical committed boundary, even when session handoff is reusable.
    check_anchor(provider,store,ready['manifest']['anchors']['target'],committed=True)
    if not qualified:
        diagnostic_context(provider,'genesis')
        if provider.get('/genesis').get('network_magic') != profile.network.magic:raise Refusal('wrong_provider_network')
        check_anchor(provider,store,ready['manifest']['anchors']['start'],committed=True)
        diagnostic_context(provider,'handoff')
        try:handoff_overlap(provider,store,profile,ready)
        except Refusal as exc:
            if exc.code=='provider_handoff_fact_conflict':store.mark_conflict()
            raise
    cached=_observation_rows(observation,target,profile.network)
    witness=target+profile.security
    observations=[]
    if pending:
        if pending['manifest']['source_identity']!=source_identity:raise Refusal('resume_adapter_mismatch')
        gen=pending['id'];manifest=pending['manifest']
        store.resume_continuation(gen,parent_capability)
        _target_anchor(provider,store,manifest['anchors']['target'])
        _observe(provider,store,gen,pending['witness_height'],'resume',observations)
    else:
        # A counted matching head snapshot removes duplicate initial GETs only.
        diagnostic_context(provider,'anchors',block_height=witness)
        head=next((r for h,r in cached.items() if h>=witness),None)
        if head is None:head=block_row(provider.get('/blocks/latest'))
        # A fixed job target comes from an earlier confirmed observation. A
        # provider head now below its witness is a shorter tip: wait, never refuse.
        if target>head['height']-profile.security:provider._wait('provider_unavailable')
        targetrow=_fresh(provider,target,cached)
        witnessrow=cached[witness] if witness in cached else _tip_row(provider,witness)
        old_w=_fresh(provider,ready['witness_height'],cached)
        if old_w['hash']==store.block_at(ready['id'],ready['witness_height'])['hash']:
            cutoff=ready['witness_height']
        else:
            cutoff=_boundary(provider,store,ready,stopped=hooks.stopped)
        base=store.block_at(ready['id'],cutoff)
        prior=provider.drain()
        # The whole provisional suffix is fetched before the pending generation
        # exists and stored right after it, with no provider call in between.
        suffix=_fetch_suffix(provider,store,base,witness,hooks=hooks)
        if hooks.stopped():raise Refusal('interrupted')
        startepoch=store.block_at(ready['id'],ready['start_height'])['epoch']
        manifest={'identity':profile.identity,'contracts':digest(profile.contract),'source_identity':source_identity,
                  'anchors':{'start':ready['manifest']['anchors']['start'],
                             'target':{'height':target,'hash':targetrow['hash'].hex()},
                             'witness':{'height':witness,'hash':witnessrow['hash'].hex()}},
                  'required_epochs':required_epochs(startepoch,targetrow['epoch']),
                  'capabilities':ready['manifest']['capabilities'],
                  'parent_ref':integrity_reference(ready)}
        from db_sync_alt.sources.blockfrost.relevance import certificate
        coverage=certificate(store,profile,target)
        if coverage is not None:manifest['relevance_coverage']=coverage
        gen=store.begin_continuation(source='blockfrost-cardano',start=profile.start,target=target,
                                     witness=witness,manifest=manifest,parent_capability=parent_capability)
        if coverage is not None:store.checkpoint(gen,'relevance','coverage',coverage)
        # The core owns the immutable inherited metadata receipt/proof shape.
        reuse_receipt={'source_kind':'blockfrost-cardano','parent':ready['id'],'height':cutoff,
                       'hash':base['hash'].hex(),
                       'session_handoff_reused':qualified,'provider':prior}
        if observation is not None and cached:reuse_receipt['observation']=observation.receipt
        store.stage_metadata_reuse(gen,cutoff,reuse_receipt)
        store.checkpoint(gen,'handoff','initial',{'source_kind':'blockfrost-cardano',
                         'qualified_parent':integrity_reference(ready),'session_reused':qualified,
                         'provider':reuse_receipt['provider']})
        _store_suffix(store,provider,gen,suffix,hooks=hooks)
        manifest=store.generation(gen)['manifest']
    g=store.generation(gen)
    # Resume rechecks the saved reuse boundary rather than silently adopting a fork.
    reuse=store.checkpoint_rows(generation=gen,phase='metadata',unit='inherited')
    if pending and not reuse:
        old_w=_fresh(provider,ready['witness_height'])
        cutoff=ready['witness_height'] if old_w['hash']==store.block_at(ready['id'],ready['witness_height'])['hash'] else _boundary(provider,store,ready,stopped=hooks.stopped)
        reuse_receipt={'source_kind':'blockfrost-cardano','parent':ready['id'],'height':cutoff,
                      'hash':store.block_at(ready['id'],cutoff)['hash'].hex(),'provider':provider.drain()}
        store.stage_metadata_reuse(gen,cutoff,reuse_receipt)
        reuse=store.checkpoint_rows(generation=gen,phase='metadata',unit='inherited')
    if pending and not store.completed(gen,'handoff','initial'):
        store.checkpoint(gen,'handoff','initial',{'source_kind':'blockfrost-cardano',
                         'qualified_parent':integrity_reference(ready),'session_reused':False,
                         'provider':provider.drain()})
    last=max(ready['target_height'],store.last_metadata_height(gen) or ready['target_height'])
    checked=None
    if pending and reuse:
        receipt=reuse[0]['receipt'];cutoff=receipt.get('hi',ready['target_height'])
        expected=store.block_at(gen,cutoff)
        if expected is None:raise Refusal('pending_anchor_changed')
        if cutoff<=g['target_height']:
            _target_anchor(provider,store,{'height':cutoff,'hash':expected['hash'].hex()})
        else:
            _,actual=_observe(provider,store,gen,cutoff,'resume',observations)
            # A complete stored suffix stays publishable; an incomplete one cannot continue from a replaced base.
            if actual['hash']!=expected['hash'] and last<g['witness_height']:raise Refusal('pending_suffix_base_changed')
        checked=cutoff
    processed_rows=0
    if last<g['witness_height']:
        # Only a pending generation interrupted before its complete suffix was stored lacks it.
        base=store.block_at(gen,last)
        if last!=checked:
            if last<=g['target_height']:_target_anchor(provider,store,{'height':last,'hash':base['hash'].hex()})
            else:
                _,actual=_observe(provider,store,gen,last,'resume',observations)
                if actual['hash']!=base['hash']:raise Refusal('pending_suffix_base_changed')
        suffix=_fetch_suffix(provider,store,base,g['witness_height'],hooks=hooks)
        processed_rows=_store_suffix(store,provider,gen,suffix,hooks=hooks)
    source=BlockfrostSource(provider,profile,store,gen,stop_requested=hooks.stopped)
    committed_events=store.last_event_height(gen)
    hooks.update(phase='events',committed=max(ready['target_height'],committed_events or ready['target_height']))
    for height in range(ready['target_height']+1,g['target_height']+1):
        if store.completed(gen,'events',height):
            if source.coverage is not None:
                # Rebuild both discovery streams from fresh provider inventories;
                # an unsealed pending candidate is never its own coverage proof.
                facts,stats=source.block_facts(store.block_at(gen,height))
                old=store.checkpoint_rows(generation=gen,phase='events',unit=height)
                if len(old)!=1 or old[0]['receipt'].get('relevance')!=stats.get('relevance'):raise Refusal('independent_provider_mismatch')
                from db_sync_alt.sources.blockfrost.relevance import verify_inventory
                verify_inventory(store,gen,stats['relevance']['inventory'])
            continue
        if hooks.stopped():raise Refusal('interrupted')
        hooks.check_space(store)
        hooks.update(phase='events',range_start=height,range_end=height)
        diagnostic_context(provider,'events',block_height=height)
        block=store.block_at(gen,height)
        if block is None:raise Refusal('provider_metadata_missing')
        try:
            facts,stats=source.block_facts(block)
            # Fact order is dependency ordered at commit, with outputs before spends.
            order=['blocks','transactions','datums','assets','outputs','output_assets','mints','inputs']
            diagnostic_context(provider,'events.commit',block_height=height,block_hash=block['hash'])
            with store.atomic():
                for table in order:
                    for t,f in facts:
                        if t==table:
                            diagnostic_context(provider,'events.commit',block_height=height,block_hash=block['hash'],transaction_hash=f.get('tx_hash',f.get('hash') if t=='transactions' else None),transaction_index=f.get('tx_index'))
                            store.insert(t,f,gen)
                            if t=='blocks':store.bind_block(gen,f['height'],f['hash'])
                store.checkpoint(gen,'events',height,{'lo':height,'hi':height,'source_kind':'blockfrost-cardano',**stats,'provider':provider.drain()})
        except Refusal as exc:
            if exc.code in ('provider_origin_conflict','provider_origin_asset_conflict','immutable_fact_conflict','canonical_block_conflict'):store.mark_conflict()
            raise
        processed_rows+=len(facts)
        hooks.sample(store,phase='events',committed=height,rows=processed_rows)
    store.block_at(gen,g['target_height'])['epoch']  # the target header must be stored
    next_epoch=store.block_at(gen,g['target_height']+1)['epoch']
    for epoch in manifest['required_epochs']:
        if store.completed(gen,'epochs',epoch):continue
        diagnostic_context(provider,'epochs',epoch=epoch)
        hooks.update(phase='epochs',range_start=epoch,range_end=epoch)
        prior=store.checkpoint_rows(phase='epochs',unit=epoch,before=gen)
        inherited=max(prior,key=lambda row:row['generation']) if prior else None
        if inherited:
            old=inherited['receipt']
            if old.get('epoch_end') is not None or epoch>=next_epoch:continue
            if hooks.stopped():raise Refusal('interrupted')
            try:receipt=complete_inherited_epoch(provider,store,g,epoch,inherited['generation'])
            except Refusal as exc:
                if exc.code=='provider_inherited_epoch_conflict':store.mark_conflict()
                raise
            diagnostic_context(provider,'epochs.commit',epoch=epoch)
            with store.atomic():store.checkpoint(gen,'epochs',epoch,receipt)
            hooks.sample(store,phase='epochs',committed=epoch,rows=processed_rows)
            continue
        if hooks.stopped():raise Refusal('interrupted')
        preparer = getattr(hooks, 'epoch_preparation', None)
        if preparer is not None:
            nonce,stakes,anchor,receipt=preparer.adopt(store,gen,epoch,g['target_height'])
        else:
            nonce,stakes,anchor,receipt=epoch_facts(provider,epoch,g['target_height'],stake_prefix=profile.network.stake_prefix)
        diagnostic_context(provider,'epochs.commit',epoch=epoch)
        with store.atomic():
            store.insert('epoch_nonces',nonce,gen)
            for row in stakes:store.insert('epoch_stakes',row,gen)
            if anchor['height']<=g['target_height']:store.insert('blocks',anchor,gen);store.bind_block(gen,anchor['height'],anchor['hash'])
            epoch_receipt = {'epoch':epoch,'source_kind':'blockfrost-cardano',**receipt}
            epoch_receipt['publication_provider' if 'preparation' in receipt else 'provider'] = provider.drain()
            store.checkpoint(gen,'epochs',epoch,epoch_receipt)
            if 'preparation' in receipt:
                from db_sync_alt.sources.blockfrost.epoch_prepare import commit_adoption
                commit_adoption(store,epoch,receipt['preparation'])
        processed_rows+=len(stakes)+1
        hooks.sample(store,phase='epochs',committed=epoch,rows=processed_rows)
    diagnostic_context(provider,'anchors')
    # The fixed target is certified strictly; the witness is the stored chain,
    # with the provider's current tip block recorded beside it.
    _target_anchor(provider,store,manifest['anchors']['target'])
    stored,_=_observe(provider,store,gen,g['witness_height'],'final',observations)
    if stored is None:raise Refusal('metadata_incomplete')
    initial=manifest['anchors']['witness']['hash']
    # A sealed pending generation was verified against its stored witness already.
    if 'completion' not in manifest:manifest=store.rebind_pending_witness(gen)
    receipt=provider.drain()
    receipt['provisional']={'witness':{'height':g['witness_height'],'manifest_initial':initial,'stored':stored['hash'].hex()},
                            'observations':observations}
    diagnostic_context(provider,'verification')
    hooks.update(phase='verifying',waiting=True)
    transition=store.verify_pending_incremental(gen)
    diagnostic_context(provider,'finalization')
    store.finalize_pending_incremental(gen,manifest,receipt,transition)
    hooks.before_publish()
    diagnostic_context(provider,'publication')
    result=store.publish_incremental(gen,transition,stop_requested=hooks.stopped)
    _QUALIFIED[store]={'parent':gen,'capability':weakref.ref(store.current_verified_parent()),'identity':copy.deepcopy(profile.identity)}
    hooks.sample(store,phase='complete',waiting=False,committed=g['target_height'],rows=processed_rows)
    return result
