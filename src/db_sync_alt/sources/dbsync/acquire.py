"""Direct db-sync -> canonical PostgreSQL acquisition; no provider access.

The normal and independent selectors are inherited from SQLSource. Only bounded
new ranges are acquired; immutable parent certification supplies the old prefix.
"""
import copy
import hashlib
import threading
from psycopg.types.json import Jsonb
from db_sync_alt.contract import TABLES, KEYS, Refusal, digest, canonical, jsonable
from db_sync_alt.sources.dbsync.reader import SQLSource
from db_sync_alt.verify.completion import SQL_ADAPTER, SQL_VALIDATION, validate_completion
from db_sync_alt.verify.store import verify_store
from db_sync_alt.verify.prerequisites import acquisition_version,prerequisite_coverage,chunk_receipt,seal_receipt
from db_sync_alt.verify.sql_receipt import admit_receipt_size


class CountedSQLSource(SQLSource):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.queries = self.rows_read = 0

    def one(self, query, params=()):
        self.queries += 1
        row = super().one(query, params)
        self.rows_read += 1
        return row

    def stream(self, query, params=(), batch=500):
        self.queries += 1
        for row in super().stream(query, params, batch):
            self.rows_read += 1
            yield row


def physical_identity(db):
    row = db.execute('SELECT system_identifier::text FROM pg_control_system()').fetchone()
    system = row['system_identifier'] if isinstance(row, dict) else row[0]
    row = db.execute('SELECT oid FROM pg_database WHERE datname=current_database()').fetchone()
    return {'system_identifier': system, 'database_oid': row['oid'] if isinstance(row, dict) else row[0]}


def source_binding(source, destination, namespace):
    """Opaque binding of an acquisition to one source database and its row namespace.

    A SHA-256 over the source's cluster and database identity and its row ids of
    the start and target anchors, keyed by the destination's own cluster identity,
    which neither the stored rows nor a backup contain. The same source and
    destination always give the same value; the value does not reveal the source.
    """
    return hashlib.sha256(canonical({'binding': 'db-sync-alt-source-v1', 'source': source,
                                     'namespace': namespace, 'destination': destination}).encode()).hexdigest()


def _binding(source, store, anchors):
    return source_binding(physical_identity(source.conn), physical_identity(store.db), source.namespace(anchors))


def _stop(stop_event):
    if stop_event.is_set():
        raise Refusal('interrupted')


def _source_manifest(source, store, target):
    with source.snapshot():
        if physical_identity(source.conn) == physical_identity(store.db):
            raise Refusal('source_equals_destination')
        m = source.preflight(target)
        m['source_identity'].update(adapter=SQL_ADAPTER, binding=_binding(source, store, m['anchors']))
    for anchor in m['anchors'].values():
        del anchor['source_id']
    m['contracts'] = digest(source.profile.contract)
    return m


def _anchors(source, store, generation):
    g = store.generation(generation)
    try:
        with source.snapshot():
            if _binding(source, store, g['manifest']['anchors']) != g['manifest']['source_identity']['binding']:
                raise Refusal('resume_namespace_mismatch')
            source.check_anchors(g['manifest'])
    except Refusal as error:
        if error.code == 'source_anchor_conflict':
            store.mark_conflict()
        raise


def _facts(source, lo, hi, independent):
    return source.facts(*source.select_ids(lo, hi, independent=independent))


def _put(store, generation, facts, stop_event):
    """Store a unit's facts in source order; consecutive headers are written in bulk."""
    count = 0; headers = []
    for table, row in facts:
        _stop(stop_event)
        if table == 'blocks':
            headers.append(row)
        else:
            if headers:
                store.insert_blocks(generation, headers); headers = []
            store.insert(table, row, generation)
        count += 1
        if count % 1000 == 0:
            store.progress('sql_facts', generation=generation, rows=count)
    if headers:
        store.insert_blocks(generation, headers)
    return count


def _stage(store, table, row):
    ordinal = row.get('ordinal') if table in ('inputs', 'output_assets') else None
    row = {k: row[k] for k in TABLES[table]}
    value = jsonable(row)
    key = [value[k] for k in KEYS[table]]
    cur = store.db.execute('INSERT INTO sql_expected VALUES(%s,%s,%s) ON CONFLICT(family,key) DO UPDATE SET row=sql_expected.row WHERE sql_expected.row=excluded.row RETURNING 1',
                           (table, Jsonb(key), Jsonb(value)))
    if cur.fetchone() is None:
        raise Refusal('independent_source_conflict')
    actual = store.fact(table, row)
    if actual is None or digest(actual) != digest(row):
        raise Refusal('independent_source_mismatch')
    if ordinal is not None and store.ordinal(table, row) != ordinal:
        raise Refusal('independent_source_mismatch')


def _stage_blocks(store, generation, rows):
    """`_stage` and the membership check for many independently read headers at once."""
    if not rows:
        return
    values = {}
    for row in rows:
        value = jsonable(row)
        if values.setdefault(value['hash'], value) != value:
            raise Refusal('independent_source_conflict')
    keys = [Jsonb([h]) for h in values]
    for key, row in store.db.execute("SELECT key,row FROM sql_expected WHERE family='blocks' AND key=ANY(%s)", (keys,)):
        if row != values[key[0]]:
            raise Refusal('independent_source_conflict')
    with store.db.cursor() as cursor:
        cursor.executemany("INSERT INTO sql_expected VALUES('blocks',%s,%s) ON CONFLICT(family,key) DO NOTHING",
                           [(Jsonb([h]), Jsonb(value)) for h, value in values.items()])
    from db_sync_alt.store.database import logical_row
    columns = TABLES['blocks']
    stored = {}
    for raw in store.db.execute('SELECT '+','.join(columns)+' FROM chain.blocks WHERE hash=ANY(%s)', ([bytes.fromhex(h) for h in values],)):
        actual = jsonable(logical_row('blocks', dict(zip(columns, raw)))); stored[actual['hash']] = actual
    for h, value in values.items():
        if digest(stored.get(h)) != digest(value):
            raise Refusal('independent_source_mismatch')
    membership, args = store.membership_sql(generation)
    bound = {}
    for height, h in store.db.execute('SELECT m.height,m.hash FROM ('+membership+') m WHERE m.height=ANY(%s)', (*args, sorted({v['height'] for v in values.values()}))):
        if height in bound:
            raise Refusal('conflicting_inherited_membership')
        bound[height] = bytes(h).hex()
    for h, value in values.items():
        if bound.get(value['height']) != h:
            raise Refusal('independent_membership_mismatch')


def _independent(source, store, generation, ranges, epochs, stop_event):
    """Independent source discovery, exact content and extra-new-fact rejection.

    TEMP data is discarded at session close. No authoritative duplicate facts.
    Earlier physical blocks may include immutable obsolete witness headers; their
    reuse is checked by value, while canonical membership is checked separately.
    """
    g = store.generation(generation)
    store.db.execute('DROP TABLE IF EXISTS pg_temp.sql_expected')
    store.db.execute('CREATE TEMP TABLE sql_expected(family text,key jsonb,row jsonb,PRIMARY KEY(family,key))')
    def unit(phase, lo, hi):
        _stop(stop_event); store.check_space()
        try:
            with source.snapshot():
                source.check_anchors(g['manifest'])
                facts = (('blocks', row) for row in source.blocks(lo, hi)) if phase == 'metadata' else _facts(source, lo, hi, True)
                with store.atomic():
                    count = 0; headers = []
                    for table, row in facts:
                        _stop(stop_event)
                        if table == 'blocks':
                            headers.append(row)
                        else:
                            _stage(store, table, row)
                        count += 1
                        if count % 1000 == 0:
                            store.progress('sql_independent', generation=generation, rows=count)
                    _stage_blocks(store, generation, headers)
        except Refusal as error:
            if error.code in ('source_timeout','work_unit_too_dense') and hi > lo:
                middle=(lo+hi)//2;unit(phase,lo,middle);unit(phase,middle+1,hi);return
            raise
        _anchors(source, store, generation)
    for phase,lo,hi in ranges:unit(phase,lo,hi)
    for epoch in epochs:
        _stop(stop_event); store.check_space()
        with source.snapshot():
            source.check_anchors(g['manifest'])
            with store.atomic():
                for table, row in source.epochs(epoch, g['target_height']):
                    _stop(stop_event)
                    _stage(store, table, row)
                    if table == 'blocks':
                        actual=store.block_at(generation,row['height'])
                        if actual is None or bytes(actual['hash']) != bytes(row['hash']):
                            raise Refusal('independent_membership_mismatch')
    # A newly bound old physical block must also have a source dependency.
    if store.db.execute("""SELECT 1 FROM chain.generation_blocks gb
        LEFT JOIN sql_expected e ON e.family='blocks'
          AND e.key=jsonb_build_array(encode(gb.hash,'hex'))
          AND (e.row->>'height')::bigint=gb.height
        WHERE gb.generation=%s AND e.key IS NULL LIMIT 1""",(generation,)).fetchone():
        raise Refusal('independent_membership_extra')
    # Every newly stored natural fact must have an independently discovered
    # matching source row. Presence/content of all expected rows checked above.
    from db_sync_alt.store.database import logical_row
    for table, columns in TABLES.items():
        with store.db.transaction():
            with store.db.cursor(name='new_'+table) as cursor:
                cursor.execute('SELECT '+','.join(columns)+' FROM chain.'+table+' WHERE first_generation=%s', (generation,))
                while batch := cursor.fetchmany(5000):
                    _stop(stop_event)
                    values = [jsonable(logical_row(table, dict(zip(columns, raw)))) for raw in batch]
                    keys = [[value[k] for k in KEYS[table]] for value in values]
                    expected = {canonical(key): row for key, row in store.db.execute(
                        'SELECT key,row FROM sql_expected WHERE family=%s AND key=ANY(%s)', (table, [Jsonb(key) for key in keys]))}
                    for key, value in zip(keys, values):
                        row = expected.get(canonical(key))
                        if row is None or digest(row) != digest(value):
                            raise Refusal('independent_source_extra_fact')
        store.progress('sql_independent_family', generation=generation, family=table)
    inventory = {}
    for table in TABLES:
        h = hashlib.sha256(); count = 0
        with store.db.transaction():
            with store.db.cursor(name='expected_'+table) as cursor:
                cursor.execute('SELECT row FROM sql_expected WHERE family=%s ORDER BY key', (table,))
                for (row,) in cursor:
                    _stop(stop_event)
                    h.update((canonical(row)+'\n').encode()); count += 1
        inventory[table] = {'count': count, 'sha256': h.hexdigest()}
    store.db.execute('DROP TABLE sql_expected')
    return {'version': 1, 'scope': 'independent-new-facts-and-membership-with-certified-parent',
            'parent_digest': g['manifest']['parent_digest'], 'ranges': ranges,
            'epochs': epochs, 'inventory': inventory}


def acquire(source, store, profile, *, target_height=None, window_blocks=1000,
            stop_event=None, progress=None):
    """One fixed target, create/bootstrap or resume/increment, then one publish."""
    stop_event = stop_event or threading.Event()
    if type(window_blocks) is not int or not 1 <= window_blocks <= 10000:
        raise Refusal('invalid_window')
    store.require_writer(); _stop(stop_event); store.check_space()
    if store.network != profile.network:
        raise Refusal('network_mismatch')
    pending = store.pending()
    if pending and (pending['source'] != 'cardano-db-sync-postgresql' or
                    pending['manifest'].get('source_identity', {}).get('adapter') != SQL_ADAPTER):
        raise Refusal('resume_source_mismatch')
    if pending:
        validate_completion(store, pending)
        if target_height is not None and target_height != pending['target_height']:
            raise Refusal('resume_target_mismatch')
        target_height = pending['target_height']
    fresh = _source_manifest(source, store, target_height)
    if fresh['identity'] != profile.identity:
        raise Refusal('source_profile_mismatch')
    ready = store.generation() if store.meta('canonical_ready') is not None else None
    if ready:
        if store.meta('profile') != profile.identity:
            raise Refusal('resume_profile_mismatch')
    if pending:
        m = pending['manifest']
        if digest(fresh['source_identity']) != digest(m['source_identity']):
            raise Refusal('resume_namespace_mismatch')
        if digest(fresh['anchors']) != digest(m['anchors']):
            store.mark_conflict(); raise Refusal('source_anchor_conflict')
        if digest(fresh['identity']) != digest(m['identity']):
            raise Refusal('resume_profile_mismatch')
        generation = pending['id']; window_blocks = m['acquisition']['window_blocks']
    else:
        from db_sync_alt.store.incremental import integrity_reference
        fresh['parent_digest'] = integrity_reference(ready)['sha256'] if ready else None
        fresh['acquisition'] = {'version': 2, 'window_blocks': window_blocks}
    admission = m if pending else fresh
    admit_receipt_size(admission, start=pending['start_height'] if pending else profile.start[0],
        range_start=ready['target_height']+1 if ready else profile.start[0],
        target=admission['anchors']['target']['height'], witness=admission['anchors']['witness']['height'],
        root=ready is None, window=window_blocks, version=acquisition_version(pending) if pending else 2)
    if ready:
        verify_store(store)
        try:
            with source.snapshot():
                source.check_anchors({'anchors': {k: ready['manifest']['anchors'][k] for k in ('start','target')}})
        except Refusal as error:
            if error.code == 'source_anchor_conflict':store.mark_conflict()
            raise
    if not pending:
        if ready and fresh['anchors']['target']['height'] <= ready['target_height']:
            raise Refusal('no_confirmed_progress')
        with store.atomic():
            if store.meta('profile') is None:store.set_meta('profile', profile.identity)
            generation = store.begin_generation(source='cardano-db-sync-postgresql', start=profile.start,
                target=fresh['anchors']['target']['height'], witness=fresh['anchors']['witness']['height'],
                manifest=fresh, parent=ready['id'] if ready else None)
            if store.meta('sql_completion_required_from') is None:
                store.set_meta('sql_completion_required_from', generation)
            if ready:
                store.checkpoint(generation, 'metadata', 'inherited', {'lo': ready['start_height'],
                    'hi': ready['target_height'], 'parent': ready['id'], 'parent_digest': fresh['parent_digest'],
                    'source_kind': 'generation-inheritance', 'inherited_source_kind': ready['source']})
    g = store.generation(generation); m = g['manifest']
    prerequisite_state=prerequisite_coverage(store,g,require_complete=False)
    if pending and prerequisite_state['version']==1 and g['parent'] is None and g['start_height']>0 and not prerequisite_state['complete']:
        raise Refusal('prerequisites_v1_pending_requires_recovery')
    ranges = []
    # Facts before the start cursor; a network whose cursor is block 0 has none.
    if g['parent'] is None and g['start_height'] > 0:
        if acquisition_version(g)==2:ranges += [('prerequisites',lo,min(g['start_height']-1,lo+window_blocks-1)) for lo in range(0,g['start_height'],window_blocks)]
        else:ranges.append(('prerequisites',0,g['start_height']-1))
    start = ready['target_height']+1 if ready else g['start_height']
    for phase, end in [('metadata', g['witness_height']), ('events', g['target_height'])]:
        ranges += [(phase, lo, min(end, lo+window_blocks-1)) for lo in range(start, end+1, window_blocks)]

    def unit(phase, lo, hi):
        _stop(stop_event); store.check_space()
        if phase=='prerequisites':
            if prerequisite_state['complete']:return
            lo=max(lo,prerequisite_state['position'])
        else:
            covered = sorted((r['receipt']['lo'], r['receipt']['hi']) for r in store.checkpoint_rows(generation=generation, phase=phase)
                             if r['receipt']['hi'] >= lo and r['receipt']['lo'] <= hi)
            position = lo
            for a,b in covered:
                if a != position or b > hi:raise Refusal('checkpoint_overlap')
                position = b+1
            lo = position
        if lo > hi:return
        try:
            with source.snapshot() as snapshot:
                source.check_anchors(m)
                facts = (('blocks', row) for row in source.blocks(lo, hi)) if phase == 'metadata' else _facts(source, lo, hi, phase == 'prerequisites')
                with store.atomic():
                    count = _put(store, generation, facts, stop_event)
                    if phase == 'metadata' and count != hi-lo+1:raise Refusal('source_metadata_gap')
                    if snapshot.get('read_only') is not True:raise Refusal('source_not_read_only')
                    receipt=chunk_receipt(g,lo,hi,count) if phase=='prerequisites' else {'lo':lo,'hi':hi,**snapshot,'rows':count,'source_kind':g['source']}
                    store.checkpoint(generation,phase,f'{lo}:{hi}',receipt)
                if phase=='prerequisites':prerequisite_state['position']=hi+1
        except Refusal as error:
            if error.code in ('source_timeout','work_unit_too_dense') and hi > lo:
                middle = (lo+hi)//2; unit(phase,lo,middle);unit(phase,middle+1,hi);return
            if error.code == 'source_anchor_conflict':store.mark_conflict()
            raise
        _anchors(source, store, generation)
        store.progress('sql_checkpoint', generation=generation, phase_name=phase, height=hi)
        if progress:progress(phase, generation=generation, height=hi)
    def seal_prerequisites():
        if acquisition_version(g)!=2 or g['parent'] is not None or not g['start_height'] or prerequisite_state['complete']:return
        _stop(stop_event);_anchors(source,store,generation)
        with store.atomic():
            state=prerequisite_coverage(store,g,require_complete=False)
            if state['position']!=g['start_height']:raise Refusal('prerequisites_incomplete')
            store.checkpoint(generation,'prerequisites','complete',seal_receipt(g,state['chunks']))
        prerequisite_state['complete']=True
    for phase,lo,hi in ranges:
        if phase!='prerequisites':seal_prerequisites()
        unit(phase,lo,hi)
    seal_prerequisites()
    epoch_list = m['acquisition'].get('epochs')
    if epoch_list is None:
        epoch_list = []
        for epoch in m['required_epochs']:
            inherited = store.checkpoint_rows(phase='epochs', unit=epoch, before=generation)
            if not inherited or (epoch < m['anchors']['target']['epoch'] and not any(r['receipt'].get('epoch_end') for r in inherited)):
                epoch_list.append(epoch)
        # Derivable from immutable parent; no manifest mutation after sealing.
    for epoch in epoch_list:
        if store.completed(generation, 'epochs', epoch):continue
        _stop(stop_event); store.check_space()
        with source.snapshot() as snapshot:
            source.check_anchors(m)
            with store.atomic():
                count=0; end=None
                for table,row in source.epochs(epoch,g['target_height']):
                    _stop(stop_event);store.insert(table,row,generation);count+=1
                    if table == 'blocks':
                        store.bind_block(generation,row['height'],bytes(row['hash']))
                        end={'height':row['height'],'hash':bytes(row['hash']).hex()}
                store.checkpoint(generation,'epochs',epoch,{'epoch':epoch,'epoch_end':end,'stake_complete':True,
                    'rows':count,**snapshot,'source_kind':g['source']})
        _anchors(source,store,generation)
        store.progress('sql_epoch',generation=generation,epoch=epoch)
    _stop(stop_event)
    report = verify_store(store, generation, require_ready=False)
    independent = _independent(source, store, generation, ranges, epoch_list, stop_event)
    _anchors(source,store,generation); _stop(stop_event)
    manifest = copy.deepcopy(store.generation(generation)['manifest'])
    manifest['digests']=report['digests']
    manifest['validation']={'kind':SQL_VALIDATION,'digests':report['digests'],'independent':independent}
    final = store.finalize_pending(generation,manifest,{'source_kind':g['source'],'anchors':m['anchors'],
        'independent':independent},report['digests'])
    if progress:progress('sealed',generation=generation)
    _anchors(source,store,generation); _stop(stop_event)
    result = store.publish(generation,final)
    return {**result,'source':'db-sync','queries':getattr(source,'queries',None),
            'query_count_scope':'logical one/stream statements; excludes snapshot controls, identity queries and cursor fetches',
            'source_rows':getattr(source,'rows_read',None),'target':g['target_height'],'witness':g['witness_height']}
