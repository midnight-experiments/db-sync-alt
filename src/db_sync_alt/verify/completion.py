"""Storage-neutral, versioned completion admission for provider generations.

A seal records expected semantics, not provider timing. It is an integrity receipt,
not a signature. Store adapters own its atomic persistence and publication fences.
"""
import copy
import re
from db_sync_alt.contract import TABLES, Refusal, canonical, digest
from db_sync_alt.networks import for_identity

COMPLETION_ERRORS = frozenset({'completion_recovery_required', 'completion_invalid', 'completion_mismatch'})
MAX_RECEIPT_BYTES = 2 * 1024 * 1024
MAX_REVALIDATIONS = 2**63 - 1
SQL_ADAPTER = 'db-sync-direct-pg-v1'
SQL_VALIDATION = 'independent-sql-delta-inventory-v2'

def completion_required(store, generation):
    if generation['source'] == 'blockfrost-cardano':
        return generation['status'] == 'pending'
    floor = store.meta('sql_completion_required_from')
    if floor is not None and (type(floor) is not int or floor < 0):
        _invalid()
    if generation['source'] != 'cardano-db-sync-postgresql':
        return False
    records = store.checkpoint_rows(phase='sql-acquisition')
    if records:
        earliest = min(r['generation'] for r in records)
        if floor != earliest:
            _invalid()
    new = generation['manifest'].get('source_identity', {}).get('adapter') == SQL_ADAPTER
    return generation['status'] == 'pending' or new or (floor is not None and generation['id'] >= floor) or any(r['generation']==generation['id'] for r in records)

def _validation_kind(generation):
    return SQL_VALIDATION if generation['source'] == 'cardano-db-sync-postgresql' else 'provider-enumeration'


def _invalid():
    raise Refusal('completion_invalid')


def _hash(value):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value) is not None


def _bounded(receipt):
    if not isinstance(receipt, dict):
        _invalid()
    try:
        encoded = canonical(receipt).encode()
    except (TypeError, ValueError, RecursionError):
        _invalid()
    if len(encoded) > MAX_RECEIPT_BYTES:
        _invalid()
    return digest(receipt)


def validate_sql_receipt(store, generation, manifest, receipt):
    """Validate the bounded declared independent inventory before persisting it.

    This is structural admission of a trusted writer's source receipt, not a
    signature or a replacement for the adapter's independent source queries.
    """
    if generation['source'] != 'cardano-db-sync-postgresql':
        return
    try:
        validation=manifest['validation']; independent=validation['independent']
        _bounded(independent)
        if set(independent) != {'version','scope','parent_digest','ranges','epochs','inventory'} or type(independent['version']) is not int or independent['version'] != 1 or independent['scope'] != 'independent-new-facts-and-membership-with-certified-parent':
            _invalid()
        if digest(independent['parent_digest']) != digest(manifest['parent_digest']) or digest(receipt['independent']) != digest(independent):
            _invalid()
        acquisition=manifest['acquisition'];window=acquisition['window_blocks']
        if type(acquisition['version']) is not int or acquisition['version'] not in (1,2) or type(window) is not int or not 1<=window<=10000:
            _invalid()
        parent=store.generation(generation['parent']) if generation['parent'] is not None else None
        start=parent['target_height']+1 if parent else generation['start_height']
        expected={'metadata':(start,generation['witness_height']),'events':(start,generation['target_height'])}
        if parent is None and generation['start_height']>0:expected['prerequisites']=(0,generation['start_height']-1)
        ranges=independent['ranges']
        if not isinstance(ranges,(list,tuple)):_invalid()
        covered={key:[] for key in expected}
        for item in ranges:
            if not isinstance(item,(list,tuple)) or len(item)!=3:_invalid()
            phase,lo,hi=item
            if phase not in expected or type(lo) is not int or type(hi) is not int or lo<0 or hi<lo or ((acquisition['version']==2 or phase!='prerequisites') and hi-lo+1>window):_invalid()
            covered[phase].append((lo,hi))
        for phase,(lo,hi) in expected.items():
            position=lo
            for first,last in sorted(covered[phase]):
                if first!=position or last>hi:_invalid()
                position=last+1
            if position!=hi+1:_invalid()
        if acquisition['version']==2:
            from db_sync_alt.verify.prerequisites import prerequisite_coverage
            prerequisite_coverage(store,{**generation,'manifest':manifest})
        expected_epochs=[]
        for epoch in manifest['required_epochs']:
            inherited=store.checkpoint_rows(phase='epochs',unit=epoch,before=generation['id'])
            if not inherited or (epoch<manifest['required_epochs'][-1] and not any(r['receipt'].get('epoch_end') for r in inherited)):
                expected_epochs.append(epoch)
        epochs=independent['epochs']
        if not isinstance(epochs,list) or any(type(e) is not int for e in epochs) or epochs!=expected_epochs:_invalid()
        inventory=independent['inventory']
        if not isinstance(inventory,dict) or set(inventory)!=set(TABLES):_invalid()
        for value in inventory.values():
            if not isinstance(value,dict) or set(value)!={'count','sha256'} or type(value['count']) is not int or value['count']<0 or not _hash(value['sha256']):_invalid()
        if inventory['blocks']['count']<generation['witness_height']-start+1:_invalid()
        if digest(validation['digests'])!=digest(manifest['digests']):_invalid()
    except (KeyError,TypeError,ValueError,IndexError):
        _invalid()


def _identity(store, generation, computed):
    m = generation['manifest']
    try:
        network = for_identity(store.meta('profile'))
        local = generation['source'] == 'cardano-db-sync-postgresql'
        if local:
            if not isinstance(m['source_identity'], dict) or m['source_identity'].get('adapter') != SQL_ADAPTER:
                _invalid()
        elif generation['source'] != 'blockfrost-cardano' or network is None or m['source_identity'] != network.source_identity:
            _invalid()
        for key in ('id', 'start_height', 'start_index', 'target_height', 'witness_height'):
            if type(generation[key]) is not int or generation[key] < 0:
                _invalid()
        if generation['parent'] is None:
            if not local or generation['id'] != 0 or m.get('parent_digest') is not None:
                _invalid()
        else:
            if type(generation['parent']) is not int or generation['parent'] < 0:
                _invalid()
            parent = store.generation(generation['parent'])
            parent_digest = digest(parent['manifest']['digests']) if 'integrity' not in parent['manifest'] else parent['manifest']['integrity'].get('root')
            if parent['status'] != 'ready' or m['parent_digest'] != parent_digest:
                raise Refusal('completion_mismatch')
        if digest(m['identity']) != digest(store.meta('profile')) or network is None or m['contracts'] != digest(network.contract):
            raise Refusal('completion_mismatch')
        if not isinstance(computed, dict) or set(computed) != set(TABLES):
            _invalid()
        for value in computed.values():
            if not isinstance(value, dict) or set(value) != {'count', 'sha256'} or type(value['count']) is not int or value['count'] < 0 or not _hash(value['sha256']):
                _invalid()
        bounds = {'start': generation['start_height'], 'target': generation['target_height'], 'witness': generation['witness_height']}
        if set(m['anchors']) != set(bounds):
            _invalid()
        for key, height in bounds.items():
            anchor = m['anchors'][key]
            if not isinstance(anchor, dict) or not {'height', 'hash'} <= set(anchor) or set(anchor) - {'height', 'hash', 'epoch', 'source_id', 'tx_count'} or type(anchor['height']) is not int or anchor['height'] != height or not _hash(anchor['hash']):
                _invalid()
            for extra in ('epoch', 'source_id', 'tx_count'):
                if extra in anchor and (type(anchor[extra]) is not int or anchor[extra] < 0):
                    _invalid()
        if not isinstance(m['required_epochs'], list) or any(type(e) is not int or e < 0 for e in m['required_epochs']):
            _invalid()
        if m['capabilities'] != {'bridge_qualified': False, 'candidates_empty': True} or any(type(v) is not bool for v in m['capabilities'].values()):
            _invalid()
        return copy.deepcopy({'generation': generation['id'], 'parent': generation['parent'],
            'source': generation['source'], 'source_identity': m['source_identity'],
            'identity': m['identity'], 'contracts': m['contracts'], 'parent_digest': m['parent_digest'],
            'start': [generation['start_height'], generation['start_index']],
            'target': generation['target_height'], 'witness': generation['witness_height'],
            'anchors': m['anchors'], 'required_epochs': m['required_epochs'],
            'capabilities': m['capabilities'], 'digests': computed,
            **({'acquisition': m['acquisition']} if local else {})})
    except (KeyError, TypeError, ValueError):
        _invalid()


def _latest(store, generation, original_hash):
    rows = store.checkpoint_rows(generation=generation, phase='completion-revalidation')
    if not rows:
        return None
    if len(rows) != 1 or rows[0]['unit'] != 'latest':
        _invalid()
    record = rows[0]['receipt']
    keys = {'version', 'count', 'previous', 'receipt_sha256', 'receipt', 'sha256'}
    if not isinstance(record, dict) or set(record) != keys or type(record['version']) is not int or record['version'] != 1:
        _invalid()
    if type(record['count']) is not int or not 1 <= record['count'] <= MAX_REVALIDATIONS or not _hash(record['previous']):
        _invalid()
    if record['count'] == 1 and record['previous'] != original_hash:
        _invalid()
    if record['receipt_sha256'] != _bounded(record['receipt']) or record['sha256'] != digest({k: v for k, v in record.items() if k != 'sha256'}):
        _invalid()
    return record


def validate_completion(store, generation, computed_digests=None, *, require_seal=False, manifest=None):
    """Read-only preflight; with computed digests also checks current facts.

    Historical ready generations may lack a seal. Completed pending generations
    never infer one from newly observed facts. A supplied publication manifest
    must equal the persisted sealed manifest, including its original provenance.
    """
    m = generation['manifest']
    if isinstance(m,dict) and 'integrity' in m:
        if not isinstance(m['integrity'],dict) or type(m['integrity'].get('version')) is not int or m['integrity']['version']!=2:_invalid()
        from db_sync_alt.store.incremental import validate_completion_v2
        return validate_completion_v2(store,generation,require_seal=require_seal,manifest=manifest)
    if not isinstance(m, dict):
        _invalid()
    required = completion_required(store, generation)
    require_seal = require_seal or (computed_digests is not None and required and generation['status'] == 'ready')
    original = store.checkpoint_rows(generation=generation['id'], phase='final-anchors')
    latest = store.checkpoint_rows(generation=generation['id'], phase='completion-revalidation')
    if 'completion' not in m:
        if latest:
            _invalid()
        if generation['status'] != 'ready' and original:
            if len(original) == 1 and original[0]['unit'] == 'complete':
                raise Refusal('completion_recovery_required')
            _invalid()
        if require_seal:
            _invalid()
        return None
    seal = m['completion']
    if not isinstance(seal, dict) or set(seal) != {'version', 'semantic', 'sha256', 'original_receipt_sha256'} or type(seal['version']) is not int or seal['version'] != 1:
        _invalid()
    if len(original) != 1 or original[0]['unit'] != 'complete':
        _invalid()
    if seal['original_receipt_sha256'] != _bounded(original[0]['receipt']) or seal['sha256'] != digest(seal['semantic']):
        _invalid()
    if not isinstance(m.get('validation'), dict) or m['validation'].get('kind') != _validation_kind(generation):
        _invalid()
    validate_sql_receipt(store,generation,m,original[0]['receipt'])
    expected = _identity(store, generation, m.get('digests'))
    if seal['sha256'] != digest(expected) or (computed_digests is not None and seal['sha256'] != digest(_identity(store, generation, computed_digests))):
        raise Refusal('completion_mismatch')
    _latest(store, generation['id'], seal['original_receipt_sha256'])
    if manifest is not None and digest(manifest) != digest(m):
        raise Refusal('completion_mismatch')
    return seal


def prepare_finalization(store, generation, manifest, receipt, computed_digests):
    """Return immutable manifest and optional replacement latest record.

    The adapter must persist this result and the first original receipt in ONE
    transaction. No mutation occurs here, including on malformed input.
    """
    g = store.generation(generation)
    if g['status'] != 'pending':
        _invalid()
    seal = validate_completion(store, g, computed_digests)
    candidate = {**g, 'manifest': manifest}
    body = _identity(store, candidate, computed_digests)
    if digest(body) != digest(_identity(store, g, computed_digests)) or digest(manifest.get('digests')) != digest(computed_digests) or not isinstance(manifest.get('validation'), dict) or manifest['validation'].get('kind') != _validation_kind(g):
        raise Refusal('completion_mismatch')
    receipt_hash = _bounded(receipt)
    validate_sql_receipt(store,g,manifest,receipt)
    if seal is None:
        if 'completion' in manifest:
            _invalid()
        result = copy.deepcopy(manifest)
        result['completion'] = {'version': 1, 'semantic': body, 'sha256': digest(body), 'original_receipt_sha256': receipt_hash}
        return result, None
    previous = _latest(store, generation, seal['original_receipt_sha256'])
    count = previous['count'] + 1 if previous else 1
    if count > MAX_REVALIDATIONS:
        _invalid()
    record = {'version': 1, 'count': count, 'previous': previous['sha256'] if previous else seal['original_receipt_sha256'],
              'receipt_sha256': receipt_hash, 'receipt': copy.deepcopy(receipt)}
    record['sha256'] = digest(record)
    return copy.deepcopy(g['manifest']), record
