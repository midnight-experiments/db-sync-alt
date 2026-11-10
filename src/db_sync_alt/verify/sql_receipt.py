"""Allocation-free conservative admission for the final SQL completion receipt."""
from db_sync_alt.contract import TABLES, Refusal, canonical
from db_sync_alt.verify.completion import MAX_RECEIPT_BYTES


def receipt_size_bound(manifest, *, start, range_start, target, witness, root, window, version):
    """Bound both receipt and its embedded independent inventory, without ranges.

    Each interval uses the largest endpoint's decimal width. Epoch acquisition
    can omit inherited epochs, so reserving every required epoch is conservative.
    Inventory cardinalities reserve PostgreSQL's signed-bigint count domain;
    hashes have a fixed 64-character encoding. No candidate fact counts enter
    this admission or the subsequent independent source enumeration.
    """
    if type(window) is not int or not 1 <= window <= 10000:
        raise Refusal('invalid_window')
    independent = {
        'version': 1,
        'scope': 'independent-new-facts-and-membership-with-certified-parent',
        'parent_digest': manifest['parent_digest'], 'ranges': [],
        'epochs': manifest['required_epochs'],
        'inventory': {table: {'count': 2**63-1, 'sha256': 'f'*64} for table in TABLES},
    }
    receipt = {'source_kind': 'cardano-db-sync-postgresql',
               'anchors': manifest['anchors'], 'independent': independent}
    size = len(canonical(receipt).encode())
    intervals = []
    if root and start > 0:
        intervals.append(('prerequisites', 0, start-1, window if version == 2 else start))
    intervals.extend((phase, range_start, hi, window) for phase, hi in
                     (('metadata', witness), ('events', target)))
    count = 0
    for phase, lo, hi, width in intervals:
        if hi < lo:
            continue
        n = (hi-lo)//width+1
        # Empty [] is already included. Every item adds its bytes and a comma,
        # except that the very first item has no comma.
        size += n*(len(canonical([phase, hi, hi]).encode())+1)
        count += n
    return size-int(count > 0)


def admit_receipt_size(manifest, **bounds):
    size = receipt_size_bound(manifest, **bounds)
    if size > MAX_RECEIPT_BYTES:
        raise Refusal('sql_completion_receipt_too_large')
    return size
