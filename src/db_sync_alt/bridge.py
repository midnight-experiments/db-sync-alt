"""Bridge readiness for midnight-node 2.1.101: metadata coverage, its verification and the checkpoint report.

Once governance sets the bridge's main chain scripts and a data checkpoint, the node runs its bridge
query on every block import: the transactions with an output at the ICS address, their ICS and
reserve token flows and `tx_metadata.json` of key 6500973 (the transfer's recipient). db-sync-alt
stores that metadata forward only: every generation acquired from Blockfrost carries the
`bridge_metadata_v1` capability and stores one `tx_metadata` fact per transaction with an output at
the ICS address in its event range (`present` 0 when the transaction has no such metadatum). A
cardano-db-sync acquisition never reads metadata, and coverage never ends once it has begun.

The coverage start C is the first Cardano block from which the facts are complete for the bridge
query: the block after the parent of the first covered generation of the ready lineage, or the
block after the ready generation while it is not covered. A checkpoint `Block(n)` is supported when
n + 1 >= C (the node reads the blocks after n); `Tx(h)` and `TxReserveTransfer(h)` when the
transaction h is served in a block >= C. Nothing below C is backfilled or guessed.
"""
import re

from db_sync_alt.contract import BRIDGE_CAPABILITY, BRIDGE_METADATA_KEY, Refusal

COVERED = "(manifest->'capabilities'->'" + BRIDGE_CAPABILITY + "')='true'::jsonb"
HEX = re.compile(r'(?:[0-9a-fA-F]{2})*')


def recipient(value):
    """The node's reading of a metadatum (JSON value): the recipient bytes of a one-element array
    holding a `0x`-prefixed hexadecimal string, else None (the node's `Invalid`)."""
    if not isinstance(value, list) or len(value) != 1 or not isinstance(value[0], str):
        return None
    text = value[0]
    if not text.startswith('0x') or HEX.fullmatch(text, 2) is None:
        return None
    return bytes.fromhex(text[2:])


def stored(store):
    """Whether the store has the bridge metadata family (a nonce-only store has not)."""
    return 'tx_metadata' in store.physical_tables


def covered(manifest):
    return isinstance(manifest, dict) and isinstance(manifest.get('capabilities'), dict) and manifest['capabilities'].get(BRIDGE_CAPABILITY) is True


def covered_generations(store, generation, *, delta=False):
    """Covered generation IDs whose facts a verification of `generation` reads: itself (delta) or all up to it."""
    if delta:
        return [generation] if store.bridge_covered(generation) else []
    return [row[0] for row in store.db.execute('SELECT id FROM chain.generations WHERE id<=%s AND ' + COVERED + ' ORDER BY id', (generation,))]


def verify_lineage(store, g):
    """Coverage never ends: a generation after a covered one is covered."""
    if g['parent'] is None or store.legacy_generation(g['id']) or store.legacy_generation(g['parent']):
        return
    if store.bridge_covered(g['parent']) and not covered(g['manifest']):
        raise Refusal('bridge_metadata_coverage_gap')


def _outside(alias):
    return ("encode({a}.policy,'hex')||encode({a}.name,'hex')<>ALL(%(units)s::text[]) AND encode({a}.policy,'hex')<>ALL(%(governance)s::text[])").format(a=alias)


def verify_assets(store, g, units, governance, identity, *, delta=False):
    """Every stored asset is a profile unit, except at an ICS or reserve output stored by a covered
    generation, which keeps every asset it holds. Mints are profile units only."""
    gen, db, scope = g['id'], store.db, '=' if delta else '<='
    configs = identity['configs']
    params = {'g': gen, 'units': sorted(units), 'governance': sorted(governance),
              'bridge': [configs['ics-config']['illiquid_circulation_supply_validator_address'],
                         configs['reserve-config']['reserve_validator_address']],
              'covered': covered_generations(store, gen, delta=delta) if stored(store) else []}
    checks = (
        'SELECT 1 FROM chain.assets a WHERE a.first_generation' + scope + '%(g)s AND ' + _outside('a') + ' AND a.first_generation<>ALL(%(covered)s::bigint[]) LIMIT 1',
        'SELECT 1 FROM chain.mints a WHERE a.first_generation' + scope + '%(g)s AND ' + _outside('a') + ' LIMIT 1',
        'SELECT 1 FROM chain.output_assets a JOIN chain.outputs o ON o.tx_hash=a.tx_hash AND o.output_index=a.output_index'
        ' WHERE a.first_generation' + scope + '%(g)s AND ' + _outside('a') +
        ' AND NOT (o.address=ANY(%(bridge)s::text[]) AND o.first_generation=a.first_generation AND a.first_generation=ANY(%(covered)s::bigint[])) LIMIT 1',
    )
    for query in checks:
        if db.execute(query, params).fetchone():
            raise Refusal('selected_asset_outside_profile')


EVENT_RANGE = ('b.height>COALESCE(p.target_height,x.start_height-1) AND b.height<=x.target_height')


def verify_family(store, g, identity, *, delta=False):
    """The tx_metadata facts of `g` (delta) or of its lineage: one row, first stored with its
    transaction, for every transaction with an output at the ICS address in the event range of a
    covered generation, and no other row."""
    if not stored(store):
        return
    gen, scope = g['id'], '=' if delta else '<='
    params = {'g': gen, 'ics': identity['configs']['ics-config']['illiquid_circulation_supply_validator_address'],
              'covered': covered_generations(store, gen, delta=delta)}
    joins = (' JOIN chain.blocks b ON b.hash=t.block_hash JOIN chain.generations x ON x.id=t.first_generation'
             ' LEFT JOIN chain.generations p ON p.id=x.parent')
    missing = ('SELECT 1 FROM chain.outputs o JOIN chain.transactions t ON t.hash=o.tx_hash' + joins +
               ' WHERE o.address=%(ics)s AND t.first_generation=ANY(%(covered)s::bigint[]) AND ' + EVENT_RANGE +
               ' AND NOT EXISTS(SELECT 1 FROM chain.tx_metadata m WHERE m.tx_hash=t.hash AND m.first_generation=t.first_generation) LIMIT 1')
    if store.db.execute(missing, params).fetchone():
        raise Refusal('bridge_metadata_incomplete')
    extra = ('SELECT 1 FROM chain.tx_metadata m JOIN chain.transactions t ON t.hash=m.tx_hash' + joins +
             ' WHERE m.first_generation' + scope + '%(g)s AND NOT (m.first_generation=ANY(%(covered)s::bigint[]) AND t.first_generation=m.first_generation'
             ' AND ' + EVENT_RANGE + ' AND EXISTS(SELECT 1 FROM chain.outputs o WHERE o.tx_hash=t.hash AND o.address=%(ics)s)) LIMIT 1')
    if store.db.execute(extra, params).fetchone():
        raise Refusal('bridge_metadata_unexpected')


def coverage_start(store):
    """C, the first Cardano block from which the ready lineage holds complete bridge facts (None: no ready generation)."""
    ready = store.state()['canonical_ready']
    if ready is None:
        return None
    g = store.generation(ready)
    if store.legacy_generation(ready) or not covered(g['manifest']):
        return g['target_height'] + 1
    first = store.db.execute('SELECT min(id) FROM chain.generations WHERE id<=%s AND ' + COVERED, (ready,)).fetchone()[0]
    root = store.generation(first)
    if root['parent'] is None:
        return root['start_height'] + (1 if root['start_index'] > 0 else 0)
    return store.generation(root['parent'])['target_height'] + 1


def parse_checkpoint(text):
    """The node's bridge data checkpoint as the operator states it: `block:<n>` or `tx:<hash>`
    (Tx and TxReserveTransfer checkpoints both name a transaction)."""
    if not isinstance(text, str):
        raise Refusal('invalid_bridge_checkpoint')
    match = re.fullmatch(r'block:(0|[1-9][0-9]{0,9})|tx:([0-9a-f]{64})', text)
    if match is None or (match.group(1) is not None and int(match.group(1)) > 2**31 - 1):
        raise Refusal('invalid_bridge_checkpoint')
    if match.group(1) is not None:
        return {'kind': 'block', 'height': int(match.group(1))}
    return {'kind': 'tx', 'hash': match.group(2)}


def report(store, checkpoint=None):
    """Status of bridge readiness: the metadata key, the coverage start and, for a stated
    checkpoint, whether the node's bridge query from it reads covered facts only."""
    if isinstance(checkpoint, str):
        checkpoint = parse_checkpoint(checkpoint)
    start = coverage_start(store)
    out = {'metadata_key': BRIDGE_METADATA_KEY, 'coverage_start': start, 'checkpoint': checkpoint,
           'checkpoint_supported': None, 'reason': None}
    if checkpoint is None or start is None:
        return out
    if checkpoint['kind'] == 'block':
        supported, reason = checkpoint['height'] + 1 >= start, 'before_coverage'
    else:
        row = store.db.execute('SELECT block_id FROM serve.tx WHERE hash=%s', (bytes.fromhex(checkpoint['hash']),)).fetchone()
        if row is None:
            supported, reason = False, 'transaction_not_served'
        else:
            supported, reason = row[0] >= start, 'before_coverage'
    out.update(checkpoint_supported=supported, reason=None if supported else reason)
    return out
