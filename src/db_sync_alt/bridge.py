"""Bridge readiness for midnight-node 2.1.101: metadata coverage, its verification and the checkpoint report.

Once governance sets the bridge's main chain scripts and a data checkpoint, the node runs its bridge
query on every block import: the transactions with an output at the ICS address, their ICS and
reserve token flows and `tx_metadata.json` of key 6500973 (the transfer's recipient). db-sync-alt
stores that metadata forward only: every generation acquired from Blockfrost carries the
`bridge_metadata_v1` capability and stores one `tx_metadata` fact per transaction with an output at
the ICS address in its event range (`present` 0 when the transaction has no such metadatum). A
cardano-db-sync acquisition never reads metadata: coverage restarts with the next Blockfrost update.

The coverage start C is the first Cardano block from which the facts are complete for the bridge
query: the block after the newest generation of the ready lineage without bridge metadata. A checkpoint `Block(n)` is supported when
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


def verify_assets(store, g, units, governance, identity, *, delta=False):
    """Every stored asset is a profile unit, except at an ICS or reserve output stored by a covered
    generation, which keeps every asset it holds. Mints are profile units only.

    Assets outside the profile are few (the assets table is small): they are found first and only
    their rows are checked, through the (policy, name) indexes."""
    gen, db, scope = g['id'], store.db, '=' if delta else '<='
    outside = db.execute("SELECT policy,name,first_generation FROM chain.assets WHERE first_generation<=%s"
                         " AND encode(policy,'hex')||encode(name,'hex')<>ALL(%s::text[]) AND encode(policy,'hex')<>ALL(%s::text[])",
                         (gen, sorted(units), sorted(governance))).fetchall()
    if not outside:
        return
    configs = identity['configs']
    bridge = [configs['ics-config']['illiquid_circulation_supply_validator_address'], configs['reserve-config']['reserve_validator_address']]
    covered_ids = covered_generations(store, gen) if stored(store) else []
    for policy, name, first in outside:
        if (not delta or first == gen) and first not in covered_ids:
            raise Refusal('selected_asset_outside_profile')
        if db.execute('SELECT 1 FROM chain.mints WHERE policy=%s AND name=%s AND first_generation' + scope + '%s LIMIT 1', (policy, name, gen)).fetchone():
            raise Refusal('selected_asset_outside_profile')
        if db.execute('SELECT 1 FROM chain.output_assets a JOIN chain.outputs o ON o.tx_hash=a.tx_hash AND o.output_index=a.output_index'
                      ' WHERE a.policy=%s AND a.name=%s AND a.first_generation' + scope + '%s AND NOT (o.address=ANY(%s::text[])'
                      ' AND o.first_generation=a.first_generation AND a.first_generation=ANY(%s::bigint[])) LIMIT 1',
                      (policy, name, gen, bridge, covered_ids)).fetchone():
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
    """C, the first Cardano block from which the ready lineage holds complete bridge facts (None: no
    ready generation): the block after the newest generation without bridge metadata.

    Read from the generations' summary columns only, never a manifest (status runs with summary-only
    access): verification admits a generation of the current format only when its bridge capability is
    exactly "acquired from Blockfrost"; generations up to an epoch parameter upgrade have none."""
    ready = store.state()['canonical_ready']
    if ready is None:
        return None
    boundary = -1 if store.epoch_upgrade is None else store.epoch_upgrade['generation']
    newest = store.db.execute("SELECT max(id) FROM chain.generations WHERE id<=%s AND (source<>'blockfrost-cardano' OR id<=%s)",
                              (ready, boundary)).fetchone()[0]
    if newest is None:
        root = store.db.execute('SELECT start_height,start_index FROM chain.generations WHERE parent IS NULL').fetchone()
        return root[0] + (1 if root[1] > 0 else 0)
    return store.db.execute('SELECT target_height FROM chain.generations WHERE id=%s', (newest,)).fetchone()[0] + 1


def coverage(store):
    """`report(store)` of a serving store that holds bridge metadata; None for any other store."""
    state = store.state()
    if not stored(store) or state['schema_version'] < 5 or state['canonical_ready'] is None or state['revoked']:
        return None
    return report(store)


def ended(before, ready, after):
    """How an update without bridge metadata ended coverage: None unless the ready lineage held covered
    blocks (`before`, the report while `ready` was the ready generation) and the start moved (`after`)."""
    if before is None or after is None or before['coverage_start'] > ready['target_height'] or after['coverage_start'] == before['coverage_start']:
        return None
    return {'previous_coverage_start': before['coverage_start'], 'coverage_start': after['coverage_start']}


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
