"""The served tables: db-sync-shaped copies of the published generation that midnight-node reads.

`serve.*` holds exactly the facts of the canonical generation with db-sync's column names and
types, under the gated `public` views. IDs of the relations the node bounds its event queries
with are positional, so their order is the chain order and a fact discovered late gets the ID of
its chain position:

  block.id     = height                         (height < 2^27)
  tx.id        = height * 2^12 + tx_index
  tx_out.id    = tx.id * 2^12 + output_index
  tx_in.id     = spending tx.id * 2^12 + input ordinal
  ma_tx_out.id = tx_out.id * 2^12 + asset ordinal

Every field is range-checked by the tables' constraints (overflow is refused, never wrapped).
From schema 5, epoch_param also holds each epoch's max_tx_size (midnight-node 2.1.101 reads
MAX(epoch_param.max_tx_size) of the cursor block's epoch); below schema 5 it has nonces only.
From schema 5, tx_metadata holds the bridge metadatum (key 6500973) of each covered transaction that
has one, with the transaction's ID (tx_metadata.id = tx_metadata.tx_id = tx.id).
multi_asset, datum, pool_hash and epoch_stake keep the stable IDs of the stored facts. The tables
are written in ID order (chain order) when built and grow by appends at each publication, inside
the publication transaction.
"""
from contextlib import contextmanager
import logging

import psycopg

from db_sync_alt.contract import Refusal
from db_sync_alt.verify.limits import any_row, check, check_all

log = logging.getLogger('db_sync_alt.serve')

HEIGHT_LIMIT = 1 << 27
FIELD = 4096
TABLES = ('block', 'tx', 'tx_out', 'tx_in', 'ma_tx_out', 'multi_asset', 'datum', 'epoch_param', 'pool_hash', 'epoch_stake')
# Served from schema 5 on.
BRIDGE = 'tx_metadata'
KEYS = {name: ('epoch_no' if name == 'epoch_param' else 'id') for name in (*TABLES, BRIDGE)}
# The index names midnight-node checks in pg_indexes; each must resolve to exactly one row.
NODE_INDEXES = ('idx_multi_asset_policy_name_hex', 'idx_ma_tx_out_ident', 'idx_tx_out_address', 'idx_ma_tx_out_id_ident',
                'idx_multi_asset_policy_name', 'idx_block_block_no', 'idx_tx_block_id', 'idx_tx_out_tx_id',
                'idx_tx_out_data_hash', 'idx_tx_in_tx_in_id', 'idx_tx_in_tx_out_id_tx_out_index')

TX = '(b.height::bigint*4096+t.tx_index)'
# Expected rows of each served table, from the stored facts of generations `{scope}` the given one.
EXPECTED = {
    'tx': 'SELECT ' + TX + ' AS id,t.hash,b.height::bigint AS block_id,t.tx_index AS block_index,t.valid=1 AS valid_contract'
          ' FROM chain.transactions t JOIN chain.blocks b ON b.hash=t.block_hash WHERE t.first_generation{scope}%(g)s{range}',
    'tx_out': 'SELECT ' + TX + '*4096+o.output_index AS id,' + TX + ' AS tx_id,o.output_index AS index,o.address,o.value,'
              'o.datum_hash AS data_hash,NULL::bigint AS consumed_by_tx_id FROM chain.outputs o JOIN chain.transactions t ON t.hash=o.tx_hash'
              ' JOIN chain.blocks b ON b.hash=t.block_hash WHERE o.first_generation{scope}%(g)s{range}',
    'tx_in': 'SELECT (sb.height::bigint*4096+st.tx_index)*4096+i.ordinal AS id,sb.height::bigint*4096+st.tx_index AS tx_in_id,'
             'ob.height::bigint*4096+ot.tx_index AS tx_out_id,i.origin_index AS tx_out_index FROM chain.inputs i'
             ' JOIN chain.transactions st ON st.hash=i.tx_hash JOIN chain.blocks sb ON sb.hash=st.block_hash'
             ' JOIN chain.transactions ot ON ot.hash=i.origin_hash JOIN chain.blocks ob ON ob.hash=ot.block_hash'
             ' WHERE i.first_generation{scope}%(g)s{range}',
    'ma_tx_out': 'SELECT (' + TX + '*4096+a.output_index)*4096+a.ordinal AS id,a.quantity,' + TX + '*4096+a.output_index AS tx_out_id,'
                 'm.id AS ident FROM chain.output_assets a JOIN chain.transactions t ON t.hash=a.tx_hash JOIN chain.blocks b ON b.hash=t.block_hash'
                 ' JOIN chain.assets m ON m.policy=a.policy AND m.name=a.name WHERE a.first_generation{scope}%(g)s{range}',
    'multi_asset': 'SELECT id,policy,name{own} FROM chain.assets WHERE first_generation{scope}%(g)s{range}',
    'datum': 'SELECT id,hash,value{own} FROM chain.datums WHERE first_generation{scope}%(g)s{range}',
    'epoch_param': 'SELECT epoch AS epoch_no,nonce{sizes}{own} FROM chain.epoch_nonces WHERE first_generation{scope}%(g)s{range}',
    'pool_hash': 'SELECT p.id,p.pool_hash AS hash_raw FROM chain.pools p WHERE EXISTS(SELECT 1 FROM chain.epoch_stakes s'
                 ' WHERE s.pool_hash=p.pool_hash AND s.first_generation{scope}%(g)s){range}',
    'epoch_stake': 'SELECT s.id,p.id AS pool_id,s.amount,s.epoch AS epoch_no{own} FROM chain.epoch_stakes s JOIN chain.pools p USING(pool_hash)'
                   ' WHERE s.first_generation{scope}%(g)s{range}',
    'tx_metadata': 'SELECT ' + TX + ' AS id,6500973::public.word64type AS key,' + TX + ' AS tx_id,m.json FROM chain.tx_metadata m'
                   ' JOIN chain.transactions t ON t.hash=m.tx_hash JOIN chain.blocks b ON b.hash=t.block_hash'
                   ' WHERE m.present=1 AND m.first_generation{scope}%(g)s{range}',
}
COLUMNS = {
    'block': ('id', 'hash', 'epoch_no', 'slot_no', 'block_no', 'time', 'tx_count'),
    'tx': ('id', 'hash', 'block_id', 'block_index', 'valid_contract'),
    'tx_out': ('id', 'tx_id', 'index', 'address', 'value', 'data_hash', 'consumed_by_tx_id'),
    'tx_in': ('id', 'tx_in_id', 'tx_out_id', 'tx_out_index'),
    'ma_tx_out': ('id', 'quantity', 'tx_out_id', 'ident'),
    'multi_asset': ('id', 'policy', 'name'),
    'datum': ('id', 'hash', 'value'),
    'epoch_param': ('epoch_no', 'nonce', 'max_tx_size'),
    'pool_hash': ('id', 'hash_raw'),
    'epoch_stake': ('id', 'pool_id', 'amount', 'epoch_no'),
    'tx_metadata': ('id', 'key', 'tx_id', 'json'),
}
# A build writes each served table in ranges: the positional tables by the height of their block, the others
# by their stable id (epoch for epoch_param). Each statement then stays small whatever the length of the
# chain, under the store's session limit (issue 00042). A range is sized by rows, not bytes: one the limit
# still cancels (dense blocks, large values, a slow disk) is rolled back to its savepoint and written as two
# halves, down to one height or id (`_fill`, issue 00050).
BUILD_HEIGHTS = 10000
BUILD_IDS = 100000
# Datum values are large: on Mainnet (2026-11) about 0.9 KiB of jsonb per id, so 100,000 ids wrote about 90 MiB
# in one statement and took up to 26 s on a disk writing 16 MiB/s. 10,000 ids write about 9 MiB.
BUILD_DATUMS = 10000
# (relation, column, step) whose stored values the ranges of a table cover.
RANGE = {'block': ('chain.current_membership', 'height', BUILD_HEIGHTS), 'tx': ('chain.blocks', 'height', BUILD_HEIGHTS),
         'tx_out': ('chain.blocks', 'height', BUILD_HEIGHTS), 'tx_in': ('chain.blocks', 'height', BUILD_HEIGHTS),
         'ma_tx_out': ('chain.blocks', 'height', BUILD_HEIGHTS), 'tx_metadata': ('chain.blocks', 'height', BUILD_HEIGHTS),
         'multi_asset': ('chain.assets', 'id', BUILD_IDS), 'datum': ('chain.datums', 'id', BUILD_DATUMS), 'pool_hash': ('chain.pools', 'id', BUILD_IDS),
         'epoch_stake': ('chain.epoch_stakes', 'id', BUILD_IDS), 'epoch_param': ('chain.epoch_nonces', 'epoch', BUILD_IDS)}
# The id-keyed tables add a range of their id to their EXPECTED query: one index range of one table.
ID_RANGE = {'multi_asset': 'id', 'datum': 'id', 'pool_hash': 'p.id', 'epoch_stake': 's.id', 'epoch_param': 'epoch'}
# The positional tables are built from one height range of blocks at a time, each block's transactions and
# their facts found by index (LATERAL ... OFFSET 0 keeps that order whatever the planner estimates: the
# relevant facts are not spread evenly over the chain). The same rows as EXPECTED, which serve.verify checks.
_TXS = ' CROSS JOIN LATERAL (SELECT hash,tx_index,valid FROM chain.transactions t WHERE t.block_hash=b.hash OFFSET 0) t'
BUILD = {
    'block': 'SELECT m.height::bigint AS id,b.hash,b.epoch AS epoch_no,b.slot AS slot_no,b.height AS block_no,b.time,b.tx_count'
             ' FROM chain.current_membership m CROSS JOIN LATERAL (SELECT hash,epoch,slot,height,time,tx_count FROM chain.blocks b'
             ' WHERE b.hash=m.hash OFFSET 0) b WHERE m.height BETWEEN %(lo)s AND %(hi)s',
    'tx': 'SELECT ' + TX + ' AS id,t.hash,b.height::bigint AS block_id,t.tx_index AS block_index,t.valid=1 AS valid_contract FROM chain.blocks b'
          ' CROSS JOIN LATERAL (SELECT hash,tx_index,valid{own} FROM chain.transactions t WHERE t.block_hash=b.hash AND t.first_generation<=%(g)s OFFSET 0) t'
          ' WHERE b.height BETWEEN %(lo)s AND %(hi)s',
    'tx_out': 'SELECT ' + TX + '*4096+o.output_index AS id,' + TX + ' AS tx_id,o.output_index AS index,o.address,o.value,'
              'o.datum_hash AS data_hash,NULL::bigint AS consumed_by_tx_id FROM chain.blocks b' + _TXS +
              ' CROSS JOIN LATERAL (SELECT output_index,address,value,datum_hash{own} FROM chain.outputs o WHERE o.tx_hash=t.hash'
              ' AND o.first_generation<=%(g)s OFFSET 0) o WHERE b.height BETWEEN %(lo)s AND %(hi)s',
    'tx_in': 'SELECT (sb.height::bigint*4096+st.tx_index)*4096+i.ordinal AS id,sb.height::bigint*4096+st.tx_index AS tx_in_id,'
             'ob.height::bigint*4096+ot.tx_index AS tx_out_id,i.origin_index AS tx_out_index FROM chain.blocks sb'
             ' CROSS JOIN LATERAL (SELECT hash,tx_index FROM chain.transactions st WHERE st.block_hash=sb.hash OFFSET 0) st'
             ' CROSS JOIN LATERAL (SELECT ordinal,origin_hash,origin_index{own} FROM chain.inputs i WHERE i.tx_hash=st.hash'
             ' AND i.first_generation<=%(g)s OFFSET 0) i'
             ' CROSS JOIN LATERAL (SELECT tx_index,block_hash FROM chain.transactions ot WHERE ot.hash=i.origin_hash OFFSET 0) ot'
             ' CROSS JOIN LATERAL (SELECT height FROM chain.blocks ob WHERE ob.hash=ot.block_hash OFFSET 0) ob'
             ' WHERE sb.height BETWEEN %(lo)s AND %(hi)s',
    'ma_tx_out': 'SELECT (' + TX + '*4096+a.output_index)*4096+a.ordinal AS id,a.quantity,' + TX + '*4096+a.output_index AS tx_out_id,'
                 'm.id AS ident FROM chain.blocks b' + _TXS +
                 ' CROSS JOIN LATERAL (SELECT output_index,ordinal,quantity,policy,name{own} FROM chain.output_assets a WHERE a.tx_hash=t.hash'
                 ' AND a.first_generation<=%(g)s OFFSET 0) a'
                 ' CROSS JOIN LATERAL (SELECT id FROM chain.assets m WHERE m.policy=a.policy AND m.name=a.name OFFSET 0) m'
                 ' WHERE b.height BETWEEN %(lo)s AND %(hi)s',
    'tx_metadata': 'SELECT ' + TX + ' AS id,6500973::public.word64type AS key,' + TX + ' AS tx_id,m.json FROM chain.blocks b' + _TXS +
                   ' CROSS JOIN LATERAL (SELECT json{own} FROM chain.tx_metadata m WHERE m.tx_hash=t.hash AND m.present=1'
                   ' AND m.first_generation<=%(g)s OFFSET 0) m WHERE b.height BETWEEN %(lo)s AND %(hi)s',
}
# The alias of each positional table's fact lateral in BUILD.
FACT = {'tx': 't', 'tx_out': 'o', 'tx_in': 'i', 'ma_tx_out': 'a', 'tx_metadata': 'm'}
BLOCKS = ('SELECT m.height::bigint AS id,b.hash,b.epoch AS epoch_no,b.slot AS slot_no,b.height AS block_no,b.time,b.tx_count'
          ' FROM ({source}) m JOIN chain.blocks b ON b.hash=m.hash')
CURRENT = 'SELECT height,hash FROM chain.current_membership'
# Served headers are compared with the projection in height ranges of this size, so no verification
# statement grows with the chain: each stays far below the store's statement timeout.
BLOCK_RANGE = 100000


# The served epoch parameters gain max_tx_size at schema 5.
SIZES_SCHEMA = 5


def sizes(db):
    """Whether the served epoch_param carries max_tx_size (schema 5 and later)."""
    return db.execute('SELECT schema_version FROM chain.service_state WHERE id=1').fetchone()[0] >= SIZES_SCHEMA


def tables(db):
    """The served tables of the database's schema: tx_metadata is served from schema 5."""
    return (*TABLES, BRIDGE) if sizes(db) else TABLES


def columns(db, table):
    if table == 'epoch_param' and not sizes(db):
        return COLUMNS[table][:2]
    return COLUMNS[table]


def expected(table, generation, *, only_new=False, db=None):
    """SQL and parameters of the rows `table` must hold for `generation` (only its own rows with `only_new`)."""
    if table == 'block':
        return BLOCKS.format(source=CURRENT), {}
    served = ',max_tx_size' if table == 'epoch_param' and (db is None or sizes(db)) else ''
    return EXPECTED[table].format(scope='=' if only_new else '<=', sizes=served, range='', own=''), {'g': generation}


def built(table, generation, bounds, db, *, only_new=False):
    """SQL and parameters of one build range of `table` for `generation`: the rows of `expected` (with
    `only_new`, of `expected(..., only_new=True)`) whose block height (positional tables) or id (the others)
    lies in `bounds`, a (low, high) pair."""
    params = {'g': generation, 'lo': bounds[0], 'hi': bounds[1]}
    if table in BUILD:
        if not only_new:
            return BUILD[table].format(own=''), params
        # The block's facts are still found by the block (the build's own lateral); the generation's own
        # are selected after it, so no plan reads every fact of the generation for each block.
        return BUILD[table].format(own=',first_generation') + ' AND ' + FACT[table] + '.first_generation=%(g)s', params
    served = ',max_tx_size' if table == 'epoch_param' and sizes(db) else ''
    ranged = ' AND ' + ID_RANGE[table] + ' BETWEEN %(lo)s AND %(hi)s'
    if not only_new or table == 'pool_hash':
        return EXPECTED[table].format(scope='=' if only_new else '<=', sizes=served, range=ranged, own=''), params
    # The same for an id range: the build's statement reads the range by id, the generation's own rows after it.
    return ('SELECT * FROM (' + EXPECTED[table].format(scope='<=', sizes=served, range=ranged, own=',first_generation')
            + ' OFFSET 0) f WHERE f.first_generation=%(g)s'), params


def ranges(db, table):
    """The build ranges of `table`: consecutive (low, high) pairs covering every stored value of its column."""
    relation, column, step = RANGE[table]
    low, high = db.execute('SELECT min(' + column + '),max(' + column + ') FROM ' + relation).fetchone()
    if low is None:
        return []
    first = low - low % step
    return [(start, start + step - 1) for start in range(first, high + 1, step)]


def _insert(db, table, query, params):
    names = ','.join(columns(db, table))
    return db.execute('INSERT INTO serve.' + table + '(' + names + ') SELECT ' + names + ' FROM (' + query + ') x ORDER BY '
                      + KEYS[table], params).rowcount


def _fill(db, table, statement, low, high):
    """Insert the rows of `statement(low, high)` (SQL and parameters of one range) in one statement under a
    savepoint. If the session's statement limit cancels it, roll back to the savepoint and fill low..middle,
    then middle+1..high, the same way, down to one height or id; a single value still cancelled raises the
    cancellation (the caller's transaction fails as before). The rows, and their order, are those of the
    single statement. Returns the number of rows."""
    try:
        with db.transaction():
            return _insert(db, table, *statement(low, high))
    except psycopg.errors.QueryCanceled:
        if low == high:
            raise
    middle = (low + high) // 2
    log.warning('serve_range_split table=%s low=%s high=%s', table, low, high)
    return _fill(db, table, statement, low, middle) + _fill(db, table, statement, middle + 1, high)


@contextmanager
def _no_jit(db):
    # A range's plan cost follows the planner's estimate, not the range: relevant facts are not spread evenly
    # over the chain, so a small range can look large. JIT would then compile every small statement; it is
    # off for the ranged statements (transaction-local, the previous setting restored).
    previous = db.execute("SELECT current_setting('jit')").fetchone()[0]
    db.execute("SELECT set_config('jit','off',true)")
    yield
    db.execute("SELECT set_config('jit',%s,true)", (previous,))


def build(db, generation):
    """Fill the empty served tables for `generation` (the current projection), in chain order, one range per
    statement (a cancelled range as two halves, see `_fill`)."""
    counts = {}
    with _no_jit(db):
        for table in tables(db):
            counts[table] = 0
            for low, high in ranges(db, table):
                counts[table] += _fill(db, table, lambda lo, hi: built(table, generation, (lo, hi), db), low, high)
    _mark(db, generation)
    return counts


def advance(db, generation, *, cutoff, bindings, bindings_args):
    """Publication of `generation`: mirror the projection's header change and append the generation's facts.

    The projection drops the headers above `cutoff` and adds the rows of `bindings` (the
    generation's own header bindings); the served blocks change the same way. Facts are only
    ever added: every fact row first stored by `generation` gets its row here, one statement per
    table under a savepoint. A statement the session limit cancels is rolled back and its rows are
    written in build ranges instead (`_new_ranges`, `_fill`): the same rows.
    """
    counts = {'block_removed': db.execute('DELETE FROM serve.block WHERE id>%s', (cutoff,)).rowcount}
    counts['block'] = _insert(db, 'block', BLOCKS.format(source=bindings), bindings_args)
    for table in tables(db)[1:]:
        try:
            with db.transaction():
                counts[table] = _insert(db, table, *_new(table, generation, db))
        except psycopg.errors.QueryCanceled:
            # A generation of many dense blocks (a cardano-db-sync update onto a serving store): its rows in the
            # build ranges that hold them, each written as a build range is (issue 00050).
            log.warning('serve_advance_ranged table=%s generation=%s', table, generation)
            with _no_jit(db):
                counts[table] = sum(_fill(db, table, lambda lo, hi: _new(table, generation, db, (lo, hi)), low, high)
                                    for low, high in _new_ranges(db, table, generation))
    _mark(db, generation)
    return counts


def _new(table, generation, db, bounds=None):
    """SQL and parameters of the rows `generation`'s own facts add to `table`: all of them, or those of one
    build range `bounds`."""
    query, params = expected(table, generation, only_new=True, db=db) if bounds is None else built(table, generation, bounds, db, only_new=True)
    if table == 'pool_hash':
        query = query + ' AND NOT EXISTS(SELECT 1 FROM serve.pool_hash x WHERE x.id=p.id)'
    return query, params


# A served positional ID shifted right by this many bits is the height of its block (see the IDs above).
HEIGHT_BITS = {'tx': 12, 'tx_metadata': 12, 'tx_out': 24, 'tx_in': 24, 'ma_tx_out': 36}


def _new_ranges(db, table, generation):
    """The build ranges of `table` that hold rows of `generation`'s own facts. One statement reads all of them:
    it grows with the generation, so it runs under the verification limit."""
    step = RANGE[table][2]
    query, params = _new(table, generation, db)
    found = check_all(db, 'SELECT DISTINCT ' + KEYS[table] + '/%(unit)s FROM (' + query + ') x',
                      {**params, 'unit': step << HEIGHT_BITS.get(table, 0)})
    return [(w * step, w * step + step - 1) for w in sorted(w for w, in found)]


def _mark(db, generation):
    require_spends(db)
    db.execute('UPDATE chain.integrity_state SET serve_generation=%s WHERE id=1', (generation,))


def require_spends(db):
    """At least one served input: the node reads `EXISTS (SELECT 1 FROM tx_in)` once per process to
    choose how it finds spends, and an empty tx_in would make it read spent outputs as unspent."""
    if not db.execute('SELECT EXISTS (SELECT 1 FROM serve.tx_in)').fetchone()[0]:
        raise Refusal('no_served_spend')


def verify(store, generation):
    """The served tables hold exactly the generation's facts with their positional IDs."""
    db = store.db
    row = db.execute('SELECT projection_generation,serve_generation FROM chain.integrity_state WHERE id=1').fetchone()
    if row != (generation, generation):
        raise Refusal('serve_projection_mismatch')
    for table in tables(db):
        if table == 'block':
            mismatch = _blocks_differ(db)
        else:
            # One statement over the whole history: it runs under the verification statement limit.
            query, params = expected(table, generation, db=db)
            mismatch = any_row(db, _difference(db, table, 'serve.' + table, query), params)
        if mismatch:
            raise Refusal('serve_rows_mismatch')
        store.progress('serve_verification', generation=generation, family=table)
    require_spends(db)
    verify_ordinals(db, generation)
    indexes = dict(db.execute("SELECT indexname,count(*) FILTER (WHERE schemaname='serve') || '/' || count(*) FROM pg_indexes"
                              ' WHERE indexname=ANY(%s) GROUP BY indexname', (list(NODE_INDEXES),)).fetchall())
    if indexes != {name: '1/1' for name in NODE_INDEXES}:
        raise Refusal('node_index_names_invalid')


def _difference(db, table, served, query):
    """A statement whose rows show where `served` and the rows of `query` differ (none: they are equal)."""
    key, cols = KEYS[table], columns(db, table)
    left = 'ROW(' + ','.join('s.' + c for c in cols) + ')'
    right = 'ROW(' + ','.join('e.' + c for c in cols) + ')'
    return ('SELECT 1 FROM ' + served + ' s FULL JOIN (' + query + ') e ON e.' + key + '=s.' + key
            + ' WHERE s.' + key + ' IS NULL OR e.' + key + ' IS NULL OR ' + left + ' IS DISTINCT FROM ' + right)


def _differ(db, table, served, query, params):
    # Counted, not LIMIT 1, for the reason given at limits.any_row.
    return db.execute('SELECT count(*) FROM (' + _difference(db, table, served, query) + ') found', params).fetchone()[0] > 0


def _blocks_differ(db):
    """serve.block against the projection's headers, one height range per statement."""
    low, high = db.execute('SELECT least((SELECT min(height) FROM chain.current_membership),(SELECT min(id) FROM serve.block)),'
                           'greatest((SELECT max(height) FROM chain.current_membership),(SELECT max(id) FROM serve.block))').fetchone()
    if low is None:
        return False
    for start in range(low, high + 1, BLOCK_RANGE):
        bounds = {'lo': start, 'hi': start + BLOCK_RANGE}
        query = BLOCKS.format(source=CURRENT + ' WHERE height>=%(lo)s AND height<%(hi)s')
        if _differ(db, 'block', '(SELECT * FROM serve.block WHERE id>=%(lo)s AND id<%(hi)s)', query, bounds):
            return True
    return False


def verify_ordinals(db, generation):
    """Stored ordinals follow ledger order: inputs by (origin hash, index), assets by (policy, name)."""
    if check(db, 'SELECT 1 FROM (SELECT ordinal,lag(ordinal) OVER (PARTITION BY tx_hash ORDER BY origin_hash,origin_index) AS previous'
             ' FROM chain.inputs WHERE first_generation<=%s) x WHERE ordinal<=previous LIMIT 1', (generation,)):
        raise Refusal('ordinal_order_invalid')
    if check(db, 'SELECT 1 FROM (SELECT ordinal,lag(ordinal) OVER (PARTITION BY tx_hash,output_index ORDER BY policy,name) AS previous'
             ' FROM chain.output_assets WHERE first_generation<=%s) x WHERE ordinal<=previous LIMIT 1', (generation,)):
        raise Refusal('ordinal_order_invalid')
