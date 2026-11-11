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
from db_sync_alt.contract import Refusal
from db_sync_alt.verify.limits import check

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
          ' FROM chain.transactions t JOIN chain.blocks b ON b.hash=t.block_hash WHERE t.first_generation{scope}%(g)s',
    'tx_out': 'SELECT ' + TX + '*4096+o.output_index AS id,' + TX + ' AS tx_id,o.output_index AS index,o.address,o.value,'
              'o.datum_hash AS data_hash,NULL::bigint AS consumed_by_tx_id FROM chain.outputs o JOIN chain.transactions t ON t.hash=o.tx_hash'
              ' JOIN chain.blocks b ON b.hash=t.block_hash WHERE o.first_generation{scope}%(g)s',
    'tx_in': 'SELECT (sb.height::bigint*4096+st.tx_index)*4096+i.ordinal AS id,sb.height::bigint*4096+st.tx_index AS tx_in_id,'
             'ob.height::bigint*4096+ot.tx_index AS tx_out_id,i.origin_index AS tx_out_index FROM chain.inputs i'
             ' JOIN chain.transactions st ON st.hash=i.tx_hash JOIN chain.blocks sb ON sb.hash=st.block_hash'
             ' JOIN chain.transactions ot ON ot.hash=i.origin_hash JOIN chain.blocks ob ON ob.hash=ot.block_hash'
             ' WHERE i.first_generation{scope}%(g)s',
    'ma_tx_out': 'SELECT (' + TX + '*4096+a.output_index)*4096+a.ordinal AS id,a.quantity,' + TX + '*4096+a.output_index AS tx_out_id,'
                 'm.id AS ident FROM chain.output_assets a JOIN chain.transactions t ON t.hash=a.tx_hash JOIN chain.blocks b ON b.hash=t.block_hash'
                 ' JOIN chain.assets m ON m.policy=a.policy AND m.name=a.name WHERE a.first_generation{scope}%(g)s',
    'multi_asset': 'SELECT id,policy,name FROM chain.assets WHERE first_generation{scope}%(g)s',
    'datum': 'SELECT id,hash,value FROM chain.datums WHERE first_generation{scope}%(g)s',
    'epoch_param': 'SELECT epoch AS epoch_no,nonce{sizes} FROM chain.epoch_nonces WHERE first_generation{scope}%(g)s',
    'pool_hash': 'SELECT p.id,p.pool_hash AS hash_raw FROM chain.pools p WHERE EXISTS(SELECT 1 FROM chain.epoch_stakes s'
                 ' WHERE s.pool_hash=p.pool_hash AND s.first_generation{scope}%(g)s)',
    'epoch_stake': 'SELECT s.id,p.id AS pool_id,s.amount,s.epoch AS epoch_no FROM chain.epoch_stakes s JOIN chain.pools p USING(pool_hash)'
                   ' WHERE s.first_generation{scope}%(g)s',
    'tx_metadata': 'SELECT ' + TX + ' AS id,6500973::public.word64type AS key,' + TX + ' AS tx_id,m.json FROM chain.tx_metadata m'
                   ' JOIN chain.transactions t ON t.hash=m.tx_hash JOIN chain.blocks b ON b.hash=t.block_hash'
                   ' WHERE m.present=1 AND m.first_generation{scope}%(g)s',
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
    return EXPECTED[table].format(scope='=' if only_new else '<=', sizes=served), {'g': generation}


def _insert(db, table, query, params):
    names = ','.join(columns(db, table))
    return db.execute('INSERT INTO serve.' + table + '(' + names + ') SELECT ' + names + ' FROM (' + query + ') x ORDER BY '
                      + KEYS[table], params).rowcount


def build(db, generation):
    """Fill the empty served tables for `generation` (the current projection), in chain order."""
    counts = {}
    for table in tables(db):
        query, params = expected(table, generation, db=db)
        counts[table] = _insert(db, table, query, params)
    _mark(db, generation)
    return counts


def advance(db, generation, *, cutoff, bindings, bindings_args):
    """Publication of `generation`: mirror the projection's header change and append the generation's facts.

    The projection drops the headers above `cutoff` and adds the rows of `bindings` (the
    generation's own header bindings); the served blocks change the same way. Facts are only
    ever added: every fact row first stored by `generation` gets its row here.
    """
    counts = {'block_removed': db.execute('DELETE FROM serve.block WHERE id>%s', (cutoff,)).rowcount}
    counts['block'] = _insert(db, 'block', BLOCKS.format(source=bindings), bindings_args)
    for table in tables(db)[1:]:
        query, params = expected(table, generation, only_new=True, db=db)
        if table == 'pool_hash':
            query = query + ' AND NOT EXISTS(SELECT 1 FROM serve.pool_hash x WHERE x.id=p.id)'
        counts[table] = _insert(db, table, query, params)
    _mark(db, generation)
    return counts


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
            mismatch = check(db, _difference(db, table, 'serve.' + table, query), params) is not None
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
    """A statement whose first row shows that `served` and the rows of `query` differ."""
    key, cols = KEYS[table], columns(db, table)
    left = 'ROW(' + ','.join('s.' + c for c in cols) + ')'
    right = 'ROW(' + ','.join('e.' + c for c in cols) + ')'
    return ('SELECT 1 FROM ' + served + ' s FULL JOIN (' + query + ') e ON e.' + key + '=s.' + key
            + ' WHERE s.' + key + ' IS NULL OR e.' + key + ' IS NULL OR ' + left + ' IS DISTINCT FROM ' + right + ' LIMIT 1')


def _differ(db, table, served, query, params):
    return db.execute(_difference(db, table, served, query), params).fetchone() is not None


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
