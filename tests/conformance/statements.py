"""Parameter types of every catalogue statement, as midnight-node binds them through SQLx.

Read from the pinned reference sources: `as i32` and the db-sync-sqlx wrappers (BlockNumber,
EpochNumber, TxIndexInBlock: INT4; SlotNumber: INT8) bind INT4/INT8, `&str`/`String` bind TEXT,
byte arrays and slices bind BYTEA, `NaiveDateTime` binds TIMESTAMP, `Vec<[u8; 28]>` binds BYTEA[].
SQLx's own driver lookups bind TEXT names and OID values.
"""
import hashlib
import re

from db_sync_alt.node_queries import CAT, norm

EVENTS = ('get_asset_creates', 'get_asset_spends', 'get_registrations', 'get_deregistrations')
HELPERS = ('get_low_bounds', 'get_high_bounds', 'resolve_ident')
I4, I8 = ['int4'], ['int8']

PARAMETERS = {
    'get_registrations': ['text', 'int8'] + I4 * 6 + I8 * 6,
    'get_deregistrations': ['text'] + I4 * 6 + I8 * 4,
    'get_asset_creates': I8 + I4 * 6 + I8 * 6,
    'get_asset_spends': I8 + I4 * 6 + I8 * 4,
    'get_low_bounds': I4,
    'get_high_bounds': I4,
    'resolve_ident': ['bytea', 'bytea'],
    'get_block_by_hash': ['bytea'],
    'get_governance_body_utxo': ['text', 'bytea', 'int4'],
    'index_exists': ['text'],
    'get_latest_block_for_epoch': I4,
    'get_latest_stable_epoch': I4,
    'get_stake_distribution': I4,
    'get_epoch_nonce': I4,
    'get_epoch_for_block_hash': ['bytea'],
    'get_utxos_for_address_tx_in_enabled': ['text', 'int4'],
    'get_utxos_for_address_tx_in_consumed': ['text', 'int4'],
    'get_blocks_by_numbers': I4 * 2,
    'get_highest_block': ['int4', 'int8', 'int8', 'timestamp', 'timestamp'],
    'get_latest_block_info': [],
    'get_block_info_for_tx_hash': ['bytea'],
    'get_bridge_txs': ['text'],
    'get_datums_at_address_with_token_tx_in_enabled': ['text', 'int4', 'bytea', 'bytea'],
    'get_datums_at_address_with_token_tx_in_consumed': ['text', 'int4', 'bytea', 'bytea'],
    'get_stake_pool_delegations_for_pools': ['int4', 'bytea[]'],
    'from_connection': [],
    'create_index_if_not_exists': [],
    'create_idx_ma_tx_out_ident': [],
    'create_idx_tx_out_address': [],
    'create_idx_ma_tx_out_id_ident': [],
    'create_cnight_observation_indexes': [],
}
# Same name, different statements: told apart by their parameter count.
VARIANTS = {'get_token_utxo_for_epoch': {2: ['int8', 'int4'], 3: ['bytea', 'bytea', 'int4']}}
DRIVER = {
    'SELECT $1::regtype::oid': ['text'],
    'SELECT oid FROM pg_catalog.pg_type WHERE typname = $1': ['text'],
    'SELECT typname, typtype, typelem, typrelid, typbasetype FROM pg_catalog.pg_type WHERE oid = $1': ['oid'],
}
# SQLx sends these through the simple query protocol (connection setup and ping).
SIMPLE = ("SET extra_float_digits = 3; SET application_name = 'sqlx'", 'SET extra_float_digits = 3', 'SELECT 1')


def sha(sql):
    return hashlib.sha256(sql.encode()).hexdigest()


def placeholders(sql):
    return max((int(n) for n in re.findall(r'\$(\d+)', sql)), default=0)


def parameter_types(entry):
    name, sql = entry['name'], entry['sql']
    if name == 'driver':
        types = DRIVER.get(sql, [])
    elif name in VARIANTS:
        types = VARIANTS[name][placeholders(sql)]
    else:
        types = PARAMETERS[name]
    if len(types) != placeholders(sql):
        raise ValueError('parameter count of ' + name)
    return list(types)


def statements():
    """Every catalogue statement: fingerprint -> entry (name, sql, sources)."""
    return {sha(entry['sql']): entry for entry in CAT.values()}


def by_name(name, *, part=None, parameters=None):
    found = [e for e in CAT.values() if e['name'] == name
             and (part is None or any(part in s['file'] for s in e.get('sources', [e])))
             and (parameters is None or placeholders(e['sql']) == parameters)]
    if len(found) != 1:
        raise ValueError('ambiguous statement ' + name)
    return found[0]


def by_text(sql):
    return CAT[norm(sql)]
