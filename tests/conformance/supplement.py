"""Catalogue statements the corpus has no case for: a case for each, with the reason it is or is not
on the node's run path and what the endpoint must answer.

Expectations:
  rows             exact rows (after normalization and the case's column projection)
  error            the endpoint must refuse (fail closed)
  error_or_rows    refuse, or answer exactly these rows (never anything else, never empty instead)
  error_or_digest  refuse, or answer with exactly the digest of another, db-sync-verified answer
  error_or_complete  refuse, or complete without returning rows (a no-op)
  complete         complete without an error
  observed         off the node's run path: run and recorded, not judged; its reachability guard is
                   the named case(s), which must pass

Facts used for the reachability arguments (pinned reference sources, midnight-node 1.0.400):
the node depends on partner-chains-db-sync-data-sources with the features block-source,
candidate-source, mc-hash, sidechain-rpc and bridge (not governed-map, not block-participation),
and builds only the partner-chains block, mc-hash, sidechain-rpc and bridge data sources; its
candidate (authority selection) data source is midnight-node's own.
"""
import re

from db_sync_alt.resources import data_dir

from conformance.statements import by_name, by_text, placeholders

INDEX_NAMES = ('idx_ma_tx_out_ident', 'idx_tx_out_address', 'idx_ma_tx_out_id_ident', 'idx_multi_asset_policy_name',
               'idx_block_block_no', 'idx_tx_block_id', 'idx_tx_out_tx_id', 'idx_tx_out_data_hash', 'idx_tx_in_tx_in_id',
               'idx_tx_in_tx_out_id_tx_out_index', 'idx_multi_asset_policy_name_hex')
TOKEN_TRANSFER_METADATUM_KEY = 6500973

REACH = {
    'driver': 'On the run path: SQLx sends the SET statements when it opens a connection, resolves the OID of every type '
              'declared by name (INT4, INT8, INT2, NUMERIC, JSONB, bytea) with the type lookups, and pings with SELECT 1.',
    'from_connection': 'On the run path: decides TxInConfiguration once per node process and caches it for the process '
                       'lifetime (a wrong answer would select the consumed_by_tx_id queries until the node restarts).',
    'index_exists': 'On the run path at startup: each required index name must resolve to exactly one pg_indexes row, '
                    'otherwise the node issues CREATE INDEX.',
    'create_index_if_not_exists': 'The EXISTS check is on the run path; the CREATE INDEX CONCURRENTLY runs only when the '
                                  'check is false, which the EXISTS case proves it is not.',
    'create_idx_ma_tx_out_ident': 'Runs only when index_exists does not find exactly one idx_ma_tx_out_ident row; the '
                                  'index_exists cases prove it does. Running it anyway must not create anything.',
    'create_idx_tx_out_address': 'Runs only when index_exists does not find exactly one idx_tx_out_address row; the '
                                 'index_exists cases prove it does. Running it anyway must not create anything.',
    'create_idx_ma_tx_out_id_ident': 'Runs only when index_exists does not find exactly one idx_ma_tx_out_id_ident row; '
                                     'the index_exists cases prove it does. Running it anyway must not create anything.',
    'create_cnight_observation_indexes': 'Each CREATE INDEX runs only when index_exists does not find exactly one row for '
                                         'its name; the index_exists cases prove it does. Running them anyway must not '
                                         'create anything.',
    'get_utxos_for_address_tx_in_consumed': 'Off the run path: used only with TxInConfiguration::Consumed, which the node '
                                            'selects only when tx_in is missing or empty; the from_connection cases prove '
                                            'Enabled.',
    'get_bridge_txs': 'These catalogue entries are fragments: the node never sends them alone. It sends the bridge query '
                      'it formats at run time (WITH transfers AS (...), inputs AS (<fragment>) ...), which reads '
                      'tx_metadata. The bridge capability is not supported, so that query must fail closed.',
    'get_block_info_for_tx_hash': 'Bridge data source only (resolving a transaction checkpoint). The bridge capability is '
                                  'not supported: the endpoint may refuse, or answer exactly what db-sync answers.',
    'get_datums_at_address_with_token_tx_in_enabled': 'Not compiled into the node: partner-chains feature governed-map '
                                                      'is not enabled.',
    'get_datums_at_address_with_token_tx_in_consumed': 'Not compiled into the node: partner-chains feature governed-map '
                                                       'is not enabled.',
    'get_stake_pool_delegations_for_pools': 'Not compiled into the node: partner-chains feature block-participation is '
                                            'not enabled (block participation is also out of scope).',
    'get_token_utxo_for_epoch#partner-chains': 'Not called by the node: it belongs to the partner-chains candidate data '
                                               'source, which the node does not build (it uses its own). Same columns, '
                                               'filter and order as the node\'s variant: the endpoint may refuse, or answer '
                                               'exactly the db-sync answer of the node\'s variant for the same epoch.',
}


def bridge_query():
    """The bridge query exactly as the node formats it (block checkpoint, no row limit, tx_in enabled)."""
    source = (data_dir('reference') / 'partner-chains/toolkit/data-sources/db-sync/src/db_model.rs').read_text()
    transfers = re.search(r'let transfers_subquery = format!\(\s*"([\s\S]*?)"\s*\);', source).group(1)
    enabled = re.search(r'TxInConfiguration::Enabled => \{\s*"([\s\S]*?)"\s*\},', source[source.index('let inputs_subquery'):]).group(1)
    outer = re.search(r'QueryBuilder::new\(&format!\("([\s\S]*?)"\)\);', source).group(1)
    transfers = transfers.replace('{checkpoint_limit}', 'block.block_no > 4500196').replace('{max_rows}', 'null')
    return outer.replace('{transfers_subquery}', transfers).replace('{inputs_subquery}', enabled)


class Case:
    def __init__(self, label, statement, expect, *, sql=None, simple=False, types=None, values=(), project=None,
                 expected=None, guard=(), post=None):
        self.label, self.statement, self.expect = label, statement, expect
        self.sql, self.simple, self.types, self.values = sql, simple, types, list(values)
        self.project, self.expected, self.guard, self.post = project, expected, tuple(guard), post


def cases(profile, header, corpus_cases):
    """Supplementary cases bound to the corpus generation (target, epochs) and the profile."""
    target = header['target']
    epoch = min(c['case']['args'][1] for c in corpus_cases if c['case']['name'] == 'get_token_utxo_for_epoch')
    node_variant = next(c for c in corpus_cases if c['case']['name'] == 'get_token_utxo_for_epoch' and c['case']['args'][1] == epoch)
    stake_epoch = min(c['case']['args'][0] for c in corpus_cases if c['case']['name'] == 'get_stake_distribution')
    permissioned = bytes.fromhex(profile.permissioned)
    driver = lambda sql: by_text(sql)
    out = [
        Case('driver:set-two', driver("SET extra_float_digits = 3; SET application_name = 'sqlx'"), 'complete', simple=True),
        Case('driver:set-one', driver('SET extra_float_digits = 3'), 'complete', simple=True),
        Case('driver:ping-simple', driver('SELECT 1'), ('rows', [{'?column?': 1}]), simple=True),
        Case('driver:ping-extended', driver('SELECT 1'), ('rows', [{'?column?': 1}])),
    ]
    for name, oid in (('INT4', 23), ('INT8', 20), ('INT2', 21), ('NUMERIC', 1700), ('JSONB', 3802), ('bytea', 17)):
        out.append(Case('driver:regtype-' + name, driver('SELECT $1::regtype::oid'), ('rows', [{'oid': oid}]), values=[name]))
    out.append(Case('driver:typname-int4', driver('SELECT oid FROM pg_catalog.pg_type WHERE typname = $1'), ('rows', [{'oid': 23}]), values=['int4']))
    out.append(Case('driver:type-23', driver('SELECT typname, typtype, typelem, typrelid, typbasetype FROM pg_catalog.pg_type WHERE oid = $1'),
                    ('rows', [{'typname': 'int4', 'typtype': 'b', 'typelem': 0, 'typrelid': 0, 'typbasetype': 0}]), values=[23]))
    count_sql, exists_sql = sorted((e for e in _all('from_connection')), key=lambda e: e['sql'].startswith('SELECT'))
    out.append(Case('from_connection:tx_in-relation', count_sql, ('rows', [{'count': 1}]), sql=count_sql['sql'] + ';'))
    out.append(Case('from_connection:tx_in-populated', exists_sql, ('rows', [{'exists': True}]), sql=exists_sql['sql'] + ';'))
    index_exists = by_name('index_exists')
    for name in INDEX_NAMES:
        out.append(Case('index_exists:' + name, index_exists, ('rows', [{'indexname': name}]), values=[name], project=['indexname']))
    check, create_hex = sorted(_all('create_index_if_not_exists'), key=lambda e: not e['sql'].upper().startswith('SELECT'))
    out.append(Case('create_index_if_not_exists:exists-check', check, ('rows', [{'exists': True}])))
    out.append(Case('create_index_if_not_exists:create', create_hex, 'error_or_complete',
                    guard=['create_index_if_not_exists:exists-check'], post='idx_multi_asset_policy_name_hex'))
    for group in ('create_idx_ma_tx_out_ident', 'create_idx_tx_out_address', 'create_idx_ma_tx_out_id_ident', 'create_cnight_observation_indexes'):
        for entry in sorted(_all(group), key=lambda e: e['sql']):
            index = re.search(r'IF NOT EXISTS (\w+)', entry['sql']).group(1)
            out.append(Case(group + ':' + index, entry, 'error_or_complete', guard=['index_exists:' + index], post=index))
    out.append(Case('get_utxos_for_address_tx_in_consumed', by_name('get_utxos_for_address_tx_in_consumed'), 'observed',
                    values=[profile.candidate, target], guard=['from_connection:tx_in-relation', 'from_connection:tx_in-populated']))
    out.append(Case('get_bridge_txs:formatted-query', None, 'error', sql=bridge_query(),
                    types=['text', 'bytea', 'bytea', 'int4', 'int8'],
                    values=[profile.mapping, permissioned, b'', target, TOKEN_TRANSFER_METADATUM_KEY]))
    for entry in sorted(_all('get_bridge_txs'), key=lambda e: e['sql']):
        out.append(Case('get_bridge_txs:fragment-' + ('consumed' if 'consumed_by_tx_id' in entry['sql'] else 'enabled'), entry, 'observed',
                        values=[profile.mapping], guard=['get_bridge_txs:formatted-query']))
    out.append(Case('get_block_info_for_tx_hash', by_name('get_block_info_for_tx_hash'), 'error_or_rows', expected='verified-asset-create'))
    for name in ('get_datums_at_address_with_token_tx_in_enabled', 'get_datums_at_address_with_token_tx_in_consumed'):
        out.append(Case(name, by_name(name), 'observed', values=[profile.candidate, target, permissioned, b'']))
    out.append(Case('get_stake_pool_delegations_for_pools', by_name('get_stake_pool_delegations_for_pools'), 'observed',
                    values=[stake_epoch, [bytes(28)]]))
    out.append(Case('get_token_utxo_for_epoch#partner-chains', by_name('get_token_utxo_for_epoch', parameters=3), 'error_or_digest',
                    values=[permissioned, b'', epoch], expected=node_variant['expected']))
    return out


def _all(name):
    from db_sync_alt.node_queries import CAT
    return [e for e in CAT.values() if e['name'] == name]


def reach(case):
    key = case.label.split(':')[0]
    return REACH[key]


def statement_types(case):
    from conformance.statements import parameter_types
    if case.types is not None:
        return case.types
    types = parameter_types(case.statement)
    if len(types) != placeholders(case.sql or case.statement['sql']):
        raise ValueError('parameter count')
    return types
