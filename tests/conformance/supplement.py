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
  component        exact non-emitted builder literal: inventory only, linked to the final query
                   refusal cases; never sent to PostgreSQL as an incomplete statement

The catalogue and the dynamic bridge builder come from the pinned midnight-node 2.1.101 reference.
Literal presence does not prove the features or runtime paths of a native artifact.
The bridge query (six binds, tx_in mode) must execute through the reader: tx_metadata is served from
the metadata coverage start on. The corpus holds no bridge answer, so its rows are compared with a
cardano-db-sync-shaped reference in the integration tests (tests/integration/test_bridge_metadata.py);
whether a checkpoint lies in coverage is the operator's status report, which a reader cannot see.
"""
import hashlib
import re

from db_sync_alt.resources import data_dir

from conformance.statements import BRIDGE_SOURCE, bridge_fragment, by_name, by_text, placeholders, sha

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
    'get_bridge_txs': 'On the run path once governance sets the bridge checkpoint: the six catalogue entries are '
                      'non-emitted bridge/reserve input/output components the pinned builder assembles into a six-bind '
                      'query reading tx_metadata. The tx_in-mode query must execute through the reader; the consumed-mode '
                      'query is off the run path (the from_connection cases prove tx_in mode). Components are inventoried '
                      'without execution and linked to the final queries.',
    'get_block_info_for_tx_hash': 'Bridge data source only (resolving a transaction checkpoint). The bridge capability is '
                                  'not supported: the endpoint may refuse, or answer exactly what db-sync answers.',
    'get_token_utxo_for_epoch#partner-chains': 'Not called by the node: it belongs to the partner-chains candidate data '
                                               'source, which the node does not build (it uses its own). Same columns, '
                                               'filter and order as the node\'s variant: the endpoint may refuse, or answer '
                                               'exactly the db-sync answer of the node\'s variant for the same epoch.',
}


def bridge_query(configuration='Enabled', checkpoint=4500197):
    """Exact released builder, block checkpoint/no row limit, in either tx_in mode: the query the node's
    cached bridge source sends for checkpoint `Block(checkpoint)` (it reads the blocks from that one on)."""
    source = (data_dir('reference') / BRIDGE_SOURCE).read_bytes()
    if hashlib.sha256(source).hexdigest() != 'a016a1a87356905083c5f817fd8d4ac1b79e639ddd315b792689c51b223a813d':
        raise ValueError('unclassified bridge builder source')
    if configuration not in ('Enabled', 'Consumed'):
        raise ValueError('unknown bridge tx_in configuration')
    body = source.decode().split('pub(crate) async fn get_bridge_txs(', 1)[1]
    if type(checkpoint) is not int or not 0 < checkpoint < 2**31:
        raise ValueError('bridge checkpoint block')
    substitutions = {'checkpoint_limit': 'block.block_no > ' + str(checkpoint - 1), 'max_rows': 'null'}
    for name in ('bridge_inputs_subquery', 'reserve_inputs_subquery'):
        block = re.search(r'let ' + name + r' = match tx_in_configuration \{([\s\S]*?)\n\t\};', body).group(1)
        substitutions[name] = re.search(r'TxInConfiguration::' + configuration + r' => \{\s*"([\s\S]*?)"', block).group(1)
    for name in ('bridge_outputs_subquery', 'reserve_outputs_subquery'):
        substitutions[name] = re.search(r'let ' + name + r' = "([\s\S]*?)";', body).group(1)
    query = re.search(r'let mut query_builder = QueryBuilder::new\(&format!\(\s*"([\s\S]*?)"\s*\)\);', body).group(1)
    for name, value in substitutions.items():
        query = query.replace('{' + name + '}', value)
    if '{' in query or '}' in query or placeholders(query) != 6:
        raise ValueError('incomplete bridge builder reconstruction')
    return query


def bridge_values(profile, target):
    """Public packaged ICS/reserve/native-token values in the released bind order."""
    ics, reserve = profile.configs['ics-config'], profile.configs['reserve-config']
    if ics['asset'] != reserve['asset']:
        raise ValueError('bridge native-token configuration mismatch')
    asset = ics['asset']
    return [ics['illiquid_circulation_supply_validator_address'], reserve['reserve_validator_address'],
            bytes.fromhex(asset['policy_id'].removeprefix('0x')), asset['asset_name'].encode(),
            TOKEN_TRANSFER_METADATUM_KEY, target]


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
    queries = {mode: bridge_query(mode) for mode in ('Enabled', 'Consumed')}
    for mode, query in queries.items():
        expect, guard = ('complete', ()) if mode == 'Enabled' else ('observed', ('from_connection:tx_in-relation', 'from_connection:tx_in-populated'))
        out.append(Case('get_bridge_txs:formatted-' + mode, None, expect, sql=query, guard=guard,
                        types=['text', 'text', 'bytea', 'bytea', 'int8', 'int4'], values=bridge_values(profile, target)))
    for entry in sorted(_all('get_bridge_txs'), key=lambda e: sha(e['sql'])):
        if not bridge_fragment(entry):
            raise ValueError('unclassified bridge component')
        normalized = ' '.join(entry['sql'].split())
        guards = ['get_bridge_txs:formatted-' + mode for mode, query in queries.items()
                  if normalized in ' '.join(query.split())]
        if not guards:
            raise ValueError('bridge component absent from assembled queries')
        out.append(Case('get_bridge_txs:component-' + sha(entry['sql'])[:12], entry, 'component', guard=guards))
    out.append(Case('get_block_info_for_tx_hash', by_name('get_block_info_for_tx_hash'), 'error_or_rows', expected='verified-asset-create'))
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
