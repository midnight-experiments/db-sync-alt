"""Supported read-only source schemas, separate from the persisted fact contract.

Upstream catalog evidence: cardano-db-sync 13.7.1.0 ef00c765 and 13.8.0.0 ccb9c0f4.
Only the core full-history mode is supported. meta.version is the database-creating
version and is never used as the current schema/version identity.
"""
from db_sync_alt.contract import Refusal

SOURCE_VERSIONS = {(15, 48, 6): '13.7.1.0', (16, 52, 6): '13.8.0.0'}
# (underlying PostgreSQL type, domain, is_nullable), identical in both supported catalogs.
SOURCE_COLUMNS = {'schema_version': {'stage_one': ('int8', None, 'NO'),
                    'stage_two': ('int8', None, 'NO'),
                    'stage_three': ('int8', None, 'NO')},
 'meta': {'network_name': ('varchar', None, 'NO')},
 'extra_migrations': {'token': ('varchar', None, 'NO')},
 'block': {'id': ('int8', None, 'NO'),
           'hash': ('bytea', 'hash32type', 'NO'),
           'block_no': ('int4', 'word31type', 'YES'),
           'previous_id': ('int8', None, 'YES'),
           'epoch_no': ('int4', 'word31type', 'YES'),
           'slot_no': ('int8', 'word63type', 'YES'),
           'time': ('timestamp', None, 'NO'),
           'tx_count': ('int8', None, 'NO')},
 'tx': {'id': ('int8', None, 'NO'),
        'hash': ('bytea', 'hash32type', 'NO'),
        'block_id': ('int8', None, 'NO'),
        'block_index': ('int4', 'word31type', 'NO'),
        'valid_contract': ('bool', None, 'NO')},
 'tx_out': {'id': ('int8', None, 'NO'),
            'tx_id': ('int8', None, 'NO'),
            'index': ('int2', 'txindex', 'NO'),
            'address': ('varchar', None, 'NO'),
            'value': ('numeric', 'lovelace', 'NO'),
            'data_hash': ('bytea', 'hash32type', 'YES'),
            'inline_datum_id': ('int8', None, 'YES')},
 'tx_in': {'id': ('int8', None, 'NO'),
           'tx_in_id': ('int8', None, 'NO'),
           'tx_out_id': ('int8', None, 'NO'),
           'tx_out_index': ('int2', 'txindex', 'NO')},
 'datum': {'id': ('int8', None, 'NO'), 'hash': ('bytea', 'hash32type', 'NO'), 'value': ('jsonb', None, 'YES')},
 'epoch_stake': {'epoch_no': ('int4', 'word31type', 'NO'),
                 'pool_id': ('int8', None, 'NO'),
                 'amount': ('numeric', 'lovelace', 'NO')},
 'epoch_param': {'epoch_no': ('int4', 'word31type', 'NO'), 'nonce': ('bytea', 'hash32type', 'YES'),
                 'max_tx_size': ('int4', 'word31type', 'NO')},
 'epoch_stake_progress': {'epoch_no': ('int4', 'word31type', 'NO'), 'completed': ('bool', None, 'NO')},
 'ma_tx_out': {'id': ('int8', None, 'NO'),
               'tx_out_id': ('int8', None, 'NO'),
               'ident': ('int8', None, 'NO'),
               'quantity': ('numeric', 'word64type', 'NO')},
 'ma_tx_mint': {'tx_id': ('int8', None, 'NO'),
                'ident': ('int8', None, 'NO'),
                'quantity': ('numeric', 'int65type', 'NO')},
 'multi_asset': {'id': ('int8', None, 'NO'),
                 'policy': ('bytea', 'hash28type', 'NO'),
                 'name': ('bytea', 'asset32type', 'NO')},
 'pool_hash': {'id': ('int8', None, 'NO'), 'hash_raw': ('bytea', 'hash28type', 'NO')}}

# All upstream ExtraMigration constructors are inspected. Only this completion token is benign.
ALLOWED_MODES = frozenset({'StakeDistrEnded'})

def source_version(schema):
    version = SOURCE_VERSIONS.get(tuple(schema))
    if version is None:raise Refusal('unsupported_source_schema')
    return version

def validate_columns(rows):
    found = {(r['table_name'],r['column_name']):r for r in rows}
    for table,columns in SOURCE_COLUMNS.items():
        for name,expected in columns.items():
            row = found.get((table,name))
            if row is None:raise Refusal('source_columns_missing')
            if row['relkind'] != 'r' or (row['udt_name'],row['domain_name'],row['is_nullable']) != expected:
                raise Refusal('unsupported_source_column_type')

def validate_modes(tokens):
    if any(token not in ALLOWED_MODES for token in tokens):raise Refusal('unsupported_source_mode')

def validate_attestation(attestation,version):
    a = attestation
    if not isinstance(a,dict) or a.get('dbsync_version') != version:
        raise Refusal('source_version_attestation_mismatch')
    if any(a.get(k) is not True for k in ('tx_in_retained','epoch_stake_retained')):
        raise Refusal('retention_attestation_required')
    if not (a.get('operator_attested') is True or a.get('retention_evidence') == 'observed-launch-and-indexed-history'):
        raise Refusal('retention_evidence_required')
    if set(a)-{'dbsync_version','tx_in_retained','epoch_stake_retained','operator_attested','retention_evidence'}:
        raise Refusal('unknown_attestation_fields')
