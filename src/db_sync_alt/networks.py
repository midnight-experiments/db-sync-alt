# SPDX-License-Identifier: Apache-2.0
"""Network profiles: every value that differs between Cardano networks.

Each packaged acquisition profile directory holds `network.json` next to the
Midnight configuration files. Code, database checks and the verifier read the
network's values from it: Cardano network magic and genesis parameters, the
security parameter k, the epoch configuration, the start cursor S, the Midnight
genesis and configuration digests, the Blockfrost host, key prefix and adapter
identity, the stake-address prefix, the db-sync `meta.network_name`, the node
query profile and the health threshold below the node's stability margin.

A profile copied elsewhere (`--profile-dir`) is accepted only when its
`network.json` equals the packaged one of the same network.
"""
from dataclasses import dataclass
from functools import lru_cache
import json
import math
import re

from db_sync_alt.contract import Refusal, canonical, digest

FORMAT = 'db-sync-alt-network-profile-v1'
FILE = 'network.json'

_HEX64 = re.compile('[0-9a-f]{64}')
_HEX40 = re.compile('[0-9a-f]{40}')
_KEYS = {
    '': {'format', 'network', 'profile', 'algorithm', 'cardano', 'midnight', 'blockfrost', 'node_queries',
         'health', 'provenance'},
    'cardano': {'network_magic', 'security_parameter', 'active_slots_coefficient', 'epoch_length',
                'system_start', 'epochs', 'stake_address_prefix', 'dbsync_network_name'},
    'cardano.epochs': {'first_epoch_number', 'first_slot_number', 'epoch_duration_millis',
                       'first_epoch_timestamp_millis', 'slot_duration_millis'},
    'midnight': {'genesis', 'chain_spec_raw_sha256', 'config_sha256', 'start', 'genesis_cardano_reference',
                 'block_stability_margin'},
    'midnight.start': {'block_number', 'tx_index_in_block'},
    'blockfrost': {'host', 'key_prefix', 'adapter', 'openapi'},
    'node_queries': {'tag', 'commit'},
    'health': {'serving_lag_max_blocks'},
}


def _invalid():
    raise Refusal('invalid_network_profile')


def _section(value, name):
    if not isinstance(value, dict) or set(value) != _KEYS[name]:
        _invalid()
    return value


def _integer(value, low, high=2**63 - 1):
    if type(value) is not int or not low <= value <= high:
        _invalid()
    return value


def _text(value, pattern):
    if not isinstance(value, str) or re.fullmatch(pattern, value) is None:
        _invalid()
    return value


def _validate(raw):
    _section(raw, '')
    if raw['format'] != FORMAT:
        _invalid()
    network = _text(raw['network'], '[a-z][a-z0-9]{0,31}')
    _text(raw['profile'], 'midnight-' + network + r'-node-[0-9]+(\.[0-9]+)*')
    _text(raw['algorithm'], network + '-selective-v[0-9]+')
    cardano = _section(raw['cardano'], 'cardano')
    _integer(cardano['network_magic'], 0, 2**32 - 1)
    _integer(cardano['security_parameter'], 1, 2**31 - 1)
    coefficient = cardano['active_slots_coefficient']
    if type(coefficient) is not float or not math.isfinite(coefficient) or not 0 < coefficient <= 1:
        _invalid()
    epochs = _section(cardano['epochs'], 'cardano.epochs')
    _integer(epochs['first_epoch_number'], 0)
    _integer(epochs['first_slot_number'], 0)
    _integer(epochs['first_epoch_timestamp_millis'], 0)
    _integer(epochs['epoch_duration_millis'], 1)
    _integer(epochs['slot_duration_millis'], 1)
    if (epochs['epoch_duration_millis'] % epochs['slot_duration_millis']
            or _integer(cardano['epoch_length'], 1) != epochs['epoch_duration_millis'] // epochs['slot_duration_millis']):
        _invalid()
    _integer(cardano['system_start'], 0)
    _text(cardano['stake_address_prefix'], 'stake(_test)?1')
    _text(cardano['dbsync_network_name'], '[a-z][a-z0-9_-]{0,31}')
    midnight = _section(raw['midnight'], 'midnight')
    for key in ('genesis', 'chain_spec_raw_sha256', 'config_sha256', 'genesis_cardano_reference'):
        _text(midnight[key], _HEX64.pattern)
    start = _section(midnight['start'], 'midnight.start')
    _integer(start['block_number'], 0, 2**31 - 1)
    _integer(start['tx_index_in_block'], 0, 2**31 - 1)
    margin = _integer(midnight['block_stability_margin'], 1, 2**31 - 1)
    blockfrost = _section(raw['blockfrost'], 'blockfrost')
    _text(blockfrost['host'], r'cardano-[a-z]+\.blockfrost\.io')
    _text(blockfrost['key_prefix'], '[a-z]+')
    _text(blockfrost['adapter'], 'blockfrost-' + network + '-cbor-v[0-9]+')
    _text(blockfrost['openapi'], _HEX40.pattern)
    queries = _section(raw['node_queries'], 'node_queries')
    _text(queries['tag'], r'node-[0-9]+(\.[0-9]+)*')
    _text(queries['commit'], _HEX40.pattern)
    health = _section(raw['health'], 'health')
    # The serving lag alarm must fire while the node can still verify blocks.
    _integer(health['serving_lag_max_blocks'], 1, margin - 1)
    if not isinstance(raw['provenance'], dict):
        _invalid()
    return raw


@dataclass(frozen=True, eq=False)
class NetworkProfile:
    """The validated values of one network; `raw` is the parsed `network.json`."""
    raw: dict

    def __eq__(self, other):
        return isinstance(other, NetworkProfile) and self.sha256 == other.sha256

    def __hash__(self):
        return hash(self.sha256)

    @property
    def sha256(self):
        return digest(self.raw)

    @property
    def network(self):
        return self.raw['network']

    @property
    def title(self):
        return self.network.capitalize()

    @property
    def profile(self):
        return self.raw['profile']

    @property
    def algorithm(self):
        return self.raw['algorithm']

    @property
    def magic(self):
        return self.raw['cardano']['network_magic']

    @property
    def security(self):
        return self.raw['cardano']['security_parameter']

    @property
    def stake_prefix(self):
        return self.raw['cardano']['stake_address_prefix']

    @property
    def dbsync_network_name(self):
        return self.raw['cardano']['dbsync_network_name']

    @property
    def genesis(self):
        return self.raw['midnight']['genesis']

    @property
    def config_sha256(self):
        return self.raw['midnight']['config_sha256']

    @property
    def start(self):
        start = self.raw['midnight']['start']
        return (start['block_number'], start['tx_index_in_block'])

    @property
    def stability_margin(self):
        return self.raw['midnight']['block_stability_margin']

    @property
    def serving_lag_max_blocks(self):
        return self.raw['health']['serving_lag_max_blocks']

    @property
    def blockfrost_host(self):
        return self.raw['blockfrost']['host']

    @property
    def blockfrost_base(self):
        return 'https://' + self.blockfrost_host + '/api/v0'

    @property
    def key_prefix(self):
        return self.raw['blockfrost']['key_prefix']

    @property
    def adapter(self):
        return self.raw['blockfrost']['adapter']

    @property
    def openapi(self):
        return self.raw['blockfrost']['openapi']

    @property
    def source_identity(self):
        """Persisted identity of the Blockfrost adapter for this network."""
        return {'adapter': self.adapter, 'openapi': self.openapi, 'network': self.network}

    @property
    def node_queries(self):
        return dict(self.raw['node_queries'])

    @property
    def contract(self):
        from db_sync_alt.contract import contract_for
        return contract_for(self)

    @property
    def legacy_contract(self):
        """The nonce-only contract of this network's stores written before epoch parameters."""
        from db_sync_alt.contract import contract_for
        return contract_for(self, legacy=True)

    def valid_key(self, key):
        """A Blockfrost project key of this network (prefix and shape only)."""
        return isinstance(key, str) and re.fullmatch(re.escape(self.key_prefix) + '[A-Za-z0-9]{13,}', key) is not None


def load(directory):
    """The validated `network.json` of a profile directory."""
    try:
        raw = json.loads((directory / FILE).read_text())
    except (OSError, ValueError, UnicodeError):
        raise Refusal('invalid_network_profile') from None
    return NetworkProfile(json.loads(canonical(_validate(raw))))


@lru_cache(maxsize=None)
def _packaged():
    from db_sync_alt.resources import data_dir
    found = {}
    for directory in sorted(data_dir('profiles').iterdir()):
        if not (directory / FILE).is_file():
            continue
        profile = load(directory)
        if profile.network in found:
            raise Refusal('duplicate_network_profile')
        found[profile.network] = (directory, profile)
    return found


def names():
    """The networks with a packaged profile, sorted."""
    return tuple(sorted(_packaged()))


def directory(network):
    """The packaged profile directory of `network`."""
    entry = _packaged().get(network if isinstance(network, str) else getattr(network, 'network', None))
    if entry is None:
        raise Refusal('unsupported_network')
    return entry[0]


def network_profile(value):
    """The packaged profile of a network name, or the packaged profile equal to `value`."""
    if isinstance(value, NetworkProfile):
        packaged = network_profile(value.network)
        if packaged != value:
            raise Refusal('untrusted_network_profile')
        return packaged
    entry = _packaged().get(value) if isinstance(value, str) else None
    if entry is None:
        raise Refusal('unsupported_network')
    return entry[1]


def for_contract(value, *, legacy=False):
    """The packaged network whose fact contract is `value` (a persisted contract); with `legacy`,
    whose nonce-only contract it is."""
    normalized = json.loads(canonical(value)) if value is not None else None
    for _, profile in _packaged().values():
        if json.loads(canonical(profile.legacy_contract if legacy else profile.contract)) == normalized:
            return profile
    raise Refusal('unsupported_format')


def for_identity(identity):
    """The packaged network a persisted profile identity belongs to, or None."""
    if not isinstance(identity, dict) or not isinstance(identity.get('network'), str):
        return None
    entry = _packaged().get(identity['network'])
    if entry is None:
        return None
    profile = entry[1]
    if (identity.get('genesis') != profile.genesis or identity.get('config_sha256') != profile.config_sha256
            or digest(identity.get('configs')) != profile.config_sha256):
        return None
    return profile
