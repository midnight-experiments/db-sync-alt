# SPDX-License-Identifier: Apache-2.0
"""Network profiles: the packaged `network.json` format, its checks and its use by the acquisition profile."""
import copy
import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import unittest

from fixtures.service import READER

from db_sync_alt import networks
from db_sync_alt.contract import TABLES, Profile, Refusal, digest
from db_sync_alt.resources import data_dir, profile_dir


def raw(network):
    return copy.deepcopy(networks.network_profile(network).raw)


def write_profile(target, value):
    (target / networks.FILE).write_text(json.dumps(value, indent=2) + '\n')


class FormatTests(unittest.TestCase):
    def test_every_packaged_profile_is_valid_and_unique(self):
        names = networks.names()
        self.assertIn('preprod', names)
        for name in names:
            with self.subTest(network=name):
                profile = networks.network_profile(name)
                directory = profile_dir(name)
                self.assertEqual(networks.load(directory), profile)
                self.assertEqual(profile.network, name)
                self.assertEqual(json.loads((directory / 'profile.json').read_text())['name'], profile.profile)
                self.assertLess(profile.serving_lag_max_blocks, profile.stability_margin)
                self.assertEqual(profile.blockfrost_base, 'https://' + profile.blockfrost_host + '/api/v0')
                self.assertEqual(profile.source_identity, {'adapter': profile.adapter, 'openapi': profile.openapi, 'network': name})
                self.assertEqual(profile.contract['security'], profile.security)
                self.assertEqual(profile.contract['algorithm'], profile.algorithm)
                self.assertIs(networks.for_contract(json.loads(json.dumps(profile.contract))), profile)

    def test_preprod_values(self):
        p = networks.network_profile('preprod')
        self.assertEqual((p.magic, p.security, p.start, p.stake_prefix, p.dbsync_network_name),
                         (1, 2160, (4500197, 4), 'stake_test1', 'preprod'))
        self.assertEqual((p.blockfrost_host, p.key_prefix, p.adapter),
                         ('cardano-preprod.blockfrost.io', 'preprod', 'blockfrost-preprod-cbor-v1'))
        self.assertEqual((p.genesis, p.config_sha256),
                         ('df831b09a8baa92badf47762ce5ac439b7e47e3ed3d39600cfdd44fad552361b',
                          '88a096f98b01f55c0680eb523f58fa9c4c141f3546794e4f5fd1084a76e32304'))
        self.assertEqual((p.profile, p.algorithm), ('midnight-preprod-node-1.0.400', 'preprod-selective-v1'))
        self.assertEqual((p.stability_margin, p.serving_lag_max_blocks), (10, 9))
        self.assertEqual(p.raw['cardano']['system_start'], 1654041600)
        self.assertEqual(p.raw['midnight']['chain_spec_raw_sha256'], 'fe96d5070ab4b624f114087482a373c3c210310449c1253bcc0261822f941560')

    def test_profile_files_and_query_reference_match_the_network_values(self):
        reference = json.loads((data_dir('reference') / 'source-manifest.json').read_text())
        for name in networks.names():
            with self.subTest(network=name):
                p = networks.network_profile(name)
                directory = profile_dir(name)
                manifest = json.loads((directory / 'source-manifest.json').read_text())
                self.assertEqual(manifest['commit'], p.raw['provenance']['midnight_configs']['commit'])
                for entry in manifest['files'].values():
                    self.assertEqual(hashlib.sha256((directory / Path(entry['source']).name).read_bytes()).hexdigest(), entry['sha256'])
                self.assertEqual(p.node_queries, {'tag': reference['tag'], 'commit': reference['commit']})
                profile = Profile(directory)
                self.assertEqual(digest(profile.configs), p.config_sha256)
                self.assertEqual(profile.start, p.start)
                self.assertEqual(profile.identity['genesis'], p.genesis)
                self.assertIs(profile.network, p)
                self.assertEqual(profile.security, p.security)

    def test_malformed_profiles_are_refused(self):
        def changed(edit):
            value = raw('preprod')
            edit(value)
            return value
        cases = {
            'format': lambda v: v.update(format='db-sync-alt-network-profile-v2'),
            'missing': lambda v: v['cardano'].pop('stake_address_prefix'),
            'extra': lambda v: v['midnight'].update(extra=1),
            'k_zero': lambda v: v['cardano'].update(security_parameter=0),
            'k_bool': lambda v: v['cardano'].update(security_parameter=True),
            'k_text': lambda v: v['cardano'].update(security_parameter='2160'),
            'magic_negative': lambda v: v['cardano'].update(network_magic=-1),
            'coefficient': lambda v: v['cardano'].update(active_slots_coefficient=0),
            'epoch_length': lambda v: v['cardano'].update(epoch_length=86400),
            'start_negative': lambda v: v['midnight']['start'].update(block_number=-1),
            'genesis': lambda v: v['midnight'].update(genesis='DF' * 32),
            'lag_at_margin': lambda v: v['health'].update(serving_lag_max_blocks=10),
            'lag_above_margin': lambda v: v['health'].update(serving_lag_max_blocks=15),
            'host': lambda v: v['blockfrost'].update(host='cardano-preprod.example.com'),
            'adapter_network': lambda v: v['blockfrost'].update(adapter='blockfrost-preview-cbor-v1'),
            'algorithm_network': lambda v: v.update(algorithm='preview-selective-v1'),
            'profile_network': lambda v: v.update(profile='midnight-preview-node-1.0.400'),
            'stake_prefix': lambda v: v['cardano'].update(stake_address_prefix='addr_test1'),
            'provenance': lambda v: v.update(provenance=[]),
        }
        with tempfile.TemporaryDirectory() as tmp:
            for name, edit in cases.items():
                with self.subTest(case=name):
                    write_profile(Path(tmp), changed(edit))
                    with self.assertRaisesRegex(Refusal, 'invalid_network_profile'):
                        networks.load(Path(tmp))
            (Path(tmp) / networks.FILE).write_text('{')
            with self.assertRaisesRegex(Refusal, 'invalid_network_profile'):
                networks.load(Path(tmp))

    def test_copied_profile_must_equal_the_packaged_profile(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / 'profile'
            shutil.copytree(profile_dir('preprod'), target)
            self.assertEqual(Profile(target).identity, Profile(profile_dir('preprod')).identity)
            self.assertEqual(Profile(target, 'preprod').network, networks.network_profile('preprod'))
            value = raw('preprod')
            value['cardano']['security_parameter'] = 432
            write_profile(target, value)
            with self.assertRaisesRegex(Refusal, 'untrusted_network_profile'):
                Profile(target)
            value = raw('preprod')
            value['health']['serving_lag_max_blocks'] = 5
            write_profile(target, value)
            with self.assertRaisesRegex(Refusal, 'untrusted_network_profile'):
                Profile(target)
            (target / networks.FILE).unlink()
            with self.assertRaisesRegex(Refusal, 'invalid_network_profile'):
                Profile(target)

    def test_unknown_networks_and_identities(self):
        for value in ('testnet', '', None, 'PREPROD'):
            with self.subTest(value=value), self.assertRaisesRegex(Refusal, 'unsupported_network'):
                networks.network_profile(value)
        with self.assertRaisesRegex(Refusal, 'unsupported_network'):
            profile_dir('testnet')
        contract = json.loads(json.dumps(networks.network_profile('preprod').contract))
        for key, value in (('security', 432), ('algorithm', 'preview-selective-v1'), ('schema_version', [16, 52, 6])):
            with self.subTest(contract=key), self.assertRaisesRegex(Refusal, 'unsupported_format'):
                networks.for_contract(dict(contract, **{key: value}))
        with self.assertRaisesRegex(Refusal, 'unsupported_format'):
            networks.for_contract(None)
        identity = Profile(profile_dir('preprod')).identity
        self.assertEqual(networks.for_identity(identity), networks.network_profile('preprod'))
        for key, value in (('network', 'testnet'), ('genesis', '0' * 64), ('config_sha256', '0' * 64)):
            with self.subTest(key=key):
                self.assertIsNone(networks.for_identity(dict(identity, **{key: value})))
        changed = copy.deepcopy(identity)
        changed['configs']['registered-candidates-addresses']['committee_candidates_address'] += 'x'
        self.assertIsNone(networks.for_identity(changed))
        self.assertIsNone(networks.for_identity(None))


if __name__ == '__main__':
    unittest.main()


class ConfigurationTests(unittest.TestCase):
    """Configuration and job checks take the network's values from its profile."""

    def test_service_configuration_resolves_the_network_profile(self):
        from db_sync_alt.service.health import HealthPolicy
        from db_sync_alt.service.runtime import ServiceConfig
        preprod = networks.network_profile('preprod')
        config = ServiceConfig('test', READER, network='preprod')
        self.assertEqual(config.profile, preprod.profile)
        self.assertEqual(config.policy.serving_lag_max_blocks, preprod.serving_lag_max_blocks)
        accepted = ServiceConfig('test', READER, network='preprod', policy=HealthPolicy(serving_lag_max_blocks=preprod.stability_margin - 1))
        self.assertEqual(accepted.policy.serving_lag_max_blocks, preprod.stability_margin - 1)
        for limit in (preprod.stability_margin, 15):
            with self.subTest(limit=limit), self.assertRaisesRegex(ValueError, 'serving_lag_above_node_margin'):
                ServiceConfig('test', READER, network='preprod', policy=HealthPolicy(serving_lag_max_blocks=limit))
        with self.assertRaisesRegex(ValueError, 'network_required'):
            ServiceConfig('test', READER)
        with self.assertRaisesRegex(ValueError, 'unsupported_configuration_profile'):
            ServiceConfig('test', READER, network='testnet')
        with self.assertRaisesRegex(ValueError, 'unsupported_configuration_profile'):
            ServiceConfig('test', READER, network='preprod', profile='midnight-preprod-node-1.0.0')

    def test_acquire_and_run_require_a_packaged_network(self):
        import contextlib
        import io
        from db_sync_alt.cli import acquire, run
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                acquire.parser().parse_args(['--source', 'db-sync', '--destination-service', 'x'])
            with self.assertRaises(SystemExit):
                acquire.parser().parse_args(['--source', 'db-sync', '--network', 'testnet', '--destination-service', 'x'])
        args = acquire.parser().parse_args(['--source', 'db-sync', '--network', 'preprod', '--destination-service', 'x'])
        self.assertEqual((args.network, args.profile_dir), ('preprod', None))
        with self.assertRaises(run.LaunchError):
            run.parser().parse_args(['--network', 'testnet'])
        self.assertEqual(run.parser().parse_args(['--network', 'preprod']).network, 'preprod')

    def test_acquisition_job_witness_is_target_plus_the_network_k(self):
        from db_sync_alt.errors import LifecycleFailure
        from db_sync_alt.sources.blockfrost.sync import validate_job
        k = networks.network_profile('preprod').security
        job = {'job_id': 'job', 'attempts': 0, 'budget': 1, 'state': 'active', 'kind': 'acquire',
               'parent': 0, 'target': 100, 'witness': 100 + k}
        self.assertIs(validate_job(job, security=k), job)
        for security in (None, k + 1, str(k)):
            with self.subTest(security=security), self.assertRaisesRegex(LifecycleFailure, 'invalid_sync_job'):
                validate_job(job, security=security)
        poll = {'job_id': 'poll', 'attempts': 0, 'budget': 1, 'state': 'active', 'kind': 'poll'}
        self.assertIs(validate_job(poll), poll)


class NetworkValueTests(unittest.TestCase):
    """Mainnet and Preview profiles, derived from midnight-node node-1.0.0 `res/<network>`."""

    def test_every_network_has_a_profile(self):
        self.assertEqual(networks.names(), ('mainnet', 'preprod', 'preview'))

    def test_mainnet_values(self):
        p = networks.network_profile('mainnet')
        self.assertEqual((p.magic, p.security, p.start, p.stake_prefix, p.dbsync_network_name),
                         (764824073, 2160, (13164005, 1), 'stake1', 'mainnet'))
        self.assertEqual((p.blockfrost_host, p.key_prefix, p.adapter, p.algorithm, p.profile),
                         ('cardano-mainnet.blockfrost.io', 'mainnet', 'blockfrost-mainnet-cbor-v1', 'mainnet-selective-v1',
                          'midnight-mainnet-node-1.0.0'))
        self.assertEqual(p.genesis, '1941ca8e2bb88146c14dea084d3be7eb6e96ca7135429c543848b628124f2854')
        self.assertEqual(p.raw['midnight']['chain_spec_raw_sha256'], 'bd81ad4571575f29309d8a10aefb731b6ea6dc3f6b9df7d2d3e3aca94df45e81')
        self.assertEqual((p.raw['cardano']['system_start'], p.raw['cardano']['epoch_length'], p.raw['cardano']['epochs']['first_epoch_number']),
                         (1506203091, 432000, 208))
        self.assertEqual(Profile(profile_dir('mainnet')).night, '0691b2fecca1ac4f53cb6dfb00b7013e561d1f34403b957cbb5af1fa' + b'NIGHT'.hex())

    def test_preview_values(self):
        p = networks.network_profile('preview')
        self.assertEqual((p.magic, p.security, p.start, p.stake_prefix, p.dbsync_network_name),
                         (2, 432, (0, 0), 'stake_test1', 'preview'))
        self.assertEqual((p.blockfrost_host, p.key_prefix, p.adapter, p.algorithm, p.profile),
                         ('cardano-preview.blockfrost.io', 'preview', 'blockfrost-preview-cbor-v1', 'preview-selective-v1',
                          'midnight-preview-node-1.0.0'))
        self.assertEqual(p.genesis, '801d3fc306115a3b538ea9498881c176376f8e3213464fe620fc1f359d13b880')
        self.assertEqual((p.raw['cardano']['system_start'], p.raw['cardano']['epoch_length']), (1666656000, 86400))
        profile = Profile(profile_dir('preview'))
        self.assertEqual(profile.start, (0, 0))
        self.assertIn('d2dbff622e509dda256fedbd31ef6e9fd98ed49ad91d5c0e07f68af1', profile.units)

    def test_contracts_differ_only_in_algorithm_and_k(self):
        contracts = {name: networks.network_profile(name).contract for name in networks.names()}
        self.assertEqual(len({digest(c) for c in contracts.values()}), 3)
        for name, value in contracts.items():
            rest = {k: v for k, v in value.items() if k not in ('algorithm', 'security')}
            self.assertEqual(rest, {k: v for k, v in contracts['preprod'].items() if k not in ('algorithm', 'security')}, name)
            self.assertIs(networks.for_contract(json.loads(json.dumps(value))), networks.network_profile(name))
        self.assertEqual(contracts['preview']['security'], 432)

    def test_start_cursor_is_the_midnight_genesis_reference_or_block_zero(self):
        for name in networks.names():
            with self.subTest(network=name):
                p = networks.network_profile(name)
                position = Profile(profile_dir(name)).configs['cnight-config']['next_cardano_position']
                if p.start[0] == 0:
                    self.assertEqual(position['block_hash'].removeprefix('0x'), '0' * 64)
                else:
                    self.assertEqual(position['block_hash'].removeprefix('0x'), p.raw['midnight']['genesis_cardano_reference'])

    def test_profile_identities_belong_to_their_network_only(self):
        for name in networks.names():
            identity = Profile(profile_dir(name)).identity
            self.assertEqual(networks.for_identity(identity).network, name)
            for other in networks.names():
                if other != name:
                    self.assertIsNone(networks.for_identity(dict(identity, network=other)), (name, other))


class StartAtBlockZeroTests(unittest.TestCase):
    """Preview's start cursor S = 0:0: no negative epochs and no history before S."""

    def test_required_epochs_never_go_below_zero(self):
        from db_sync_alt.contract import required_epochs
        self.assertEqual(required_epochs(0, 0), [0])
        self.assertEqual(required_epochs(0, 3), [0, 1, 2, 3])
        self.assertEqual(required_epochs(1, 2), [0, 1, 2])
        self.assertEqual(required_epochs(276, 280), list(range(274, 281)))

    def test_root_receipt_without_prerequisites_is_accepted_at_block_zero(self):
        from db_sync_alt.verify.completion import validate_sql_receipt
        def receipt(start, target, ranges):
            generation = {'id': 0, 'parent': None, 'source': 'cardano-db-sync-postgresql', 'start_height': start,
                          'target_height': target, 'witness_height': target + 432}
            independent = {'version': 1, 'scope': 'independent-new-facts-and-membership-with-certified-parent',
                           'parent_digest': None, 'ranges': ranges, 'epochs': [],
                           'inventory': {t: {'count': 0, 'sha256': '0' * 64} for t in TABLES}}
            independent['inventory']['blocks']['count'] = target + 433 - start
            manifest = {'parent_digest': None, 'required_epochs': [], 'digests': {},
                        'acquisition': {'version': 1, 'window_blocks': 1000},
                        'validation': {'independent': independent, 'digests': {}}}
            class Store:
                def checkpoint_rows(self, **kw): return []
                def sealed_tables(self, generation): return TABLES
            return Store(), generation, manifest, {'independent': independent}
        ranges = [['metadata', 0, 435], ['events', 0, 3]]
        validate_sql_receipt(*receipt(0, 3, ranges))
        for bad in ([['prerequisites', 0, -1], *ranges], [['prerequisites', 0, 0], *ranges]):
            with self.subTest(ranges=bad), self.assertRaisesRegex(Refusal, 'completion_invalid'):
                validate_sql_receipt(*receipt(0, 3, bad))
        with self.assertRaisesRegex(Refusal, 'completion_invalid'):
            validate_sql_receipt(*receipt(10, 13, [['metadata', 10, 445], ['events', 10, 13]]))
        validate_sql_receipt(*receipt(10, 13, [['prerequisites', 0, 9], ['metadata', 10, 445], ['events', 10, 13]]))


class SecurityParameterTests(unittest.TestCase):
    """k = 432: the Blockfrost driver, jobs and poller use Preview's security parameter, never 2160."""

    def setUp(self):
        import threading
        import fixtures.sync as fakes
        self.fakes = fakes
        self.preview = Profile(profile_dir('preview'))
        self.clock = fakes.Clock()
        self.store = fakes.Store(security=432)
        self.client = fakes.Client(self.clock, security=432)
        self.update = fakes.FixtureUpdate(security=432)
        self.health = fakes.health()
        self.stop = threading.Event()

    def driver(self, profile):
        from types import SimpleNamespace
        from db_sync_alt.sources.blockfrost.sync import SyncDriver
        return SyncDriver(SimpleNamespace(sync={}), client=self.client, profile=profile, clock=self.clock, update_fn=self.update)

    def test_confirmed_target_and_job_witness_use_k_432(self):
        self.client.confirmed = 130
        self.driver(self.preview).run_once(self.store, self.health, self.stop)
        self.assertEqual([p for p, _ in self.client.calls][:3], ['/metrics', '/blocks/latest', '/blocks/130'])
        job = self.store.jobs['provider-active']
        self.assertEqual((job['target'], job['witness'], job['state']), (130, 130 + 432, 'completed'))
        self.assertEqual(self.update.calls, [(101, 130)])

    def test_a_2160_job_is_refused_on_preview(self):
        self.store.jobs['provider-active'] = {'job_id': 'old', 'attempts': 0, 'budget': 10000, 'state': 'active',
                                              'kind': 'acquire', 'parent': 0, 'target': 130, 'witness': 130 + 2160,
                                              'created_at': self.clock(), 'retry_at': 0, 'failure_count': 0}
        from db_sync_alt.errors import LifecycleFailure
        with self.assertRaisesRegex(LifecycleFailure, 'invalid_sync_job'):
            self.driver(self.preview).run_once(self.store, self.health, self.stop)

    def test_preprod_profile_against_a_432_provider_never_confirms(self):
        # A provider head that is only 432 blocks ahead is not 2160-confirmed for Preprod.
        from db_sync_alt.errors import TransientSyncFailure
        self.client.confirmed = 130
        with self.assertRaises(TransientSyncFailure):
            self.driver(Profile(profile_dir('preprod'))).run_once(self.store, self.health, self.stop)
        self.assertNotIn('provider-active', self.store.jobs)

    def test_poller_reads_head_minus_432(self):
        from db_sync_alt.service.health import HealthState
        from db_sync_alt.sources.blockfrost.poller import HeadPoller
        self.health = HealthState(service_version='1', schema_version=2, network='preview', profile='test',
                                  wall_clock=self.clock, monotonic=self.clock)
        self.assertEqual(self.health.policy.serving_lag_max_blocks, networks.network_profile('preview').serving_lag_max_blocks)
        poller = HeadPoller(lambda: self.store, lambda: self.client, self.health, self.stop, clock=self.clock,
                            monotonic=self.clock, network=self.preview.network)
        self.client.confirmed = 500
        poller.tick(self.store, self.client)
        self.assertEqual([p for p, _ in self.client.calls][-2:], ['/blocks/latest', '/blocks/500'])
        self.assertEqual(poller.latest().confirmed['height'], 500)
        self.assertEqual(poller.latest().head['height'], 932)


class MismatchTests(unittest.TestCase):
    """Credentials, providers, sources and backups of another network are refused."""

    def key(self, network):
        return network + 'synthetic0000000000000000'

    def test_portable_client_takes_host_and_key_prefix_from_the_network(self):
        from unittest.mock import patch
        import db_sync_alt.sources.blockfrost.provider as provider
        with tempfile.TemporaryDirectory() as tmp:
            for name in networks.names():
                network = networks.network_profile(name)
                for other in networks.names():
                    if other != name:
                        with self.subTest(network=name, key=other), self.assertRaisesRegex(provider.ProviderError, 'credential_network_mismatch'):
                            provider.PortableClient(self.key(other), Path(tmp) / name, network=network)
                connections = []
                class Response:
                    status = 200
                    def read(self, size): return b'[]'
                    def getheaders(self): return []
                class Connection:
                    def __init__(self, *args, **kwargs): self.args = args; connections.append(self)
                    def request(self, *args, **kwargs): self.request_args = args
                    def getresponse(self): return Response()
                    def close(self): pass
                client = provider.PortableClient(self.key(name), Path(tmp) / name, network=network, sleep=lambda seconds: None)
                self.assertEqual(client.base, 'https://cardano-' + name + '.blockfrost.io/api/v0')
                with patch.object(provider.http.client, 'HTTPSConnection', Connection):
                    self.assertEqual(client.get('/metrics'), [])
                self.assertEqual(connections[0].args, ('cardano-' + name + '.blockfrost.io',))

    def test_configured_client_refuses_a_key_file_of_another_network(self):
        import os
        from unittest.mock import patch
        import db_sync_alt.sources.blockfrost.provider as provider
        with tempfile.TemporaryDirectory() as tmp:
            key = Path(tmp) / 'key'
            key.write_text(self.key('preprod') + '\n')
            key.chmod(0o600)
            env = {'DBSYNC_PROVIDER_ADAPTER': 'portable', 'DBSYNC_PROVIDER_KEY_FILE': str(key), 'DBSYNC_PROVIDER_STATE': str(Path(tmp) / 'state')}
            with patch.dict(os.environ, env):
                self.assertEqual(provider.configured_client(networks.network_profile('preprod')).base, networks.network_profile('preprod').blockfrost_base)
                with self.assertRaisesRegex(provider.ProviderError, 'provider_configuration_rejected'):
                    provider.configured_client(networks.network_profile('preview'))

    def test_shared_client_must_point_to_the_network_host(self):
        from unittest.mock import patch
        import db_sync_alt.sources.blockfrost.provider as provider
        preprod, preview = networks.network_profile('preprod'), networks.network_profile('preview')
        class Client:
            base = preprod.blockfrost_base
            def __init__(self, profile, root): self.profile = profile
        module = type('Module', (), {'Client': Client})
        with patch.object(provider, 'external_module', return_value=module):
            self.assertEqual(provider.SharedClient('/synthetic', network=preprod).client.profile, 'preprod')
            with self.assertRaisesRegex(provider.ProviderError, 'wrong_provider_network'):
                provider.SharedClient('/synthetic', network=preview)

    def test_launcher_refuses_a_key_of_another_network(self):
        import contextlib
        import io
        from db_sync_alt.cli import run
        with tempfile.TemporaryDirectory() as tmp:
            for network, key in (('preview', self.key('preprod')), ('preprod', self.key('preview')), ('mainnet', self.key('preprod'))):
                path = Path(tmp) / (network + '.key')
                path.write_text(key + '\n')
                path.chmod(0o600)
                err = io.StringIO()
                with self.subTest(network=network), contextlib.redirect_stderr(err):
                    self.assertEqual(run.main(['--network', network, '--blockfrost-key-file', str(path),
                                               '--state-dir', str(Path(tmp) / 'state')]), 2)
                self.assertIn(network + '_credential_required', err.getvalue())
                self.assertNotIn(key, err.getvalue())
                self.assertFalse((Path(tmp) / 'state').exists())

    def test_blockfrost_genesis_magic_must_be_the_network_magic(self):
        from db_sync_alt.sources.blockfrost.adapter import update
        preview = Profile(profile_dir('preview'))
        class Stop(Exception):
            pass
        class Store:
            def pending(self): return None
            def generation(self, generation=None):
                anchor = {'height': 0, 'hash': '00' * 32}
                return {'id': 0, 'target_height': 3, 'manifest': {'anchors': {'start': anchor, 'target': anchor}}}
            def meta(self, key): return preview.identity
        class Hooks:
            def update(self, **fields): pass
            def verify(self, store, generation=None, **kwargs): pass
        class Provider:
            def __init__(self, magic): self.magic = magic
            def get(self, path):
                if path == '/genesis':
                    return {'network_magic': self.magic}
                raise Stop(path)
        for magic in (1, 764824073):
            with self.subTest(magic=magic), self.assertRaisesRegex(Refusal, 'wrong_provider_network'):
                update(Store(), Provider(magic), preview, hooks=Hooks())
        with self.assertRaises(Stop):
            update(Store(), Provider(2), preview, hooks=Hooks())

    def test_backup_of_another_network_is_refused(self):
        import argparse
        from db_sync_alt.backup import BACKUP_FORMAT, file_hash, restore
        from db_sync_alt.cli import run
        from db_sync_alt.store.database import OWNER, POSTGRES_MAJOR, SCHEMA_VERSION
        with tempfile.TemporaryDirectory() as tmp:
            archive = Path(tmp) / 'state.dump'
            archive.write_bytes(b'synthetic archive')
            for name in networks.names():
                manifest = Path(tmp) / (name + '.json')
                manifest.write_text(json.dumps({'format': BACKUP_FORMAT, 'postgres_major': POSTGRES_MAJOR, 'schema_version': SCHEMA_VERSION,
                                                'owner': OWNER, 'archive': file_hash(archive),
                                                'profile': Profile(profile_dir(name)).identity}))
                accepted = run.accepted_backup(argparse.Namespace(network=name, archive=archive, manifest=manifest))
                self.assertEqual(accepted[1], manifest.absolute())
                for other in networks.names():
                    if other == name:
                        continue
                    with self.subTest(backup=name, network=other):
                        with self.assertRaisesRegex(run.LaunchError, 'invalid_accepted_backup'):
                            run.accepted_backup(argparse.Namespace(network=other, archive=archive, manifest=manifest))
                        with self.assertRaisesRegex(Refusal, 'backup_profile_mismatch'):
                            restore(archive, manifest, conninfo='host=/nonexistent', expected_network=other)

    def test_compose_configures_the_network_of_the_supplied_backup(self):
        import argparse
        import contextlib
        import io
        from db_sync_alt.cli import compose
        with tempfile.TemporaryDirectory() as tmp:
            archive = Path(tmp) / 'state.dump'
            archive.write_bytes(b'synthetic archive')
            manifest = Path(tmp) / 'manifest.json'
            manifest.write_text(json.dumps({'profile': Profile(profile_dir('preview')).identity}))
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                compose.initialize(argparse.Namespace(private=str(Path(tmp) / 'private'), archive=str(archive), manifest=str(manifest),
                                                      http_port=18080))
            config = json.loads((Path(tmp) / 'private/service/service.json').read_text())
            self.assertEqual((config['network'], config['profile']), ('preview', 'midnight-preview-node-1.0.0'))
            self.assertIn('Blockfrost Preview key', out.getvalue())
            manifest.write_text(json.dumps({'profile': dict(Profile(profile_dir('preview')).identity, genesis='0' * 64)}))
            with self.assertRaisesRegex(ValueError, 'backup_profile_mismatch'):
                compose.initialize(argparse.Namespace(private=str(Path(tmp) / 'other'), archive=str(archive), manifest=str(manifest),
                                                      http_port=18080))
