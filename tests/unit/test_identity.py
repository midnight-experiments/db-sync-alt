# SPDX-License-Identifier: Apache-2.0
"""Persisted identity of the Preprod profile: any change here invalidates existing stores and backups."""
import hashlib
import json
from pathlib import Path
import unittest

from db_sync_alt import contract, networks
from db_sync_alt.node_queries import CAT
from db_sync_alt.resources import data_dir, profile_dir
from db_sync_alt.sources.blockfrost import adapter
from db_sync_alt.verify import completion


def sha(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


class IdentityTests(unittest.TestCase):
    def test_contract_and_profile(self):
        profile = contract.Profile(profile_dir('preprod'))
        preprod = networks.network_profile('preprod')
        self.assertEqual(contract.digest(contract.contract_for(preprod)), '985d815697c6586548247a350c9e18aaea1fad63e0b9512e99016cee1998d5db')
        self.assertEqual(contract.digest(profile.contract), '985d815697c6586548247a350c9e18aaea1fad63e0b9512e99016cee1998d5db')
        # The nonce-only contract stored by every earlier release: stores and backups that carry it
        # must still be recognised to be verified and upgraded.
        self.assertEqual(contract.digest(contract.contract_for(preprod, legacy=True)), '65da5fbe1e66529141f8ae3e8b9330e0b2110d2ed17ef3ac94899c84f6e5cbab')
        self.assertEqual(contract.digest(preprod.legacy_contract), '65da5fbe1e66529141f8ae3e8b9330e0b2110d2ed17ef3ac94899c84f6e5cbab')
        self.assertEqual((contract.FORMAT, contract.LEGACY_FORMAT, preprod.algorithm, preprod.security, contract.SCHEMA_VERSION),
                         ('midnight-compact-cardano-v2', 'midnight-compact-cardano-v1', 'preprod-selective-v1', 2160, (15, 48, 6)))
        self.assertEqual(contract.TABLES['epoch_nonces'], ('epoch', 'nonce', 'max_tx_size'))
        self.assertEqual(contract.LEGACY_TABLES['epoch_nonces'], ('epoch', 'nonce'))
        self.assertEqual(contract.generation_capabilities(), {'bridge_qualified': False, 'candidates_empty': True, 'epoch_max_tx_size_v1': True})
        self.assertEqual(contract.generation_capabilities(legacy=True), {'bridge_qualified': False, 'candidates_empty': True})
        self.assertEqual(preprod.config_sha256, '88a096f98b01f55c0680eb523f58fa9c4c141f3546794e4f5fd1084a76e32304')
        self.assertEqual(contract.digest(profile.configs), preprod.config_sha256)
        self.assertEqual(contract.digest(profile.identity), 'd69ed610ff7a698df5b1186e03f4c1c1bcc25a5530861aa9ffcb167cdd5a3d48')
        self.assertEqual(profile.identity['profile'], 'midnight-preprod-node-1.0.400')
        self.assertEqual(preprod.genesis, 'df831b09a8baa92badf47762ce5ac439b7e47e3ed3d39600cfdd44fad552361b')

    def test_source_identities(self):
        preprod = networks.network_profile('preprod')
        self.assertEqual(preprod.source_identity, {'adapter': 'blockfrost-preprod-cbor-v1', 'network': 'preprod',
                                                   'openapi': '1d4d5d8bac8f0c641eea5c6f7b1f3fbbed8d1505'})
        self.assertEqual(contract.digest(preprod.source_identity), 'b30066cd2960b6e4c790583b443f63ce8ca84fd1e42dcdcc0fdc13e048d61e69')
        self.assertEqual((completion.SQL_ADAPTER, completion.SQL_VALIDATION), ('db-sync-direct-pg-v1', 'independent-sql-delta-inventory-v2'))
        self.assertEqual((preprod.adapter, adapter.OPENAPI), ('blockfrost-preprod-cbor-v1', '1d4d5d8bac8f0c641eea5c6f7b1f3fbbed8d1505'))
        for name in networks.names():
            self.assertEqual(networks.network_profile(name).openapi, adapter.OPENAPI, name)

    def test_recorded_preprod_manifest_matches_the_profile(self):
        """A generation manifest written by the deployed Preprod service carries exactly the profile's values."""
        recorded = json.loads((Path(__file__).resolve().parents[1] / 'fixtures/data/completion-g3-manifest.json').read_text())
        g, m = recorded['generation'], recorded['generation']['manifest']
        profile = contract.Profile(profile_dir('preprod'))
        # Recorded before epoch parameters: it binds the nonce-only contract.
        self.assertEqual(m['contracts'], contract.digest(profile.network.legacy_contract))
        self.assertEqual(m['source_identity'], profile.network.source_identity)
        self.assertEqual(contract.canonical(m['identity']), contract.canonical(profile.identity))
        self.assertEqual(g['witness_height'] - g['target_height'], profile.security)
        self.assertEqual((g['start_height'], g['start_index']), profile.start)
        self.assertEqual(networks.for_identity(m['identity']), profile.network)
        self.assertEqual(m['required_epochs'], contract.required_epochs(m['anchors']['start']['epoch'], m['required_epochs'][-1]))

    def test_preprod_receipt_strings(self):
        import threading
        from db_sync_alt.sources.blockfrost.sync import AccountedProvider
        job = {'job_id': 'job', 'attempts': 0, 'budget': 1, 'state': 'active', 'kind': 'poll'}
        drained = AccountedProvider(None, 'k', job, None, None, threading.Event(), network=networks.network_profile('preprod')).drain()
        self.assertEqual({k: drained[k] for k in ('source', 'shared_profile', 'adapter', 'openapi')},
                         {'source': 'blockfrost-cardano', 'shared_profile': 'preprod', 'adapter': 'blockfrost-preprod-cbor-v1',
                          'openapi': '1d4d5d8bac8f0c641eea5c6f7b1f3fbbed8d1505'})

    def test_query_catalogue(self):
        catalogue = sorted((k, v['name'], v['file'], json.dumps(v.get('sources', []), sort_keys=True)) for k, v in CAT.items())
        self.assertEqual(len(CAT), 53)
        self.assertEqual(sha(catalogue), 'cce2bb97d3bc7b44869990c09bc423b88be61aeb410e5b6b81be478c47c9029d')
        for name in networks.names():
            self.assertEqual(networks.network_profile(name).node_queries,
                             {'tag': 'node-2.1.101', 'commit': '86774edf8cc76345a5c3a4221f3fe80c094b1c33'}, name)

    def test_persisted_database_and_backup_identifiers(self):
        from db_sync_alt import backup
        from db_sync_alt.store import database, incremental
        self.assertEqual((database.OWNER, database.RESTORE_GUARD, database.PROJECTION_WRITE),
                         ('db-sync-alt', 'db_sync_alt_restore_guard', 'db_sync_alt.projection_write'))
        self.assertEqual(backup.BACKUP_FORMAT, 'db-sync-alt-postgresql-backup-v4')
        self.assertEqual(backup.FORMATS, {'db-sync-alt-postgresql-backup-v1': (1, 2), 'db-sync-alt-postgresql-backup-v2': (3,),
                                          'db-sync-alt-postgresql-backup-v3': (4,), 'db-sync-alt-postgresql-backup-v4': (5,)})
        self.assertEqual(backup.SERVING_FORMATS, ('db-sync-alt-postgresql-backup-v2', 'db-sync-alt-postgresql-backup-v3',
                                                  'db-sync-alt-postgresql-backup-v4'))
        self.assertEqual((database.SCHEMA_VERSION, database.SUPPORTED_SCHEMA_VERSIONS), (5, (1, 2, 3, 4, 5)))
        self.assertEqual(database.EPOCH_UPGRADE, 'epoch-parameters-upgrade')
        self.assertEqual(incremental.FULL_DIGESTS, 'full-digests-v1')
        self.assertEqual(incremental.PROFILE_FIELD, {'certified-parent-delta-v1': 'identity', 'certified-parent-delta-v2': 'identity_sha256'})
        self.assertEqual((incremental.KIND_V1, incremental.KIND_V2), ('certified-parent-delta-v1', 'certified-parent-delta-v2'))
        store = Path(database.__file__).parent
        for name in ('schema.sql', 'schema_v2.sql', 'schema_v3.sql', 'schema_v4.sql', 'schema_v5.sql'):
            self.assertIn("s.owner='" + database.OWNER + "'", (store / name).read_text(), name)
        for name in ('schema_v2.sql', 'schema_v4.sql'):
            self.assertIn("current_setting('" + database.PROJECTION_WRITE + "',true)", (store / name).read_text(), name)
        self.assertEqual((database.WRITER_LOCK, database.SERVING_LOCK), ((36, 1), (36, 2)))
        for lock in ('classid=36 AND objid=1 AND objsubid=2', 'classid=36 AND objid=2 AND objsubid=2'):
            self.assertIn(lock, (store / 'schema_v4.sql').read_text())

    def test_packaged_reference_matches_its_manifest(self):
        reference = data_dir('reference')
        manifest = json.loads((reference / 'source-manifest.json').read_text())
        for name, entry in manifest['files'].items():
            self.assertEqual(hashlib.sha256((reference / name).read_bytes()).hexdigest(), entry['sha256'], name)


if __name__ == '__main__':
    unittest.main()
