"""Backups of schema 4 against actual PostgreSQL: one verification walk, format v3 with the integrity
checkpoints, and schema-3 (format v2) backups restored and upgraded before the node reads them."""
import json
import unittest
from pathlib import Path
from unittest.mock import patch

from db_sync_alt import backup as backups
from db_sync_alt.backup import backup, file_hash, restore
from db_sync_alt.contract import Refusal
from db_sync_alt.store.database import PgStore
from db_sync_alt.store.reader import verify_reader
from db_sync_alt.verify.store import verify_store
from fixtures.checkpoints import OFFSET, CheckpointFixture, Walk

QUERY = 'SELECT count(*),max(block_no),(SELECT count(*) FROM tx_in),(SELECT count(*) FROM epoch_param) FROM block'


class CheckpointBackupPGTests(CheckpointFixture):
    def archive(self, name):
        folder = Path(self.tmp.name)
        return folder / (name + '.dump'), folder / (name + '.json')

    def state_of(self, dsn):
        with PgStore(conninfo=dsn, readonly=True) as store:
            rows = store.db.execute("SELECT generation,previous_base,root,digests,service_version,checkpointed_at FROM chain.integrity_checkpoints ORDER BY generation").fetchall()
            return store.state()['schema_version'], store.integrity()['base'], rows, store.generation()['id']

    def test_backup_v3_verifies_once_and_restores_checkpoints(self):
        dsn, store = self.history(1)
        store, _ = self.startup(dsn, store)  # checkpoint at 1
        for n in range(2):
            self.follow(store, dsn, OFFSET + 12 + n)  # generations 2 and 3 after it
        store.close()
        archive, manifest = self.archive('v3')
        calls = []
        original = backups.verify_store

        def counted(*args, **kwargs):
            calls.append(args[1:])
            return original(*args, **kwargs)
        with patch.object(backups, 'verify_store', counted):
            receipt = backup(archive, manifest, conninfo=dsn)
        self.assertEqual(calls, [(3,)])
        body = json.loads(manifest.read_text())
        self.assertEqual((body['format'], body['schema_version'], body['canonical_ready']), ('db-sync-alt-postgresql-backup-v4', 5, 3))
        self.assertEqual(list(body['generations']), ['3'])
        self.assertIn('integrity_checkpoints', body['physical_inventory'])
        self.assertEqual(body['physical_inventory']['integrity_checkpoints']['count'], 1)
        # The verification of a backup walks from the checkpoint: only the two later transitions.
        walk = Walk()
        with PgStore(conninfo=dsn, readonly=True, progress_callback=walk) as reader_store:
            self.assertEqual(backups.reports(reader_store), body['generations'])
        self.assertEqual(walk.generations, {2, 3})
        destination = self.database(); role = self.reader(destination)
        calls.clear()
        # The restore reads the database size, not the list of generations with their manifests.
        with patch.object(backups, 'verify_store', counted), \
                patch.object(PgStore, 'status', side_effect=AssertionError('restore_listed_generations')):
            result = restore(archive, manifest, conninfo=destination, expected_manifest_sha256=receipt['manifest_sha256'], reader_role=role)
        self.assertEqual(calls, [(3,)])
        self.assertEqual((result['status'], result['canonical_ready']), ('restored', 3))
        self.assertEqual(self.state_of(destination), self.state_of(dsn))
        self.assertEqual(self.node(destination, role).execute(QUERY).fetchone(), self.node_answer(dsn))
        with PgStore(conninfo=destination) as restored:
            self.assertEqual(verify_reader(restored.db, role)['privileges'], 'node_relations_only')
        restored, walked = self.startup(destination)
        self.assertEqual(walked, {2, 3})
        self.assertEqual(restored.integrity()['base'], 3)

    def node_answer(self, dsn):
        role = self.reader(dsn)
        with PgStore(conninfo=dsn) as store:
            from db_sync_alt.store.reader import grant_reader
            grant_reader(store.db, role)
        return self.node(dsn, role).execute(QUERY).fetchone()

    def test_schema3_backup_restores_upgraded_with_its_first_checkpoint(self):
        """A schema-3 backup of an earlier release (the 0.1.0 snapshot's shape) restores, then is
        upgraded to schema 5 with its first checkpoint and every stored epoch's size."""
        from db_sync_alt.store import database
        from db_sync_alt.store.epoch_parameters import ProviderEpochSource
        from fixtures.database import AccountedFake
        from fixtures.legacy import nonce_only
        archive, manifest = self.archive('v2')
        with nonce_only():
            dsn, store = self.history(2, schema=3)
            expected = verify_store(store)
            store.close()
            with patch.object(database, 'SCHEMA_VERSION', 3):
                backup(archive, manifest, conninfo=dsn)
        body = json.loads(manifest.read_text())
        self.assertEqual((body['format'], body['schema_version']), ('db-sync-alt-postgresql-backup-v2', 3))
        self.assertEqual(sorted(body['generations']), ['0', '1', '2'])  # format v2 records every ready generation
        sizes = {'/epochs/%d/parameters' % e: {'epoch': e, 'nonce': '6e' * 32, 'max_tx_size': 12000 + e} for e in range(3)}
        epochs = lambda network: ProviderEpochSource(AccountedFake(sizes), network)
        destination = self.database(); role = self.reader(destination)
        with self.assertRaisesRegex(Refusal, 'epoch_parameters_source_required'):
            restore(archive, manifest, conninfo=destination, reader_role=role)
        result = restore(archive, manifest, conninfo=destination, expected_manifest_sha256=file_hash(manifest)['sha256'], reader_role=role,
                         epoch_source=epochs)
        self.assertEqual(result['reader']['reader'], role)
        schema, base, rows, canonical = self.state_of(destination)
        self.assertEqual((schema, base, canonical, [row[:2] for row in rows]), (5, 2, 2, [(2, 0)]))
        self.assertEqual(rows[0][3], expected['digests'])
        answer = self.node(destination, role).execute(QUERY).fetchone()
        self.assertEqual(answer[1], OFFSET + 2170 + 2)
        restored, walked = self.startup(destination)
        self.assertEqual(walked, set())
        # Without the reader as well: a restored schema-3 backup is always upgraded.
        plain = self.database()
        restore(archive, manifest, conninfo=plain, epoch_source=epochs)
        self.assertEqual(self.state_of(plain)[:2], (5, 2))

    def test_tampered_v3_backups_are_refused(self):
        dsn, store = self.history(1)
        store, _ = self.startup(dsn, store)
        self.follow(store, dsn, OFFSET + 12)
        store.close()
        archive, manifest = self.archive('source')
        receipt = backup(archive, manifest, conninfo=dsn)
        original = json.loads(manifest.read_text())

        def variant(name, change=None, archive_bytes=None):
            path_archive, path_manifest = self.archive(name)
            path_archive.write_bytes(archive_bytes if archive_bytes is not None else archive.read_bytes())
            body = json.loads(json.dumps(original))
            if change:
                change(body)
            body['archive'] = file_hash(path_archive) if archive_bytes is None else original['archive']
            path_manifest.write_text(json.dumps(body))
            return path_archive, path_manifest

        def report(body):
            body['generations']['2']['digests']['blocks']['count'] += 1

        def checkpoints(body):
            body['physical_inventory']['integrity_checkpoints']['sha256'] = '0' * 64
        cases = [
            ('archive bytes', variant('bytes', archive_bytes=archive.read_bytes() + b'x'), {}, 'backup_checksum_mismatch'),
            ('manifest checksum', variant('checksum', report), {'expected_manifest_sha256': receipt['manifest_sha256']}, 'backup_manifest_checksum_mismatch'),
            ('generation report', variant('report', report), {}, 'restored_database_mismatch'),
            ('checkpoint inventory', variant('inventory', checkpoints), {}, 'restored_database_mismatch'),
            ('canonical', variant('canonical', lambda body: body.update(canonical_ready='2')), {}, 'invalid_backup_canonical'),
            ('schema of the format', variant('schema', lambda body: body.update(schema_version=3)), {}, 'unsupported_backup_identity'),
        ]
        for name, (path_archive, path_manifest), options, code in cases:
            with self.subTest(name), self.assertRaisesRegex(Refusal, '^' + code + '$'):
                restore(path_archive, path_manifest, conninfo=self.database(), **options)
        destination = self.database()
        restore(archive, manifest, conninfo=destination, expected_manifest_sha256=receipt['manifest_sha256'])
        self.assertEqual(self.state_of(destination), self.state_of(dsn))


if __name__ == '__main__':
    unittest.main(verbosity=2)
