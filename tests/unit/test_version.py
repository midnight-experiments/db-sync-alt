# SPDX-License-Identifier: Apache-2.0
"""The package declares one version, and every reported service version is that version."""
from pathlib import Path
import tomllib
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from db_sync_alt import __version__
from db_sync_alt.cli.acquire import blockfrost_once
from db_sync_alt.contract import Refusal
from db_sync_alt.service.runtime import Service
from fixtures.service import Runtime, config, get


class Idle:
    def run_once(self, store, health, stop_event):
        return False


class VersionTests(unittest.TestCase):
    def test_pyproject_declares_the_package_version(self):
        pyproject = Path(__file__).resolve().parents[2] / 'pyproject.toml'
        self.assertEqual(tomllib.loads(pyproject.read_text())['project']['version'], __version__)

    def test_service_status_reports_the_package_version(self):
        service = Service(config(), runtime=Runtime(), sync_driver=Idle())
        self.addCleanup(service.stop)
        service.start()
        code, status = get(service, '/status')
        self.assertEqual(code, 200)
        self.assertEqual(status['service_version'], __version__)

    def test_blockfrost_acquire_reports_the_package_version(self):
        class Store:
            supports_incremental = False
            def generation(self):return {'id': 1, 'target_height': 100, 'witness_height': 100}
            def pending(self):return None
            def sync_job(self, name):return None
            def state(self):return {'publication_serial': 0}
        reported = []
        class Driver:
            def __init__(self, *args, **kwargs):pass
            def run_once(self, store, health, stop_event):
                reported.append(health.snapshot()['service_version'])
                stop_event.set()
        profile = SimpleNamespace(network=SimpleNamespace(network='preprod'), identity={'config_sha256': 'f' * 64})
        with patch('db_sync_alt.sources.blockfrost.sync.SyncDriver', Driver):
            with self.assertRaises(Refusal) as stopped:
                blockfrost_once(Store(), profile, target_height=101)
        self.assertEqual(stopped.exception.code, 'interrupted')
        self.assertEqual(reported, [__version__])


if __name__ == '__main__':
    unittest.main()
