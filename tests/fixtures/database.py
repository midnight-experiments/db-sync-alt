"""PostgreSQL fixture databases for sync tests: per-test databases holding generation 0, a sync driver run and the full-update reference."""
import os
import tempfile
import threading
import unittest
import uuid
from types import SimpleNamespace
import psycopg
from psycopg import sql
from db_sync_alt.sources.blockfrost.sync import SyncDriver
from db_sync_alt.service.health import HealthState
from fixtures.blockfrost import Fake
import fixtures.blockfrost_generation as gen_fixture


class AccountedFake(Fake):
    def status(self):return {'budget_used':len(self.calls),'daily_limit':50000}


class SyncFixture(unittest.TestCase):
    """PostgreSQL databases built from the two-generation Blockfrost fixture; no test methods of its own."""
    @classmethod
    def setUpClass(cls):cls.admin=psycopg.connect(os.environ['DB_SYNC_ALT_TEST_DSN'],autocommit=True)
    @classmethod
    def tearDownClass(cls):cls.admin.close()
    def setUp(self):
        self.databases=[];self.tmp=tempfile.TemporaryDirectory();self.profile=gen_fixture.PROFILE
    def tearDown(self):
        for name in self.databases:
            self.admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(name)))
        self.tmp.cleanup()
    def database(self):
        name='db_sync_alt_test_'+uuid.uuid4().hex[:10];self.databases.append(name)
        self.admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
        return psycopg.conninfo.make_conninfo(os.environ['DB_SYNC_ALT_TEST_DSN'],dbname=name)
    def make(self,transition=False):
        """A new database holding generation 0 of the Blockfrost scenario; returns its DSN."""
        dsn=self.database()
        with gen_fixture.baseline(dsn,transition=transition):pass
        return dsn
    def state(self):return HealthState(service_version='1',schema_version=1,network='preprod',profile='fixture')
    def run_driver(self,store,routes,**kw):
        driver=SyncDriver(SimpleNamespace(sync={}),client=AccountedFake(routes),profile=self.profile,**kw)
        driver.run_once(store,self.state(),threading.Event())
        return driver
    def reference(self,routes,transition=False):
        """Report of the full update (`adapter.update`) to OFFSET+12 on a fresh copy of generation 0."""
        with gen_fixture.baseline(self.database(),transition=transition) as reference:
            return gen_fixture.advance(reference,routes,budget=500)
    def compare_reference(self,routes,store,transition=False):
        """The ready generation of `store` has the same fact digests as the full update with `routes`."""
        expected=self.reference(routes,transition)
        self.assertEqual(store.digests(store.generation()['id']),expected['digests'])
        self.assertEqual(set(expected['digests']),{'blocks','transactions','outputs','inputs','assets','output_assets','mints','datums','epoch_nonces','epoch_stakes'})
