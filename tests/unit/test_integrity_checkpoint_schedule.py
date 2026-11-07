"""The periodic integrity checkpoint of the sync driver: its option and when it runs (no database)."""
import unittest
from types import SimpleNamespace
from db_sync_alt.contract import Refusal
from db_sync_alt.service.runtime import LifecycleFailure
from db_sync_alt.sources.blockfrost.sync import SyncDriver
from fixtures.networks import PREPROD_PROFILE
from fixtures.sync import Clock, Store, health

DAY = 86400


class CheckpointStore(Store):
    """The in-memory store with an integrity base and a full verification that can be observed."""
    supports_checkpoints = True

    def __init__(self, *, base=0, checkpointed_at=None, observe=None):
        super().__init__()
        self.base, self.checkpointed_at, self.observe = base, checkpointed_at, observe
        self.verified, self.written, self.refusal = [], [], None

    def state(self):return {'revoked': self.revoked}
    def integrity(self):return {'base': self.base, 'checkpointed_at': self.checkpointed_at}

    def certify_checkpoint(self):
        if self.observe:self.observe()
        if self.refusal:raise Refusal(self.refusal)
        self.verified.append(self.gid)
        return ('capability', self.gid)

    def write_checkpoint(self, capability):
        self.written.append(capability)
        self.base, self.checkpointed_at = capability[1], self.clock()


class ScheduleTests(unittest.TestCase):
    def driver(self, interval=None, clock=None):
        sync = {} if interval is None else {'integrity_checkpoint_interval_seconds': interval}
        return SyncDriver(SimpleNamespace(sync=sync), client=object(), profile=PREPROD_PROFILE, clock=clock or Clock())

    def test_interval_option_bounds_and_default(self):
        self.assertEqual(self.driver().options['integrity_checkpoint_interval_seconds'], 21600)
        for value in (600, 604800):
            self.assertEqual(self.driver(value).options['integrity_checkpoint_interval_seconds'], value)
        for value in (599, 604801, 0, -600, True, 600.0, '600', None):
            with self.subTest(value=value), self.assertRaisesRegex(LifecycleFailure, 'invalid_sync_configuration'):
                SyncDriver(SimpleNamespace(sync={'integrity_checkpoint_interval_seconds': value}), client=object(), profile=PREPROD_PROFILE)

    def test_runs_once_the_interval_since_the_base_checkpoint_has_passed(self):
        clock = Clock(); state = health(); seen = []
        store = CheckpointStore(base=3, checkpointed_at=clock.now - 600, observe=lambda: seen.append(state.snapshot()['sync']))
        store.clock = clock; store.gid = 5
        driver = self.driver(600, clock)
        state.begin_operation('provider')
        self.assertIs(driver._checkpoint(store, state), True)
        self.assertEqual((store.written, seen), ([('capability', 5)], ['checkpointing']))
        self.assertEqual(state.snapshot()['integrity'], {'base': 5, 'checkpointed_at': clock.now})
        self.assertEqual((state.snapshot()['sync'], state.snapshot()['operation']['stage']), ('syncing', 'verification'))
        # The next one is due an interval after this checkpoint.
        store.gid = 6; clock.now += 599
        self.assertIs(driver._checkpoint(store, state), False)
        clock.now += 1
        self.assertIs(driver._checkpoint(store, state), True)
        self.assertEqual(store.written[-1], ('capability', 6))

    def test_a_base_without_checkpoint_counts_from_the_first_pass(self):
        clock = Clock(); store = CheckpointStore(base=0, checkpointed_at=None); store.clock = clock; store.gid = 2
        driver = self.driver(None, clock)
        self.assertIs(driver._checkpoint(store, health()), False)
        clock.now += 21599
        self.assertIs(driver._checkpoint(store, health()), False)
        clock.now += 1
        self.assertIs(driver._checkpoint(store, health()), True)
        self.assertEqual(store.written, [('capability', 2)])

    def test_nothing_to_checkpoint(self):
        clock = Clock()
        cases = {'no new generation': dict(gid=4, base=4), 'revoked': dict(gid=5, base=4, revoked=True),
                 'schema without checkpoints': dict(gid=5, base=4, supports_checkpoints=False)}
        for name, values in cases.items():
            with self.subTest(name):
                store = CheckpointStore(base=values['base'], checkpointed_at=clock.now - DAY)
                store.gid, store.revoked = values['gid'], values.get('revoked', False)
                if 'supports_checkpoints' in values:store.supports_checkpoints = values['supports_checkpoints']
                self.assertIs(self.driver(600, clock)._checkpoint(store, health()), False)
                self.assertEqual((store.verified, store.written), ([], []))

    def test_changed_contents_retry_and_other_refusals_fail(self):
        clock = Clock(); store = CheckpointStore(base=1, checkpointed_at=clock.now - DAY); store.clock = clock; store.gid = 2
        driver = self.driver(600, clock)
        store.refusal = 'integrity_revision_changed'
        self.assertIs(driver._checkpoint(store, health()), True)
        self.assertEqual(store.written, [])
        store.refusal = 'checkpoint_digest_mismatch'
        with self.assertRaisesRegex(Refusal, 'checkpoint_digest_mismatch'):
            driver._checkpoint(store, health())
        store.refusal = None
        self.assertIs(driver._checkpoint(store, health()), True)
        self.assertEqual(store.written, [('capability', 2)])


if __name__ == '__main__':
    unittest.main()
