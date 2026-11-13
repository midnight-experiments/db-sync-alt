"""The checks of a published update read the update's rows, not the history (issue 00054).

On the real Mainnet store, a follow published 2-block updates every 191 s: the historical-selected-event
checks re-read the parent's whole interval, and the dependency checks of the update switched to a
generic plan after the tenth publication of the process, which scanned whole tables.

Here a store holds a long history and receives many small updates, each checked on one connection as
the follow does. `auto_explain` reports the buffers of every statement the checks run. Each statement
stays under a fixed bound, and tripling the history does not change it. The 0.4.5 statements on the
same store read the history on every update (the negative control). The rewritten statements give the
0.4.5 answers on stores that hit and miss each of their branches.
"""
import json
import os
import unittest
import uuid

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.types.json import Jsonb

from db_sync_alt.contract import Refusal
from db_sync_alt.store.database import PgStore
from db_sync_alt.store.incremental import delta_rows
from db_sync_alt.store.upgrade import upgrade_store
from db_sync_alt.verify.limits import any_row, verification
from db_sync_alt.verify.store import DEPENDENCIES, mapping_policy, prior_answer_checks, validate_prior_answers, verify_fact_placement
from fixtures.generations import PROFILE, START, block, h, make_store

NEW = 3
NIGHT = (bytes.fromhex(PROFILE.night[:56]), bytes.fromhex(PROFILE.night[56:]))
CONFIGS = PROFILE.identity['configs']
MAPPING = CONFIGS['cnight-config']['addresses']['mapping_validator_address']
AUTH = mapping_policy(MAPPING)
PERMISSIONED = bytes.fromhex(PROFILE.permissioned)
COUNCIL, COMMITTEE = ((v['address'], bytes.fromhex(v['policy_id'])) for v in CONFIGS['federated-authority-config'].values())
# Heights against the parent (generation 2): its interval starts at (START, 4) and ends at START+2.
INSIDE, ABOVE, BEFORE = block(START + 1)['hash'], block(START + 3, True)['hash'], block(START - 10)['hash']
# History rows per table at generation 0, then twice as many more; updates per phase.
HISTORY = 60000
UPDATES = 12
# Buffers one statement of an update's checks may read (measured: at most 42), and its growth when the
# history triples (one more B-tree level per probe at most).
BOUND = 200
GROWTH = 20


def old_prior_answer_checks(store, generation):
    """The 0.4.5 statements of `validate_prior_answers`, verbatim and in the same order (a reference)."""
    g = store.generation(generation)
    parent = store.generation(g['parent']); pid = parent['id']
    configs = store.meta('profile')['configs']; cn = configs['cnight-config']; mapping = cn['addresses']['mapping_validator_address']
    night_policy = bytes.fromhex(cn['addresses']['cnight_policy_id']); night_name = cn['addresses']['cnight_asset_name'].encode()
    auth_policy = mapping_policy(mapping)
    start = (parent['start_height'], parent['start_index']); end = parent['target_height']
    interval = '(b.height,t.tx_index)>=(%s,%s) AND b.height<=%s'
    creation = """SELECT 1 FROM chain.output_assets a JOIN chain.outputs o ON o.tx_hash=a.tx_hash AND o.output_index=a.output_index JOIN chain.transactions t ON t.hash=o.tx_hash JOIN chain.blocks b ON b.hash=t.block_hash WHERE (a.first_generation>%s OR o.first_generation>%s OR t.first_generation>%s) AND a.first_generation<=%s AND """ + interval + """ AND ((a.policy=%s AND a.name=%s) OR (o.address=%s AND o.datum_hash IS NOT NULL AND a.policy=%s AND a.name=''::bytea AND a.quantity=1))"""
    spending = """SELECT 1 FROM chain.inputs i JOIN chain.transactions t ON t.hash=i.tx_hash JOIN chain.blocks b ON b.hash=t.block_hash JOIN chain.outputs o ON o.tx_hash=i.origin_hash AND o.output_index=i.origin_index WHERE (i.first_generation>%s OR EXISTS(SELECT 1 FROM chain.output_assets changed WHERE changed.tx_hash=o.tx_hash AND changed.output_index=o.output_index AND changed.policy=%s AND changed.name=%s AND changed.first_generation>%s AND changed.first_generation<=%s)) AND i.first_generation<=%s AND """ + interval + """ AND ((o.address=%s AND o.datum_hash IS NOT NULL) OR EXISTS(SELECT 1 FROM chain.output_assets a WHERE a.tx_hash=o.tx_hash AND a.output_index=o.output_index AND a.policy=%s AND a.name=%s))"""
    out = [(creation, (pid, pid, pid, generation, *start, end, night_policy, night_name, mapping, auth_policy)),
           (spending, (pid, night_policy, night_name, pid, generation, generation, *start, end, mapping, night_policy, night_name))]
    permissioned = bytes.fromhex(configs['permissioned-candidates-config']['permissioned_candidates_policy_id'].removeprefix('0x'))
    gov = [(v['address'], bytes.fromhex(v['policy_id'])) for v in configs['federated-authority-config'].values()]
    for address, policy in [(None, permissioned), *gov]:
        selector = 'a.policy=%s' + (' AND o.address=%s' if address else '')
        params = (policy, address) if address else (policy,)
        base = ' FROM chain.output_assets a JOIN chain.outputs o ON o.tx_hash=a.tx_hash AND o.output_index=a.output_index JOIN chain.transactions t ON t.hash=o.tx_hash JOIN chain.blocks b ON b.hash=t.block_hash WHERE ' + selector
        out.append(('SELECT 1' + base + ' AND b.height<=%s AND (a.first_generation>%s OR o.first_generation>%s) AND a.first_generation<=%s', (*params, end, pid, pid, generation)))
        out.append(('SELECT 1 FROM chain.inputs i JOIN chain.transactions spending ON spending.hash=i.tx_hash JOIN chain.blocks sb ON sb.hash=spending.block_hash JOIN chain.outputs o ON o.tx_hash=i.origin_hash AND o.output_index=i.origin_index JOIN chain.output_assets a ON a.tx_hash=o.tx_hash AND a.output_index=o.output_index WHERE ' + selector + ' AND sb.height<=%s AND i.first_generation>%s AND i.first_generation<=%s LIMIT 1', (*params, end, pid, generation)))
    for table in ('epoch_nonces', 'epoch_stakes'):
        out.append(('SELECT 1 FROM chain.' + table + ' WHERE epoch=ANY(%s) AND first_generation>%s AND first_generation<=%s LIMIT 1', (parent['manifest']['required_epochs'], pid, generation)))
    return out


def old_any_row(db, query, params):
    """0.4.5 `any_row`: the statement counted, prepared by psycopg after its fifth execution."""
    counted = 'SELECT count(*) FROM (' + query + ') found'
    with verification(db, counted, params):
        return db.execute(counted, params).fetchone()[0] > 0


def old_check(db, query, params):
    """0.4.5 `check`: the first row, prepared by psycopg after its fifth execution."""
    with verification(db, query, params):
        return db.execute(query, params).fetchone()


def old_coverage(store, generation):
    """0.4.5 delta coverage: the update's transactions against the UNION ALL of the membership."""
    membership, args = store.membership_sql(generation)
    return ('SELECT 1 FROM chain.transactions t JOIN chain.blocks b ON b.hash=t.block_hash LEFT JOIN (' + membership + ') m ON m.hash=b.hash'
            ' WHERE t.first_generation=%s AND (m.hash IS NULL OR b.height>%s OR t.tx_index>=b.tx_count OR t.valid NOT IN (0,1))',
            (*args, generation, store.generation(generation)['target_height']))


def old_height(store, generation):
    """0.4.5 `last_metadata_height` of an update: max() over the UNION ALL of the membership."""
    membership, args = store.membership_sql(generation)
    return 'SELECT max(height) FROM (' + membership + ') m', args


def explained(db):
    """The executed plans of every later statement of `db`'s session, as `auto_explain` reports them."""
    plans = []

    def notice(diagnostic):
        text = diagnostic.message_primary or ''
        if 'plan:' in text:
            plans.append(json.loads(text.split('plan:', 1)[1]))
    db.execute("LOAD 'auto_explain'")
    for name, value in (('log_min_duration', '0'), ('log_analyze', 'on'), ('log_buffers', 'on'), ('log_timing', 'off'),
                        ('log_format', 'json'), ('log_level', 'notice')):
        db.execute('SELECT set_config(%s,%s,false)', ('auto_explain.' + name, value))
    db.add_notice_handler(notice)
    return plans


def buffers(plan):
    node = plan['Plan']
    return node['Shared Hit Blocks'] + node['Shared Read Blocks'] + node.get('Temp Read Blocks', 0)


def insert(db, table, generation, **row):
    db.execute(sql.SQL('INSERT INTO chain.{}({},first_generation) VALUES({},%s)').format(
        sql.Identifier(table), sql.SQL(',').join(map(sql.Identifier, row)), sql.SQL(',').join(sql.Placeholder() * len(row))),
        (*row.values(), generation))


def history(db, count, offset):
    """`count` transactions at generation 0 in the parent's interval, each with a night output spent by the
    next, and `count` blocks of served membership below the start."""
    with db.transaction():
        db.execute("SET LOCAL session_replication_role='replica'")
        tx = "decode(md5('history'||i)||md5('tx'||i),'hex')"
        header = "decode(md5('history'||i)||md5('block'||i),'hex')"
        args = (offset + 1, offset + count)
        db.execute('INSERT INTO chain.transactions(hash,block_hash,tx_index,valid,first_generation) SELECT ' + tx +
                   ',%s,i+10,1,0 FROM generate_series(%s::int,%s::int) i', (block(START)['hash'], *args))
        db.execute('INSERT INTO chain.outputs(tx_hash,output_index,address,value,datum_hash,first_generation) SELECT ' + tx +
                   ",0,'holder',1,NULL,0 FROM generate_series(%s::int,%s::int) i", args)
        db.execute('INSERT INTO chain.output_assets(tx_hash,output_index,policy,name,quantity,ordinal,first_generation) SELECT ' + tx +
                   ',0,%s,%s,1,0,0 FROM generate_series(%s::int,%s::int) i', (*NIGHT, *args))
        db.execute('INSERT INTO chain.inputs(tx_hash,origin_hash,origin_index,ordinal,first_generation) SELECT ' + tx +
                   ",decode(md5('history'||(i-1))||md5('tx'||(i-1)),'hex'),0,0,0 FROM generate_series(%s::int,%s::int) i", (max(offset + 1, 2), offset + count))
        db.execute('INSERT INTO chain.blocks(hash,height,parent_hash,epoch,slot,time,tx_count,first_generation) SELECT ' + header +
                   ",%s-10-i,NULL,0,0,'2026-01-01',0,0 FROM generate_series(%s::int,%s::int) i", (START, *args))
        db.execute('INSERT INTO chain.current_membership SELECT %s-10-i,' + header + ' FROM generate_series(%s::int,%s::int) i', (START, *args))


def pages(db, table):
    return db.execute('SELECT pg_relation_size(%s::regclass)/8192', ('chain.' + table,)).fetchone()[0]


class PriorAnswerEqualityTests(unittest.TestCase):
    """The rewritten statements give the 0.4.5 answer, statement by statement, on hits and misses of every branch."""

    @classmethod
    def setUpClass(cls):
        cls.admin = psycopg.connect(os.environ['DB_SYNC_ALT_TEST_DSN'], autocommit=True)
        cls.name = 'db_sync_alt_test_' + uuid.uuid4().hex[:12]
        cls.admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(cls.name)))
        cls.dsn = make_conninfo(os.environ['DB_SYNC_ALT_TEST_DSN'], dbname=cls.name)
        make_store(cls.dsn)
        with PgStore(conninfo=cls.dsn) as store:
            upgrade_store(store)

    @classmethod
    def tearDownClass(cls):
        cls.admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(cls.name)))
        cls.admin.close()

    def answers(self, *rows):
        """Which statements find a row (old, new, refusal) once `rows` are stored, generation 3 pending on 2; rolled back."""
        with PgStore(conninfo=self.dsn) as store, store.db.transaction(force_rollback=True):
            db = store.db
            parent = store.generation(2)
            db.execute("SET LOCAL session_replication_role='replica'")
            db.execute("INSERT INTO chain.generations VALUES(%s,2,'pending','blockfrost-cardano',%s,%s,%s,%s,%s)",
                       (NEW, parent['start_height'], parent['start_index'], parent['target_height'] + 1, parent['witness_height'] + 1, Jsonb({})))
            for statement in rows:
                if isinstance(statement, str):
                    db.execute(statement)
                else:
                    insert(db, statement[0], statement[1], **statement[2])
            old = [db.execute('SELECT EXISTS(' + q + ')', p).fetchone()[0] for q, p in old_prior_answer_checks(store, NEW)]
            new = [any_row(db, q, p) for _, q, p in prior_answer_checks(store, NEW)]
            try:
                validate_prior_answers(store, NEW); refusal = None
            except Refusal as exc:
                refusal = exc.code
        return old, new, refusal

    def same(self, expected, *rows):
        old, new, refusal = self.answers(*rows)
        hits = {i for i, hit in enumerate(new) if hit}
        self.assertEqual(new, old)
        self.assertEqual(hits, set(expected))
        codes = ['historical_selected_event_changed'] * 2 + ['historical_state_changed'] * 6 + ['historical_epoch_changed'] * 2
        self.assertEqual(refusal, codes[min(hits)] if hits else None)

    @staticmethod
    def tx(name, generation, block_hash=INSIDE, index=10):
        return ('transactions', generation, {'hash': h(name), 'block_hash': block_hash, 'tx_index': index, 'valid': 1})

    @staticmethod
    def out(name, generation, address='holder', datum=None, index=0):
        return ('outputs', generation, {'tx_hash': h(name), 'output_index': index, 'address': address, 'value': '1', 'datum_hash': datum})

    @staticmethod
    def asset(name, generation, unit=NIGHT, quantity='1', index=0):
        return ('output_assets', generation, {'tx_hash': h(name), 'output_index': index, 'policy': unit[0], 'name': unit[1], 'quantity': quantity, 'ordinal': 0})

    @staticmethod
    def spend(name, origin, generation):
        return ('inputs', generation, {'tx_hash': h(name), 'origin_hash': h(origin), 'origin_index': 0, 'ordinal': 0})

    def membership(self, reuse, *rows):
        """Update coverage and metadata height of pending generation 3 (target START+4, metadata reused through
        `reuse`), 0.4.5 and shipped, once `rows` are stored; rolled back."""
        with PgStore(conninfo=self.dsn) as store, store.db.transaction(force_rollback=True):
            db = store.db
            db.execute("SET LOCAL session_replication_role='replica'")
            db.execute("INSERT INTO chain.generations VALUES(%s,2,'pending','blockfrost-cardano',%s,%s,%s,%s,%s)",
                       (NEW, *PROFILE.start, START + 4, START + 2164, Jsonb({})))
            db.execute("INSERT INTO chain.generation_reuse VALUES(%s,%s,'{}'::jsonb)", (NEW, reuse))
            for statement in rows:
                if isinstance(statement, tuple) and isinstance(statement[0], str) and statement[0].startswith('INSERT'):
                    db.execute(*statement)
                else:
                    insert(db, statement[0], statement[1], **statement[2])
            query, params = old_coverage(store, NEW)
            old = 'transaction_coverage_invalid' if db.execute('SELECT EXISTS(' + query + ')', params).fetchone()[0] else None
            try:
                verify_fact_placement(store, store.generation(NEW), delta=True); new = None
            except Refusal as exc:
                new = exc.code
            heights = (db.execute(*old_height(store, NEW)).fetchone()[0], store.last_metadata_height(NEW))
        return old, new, heights

    def test_update_membership_lookups(self):
        x = h('x')
        new_block = lambda height: ('blocks', NEW, {'hash': x, 'height': height, 'parent_hash': None, 'epoch': 2, 'slot': height * 20, 'time': '2026-01-01', 'tx_count': 2})
        bind = lambda height, header: ('INSERT INTO chain.membership_delta VALUES(%s,%s,%s)', (NEW, height, header))
        for expected, reuse, rows in (
                (None, START + 2, [self.tx('t', NEW, block(START)['hash'], 3)]),
                ('transaction_coverage_invalid', START - 1, [self.tx('t', NEW, block(START)['hash'], 3)]),
                (None, START - 1, [self.tx('t', NEW, block(START)['hash'], 3), bind(START, block(START)['hash'])]),
                ('transaction_coverage_invalid', START + 2, [new_block(START + 3), self.tx('t', NEW, x, 0)]),
                (None, START + 2, [new_block(START + 3), bind(START + 3, x), self.tx('t', NEW, x, 0)]),
                ('transaction_coverage_invalid', START + 2, [new_block(START + 5), bind(START + 5, x), self.tx('t', NEW, x, 0)]),
                ('transaction_coverage_invalid', START + 2, [self.tx('t', NEW, block(START)['hash'], 7)])):
            with self.subTest(expected=expected, reuse=reuse - START, rows=len(rows)):
                old, new, (old_top, new_top) = self.membership(reuse, *rows)
                self.assertEqual((new, new_top), (old, old_top))
                self.assertEqual(new, expected)

    def test_the_statements_line_up(self):
        with PgStore(conninfo=self.dsn) as store, store.db.transaction(force_rollback=True):
            store.db.execute("INSERT INTO chain.generations VALUES(%s,2,'pending','blockfrost-cardano',%s,%s,%s,%s,%s)",
                             (NEW, *PROFILE.start, START + 3, START + 2163, Jsonb({})))
            self.assertEqual(len(prior_answer_checks(store, NEW)), len(old_prior_answer_checks(store, NEW)))
            self.assertEqual(len(prior_answer_checks(store, NEW)), 10)
            self.assertEqual(prior_answer_checks(store, 0), [])

    def test_nothing_new_and_parent_facts(self):
        self.same(())
        # Facts of the parent itself are not new.
        self.same((), self.tx('p', 2), self.out('p', 2), self.asset('p', 2), self.tx('q', 2, index=11), self.spend('q', 'p', 2))

    def test_creation_branches(self):
        self.same({0}, self.tx('a', 0), self.out('a', 0), self.asset('a', NEW))
        self.same({0}, self.tx('a', 0), self.out('a', NEW), self.asset('a', 0))
        self.same({0}, self.tx('a', NEW), self.out('a', 0), self.asset('a', 0))
        self.same({0}, self.tx('a', 0), self.out('a', 0, MAPPING, h('d')), self.asset('a', NEW, (AUTH, b'')))
        # The start cursor's own position is inside, the one before it is not.
        self.same({0}, self.out('spend', NEW, index=1), self.asset('spend', NEW, index=1))
        self.same((), self.tx('a', 0, block(START)['hash'], 3), self.out('a', 0), self.asset('a', NEW))

    def test_creation_misses(self):
        self.same((), self.tx('a', 0), self.out('a', 0, MAPPING, h('d')), self.asset('a', NEW, (AUTH, b''), '2'))
        self.same((), self.tx('a', 0), self.out('a', 0, MAPPING), self.asset('a', NEW, (AUTH, b'')))
        self.same((), self.tx('a', 0), self.out('a', 0), self.asset('a', NEW, (b'o' * 28, b'')))
        self.same((), self.tx('a', 0, ABOVE), self.out('a', 0), self.asset('a', NEW))
        self.same((), self.tx('a', NEW, ABOVE), self.out('a', NEW), self.asset('a', NEW))

    def test_spending_branches(self):
        night = [self.tx('o', 0), self.out('o', 0), self.asset('o', 0), self.tx('s', 0, index=11)]
        self.same({1}, *night, self.spend('s', 'o', NEW))
        # A spent output gains a night entry: inside the interval it is also a new creation.
        self.same({0, 1}, self.tx('o', 0), self.out('o', 0), self.tx('s', 0, index=11), self.spend('s', 'o', 0), self.asset('o', NEW))
        self.same({1}, self.tx('o', 0, BEFORE, 5), self.out('o', 0), self.tx('s', 0, index=11), self.spend('s', 'o', 0), self.asset('o', NEW))
        self.same({1}, self.tx('o', 0), self.out('o', 0, MAPPING, h('d')), self.tx('s', 0, index=11), self.spend('s', 'o', NEW))

    def test_spending_misses(self):
        self.same((), self.tx('o', 0), self.out('o', 0, MAPPING), self.tx('s', 0, index=11), self.spend('s', 'o', NEW))
        self.same((), self.tx('o', 0), self.out('o', 0), self.tx('s', 0, index=11), self.spend('s', 'o', NEW))
        self.same((), self.tx('o', 0), self.out('o', 0), self.asset('o', 0), self.tx('s', NEW, ABOVE), self.spend('s', 'o', NEW))
        self.same((), self.tx('o', 0, BEFORE, 5), self.out('o', 0), self.tx('s', 0, ABOVE), self.spend('s', 'o', 0), self.asset('o', NEW))

    def test_state_branches(self):
        for held, spent, (address, policy) in ((2, 3, ('holder', PERMISSIONED)), (4, 5, COUNCIL), (6, 7, COMMITTEE)):
            with self.subTest(policy=policy.hex()):
                unit = (policy, b'')
                self.same({held}, self.tx('a', 0), self.out('a', 0, address), self.asset('a', NEW, unit))
                self.same({held}, self.tx('a', 0), self.out('a', NEW, address), self.asset('a', 0, unit))
                # State checks have no start cursor: anything at or below the parent's target.
                self.same({held}, self.tx('a', 0, BEFORE, 5), self.out('a', 0, address), self.asset('a', NEW, unit))
                self.same((), self.tx('a', 0, ABOVE), self.out('a', 0, address), self.asset('a', NEW, unit))
                self.same({spent}, self.tx('a', 0), self.out('a', 0, address), self.asset('a', 0, unit), self.tx('s', 0, index=11), self.spend('s', 'a', NEW))
                self.same((), self.tx('a', 0), self.out('a', 0, address), self.asset('a', 0, unit), self.tx('s', NEW, ABOVE), self.spend('s', 'a', NEW))
        # Governance selectors also need the governance address.
        self.same((), self.tx('a', 0), self.out('a', 0, 'holder'), self.asset('a', NEW, (COUNCIL[1], b'')))

    def test_epoch_branches(self):
        self.same({8}, 'DELETE FROM chain.epoch_nonces WHERE epoch=1',
                  ('epoch_nonces', NEW, {'epoch': 1, 'nonce': h('n'), 'max_tx_size': 1}))
        self.same({9}, ('epoch_stakes', NEW, {'epoch': 0, 'pool_hash': b'q' * 28, 'amount': '1'}))
        self.same((), ('epoch_stakes', NEW, {'epoch': 7, 'pool_hash': b'q' * 28, 'amount': '1'}))


class UpdateBoundedTests(unittest.TestCase):
    """Many small updates on a long history, checked on one connection as the follow does."""

    def setUp(self):
        self.admin = psycopg.connect(os.environ['DB_SYNC_ALT_TEST_DSN'], autocommit=True)
        self.addCleanup(self.admin.close)
        name = 'db_sync_alt_test_' + uuid.uuid4().hex[:12]
        self.admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
        self.addCleanup(self.admin.execute, sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(name)))
        self.dsn = make_conninfo(os.environ['DB_SYNC_ALT_TEST_DSN'], dbname=name)
        make_store(self.dsn)
        with PgStore(conninfo=self.dsn) as store:
            upgrade_store(store)
        self.fixture = psycopg.connect(self.dsn, autocommit=True)
        self.addCleanup(self.fixture.close)
        history(self.fixture, HISTORY, 0)
        # As after a bulk load: the statistics know generation 0 only.
        self.fixture.execute('ANALYZE')

    def begin(self, generation):
        """Store update `generation` as pending on the canonical generation: one new block above the
        membership with 2 transactions, their night outputs, and inputs spending the previous update's."""
        db = self.fixture
        with db.transaction():
            db.execute("SET LOCAL session_replication_role='replica'")
            parent = db.execute('SELECT canonical_ready FROM chain.service_state').fetchone()[0]
            top = db.execute('SELECT max(height) FROM chain.current_membership').fetchone()[0]
            manifest = db.execute('SELECT manifest FROM chain.generations WHERE id=2').fetchone()[0]
            db.execute("INSERT INTO chain.generations VALUES(%s,%s,'pending','blockfrost-cardano',%s,%s,%s,%s,%s)",
                       (generation, parent, *PROFILE.start, top + 1, top + 1, Jsonb(manifest)))
            header = h('update-block-%d' % generation)
            db.execute("INSERT INTO chain.blocks(hash,height,parent_hash,epoch,slot,time,tx_count,first_generation)"
                       " VALUES(%s,%s,NULL,2,%s,'2026-01-01',2,%s)", (header, top + 1, (top + 1) * 20, generation))
            db.execute("INSERT INTO chain.generation_reuse VALUES(%s,%s,'{}'::jsonb)", (generation, top))
            db.execute('INSERT INTO chain.membership_delta VALUES(%s,%s,%s)', (generation, top + 1, header))
            for k in range(2):
                tx = h('update-tx-%d-%d' % (generation, k))
                insert(db, 'transactions', generation, hash=tx, block_hash=header, tx_index=k, valid=1)
                insert(db, 'outputs', generation, tx_hash=tx, output_index=0, address='holder', value='1', datum_hash=None)
                insert(db, 'output_assets', generation, tx_hash=tx, output_index=0, policy=NIGHT[0], name=NIGHT[1], quantity='1', ordinal=0)
                if parent > 2:
                    insert(db, 'inputs', generation, tx_hash=tx, origin_hash=h('update-tx-%d-%d' % (parent, k)), origin_index=0, ordinal=0)

    def publish(self, generation):
        db = self.fixture
        with db.transaction():
            db.execute("SET LOCAL session_replication_role='replica'")
            db.execute("UPDATE chain.generations SET status='ready' WHERE id=%s", (generation,))
            db.execute('UPDATE chain.service_state SET canonical_ready=%s,publication_serial=publication_serial+1', (generation,))
            db.execute('UPDATE chain.integrity_state SET projection_generation=%s', (generation,))
            db.execute('INSERT INTO chain.current_membership SELECT height,hash FROM chain.membership_delta WHERE generation=%s', (generation,))

    def updates(self, store, plans, generations, old):
        """Check and publish each update; the most buffers per statement, for the shipped checks and the 0.4.5 ones."""
        new, reference = {}, {}
        for generation in generations:
            self.begin(generation)
            plans.clear()
            g = store.generation(generation)
            verify_fact_placement(store, g, delta=True)
            validate_prior_answers(store, generation)
            self.assertEqual(store.last_metadata_height(generation), g['target_height'])
            for table in store.sealed_tables(generation):
                self.assertEqual(len(list(delta_rows(store, table, generation))), {'blocks': 1, 'transactions': 2, 'outputs': 2, 'output_assets': 2}.get(table, 2 if table == 'inputs' and g['parent'] > 2 else 0))
            for plan in plans:
                new[plan['Query Text']] = max(new.get(plan['Query Text'], 0), buffers(plan))
            creation, spending = old_prior_answer_checks(store, generation)[:2]
            child, parent, join = DEPENDENCIES[3]
            dependency = ('SELECT 1 FROM chain.' + child + ' c JOIN chain.' + parent + ' p ON ' + join +
                          ' WHERE c.first_generation=%s AND p.first_generation>c.first_generation LIMIT 1', (generation,))
            for label, run, (query, params) in (('creation', old_any_row, creation), ('spending', old_any_row, spending),
                                                ('dependency', old_check, dependency), ('coverage', old_any_row, old_coverage(store, generation)),
                                                ('height', lambda db, q, p: db.execute(q, p).fetchone(), old_height(store, generation))):
                old[1].clear()
                answer = run(old[0], query, params)
                reference.setdefault(label, []).append(max(buffers(plan) for plan in old[1] if 'chain.' in plan['Query Text']))
                if label == 'height':
                    self.assertEqual(answer[0], g['target_height'])
            self.publish(generation)
        return new, reference

    def test_update_checks_read_the_update_not_the_history(self):
        with PgStore(conninfo=self.dsn) as store, psycopg.connect(self.dsn, autocommit=True) as old:
            plans, old_plans = explained(store.db), explained(old)
            floors = [{t: pages(self.fixture, t) for t in ('outputs', 'current_membership')}]
            first, old_first = self.updates(store, plans, range(NEW, NEW + UPDATES), (old, old_plans))
            history(self.fixture, 2 * HISTORY, HISTORY)
            floors.append({t: pages(self.fixture, t) for t in ('outputs', 'current_membership')})
            second, old_second = self.updates(store, plans, range(NEW + UPDATES, NEW + 2 * UPDATES), (old, old_plans))
        print(json.dumps({'pages': floors, 'shipped': {q[:100]: (first.get(q), second.get(q)) for q in sorted(set(first) | set(second))},
                          'reference': [old_first, old_second]}, indent=1))
        # Every statement of the shipped checks, on every update, before and after the history tripled.
        for query in set(first) | set(second):
            with self.subTest(query=query[:120]):
                self.assertLessEqual(max(first.get(query, 0), second.get(query, 0)), BOUND)
                if query in first and query in second:
                    self.assertLessEqual(second[query], first[query] + GROWTH)
        self.assertTrue(any('chain.output_assets changed' in q for q in first), 'the spending check ran')
        self.assertTrue(any(q.startswith('SELECT greatest(') for q in first), 'the metadata height ran')
        # Negative control on the same store: the 0.4.5 creation and spending statements read the history on every
        # update, its dependency check once PostgreSQL switches to the generic plan (from the 11th execution on one
        # connection), and its coverage check and metadata height read the whole membership. All grow with the history.
        for reference, floor, prepared in zip((old_first, old_second), floors, (slice(10, None), slice(None))):
            for label, table in (('creation', 'outputs'), ('spending', 'outputs'), ('coverage', 'current_membership'), ('height', 'current_membership')):
                self.assertGreater(min(reference[label]), floor[table], label)
            self.assertGreater(min(reference['dependency'][prepared]), floor['outputs'])
        self.assertLessEqual(max(old_first['dependency'][:10]), BOUND)
        for label in ('creation', 'spending', 'dependency', 'coverage', 'height'):
            self.assertGreater(min(old_second[label]), 2 * min(old_first[label][10:]), label)

if __name__ == '__main__':
    unittest.main()
