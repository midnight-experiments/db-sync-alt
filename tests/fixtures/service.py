"""Service fakes (runtime with a reader check), configuration and HTTP helpers."""
import http.client
import json
import random
import socket
import threading
import time
import uuid
from db_sync_alt.service.health import ChainPoint, HealthPolicy
from db_sync_alt.service.runtime import (DatabaseObservation, LifecycleFailure, ServiceConfig)


READER = {'role': 'node_reader', 'pg_service': 'test_reader'}
REPORT = {'role': 'node_reader', 'privileges': 'node_relations_only',
          'settings': {'statement_timeout': '60s', 'idle_in_transaction_session_timeout': '60s', 'search_path': 'public',
                       'default_transaction_read_only': None, 'connection_limit': 40}, 'warnings': []}


def real_reader(admin, dsn, servicefile, *, settings=True):
    """An operator-created reader login (as the test superuser `admin`) with a libpq service entry for
    `dsn` appended to `servicefile`; returns the service's reader configuration. Drop the role after
    the database: `DROP ROLE IF EXISTS <role>`."""
    from psycopg import sql
    from psycopg.conninfo import conninfo_to_dict
    role = 'node_reader_' + uuid.uuid4().hex[:10]
    admin.execute(sql.SQL('CREATE ROLE {} LOGIN NOINHERIT CONNECTION LIMIT 40').format(sql.Identifier(role)))
    if settings:
        for name, value in (('statement_timeout', '60s'), ('idle_in_transaction_session_timeout', '60s'), ('search_path', 'public')):
            admin.execute(sql.SQL('ALTER ROLE {} SET {} = {}').format(sql.Identifier(role), sql.Identifier(name), sql.Literal(value)))
    values = dict(conninfo_to_dict(dsn), user=role)
    with open(servicefile, 'a') as out:
        out.write('[' + role + ']\n' + ''.join(k + '=' + str(v) + '\n' for k, v in values.items()) + '\n')
    return {'role': role, 'pg_service': role}


def point(height):
    return ChainPoint(height, f'{height:064x}', float(height * 20))

def config(**overrides):
    # Reserve no host port; tests bind a random available internal high port.
    with socket.socket() as sock:
        for _ in range(30):
            port = random.randint(10001, 60000)
            try:
                sock.bind(('127.0.0.1', port))
                break
            except OSError:
                pass
        else:
            raise RuntimeError('test_port_unavailable')
    return ServiceConfig(pg_service='test', reader=READER, network='preprod', http_port=port,
        policy=HealthPolicy(poll_seconds=0.02, heartbeat_seconds=0.01,
                            provider_operation_seconds=0.08), shutdown_seconds=0.3, **overrides)

class Store:
    def __init__(self, runtime):
        self.runtime = runtime
        self.owner = threading.get_ident()

    def close(self):
        if threading.get_ident() != self.owner:
            raise AssertionError('writer connection closed from a different thread')
        self.runtime.closed += 1
        self.runtime.writer_lock.release()

class Runtime:
    def __init__(self):
        self.writer_lock = threading.Lock()
        self.inspection = {'empty': False, 'migration_state': 'ready', 'revoked': False}
        self.target = 1000
        self.revoked = False
        self.reachable = True
        self.closed = self.restores = self.verifications = 0
        self.block_inspect = self.block_observe = None
        self.inspect_error = self.restore_error = self.verify_error = self.bind_error = None
        self.reader_working = True
        self.bound = 0

    def inspect(self):
        if self.block_inspect:
            self.block_inspect.wait(2)
        if self.inspect_error:
            raise self.inspect_error
        return self.inspection.copy()

    def restore(self):
        self.restores += 1
        if self.restore_error:
            raise self.restore_error
        self.inspection = {'empty': False, 'migration_state': 'ready', 'revoked': False}

    def open_writer(self, health):
        if not self.writer_lock.acquire(blocking=False):
            raise LifecycleFailure('writer_busy')
        return Store(self)

    def writer_check(self,store,timeout):
        if threading.get_ident()!=store.owner:raise AssertionError('writer check crossed thread')

    def verify(self, store):
        self.verifications += 1
        if self.verify_error:
            raise self.verify_error

    def observe(self):
        if self.block_observe:
            self.block_observe.wait(2)
        if not self.reachable:
            raise RuntimeError('postgres://username:secret@private-directory')
        return DatabaseObservation(point(self.target), point(self.target + 2160), revoked=self.revoked)

    def database_check(self, timeout):
        return self.reachable

    def bind_reader(self, store):
        if threading.get_ident() != store.owner:
            raise AssertionError('reader bound off the writer thread')
        if self.bind_error:
            raise self.bind_error
        self.bound += 1
        return dict(REPORT)

    def reader_check(self):
        if isinstance(self.reader_working, Exception):
            raise self.reader_working
        return self.bound > 0 and self.reader_working

def eventually(predicate, seconds=1):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.005)
    raise AssertionError('condition did not become true')

def get(service, path):
    connection = http.client.HTTPConnection(*service.http.server_address, timeout=2)
    try:
        connection.request('GET', path)
        response = connection.getresponse()
        return response.status, json.loads(response.read())
    finally:
        connection.close()
