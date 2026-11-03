"""Typed PostgreSQL client that talks to an endpoint the way midnight-node's SQLx driver does.

Each statement is prepared once per connection under a generated name with explicit parameter type
OIDs (Parse), described (Describe statement), then executed with binary parameters and binary
result columns (Bind/Execute). Parameter values are encoded byte for byte as SQLx encodes the Rust
types the node binds (i16/i32/i64, &str, &[u8], NaiveDateTime, Vec<[u8; N]>). Statements the
driver sends as simple queries (multi-statement SET) use the simple query protocol.

Every wait has a deadline: a stalled endpoint becomes a `timeout` error, never a hang.
"""
import datetime as dt
import select
import struct
import time

from psycopg import pq
from psycopg.adapt import Transformer

OIDS = {'int2': 21, 'int4': 23, 'int8': 20, 'oid': 26, 'text': 25, 'bytea': 17, 'timestamp': 1114, 'bytea[]': 1001}
POSTGRES_EPOCH = dt.datetime(2000, 1, 1)


class QueryError(Exception):
    """A failed exchange. kind: connection | statement | timeout; sqlstate when the server sent one."""
    def __init__(self, kind, sqlstate=None, message=''):
        self.kind, self.sqlstate, self.message = kind, sqlstate, message
        super().__init__(kind + (':' + sqlstate if sqlstate else ''))

    def summary(self):
        return {'error': self.kind, **({'sqlstate': self.sqlstate} if self.sqlstate else {})}


def encode(kind, value):
    """Binary wire value of one parameter, as SQLx encodes the matching Rust type."""
    if kind == 'int2':
        return struct.pack('!h', value)
    if kind == 'int4':
        return struct.pack('!i', value)
    if kind == 'int8':
        return struct.pack('!q', value)
    if kind == 'oid':
        return struct.pack('!I', value)
    if kind == 'text':
        return value.encode()
    if kind == 'bytea':
        return bytes(value)
    if kind == 'timestamp':
        if value.tzinfo is not None:
            raise ValueError('naive timestamp required')
        delta = value - POSTGRES_EPOCH
        return struct.pack('!q', (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds)
    if kind == 'bytea[]':
        items = [bytes(v) for v in value]
        out = struct.pack('!iiI', 1, 0, OIDS['bytea']) + struct.pack('!ii', len(items), 1)
        return out + b''.join(struct.pack('!i', len(v)) + v for v in items)
    raise ValueError('unknown parameter type ' + kind)


class Result:
    def __init__(self, columns, rows, command):
        self.columns, self.rows, self.command = columns, rows, command


class TypedClient:
    def __init__(self, conninfo, *, timeout=30.0):
        self.conninfo, self.timeout = conninfo, timeout
        self.conn = None
        self.statements = {}
        self.transformer = Transformer()

    def connect(self):
        """Asynchronous libpq connection setup under the same deadline as a query."""
        self.close()
        conn = pq.PGconn.connect_start(self.conninfo.encode())
        deadline = time.monotonic() + self.timeout
        try:
            while True:
                status = conn.connect_poll()
                if status == pq.PollingStatus.OK:
                    break
                if status == pq.PollingStatus.FAILED:
                    raise QueryError('connection', message=_short(conn.error_message.decode(errors='replace')))
                left = deadline - time.monotonic()
                sock = conn.socket
                ready = select.select([sock] if status == pq.PollingStatus.READING else [],
                                      [sock] if status == pq.PollingStatus.WRITING else [], [], max(left, 0))
                if left <= 0 or not (ready[0] or ready[1]):
                    raise QueryError('timeout')
        except BaseException:
            conn.finish()
            raise
        conn.nonblocking = 1
        self.conn, self.statements = conn, {}
        return self

    def close(self):
        if self.conn is not None:
            self.conn.finish()
        self.conn, self.statements = None, {}

    @property
    def connected(self):
        return self.conn is not None and self.conn.status == pq.ConnStatus.OK

    def parameter(self, name):
        value = self.conn.parameter_status(name.encode()) if self.connected else None
        return value.decode() if value is not None else None

    def _wait(self, deadline, write=False):
        left = deadline - time.monotonic()
        if left <= 0:
            raise TimeoutError
        sock = self.conn.socket
        readable, writable, _ = select.select([] if write else [sock], [sock] if write else [], [], left)
        if not readable and not writable:
            raise TimeoutError

    def _exchange(self, send):
        if not self.connected:
            self.connect()
        deadline = time.monotonic() + self.timeout
        try:
            send()
            while self.conn.flush():
                self._wait(deadline, write=True)
            results = []
            while True:
                self.conn.consume_input()
                while self.conn.is_busy():
                    self._wait(deadline)
                    self.conn.consume_input()
                result = self.conn.get_result()
                if result is None:
                    break
                results.append(result)
        except TimeoutError:
            self.close()
            raise QueryError('timeout') from None
        except Exception as error:
            if isinstance(error, QueryError):
                raise
            self.close()
            raise QueryError('connection', message=_short(str(error))) from None
        if self.conn.status != pq.ConnStatus.OK:
            failed = next((r for r in results if r.status == pq.ExecStatus.FATAL_ERROR), None)
            self.close()
            raise QueryError('connection', _sqlstate(failed) if failed else None)
        for result in results:
            if result.status == pq.ExecStatus.FATAL_ERROR:
                raise QueryError('statement', _sqlstate(result), _short(result.error_message.decode(errors='replace')))
        return results

    def prepare(self, sql, types):
        key = (sql, tuple(types))
        if key not in self.statements:
            name = ('sqlx_s_' + str(len(self.statements) + 1)).encode()
            oids = [OIDS[t] for t in types]
            self._exchange(lambda: self.conn.send_prepare(name, sql.encode(), oids))
            self.statements[key] = name
            self._exchange(lambda: self.conn.send_describe_prepared(name))
        return self.statements[key]

    def query(self, sql, types, values):
        """Prepared statement with binary parameters and binary results."""
        if len(types) != len(values):
            raise ValueError('parameter count')
        name = self.prepare(sql, types)
        params = [encode(t, v) for t, v in zip(types, values)]
        results = self._exchange(lambda: self.conn.send_query_prepared(name, params, [1] * len(params), 1))
        return self._decode(results[-1], pq.Format.BINARY)

    def simple(self, sql):
        """Simple query protocol (text results), for statements SQLx sends that way."""
        results = self._exchange(lambda: self.conn.send_query(sql.encode()))
        return self._decode(results[-1], pq.Format.TEXT)

    def _decode(self, result, fmt):
        columns = [(result.fname(i).decode(), result.ftype(i)) for i in range(result.nfields)]
        loaders = [self.transformer.get_loader(oid, fmt) for _, oid in columns]
        rows = []
        for r in range(result.ntuples):
            row = {}
            for c, (name, _) in enumerate(columns):
                raw = result.get_value(r, c)
                row[name] = None if raw is None else loaders[c].load(raw)
            rows.append(row)
        status = result.command_status
        return Result(columns, rows, status.decode() if status else '')


def _sqlstate(result):
    value = result.error_field(pq.DiagnosticField.SQLSTATE) if result is not None else None
    return value.decode() if value else None


def _short(text):
    # Server and libpq messages can name hosts or roles; keep only a bounded first line.
    return (text or '').strip().splitlines()[0][:160] if text else ''
