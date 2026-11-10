# SPDX-License-Identifier: Apache-2.0
"""Counted Blockfrost transport for one network. All callers sharing credentials must share one root.

The network profile gives the API host and the key prefix: a key of another
network is refused before any request. No retries or redirect following. The exclusive flock covers durable reservation,
transport and completion; a killed process leaves its reservation spent.
"""
import contextlib
import datetime as dt
import fcntl
import hashlib
import http.client
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import stat
import tempfile
import time
from urllib.parse import unquote, urlsplit

MAX_BYTES = 32 * 1024 * 1024
# Arrays and objects nested deeper than this in a provider answer are cut out. Python's JSON decoder and
# encoder recurse in C and fail between 5,000 and 10,000 levels, and anyone can put metadata that deep on
# chain. Every value cardano-db-sync stores as JSON (nesting up to 2,000) arrives whole.
RESPONSE_DEPTH = 4000
_STRUCTURE = re.compile(r'"(?:[^"\\]|\\.)*"|[\[\]{}"]')
_PAIRS = {'[': ']', '{': '}'}
# Stands in for a cut container while the shallow rest is decoded. U+0000 never occurs in provider JSON
# (PostgreSQL's jsonb cannot hold it).
_CUT_KEY = '\x00db-sync-alt:nested-too-deep'


class TooDeep(dict):
    """An array or object nested deeper than RESPONSE_DEPTH in a provider answer: cut, empty."""


def _object(pairs):
    return TooDeep() if pairs == [(_CUT_KEY, True)] else dict(pairs)


def loads(raw):
    """A provider answer as `json.loads` decodes it, except that every array or object nested deeper
    than RESPONSE_DEPTH is a `TooDeep`. Malformed JSON raises ValueError, as `json.loads` does; no input
    raises RecursionError."""
    text = raw.decode('utf-8') if isinstance(raw, (bytes, bytearray)) else raw
    if text.count('[') + text.count('{') <= RESPONSE_DEPTH:
        return json.loads(raw)  # cannot nest deeper: the common answer, decoded as before
    stack, spans, start = [], [], None
    for token in _STRUCTURE.finditer(text):
        value = token.group()
        if value[0] == '"':
            if len(value) == 1:
                raise ValueError('unterminated string')
        elif value in _PAIRS:
            stack.append(value)
            if len(stack) == RESPONSE_DEPTH + 1:
                start = token.start()
        else:
            if not stack or _PAIRS[stack.pop()] != value:
                raise ValueError('unbalanced brackets')
            if len(stack) == RESPONSE_DEPTH and start is not None:
                spans.append((start, token.end()));start = None
    if stack:
        raise ValueError('unbalanced brackets')
    if not spans:
        return json.loads(raw)
    cut, pieces, position = json.dumps({_CUT_KEY: True}), [], 0
    for first, end in spans:
        pieces += [text[position:first], cut];position = end
    pieces.append(text[position:])
    return json.loads(''.join(pieces), object_pairs_hook=_object)


class ProviderError(RuntimeError):
    def __init__(self, code, *, http_status=None, kind=None):
        super().__init__(code)
        self.http_status, self.kind = http_status, kind


def safe_path(path):
    if not isinstance(path, str) or not path.startswith('/') or path.startswith('//'):
        raise ProviderError('invalid_provider_path')
    decoded = unquote(path)
    parsed = urlsplit(path)
    if (parsed.scheme or parsed.netloc or parsed.fragment or any(ord(c)<33 or ord(c)>126 for c in decoded)
            or any(c in decoded for c in ('\\', '#')) or decoded.startswith('//')
            or any(p in ('.', '..') for p in decoded.split('?')[0].split('/'))):
        raise ProviderError('invalid_provider_path')
    return path

def external_module(root):
    try:
        root = Path(root)
        if not root.is_absolute():raise ValueError()
        spec = importlib.util.spec_from_file_location('db_sync_alt_external_provider', root / 'client.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    except Exception:raise ProviderError('shared_client_unavailable') from None

class SharedClient:
    """Opt-in adapter; never copies or modifies external accounting semantics.

    `profile` names the shared client's profile (default: the network name); its
    API base must be the network's.
    """
    def __init__(self, root, profile=None, *, network):
        self.module = external_module(root)
        self.client = self.module.Client(profile or network.network, root=Path(root))
        self.base = self.client.base
        if self.base != network.blockfrost_base:raise ProviderError('wrong_provider_network')
    def status(self):
        try:
            value = self.client.status()
            with self.module.locked(self.client.directory / '.usage.lock'):
                value['cooldown_until'] = self.client._load().get('cooldown_until', 0)
            return value
        except Exception:raise ProviderError('provider_accounting_unavailable') from None
    def get(self, path, purpose='db-sync-alt'):
        safe_path(path)
        try:return self.client.get(path, purpose=purpose)
        except Exception as error:
            text=str(error)
            matched=re.match(r'HTTP (\d{3});',text)
            status=int(matched[1]) if matched else None
            kind='timeout' if text.startswith('TimeoutError;') else 'connection' if text.startswith(('URLError;','ConnectionError;')) else 'cooldown' if text.startswith(('Provider cooldown active','Shared rate-limit cooldown')) else None
            raise ProviderError('provider_request_failed_counted',http_status=status,kind=kind) from None

def _private(path, directory=False):
    info = path.lstat()
    if (stat.S_ISLNK(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077
            or not (stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode))):
        raise ProviderError('unsafe_provider_permissions')

def _atomic(path, value):
    fd, tmp = tempfile.mkstemp(prefix='.ledger-', dir=path.parent)
    try:
        with os.fdopen(fd, 'w') as out:
            json.dump(value, out, allow_nan=False);out.flush();os.fsync(out.fileno())
        os.replace(tmp, path)
        fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:os.fsync(fd)
        finally:os.close(fd)
    finally:
        if os.path.exists(tmp):os.unlink(tmp)

class PortableClient:
    def __init__(self, key, root, *, network, limit=50000, transport=None, clock=time.time, sleep=time.sleep):
        # A Blockfrost project key names its network in its prefix.
        if not network.valid_key(key):
            raise ProviderError('credential_network_mismatch')
        self.host, self.base = network.blockfrost_host, network.blockfrost_base
        if type(limit) is not int or not 1 <= limit <= 50000:raise ProviderError('invalid_daily_limit')
        self.key, self.root, self.limit = key, Path(root), limit
        self.clock, self.sleep, self.transport = clock, sleep, transport or self._https
        self.fingerprint = hashlib.sha256(key.encode()).hexdigest()
        if not self.root.is_absolute():raise ProviderError('absolute_accounting_root_required')
        if any(p.is_symlink() for p in (self.root, *self.root.parents)):raise ProviderError('unsafe_accounting_root')
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        _private(self.root, True)
        self.state = self.root / 'usage.json'
    @contextlib.contextmanager
    def _locked(self):
        fd = os.open(self.root / '.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'r+') as lock:
            _private(self.root / '.lock');fcntl.flock(lock, fcntl.LOCK_EX)
            yield
    def _read(self):
        marker = self.root / '.initialized'
        if not self.state.exists():
            if marker.exists():raise ProviderError('provider_ledger_missing')
            value = {'version':1, 'next_request':0, 'keys':{}}
            _atomic(marker, {'version':1});_atomic(self.state, value)
        try:
            _private(self.state)
            value = json.loads(self.state.read_text())
            if value['version'] != 1 or not isinstance(value['keys'], dict):raise ValueError()
            if type(value['next_request']) not in (int,float) or not math.isfinite(value['next_request']) or value['next_request'] < 0:raise ValueError()
            for fingerprint, entry in value['keys'].items():
                if not re.fullmatch('[0-9a-f]{64}',fingerprint):raise ValueError()
                if type(entry['limit']) is not int or not 1 <= entry['limit'] <= 50000:raise ValueError()
                if type(entry['cooldown']) not in (int,float) or not math.isfinite(entry['cooldown']) or entry['cooldown'] < 0:raise ValueError()
                if not isinstance(entry['days'],dict):raise ValueError()
                for day, count in entry['days'].items():
                    if dt.date.fromisoformat(day).isoformat() != day or type(count) is not int or count < 0:raise ValueError()
            return value
        except Exception:raise ProviderError('provider_ledger_corrupt') from None
    def _entry(self, data):
        entry = data['keys'].setdefault(self.fingerprint, {'limit':self.limit,'cooldown':0,'days':{}})
        entry['limit'] = min(entry['limit'], self.limit)
        return entry
    def _day(self):return dt.datetime.fromtimestamp(self.clock(), dt.timezone.utc).date().isoformat()
    def status(self):
        with self._locked():
            data = self._read();entry = self._entry(data);day = self._day();used = entry['days'].get(day,0)
            _atomic(self.state,data)
            return {'utc_day':day,'budget_used':used,'daily_limit':entry['limit'], 'remaining_budget':max(0,entry['limit']-used), 'exhausted':used>=entry['limit'], 'cooldown_until':max(entry['cooldown'],data['next_request'] if data['next_request'] > self.clock()+1 else 0)}
    def _https(self, path):
        conn = http.client.HTTPSConnection(self.host, timeout=60)
        try:
            conn.request('GET', '/api/v0'+path, headers={'project_id':self.key, 'Accept':'application/json'})
            response = conn.getresponse();raw = response.read(MAX_BYTES+1)
            return response.status, dict(response.getheaders()), raw
        finally:conn.close()
    def get(self, path, purpose='db-sync-alt'):
        safe_path(path)
        if self.key in unquote(path):raise ProviderError('credential_in_path')
        with self._locked():
            data = self._read();entry = self._entry(data)
            if entry['cooldown'] > self.clock():raise ProviderError('provider_cooldown',kind='cooldown')
            delay = data['next_request']-self.clock()
            if delay > 60:raise ProviderError('provider_clock_or_cooldown',kind='cooldown')
            if delay > 0:self.sleep(delay)
            day = self._day();used = entry['days'].get(day,0)
            if used >= entry['limit']:raise ProviderError('daily_budget_exhausted')
            entry['days'][day] = used+1;data['next_request'] = self.clock()+61
            _atomic(self.state, data)  # Reservation survives transport failure/process death.
            try:
                code, headers, raw = self.transport(path)
                if code in (402,418,429):
                    retry = next((v for k,v in headers.items() if k.lower()=='retry-after'), '60')
                    try:delay=max(60,min(86400,int(retry)))
                    except (ValueError,TypeError):delay=60
                    entry['cooldown'] = self.clock() + (3600 if code==418 else delay)
                    if code==402:entry['days'][day] = max(entry['days'][day],entry['limit'])
                    _atomic(self.state,data)
                if code != 200:raise ProviderError('provider_request_failed_counted',http_status=code)
                if len(raw)>MAX_BYTES or self.key.encode() in raw:raise ProviderError('unsafe_provider_response')
                result=loads(raw)
                if self.key in json.dumps(result,ensure_ascii=False):raise ProviderError('unsafe_provider_response')
                if path=='/metrics':
                    if not isinstance(result,list):raise ProviderError('invalid_provider_metrics')
                    updates={}
                    for row in result:
                        if (not isinstance(row,dict) or type(row.get('calls')) is not int or row['calls']<0
                                or type(row.get('time')) is not int or row['time']<0):raise ProviderError('invalid_provider_metrics')
                        date=dt.datetime.fromtimestamp(row['time'],dt.timezone.utc).date().isoformat()
                        updates[date]=max(updates.get(date,0),row['calls']+(1 if date==day else 0))
                    for date,count in updates.items():entry['days'][date]=max(entry['days'].get(date,0),count)
                    _atomic(self.state,data)
                return result
            except ProviderError:raise
            except TimeoutError:raise ProviderError('provider_request_failed_counted',kind='timeout') from None
            except (OSError,http.client.HTTPException):raise ProviderError('provider_request_failed_counted',kind='connection') from None
            except Exception:raise ProviderError('provider_request_failed_counted') from None
            finally:
                data['next_request']=self.clock()+0.125
                _atomic(self.state,data)

def configured_client(network):
    """The accounted client of `network`; shared accounting is selected explicitly on hosts that already have a ledger."""
    try:
        adapter = os.environ.get('DBSYNC_PROVIDER_ADAPTER', 'portable')
        if adapter == 'shared':return SharedClient(os.environ['DBSYNC_SHARED_ROOT'], os.environ.get('DBSYNC_SHARED_PROFILE'), network=network)
        if adapter != 'portable':raise ValueError()
        path=Path(os.environ['DBSYNC_PROVIDER_KEY_FILE']);_private(path)
        return PortableClient(path.read_text().strip(),os.environ['DBSYNC_PROVIDER_STATE'],network=network)
    except Exception:raise ProviderError('provider_configuration_rejected') from None
