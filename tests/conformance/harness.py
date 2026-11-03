"""Run node queries against an endpoint and compare the answers with cardano-db-sync's.

Answers are compared as the corpus captured them: each row is normalized (timestamps ISO without
time zone, integral decimals as strings, bytes as hex, JSON values as canonical JSON), rows are
hashed in result order, or sorted first where the node's query has no total order.

IDs are endpoint-local. The helper statements that return surrogate IDs (asset resolution and the
coarse ID bounds) are run on the endpoint itself, exactly as the node runs them before an event
query, and their values feed that endpoint's event query; they are checked for shape and
consistency, never compared with another endpoint's IDs. A compared answer that contains an ID
column is a harness error, so a surrogate ID can never decide a match.
"""
import collections
import datetime
import decimal
import gzip
import hashlib
import json
import time

from db_sync_alt.contract import Refusal, canonical

from conformance.client import QueryError, TypedClient
from conformance.statements import EVENTS, by_name, parameter_types, statements

MAX_ID = (1 << 63) - 1
BOUND_COLUMNS = ('tx_id!', 'tx_out_id!', 'ma_tx_out_id!', 'tx_in_id!')
# Result columns of each event query's ORDER BY. Rows with equal keys have no defined order in
# cardano-db-sync either: its own answers to these pages vary with the plan (tie order).
TIE_KEYS = {
    'get_asset_creates': ('block_number!: _', 'tx_index_in_block: _', 'utxo_index: _'),
    'get_asset_spends': ('block_number!: _', 'tx_index_in_block: _', 'utxo_index: _'),
    'get_registrations': ('block_number!: _', 'tx_index_in_block: _'),
    'get_deregistrations': ('block_number!: _', 'tx_index_in_block: _'),
}


def canonical_row(value):
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        if value != int(value):
            raise Refusal('nonintegral_numeric')
        return str(value)
    if isinstance(value, (bytes, memoryview)):
        return bytes(value).hex()
    if isinstance(value, dict):
        return {k: canonical_row(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [canonical_row(v) for v in value]
    return value


def encoded_rows(rows, ordered=True):
    encoded = [canonical(canonical_row(row)) for row in rows]
    return encoded if ordered else sorted(encoded)


def digest_encoded(encoded):
    return {'rows': len(encoded), 'sha256': hashlib.sha256(''.join(r + '\n' for r in encoded).encode()).hexdigest()}


def digest(rows, ordered=True):
    return digest_encoded(encoded_rows(rows, ordered))


def id_column(name):
    base = name.split(':')[0].strip().rstrip('!')
    return base == 'id' or base.endswith('_id')


class Endpoint:
    """One node-facing connection; reconnects after a lost connection like a pooled client."""
    def __init__(self, conninfo, *, timeout=30.0):
        self.client = TypedClient(conninfo, timeout=timeout)
        self.calls = collections.Counter()

    def run(self, entry, values):
        self.calls[entry['name']] += 1
        return self.client.query(entry['sql'], parameter_types(entry), values)

    def simple(self, sql):
        self.calls['driver'] += 1
        return self.client.simple(sql)

    def close(self):
        self.client.close()


def bounds(endpoint, name, height):
    result = endpoint.run(by_name(name), [height])
    if len(result.rows) != 1 or [c for c, _ in result.columns] != list(BOUND_COLUMNS):
        raise Refusal('coarse_bound_missing')
    values = [result.rows[0][c] for c in BOUND_COLUMNS]
    if any(type(v) is not int or not 0 <= v <= MAX_ID for v in values):
        raise Refusal('coarse_bound_value')
    return values


def ident(endpoint, unit):
    result = endpoint.run(by_name('resolve_ident'), [bytes.fromhex(unit[:56]), bytes.fromhex(unit[56:])])
    if len(result.rows) != 1 or list(result.rows[0]) != ['id'] or type(result.rows[0]['id']) is not int:
        raise Refusal('asset_resolution')
    return result.rows[0]['id']


def event_arguments(endpoint, profile, case):
    name, start, end = case['name'], case['start'], case['end']
    limit, offset = case.get('limit', 128), case.get('offset', 0)
    low = bounds(endpoint, 'get_low_bounds', start[0])
    high = bounds(endpoint, 'get_high_bounds', end[0])
    if any(lo > hi for lo, hi in zip(low, high)):
        raise Refusal('coarse_bounds_inverted')
    if name in ('get_asset_creates', 'get_asset_spends'):
        values = [ident(endpoint, profile.night), *start, *end, limit, offset, low[0], high[0]]
        return values + ([low[1], high[1], low[2], high[2]] if name == 'get_asset_creates' else [low[3], high[3]])
    if name == 'get_registrations':
        return [profile.mapping, ident(endpoint, profile.auth), *start, *end, limit, offset, low[0], high[0], low[1], high[1], low[2], high[2]]
    return [profile.mapping, *start, *end, limit, offset, low[0], high[0], low[3], high[3]]


def plain_arguments(endpoint, case):
    values = []
    for value in case.get('args', []):
        if isinstance(value, dict) and 'asset' in value:
            values.append(ident(endpoint, value['asset']))
        elif isinstance(value, dict) and 'bytes' in value:
            values.append(bytes.fromhex(value['bytes']))
        elif isinstance(value, dict) and 'timestamp' in value:
            values.append(datetime.datetime.fromisoformat(value['timestamp'].replace('Z', '+00:00')).replace(tzinfo=None))
        else:
            values.append(value)
    return values


def run_corpus_case(endpoint, profile, case, known):
    entry = known[case['query_sha256']]
    values = event_arguments(endpoint, profile, case) if case['name'] in EVENTS else plain_arguments(endpoint, case)
    started = time.monotonic()
    result = endpoint.run(entry, values)
    elapsed = time.monotonic() - started
    if any(id_column(c) for c, _ in result.columns):
        raise Refusal('id_column_in_compared_answer')
    return result, elapsed


def statement_sha(entry):
    return hashlib.sha256(entry['sql'].encode()).hexdigest()


def statement_variant(entry):
    """Short name of the reference source a statement comes from (tells same-name statements apart)."""
    files = sorted(s['file'] for s in entry.get('sources', [entry]))
    if any('cnight_observation' in f for f in files):
        return 'cnight'
    if any('candidates_data_source' in f for f in files):
        return 'candidates'
    return 'partner-chains'


def index_rows(endpoint, name):
    """How many pg_indexes rows the node's index_exists check sees for one index name."""
    return len(endpoint.run(by_name('index_exists'), [name]).rows)


def check_generation(endpoint, header):
    """Refuse an endpoint whose published generation is not the corpus's: anchors and witness tip."""
    entry = by_name('get_block_by_hash', part='cnight_observation')
    for key, anchor in sorted(header.get('anchors', {}).items()):
        result = endpoint.run(entry, [bytes.fromhex(anchor['hash'])])
        if len(result.rows) != 1 or result.rows[0]['block_number'] != anchor['height']:
            raise Refusal('corpus_generation_mismatch')
    latest = endpoint.run(by_name('get_latest_block_info'), [])
    if len(latest.rows) != 1 or latest.rows[0]['block_no'] != header['witness']:
        raise Refusal('corpus_generation_mismatch')


def latency_summary(values):
    ordered = sorted(values)
    count = len(ordered)
    if not count:
        return {'count': 0}
    rank = lambda p: ordered[(count * p + 99) // 100 - 1]
    return {'count': count, 'total_seconds': round(sum(ordered), 6), 'p50_seconds': round(rank(50), 6),
            'p95_seconds': round(rank(95), 6), 'max_seconds': round(ordered[-1], 6)}


def load_baseline(path, corpus):
    """Answers of an earlier run (a capture) whose page digests equal the corpus: case id -> encoded rows.

    Used only to recognise tie order: the corpus holds digests, not rows."""
    expected = {item['id']: item['expected'] for item in corpus.cases}
    out = {}
    with gzip.open(path, 'rt', encoding='utf-8') as lines:
        for line in lines:
            record = json.loads(line)
            rows = record.get('rows')
            if rows is not None and record['id'] in expected and record['name'] in TIE_KEYS:
                ordered = digest_encoded(rows)
                if ordered == expected[record['id']]:
                    out[record['id']] = rows
    return out


def order_keys(name, encoded):
    return [tuple(json.loads(r)[k] for k in TIE_KEYS[name]) for r in encoded]


def tie_order(name, rows, baseline_rows):
    """The page holds the same rows as the reference page with the same ORDER BY key sequence:
    only rows with equal keys are in another order."""
    if baseline_rows is None or name not in TIE_KEYS or sorted(rows) != sorted(baseline_rows):
        return False
    return order_keys(name, rows) == order_keys(name, baseline_rows)


def window_of(case):
    """The event window a page belongs to: the case without its LIMIT and OFFSET."""
    return canonical({k: v for k, v in case.items() if k not in ('limit', 'offset')})


def reference_windows(corpus, baseline):
    """Each event window whose rows the baseline pages determine at every position: window -> rows in
    the captured order. Pages that disagree at a position, or a gap, leave the window out."""
    positions, ends = collections.defaultdict(dict), {}
    for item in corpus.cases:
        case, rows = item['case'], baseline.get(item['id'])
        if rows is None:
            continue
        key, offset, limit = window_of(case), case.get('offset', 0), case.get('limit', 128)
        for i, row in enumerate(rows):
            if positions[key].setdefault(offset + i, row) != row:
                positions[key][offset + i] = None
        if len(rows) < limit:
            ends[key] = offset + len(rows)
    out = {}
    for key, rows in positions.items():
        end = ends.get(key)
        if end is not None and all(rows.get(i) is not None for i in range(end)) and not any(i >= end for i in rows):
            out[key] = [rows[i] for i in range(end)]
    return out


def window_tie_order(name, case, rows, reference, native_window):
    """A page of a window paged across rows with equal ORDER BY keys: the endpoint's whole window holds
    the captured window's rows, the page has the captured key sequence at its positions, and every
    row of the page belongs to the captured rows with its key (only tied rows changed places)."""
    if reference is None or native_window is None or sorted(native_window) != sorted(reference):
        return False
    offset = case.get('offset', 0)
    expected = reference[offset:offset + len(rows)]
    if len(expected) != len(rows) or order_keys(name, rows) != order_keys(name, expected):
        return False
    groups = collections.defaultdict(collections.Counter)
    for key, row in zip(order_keys(name, reference), reference):
        groups[key][row] += 1
    used = collections.defaultdict(collections.Counter)
    for key, row in zip(order_keys(name, rows), rows):
        used[key][row] += 1
        if used[key][row] > groups[key][row]:
            return False
    return True


def run_corpus(endpoint, profile, corpus, *, capture=None, progress=None, failure_details=50, baseline=None):
    """Every corpus case in order; never stops at the first mismatch.

    With `baseline` (see load_baseline), a page that differs from the corpus only by the order of
    rows with equal ORDER BY keys is judged `tie_order` instead of `mismatch`: within the page
    (`tie_order`), or across the pages of its window (`window_tie_order`, one extra query of the
    whole window on the endpoint)."""
    known = statements()
    families = collections.defaultdict(lambda: collections.Counter())
    latency, statement_latency = collections.defaultdict(list), collections.defaultdict(list)
    failures, verified, windows_left = [], {}, []
    ties = {'page': [], 'window': []}
    out = gzip.open(capture, 'wt', encoding='utf-8', compresslevel=6) if capture else None
    try:
        for index, item in enumerate(corpus.cases):
            case, expected = item['case'], item['expected']
            name = case['name']
            started = time.monotonic()
            record = {'id': item['id'], 'name': name}
            try:
                result, elapsed = run_corpus_case(endpoint, profile, case, known)
                actual = digest(result.rows, case.get('ordered', True))
                statement_latency[name].append(elapsed)
                outcome = 'pass' if actual == expected else 'mismatch'
                record['rows'] = encoded_rows(result.rows, case.get('ordered', True))
                if outcome == 'mismatch' and tie_order(name, record['rows'], (baseline or {}).get(item['id'])):
                    outcome = 'tie_order'
                    ties['page'].append(item['id'])
                elif outcome == 'mismatch' and baseline and name in TIE_KEYS:
                    windows_left.append((item, record['rows']))
                if outcome == 'pass' and name not in verified and result.rows:
                    verified[name] = {'case': case, 'rows': result.rows}
            except QueryError as error:
                outcome, actual = 'error', error.summary()
                record['error'] = actual
            except Refusal as error:
                outcome, actual = 'error', {'error': 'harness', 'code': error.code}
                record['error'] = actual
            latency[name].append(time.monotonic() - started)
            families[name][outcome] += 1
            if outcome not in ('pass', 'tie_order') and len(failures) < failure_details:
                failures.append({'id': item['id'], 'name': name, 'expected': expected, 'actual': actual})
            if out:
                record['digest'] = actual if outcome != 'error' else None
                out.write(canonical(record) + '\n')
            if progress and (index + 1) % 250 == 0:
                progress({'phase': 'corpus', 'cases': index + 1, 'failures': sum(f['mismatch'] + f['error'] for f in families.values())})
    finally:
        if out:
            out.close()
    if windows_left:
        # Pages of windows that are paged across tied rows: judged against the captured window.
        references, native = reference_windows(corpus, baseline), {}
        for item, rows in windows_left:
            case, name = item['case'], item['case']['name']
            key = window_of(case)
            if key not in native:
                try:
                    result, _ = run_corpus_case(endpoint, profile, dict(case, limit=1000000, offset=0), known)
                    native[key] = encoded_rows(result.rows)
                except (QueryError, Refusal):
                    native[key] = None
            if window_tie_order(name, case, rows, references.get(key), native[key]):
                families[name]['mismatch'] -= 1
                families[name]['tie_order'] += 1
                failures = [f for f in failures if f['id'] != item['id']]
                ties['window'].append(item['id'])
    report = {name: {'cases': sum(c.values()), 'pass': c['pass'], 'mismatch': c['mismatch'], 'error': c['error'], 'tie_order': c['tie_order'],
                     'latency_with_helpers': latency_summary(latency[name]), 'latency_statement': latency_summary(statement_latency[name])}
              for name, c in sorted(families.items())}
    return report, failures, verified, ties
