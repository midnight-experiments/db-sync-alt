"""Conformance run: every corpus case and every supplementary case against one endpoint.

usage: python tests/conformance/run.py --corpus PATH --dsn CONNINFO --report REPORT.json
           [--corpus-sha256 HEX] [--capture ROWS.jsonl.gz] [--baseline ROWS.jsonl.gz] [--profile-dir DIR] [--timeout SECONDS]

--baseline: the capture of an earlier run whose pages equal the corpus; a page that differs from the
corpus only in the order of rows with equal ORDER BY keys (no total order, as in cardano-db-sync) is
then counted as `tie_order`, which passes and is reported per family.

The connection string must not carry a password; use a libpq password file (PGPASSFILE) or service
file (PGSERVICEFILE). Exit 0: every judged case passes and every catalogue statement is covered;
1: a case failed; 2: refused before running (corpus, profile, generation or configuration).
"""
import argparse
import collections
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from psycopg.conninfo import conninfo_to_dict, make_conninfo  # noqa: E402

from db_sync_alt.contract import Profile, Refusal, canonical  # noqa: E402
from db_sync_alt.resources import profile_dir  # noqa: E402

from conformance import corpus as corpus_file, harness, supplement  # noqa: E402
from conformance.client import QueryError  # noqa: E402
from conformance.statements import HELPERS, bridge_fragment, sha, statements  # noqa: E402


def conninfo(dsn):
    params = conninfo_to_dict(dsn)
    if 'password' in params:
        raise Refusal('password_in_connection_string')
    params.setdefault('connect_timeout', '10')
    params.setdefault('client_encoding', 'UTF8')
    return make_conninfo(**params)


def run_supplement(endpoint, profile, corpus, verified):
    results, by_label = [], {}
    for case in supplement.cases(profile, corpus.header, corpus.cases):
        values, expected = case.values, case.expected
        if expected == 'verified-asset-create':
            row = verified.get('get_asset_creates', {}).get('rows', [None])[0]
            if row is None:
                raise Refusal('no_verified_event_row')
            values = [row['tx_hash: _']]
            expected = [{'block_number': row['block_number!: _'], 'tx_ix': row['tx_index_in_block: _']}]
        record = {'label': case.label, 'expect': case.expect if isinstance(case.expect, str) else case.expect[0],
                  'statement_sha256': sha(case.statement['sql']) if case.statement else None}
        if case.expect == 'component':
            if case.statement is None or not bridge_fragment(case.statement) or not case.guard:
                raise Refusal('unbound_non_emitted_component')
            record.update(outcome={'executed': False, 'kind': 'non_emitted_component'}, verdict='component',
                          reach=supplement.reach(case), guard=list(case.guard))
            results.append(record)
            by_label[case.label] = record
            continue
        try:
            if case.simple:
                result = endpoint.simple(case.statement['sql'])
            else:
                types = supplement.statement_types(case)
                endpoint.calls[case.statement['name'] if case.statement else 'formatted'] += 1
                result = endpoint.client.query(case.sql or case.statement['sql'], types, values)
            rows = [{k: r[k] for k in case.project} for r in result.rows] if case.project else result.rows
            encoded = harness.encoded_rows(rows)
            outcome = {**harness.digest_encoded(encoded), 'command': result.command, 'first_rows': encoded[:3]}
        except QueryError as error:
            encoded, outcome = None, error.summary()
        record['outcome'] = outcome
        record['verdict'] = judge(case, encoded, expected)
        if case.post and record['verdict'] in ('pass', 'observed'):
            try:
                check = harness.index_rows(endpoint, case.post)
            except QueryError as error:
                check = error.summary()
            record['post_index_rows'] = check
            if check != 1:
                record['verdict'] = 'fail'
        record['reach'] = supplement.reach(case)
        record['guard'] = list(case.guard)
        results.append(record)
        by_label[case.label] = record
    # A guard holds when it passes, or when it is an observed case whose own guards hold.
    def holds(label):
        record = by_label.get(label, {})
        return record.get('verdict') == 'pass' or (record.get('verdict') == 'observed' and record.get('guard_verdict') == 'pass')
    for record in results:
        if not all(holds(g) for g in record['guard']):
            record['guard_verdict'] = 'fail'
        elif record['guard']:
            record['guard_verdict'] = 'pass'
    return results


def judge(case, encoded, expected):
    """Verdict of one supplementary case; `encoded` is None when the endpoint refused."""
    failed = encoded is None
    kind = case.expect if isinstance(case.expect, str) else case.expect[0]
    if kind == 'observed':
        return 'observed'
    if kind == 'error':
        return 'pass' if failed else 'fail'
    if kind == 'complete':
        return 'pass' if not failed else 'fail'
    if kind == 'error_or_complete':
        return 'pass' if failed or not encoded else 'fail'
    if kind == 'rows':
        return 'pass' if not failed and encoded == harness.encoded_rows(case.expect[1]) else 'fail'
    if kind == 'error_or_rows':
        return 'pass' if failed or encoded == harness.encoded_rows(expected) else 'fail'
    if kind == 'error_or_digest':
        return 'pass' if failed or harness.digest_encoded(encoded) == expected else 'fail'
    raise ValueError('unknown expectation ' + kind)


def coverage(corpus, endpoint, supplementary):
    """Every catalogue literal: executed statements or source-bound dynamic components.

    Component inventory is explicitly distinguished from SQL execution; its final
    query guards are judged separately and cannot be replaced by inventory alone.
    """
    used = collections.Counter(item['case']['query_sha256'] for item in corpus.cases)
    labels = collections.defaultdict(list)
    for record in supplementary:
        if record['statement_sha256']:
            labels[record['statement_sha256']].append(record['label'])
    table = {}
    for fingerprint, entry in sorted(statements().items(), key=lambda kv: (kv[1]['name'], kv[0])):
        row = {'name': entry['name']}
        if used[fingerprint]:
            row['corpus_cases'] = used[fingerprint]
        if entry['name'] in HELPERS and endpoint.calls[entry['name']]:
            row['helper_calls'] = endpoint.calls[entry['name']]
        if labels[fingerprint]:
            row['supplement'] = labels[fingerprint]
        if bridge_fragment(entry):
            row['kind'] = 'non_emitted_component'
        table[fingerprint] = row
    missing = [fp for fp, row in table.items() if not any(k in row for k in ('corpus_cases', 'helper_calls', 'supplement'))]
    return table, missing


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--corpus', required=True)
    p.add_argument('--corpus-sha256')
    p.add_argument('--dsn', required=True)
    p.add_argument('--report', required=True)
    p.add_argument('--capture')
    p.add_argument('--baseline')
    p.add_argument('--profile-dir', default=str(profile_dir('preprod')))
    p.add_argument('--timeout', type=float, default=30.0)
    a = p.parse_args(argv)
    started = time.monotonic()
    report = {'harness': 'node-query-conformance', 'started_unix': time.time()}
    try:
        if Path(a.report).exists() or (a.capture and Path(a.capture).exists()):
            raise Refusal('output_exists')
        profile = Profile(a.profile_dir)
        corpus = corpus_file.load(a.corpus, expect_sha256=a.corpus_sha256, profile=profile)
        report['corpus'] = {'sha256': corpus.sha256, 'cases': len(corpus.cases), 'families': corpus.families,
                            'target': corpus.header['target'], 'witness': corpus.header['witness']}
        endpoint = harness.Endpoint(conninfo(a.dsn), timeout=a.timeout)
        try:
            endpoint.client.connect()
            report['endpoint'] = {'server_version': endpoint.client.parameter('server_version')}
            harness.check_generation(endpoint, corpus.header)
            progress = lambda event: print(canonical(event), file=sys.stderr, flush=True)
            baseline = harness.load_baseline(a.baseline, corpus) if a.baseline else None
            if a.baseline:
                report['baseline'] = {'sha256': corpus_file.hashlib.sha256(Path(a.baseline).read_bytes()).hexdigest(), 'event_pages': len(baseline)}
            families, failures, verified, ties = harness.run_corpus(endpoint, profile, corpus, capture=a.capture, progress=progress, baseline=baseline)
            supplementary = run_supplement(endpoint, profile, corpus, verified)
            table, missing = coverage(corpus, endpoint, supplementary)
        finally:
            endpoint.close()
    except (Refusal, QueryError) as error:
        code = error.code if isinstance(error, Refusal) else 'endpoint_' + error.kind
        report.update(result='REFUSED', error=code, elapsed_seconds=round(time.monotonic() - started, 3))
        if isinstance(error, QueryError):
            report['endpoint_error'] = error.summary()
        _write(a.report, report)
        print(canonical({'result': 'REFUSED', 'error': code}))
        return 2
    corpus_failed = sum(f['mismatch'] + f['error'] for f in families.values())
    judged_failed = [r['label'] for r in supplementary if r['verdict'] == 'fail' or r.get('guard_verdict') == 'fail']
    report.update(
        result='PASS' if not corpus_failed and not judged_failed and not missing else 'FAIL',
        cases={'corpus': len(corpus.cases), 'corpus_pass': len(corpus.cases) - corpus_failed, 'corpus_failed': corpus_failed,
               'corpus_tie_order': sum(f['tie_order'] for f in families.values()),
               'supplement': len(supplementary), 'supplement_failed': judged_failed},
        families=families, failures=failures, tie_order_cases=ties, supplement=supplementary,
        coverage={'statements': len(table), 'uncovered': missing, 'table': table},
        helper_calls=dict(sorted(endpoint.calls.items())),
        elapsed_seconds=round(time.monotonic() - started, 3))
    if a.capture:
        report['capture'] = {'sha256': corpus_file.hashlib.sha256(Path(a.capture).read_bytes()).hexdigest(), 'bytes': Path(a.capture).stat().st_size}
    _write(a.report, report)
    print(canonical({'result': report['result'], **report['cases'], 'uncovered': len(missing), 'elapsed_seconds': report['elapsed_seconds']}))
    return 0 if report['result'] == 'PASS' else 1


def _write(path, value):
    Path(path).write_text(json.dumps(value, indent=1, sort_keys=True, default=str) + '\n')


if __name__ == '__main__':
    raise SystemExit(main())
