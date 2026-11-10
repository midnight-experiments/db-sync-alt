"""Fail-closed drills: node queries in states where the data cannot be trusted or is not published.

usage: python tests/conformance/drills.py --corpus PATH --params PARAMS.json --dsn CONNINFO
           --report REPORT.json [--control-dir DIR] [--drill NAME ...] [--outage-seconds 600] [--profile-dir DIR]

The runner only reads what the node reads: PostgreSQL, as the reader role (--dsn). Drills that need the deployment to change
state (revoke, restart, stop, publish) ask an external controller through --control-dir: the runner
writes `request-<n>.json` {"seq", "action"} and waits for `done-<n>.json` {"seq", "action", "ok"}.
Actions: restart (stop the updater and start it again, answered at once), wait_ready, stop_updater,
start_updater, publish (publish the next generation), revoke (revoke the served database).

PARAMS.json holds public chain facts the corpus does not contain: {"blocks": {"<height>": "<hash>"},
"other_address": ..., "other_asset": ...}: the block just below the coverage start S, the block just
above the witness W, and an address and an asset (policy + name, hex) that hold data on chain but are
not in the profile.

Every drill judges each probe: `error` (the endpoint refused, which is fail-closed), `ready` (the
same answer as in the published state), or `wrong` (rows that differ, including empty rows where
the published state has rows). A drill fails on any `wrong` probe. While the updater is stopped,
every probe must keep answering from the last published generation.

Coverage drills (uncovered epoch, below the start cursor, beyond the witness, outside the profile)
ask what a correctly configured node never asks. The database cannot see a statement's parameters:
each probe is either refused or `unreachable` — answered, recorded, and outside the node's request
domain by construction (the
node's Cardano references lie in [S, T] with T = W - k, its data epochs in the published required
epochs, its addresses and assets in the profile) — and the drill also checks that the published
generation covers the edges of that domain (the first and last required epoch, the blocks at S and
W), the publication invariants that make the probes unreachable.
"""
import argparse
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db_sync_alt.contract import Profile, Refusal, canonical  # noqa: E402
from db_sync_alt.resources import profile_dir  # noqa: E402

from conformance import corpus as corpus_file, harness, supplement  # noqa: E402
from conformance.client import QueryError  # noqa: E402
from conformance.statements import by_name, statements  # noqa: E402

DRILLS = ('ready', 'uncovered_epoch', 'below_floor', 'beyond_witness', 'profile_mismatch', 'bridge', 'unready',
          'updater_stopped', 'publication_during_sequence', 'revoked')
# The node's catalogue lookups (relation count in information_schema, index names in pg_indexes) read
# PostgreSQL's catalogue, not the served data: the gate cannot cover them. While revoked they
# may answer, but only with the published answer (the relation and the indexes still exist).
CATALOG_PROBES = ('cache:tx_in-relation', 'cache:index_exists')


class Runner:
    def __init__(self, conninfo, corpus, profile, params, control_dir, timeout):
        self.conninfo, self.corpus, self.profile, self.params = conninfo, corpus, profile, params
        self.control_dir, self.timeout = Path(control_dir) if control_dir else None, timeout
        self.endpoint = harness.Endpoint(conninfo, timeout=timeout)
        self.known = statements()
        self.seq = 0
        self.reference = {}

    # ---- control protocol -------------------------------------------------------------
    def control(self, action, wait=900):
        if self.control_dir is None:
            raise Refusal('control_dir_required')
        self.seq += 1
        request = self.control_dir / ('request-' + str(self.seq) + '.json')
        request.write_text(canonical({'seq': self.seq, 'action': action, 'ts': time.time()}) + '\n')
        done = self.control_dir / ('done-' + str(self.seq) + '.json')
        deadline = time.monotonic() + wait
        while not done.exists():
            if time.monotonic() > deadline:
                raise Refusal('control_timeout_' + action)
            time.sleep(0.5)
        answer = json.loads(done.read_text())
        if answer.get('seq') != self.seq or answer.get('action') != action or answer.get('ok') is not True:
            raise Refusal('control_failed_' + action)
        return answer

    # ---- probes -------------------------------------------------------------------------
    def corpus_probe(self, name):
        """The middle case of a family (by id), preferring cases with rows: a typical answer, not an edge."""
        items = [c for c in self.corpus.cases if c['case']['name'] == name and c['expected']['rows'] > 0] or \
                [c for c in self.corpus.cases if c['case']['name'] == name]
        return sorted(items, key=lambda c: c['id'])[len(items) // 2]

    def probes(self):
        """One node query per family plus the cached-state checks, each with its published-state answer."""
        out = []
        for name in sorted(self.corpus.families):
            item = self.corpus_probe(name)
            out.append(('corpus:' + name, ('corpus', item['case']), item['expected']))
        fc = sorted((e for e in self.known.values() if e['name'] == 'from_connection'), key=lambda e: e['sql'].startswith('SELECT'))
        out.append(('cache:tx_in-relation', ('statement', fc[0], []), None))
        out.append(('cache:tx_in-populated', ('statement', fc[1], []), None))
        out.append(('cache:resolve_ident-night', ('statement', by_name('resolve_ident'),
                                                  [bytes.fromhex(self.profile.night[:56]), bytes.fromhex(self.profile.night[56:])]), None))
        out.append(('cache:high_bounds-target', ('statement', by_name('get_high_bounds'), [self.corpus.header['target']]), None))
        out.append(('cache:low_bounds-start', ('statement', by_name('get_low_bounds'), [self.profile.start[0]]), None))
        out.append(('cache:index_exists', ('statement', by_name('index_exists'), ['idx_tx_out_address']), None))
        return out

    def execute(self, probe):
        kind = probe[0]
        try:
            if kind == 'corpus':
                case = probe[1]
                result, _ = harness.run_corpus_case(self.endpoint, self.profile, case, self.known)
                return harness.digest(result.rows, case.get('ordered', True))
            if kind == 'formatted':
                result = self.endpoint.client.query(probe[1], probe[2], probe[3])
            else:
                result = self.endpoint.run(probe[1], probe[2])
            return harness.digest(result.rows)
        except QueryError as error:
            return error.summary()
        except Refusal as error:
            # The harness could not even form the node's query (for example a missing ID bound,
            # which panics the node): never a fail-closed answer.
            return {'harness_refusal': error.code}

    def judge(self, label, answer):
        if 'error' in answer:
            return 'error'
        return 'ready' if self.reference.get(label) == answer else 'wrong'

    def round(self, probes):
        return [{'probe': label, 'answer': answer, 'verdict': self.judge(label, answer)}
                for label, probe, _ in probes for answer in [self.execute(probe)]]

    # ---- drills -------------------------------------------------------------------------
    def drill_ready(self):
        """Published state: every probe answers, corpus probes equal db-sync's answers; this sets the reference."""
        results, ok = [], True
        for label, probe, expected in self.probes():
            answer = self.execute(probe)
            self.reference[label] = answer
            verdict = 'error' if 'error' in answer else ('pass' if expected is None or answer == expected else 'mismatch')
            ok &= verdict == 'pass'
            results.append({'probe': label, 'answer': answer, 'verdict': verdict})
        tx_in = [r['answer'] for r in results if r['probe'].startswith('cache:tx_in')]
        expected_tx_in = [harness.digest([{'count': 1}]), harness.digest([{'exists': True}])]
        ok &= tx_in == expected_tx_in
        return ok, {'probes': results, 'tx_in_configuration': 'Enabled' if tx_in == expected_tx_in else 'not Enabled'}

    def must_fail(self, queries):
        results = []
        for label, probe in queries:
            answer = self.execute(probe)
            results.append({'probe': label, 'answer': answer, 'verdict': 'error' if 'error' in answer else 'wrong'})
        return all(r['verdict'] == 'error' for r in results), {'probes': results}

    def outside_domain(self, queries, reason, edges):
        """A coverage drill: each probe the node never sends is refused or answered (`unreachable`, by
        `reason`), and the domain's edges are covered: `edges` are (label, probe, rows) whose answer
        must have exactly `rows` rows (None: at least one)."""
        results = []
        for label, probe in queries:
            answer = self.execute(probe)
            results.append({'probe': label, 'answer': answer, 'verdict': 'error' if 'error' in answer else 'unreachable'})
        edge_results = []
        for label, probe, rows in edges:
            answer = self.execute(probe)
            ok = 'error' not in answer and (answer.get('rows', 0) >= 1 if rows is None else answer.get('rows') == rows)
            edge_results.append({'probe': label, 'answer': answer, 'verdict': 'covered' if ok else 'wrong'})
        ok = all(r['verdict'] in ('error', 'unreachable') for r in results) and all(r['verdict'] == 'covered' for r in edge_results)
        return ok, {'probes': results, 'unreachable_because': reason, 'domain_edges': edge_results}

    def required_epochs(self):
        return sorted({c['case']['args'][0] for c in self.corpus.cases if c['case']['name'] == 'get_epoch_nonce'})

    def drill_uncovered_epoch(self):
        epochs = self.required_epochs()
        permissioned = None
        try:
            permissioned = harness.ident(self.endpoint, self.profile.permissioned)
        except (QueryError, Refusal):
            pass
        queries = []
        for epoch in (epochs[0] - 1, epochs[-1] + 1, epochs[-1] + 2):
            queries += [('epoch_nonce:' + str(epoch), ('statement', by_name('get_epoch_nonce'), [epoch])),
                        ('stake_distribution:' + str(epoch), ('statement', by_name('get_stake_distribution'), [epoch]))]
            for entry in sorted((e for e in self.known.values() if e['name'] == 'get_latest_block_for_epoch'), key=lambda e: e['sql']):
                queries.append(('latest_block_for_epoch:' + str(epoch) + ':' + harness.statement_variant(entry), ('statement', entry, [epoch])))
            if permissioned is not None:
                queries.append(('token_utxo_for_epoch:' + str(epoch), ('statement', by_name('get_token_utxo_for_epoch', parameters=2), [permissioned, epoch])))
        edges = []
        for epoch in (epochs[0], epochs[-1]):
            edges += [('epoch_nonce:' + str(epoch), ('statement', by_name('get_epoch_nonce'), [epoch]), 1),
                      ('stake_distribution:' + str(epoch), ('statement', by_name('get_stake_distribution'), [epoch]), None)]
        ok, detail = self.outside_domain(queries, 'the node reads data epochs only inside the published required epochs (E3 bound 2)', edges)
        detail['required_epochs'] = [epochs[0], epochs[-1], len(epochs)]
        return ok, detail

    def block(self, height):
        value = self.params.get('blocks', {}).get(str(height))
        if value is None:
            raise Refusal('drill_parameter_missing_block_' + str(height))
        return bytes.fromhex(value)

    def event_probe(self, name, start, end):
        return ('corpus', {'name': name, 'query_sha256': harness.statement_sha(by_name(name)), 'start': start, 'end': end, 'limit': 128, 'offset': 0})

    def drill_below_floor(self):
        s = self.profile.start[0]
        below = self.block(s - 1)
        queries = [('blocks_by_numbers:' + str(s - 10), ('statement', by_name('get_blocks_by_numbers'), [s - 10, s - 1])),
                   ('low_bounds:' + str(s - 1), ('statement', by_name('get_low_bounds'), [s - 1])),
                   ('high_bounds:' + str(s - 1), ('statement', by_name('get_high_bounds'), [s - 1])),
                   ('epoch_for_block_hash:' + str(s - 1), ('statement', by_name('get_epoch_for_block_hash'), [below]))]
        for entry in sorted((e for e in self.known.values() if e['name'] == 'get_block_by_hash'), key=lambda e: e['sql']):
            queries.append(('block_by_hash:' + str(s - 1) + ':' + harness.statement_variant(entry), ('statement', entry, [below])))
        for name in ('get_asset_creates', 'get_asset_spends', 'get_registrations', 'get_deregistrations'):
            queries.append(('events:' + name + ':below-start', self.event_probe(name, [s - 1000, 0], [s, 0])))
        edges = [('blocks_by_numbers:' + str(s), ('statement', by_name('get_blocks_by_numbers'), [s, s]), 1),
                 ('high_bounds:' + str(s), ('statement', by_name('get_high_bounds'), [s]), 1)]
        return self.outside_domain(queries, 'every Cardano reference of a Midnight block is at or above the start cursor S (E3 bound 1)', edges)

    def drill_beyond_witness(self):
        w = self.corpus.header['witness']
        above = self.block(w + 1)
        queries = [('blocks_by_numbers:' + str(w + 1), ('statement', by_name('get_blocks_by_numbers'), [w + 1, w + 10])),
                   ('high_bounds:' + str(w + 1), ('statement', by_name('get_high_bounds'), [w + 1])),
                   ('epoch_for_block_hash:' + str(w + 1), ('statement', by_name('get_epoch_for_block_hash'), [above]))]
        for entry in sorted((e for e in self.known.values() if e['name'] == 'get_block_by_hash'), key=lambda e: e['sql']):
            queries.append(('block_by_hash:' + str(w + 1) + ':' + harness.statement_variant(entry), ('statement', entry, [above])))
        t = self.corpus.header['target']
        for name in ('get_asset_creates', 'get_registrations'):
            queries.append(('events:' + name + ':beyond-target', self.event_probe(name, [t + 1, 0], [w + 1, 0])))
        edges = [('blocks_by_numbers:' + str(w), ('statement', by_name('get_blocks_by_numbers'), [w, w]), 1),
                 ('high_bounds:' + str(t), ('statement', by_name('get_high_bounds'), [t]), 1)]
        return self.outside_domain(queries, 'the node references only blocks k deep below the served tip W, so at most T = W - k (E3 bound 1)', edges)

    def drill_profile_mismatch(self):
        t = self.corpus.header['target']
        body = next(iter(self.profile.configs['federated-authority-config'].values()))
        other_address, other_asset = self.params.get('other_address'), self.params.get('other_asset')
        if not other_address or not other_asset or other_address in self.profile.addresses or self.profile.required(bytes.fromhex(other_asset[:56]), bytes.fromhex(other_asset[56:])):
            raise Refusal('drill_parameter_profile_values')
        policy, name = bytes.fromhex(other_asset[:56]), bytes.fromhex(other_asset[56:])
        queries = [('governance:other-address', ('statement', by_name('get_governance_body_utxo'), [other_address, bytes.fromhex(body['policy_id']), t])),
                   ('governance:other-policy', ('statement', by_name('get_governance_body_utxo'), [body['address'], policy, t])),
                   ('candidates:other-address', ('statement', by_name('get_utxos_for_address_tx_in_enabled'), [other_address, t])),
                   ('resolve_ident:other-asset', ('statement', by_name('resolve_ident'), [policy, name])),
                   ('deregistrations:other-address', ('formatted', by_name('get_deregistrations')['sql'], ['text'] + ['int4'] * 6 + ['int8'] * 4,
                                                      [other_address, self.profile.start[0], self.profile.start[1], t + 1, 0, 128, 0, 0, harness.MAX_ID, 0, harness.MAX_ID]))]
        try:
            night = harness.ident(self.endpoint, self.profile.night)
            queries.append(('token_utxo_for_epoch:night-asset', ('statement', by_name('get_token_utxo_for_epoch', parameters=2), [night, self.required_epochs()[-1]])))
        except (QueryError, Refusal):
            pass
        edges = [('governance:profile-address', ('statement', by_name('get_governance_body_utxo'), [body['address'], bytes.fromhex(body['policy_id']), t]), None),
                 ('resolve_ident:night', ('statement', by_name('resolve_ident'), [bytes.fromhex(self.profile.night[:56]), bytes.fromhex(self.profile.night[56:])]), 1)]
        return self.outside_domain(queries, 'the node queries only the addresses and assets of its configuration, which the profile must equal (documented operator requirement)', edges)

    def drill_bridge(self):
        queries = [('bridge:formatted-' + mode, ('formatted', supplement.bridge_query(mode),
                    ['text', 'text', 'bytea', 'bytea', 'int8', 'int4'],
                    supplement.bridge_values(self.profile, self.corpus.header['target'])))
                   for mode in ('Enabled', 'Consumed')]
        queries.append(('bridge:tx_metadata', ('formatted', 'SELECT id, key, tx_id, json FROM tx_metadata LIMIT 1', [], [])))
        return self.must_fail(queries)

    def watch(self, until_ready, limit, interval=1.0):
        """Probe repeatedly while the deployment changes state; stop after the first fully ready round."""
        rounds, started = [], time.monotonic()
        probes = self.probes()
        while time.monotonic() - started < limit:
            self.endpoint.client.close()
            results = self.round(probes)
            rounds.append({'t': round(time.monotonic() - started, 1), 'verdicts': summary(results), 'wrong': [r for r in results if r['verdict'] == 'wrong'],
                           'answered': sorted(r['probe'] for r in results if r['verdict'] == 'ready')})
            if until_ready and all(r['verdict'] == 'ready' for r in results):
                break
            time.sleep(interval)
        return rounds

    def drill_unready(self):
        """Restart the updater; probe through its startup (restore/verification) until it serves again."""
        self.control('restart')
        rounds = self.watch(until_ready=True, limit=900)
        ok = bool(rounds) and not any(r['wrong'] for r in rounds) and set(rounds[-1]['verdicts']) == {'ready'}
        unready = [r for r in rounds if r['verdicts'].get('error')]
        return bool(ok and unready), {'rounds': len(rounds), 'unready_rounds': len(unready),
                                      'first_ready_after_seconds': rounds[-1]['t'] if rounds else None, 'detail': compact(rounds)}

    def drill_updater_stopped(self, outage):
        self.control('stop_updater')
        during = self.watch(until_ready=False, limit=outage, interval=10.0)
        self.control('start_updater')
        after = self.watch(until_ready=True, limit=900)
        wrong = any(r['wrong'] for r in during + after)
        recovered = bool(after) and set(after[-1]['verdicts']) == {'ready'}
        served = bool(during) and all(set(r['verdicts']) == {'ready'} for r in during)
        ok = not wrong and recovered and served
        return ok, {'outage_seconds': outage, 'rounds_during': len(during),
                    'during_verdicts': merge(r['verdicts'] for r in during), 'served_during_outage': served,
                    'recovered_after_seconds': after[-1]['t'] if after else None, 'during': compact(during), 'after': compact(after)}

    def drill_publication_during_sequence(self):
        """Bounds before a publication, the event query after it: no event may be excluded."""
        t = self.corpus.header['target']
        start, end = list(self.profile.start), [t + 1, 0]
        low = harness.bounds(self.endpoint, 'get_low_bounds', start[0])
        high = harness.bounds(self.endpoint, 'get_high_bounds', end[0])
        night = harness.ident(self.endpoint, self.profile.night)
        before_latest = self.execute(('statement', by_name('get_latest_block_info'), []))
        self.control('publish')
        entry = by_name('get_asset_creates')
        args = [night, *start, *end, 100000, 0, low[0], high[0], low[1], high[1], low[2], high[2]]
        try:
            stale = harness.digest(self.endpoint.run(entry, args).rows)
        except QueryError as error:
            stale = error.summary()
        fresh_low = harness.bounds(self.endpoint, 'get_low_bounds', start[0])
        fresh_high = harness.bounds(self.endpoint, 'get_high_bounds', end[0])
        fresh = harness.digest(self.endpoint.run(entry, [night, *start, *end, 100000, 0, fresh_low[0], fresh_high[0], fresh_low[1], fresh_high[1], fresh_low[2], fresh_high[2]]).rows)
        after_latest = self.execute(('statement', by_name('get_latest_block_info'), []))
        ok = ('error' in stale or stale == fresh) and after_latest != before_latest
        return ok, {'bounds_before': {'low': low, 'high': high}, 'bounds_after': {'low': fresh_low, 'high': fresh_high},
                    'events_with_old_bounds': stale, 'events_with_new_bounds': fresh,
                    'latest_before': before_latest, 'latest_after': after_latest, 'published': after_latest != before_latest}

    def drill_revoked(self):
        self.control('revoke')
        time.sleep(1)
        rounds = self.watch(until_ready=False, limit=30, interval=5.0)
        ok = bool(rounds) and all(not r['wrong'] and set(r['answered']) <= set(CATALOG_PROBES) for r in rounds)
        return ok, {'rounds': len(rounds), 'verdicts': merge(r['verdicts'] for r in rounds),
                    'answered_catalogue_probes': sorted({p for r in rounds for p in r['answered']}), 'detail': compact(rounds)}


def summary(results):
    out = {}
    for r in results:
        out[r['verdict']] = out.get(r['verdict'], 0) + 1
    return out


def merge(verdicts):
    out = {}
    for v in verdicts:
        for key, count in v.items():
            out[key] = out.get(key, 0) + count
    return out


def compact(rounds):
    """Consecutive rounds with the same verdicts collapse into one entry."""
    out = []
    for r in rounds:
        if out and out[-1]['verdicts'] == r['verdicts'] and not r['wrong']:
            out[-1]['until'] = r['t']
        else:
            out.append(dict(r))
    return out


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--corpus', required=True)
    p.add_argument('--corpus-sha256')
    p.add_argument('--params', required=True)
    p.add_argument('--dsn', required=True)
    p.add_argument('--report', required=True)
    p.add_argument('--control-dir')
    p.add_argument('--drill', action='append', choices=DRILLS)
    p.add_argument('--outage-seconds', type=float, default=600.0)
    p.add_argument('--profile-dir', default=str(profile_dir('preprod')))
    p.add_argument('--timeout', type=float, default=30.0)
    a = p.parse_args(argv)
    from conformance.run import conninfo
    report = {'harness': 'fail-closed-drills', 'started_unix': time.time(), 'drills': {}}
    try:
        if Path(a.report).exists():
            raise Refusal('output_exists')
        profile = Profile(a.profile_dir)
        corpus = corpus_file.load(a.corpus, expect_sha256=a.corpus_sha256, profile=profile)
        params = json.loads(Path(a.params).read_text())
        runner = Runner(conninfo(a.dsn), corpus, profile, params, a.control_dir, a.timeout)
        names = a.drill or list(DRILLS)
        if 'ready' not in names:
            names = ['ready'] + names
        for name in DRILLS:
            if name not in names:
                continue
            started = time.monotonic()
            print(canonical({'drill': name, 'phase': 'start'}), file=sys.stderr, flush=True)
            if name == 'updater_stopped':
                ok, detail = runner.drill_updater_stopped(a.outage_seconds)
            else:
                ok, detail = getattr(runner, 'drill_' + name)()
            report['drills'][name] = {'result': 'PASS' if ok else 'FAIL', 'seconds': round(time.monotonic() - started, 1), **detail}
            print(canonical({'drill': name, 'result': report['drills'][name]['result']}), file=sys.stderr, flush=True)
            _write(a.report + '.partial', report)
        runner.endpoint.close()
    except Refusal as error:
        report.update(result='REFUSED', error=error.code)
        _write(a.report, report)
        print(canonical({'result': 'REFUSED', 'error': error.code}))
        return 2
    report['result'] = 'PASS' if all(d['result'] == 'PASS' for d in report['drills'].values()) else 'FAIL'
    _write(a.report, report)
    Path(a.report + '.partial').unlink(missing_ok=True)
    print(canonical({'result': report['result'], 'drills': {k: v['result'] for k, v in report['drills'].items()}}))
    return 0 if report['result'] == 'PASS' else 1


def _write(path, value):
    Path(path).write_text(json.dumps(value, indent=1, sort_keys=True, default=str) + '\n')


if __name__ == '__main__':
    raise SystemExit(main())
