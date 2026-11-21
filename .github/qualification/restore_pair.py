"""Prepared, manually dispatched generated-only restore comparison; no source DB."""
import hashlib
import json
import re
import shutil
import struct
import subprocess
import threading
import time
from pathlib import Path

# Match the existing archival launcher, not the older hosted checkpoint experiment.
# Diagnostic logging/IO timing also match that launcher. No wal_skip_threshold override.
SERVER_SETTINGS = [
    'shared_buffers=64MB', 'maintenance_work_mem=64MB',
    'max_parallel_maintenance_workers=0', 'max_parallel_workers_per_gather=0',
    'wal_level=minimal', 'max_wal_senders=0', 'archive_mode=off',
    'fsync=on', 'synchronous_commit=on', 'full_page_writes=on',
    'log_min_duration_statement=250', 'log_temp_files=0', 'track_io_timing=on',
    'log_lock_waits=on', 'log_parameter_max_length=0',
    'log_parameter_max_length_on_error=0',
]
FULL_SNAPSHOT_SHA = 'a1643f0c1f1eea0efd18d39d556b2776af107331c0de7b405739fbf1125cf2e1'
FULL_SNAPSHOT_BYTES = 18_523_014
FULL_PLAIN_BYTES = 77_564_270
EXPECTED_FIELDS = ['content_digest', 'state_digest', 'families', 'protected_state', 'state', 'supply']
PREDECESSOR = 'c384207b9bffaffb7995d26a815bdfb333b97555'


def configuration(candidate):
    assert re.fullmatch('[0-9a-f]{40}', candidate) and candidate != PREDECESSOR
    return {'source': candidate, 'predecessor': PREDECESSOR, 'reuse_run': 44724041124,
            'mode': 'qualify', 'divisor': 1, 'target': 'restore_pair',
            'portable_bytes': 134217728, 'restore_tcp_user_timeout_seconds': 600,
            'restore_server_settings': SERVER_SETTINGS,
            'scope': 'ONE retained GENERATED snapshot paired restore and independent verify; no source generation/conversion or actual archive timing'}


def snapshot_description(raw):
    assert raw[:8] == b'DSASNAP\0' and struct.unpack('>I', raw[8:12])[0] == 1
    size = struct.unpack('>Q', raw[12:20])[0]
    description = raw[52:52 + size]
    assert hashlib.sha256(description).digest() == raw[20:52]
    return json.loads(description)


def query_timings(log):
    """Retain logged >=250ms statements; never present this as every SQL execution."""
    items = []
    for line in log.splitlines():
        found = re.search(r'duration: ([0-9.]+) ms\s+(?:statement:|execute [^:]*:)\s*(.*)', line)
        if found:
            items.append({'seconds': float(found[1]) / 1000, 'first_sql_line': found[2],
                          'raw_log_line': line, 'sql_may_continue_in_raw_log': True})
    return items


def io_delta(before, after):
    # Mapped and backing devices are different views of the same IO. Never sum them.
    return {device: {field: value - before.get(device, {}).get(field, 0)
                     for field, value in row.items()}
            for device, row in after.items()}


def wal_delta(before, after):
    def lsn(value):
        high, low = value.split('/')
        return (int(high, 16) << 32) + int(low, 16)
    assert before['wal']['stats_reset'] == after['wal']['stats_reset'], 'WAL counters reset during measurement'
    values = {key: after['wal'][key] - value for key, value in before['wal'].items()
              if isinstance(value, (int, float)) and not isinstance(value, bool)}
    assert all(value >= 0 for value in values.values()), 'WAL counter decreased'
    return {'insert_lsn_bytes': lsn(after['insert_lsn']) - lsn(before['insert_lsn']),
            'flush_lsn_bytes': lsn(after['flush_lsn']) - lsn(before['flush_lsn']),
            'pg_stat_wal_delta': values, 'stats_reset': before['wal']['stats_reset'],
            'physical_bytes_must_be_read_from_cgroup_not_these_generated_byte_counts': True}


def run(g):
    root, out, config = g['ROOT'], g['OUT'], g['CONFIG']
    docker, output, save = (g[k] for k in ['docker', 'output', 'save'])
    names, ids = g['NAMES'], g['IDS']
    assert config['restore_server_settings'] == SERVER_SETTINGS
    assert config['reuse_run'] == 44724041124
    pins = [('predecessor', config['predecessor'], root / 'predecessor'),
            ('candidate', g['PIN'], root / 'product')]
    assert pins[0][1] != pins[1][1]
    for label, pin, path in pins:
        assert re.fullmatch('[0-9a-f]{40}', pin)
        assert output(['git', '-C', str(path), 'rev-parse', 'HEAD']).strip() == pin
        assert output(['git', '-C', str(path), 'status', '--porcelain']) == ''
    save('source.json', {'product_head': g['PIN'], 'predecessor': config['predecessor'],
                         'wrapper_head': g['os'].environ['GITHUB_SHA'], 'config': config,
                         'generated_only': True, 'source_generation': False,
                         'source_conversion': False, 'no_source_database': True})
    reused = root / 'reuse'
    seal = json.loads((reused / 'SHA256SUMS.json').read_text())
    for name, sha in seal.items():
        assert Path(name).name == name
        assert hashlib.sha256((reused / name).read_bytes()).hexdigest() == sha
    raw = (reused / 'converted.snapshot').read_bytes()
    assert len(raw) == FULL_SNAPSHOT_BYTES
    assert hashlib.sha256(raw).hexdigest() == FULL_SNAPSHOT_SHA
    description = snapshot_description(raw)
    assert sum(row['plain_bytes'] for row in description['sections']) == FULL_PLAIN_BYTES
    expected = description['verified']
    original = json.loads((reused / 'conversion.json').read_text())['snapshot']['verified']
    assert all(expected[key] == original[key] for key in EXPECTED_FIELDS)
    shutil.copyfile(reused / 'converted.snapshot', out / 'converted.snapshot')
    save('reuse-origin.json', {'run': config['reuse_run'], 'verified_original_files': len(seal),
                             'snapshot_sha256': FULL_SNAPSHOT_SHA,
                             'snapshot_file_bytes': len(raw), 'plain_section_bytes': FULL_PLAIN_BYTES,
                             'sections': description['sections'], 'expected': expected,
                             'scope': 'sealed generated bytes, no generation or source conversion'})
    g['call'](['docker', 'pull', g['RUST']]); g['call'](['docker', 'pull', g['PG']])
    save('images.json', json.loads(docker('image', 'inspect', g['RUST'], g['PG'])))
    targets = {}
    for label, pin, path in pins:
        target = Path(g['os'].environ['RUNNER_TEMP']) / ('restore-pair-build-' + label)
        target.mkdir(exist_ok=True); targets[label] = target
        args = ['docker', 'run', '--rm', '--name', g['PREFIX'] + '-build-' + label,
                '-e', 'CARGO_TARGET_DIR=/work/target', '-e', 'CARGO_PROFILE_RELEASE_DEBUG=0',
                '--mount', f'type=bind,src={path},dst=/work/product,readonly',
                '--mount', f'type=bind,src={target},dst=/work/target', '-w', '/work/product',
                g['RUST'], 'cargo', 'build', '--locked', '--release', '-p', 'db-sync-alt',
                '--bin', 'db-sync-alt']
        with (out / (label + '-build.stdout')).open('w') as stdout, (out / (label + '-build.stderr')).open('w') as stderr:
            g['call'](args, stdout=stdout, stderr=stderr, timeout=1800)
        for verb in ['--version', 'restore', 'verify', 'status']:
            text = output(['docker', 'run', '--rm', '--mount',
                           f'type=bind,src={target},dst=/work/target,readonly', g['RUST'],
                           '/work/target/release/db-sync-alt', verb,
                           *([] if verb == '--version' else ['--help'])])
            (out / (label + '-' + verb.replace('--', '') + '-preflight.txt')).write_text(text)
            if verb == '--version': assert text.strip() == 'db-sync-alt 0.5.0'
            else:
                assert '--statement-timeout-seconds' in text and '--idle-transaction-timeout-seconds' in text
                if verb == 'restore': assert '--file' in text
        save(label + '-binary.json', {'source_head': pin, 'clean': True,
                                     'sha256': hashlib.sha256((target / 'release/db-sync-alt').read_bytes()).hexdigest(),
                                     'rust_image': g['RUST'], 'normal_cli_help_checked_before_pg': True})
    docker('network', 'create', g['NETWORK'])
    mount_args = [arg for label, target in targets.items() for arg in
                  ['--mount', f'type=bind,src={target},dst=/work/{label},readonly']]
    ids[names['worker']] = docker('run', '-d', '--name', names['worker'],
        '--network', g['NETWORK'], '--cpus', '1', '--memory', '1g', '--memory-swap', '1g',
        *mount_args, '--mount', f'type=bind,src={out},dst=/evidence,readonly',
        g['RUST'], 'sleep', 'infinity').strip()
    g['thread'] = threading.Thread(target=g['monitor'], daemon=True); g['thread'].start()
    deadline = time.monotonic() + 7200
    results = []
    for label, pin, _ in pins:
        fresh = g['fresh_restore_target'](label)
        name, owner = 'restore_pair', 'restore_pair_owner'
        docker('exec', names['restored'], 'psql', '-XAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres',
               '-d', 'postgres', '-c', f'CREATE ROLE {owner} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS')
        docker('exec', names['restored'], 'psql', '-XAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres',
               '-d', 'postgres', '-c', f'CREATE DATABASE {name} OWNER {owner}')
        save('databases.json', {'restored': name})
        save('baseline-volumes.json', {'source': {'allocated_bytes': 0}, 'restored': {
            'allocated_bytes': int(docker('exec', names['restored'], 'du', '-sB1', '/var/lib/postgresql/data').split()[0])}})
        def query(sql):
            return json.loads(output(['docker', 'exec', '-e', 'PGOPTIONS=-c statement_timeout=3000',
                names['restored'], 'psql', '-XAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', name, '-c', sql]))
        settings = query("SELECT json_object_agg(name,json_build_object('setting',setting,'unit',unit,'source',source)) FROM pg_settings WHERE name=ANY(ARRAY['wal_level','max_wal_senders','archive_mode','fsync','synchronous_commit','full_page_writes','wal_skip_threshold','shared_buffers','maintenance_work_mem','work_mem','temp_buffers','jit','random_page_cost','max_parallel_workers_per_gather','max_parallel_maintenance_workers','max_wal_size','min_wal_size','checkpoint_timeout','checkpoint_completion_target','log_min_duration_statement','track_io_timing','log_parameter_max_length','log_parameter_max_length_on_error','log_line_prefix','server_version_num'])")
        assert settings['wal_level']['setting'] == 'minimal' and settings['shared_buffers']['setting'] == '8192'
        save(label + '-settings.json', settings)
        def counters():
            value = query("SELECT json_build_object('at',clock_timestamp(),'insert_lsn',pg_current_wal_insert_lsn(),'flush_lsn',pg_current_wal_flush_lsn(),'wal',row_to_json(w),'checkpointer',(SELECT row_to_json(c) FROM pg_stat_checkpointer c),'io',(SELECT json_agg(i) FROM pg_stat_io i),'database',(SELECT row_to_json(d) FROM pg_stat_database d WHERE datname=current_database())) FROM pg_stat_wal w")
            value['cgroup_io'] = g['io_stat'](g['cg'](names['restored']))
            value['pgdata_allocated_bytes'] = int(docker('exec', names['restored'], 'du', '-sB1', '/var/lib/postgresql/data').split()[0])
            value['wal_allocated_bytes'] = int(docker('exec', names['restored'], 'du', '-sB1', '/var/lib/postgresql/data/pg_wal').split()[0])
            return value
        phases = {}
        dsn = f'host=restored port=5432 user={owner} dbname={name} sslmode=disable tcp_user_timeout=600'
        cli = ['docker', 'exec', names['worker'], f'/work/{label}/release/db-sync-alt']
        common = ['--database', dsn, '--network', 'mainnet', '--statement-timeout-seconds', '600',
                  '--idle-transaction-timeout-seconds', '7200']
        def execute(verb, extra):
            before = counters(); started = time.time(); clock = time.monotonic()
            save('active-command.json', {'name': label + '-' + verb, 'started_unix': started})
            with (out / (label + '-' + verb + '.stdout')).open('w') as stdout, (out / (label + '-' + verb + '.stderr')).open('w') as stderr:
                process = subprocess.Popen([*cli, verb, *common, *extra], stdout=stdout, stderr=stderr)
                try: code = process.wait(timeout=max(.1, min(1800, deadline - time.monotonic())))
                except subprocess.TimeoutExpired:
                    # Kill only our identified worker container; generic finally drains own PG.
                    assert g['inspect'](names['worker'])['Id'] == ids[names['worker']]
                    docker('kill', names['worker']); process.wait(timeout=30)
                    code = process.returncode
            elapsed = time.monotonic() - clock; finished = time.time()
            after = counters()
            logs = docker('logs', '--since', str(started), '--until', str(finished), names['restored'])
            (out / (label + '-' + verb + '-postgres.log')).write_text(logs)
            measurement = {'seconds': elapsed, 'exit_code': code, 'started_unix': started,
                           'finished_unix': finished, 'before': before, 'after': after,
                           'wal_delta': wal_delta(before, after),
                           'io_delta_by_device_not_summed': io_delta(before['cgroup_io'], after['cgroup_io']),
                           'logged_query_threshold_ms': 250, 'logged_queries': query_timings(logs)}
            phases[verb] = measurement; save(label + '-' + verb + '-measurement.json', measurement)
            assert code == 0, (label, verb, 'real CLI failed; raw stdout/stderr retained before JSON parsing')
            value = json.loads((out / (label + '-' + verb + '.stdout')).read_text())
            actual = value
            assert all(actual[key] == expected[key] for key in EXPECTED_FIELDS), (label, verb, 'digest/state/family/supply mismatch')
            return value
        restore = execute('restore', ['--file', '/evidence/converted.snapshot'])
        verify = execute('verify', [])
        state = query('SELECT to_jsonb(p) FROM dsa.position p')
        assert state['serving']
        release_deadline = time.monotonic() + 5
        while query("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND application_name='db-sync-alt')"):
            assert time.monotonic() < release_deadline, 'own CLI backend did not disconnect'
            time.sleep(.1)
        # Drain is deliberately outside restore + verify: it exposes delayed physical IO,
        # but cannot improve the SC elapsed number or prove every storage-cache write.
        before_drain = counters(); drain_clock = time.monotonic()
        drain = subprocess.run(['docker', 'exec', '-e', 'PGOPTIONS=-c statement_timeout=600000',
            names['restored'], 'psql', '-XAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres',
            '-d', name, '-c', 'CHECKPOINT'], text=True, capture_output=True,
            timeout=max(.1, min(660, deadline - time.monotonic())))
        (out / (label + '-checkpoint.stdout')).write_text(drain.stdout)
        (out / (label + '-checkpoint.stderr')).write_text(drain.stderr)
        after_drain = counters()
        save(label + '-drain.json', {'seconds': time.monotonic() - drain_clock, 'exit_code': drain.returncode,
            'before': before_drain, 'after': after_drain, 'outside_sc_restore_plus_verify': True,
            'wal_delta': wal_delta(before_drain, after_drain),
            'io_delta_by_device_not_summed': io_delta(before_drain['cgroup_io'], after_drain['cgroup_io'])})
        assert drain.returncode == 0
        events = []
        for line in (out / (label + '-restore.stderr')).read_text().splitlines():
            try: events.append(json.loads(line))
            except ValueError: pass
        socket_limits = [event for event in events if event.get('event') == 'postgres_socket_limits']
        assert socket_limits and all(event['tcp_user_timeout_ms'] == 600000 for event in socket_limits)
        combined = phases['restore']['seconds'] + phases['verify']['seconds']
        item = {'label': label, 'source_head': pin, 'fresh_target': fresh, 'state': state,
                'restore_seconds': phases['restore']['seconds'], 'independent_verify_seconds': phases['verify']['seconds'],
                'restore_plus_independent_verify_seconds': combined, 'criterion_seconds': 180,
                'criterion_met_on_this_generated_fixture': combined <= 180,
                'phase_events': events, 'socket_limits': socket_limits, 'verified': verify, 'restored': restore}
        results.append(item); save(label + '-result.json', item)
        print(json.dumps({'event': 'generated_restore_pair_variant_finished', 'label': label,
                          'combined_seconds': combined, 'functional_pass': True}), flush=True)
    assert all(results[0]['verified'][k] == results[1]['verified'][k] for k in EXPECTED_FIELDS)
    save('restore-pair-result.json', {'status': 'PASS_GENERATED_FUNCTIONAL_PAIR', 'variants': results,
        'snapshot_file_bytes': FULL_SNAPSHOT_BYTES, 'plain_section_bytes': FULL_PLAIN_BYTES,
        'source_generation': False, 'source_conversion': False, 'historical_timing_proven': False,
        'limitations': ['one predecessor-first/candidate-second pair, not order-balanced speedup proof',
                        'fresh PGDATA each; no forced cache eviction', 'per-statement logs omit executions below 250 ms',
                        'checkpoint drain is reported separately; per-device IO must not be summed',
                        'generated addresses/payloads and storage differ from real archived data']})
