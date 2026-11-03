#!/usr/bin/env python3
"""Measure the memory of db-sync-alt and its PostgreSQL on a Linux host (cgroup v2 and /proc).

  sample   --container NAME... [--unit UNIT...] [--user-units] [--pg-container NAME] [--pg-user USER]
           [--interval SECONDS] [--pss-every N] (--duration SECONDS | --stop-file PATH) --out SAMPLES.tsv
  snapshot --container NAME... [--unit UNIT...] [--user-units] [--pg-container NAME] [--pg-user USER]
  summary  SAMPLES.tsv

For each Docker container and systemd unit: memory.current, memory.peak, memory.swap.current and
memory.stat (anon, file, kernel, shmem, file_mapped). For the PostgreSQL container: the proportional
set size (PSS, from /proc/<pid>/smaps_rollup) summed over every PostgreSQL process, split by
process kind (client backends, checkpointer, ...), the number of client backends, and the settings
that bound PostgreSQL memory (shared_buffers, work_mem, maintenance_work_mem, max_connections).
PSS counts each shared page once in total, so the PostgreSQL sum is the server's real footprint;
memory.current additionally counts the page cache the cgroup is charged for.

Sampling only reads cgroup and /proc files and runs `docker inspect`, `docker exec ... psql -c SHOW`
and `docker exec ... cat` in the PostgreSQL container; it never changes a container or a database.
Uses the Docker context of the caller (DOCKER_HOST). Values are bytes.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

COLUMNS = ('ts', 'target', 'kind', 'current', 'peak', 'swap_current', 'anon', 'file', 'kernel', 'shmem', 'file_mapped',
           'pg_pss', 'pg_pss_anon', 'pg_pss_file', 'pg_pss_shmem', 'pg_swap_pss', 'pg_processes', 'pg_backends')
STAT_KEYS = ('anon', 'file', 'kernel', 'shmem', 'file_mapped')
SMAPS_KEYS = {'Pss': 'pss', 'Pss_Anon': 'pss_anon', 'Pss_File': 'pss_file', 'Pss_Shmem': 'pss_shmem', 'SwapPss': 'swap_pss'}
SETTINGS = ('shared_buffers', 'work_mem', 'maintenance_work_mem', 'max_connections', 'effective_cache_size', 'huge_pages')
CGROUP_ROOT = Path('/sys/fs/cgroup')


def parse_stat(text):
    """memory.stat -> {key: bytes} for the keys this tool reports."""
    values = dict(line.split() for line in text.splitlines() if line.strip())
    return {key: int(values[key]) for key in STAT_KEYS if key in values}


def parse_smaps_rollup(text):
    """smaps_rollup -> {pss, pss_anon, ...} in bytes (the file reports kB)."""
    out = {}
    for line in text.splitlines():
        parts = line.split()
        if len(parts) >= 2 and parts[0].rstrip(':') in SMAPS_KEYS:
            out[SMAPS_KEYS[parts[0].rstrip(':')]] = int(parts[1]) * 1024
    return out


def process_kind(cmdline):
    """Kind of a PostgreSQL process from its title; None when the process is not PostgreSQL."""
    title = cmdline.replace('\0', ' ').strip()
    if not title.startswith('postgres'):
        return None
    if not title.startswith('postgres:'):
        return 'postmaster'
    rest = title[len('postgres:'):].strip()
    for kind in ('checkpointer', 'background writer', 'walwriter', 'autovacuum launcher', 'autovacuum worker',
                 'logical replication launcher', 'walsender', 'walreceiver', 'startup', 'archiver', 'stats collector',
                 'parallel worker', 'io worker', 'walsummarizer'):
        if rest.startswith(kind):
            return kind
    return 'client backend'


def pg_totals(processes):
    """processes: [(kind, smaps dict)] -> PSS totals, process count, client backend count, per-kind PSS."""
    totals = {value: 0 for value in SMAPS_KEYS.values()}
    kinds = {}
    for kind, smaps in processes:
        for key in totals:
            totals[key] += smaps.get(key, 0)
        kinds[kind] = kinds.get(kind, 0) + smaps.get('pss', 0)
    return {**totals, 'processes': len(processes), 'backends': sum(1 for kind, _ in processes if kind == 'client backend'),
            'pss_by_kind': kinds}


def docker(*args):
    return subprocess.run(['docker', *args], capture_output=True, text=True, timeout=30)


def container_cgroup(name):
    found = docker('inspect', '--format', '{{.Id}}', name)
    if found.returncode:
        return None
    cid = found.stdout.strip()
    for path in (CGROUP_ROOT / 'system.slice' / ('docker-' + cid + '.scope'), CGROUP_ROOT / 'docker' / cid):
        if path.is_dir():
            return path
    return None


def unit_cgroup(unit, user):
    found = subprocess.run(['systemctl', *(['--user'] if user else []), 'show', '-p', 'ControlGroup', '--value', unit],
                           capture_output=True, text=True, timeout=10)
    group = found.stdout.strip()
    return CGROUP_ROOT / group.lstrip('/') if found.returncode == 0 and group else None


def read(path):
    try:
        return Path(path).read_text()
    except OSError:
        return None


def cgroup_values(path):
    integer = lambda name: int(read(path / name).strip()) if read(path / name) not in (None, '') else None
    return {'current': integer('memory.current'), 'peak': integer('memory.peak'), 'swap_current': integer('memory.swap.current'),
            **parse_stat(read(path / 'memory.stat') or '')}


def pg_processes_host(path):
    """PSS of the PostgreSQL processes of a cgroup, read on the host (needs the same uid as the server)."""
    out = []
    for pid in (read(path / 'cgroup.procs') or '').split():
        kind = process_kind(read('/proc/' + pid + '/cmdline') or '')
        smaps = read('/proc/' + pid + '/smaps_rollup')
        if kind is None:
            continue
        if smaps is None:
            return None
        out.append((kind, parse_smaps_rollup(smaps)))
    return out


PROC_SCRIPT = 'for d in /proc/[0-9]*; do printf "==%s\\n" "$(tr "\\0" " " < $d/cmdline 2>/dev/null)"; cat $d/smaps_rollup 2>/dev/null; done'


def pg_processes_exec(container):
    """PSS of the PostgreSQL processes, read inside the container (its own uid can read them)."""
    found = docker('exec', container, 'sh', '-c', PROC_SCRIPT)
    if found.returncode:
        return None
    out, kind, block = [], None, []
    for line in found.stdout.splitlines() + ['==']:
        if line.startswith('=='):
            if kind is not None and block:
                out.append((kind, parse_smaps_rollup('\n'.join(block))))
            kind, block = process_kind(line[2:]), []
        else:
            block.append(line)
    return out


def pg_settings(container, user):
    query = "SELECT name, setting || COALESCE(unit, '') FROM pg_settings WHERE name IN (" + ','.join("'" + s + "'" for s in SETTINGS) + ') ORDER BY 1'
    found = docker('exec', container, 'psql', '-U', user, '-d', 'postgres', '-AtX', '-F', '=', '-c', query)
    if found.returncode:
        return None
    return dict(line.split('=', 1) for line in found.stdout.splitlines() if '=' in line)


def collect(args, pss):
    rows, now = [], time.time()
    targets = [(name, 'container', container_cgroup(name)) for name in args.container]
    targets += [(unit, 'unit', unit_cgroup(unit, args.user_units)) for unit in args.unit]
    for name, kind, path in targets:
        if path is None:
            continue
        row = {'ts': round(now, 3), 'target': name, 'kind': kind, **cgroup_values(path)}
        if pss and name == args.pg_container:
            processes = pg_processes_host(path)
            if processes is None:
                processes = pg_processes_exec(name)
            if processes is not None:
                totals = pg_totals(processes)
                row.update({'pg_' + key: totals[key] for key in ('pss', 'pss_anon', 'pss_file', 'pss_shmem', 'swap_pss', 'processes', 'backends')})
                row['pss_by_kind'] = totals['pss_by_kind']
        rows.append(row)
    return rows


def sample(args):
    out = Path(args.out)
    new = not out.exists()
    deadline = time.monotonic() + args.duration if args.duration else None
    with out.open('a', buffering=1) as handle:
        if new:
            handle.write('\t'.join(COLUMNS) + '\n')
        count = 0
        while True:
            for row in collect(args, pss=count % args.pss_every == 0):
                handle.write('\t'.join('' if row.get(c) is None else str(row[c]) for c in COLUMNS) + '\n')
            count += 1
            if (deadline and time.monotonic() >= deadline) or (args.stop_file and os.path.exists(args.stop_file)):
                break
            time.sleep(args.interval)


def snapshot(args):
    rows = collect(args, pss=True)
    settings = pg_settings(args.pg_container, args.pg_user) if args.pg_container else None
    print(json.dumps({'ts': time.time(), 'targets': rows, 'pg_settings': settings}, indent=1, sort_keys=True))


def summarize(lines):
    """TSV sample lines (header first) -> per target: max and last of every value column."""
    header = lines[0].split('\t')
    values = {}
    for line in lines[1:]:
        row = dict(zip(header, line.split('\t')))
        target = values.setdefault(row['target'], {'kind': row['kind'], 'samples': 0, 'first_ts': float(row['ts']), 'max': {}, 'last': {}})
        target['samples'] += 1
        target['last_ts'] = float(row['ts'])
        for column in header[3:]:
            if row.get(column):
                number = int(row[column])
                target['max'][column] = max(number, target['max'].get(column, number))
                target['last'][column] = number
    return values


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest='command', required=True)
    for name in ('sample', 'snapshot'):
        s = sub.add_parser(name)
        s.add_argument('--container', action='append', default=[])
        s.add_argument('--unit', action='append', default=[])
        s.add_argument('--user-units', action='store_true', help='the units are systemd --user units')
        s.add_argument('--pg-container', help='container whose PostgreSQL processes are summed by PSS')
        s.add_argument('--pg-user', default='postgres', help='database role for reading settings (snapshot)')
        if name == 'sample':
            s.add_argument('--interval', type=float, default=2.0)
            s.add_argument('--pss-every', type=int, default=5, help='read PSS every N samples (it reads every process)')
            s.add_argument('--duration', type=float)
            s.add_argument('--stop-file')
            s.add_argument('--out', required=True)
    s = sub.add_parser('summary')
    s.add_argument('samples')
    a = p.parse_args(argv)
    if a.command == 'summary':
        print(json.dumps(summarize(Path(a.samples).read_text().splitlines()), indent=1, sort_keys=True))
        return 0
    if a.pg_container and a.pg_container not in a.container:
        a.container.append(a.pg_container)
    if not a.container and not a.unit:
        p.error('name at least one --container or --unit')
    if a.command == 'sample':
        if not a.duration and not a.stop_file:
            p.error('sample needs --duration or --stop-file')
        sample(a)
    else:
        snapshot(a)
    return 0


if __name__ == '__main__':
    sys.exit(main())
