#!/usr/bin/env python3
"""Build the image and run checks in isolated Docker containers (respects the caller's Docker context).

  lint                     ruff on the source tree
  unit                     tests/unit with no network
  integration [--shard I/N] tests/integration against one disposable PostgreSQL 17
  e2e                      Compose smoke of the built image with synthetic data
  all                      lint, unit, integration and e2e
  conformance              node-query conformance of one endpoint against a frozen corpus (not in `all`)
  drills                   fail-closed drills against one endpoint (not in `all`)

Every container, image and directory is created under a per-run name and removed afterwards.

conformance and drills need real data: --corpus (a frozen corpus of cardano-db-sync answers) and
--dsn (a libpq connection string of the PostgreSQL database as the node's reader role, without a password; PGPASSFILE and
PGSERVICEFILE of the caller are passed through read-only). Without them they fail; they never skip.
The runner uses the host network unless --network names a Docker network.
"""
import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
PG_IMAGE = 'postgres:17.11-bookworm@sha256:839fbb5a4942a7758dd93e18b42ea5649807491a0545b5eeeb8f0b8bb11857a5'
PYTHON_IMAGE = 'python:3.12-slim-bookworm@sha256:e697c81897302a4c4333bade000d98038fbb21eb064f8ee1fd42d02ebcd9d337'
RUFF = 'ruff==0.17.3'
LIMITS = ('--memory', '2g', '--cpus', '2')
# Conservative scheduling weights balance the non-density modules.
# The density regression gets its own shard without changing the job deadline.
WEIGHTS = {
    'test_dbsync_acquire': 900, 'test_dbsync_interchange': 700, 'test_completion': 308, 'test_incremental': 190, 'test_blockfrost_follow': 110,
    'test_tip_fork': 91, 'test_sync_store': 70, 'test_service_store': 60, 'test_store': 43,
    'test_service_cancellation': 34, 'test_sync_diagnostics': 30, 'test_cli_run': 24,
    'test_poll_accounting': 1, 'test_blockfrost_generation': 150,
    'test_pending_check': 10, 'test_dbsync_versions': 900,
    'test_prerequisites': 1165, 'test_prerequisite_resume': 74,
    'test_coexistence': 60, 'test_conformance_client': 20, 'test_event_windows': 30,
    'test_lifecycle_inspection': 57, 'test_native_serving': 120, 'test_network_profiles': 120,
    'test_checkpoint_backup': 240, 'test_integrity_checkpoints': 300,
    'test_manifest_kinds': 180, 'test_status_summaries': 120,
    'test_relevance': 600, 'test_relevance_checkpoint': 900, 'test_relevance_recovery': 120,
    'test_epoch_upgrade': 150, 'test_bridge_metadata': 120, 'test_candidates_address': 90, 'test_work_limits': 600,
    'test_global_selection': 700, 'test_bounded_writes': 600, 'test_epoch_end_anchor': 10, 'test_content_bounded_writes': 60,
    'test_update_bounded_checks': 4,
}


def docker(*args, **kwargs):
    return subprocess.run(['docker', *args], check=True, **kwargs)


def shard_modules(shard):
    modules = sorted(p.stem for p in (ROOT / 'tests/integration').glob('test_*.py'))
    if not shard:
        return ['integration.' + m for m in modules]
    index, count = (int(x) for x in shard.split('/'))
    if not 1 <= index <= count:
        raise SystemExit('--shard must be I/N with 1 <= I <= N')
    density = 'test_prerequisites'
    if count >= 2 and density in modules:
        if index == count:
            return ['integration.' + density]
        modules.remove(density)
        count -= 1
    loads = [[0, []] for _ in range(count)]
    for module in sorted(modules, key=lambda m: (-WEIGHTS.get(m, 1), m)):
        target = min(loads, key=lambda load: load[0])
        target[0] += WEIGHTS.get(module, 1)
        target[1].append(module)
    return ['integration.' + m for m in sorted(loads[index - 1][1])]


def wait_ready(pg):
    for _ in range(120):
        if subprocess.run(['docker', 'exec', pg, 'pg_isready', '-h', '127.0.0.1', '-U', 'postgres'],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0:
            return
        time.sleep(0.5)
    raise RuntimeError('isolated PostgreSQL did not start')


def restart_controller(pg, control, stop):
    """Restart the PostgreSQL server when a test asks for it through the shared control directory."""
    request, complete = control / 'restart-request', control / 'restart-complete'
    while not stop.is_set():
        if request.exists() and not complete.exists():
            docker('exec', '-u', 'postgres', pg, 'pg_ctl', '-D', '/var/lib/postgresql/data', 'restart', '-m', 'fast', '-w', '-t', '60',
                   stdout=subprocess.DEVNULL)
            wait_ready(pg)
            complete.write_text('restarted\n')
        stop.wait(0.2)


def lint():
    docker('run', '--rm', '--name', 'db-sync-alt-lint-' + uuid.uuid4().hex[:12], *LIMITS, '-v', str(ROOT) + ':/src:ro', '-w', '/src', PYTHON_IMAGE,
           'sh', '-c', 'python .github/scripts/check_integration_shards.py --root /src && '
           'pip install --quiet --disable-pip-version-check --root-user-action=ignore ' + RUFF + ' && ruff check --no-cache .')


def unit(image):
    docker('run', '--rm', '--network', 'none', *LIMITS, image, 'python', 'tools/run_tests.py', 'unit')


def integration(image, shard, token):
    modules = shard_modules(shard)
    print('integration modules: ' + ' '.join(modules), flush=True)
    pg = 'db-sync-alt-test-' + token
    control = Path(tempfile.mkdtemp(prefix='db-sync-alt-restart-'))
    control.chmod(0o777)
    stop = threading.Event()
    started = False
    try:
        # The server runs beside a keep-alive loop so a test-requested restart keeps the container
        # (and the test container's network namespace) alive.
        # Its data directory is tmpfs inside the container's memory limit, and recycled WAL stays there:
        # max_wal_size keeps WAL to an eighth of the limit (the 1 GB default took half of it).
        docker('run', '--detach', '--name', pg, '--network', 'none', *LIMITS, '--tmpfs', '/var/lib/postgresql/data:rw',
               '-e', 'POSTGRES_HOST_AUTH_METHOD=trust', '--entrypoint', 'bash', PG_IMAGE,
               '-c', 'docker-entrypoint.sh postgres -c max_wal_size=256MB & while :; do sleep 1; done', stdout=subprocess.DEVNULL)
        started = True
        wait_ready(pg)
        controller = threading.Thread(target=restart_controller, args=(pg, control, stop), daemon=True)
        controller.start()
        docker('run', '--rm', '--network', 'container:' + pg, *LIMITS,
               '-e', 'DB_SYNC_ALT_TEST_DSN=host=127.0.0.1 port=5432 user=postgres dbname=postgres',
               '-e', 'DB_SYNC_ALT_TEST_RESTART_DIR=/restart-control', '-v', str(control) + ':/restart-control',
               image, 'python', 'tools/run_tests.py', 'integration', *modules)
    finally:
        stop.set()
        if started:
            subprocess.run(['docker', 'rm', '-f', pg], stdout=subprocess.DEVNULL, check=False)
        shutil.rmtree(control, ignore_errors=True)


def e2e(image):
    sys.path.insert(0, str(ROOT / 'tests/e2e'))
    from compose_smoke import smoke
    smoke(image, PG_IMAGE)


def endpoint_run(image, a):
    """conformance/drills inside the image, reading the corpus and writing the report through mounts."""
    report = Path(a.report).absolute()
    if report.exists():
        raise SystemExit('report exists: ' + str(report))
    out = report.parent
    mounts = ['-v', str(Path(a.corpus).absolute()) + ':/conformance/corpus.jsonl.gz:ro', '-v', str(out) + ':/conformance/out']
    env = []
    for name in ('PGPASSFILE', 'PGSERVICEFILE'):
        if os.environ.get(name):
            mounts += ['-v', str(Path(os.environ[name]).absolute()) + ':/conformance/' + name.lower() + ':ro']
            env += ['-e', name + '=/conformance/' + name.lower()]
    args = ['--corpus', '/conformance/corpus.jsonl.gz', '--dsn', a.dsn, '--report', '/conformance/out/' + report.name]
    if a.corpus_sha256:
        args += ['--corpus-sha256', a.corpus_sha256]
    if a.command == 'conformance':
        script = 'tests/conformance/run.py'
        if a.baseline:
            mounts += ['-v', str(Path(a.baseline).absolute()) + ':/conformance/baseline.jsonl.gz:ro']
            args += ['--baseline', '/conformance/baseline.jsonl.gz']
        if a.capture:
            capture = Path(a.capture).absolute()
            if capture.parent != out:
                raise SystemExit('--capture must be in the directory of --report')
            args += ['--capture', '/conformance/out/' + capture.name]
    else:
        script = 'tests/conformance/drills.py'
        mounts += ['-v', str(Path(a.params).absolute()) + ':/conformance/params.json:ro']
        args += ['--params', '/conformance/params.json', '--outage-seconds', str(a.outage_seconds)]
        if a.control_dir:
            mounts += ['-v', str(Path(a.control_dir).absolute()) + ':/conformance/control']
            args += ['--control-dir', '/conformance/control']
        for drill in a.drill or []:
            args += ['--drill', drill]
    name = 'db-sync-alt-' + a.command + '-' + uuid.uuid4().hex[:12]
    result = subprocess.run(['docker', 'run', '--rm', '--name', name, '--network', a.network or 'host', '--read-only',
                             '--tmpfs', '/tmp', '--user', str(os.getuid()) + ':' + str(os.getgid()), *LIMITS, *mounts, *env,
                             image, 'python', script, *args])
    return result.returncode


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('command', choices=['lint', 'unit', 'integration', 'e2e', 'all', 'conformance', 'drills'])
    p.add_argument('--shard', help='run part I of N of the integration lane, e.g. 2/3')
    p.add_argument('--image', help='use this image instead of building one')
    p.add_argument('--corpus', help='conformance/drills: frozen corpus (gzip JSON Lines)')
    p.add_argument('--corpus-sha256', help='conformance/drills: required SHA-256 of the corpus file')
    p.add_argument('--dsn', help='conformance/drills: libpq connection string of the database as the reader, without a password')
    p.add_argument('--report', help='conformance/drills: report file to write (must not exist)')
    p.add_argument('--capture', help='conformance: also write every normalized answer here (gzip JSON Lines)')
    p.add_argument('--baseline', help='conformance: capture of an earlier run equal to the corpus; recognises tie order')
    p.add_argument('--network', help='conformance/drills: Docker network of the runner (default: host)')
    p.add_argument('--params', help='drills: public chain facts the drills need (JSON)')
    p.add_argument('--control-dir', help='drills: directory shared with the deployment controller')
    p.add_argument('--drill', action='append', help='drills: run only these drills')
    p.add_argument('--outage-seconds', type=float, default=600.0, help='drills: how long the updater stays stopped')
    a = p.parse_args()
    if a.shard and a.command != 'integration':
        p.error('--shard applies to integration only')
    if a.command in ('conformance', 'drills'):
        missing = [flag for flag, value in (('--corpus', a.corpus), ('--dsn', a.dsn), ('--report', a.report)) if not value]
        if a.command == 'drills':
            missing += [flag for flag, value in (('--params', a.params),) if not value]
        if missing:
            p.error(a.command + ' needs ' + ' '.join(missing))
        if 'password' in a.dsn:
            p.error('--dsn must not carry a password; use PGPASSFILE or PGSERVICEFILE')
    if a.command in ('lint', 'all'):
        lint()
        if a.command == 'lint':
            return
    token = uuid.uuid4().hex[:12]
    image = a.image or 'db-sync-alt-dev:' + token
    built = False
    try:
        if not a.image:
            docker('build', '-f', str(ROOT / 'deploy/Dockerfile'), '-t', image, str(ROOT))
            built = True
        if a.command in ('unit', 'all'):
            unit(image)
        if a.command in ('integration', 'all'):
            integration(image, a.shard, token)
        if a.command in ('e2e', 'all'):
            e2e(image)
        if a.command in ('conformance', 'drills'):
            code = endpoint_run(image, a)
            if code:
                raise SystemExit(code)
    finally:
        if built:
            subprocess.run(['docker', 'image', 'rm', image], stdout=subprocess.DEVNULL, check=False)


if __name__ == '__main__':
    main()
