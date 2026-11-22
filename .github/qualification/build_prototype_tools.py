"""Hosted-only exact-public-source tools for independent generated PG tests.

This does not execute PostgreSQL, run a benchmark, or qualify a prototype.
"""
import hashlib
import json
import os
import re
import shutil
import subprocess
from pathlib import Path

RUST = 'rust:1.100.0-bookworm@sha256:ade39a710ef85f3506cffcfec3e8a24ba090badef1dff2fb798f71d878833fea'
EXPECTED_PG = 'postgres:17.11-bookworm@sha256:67f41722b7a8cbdb868a44a4995c846eddfdc2973bccb291ce937dce88ad5675'
root = Path(os.environ['GITHUB_WORKSPACE'])
source = root / 'product'
pin = os.environ['PRODUCT_PIN']
assert re.fullmatch('[0-9a-f]{40}', pin)
output = Path(os.environ['RUNNER_TEMP']) / 'a7-prototype-tools'
target = Path(os.environ['RUNNER_TEMP']) / 'a7-prototype-target'
output.mkdir(exist_ok=False)
target.mkdir(exist_ok=False)
(output / 'bin').mkdir()


def capture(args, name):
    run = subprocess.run(args, capture_output=True, text=True, timeout=1800)
    (output / (name + '.stdout')).write_text(run.stdout)
    (output / (name + '.stderr')).write_text(run.stderr)
    assert run.returncode == 0, (name, run.returncode)
    return run.stdout


def git(*args):
    return subprocess.check_output(['git', '-C', str(source), *args], text=True).strip()


assert git('rev-parse', 'HEAD') == pin
assert git('status', '--porcelain') == ''
example = source / 'crates/dsa-store/examples/restore_writer_probe.rs'
assert example.is_file()
capture(['docker', 'pull', RUST], 'rust-pull')
image = json.loads(capture(['docker', 'image', 'inspect', RUST], 'rust-image'))
rustc = capture(['docker', 'run', '--rm', '--network', 'none', RUST, 'rustc', '-Vv'], 'rustc')
base = ['docker', 'run', '--rm', '-e', 'CARGO_TARGET_DIR=/work/target',
        '-e', 'CARGO_PROFILE_RELEASE_DEBUG=0', '--mount',
        f'type=bind,src={source},dst=/work/product,readonly', '--mount',
        f'type=bind,src={target},dst=/work/target', '-w', '/work/product', RUST]
capture([*base, 'cargo', 'build', '--locked', '--release', '-p', 'db-sync-alt',
         '--bin', 'db-sync-alt'], 'build-cli')
capture([*base, 'cargo', 'build', '--locked', '--release', '-p', 'dsa-store',
         '--example', 'restore_writer_probe'], 'build-writer-probe')
for label, path in [('db-sync-alt', target / 'release/db-sync-alt'),
                    ('restore_writer_probe', target / 'release/examples/restore_writer_probe')]:
    assert path.read_bytes()[:4] == b'\x7fELF'
    shutil.copyfile(path, output / 'bin' / label)
    (output / 'bin' / label).chmod(0o755)
for verb, required in [('--version', 'db-sync-alt 0.5.0'), ('restore', '--file'),
                       ('verify', '--database'), ('snapshot', '--file'),
                       ('compare', '--a'), ('status', '--database'), ('grant-reader', '--role')]:
    text = capture(['docker', 'run', '--rm', '--network', 'none', '--mount',
                    f'type=bind,src={output},dst=/tool,readonly', RUST,
                    '/tool/bin/db-sync-alt', verb,
                    *([] if verb == '--version' else ['--help'])], 'help-' + verb.lstrip('-'))
    assert required in text
assert git('status', '--porcelain') == ''
sha = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
manifest = {
    'scope': 'hosted public-source tool build only; no PG execution or benchmark',
    'source_pin': pin, 'source_clean_before_after': True,
    'source_origin': git('remote', 'get-url', 'origin'),
    'wrapper_pin': os.environ['GITHUB_SHA'], 'run_id': os.environ['GITHUB_RUN_ID'],
    'rust_image': RUST, 'observed_rust_image_id': image[0]['Id'],
    'rustc': rustc, 'cargo_lock_sha256': sha(source / 'Cargo.lock'),
    'writer_probe_source_sha256': sha(example),
    'binary_sha256': {path.name: sha(path) for path in (output / 'bin').iterdir()},
    'expected_generated_pg_image': EXPECTED_PG,
    'postgres_executed': False, 'candidate_functional_qualification': False,
    'performance_dispatched': False,
}
(output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
files = {str(path.relative_to(output)): sha(path) for path in output.rglob('*') if path.is_file()}
(output / 'SHA256SUMS.json').write_text(json.dumps(files, indent=2) + '\n')
print(json.dumps({'source_pin': pin, 'binaries': manifest['binary_sha256'],
                  'manifest_sha256': sha(output / 'manifest.json')}))
