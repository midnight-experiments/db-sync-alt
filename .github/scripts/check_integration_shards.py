#!/usr/bin/env python3
"""Check the source workflow against the exhaustive integration shard assignment."""
import argparse
import importlib.util
from pathlib import Path
import sys


def check(root):
    workflow_path = root/'.github/workflows/ci.yml'
    try:
        workflow = workflow_path.read_text()
    except OSError:
        return ['workflow_missing_or_unreadable']
    spec = importlib.util.spec_from_file_location('dev_shards', root/'tools/dev.py')
    dev = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(dev)
    errors = []
    if 'shard: [1, 2, 3, 4, 5, 6, 7, 8]' not in workflow:
        errors.append('workflow_matrix_mismatch')
    if '--shard ${{ matrix.shard }}/8' not in workflow:
        errors.append('workflow_command_mismatch')
    if dev.shard_modules('8/8') != ['integration.test_prerequisites']:
        errors.append('density_shard_mismatch')
    if {p.stem for p in (root/'tests/integration').glob('test_*.py')} != set(dev.WEIGHTS):
        errors.append('integration_weights_not_exhaustive')
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[2])
    args = parser.parse_args()
    errors = check(args.root.resolve())
    for code in errors:
        print('Integration-shard check failed: '+code)
    if errors:
        return 1
    print('Integration-shard source check passed: eight shards, density isolated, exhaustive weights.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
