"""Run one test lane inside the image; any skip, expected failure or unexpected success fails it.

Usage: python tools/run_tests.py unit
       python tools/run_tests.py integration [integration.test_module ...]
"""
from pathlib import Path
import os
import sys
import unittest

TESTS = Path(__file__).resolve().parents[1] / 'tests'


def main(argv):
    lane, modules = argv[0], argv[1:]
    if lane not in ('unit', 'integration'):
        raise SystemExit('lane must be unit or integration')
    os.chdir(TESTS)
    sys.path.insert(0, str(TESTS))
    loader = unittest.defaultTestLoader
    suite = loader.loadTestsFromNames(modules) if modules else loader.discover(lane, top_level_dir='.')
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    unexplained = [('skipped', t, reason) for t, reason in result.skipped]
    unexplained += [('expected failure', t, '') for t, _ in result.expectedFailures]
    unexplained += [('unexpected success', t, '') for t in result.unexpectedSuccesses]
    for kind, test, reason in unexplained:
        print('NOT ALLOWED: ' + kind + ': ' + test.id() + (' — ' + reason if reason else ''), file=sys.stderr)
    return 0 if result.wasSuccessful() and not unexplained else 1


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
