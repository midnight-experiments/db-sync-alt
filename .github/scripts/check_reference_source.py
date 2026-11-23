#!/usr/bin/env python3
"""Verify the vendored node files against the pinned upstream checkout, and that
every other node source file that issues SQL is listed, statement by statement,
in crates/dsa-store/sql-outside-catalogue.json.

SQL-looking literals are recognised by the rule of the catalogue extractor
(crates/dsa-store/tests/extract/mod.rs); both implement the same rule."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
REFERENCE = ROOT / 'reference'
OUTSIDE = ROOT / 'crates' / 'dsa-store' / 'sql-outside-catalogue.json'
KEYWORDS = {'SELECT', 'WITH', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP',
            'TRUNCATE', 'COPY', 'GRANT', 'REVOKE', 'ANALYZE', 'VACUUM', 'EXPLAIN', 'LOCK',
            'REINDEX', 'REFRESH', 'SET'}
STRUCTURE = {'FROM', 'INTO', 'TABLE', 'INDEX', 'WHERE'}
ESCAPES = {'n': '\n', 'r': '\r', 't': '\t', '"': '"', "'": "'", '\\': '\\', '0': '\0'}


def literals(text):
    """(offset, value) of every string literal of a Rust source, outside comments."""
    out = []
    i, n = 0, len(text)
    word = lambda k: k > 0 and (text[k - 1].isalnum() or text[k - 1] == '_')
    while i < n:
        if text.startswith('//', i):
            end = text.find('\n', i)
            i = n if end < 0 else end
        elif text.startswith('/*', i):
            depth, i = 1, i + 2
            while depth and i < n:
                if text.startswith('/*', i):
                    depth, i = depth + 1, i + 2
                elif text.startswith('*/', i):
                    depth, i = depth - 1, i + 2
                else:
                    i += 1
        elif text[i] == "'":
            # A character literal, or a lifetime (no closing quote).
            if text.startswith('\\', i + 1):
                close = text.find("'", i + 3 if text[i + 2] in "'\\" else i + 2)
                i = close + 1
            elif i + 2 < n and text[i + 2] == "'":
                i += 3
            else:
                i += 1
        elif not word(i) and (text.startswith('r', i) or text.startswith('br', i)):
            start = i
            j = i + (2 if text[i] == 'b' else 1)
            hashes = 0
            while j < n and text[j] == '#':
                hashes, j = hashes + 1, j + 1
            if j < n and text[j] == '"':
                end = '"' + '#' * hashes
                close = text.index(end, j + 1)
                out.append((start, text[j + 1:close]))
                i = close + len(end)
            else:
                i += 1
        elif text[i] == '"' or (text.startswith('b"', i) and not word(i)):
            start = i
            i += 2 if text[i] == 'b' else 1
            value = []
            while text[i] != '"':
                if text[i] == '\\':
                    escape = text[i + 1]
                    if escape in '\r\n':
                        i += 2
                        while text[i] in ' \t\r\n':
                            i += 1
                        continue
                    if escape == 'x':
                        value.append(chr(int(text[i + 2:i + 4], 16)))
                        i += 4
                    elif escape == 'u':
                        close = text.index('}', i)
                        value.append(chr(int(text[i + 3:close], 16)))
                        i = close + 1
                    else:
                        value.append(ESCAPES[escape])
                        i += 2
                else:
                    value.append(text[i])
                    i += 1
            out.append((start, ''.join(value)))
            i += 1
        else:
            i += 1
    return out


def sql_looking(value):
    """A literal whose first word is an SQL statement keyword in SQL spelling
    (all capitals, or all lower case followed by an SQL structure word)."""
    words = value.split()
    if not words:
        return False
    first = words[0].rstrip(';')
    if first.upper() not in KEYWORDS:
        return False
    if first == first.upper():
        return True
    if first != first.lower():
        return False
    i, n = 0, len(value)
    while i < n:
        if value[i].isascii() and (value[i].isalnum() or value[i] == '_'):
            start = i
            while i < n and value[i].isascii() and (value[i].isalnum() or value[i] == '_'):
                i += 1
            token = value[start:i].upper()
            if token in STRUCTURE:
                return True
            if token == 'AS' and value[i:].lstrip().startswith('('):
                return True
        else:
            i += 1
    return False


def statement_hash(value):
    return hashlib.sha256(value.strip().rstrip(';').encode()).hexdigest()


def statements(text):
    return {(text[:offset].count('\n') + 1, statement_hash(value))
            for offset, value in literals(text) if sql_looking(value)}


def test_file(name):
    parts = name.split('/')
    return ('tests' in parts or 'benches' in parts or parts[-1] in {'tests.rs', 'test.rs'}
            or parts[-1].endswith('_tests.rs'))


def scan(sources, vendored, outside):
    """Errors for SQL in `sources` ({path: text}, non-test files) that is neither
    vendored nor listed, and for listed statements that no longer exist."""
    errors = []
    listed = {}
    for entry in outside['statements']:
        listed.setdefault(entry['file'], set()).add((entry['line'], entry['sha256']))
    for name in sorted(sources):
        if name in vendored or test_file(name):
            continue
        found = statements(sources[name])
        for line, _ in sorted(found - listed.get(name, set())):
            errors.append(f'node_sql_outside_reference: {name}:{line}')
        for line, _ in sorted(listed.get(name, set()) - found):
            errors.append(f'stale_sql_exclusion: {name}:{line}')
    for name in sorted(set(listed) - set(sources) - set(vendored)):
        errors.append(f'stale_sql_exclusion: {name}')
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--node-source', type=Path, required=True)
    args = parser.parse_args()
    manifest = json.loads((REFERENCE / 'source-manifest.json').read_text())
    outside = json.loads(OUTSIDE.read_text())
    head = subprocess.check_output(['git', '-C', str(args.node_source), 'rev-parse', 'HEAD'],
                                   text=True).strip()
    if head != manifest['commit'] or outside['node_commit'] != manifest['commit']:
        raise SystemExit('node_source_commit_mismatch')
    for name, entry in manifest['files'].items():
        local = (REFERENCE / name).read_bytes()
        if hashlib.sha256(local).hexdigest() != entry['sha256']:
            raise SystemExit('reference_hash_mismatch: ' + name)
        if local != (args.node_source / entry['source']).read_bytes():
            raise SystemExit('node_source_bytes_mismatch: ' + name)
    names = subprocess.check_output(['git', '-C', str(args.node_source), 'ls-files', '*.rs'],
                                    text=True).split()
    sources = {name: (args.node_source / name).read_text(encoding='utf-8') for name in names}
    vendored = {entry['source'] for entry in manifest['files'].values()}
    errors = scan(sources, vendored, outside)
    for error in errors:
        print(error)
    if errors:
        print('Reference check failed: ' + str(len(errors)) + ' finding(s).')
        return 1
    print('Pinned reference source verified:', len(manifest['files']), 'files;',
          len(sources), 'node source files scanned for SQL')
    return 0


if __name__ == '__main__':
    sys.exit(main())
