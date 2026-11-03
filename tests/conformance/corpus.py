"""Frozen node-query corpus: canonical gzip JSON Lines of cardano-db-sync answers.

Line 1 is `{"corpus": FORMAT, "header": {...}, "origin": {...}}`; every further line is
`{"case": descriptor, "expected": {"rows": n, "sha256": h}, "id": i}`. The header names the
capture's generation (target, witness, anchors), its profile identity and its inventory
(`cases`, `families`). The inventory is read from the file; nothing here assumes a case count.
"""
import collections
import gzip
import hashlib
import json

from db_sync_alt.contract import Refusal, canonical

from conformance.statements import statements

FORMAT = 'db-sync-alt-node-query-corpus-v1'


class Corpus:
    def __init__(self, sha256, header, cases):
        self.sha256, self.header, self.cases = sha256, header, cases

    @property
    def families(self):
        return dict(collections.Counter(c['case']['name'] for c in self.cases))


def load(path, *, expect_sha256=None, profile=None):
    with open(path, 'rb') as handle:
        data = handle.read()
    actual = hashlib.sha256(data).hexdigest()
    if expect_sha256 and actual != expect_sha256:
        raise Refusal('corpus_sha256_mismatch')
    try:
        lines = gzip.decompress(data).decode().splitlines()
        first = json.loads(lines[0])
    except (OSError, ValueError, IndexError):
        raise Refusal('corpus_unreadable') from None
    if first.get('corpus') != FORMAT or not isinstance(first.get('header'), dict):
        raise Refusal('corpus_format')
    header = first['header']
    if header.get('complete') is not True or type(header.get('cases')) is not int or not isinstance(header.get('families'), dict):
        raise Refusal('corpus_header')
    if sum(header['families'].values()) != header['cases'] or len(lines) - 1 != header['cases']:
        raise Refusal('corpus_inventory')
    for key in ('target', 'witness'):
        if type(header.get(key)) is not int:
            raise Refusal('corpus_header')
    if profile is not None and header.get('identity') != profile.identity:
        raise Refusal('corpus_profile_mismatch')
    known = statements()
    cases = []
    for index, line in enumerate(lines[1:]):
        item = json.loads(line)
        if canonical(item) != line or item.get('id') != index:
            raise Refusal('corpus_case_sequence')
        case, expected = item.get('case'), item.get('expected')
        entry = known.get(case.get('query_sha256')) if isinstance(case, dict) else None
        if entry is None or entry['name'] != case.get('name'):
            raise Refusal('corpus_query_fingerprint')
        if (not isinstance(expected, dict) or set(expected) != {'rows', 'sha256'} or type(expected['rows']) is not int
                or expected['rows'] < 0 or len(expected['sha256']) != 64):
            raise Refusal('corpus_answer_shape')
        cases.append(item)
    corpus = Corpus(actual, header, cases)
    if corpus.families != header['families']:
        raise Refusal('corpus_inventory')
    return corpus
