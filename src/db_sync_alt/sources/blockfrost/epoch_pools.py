"""Current-only, independently enumerated pool stake preparation.

This module owns no database, thread or client. One step performs exactly one
provider GET (the accounted wrapper may reconcile metrics). Responses are kept
as a bounded replayable proof; expected SQL/candidate IDs are never selectors.
"""
import copy
import re

from db_sync_alt.contract import Refusal, canonical, digest, jsonable
from db_sync_alt.sources.blockfrost.adapter import OPENAPI, block_row, pool_hash

VERSION = 1
SEMANTICS = 'dbsync-13.8-active-stake-nonzero-v1'
SEMANTICS_COMMIT = 'ccb9c0f4f6168593ec8d8568a6358dbac84d2045'
MAX_TOTAL = 45_000_000_000_000_000
MAX_RECORDS = 8000
MAX_PAGES = 1000
MAX_IDENTITIES = 100000
MAX_POSITIVE = 10000
MAX_RECORD_BYTES = 2 * 1024**2
MAX_TRANSCRIPT_BYTES = 32 * 1024**2


def refuse(code='epoch_preparation_invalid'):
    raise Refusal(code)


def amount(value):
    if not isinstance(value, str) or re.fullmatch(r'0|[1-9][0-9]{0,16}', value) is None:
        refuse()
    result = int(value)
    if result > MAX_TOTAL:
        refuse()
    return result


def integer(value):
    if type(value) is not int or not 0 <= value < 2**31:
        refuse()
    return value


def header(raw):
    try:
        return jsonable(block_row(raw))
    except (KeyError, TypeError, ValueError):
        refuse()


def _pool(raw, extended):
    if not isinstance(raw, dict):
        refuse()
    try:
        identity = pool_hash(raw['pool_id']).hex()
        if extended:
            if raw.get('hex') != identity:
                refuse()
            return identity, {'pool_id': raw['pool_id'], 'active_stake': str(amount(raw['active_stake']))}
        return identity, {'pool_id': raw['pool_id'], 'retirement_epoch': integer(raw['epoch'])}
    except (KeyError, TypeError, ValueError):
        refuse()


class PoolCollector:
    """A deterministic current-epoch transcript, resumable by strict replay."""
    def __init__(self, profile, epoch):
        if profile.network.network != 'mainnet':
            refuse('epoch_preparation_unsupported')
        self.profile, self.epoch = profile, integer(epoch)
        self.phase, self.page, self.pass_number = 'genesis', 1, 1
        self.anchor = None
        self.nonce = self.total = None
        self.head = None
        self.current = self._empty_pass()
        self.passes, self.records = [], []
        self.transcript_bytes = 0
        self.complete = False

    @staticmethod
    def _empty_pass():
        return {'registered': {}, 'retired': {}, 'positive': {}, 'specific': {}, 'observations': {}}

    def request(self):
        if self.complete:
            return None
        if self.phase == 'genesis':
            return '/genesis'
        if self.phase in ('head-start', 'head-end'):
            return '/blocks/latest'
        if self.phase in ('summary-start', 'summary-end'):
            return '/epochs/' + str(self.epoch)
        if self.phase in ('nonce-start', 'nonce-end'):
            return '/epochs/' + str(self.epoch) + '/parameters'
        if self.phase in ('anchor-start', 'anchor-end'):
            return '/blocks/' + str(self.head['height'] - self.profile.security if self.anchor is None else self.anchor['height'])
        if self.phase in ('registered', 'retired'):
            name = 'extended' if self.phase == 'registered' else 'retired'
            return '/pools/' + name + '?count=100&page=' + str(self.page) + '&order=asc'
        if self.phase == 'specific':
            raw = next((raw for raw in self.search_order() if raw not in self.current['specific']), None)
            if raw is None:
                refuse('epoch_preparation_deficit')
            return '/pools/' + raw
        refuse()

    def search_order(self):
        # API metadata changes search order only. No age or SQL membership exclusion.
        return sorted(self.current['retired'], key=lambda raw: (-self.current['retired'][raw]['retirement_epoch'], raw))

    def _sum(self):
        return sum(map(int, self.current['positive'].values()))

    def _close_or_search(self):
        total = self._sum()
        if total > self.total:
            refuse('epoch_preparation_overcount')
        self.phase = 'head-end' if total == self.total else 'specific'
        if self.phase == 'specific' and len(self.current['specific']) == len(self.current['retired']):
            refuse('epoch_preparation_deficit')

    def _observe(self, raw):
        row = header(raw)
        if row['epoch'] != self.epoch or row['height'] < self.profile.security:
            refuse('epoch_preparation_epoch_changed')
        if self.head is not None and row['height'] < self.head['height']:
            refuse('epoch_preparation_epoch_changed')
        self.head = row
        self.current['observations'][self.phase] = row

    def _summary(self, raw):
        if not isinstance(raw, dict) or raw.get('epoch') != self.epoch:
            refuse()
        total = amount(raw.get('active_stake'))
        if not total:
            refuse()
        if self.total is not None and total != self.total:
            refuse('epoch_preparation_changed')
        self.total = total
        self.current['observations'][self.phase] = {'epoch': self.epoch, 'active_stake': str(total)}

    def _nonce(self, raw):
        if (not isinstance(raw, dict) or raw.get('epoch') != self.epoch
                or not isinstance(raw.get('nonce'), str) or re.fullmatch('[0-9a-f]{64}', raw['nonce']) is None):
            refuse()
        if self.nonce is not None and self.nonce != raw['nonce']:
            refuse('epoch_preparation_changed')
        self.nonce = raw['nonce']
        self.current['observations'][self.phase] = {'epoch': self.epoch, 'nonce': self.nonce}

    def _anchor(self, raw):
        row = header(raw)
        expected = self.head['height'] - self.profile.security if self.anchor is None else self.anchor['height']
        if row['height'] != expected or row['epoch'] > self.epoch:
            refuse('epoch_preparation_anchor_changed')
        if self.anchor is not None and row != self.anchor:
            refuse('epoch_preparation_anchor_changed')
        if self.anchor is None:
            self.anchor = row
        self.current['observations'][self.phase] = row

    def _inventory(self, raw, registered):
        if not isinstance(raw, list) or len(raw) > 100:
            refuse()
        family = 'registered' if registered else 'retired'
        result = self.current[family]
        for item in raw:
            identity, row = _pool(item, registered)
            if identity in result:
                refuse('epoch_preparation_duplicate')
            if not registered and (row['retirement_epoch'] > self.epoch or identity in self.current['registered']):
                refuse('epoch_preparation_inventory_changed')
            result[identity] = row
            if registered and int(row['active_stake']) > 0:
                self.current['positive'][identity] = row['active_stake']
            if len(result) > MAX_IDENTITIES or len(self.current['positive']) > MAX_POSITIVE:
                refuse('epoch_preparation_size_limit')
        if len(raw) == 100:
            self.page += 1
            if self.page > MAX_PAGES:
                refuse('epoch_preparation_size_limit')
            return
        if not result:
            refuse()
        self.page = 1
        if registered:
            if self.passes and set(result) != set(self.passes[0]['registered']):
                refuse('epoch_preparation_inventory_changed')
            self.phase = 'retired'
        else:
            if self.passes and result != self.passes[0]['retired']:
                refuse('epoch_preparation_inventory_changed')
            self._close_or_search()

    def _specific(self, raw):
        expected = self.request().removeprefix('/pools/')
        identity, row = _pool(raw, True)
        if identity != expected or identity not in self.current['retired'] or identity in self.current['specific']:
            refuse()
        self.current['specific'][identity] = row
        if int(row['active_stake']) > 0:
            self.current['positive'][identity] = row['active_stake']
        if len(self.current['positive']) > MAX_POSITIVE:
            refuse('epoch_preparation_size_limit')
        self._close_or_search()

    def _end_pass(self):
        if self._sum() != self.total:
            refuse('epoch_preparation_deficit')
        self.passes.append(copy.deepcopy(self.current))
        if self.pass_number == 1:
            self.pass_number = 2
            self.phase, self.page, self.current = 'head-start', 1, self._empty_pass()
            return
        first, second = self.passes
        if (set(first['registered']) != set(second['registered']) or first['retired'] != second['retired']):
            refuse('epoch_preparation_inventory_changed')
        if first['positive'] != second['positive']:
            refuse('epoch_preparation_distribution_changed')
        self.complete = True
        self.phase = 'complete'

    def _receipt(self, endpoint, response, receipt):
        if not isinstance(receipt, dict):
            refuse('epoch_preparation_receipt_invalid')
        network = self.profile.network
        if (receipt.get('source') != 'blockfrost-cardano' or receipt.get('shared_profile') != network.network
                or receipt.get('adapter') != network.adapter or receipt.get('openapi') != OPENAPI):
            refuse('epoch_preparation_receipt_invalid')
        requests = receipt.get('requests')
        if not isinstance(requests, list) or len(requests) > 4:
            refuse('epoch_preparation_receipt_invalid')
        matching = [r for r in requests if r.get('endpoint') == endpoint and r.get('sha256') == digest(response)
                    and r.get('bytes') == len(canonical(response).encode())]
        if len(matching) != 1 or type(matching[0].get('attempt')) is not int or matching[0]['attempt'] < 1:
            refuse('epoch_preparation_receipt_invalid')
        attempts = [record['provider']['requests'][-1]['attempt'] for record in self.records]
        if attempts and matching[0]['attempt'] <= attempts[-1]:
            refuse('epoch_preparation_receipt_invalid')
        if requests[-1] != matching[0]:
            refuse('epoch_preparation_receipt_invalid')

    def accept(self, endpoint, response, receipt):
        if self.complete or endpoint != self.request():
            refuse('epoch_preparation_transcript_invalid')
        self._receipt(endpoint, response, receipt)
        record = {'endpoint': endpoint, 'response': copy.deepcopy(response), 'provider': copy.deepcopy(receipt)}
        size = len(canonical(record).encode())
        if size > MAX_RECORD_BYTES or self.transcript_bytes + size > MAX_TRANSCRIPT_BYTES or len(self.records) >= MAX_RECORDS:
            refuse('epoch_preparation_size_limit')
        phase = self.phase
        if phase == 'genesis':
            if not isinstance(response, dict) or response.get('network_magic') != self.profile.network.magic:
                refuse('wrong_provider_network')
            self.phase = 'head-start'
        elif phase.startswith('head-'):
            self._observe(response)
            self.phase = 'summary-' + phase.split('-')[1]
        elif phase.startswith('summary-'):
            self._summary(response)
            self.phase = 'nonce-' + phase.split('-')[1]
        elif phase.startswith('nonce-'):
            self._nonce(response)
            self.phase = 'anchor-' + phase.split('-')[1]
        elif phase == 'anchor-start':
            self._anchor(response)
            self.phase = 'registered'
        elif phase == 'anchor-end':
            self._anchor(response)
            self._end_pass()
        elif phase in ('registered', 'retired'):
            self._inventory(response, phase == 'registered')
        elif phase == 'specific':
            self._specific(response)
        else:
            refuse()
        self.records.append(record)
        self.transcript_bytes += size

    def step(self, provider):
        endpoint = self.request()
        if endpoint is None:
            return False
        response = provider.get(endpoint)
        self.accept(endpoint, response, provider.drain())
        return True

    @classmethod
    def replay(cls, profile, epoch, records):
        if not isinstance(records, list) or len(records) > MAX_RECORDS:
            refuse('epoch_preparation_transcript_invalid')
        collector = cls(profile, epoch)
        for record in records:
            if not isinstance(record, dict) or set(record) != {'endpoint', 'response', 'provider'}:
                refuse('epoch_preparation_transcript_invalid')
            collector.accept(record['endpoint'], record['response'], record['provider'])
        return collector

    def result(self):
        if not self.complete:
            refuse('epoch_preparation_not_ready')
        positive = dict(sorted(self.passes[0]['positive'].items()))
        return {'version': VERSION, 'kind': 'current-pool-aggregate-two-independent-passes',
                'semantics': SEMANTICS, 'semantics_commit': SEMANTICS_COMMIT,
                'profile_sha256': digest(self.profile.identity), 'network': self.profile.network.network,
                'source_identity': self.profile.network.source_identity, 'epoch': self.epoch,
                'nonce': self.nonce, 'active_stake': str(self.total), 'anchor': self.anchor,
                'positive': positive, 'positive_sha256': digest(positive),
                'pass_sha256': [digest(p) for p in self.passes],
                'inventories_sha256': [digest({'registered': p['registered'], 'retired': p['retired']}) for p in self.passes],
                'transcript_sha256': digest(self.records), 'requests': len(self.records),
                'bytes': self.transcript_bytes}
