"""Token bridge transfers on Preprod (synthetic): Blockfrost routes and the cardano-db-sync rows of the same chain.

Generation 0 is the synthetic generation of `fixtures.blockfrost_generation` (target h=10); the next
update reads blocks h=11 and h=12, which hold the bridge scenario:

  h=11  0 transfer to the ICS address, recipient as CBOR bytes        (100 NIGHT)
        1 transfer, recipient as `0x` text                             (50)
        2 transfer, metadatum of another shape (no recipient)          (7)
        3 transfer without the bridge label (another label only)       (5)
        4 transfer whose metadatum holds U+0000 (NULL in db-sync 13.8) (3)
        5 deposit to the reserve address (no ICS output)               (1000)
  h=12  0 transfer, metadatum 2^60 (Blockfrost rounds it)               (2)
        1 transfer, metadatum ["\\x01"] (NULL from a 13.7-based provider) (1)
        2 reserve transfer: spends the reserve deposit, 900 back to the reserve, 100 to the ICS address
        3 spend of the ICS output of h=11 #0 (100) with 120 back to the ICS address (a transfer of 20)
        4 transfer carrying an asset outside the profile beside NIGHT   (10 NIGHT, 5 OTHER)

`db_sync_rows` gives the same chain as cardano-db-sync 13.8 stores it (its own JSON rendering of the
metadata, all labels), for a db-sync-shaped reference database.
"""
import hashlib
import json

import contextlib
import datetime as dt
from unittest.mock import patch

from db_sync_alt.contract import BRIDGE_METADATA_KEY
from db_sync_alt.sources.blockfrost.cbor import Tag
from fixtures.cbor import enc
import fixtures.blockfrost_generation as base

OFFSET, PROFILE = base.OFFSET, base.PROFILE
KEY = BRIDGE_METADATA_KEY
ICS = PROFILE.ics
RESERVE = PROFILE.configs['reserve-config']['reserve_validator_address']
ASSET = PROFILE.configs['ics-config']['asset']
NIGHT = (bytes.fromhex(ASSET['policy_id'].removeprefix('0x')), ASSET['asset_name'].encode())
OTHER = (b'\x0f' * 28, b'OTHER')
TX_COUNT = {10: 5, 11: 6, 12: 5}
TARGET = OFFSET + 12
RECIPIENTS = [hashlib.sha256(b'recipient-%d' % n).digest() for n in range(4)]


def block(h):
    bh = lambda n: n.to_bytes(32, 'big').hex()
    return {'hash': bh(h), 'height': OFFSET + h, 'previous_block': bh(h - 1), 'epoch': 0 if h == 8 else 1 if h == 9 else 2,
            'slot': h * 20, 'time': 1700000000 + h, 'tx_count': TX_COUNT.get(h, 0)}


def unit(asset):
    return asset[0].hex() + asset[1].hex()


def output(index, address, coin, assets=()):
    return {'output_index': index, 'address': address, 'data_hash': None,
            'amount': [{'unit': 'lovelace', 'quantity': str(coin)}] + [{'unit': unit(a), 'quantity': str(q)} for a, q in assets]}


def metadata_cbor(metadata):
    """Auxiliary data (Alonzo form, tag 259) holding the metadata map, or None."""
    return None if metadata is None else enc(Tag(259, {0: metadata}))


class Tx:
    """One transaction: CBOR (with its auxiliary data), its provider rows and its db-sync metadata rows."""
    def __init__(self, height, index, *, spends=(), outputs, metadata=None, provider=None, dbsync=None):
        self.height, self.index, self.spends, self.outputs = height, index, list(spends), outputs
        user = hashlib.sha256(b'user-%d-%d' % (height, index)).digest()
        self.inputs = [(user, 0)] + [(origin.hash, i) for origin, i in self.spends]
        aux = metadata_cbor(metadata)
        body = {0: [[h, i] for h, i in sorted(self.inputs)], 1: [[b'\x60' + bytes([n]) * 28, 1] for n in range(len(outputs))], 2: 3}
        if aux is not None:
            body[7] = hashlib.blake2b(aux, digest_size=32).digest()
        raw = enc(body)
        self.hash = hashlib.blake2b(raw, digest_size=32).digest()
        self.cbor = (b'\x84' + raw + enc({}) + enc(True) + (aux if aux is not None else enc(None))).hex()
        # Blockfrost's /txs/{hash}/metadata: db-sync's json after a JavaScript round trip.
        self.provider = [] if provider is None else provider
        # cardano-db-sync 13.8's tx_metadata rows: (key, json).
        self.dbsync = [] if dbsync is None else dbsync
        self.user = output(0, 'addr_test1user', 5000000)

    def input_rows(self):
        rows = [dict(self.user, tx_hash=self.inputs[0][0].hex(), output_index=0, reference=False, collateral=False)]
        for origin, i in self.spends:
            rows.append(dict(origin.outputs[i], tx_hash=origin.hash.hex(), output_index=i, reference=False, collateral=False))
        return rows

    @property
    def ics_producing(self):
        return any(o['address'] == ICS for o in self.outputs)


def transfer(height, index, amount, metadata, provider, dbsync, *, assets=None, spends=()):
    outputs = [output(0, ICS, 2000000, assets or [(NIGHT, amount)]), output(1, 'addr_test1change', 1000000)]
    return Tx(height, index, spends=spends, outputs=outputs, metadata=metadata, provider=provider, dbsync=dbsync)


def scenario():
    hexed = lambda b: '0x' + b.hex()
    text = '0x' + RECIPIENTS[1][:20].hex()
    t11 = [
        transfer(11, 0, 100, {KEY: [RECIPIENTS[0]]}, [{'label': '6500973', 'json_metadata': [hexed(RECIPIENTS[0])]}], [(KEY, [hexed(RECIPIENTS[0])])]),
        transfer(11, 1, 50, {KEY: [text]}, [{'label': '6500973', 'json_metadata': [text]}], [(KEY, [text])]),
        transfer(11, 2, 7, {KEY: ['abc']}, [{'label': '6500973', 'json_metadata': ['abc']}], [(KEY, ['abc'])]),
        transfer(11, 3, 5, {674: {'msg': ['hi']}}, [{'label': '674', 'json_metadata': {'msg': ['hi']}}], [(674, {'msg': ['hi']})]),
        transfer(11, 4, 3, {KEY: ['a\x00b']}, [{'label': '6500973', 'json_metadata': None}], [(KEY, None)]),
        Tx(11, 5, outputs=[output(0, RESERVE, 2000000, [(NIGHT, 1000)])]),
    ]
    t12 = [
        transfer(12, 0, 2, {KEY: 2**60}, [{'label': '6500973', 'json_metadata': 1152921504606847000}], [(KEY, 2**60)]),
        transfer(12, 1, 1, {KEY: ['\x01']}, [{'label': '6500973', 'json_metadata': None}], [(KEY, ['\x01'])]),
        Tx(12, 2, spends=[(t11[5], 0)], outputs=[output(0, RESERVE, 2000000, [(NIGHT, 900)]), output(1, ICS, 2000000, [(NIGHT, 100)])]),
        transfer(12, 3, 120, {KEY: [RECIPIENTS[2]]}, [{'label': '6500973', 'json_metadata': [hexed(RECIPIENTS[2])]}], [(KEY, [hexed(RECIPIENTS[2])])],
                 spends=[(t11[0], 0)]),
        transfer(12, 4, 10, {KEY: [RECIPIENTS[3]]}, [{'label': '6500973', 'json_metadata': [hexed(RECIPIENTS[3])]}], [(KEY, [hexed(RECIPIENTS[3])])],
                 assets=[(NIGHT, 10), (OTHER, 5)]),
    ]
    return {11: t11, 12: t12}


def routes(chain=None):
    """Fake Blockfrost routes: generation 0's fixture routes with this scenario's blocks h=11, 12."""
    chain = chain or scenario()
    out, _ = base.fixtures()
    for h in range(8, 2173):
        b = block(h)
        out['/blocks/' + str(OFFSET + h)] = b
        out['/blocks/' + b['hash']] = b
        out['/blocks/' + b['hash'] + '/next?count=100&page=1'] = [block(n) for n in range(h + 1, min(h + 101, 2173))]
    out['/blocks/latest'] = block(2172)
    for h, txs in chain.items():
        b = block(h)
        out['/blocks/' + b['hash'] + '/txs/cbor?count=100&page=1&order=asc'] = [{'tx_hash': t.hash.hex(), 'cbor': t.cbor} for t in txs]
        for t in txs:
            th = t.hash.hex()
            out['/txs/' + th + '/cbor'] = {'cbor': t.cbor}
            out['/txs/' + th] = {'hash': th, 'block': b['hash'], 'block_height': OFFSET + h, 'index': t.index, 'valid_contract': True}
            out['/txs/' + th + '/utxos'] = {'hash': th, 'inputs': t.input_rows(), 'outputs': t.outputs}
            out['/txs/' + th + '/metadata'] = t.provider
    return out


def baseline(conninfo):
    """Generation 0 (as `blockfrost_generation.baseline`) with this scenario's headers; returns the open store."""
    with contextlib.ExitStack() as stack:
        stack.enter_context(patch.object(base, 'block', lambda h, fork=0, transition=False: block(h)))
        return base.baseline(conninfo)


def db_sync_rows(chain=None):
    """The scenario's chain as cardano-db-sync stores it: rows per table of the db-sync-shaped schema."""
    chain = chain or scenario()
    rows = {t: [] for t in ('block', 'tx', 'tx_out', 'tx_in', 'multi_asset', 'ma_tx_out', 'tx_metadata')}
    assets = {NIGHT: 1, OTHER: 2}
    rows['multi_asset'] = [(i, p, n) for (p, n), i in assets.items()]
    tx_ids, out_ids, consumed = {}, {}, {}
    for h, txs in chain.items():
        b = block(h)
        rows['block'].append((OFFSET + h, bytes.fromhex(b['hash']), b['epoch'], b['slot'], OFFSET + h, b['time'], b['tx_count']))
        for t in txs:
            tx_ids[t.hash] = len(tx_ids) + 1
            for o in t.outputs:
                out_ids[(t.hash, o['output_index'])] = len(out_ids) + 1
            for origin, i in t.spends:
                consumed[(origin.hash, i)] = tx_ids[t.hash]
    for h, txs in chain.items():
        for t in txs:
            tid = tx_ids[t.hash]
            rows['tx'].append((tid, t.hash, OFFSET + h, t.index, True))
            for o in t.outputs:
                oid = out_ids[(t.hash, o['output_index'])]
                rows['tx_out'].append((oid, tid, o['output_index'], o['address'], int(o['amount'][0]['quantity']), None, consumed.get((t.hash, o['output_index']))))
                for a in o['amount'][1:]:
                    key = (bytes.fromhex(a['unit'][:56]), bytes.fromhex(a['unit'][56:]))
                    rows['ma_tx_out'].append((len(rows['ma_tx_out']) + 1, int(a['quantity']), oid, assets[key]))
            for origin, i in t.spends:
                rows['tx_in'].append((len(rows['tx_in']) + 1, tid, tx_ids[origin.hash], i))
            for key, value in t.dbsync:
                rows['tx_metadata'].append((len(rows['tx_metadata']) + 1, key, tid, None if value is None else json.dumps(value)))
    return rows


def load_reference(db, rows):
    """Write `db_sync_rows` into a database holding the db-sync-shaped schema."""
    for b in rows['block']:
        db.execute('INSERT INTO block VALUES(%s,%s,%s,%s,%s,%s,%s)', (*b[:5], dt.datetime.fromtimestamp(b[5], dt.timezone.utc).replace(tzinfo=None), b[6]))
    for table, width in (('tx', 5), ('tx_out', 7), ('tx_in', 4), ('multi_asset', 3), ('ma_tx_out', 4)):
        for row in rows[table]:
            db.execute('INSERT INTO ' + table + ' VALUES(' + ','.join(['%s'] * width) + ')', row)
    for row in rows['tx_metadata']:
        db.execute('INSERT INTO tx_metadata VALUES(%s,%s,%s,%s::jsonb)', row)

