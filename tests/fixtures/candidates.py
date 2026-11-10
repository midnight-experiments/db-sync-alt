# SPDX-License-Identifier: Apache-2.0
"""Payments to the registered-candidates address (synthetic, Preprod), anyone can make them.

Block h=12 of `fixtures.blockfrost_generation` (fork 1) holds its relevant mint and three more transactions,
each spending a wallet output db-sync-alt does not hold:

  1  1 ADA to the candidates address, no datum
  2  1 ADA to the candidates address with an inline datum and an asset outside the profile
  3  NIGHT to the candidates address with an inline datum (a profile asset: kept as at any address)

`routes(payments)` puts the chosen payments at those positions and a clean unrelated transaction at the
others; `routes(())` is the same block without any payment. Block headers are equal in every variant.
"""
import hashlib

import fixtures.blockfrost_generation as base
from fixtures.cbor import tx

PROFILE = base.PROFILE
CANDIDATES = PROFILE.configs['registered-candidates-addresses']['committee_candidates_address']
OTHER = (b'\x0f' * 28).hex() + b'OTHER'.hex()
# Constr 0 [42]: the datum and the JSON Blockfrost renders for it.
DATUM = bytes.fromhex('d87981182a')
DATUM_HASH = hashlib.blake2b(DATUM, digest_size=32).hexdigest()
DATUM_JSON = {'constructor': 0, 'fields': [{'int': 42}]}
PLAIN, WITH_DATUM, NIGHT = 1, 2, 3
POSITIONS = (PLAIN, WITH_DATUM, NIGHT)


def payment(index):
    """(CBOR hex, hash hex, provider output row) of payment `index` in block h=12."""
    wallet = hashlib.sha256(b'candidate-wallet-%d' % index).digest()
    raw, th = tx({0: [[wallet, 0]], 1: [[b'\x70' + bytes([index]) * 28, 1000000]], 2: 200 + index})
    amount = [{'unit': 'lovelace', 'quantity': '1000000'}]
    if index == WITH_DATUM:
        amount.append({'unit': OTHER, 'quantity': '1'})
    if index == NIGHT:
        amount.append({'unit': PROFILE.night, 'quantity': '5'})
    row = {'output_index': 0, 'address': CANDIDATES, 'amount': amount, 'data_hash': None}
    if index != PLAIN:
        row.update(data_hash=DATUM_HASH, inline_datum=DATUM.hex())
    source = {'tx_hash': wallet.hex(), 'output_index': 0, 'address': 'addr_test1wallet', 'data_hash': None,
              'amount': [{'unit': 'lovelace', 'quantity': '1200000'}], 'reference': False, 'collateral': False}
    return raw, th, row, source


def routes(payments=POSITIONS):
    """Provider routes of block h=12 with `payments` (a subset of POSITIONS) and clean transactions elsewhere."""
    out, _ = base.fixtures(fork=1, witnesses=base.CLEAN_WITNESSES)
    block = base.block(12, 1, False, {12: len(POSITIONS)})
    listing = out['/blocks/' + block['hash'] + '/txs/cbor?count=100&page=1&order=asc']
    for index in payments:
        raw, th, row, source = payment(index)
        listing[index] = {'tx_hash': th, 'cbor': raw}
        out['/txs/' + th + '/cbor'] = {'cbor': raw}
        out['/txs/' + th] = {'hash': th, 'block': block['hash'], 'block_height': base.OFFSET + 12, 'index': index, 'valid_contract': True}
        out['/txs/' + th + '/utxos'] = {'hash': th, 'inputs': [source], 'outputs': [row]}
    out['/scripts/datum/' + DATUM_HASH] = {'json_value': DATUM_JSON}
    return out


def transaction(index):
    """The hash of payment `index`, as bytes."""
    return bytes.fromhex(payment(index)[1])
