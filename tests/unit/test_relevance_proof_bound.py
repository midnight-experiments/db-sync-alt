"""The relevance proof bound holds for the largest proof a valid block can produce.

The block body is filled up to Conway's maxBlockBodySize with transactions of at most maxTxSize that pay the
bridge-covered ICS address (every asset kept) values of at most maxValueSize, each asset new to the block, names
chosen cheapest first. The proof is built as `BlockfrostSource.block_facts` builds it.
"""
import hashlib
import unittest

from db_sync_alt.contract import Profile, canonical, jsonable
from db_sync_alt.resources import profile_dir
from db_sync_alt.sources.blockfrost.relevance import KIND, MAX_BLOCK_BODY, MAX_KEYS, MAX_PROOF, address_bytes, inventory
from fixtures.cbor import enc

MAX_TX, MAX_VALUE = 16384, 5000
PROFILE = Profile(profile_dir('mainnet'))
ICS = address_bytes(PROFILE.ics, 'mainnet')
NAMES = [b''] + [bytes([i]) for i in range(256)] + [bytes([i, j]) for i in range(256) for j in range(256)]
WITNESS = 110  # one vkey witness and the transaction envelope


def policy(*parts):
    return hashlib.blake2b(b''.join(p.to_bytes(4, 'big') for p in parts), digest_size=28).digest()


def value(policies, per_policy):
    return [10**7, {p: {n: 1 for n in NAMES[:per_policy]} for p in policies}]


def block_proof(per_policy):
    """(body bytes, keys, proof bytes) of a block filled with ICS outputs whose values hold as many policies of
    `per_policy` cheapest names as maxValueSize allows."""
    facts, used, txs, serial = [], 0, 0, 0
    while True:
        outputs = []
        while True:
            policies = []
            while len(enc(value(policies + [policy(serial, len(policies))], per_policy))) <= MAX_VALUE:
                policies.append(policy(serial, len(policies)))
            serial += 1
            trial = outputs + [[ICS, value(policies, per_policy)]]
            if len(enc({0: [[bytes(32), 0]], 1: trial, 2: 200000})) + WITNESS > MAX_TX:
                break
            outputs = trial
        size = len(enc({0: [[bytes(32), 0]], 1: outputs, 2: 200000})) + WITNESS
        if not outputs or used + size > MAX_BLOCK_BODY:
            break
        used += size
        h = hashlib.blake2b(enc(outputs) + txs.to_bytes(4, 'big'), digest_size=32).digest()
        facts += [('transactions', {'hash': h, 'block_hash': bytes(32), 'tx_index': txs, 'valid': 1}),
                  ('tx_metadata', {'tx_hash': h, 'present': 0, 'json': None})]
        for index, (_, (coin, multi)) in enumerate(outputs):
            facts.append(('outputs', {'tx_hash': h, 'output_index': index, 'address': PROFILE.ics, 'value': str(coin), 'datum_hash': None}))
            ordinal = 0
            for p in sorted(multi):
                for name in sorted(multi[p]):
                    facts.append(('assets', {'policy': p, 'name': name}))
                    facts.append(('output_assets', {'tx_hash': h, 'output_index': index, 'policy': p, 'name': name, 'quantity': '1', 'ordinal': ordinal}))
                    ordinal += 1
        txs += 1
    created = sorted({(row['tx_hash'], row['output_index']) for table, row in facts if table == 'outputs'})
    entries = inventory(facts)
    proof = {'kind': KIND, 'version': 1, 'parent_ref': {'kind': 'parent-delta-v2', 'sha256': '0' * 64}, 'inventory': entries,
             'transactions': txs, 'transaction_hashes_sha256': '0' * 64, 'created_outputs': jsonable(created)}
    return used, sum(entry['count'] for entry in entries.values()), len(canonical(proof).encode())


class ProofBoundTests(unittest.TestCase):
    def test_the_largest_proof_of_a_valid_block_fits_the_bound(self):
        results = {per_policy: block_proof(per_policy) for per_policy in (1, 257, 1304)}
        for per_policy, (used, keys, size) in results.items():
            with self.subTest(names_per_policy=per_policy):
                self.assertLessEqual(used, MAX_BLOCK_BODY)
                self.assertLessEqual(size, MAX_PROOF)
                self.assertLessEqual(keys, MAX_PROOF // 63)
                self.assertLess(keys, MAX_KEYS)
        # Policies of the 257 cheapest names (1 empty, 256 one-byte) give the largest proof, above the former 2 MiB.
        worst = max(results.values(), key=lambda r: r[2])
        self.assertEqual(worst, results[257])
        self.assertGreater(worst[2], 2 * 1024 * 1024)
        self.assertGreater(worst[2], MAX_PROOF * 0.85)


if __name__ == '__main__':
    unittest.main(verbosity=2)
