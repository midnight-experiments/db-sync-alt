"""Payments to profile addresses carrying datums that anyone can put on chain, and what cardano-db-sync
13.8 serves for them (`datum.value`, `tx_out.data_hash`).

The served values are built from how each datum is made, independently of db-sync-alt's decoder.
cardano-db-sync stores a datum's value as NULL when its JSON nests deeper than 2000 levels.
"""
from dataclasses import dataclass
import hashlib
from db_sync_alt.contract import Profile
from db_sync_alt.resources import profile_dir

PROFILE = Profile(profile_dir('preprod'))
ICS = PROFILE.configs['ics-config']['illiquid_circulation_supply_validator_address']
RESERVE = PROFILE.configs['reserve-config']['reserve_validator_address']
MAPPING = PROFILE.mapping
GOVERNANCE = next(iter(PROFILE.configs['federated-authority-config'].values()))['address']


@dataclass(frozen=True)
class Payment:
    """One output at `address` whose datum is `datum` (CBOR): inline, or only its hash."""
    address: str
    datum: bytes
    served: object  # db-sync's datum.value: the JSON, or None for NULL
    inline: bool = True
    on_chain: bool = True  # the preimage is on chain (Blockfrost answers it)
    height: int = 12

    @property
    def hash(self):
        return hashlib.blake2b(self.datum, digest_size=32).digest()


def nested_lists(levels):
    """(CBOR, db-sync value) of `levels` nested lists around 0: JSON 2*levels+1 deep."""
    value = {'int': 0}
    for _ in range(levels):
        value = {'list': [value]}
    return b'\x81' * levels + b'\x00', value if 2 * levels + 1 <= 2000 else None


def nested_constructors(levels):
    """(CBOR, db-sync value) of `levels` nested `Constr 0 [...]` around 0: JSON 2*levels+1 deep."""
    value = {'int': 0}
    for _ in range(levels):
        value = {'constructor': 0, 'fields': [value]}
    return b'\xd8\x79\x81' * levels + b'\x00', value if 2 * levels + 1 <= 2000 else None


REGISTRATION = (b'\xd8\x79\x82\x58\x1c' + b'\x01' * 28 + b'\x07', {'constructor': 0, 'fields': [{'bytes': '01' * 28}, {'int': 7}]})
GOVERNANCE_DATUM = (b'\xd8\x7a\x81\x41\x02', {'constructor': 1, 'fields': [{'bytes': '02'}]})
REPEATED_KEY = (b'\xa2\x01\x00\x01\x02', {'map': [{'k': {'int': 1}, 'v': {'int': 0}}, {'k': {'int': 1}, 'v': {'int': 2}}]})

# The regression block: hostile datums at the ICS and reserve addresses (deep inline: NULL past the
# limit and exact just below it, a repeated map key, hashes whose preimage is not on chain) next to
# ordinary cNIGHT and governance datums.
HOSTILE = (
    Payment(ICS, *nested_lists(1000)),
    Payment(ICS, *nested_lists(999)),
    Payment(ICS, b'\x43ics', None, inline=False, on_chain=False),
    Payment(RESERVE, *nested_constructors(1000)),
    Payment(RESERVE, *REPEATED_KEY),
    Payment(RESERVE, b'\x43res', None, inline=False, on_chain=False),
    Payment(MAPPING, *REGISTRATION),
    Payment(GOVERNANCE, *GOVERNANCE_DATUM, inline=False),
)


def served(store, payments):
    """What the store serves for `payments`: ({address, data_hash} rows, {hash: value} of datum rows)."""
    outputs = sorted(store.db.execute('SELECT address,data_hash FROM public.tx_out WHERE address=ANY(%s) AND data_hash IS NOT NULL',
                                      ([p.address for p in payments],)).fetchall())
    datums = dict(store.db.execute('SELECT hash,value FROM public.datum WHERE hash=ANY(%s)', ([p.hash for p in payments],)).fetchall())
    return [(a, bytes(h)) for a, h in outputs], {bytes(h): v for h, v in datums.items()}


def expected(payments):
    """What cardano-db-sync serves for `payments`: every output keeps its hash; a datum row only when the preimage is on chain."""
    outputs = sorted((p.address, p.hash) for p in payments)
    return outputs, {p.hash: p.served for p in payments if p.on_chain}
