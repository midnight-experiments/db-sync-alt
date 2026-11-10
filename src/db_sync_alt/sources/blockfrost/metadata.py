"""Bridge metadata of one transaction from Blockfrost, checked against the transaction's own CBOR.

One counted `GET /txs/{hash}/metadata` per transaction with an output at the ICS address in a
covered block. Blockfrost's `json_metadata` is cardano-db-sync's `tx_metadata.json` after a
JavaScript round trip; it is stored as jsonb with this normalization only:

- the label `"6500973"` alone is kept; a transaction without it is stored with `present` 0;
- a number written as a float with an integral value (`1e+21`, from an integer beyond 2^53) is an
  integer; a non-finite number is a provider error;
- like cardano-db-sync 13.8, a value containing U+0000 (which jsonb cannot hold), text that is not
  Unicode, or arrays and objects nested deeper than 2000 is NULL (`present` 1, `json` None).

Big integers beyond 2^53 therefore keep Blockfrost's rounded value: midnight-node reads only a
one-element array holding a `0x` string, so its decision is the same as on cardano-db-sync.

The check needs no further request: the auxiliary data in the transaction CBOR (bound to the
transaction hash through the body's auxiliary data hash) must agree on whether the label is present
and on the recipient the node would read. A value the CBOR reader cannot settle is never a refusal.
"""
import hashlib
import json
import math

from db_sync_alt.bridge import recipient
from db_sync_alt.contract import BRIDGE_METADATA_KEY, Refusal, canonical
from db_sync_alt.sources.blockfrost.cbor import ABSENT, UNKNOWN, bridge_metadatum

LABEL = str(BRIDGE_METADATA_KEY)
MAX_NESTING = 2000


def _integral(text):
    value = float(text)
    return int(value) if value.is_integer() else value


def normalize(value):
    """The stored form of a `json_metadata` value: a JSON value, or None where cardano-db-sync stores NULL."""
    stack = [(value, 0)]
    while stack:
        item, depth = stack.pop()
        if isinstance(item, (list, dict)):
            depth += 1
            if depth > MAX_NESTING:
                return None
            if isinstance(item, dict):
                for key, child in item.items():
                    stack.append((key, depth))
                    stack.append((child, depth))
            else:
                stack.extend((child, depth) for child in item)
        elif isinstance(item, str):
            try:
                item.encode('utf-8')
            except UnicodeEncodeError:
                return None
            if '\x00' in item:
                return None
        elif isinstance(item, float):
            if not math.isfinite(item):
                raise Refusal('provider_bad_metadata')
        elif item is not None and not isinstance(item, (bool, int)):
            raise Refusal('provider_bad_metadata')
    # Within MAX_NESTING the standard encoder and decoder do not run out of recursion.
    return json.loads(json.dumps(value, ensure_ascii=False, allow_nan=False), parse_float=_integral)


def provider_metadatum(response):
    """(present, value) of the bridge label in a `/txs/{hash}/metadata` response."""
    if not isinstance(response, list):
        raise Refusal('provider_bad_metadata')
    found = []
    for row in response:
        if not isinstance(row, dict) or not isinstance(row.get('label'), str) or 'json_metadata' not in row:
            raise Refusal('provider_bad_metadata')
        if row['label'] == LABEL:
            found.append(row['json_metadata'])
    if len(found) > 1:
        raise Refusal('provider_bad_metadata')
    if not found:
        return False, None
    return True, normalize(found[0])


def cross_check(decoded, present, value):
    """The provider's metadatum against the hash-bound auxiliary data of the same transaction."""
    aux, declared = decoded.get('auxiliary_data'), decoded.get('auxiliary_data_hash')
    if (aux is None) != (declared is None) or (aux is not None and hashlib.blake2b(aux, digest_size=32).digest() != declared):
        raise Refusal('provider_metadata_conflict')
    state, expected = bridge_metadatum(aux, BRIDGE_METADATA_KEY)
    if state == UNKNOWN:
        return
    if present != (state != ABSENT) or (present and recipient(value) != expected):
        raise Refusal('provider_metadata_conflict')


def fact(tx_hash, present, value):
    return {'tx_hash': tx_hash, 'present': 1 if present else 0, 'json': canonical(value) if present and value is not None else None}


def fetch(provider, decoded):
    """The tx_metadata fact of one transaction: one counted request, checked against its CBOR."""
    h = decoded['hash']
    response = provider.get('/txs/' + h.hex() + '/metadata')
    present, value = provider_metadatum(response)
    cross_check(decoded, present, value)
    return fact(h, present, value)


def stored_agrees(decoded, row):
    """A stored fact against the transaction's CBOR (the hand-off check); no request is made."""
    try:
        cross_check(decoded, row['present'] == 1, None if row['json'] is None else json.loads(row['json']))
    except Refusal:
        return False
    return True
