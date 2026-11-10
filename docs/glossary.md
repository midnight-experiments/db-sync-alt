# Glossary

## Profile

The pinned description of what Midnight needs from one Cardano network: six
configuration files copied unchanged from a Midnight node release
(`profiles/preprod-node-1.0.400/`, `profiles/mainnet-node-1.0.0/`,
`profiles/preview-node-1.0.0/`) and `network.json` with the network's values
([networks](networks.md)). The files name the Cardano assets, addresses and the
start point that matter to Midnight; db-sync-alt keeps only data related to
them. The SHA-256 of the files, the network and its Midnight genesis form the
profile identity; it is stored in the database and in every backup, and anything
built for another profile is refused.

## Start cursor

The first Cardano position Midnight reads, taken from the profile: block
4,500,197, transaction 4 on Preprod; block 13,164,005, transaction 1 on Mainnet;
block 0, transaction 0 on Preview. Events (transactions, outputs, mints) are kept
from there on; older blocks appear only when a later event needs them.

## Target

The highest Cardano block whose events a generation covers. A target is always
k blocks below the newest block the generation has seen, where k is the
network's security parameter (2160 on Mainnet and Preprod, 432 on Preview), so it
can no longer be rolled back.

## Witness

The newest block of a generation: its target + k. The generation stores every
block header from the start cursor up to the witness, linked parent to child,
which proves the target is k blocks deep. Headers above the target are
provisional: a later generation may replace them after a Cardano fork.

## Generation

One verified, append-only step of the data: the facts up to a target, the
headers up to the witness, and receipts of how they were obtained. Each new
generation builds on its parent and is checked (block chain, anchors, receipts,
fact digests, profile) before it is published. Exactly one generation is the
*ready* generation that the database serves; the next one stays *pending* until it
is complete and verified.

## Publication

Making a pending generation the ready one, in one database transaction under a
lock that readers share, so a query never sees half of an update.

## Coverage

The range the ready generation answers for: events from the start cursor through
the target, headers through the witness, the required epochs, and the profile's
addresses and assets. A correctly configured node asks only inside it; the
database cannot see a query's parameters, so a node whose k or addresses differ
from the profile gets empty answers outside it
([node requirements](connect-a-node.md#node-requirements)).

## Schemas 1 to 5

The layouts of a db-sync-alt database. A new database is created as schema 1;
`db-sync-alt upgrade` turns it into schema 5, which the running service needs.
Schema 2 adds a projection of the current block membership and lets the follower
publish small certified updates instead of re-checking all history each time.
Schema 3 serves the node directly: every node-facing relation calls the
[serving gate](#serving-gate), and a reader role gets read access to those
relations only (the service, `restore --reader-role`, `db-sync-alt grant-reader`).
Schema 4 adds [integrity checkpoints](#integrity-checkpoint), so verification
starts at the last one instead of the first generation. Schema 5 serves each
epoch's `max_tx_size` in `epoch_param`; a database written before it (fact
format `midnight-compact-cardano-v1`, nonces only) gets every stored epoch's
size from an authoritative source when it is upgraded. Backups record their schema.

## Integrity checkpoint

A record, for one fully verified generation, of the digests of every fact family
and of the generation's integrity root. Writing it moves the integrity base to
that generation in the same transaction, under the same gate as a publication.
Every later verification recomputes the base's digests and compares them with
the checkpoint (it never trusts them), then verifies only the updates published
after the base; a generation older than the base is not verified on its own.
Checkpoints are never changed or removed, form a chain back to the first
generation the base started from, and are not readable by the node's reader.
The service writes one after each startup verification and periodically while
it runs; `db-sync-alt upgrade` and the restore of a schema-3 backup write the
first one.

## Positional ID

The ID of a served block, transaction, output, input or output asset, computed
from its chain position with 12 bits per level: block = height (below 2^27),
transaction = height · 4096 + index in the block, output = transaction · 4096 +
output index, input = spending transaction · 4096 + [ledger ordinal](#ledger-ordinal),
output asset = output · 4096 + ledger ordinal. A value outside its field is
refused, never wrapped.

## Ledger ordinal

The position of an input among all ordinary inputs of its transaction (ordered by
origin transaction hash, then index) and of an asset among all assets of its
output (ordered by policy, then name, bytewise) — the order cardano-db-sync
stores them in. Kept beside each stored input and output asset; not part of the
fact digests.

## Serving gate

`chain.gate()`, called by every node-facing relation. It raises SQLSTATE 55000
(`db_sync_alt_not_serving`) unless the database serves a ready, unrevoked
canonical generation whose projection is current, so a query in any other state
fails instead of answering with missing rows.

## Snapshot

A db-sync-alt backup (a PostgreSQL archive plus its manifest) used as initial
data. The manifest records the archive's SHA-256, the profile, the verification
result of the canonical generation (of every generation in formats v1 and v2) and
the physical inventory; restoring checks all of them.
