# Limitations

What a db-sync-alt database does not serve, and what that means for a node.
`/status` lists the first three under `limitations`, each with a code.

## History older than the window

The database keeps the events of the window and the unspent set, and deletes
the rest (`history_below_horizon`). It cannot answer queries about history
older than the horizon:

- A node whose Cardano position is below the horizon (an old node snapshot, a
  sync from Midnight's genesis, a node stopped for longer than the retention)
  fails with its own "missing block" or "reference invalid" error. Start it from
  a newer node snapshot. The horizon is in `/status` (`horizon`,
  `retention.next_horizon_move_at`); db-sync-alt cannot see the node's
  position.
- A node whose position is exactly on one of the old governance blocks the
  database keeps finds that block, reads an incomplete range, and rejects the
  block by its own check: it stalls instead of following a wrong chain.
- A bridge checkpoint below the horizon, given as a block number, makes the
  node's bridge query read only from the horizon on. The bridge checkpoint is
  not set on Mainnet or Preprod.

There is no full-history mode. Whoever needs history keeps a 0.4.x database or
a cardano-db-sync.

## Registered candidates

The registered-candidate results of the node's operator RPCs and CLI commands
are not supported (`registered_candidates_unsupported`): the RPC methods
`sidechain_getRegistrations`, `sidechain_getAriadneParameters` and
`systemParameters_getAriadneParameters`, and the `registration-status` and
`ariadne-parameters` subcommands. Their answers can differ from those of a
cardano-db-sync database. Midnight node 2.1.101 does not read registrations for block
import or production: its committee selection uses the permissioned
candidates, which are served in full. A node release that reads registrations
again needs a new keep rule.

## The node's genesis subcommands

The node's genesis subcommands read headers and unspent outputs at an
arbitrary reference block (`genesis_subcommands_incomplete`):
`generate-genesis-config`, `generate-ics-genesis`, `generate-reserve-genesis`,
`verify-cardano-tip-finalized`, `verify-auth-script`,
`verify-federated-authority-auth-script`, `verify-ics-auth-script`,
`verify-permissioned-candidates-auth-script` and `verify-reserve-auth-script`.
For a reference below the horizon a pruned database answers incompletely, and
without an error. Run them against a cardano-db-sync database. `/status` lists
their statements under `unsupported_node_statements`.

## Stake distributions for recent epochs only

The stake distribution of an epoch (`epoch_stake`, `pool_hash`) is read in the
background once the database is at the tip, and kept from two epochs before
the horizon's epoch on. The three Ariadne and registration RPCs above are the
only readers of it; block import and production do not read it. They have no
stake for older epochs, and none for a new epoch until its distribution is
stored after the epoch starts. `epochs.newest_stake_epoch` in `/status` shows
the newest epoch with stake.

## Byron addresses in a non-canonical form

A Byron (bootstrap) address whose payload is not written in canonical CBOR is
valid on the ledger when its checksum matches the bytes as written.
db-sync-alt stores such an address as written, while cardano-db-sync stores the
canonical form, so the two texts differ. An output at an address is stored
only when it carries an asset the node reads, such as cNIGHT, and such an
address has to be made by hand: none occurs in the real transactions the tests
record. If one occurs, a node served by db-sync-alt reads another holder
address text for that output than a node served by cardano-db-sync, and the
content digest of a followed database differs from a conversion's.

## Networks and node release

Mainnet and Preprod only. The keep rule and the profiles are bound to Midnight
node 2.1.101's queries and configuration; another node release works only if
it sends the same queries and reads the same addresses and assets
([compatibility](compatibility.md)).

## One provider

Blockfrost is the only provider, read once per block. A provider that answers
a wrong but self-consistent list of transactions for a final block is not
detected; the hash, count, link and conservation checks catch any answer that
contradicts itself or the chain.
