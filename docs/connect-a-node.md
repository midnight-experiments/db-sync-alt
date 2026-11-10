# Connect a Midnight node

A Midnight node reads Cardano data with SQL through its db-sync settings. It
connects to db-sync-alt's PostgreSQL database directly, exactly as it would to a
cardano-db-sync database, with the reader login you created for it
([provision PostgreSQL](operations.md#provision-postgresql)).

## Settings

```sh
DB_SYNC_POSTGRES_CONNECTION_STRING='postgresql://db_sync_alt_reader:<password>@<host>:5432/db_sync_alt'
CARDANO_SECURITY_PARAMETER=2160
```

| Setting | Value |
| --- | --- |
| Host and port | Your PostgreSQL server as the node reaches it |
| Database | db-sync-alt's database (`db_sync_alt` in the examples) |
| User and password | The reader login and its password, never the owner login db-sync-alt runs as |
| TLS | Your server's configuration, as for cardano-db-sync. With TLS on the server the node uses it; without it the node needs `ALLOW_NON_SSL=true`, and the connection must stay on loopback or a private network |
| `CARDANO_SECURITY_PARAMETER` | The network's security parameter k (2160 on Mainnet and Preprod, 432 on Preview); it must equal the profile's k (see below) |

With the [Compose deployment](docker.md) PostgreSQL is not published on the
host: attach the node's container to the deployment's `node` network and use
host `postgres`, port 5432; the reader's password is
`~/.local/share/db-sync-alt/private/operator/reader.password`.

## Node requirements

db-sync-alt cannot see the node's settings; a node that does not match the
profile gets empty answers where the database cannot tell its request apart
from a valid one:

- **k.** `CARDANO_SECURITY_PARAMETER` must equal the profile's k. db-sync-alt
  publishes block headers up to k blocks above the newest block it serves events
  for; a node with a smaller k treats younger blocks as stable and asks for
  events db-sync-alt has not published yet. A larger k is harmless.
- **Addresses and policies.** The node's Cardano addresses and policies (cNIGHT
  mapping validator and asset, governance bodies, candidates, reserve and ICS)
  must be the profile's ([networks](networks.md)). They come from the node's
  network configuration, and Midnight governance can change some of them on
  chain; when that happens, update db-sync-alt's profile before the change takes
  effect, or the node reads an address db-sync-alt does not follow.

Give the service the node's values (`node` in the service configuration,
[operations](operations.md#node-requirements)) and `/status` and `/health` report
any mismatch in `warnings`; they never refuse serving because of it.

## What the node can read

The reader login can connect to db-sync-alt's database, read the eleven
db-sync-shaped relations the node's queries use (`block`, `tx`, `tx_out`,
`tx_in`, `ma_tx_out`, `multi_asset`, `datum`, `epoch_param`, `epoch_stake`,
`pool_hash`, `tx_metadata`) and run the serving check they call; nothing else.
The node's SQL runs unchanged. `epoch_param` includes each epoch's
`max_tx_size`: midnight-node 2.1.101 reads `MAX(epoch_param.max_tx_size)` of its
cursor block's epoch once its runtime selects the cost-based cNIGHT observation,
and stops cNIGHT observations without it. `tx_metadata` holds the bridge
metadata of the [bridge coverage](#bridge-readiness). Each of those relations raises SQLSTATE 55000
(`db_sync_alt_not_serving`) instead of answering while the database does not
hold a verified, published update (empty, restoring, revoked, or an update not
yet complete), so the node never accepts an empty answer from a database that is
not serving. Every statement sees one published update, also while the next one
is being published. The node keeps reading the last published update while
db-sync-alt is stopped or restarting; `/health` then reports how far behind it
is.

The reader cannot connect at all until db-sync-alt has restored or verified the
database and granted it access: a node connected to an empty database would
find no `tx_in` relation and remember that until it restarts.

## Bridge readiness

midnight-node 2.1.101 builds and checks the token bridge's inherent data on every
block import, also when it only follows the chain. While the bridge's on-chain
data checkpoint (`Bridge.DataCheckpoint`) is unset, the bridge reads nothing; that
is the state of Mainnet and Preprod from genesis. One governance call
(`bridge.setMainChainScripts`) sets the scripts and the checkpoint, with no node
or runtime release. From then on, every block import runs the node's bridge
query from the checkpoint on: the transactions that send the bridge token to the
ICS address, the token flows of the ICS and reserve addresses, and the
`tx_metadata.json` of key 6500973 (the transfer's recipient) of those
transactions. The answer must equal what the block's author computed; a
different answer stops the node at that block.

db-sync-alt serves that data forward only, from its **coverage start**:

- Every update acquired from Blockfrost stores the metadata of key 6500973 of
  each transaction with an output at the ICS address (one request per such
  transaction), and every asset of the ICS and reserve outputs it stores.
- A database built from cardano-db-sync has no bridge metadata: coverage starts
  with the first update from Blockfrost after it. A database upgraded from 0.3.x
  starts coverage after its upgraded update. Nothing before the coverage start is
  fetched or guessed.
- An update from cardano-db-sync (`acquire --source dbsync`, for example to
  catch up after a provider outage) holds no metadata either: coverage then
  starts again with the next update from Blockfrost, and `coverage_start` moves
  past it. `acquire` says so when it publishes such an update after covered
  blocks: its result carries `bridge_coverage_ended` (the previous and the new
  coverage start) and the warning `bridge_metadata_coverage_restarted`, also
  printed on stderr. Once the bridge is active, keep following from Blockfrost
  and state the checkpoint (below), so that such a gap shows as an unsupported
  checkpoint.

`db-sync-alt status` and the service's `/status` report it under `bridge`:

```json
{"bridge": {"metadata_key": 6500973, "coverage_start": 4612001, "checkpoint": null, "checkpoint_supported": null, "reason": null}}
```

`coverage_start` is the first Cardano block from which the bridge query reads
complete data. The node's checkpoint is supported when the query only reads
covered blocks: `Block(n)` needs `n + 1 >= coverage_start` (the node reads the
blocks after `n`); `Tx(h)` or `TxReserveTransfer(h)` needs transaction `h` served
in a block at or after `coverage_start`.

Check the checkpoint before the governance call takes effect, and again after:

1. Read the storage item `Bridge.DataCheckpoint` from a Midnight node of the
   network, for example over RPC:

   ```sh
   curl -s -H 'Content-Type: application/json' http://127.0.0.1:9944 \
     -d '{"id":1,"jsonrpc":"2.0","method":"state_getStorage","params":["0x6bbd34f158a9e1a5e5600b03adee7932fd4cc2f0df8f898480e1be4bf3052e3e"]}'
   ```

   `"result": null` means the bridge is not active. Otherwise decode the value
   with the node's metadata (for example the chain state view of a Polkadot
   JavaScript client: `bridge.dataCheckpoint`); it names a Cardano block
   number or a transaction hash.
2. State it: `db-sync-alt status --service <name> --bridge-checkpoint block:<n>`
   (or `tx:<hash>`), or permanently as `node.bridge_checkpoint` in the service
   configuration (`db-sync-alt run --node-bridge-checkpoint`).
3. `checkpoint_supported: true` means the node's bridge query reads covered data
   only. `false` comes with `reason` `before_coverage` (the checkpoint is older
   than the coverage start) or `transaction_not_served` (the checkpoint
   transaction does not touch the profile's addresses or assets, so the node's
   lookup of it finds no row). The service then answers `/health` with 503 and
   `bridge_checkpoint_unsupported`. Serving continues, but the node's bridge
   answer can then differ from the block author's: with `before_coverage`, a
   transfer in an uncovered block reads without its recipient; with
   `transaction_not_served`, the lookup fails. Either stops the node at that
   block. This version has no backfill of earlier metadata.

## Connections

midnight-node 2.1.101 opens up to 32 connections to its cardano-db-sync database
(six pools: candidates 5, sidechain 5, main chain hash 5, cNIGHT observation 10,
federated authority 5, bridge 2); db-sync-alt's own serving check opens one more
as the reader from time to time. db-sync-alt requires a reader connection limit
of at least 36, which leaves 3 spare, plus your headroom (40 in the examples),
and the server enough `max_connections` for every database on it.

## Checking the connection

With `psql`, as the reader:

```sh
psql 'postgresql://db_sync_alt_reader@<host>:5432/db_sync_alt' \
  -c "select count(*) from information_schema.tables where table_name = 'tx_in'" \
  -c 'select block_no from block order by block_no desc limit 1'
```

The first query is the node's own startup check and returns 1; the second
returns the newest block db-sync-alt serves. `permission denied` means the
reader was not granted (yet); SQLSTATE 55000 means the database is not serving
right now: check [health](troubleshooting.md).
