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

The reader login can connect to db-sync-alt's database, read the ten
db-sync-shaped relations the node's queries use (`block`, `tx`, `tx_out`,
`tx_in`, `ma_tx_out`, `multi_asset`, `datum`, `epoch_param`, `epoch_stake`,
`pool_hash`) and run the serving check they call; nothing else. The node's SQL
runs unchanged. Each of those relations raises SQLSTATE 55000
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

## Connections

midnight-node's cardano-db-sync pool opens up to 35 connections; db-sync-alt's
own serving check opens one more as the reader from time to time. Give the
reader a connection limit of at least 36 plus your headroom (40 in the
examples), and the server enough `max_connections` for every database on it.

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
