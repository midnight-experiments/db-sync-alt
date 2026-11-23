# Connect a Midnight node

A Midnight node reads Cardano data with SQL through its db-sync settings. It
connects to db-sync-alt's database directly, as it would to a cardano-db-sync
database, with the reader login ([PostgreSQL](postgresql.md#the-reader-login)).

## Settings

```sh
DB_SYNC_POSTGRES_CONNECTION_STRING='postgresql://db_sync_alt_reader:<password>@<host>:5432/db_sync_alt'
SSL_ROOT_CERT=/path/to/root-ca.crt
CARDANO_SECURITY_PARAMETER=2160
```

| Setting | Value |
| --- | --- |
| Host and port | Your PostgreSQL server as the node reaches it. |
| Database | db-sync-alt's database (`db_sync_alt` in the examples). |
| User and password | The reader login and its password, never the owner login db-sync-alt runs as. |
| `SSL_ROOT_CERT` | The certificate authority of the server's certificate. Midnight node 2.1.101 connects only over TLS and refuses a server without it; `ALLOW_NON_SSL` has no effect. With `SSL_ROOT_CERT` the node checks the certificate and the host name ([TLS](postgresql.md#tls)). |
| `CARDANO_SECURITY_PARAMETER` | k of the network: 2160 on Mainnet and Preprod. |

With the [Compose deployment](../deploy/README.md#5-connect-a-midnight-node)
PostgreSQL is not published on the host: the node joins the deployment's
`node` network and uses host `postgres`.

## Node requirements

db-sync-alt cannot see the node's settings. A node that does not match the
profile gets empty answers where the database cannot tell its request from a
valid one:

- **k.** `CARDANO_SECURITY_PARAMETER` must equal the profile's k. The database
  stores headers up to k blocks above the newest block it serves events for; a
  node with a smaller k takes younger blocks as stable and asks for events that
  are not published yet. A larger k is harmless.
- **Addresses and assets.** The node's Cardano addresses and policies (cNIGHT
  mapping validator and asset, governance bodies, reserve and ICS) must be the
  profile's (`db-sync-alt profile --network <network>`). They come from the
  node's network configuration of release 2.1.101.
- **Position.** The node's Cardano position must not be below the database's
  horizon ([limitations](limitations.md#history-older-than-the-window)).

## What the node can read

The reader login reads the eleven node-facing views (`block`, `tx`, `tx_out`,
`tx_in`, `ma_tx_out`, `multi_asset`, `datum`, `epoch_param`, `epoch_stake`,
`pool_hash`, `tx_metadata`) and nothing else. The node's SQL runs unchanged.

Every view raises SQLSTATE 55000 (`db_sync_alt_not_serving`) instead of
answering while the database is not serving: empty, restoring, or closed by a
check. The node therefore never takes an empty answer for a real one. Every
statement sees one complete update, also while the next one is written. While
the service is stopped, the node keeps reading the last update; `/health` then
fails, and `/status` shows how far behind the database is.

The reader can connect once `run` or `grant-reader` has granted it, which
happens after the database first serves.

## Check the connection

As the reader, over TLS as the node connects:

```sh
psql 'postgresql://db_sync_alt_reader@<host>:5432/db_sync_alt?sslmode=verify-full&sslrootcert=root-ca.crt' \
  -c "select count(*) from information_schema.tables where table_name = 'tx_in'" \
  -c 'select block_no from block order by block_no desc limit 1'
```

The first query is the node's own startup check and returns 1; the second
returns the newest stored block, the stored tip. `permission
denied` means the reader was not granted yet; SQLSTATE 55000 means the
database is not serving right now: read `/status`
([troubleshooting](troubleshooting.md)).
