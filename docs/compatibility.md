# Compatibility

What db-sync-alt 0.6.0 works with. Anything else is refused or unsupported.

| Component | Supported | Notes |
| --- | --- | --- |
| Midnight node | 2.1.101 | The SQL catalogue is extracted from the node's sources at tag `node-2.1.101` (commit `86774edf8cc76345a5c3a4221f3fe80c094b1c33`), kept unchanged under `reference/`. The keep rule (version 2) is derived from these queries. Another release works only if it sends the same queries and reads the same addresses and assets. |
| PostgreSQL | 17 or newer | Provided by you ([PostgreSQL](postgresql.md)). Tested on 17, 18 and 19. Every connection checks the version; a server older than 17 is refused (`postgresql_17_or_newer_required`). The node connects over TLS only. |
| PostgreSQL client tools | none | Restore and snapshot need no `psql`, `pg_dump` or `pg_restore`. |
| Cardano networks | Mainnet, Preprod | `--network mainnet` or `--network preprod`. Other names are refused (`unsupported_network`). |
| Provider | Blockfrost API v0 of the network | One key file. See [provider](provider.md). |
| Operating system | Linux x86-64 with glibc 2.39 or newer | The release binary is built on Ubuntu 24.04. The image is based on Debian 12 (distroless) and runs as user 65532. |
| Database format | 1 | Stored in the database (`identity.database_format`); another format is refused (`unsupported_database_format`). |
| Snapshot file format | 1 | Another format is refused (`unsupported_snapshot_format`). |

## What the release checks cover

- Queries: every statement of the node's query catalogue returns the same rows
  from a db-sync-alt database as from a full-history database of the same
  chain at the same block ([comparison](comparison.md)).
- Following: the follower ran on real Preprod and Mainnet blocks, and on
  Preprod its database had the content digest of a database built from the
  full history at the same blocks ([provider](provider.md#catching-up)).
- A build from the provider: on Preprod, `rebuild` with the horizon and target
  of the Preprod snapshot file of release 0.5.1 gave the file's content
  digest. A build with the default positions passed `verify`, `run` followed
  real blocks from it to the chain's tip, and `compare` found no difference
  against a database that had followed the chain ([rebuild](rebuild.md#cost)).
  A Mainnet build is not part of these checks.
- A node: a Midnight node 2.1.101 on Preprod reads a db-sync-alt database and
  imports blocks at the tip of its chain. A node on Mainnet, and a Cardano
  epoch change while a node reads the database, are not part of these checks.
  Before a node depends on a new database, run the connection check of
  [connect a node](connect-a-node.md) and watch the node follow.

## Networks and their profiles

The values of each network are compiled into the binary as its profile,
generated from the configuration files of Midnight node 2.1.101.
`db-sync-alt profile --network mainnet` prints the whole profile: the
addresses and asset units the node watches and where each value comes from.

| | Mainnet | Preprod |
| --- | --- | --- |
| Security parameter k | 2,160 | 2,160 |
| The node's block stability margin | 30 | 30 |
| First block the node reads (block : transaction index) | 13,164,005 : 1 | 4,500,197 : 4 |
| Provider URL (`--provider-url` default) | `https://cardano-mainnet.blockfrost.io/api/v0` | `https://cardano-preprod.blockfrost.io/api/v0` |
| `/health` lag limit (`--health-max-lag-blocks` default) | 9 | 9 |

On Preprod the cNIGHT unit, which the ICS and reserve inputs also use, has an
empty asset name; the bridge unit is the same policy with the asset name
`NIGHT`. Both are served.

A database records its network, its profile's hash and the keep-rule version
when it is created or restored. A binary with another profile or keep rule
refuses it (`network_mismatch`, `profile_hash_mismatch`,
`keep_rule_version_mismatch`), and a snapshot file is refused the same way.

## The node's reader

The node connects with its own login, which may only read the eleven
node-facing views and call the gate function. db-sync-alt grants exactly this
and refuses a login with more ([PostgreSQL](postgresql.md#the-reader-login)).
