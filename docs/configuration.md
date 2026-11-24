# Configuration

db-sync-alt has no configuration file. Every setting is a flag of the command
that uses it. Where the table names a variable, the flag can also be given as
that environment variable; a flag on the command line wins over its variable.
A variable that is set but empty is a value, and an empty value is refused when
the flag needs one: leave an unused variable out instead of setting it to
nothing. Secrets are only ever paths of files: a password or key is never a
flag value.

Every value is checked before the command connects anywhere. An invalid value
ends the command with exit status 2 and one JSON line on standard error that
names the flag, for example `{"error":"invalid_password_file","flag":"--password-file"}`
([troubleshooting](troubleshooting.md#usage-errors)). `db-sync-alt --help` and
`db-sync-alt <command> --help` print the same flags; `--version` prints the
version.

A test of the repository compares this page with the `--help` of every
command: each flag, its variable and its default.

## Database flags

`run`, `rebuild`, `create`, `restore`, `snapshot`, `verify`, `status` and
`grant-reader` take these flags for the database db-sync-alt writes or reads.

| Flag | Variable | Default | Meaning |
| --- | --- | --- | --- |
| `--database` | `DSA_DATABASE` | required | PostgreSQL connection string, as `key=value` pairs (`host=db.example port=5432 dbname=db_sync_alt user=db_sync_alt_owner`) or as a URI (`postgresql://db_sync_alt_owner@db.example:5432/db_sync_alt`). It must not hold a password. |
| `--network` | `DSA_NETWORK` | required | `mainnet` or `preprod`. The database must belong to this network. |
| `--password-file` | `DSA_PASSWORD_FILE` | none | A file holding the login's password on one line; a final line break is ignored. |
| `--tls-root-cert` | `DSA_TLS_ROOT_CERT` | none | PEM certificates of the authorities that signed the server's certificate. With it, the connection uses TLS and checks the certificate and the host name in `--database`. |
| `--statement-timeout-seconds` | `DSA_STATEMENT_TIMEOUT_SECONDS` | PostgreSQL's setting | `statement_timeout` of this connection, 1 to 86,400 seconds. |
| `--idle-transaction-timeout-seconds` | `DSA_IDLE_TRANSACTION_TIMEOUT_SECONDS` | PostgreSQL's setting | `idle_in_transaction_session_timeout` of this connection, 1 to 86,400 seconds. |
| `--overall-timeout-seconds` | `DSA_OVERALL_TIMEOUT_SECONDS` | none | Stop the command after this many seconds, 1 to 86,400. `run` refuses it, also as the variable, with exit status 2: the service runs until it is stopped. |

TLS: without `--tls-root-cert` the connection still requires TLS and checks
the server's certificate against the public certificate authorities. Put
`sslmode=disable` in `--database` to connect without TLS, for example to a
server on the same host; `--tls-root-cert` is then refused.

## Provider flags

`run` and `rebuild` take these flags for the provider ([provider](provider.md)).

| Flag | Variable | Default | Meaning |
| --- | --- | --- | --- |
| `--provider-url` | `DSA_PROVIDER_URL` | Blockfrost for `--network` | The provider's base URL: `https://cardano-mainnet.blockfrost.io/api/v0` or `https://cardano-preprod.blockfrost.io/api/v0` unless given. The provider must serve `--network`: the network magic of its `/genesis` is checked, and another one ends `run` with exit status 1 (`provider_network_mismatch`). |
| `--provider-key-file` | `DSA_PROVIDER_KEY_FILE` | required | A file holding the provider key. |
| `--provider-root-cert` | `DSA_PROVIDER_ROOT_CERT` | none | PEM certificates trusted for the provider in addition to the public authorities. |
| `--requests-per-second` | `DSA_REQUESTS_PER_SECOND` | `8` | Requests sent per second at most; above 0 and at most 10. |
| `--max-in-flight` | `DSA_MAX_IN_FLIGHT` | `4` | Requests in flight at once at most, 1 to 64. |

## Follower flags

`run` and `rebuild` take these flags for following the chain.

| Flag | Variable | Default | Meaning |
| --- | --- | --- | --- |
| `--poll-interval-seconds` | `DSA_POLL_INTERVAL_SECONDS` | `20` | Seconds between two polls at the tip, 1 to 3,600. |
| `--prune-interval-seconds` | `DSA_PRUNE_INTERVAL_SECONDS` | `3600` | Least seconds between two horizon advances, 60 to 86,400. |
| `--max-batch-blocks` | `DSA_MAX_BATCH_BLOCKS` | `100` | Blocks made final by one update at most, 1 to 1,000. |
| `--max-batch-cbor-bytes` | `DSA_MAX_BATCH_CBOR_BYTES` | `4194304` | Transaction bytes of one update; the update ends with the block that reaches it. 65,536 to 67,108,864. |
| `--max-batch-rows` | `DSA_MAX_BATCH_ROWS` | `50000` | Rows of one update; the update ends with the block that reaches it. 1,000 to 1,000,000. |

## Service flags

`run` takes these flags for the service around the follower.

| Flag | Variable | Default | Meaning |
| --- | --- | --- | --- |
| `--listen` | `DSA_LISTEN` | `0.0.0.0:18080` | Address and port of `/health` and `/status`. |
| `--reader` | `DSA_READER` | required | The Midnight node's PostgreSQL login. It is granted the node's rights at every start and checked ([PostgreSQL](postgresql.md#the-reader-login)). |
| `--snapshot` | `DSA_SNAPSHOT` | none | A snapshot file to restore when the database is empty, or when an interrupted restore left its build. Read only then: on a serving database the file need not exist, so the flag may stay set after the first start. A missing or unreadable file when a restore is needed ends `run` with exit status 1 (`snapshot_file_unreadable`). |
| `--expect-content-digest` | `DSA_EXPECT_CONTENT_DIGEST` | none | Refuse the `--snapshot` file unless its content digest is this one (64 lowercase hexadecimal characters). Only with `--snapshot`. |
| `--retention-seconds` | `DSA_RETENTION_SECONDS` | the stored value | Seconds an event stays after it is first served, at least 86,400. A value that differs from the stored one is refused unless `--change-retention` is given. |
| `--change-retention` | `DSA_CHANGE_RETENTION` | off | Store `--retention-seconds` when it differs from the stored value ([operations](operations.md#change-the-retention)). Only with `--retention-seconds`. |
| `--snapshot-dir` | `DSA_SNAPSHOT_DIR` | none are written | A writable directory for the daily snapshot files ([operations](operations.md#daily-snapshot-files)). |
| `--snapshot-keep` | `DSA_SNAPSHOT_KEEP` | `3` | The newest daily files of the network kept in `--snapshot-dir`, at least 1. |
| `--snapshot-at` | `DSA_SNAPSHOT_AT` | `00:30` | UTC time of day of the daily file, as `HH:MM`. |
| `--health-max-lag-blocks` | `DSA_HEALTH_MAX_LAG_BLOCKS` | `9` | `/health` answers 503 when the served target is more blocks behind the provider's tip minus k. |
| `--database-fatal-after-seconds` | `DSA_DATABASE_FATAL_AFTER_SECONDS` | `1800` | A database that stays unreachable, or a provider whose transport fails or that answers 5xx, for this many seconds ends the service with exit status 75. At least 60. |

## `run`

The service ([operations](operations.md)). It takes the
[database flags](#database-flags), the [provider flags](#provider-flags), the
[follower flags](#follower-flags) and the [service flags](#service-flags), and
no others. It refuses `--overall-timeout-seconds` and
`DSA_OVERALL_TIMEOUT_SECONDS` with exit status 2
(`overall_timeout_not_supported`): the service runs until it is stopped.

## `rebuild`

Builds an empty database from the provider alone, without a snapshot file,
checks it and opens the gate. It takes the
[database flags](#database-flags), the [provider flags](#provider-flags), the
[follower flags](#follower-flags) and these, which have no variable:

| Flag | Variable | Default | Meaning |
| --- | --- | --- | --- |
| `--retention-seconds` | — | `604800` | Seconds an event stays after it is first served, stored in the built database, at least 86,400. A stopped build continues only with the same value. |
| `--window-seconds` | — | `--retention-seconds` | Seconds of chain time from the horizon to the target, from 86,400 to `--retention-seconds`. Without `--horizon-block`, the horizon is the newest block at or before the target's time minus this. |
| `--horizon-block` | — | the newest block at or before the target's time minus `--window-seconds` | The height of the horizon, the oldest block of the window. Not above `--target-block`. |
| `--target-block` | — | the provider's tip minus k | The height of the target, the newest block whose events the built database serves; at most the provider's tip minus k. |

A stopped build continues with the same command; `--horizon-block` and
`--target-block` may then be left out, and when given they must name the
stored horizon and target (`rebuild_plan_mismatch` otherwise).

## `create`

Creates the empty layout in a blank database. `restore` and `run` create it
themselves, so this command is optional. Takes the
[database flags](#database-flags) only.

## `restore`

Loads a snapshot file into an empty database, checks it and opens the gate
([snapshots](snapshots.md#restore)). Takes the [database flags](#database-flags)
and:

| Flag | Variable | Default | Meaning |
| --- | --- | --- | --- |
| `--file` | — | required | The snapshot file. |
| `--expect-content-digest` | — | none | Refuse the file unless its content digest is this one. |

## `snapshot`

Writes a snapshot file of a serving database ([snapshots](snapshots.md#write-a-file)).
Takes the [database flags](#database-flags) and:

| Flag | Variable | Default | Meaning |
| --- | --- | --- | --- |
| `--file` | — | required | The new file. An existing file is refused. |

## `verify`

Checks a serving database in full and prints its digests
([snapshots](snapshots.md#verify)). Takes the [database flags](#database-flags)
only.

## `status`

Prints the status document from the database alone, without a running service
([operations](operations.md#status-without-the-service)). Takes the
[database flags](#database-flags) and:

| Flag | Variable | Default | Meaning |
| --- | --- | --- | --- |
| `--reader` | `DSA_READER` | none | The node's reader login to check, as `grant-reader --check` does. |

## `grant-reader`

Grants the node's reader login its rights and checks that it has no others
([PostgreSQL](postgresql.md#the-reader-login)). `run` does the same at every
start. Takes the [database flags](#database-flags) and:

| Flag | Variable | Default | Meaning |
| --- | --- | --- | --- |
| `--role` | — | required | The reader login. |
| `--check` | — | off | Only check the existing rights; change nothing. |

## `compare`

Runs the node's queries against two databases and reports every difference
([comparison](comparison.md)). It takes these flags and no database flags:

| Flag | Variable | Default | Meaning |
| --- | --- | --- | --- |
| `--a` | — | required | Connection string of side A, as a reader login. |
| `--b` | — | required | Connection string of side B, as a reader login. |
| `--network` | — | required | `mainnet` or `preprod`. |
| `--positions` | — | `auto` | A JSON file of cursor and reference positions, or `auto`: positions across the window of `--horizon-block`. |
| `--horizon-block` | — | none | The horizon of the positions. |
| `--max-pairs` | — | `500` | Position pairs of the uniform sample, 1 to 5,000. |
| `--a-password-file` | — | none | Password file of side A. |
| `--b-password-file` | — | none | Password file of side B. |
| `--a-tls-root-cert` | — | none | PEM certificates for side A's server. |
| `--b-tls-root-cert` | — | none | PEM certificates for side B's server. |
| `--statement-timeout-seconds` | — | PostgreSQL's setting | `statement_timeout` of both connections, 1 to 86,400 seconds. |
| `--idle-transaction-timeout-seconds` | — | PostgreSQL's setting | `idle_in_transaction_session_timeout` of both connections, 1 to 86,400 seconds. |
| `--a-verify-unbounded` | — | off | Also repeat side A's event queries with unbounded IDs, as side B always does. This reads A's whole event history. |
| `--max-differing-rows` | — | `1000` | Differing rows reported per query, 1 to 1,000,000. |
| `--candidate-address` | — | the network's | The registered-candidates address the candidate queries run with. |
| `--overall-timeout-seconds` | — | `3600` | Stop after this many seconds, 1 to 86,400. |

## `profile`

Prints the network profile compiled into the binary ([glossary](glossary.md#profile)).

| Flag | Variable | Default | Meaning |
| --- | --- | --- | --- |
| `--network` | — | required | `mainnet` or `preprod`. |

## `health`

Asks a running service's `/health` and exits 0 when it answers 200, 1
otherwise, within 3 seconds. A container's health check uses it; it needs no
HTTP client in the image.

| Flag | Variable | Default | Meaning |
| --- | --- | --- | --- |
| `--url` | `DSA_HEALTH_URL` | `http://127.0.0.1:18080/health` | The address of the service's `/health`. |
