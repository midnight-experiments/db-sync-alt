# Troubleshooting

Every failure prints a JSON report on standard error whose `error` field is a
stable code, and `/status` shows the code of the current condition in
`state_reason`, `error_code` and `last_error`. This page lists the codes by
where they appear. The `/health` reason codes are in
[operations](operations.md#health); the states and their reasons in
[operations](operations.md#states).

A test of the repository checks that every code in the tables below exists in
the code.

## Usage errors

Exit status 2, before anything connects. The report names the flag:
`{"error":"<code>","flag":"<flag>"}`. A flag the command does not know, or a
missing required flag, is reported by the command-line parser with the same
exit status.

| Code | Cause | What to do |
| --- | --- | --- |
| `invalid_connection_string` | The connection string does not parse. | Use `key=value` pairs or a `postgresql://` URI ([configuration](configuration.md#database-flags)). |
| `password_requires_file` | The connection string holds a password. | Remove it; give the password in the password file. |
| `invalid_password_file` | The password file cannot be read or holds more than one line. | Put the password alone on one line. |
| `invalid_tls_root_certificate` | The certificate file cannot be read or holds no PEM certificate. | Give the PEM file of the server's certificate authority. |
| `conflicting_tls_configuration` | A certificate file was given for a connection with `sslmode=disable`. | Drop one of the two. |
| `unsupported_network` | `--network` is not `mainnet` or `preprod`. | |
| `provider_url_required` | No `--provider-url` and no default for the network. | |
| `invalid_provider_configuration` | A provider value is out of range, or the key or certificate file cannot be read. | Check `--requests-per-second` (at most 10), `--max-in-flight` and the files. |
| `invalid_listen_address` | `--listen` is not an address and port. | For example `0.0.0.0:18080`. |
| `invalid_reader_role` | `--reader` is empty or too long. | |
| `expect_content_digest_requires_snapshot` | `--expect-content-digest` without `--snapshot`. | |
| `invalid_content_digest` | The digest is not 64 lowercase hexadecimal characters. | |
| `invalid_retention` | `--retention-seconds` below 86,400. | |
| `change_retention_requires_retention_seconds` | `--change-retention` without `--retention-seconds`. | |
| `invalid_snapshot_dir` | `--snapshot-dir` does not exist or is not writable for the service's user. | |
| `invalid_snapshot_keep` | `--snapshot-keep` is 0. | |
| `invalid_snapshot_at` | `--snapshot-at` is not `HH:MM`. | |
| `invalid_database_fatal_after` | `--database-fatal-after-seconds` below 60. | |
| `overall_timeout_not_supported` | `run` was given `--overall-timeout-seconds` or `DSA_OVERALL_TIMEOUT_SECONDS`. | Remove it: the service runs until it is stopped. |

An empty `DSA_` variable is a value: the command-line parser refuses
`DSA_SNAPSHOT=` because `--snapshot` needs a value, and `DSA_MAX_IN_FLIGHT=` as
not a number. Leave unused variables out.

## Connecting to PostgreSQL

A connection failure is reported as `database_error` with a `cause`, the
SQLSTATE and the server's message when there is one. It is temporary (exit
status 75; the service retries) when the server cannot be reached, closed the
connection, is starting or stopping, or has too many connections.

| Code | Cause | What to do |
| --- | --- | --- |
| `connection_refused` | Nothing listens at the host and port. | Check the server and `--database`. |
| `connection_timeout` | The server did not answer within 10 seconds. | Check the network path. |
| `tls_host_name_mismatch` | The server's certificate is not for the host name in `--database`. | Use the name the certificate is for, or a certificate with that name. |
| `tls_unknown_ca` | The server's certificate is not signed by `--tls-root-cert` (or a public authority without it). | Give the right certificate authority. |
| `tls_certificate_invalid` | The certificate is expired or otherwise invalid. | Renew it. |
| `tls_error` | Another TLS failure, for example a server without TLS. | Turn TLS on ([PostgreSQL](postgresql.md#tls)) or use `sslmode=disable` for a local server. |
| `postgresql_17_required` | The server is not PostgreSQL 17. | |
| `invalid_owner_role` | The login does not own the database, or has superuser or other role attributes. | [PostgreSQL](postgresql.md#the-database-and-its-logins). |
| `writer_busy` | Another session holds the writer lock: a running service, a restore or a conversion, or the session of a db-sync-alt process that is gone, whose host lost power or whose network to the server was cut. `run` waits for the lock, at start and when its writer reconnects, with the state `waiting` and the reason `writer_busy`, and ends with exit status 1 after 120 seconds; the other commands refuse at once. | Stop the other process, or wait for it. A session left by a process that is gone ends by itself within about a minute; to end it at once, see below. |

Every session of db-sync-alt sets the server-side TCP keepalive
(`tcp_keepalives_idle` 30 seconds, `tcp_keepalives_interval` 10 seconds,
`tcp_keepalives_count` 3), so the server notices a client that is gone within
about a minute and ends its session, which releases the writer lock. To find
the session that holds the lock, and its client, run as the owner login or a
superuser in the database:

```sql
SELECT a.pid, a.client_addr, a.backend_start, a.state, a.state_change
FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
WHERE l.locktype = 'advisory' AND l.classid = 6583137 AND l.objid = 1
  AND l.granted AND a.datname = current_database();
```

If no db-sync-alt process runs on that client any more, end the session with
`SELECT pg_terminate_backend(<pid>);`.

## The database

| Code | Cause | What to do |
| --- | --- | --- |
| `unsupported_database_format` | The database is not a db-sync-alt 0.5.0 database: for example a 0.4.x database. | Give the right database; a 0.4.x database is the source of [`convert`](upgrade.md). |
| `network_mismatch` | The database belongs to another network. | Check `--network` and `--database`. |
| `profile_hash_mismatch` | The database was made with another profile. | Restore a file made by this release into a new database. |
| `keep_rule_version_mismatch` | The database was made with another keep rule. | As above. |
| `destination_not_empty` | `restore`, `convert` or `create` was given a database that holds data. | Use an empty database. |
| `database_not_serving` | The gate is closed, or the database never served. | See `position.reason` in `db-sync-alt status`; for a closed gate, [restore a snapshot file](operations.md#a-closed-gate). |
| `stored_headers_incomplete` | The headers between the target and the tip are not all stored. | Run `verify`; restore a snapshot file into a new database. |

`run` also refuses an empty database without `--snapshot`
(`snapshot_required`), a `--snapshot` file that is missing or unreadable when a
restore is needed (`snapshot_file_unreadable`, with the flag and the kind of
failure; the file is not read when the database serves), a
`--retention-seconds` that differs from the stored one (`retention_mismatch`,
[change the retention](operations.md#change-the-retention)), and a `--listen`
address that is in use (`listen_address_unavailable`).

## The reader login

| Code | Cause | What to do |
| --- | --- | --- |
| `reader_role_missing` | The reader login does not exist. | Create it ([PostgreSQL](postgresql.md#the-database-and-its-logins)). |
| `invalid_reader_role` | The reader login cannot log in, has role attributes, or is the owner login. | Use a separate login without attributes. |
| `reader_privileges_not_exact` | The reader has rights beyond the node's; `details` lists each. | Revoke them, including grants to `PUBLIC` ([the reader login](postgresql.md#the-reader-login)). |

## Snapshot files

| Code | Cause | What to do |
| --- | --- | --- |
| `unsupported_snapshot_magic` | The file is not a snapshot file. | |
| `unsupported_snapshot_format` | The file has another format. | Use a file written by this release. |
| `snapshot_content_digest_unexpected` | The file's content digest is not `--expect-content-digest`. | Get the file again, or the right digest. |
| `snapshot_section_hash_mismatch` | A section of the file does not match its hash: the file is damaged. | Get the file again. |
| `snapshot_file_size_mismatch` | The file is shorter or longer than its description says. | Get the file again. |
| `snapshot_io_error` | Reading or writing the file failed; `kind` names the failure. | Check the path, the space and the permissions. |
| `snapshot_already_exists` | The output file exists. | Choose a new path. |
| `snapshot_published_durability_uncertain` | The file was published but its directory could not be synced. | The file exists; do not write over it. |
| `publication_lock_unavailable` | A reader held a view for 60 seconds while a restore wanted to publish. | Run the restore again. |

## Provider

| Code | Cause | What to do |
| --- | --- | --- |
| `provider_key_rejected` | The provider answered 401 or 403: the key is wrong, revoked, or for another network. The service stops. | Fix the key file. |
| `quota_exhausted` | 402: the key's daily allowance is spent. Waited out until the next UTC day. | Nothing; a larger plan if it repeats ([provider](provider.md#refusals-and-outages)). |
| `provider_rate_limited` | 429. Waited out. | Lower `--requests-per-second` if another program shares the key. |
| `provider_banned` | 418. Waited out, 1 to 24 hours. | Check what else uses the key. |
| `provider_unavailable` | A transport failure or a 5xx answer. Retried. | Nothing while it is short; the service ends with exit status 75 after `--database-fatal-after-seconds`. |
| `provider_timeout` | No answer in time. Retried. | As above. |

## Answers the follower refuses

The provider client reads every answer into the endpoint's type, and the
follower checks it against the chain before it writes anything. When an
answer fails either, nothing is written and the step is retried every 30
seconds without end; `/status` shows the state `waiting` with the code, and
`/health` fails once the database falls behind. A short spell is usually the
provider's; a code that stays for one block points at that block.

| Code | What did not match |
| --- | --- |
| `provider_invalid_response` | An answer is not valid for its endpoint: not JSON of the expected shape, or a header of another height or epoch than asked for. |
| `provider_bad_page` | A list holds more rows than one page. |
| `provider_not_found` | The provider answered 404 where the endpoint must answer, for example `/blocks/latest` or a transaction's metadata. |
| `provider_response_size_limit` | An answer is larger than the limit of its endpoint. |
| `provider_json_depth_limit` | A `/txs/{hash}/utxos` answer nests deeper than the client reads. |
| `provider_request_rejected` | The provider answered another 4xx or a 3xx status. |
| `transaction_count` | A block's transaction list is shorter or longer than its header says. |
| `cbor_body_hash_mismatch` | A transaction's bytes do not hash to its ID. |
| `duplicate_transaction` | A transaction appears twice in one block. |
| `header_height` | A block header is not at the height that follows the previous one. |
| `header_parent` | A header does not link to the one before it. |
| `provider_header_link_mismatch` | A page of headers does not link to the stored tip. |
| `provider_page_missing` | A page of a block's transactions is missing. |
| `provider_block_unknown` | The provider does not know a block it listed. |
| `supplements_unresolved` | A datum, metadata or output answer did not resolve what the block needs. |
| `unsupported_transaction_body_field` | A transaction body holds a field the decoder does not know. |
| `transaction_size_limit` | A transaction is larger than any valid transaction. |
| `asset_quantity` | An asset amount is out of range. |

Codes starting with `cbor_` name what the decoder refused in a transaction's
bytes. If one stays, report it with the block height from `last_error.detail`:
content the ledger accepts must never stop the follower.

## Checks that close the gate

| Code | Cause | What to do |
| --- | --- | --- |
| `canonical_conflict` | The provider contradicted a final block the database serves, twice. | [Restore a snapshot file](operations.md#a-closed-gate); report it. |
| `asset_conservation` | A new transaction spends more of a profile asset than the database holds: an output is missing. | As above. |
| `supply_mismatch` | The unspent amount of a fixed-supply asset differs from the recorded supply. | As above. |

## `verify`

`verify` exits 3 and names the first fault it finds. Every one means the
database is not what the keep rule and its description say; restore a snapshot
file into a new database. The faults include:

| Code | Fault |
| --- | --- |
| `row_outside_keep_rule` | A row the keep rule does not select. |
| `first_served_incomplete` | A block of the window without a first-served time. |
| `epoch_param_missing` | An epoch of the window without parameters. |
| `unspent_count_mismatch` | The count of unspent outputs at a profile address differs from a recount. |
| `header_gap` | A header between the horizon and the tip is missing. |
| `missing_supply` | A fixed-supply asset has no recorded supply. |

## `convert`

| Code | Cause | What to do |
| --- | --- | --- |
| `source_not_serving` | The 0.4.x database does not serve: it answered SQLSTATE 55000. | Let the 0.4.x service publish, then convert. |
| `invalid_source_reader_role` | The source login is not exactly the 0.4.x reader. | Use the 0.4.x reader login. |
| `unsupported_legacy_schema` | The source is not a 0.4.x schema-5 database. | Upgrade the 0.4.x database with 0.4.x first. |
| `source_network_ambiguous` | The source holds nothing that proves its network. | |
| `horizon_before_source_history` | `--horizon-block` is older than the source's history. | Choose a later horizon. |
| `horizon_above_source_target` | `--horizon-block` is above the source's target. | |
| `source_statement_timeout` | A source statement reached its limit. | Raise `--source-statement-timeout-seconds`. |
| `source_read_failed` | A source read failed; the report has the server's message. | |
| `conversion_completed_snapshot_failed` | The database was converted and serves; only the snapshot file failed. | Run `snapshot` with a new path; do not convert again. |

## `compare`

| Code | Cause | What to do |
| --- | --- | --- |
| `comparison_not_serving` | A side answered SQLSTATE 55000. | Compare a serving database. |
| `comparison_statement_timeout` | A query reached its statement limit. | Raise `--statement-timeout-seconds`. |
| `comparison_requires_finite_read_limits` | A side's login has no statement or idle-transaction limit. | Give `--statement-timeout-seconds` and `--idle-transaction-timeout-seconds`. |

## Stopped commands

| Code | Cause | What to do |
| --- | --- | --- |
| `operation_interrupted` | SIGINT or SIGTERM. | With `"published": false` nothing was published: run the command again. With `"outcome": "unknown"`, run `status` and `verify`. |
| `operation_deadline_exceeded` | `--overall-timeout-seconds` passed. | As above; raise the limit. |
