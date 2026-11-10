# Troubleshooting

db-sync-alt reports problems as short stable codes: in `/health` and `/status`
(`reason_codes`, `error_code`, `poll_error_code`), and as `{"error_code": "…"}` or
`{"error": "…"}` printed by a command that stops. Commands exit with 2 for a
refusal and 75 when the condition is temporary and a retry can succeed (a
provider failure during `acquire`, a lost database connection of the service).
Codes never contain secrets, URLs or raw exception text.

General rules: stop the service before administrative commands; never delete
the state directory, the database or the request ledger to get past an error;
preserve a failed database for diagnosis and recover into a new empty one.

## Health reason codes

`GET /health` returns 503 with one or more of these in `reason_codes`.

| Code | Meaning | What to do |
| --- | --- | --- |
| `lifecycle_starting`, `lifecycle_restoring` | Starting up: restoring, verifying or granting the reader | Wait; full verification of a large database takes time |
| `lifecycle_failed`, `lifecycle_halted`, `lifecycle_stopping`, `lifecycle_stopped` | The service stopped working; `error_code` says why (see below). `halted` means it refuses to continue on purpose | Read `error_code`, fix the cause, restart |
| `database_unavailable`, `database_timeout`, `database_unknown`, `database_observation_stale` | The PostgreSQL probe failed, is slow, has not run yet or is older than 5 seconds | Check PostgreSQL, its load and the network path |
| `serving_unready`, `serving_revoked`, `serving_stopped`, `serving_unknown` | The reader's check of the node-facing relations did not answer (no ready generation, the reader cannot connect or read, or it reached another database: see `error_code`), serving was withdrawn after a conflict, or the service stopped observing it | `revoked`: see `canonical_conflict`; otherwise see `sql_unavailable` |
| `coverage_unknown` | No committed, served or witness position is known yet | Normal right after start |
| `sync_unknown`, `sync_halted` | The follower has not reported, or stopped on purpose | Read `error_code` |
| `worker_poll_unknown`, `worker_poll_stalled` (and other worker names) | A worker thread has not reported for 120 seconds | Check CPU and database load; restart if it persists |
| `operation_stalled` | One step (a provider call or a local verification) passed its deadline | Check provider reachability and database load |
| `retry_overdue` | A scheduled retry is more than 120 seconds late | Check that the process is not suspended or overloaded |
| `provider_poll_<code>` | The last tip poll failed with `<code>` (for example `provider_poll_provider_timeout`) | See the error code below |
| `provider_observation_unknown`, `provider_observation_stale` | No tip observation yet, or the newest is older than 120 seconds | Check Blockfrost reachability and the request ceiling |
| `serving_block_lag_exceeded`, `serving_lag_exceeded` | The served target is more blocks behind the confirmed target than the block-lag limit (the network profile's 9, one below the node's stability margin of 10), or more than 600 seconds | Usually catching up; persistent lag means requests are failing or too slow, and a node on the default margin stops verifying new blocks one block later |
| `serving_block_lag_unknown`, `serving_lag_unknown` | Lag cannot be computed yet | Normal right after start |
| `provider_behind_served` | Blockfrost reports a confirmed target lower than the one already served | Usually a short provider delay; persistent: check the network setting and key |
| `bridge_checkpoint_unsupported` | The bridge checkpoint stated in the configuration (`node.bridge_checkpoint`) is older than the bridge metadata coverage start (`bridge.reason` `before_coverage`), or names a transaction the database does not serve (`transaction_not_served`) | Check the stated value against the chain ([bridge readiness](connect-a-node.md#bridge-readiness)). A correct, unsupported checkpoint means the node's bridge query reads data this database does not hold; serving continues, but the node can stop at a bridge transfer in the uncovered range |

`/status` returns the same snapshot with HTTP 200, including `lag`, `quota`,
`pending` progress, per-worker heartbeats, the verified `reader`, the
`integrity` base and `bridge` (metadata coverage start and the stated
checkpoint). `sync` reads `checkpointing` while the service writes a
periodic [integrity checkpoint](operations.md#integrity-checkpoints); publication
waits meanwhile, so the lag grows by a few blocks.

## Warnings

`warnings` in `/health` and `/status` lists what the operator should fix; a
warning never makes `/health` fail.

| Code | Meaning |
| --- | --- |
| `reader_statement_timeout_unset`, `reader_idle_in_transaction_timeout_unset` | The reader login has no `statement_timeout` or `idle_in_transaction_session_timeout` default |
| `reader_search_path_not_public` | The reader login's `search_path` default is not `public` |
| `reader_connection_limit_unset`, `reader_connection_limit_below_node_pool` | The reader login has no connection limit, or one below 36 (node 2.1.101's pools of up to 32 connections, the service's check and 3 spare) |
| `node_requirements_unchecked` | The service configuration has no `node` section, so the node's k and configuration were not compared with the profile |
| `node_security_parameter_mismatch` | The node's configured k differs from the profile's ([node requirements](connect-a-node.md#node-requirements)) |
| `node_configuration_mismatch` | A Cardano address or policy file of the node's network configuration differs from the profile's, or is missing |

| `bridge_metadata_coverage_restarted` (`acquire --source dbsync`) | The published update from cardano-db-sync follows blocks that held bridge metadata; it holds none, so the bridge metadata coverage now starts after it (`bridge_coverage_ended` gives the previous and the new start). Before bridge activation this needs nothing; after it, the node can stop at a transfer inside the update: follow from Blockfrost instead ([bridge readiness](connect-a-node.md#bridge-readiness)) |

## Service error codes

Shown as `error_code` in `/status`, or printed when the service exits.

| Code | Meaning | What to do |
| --- | --- | --- |
| `missing_backup` | The database is empty and no archive/manifest was given | Supply the snapshot (`--archive`, `--manifest`) or build initial data from your own db-sync |
| `invalid_backup` | The archive or manifest was refused: checksum, format, profile, unsafe path or a failed restore check | Re-download and check the snapshot; use the matching manifest |
| `incomplete_restore` | A restore or initial build was interrupted; the database is owned but not ready | Keep it for diagnosis; restore into a new empty database |
| `invalid_identity` | The database is not an empty one and not a db-sync-alt database for this profile, or its identity does not match; an earlier release also reports this (`unsupported_database_identity`) for a database a newer release upgraded | Point db-sync-alt at its own database or at an empty one; run the release that upgraded the database |
| `backend_identity_mismatch` | An observer or the reader's check reached a different PostgreSQL server or database than the writer | Fix the host, port and database of the owner's or the reader's libpq service; never copy identity rows |
| `invalid_reader_role` | The configured reader login is missing, cannot log in, is privileged (superuser, `CREATEDB`, `CREATEROLE`, `REPLICATION`, `BYPASSRLS`), owns the database, is a member of another role, or can do more than read the node's relations (`TEMP`, `CREATE`, internal schemas, writes, other functions); or the reader's libpq service logs in as another role | Create the reader as in [provision PostgreSQL](operations.md#provision-postgresql) and give it nothing else; db-sync-alt grants what the node needs |
| `schema_upgrade_required` | The database is of an older schema (1 to 4) or was written before epoch parameters (0.3.x or earlier); the service runs on schema 5 only. Also reported when the upgrade step of a first-run restore refused: the backup is restored and ready but not upgraded | Run `db-sync-alt upgrade` while the service and the node are stopped, with the service's Blockfrost settings or a cardano-db-sync source ([upgrade to schema 5](recovery.md#upgrade-to-schema-5)); do not restore again |
| `writer_busy` | Another db-sync-alt process (or administrative command) holds the writer lock | Stop the other process first |
| `canonical_conflict` | A committed block or fact disagrees with the source; serving is revoked | Stop, keep the database and the logs, check the provider and network; recover from a verified backup |
| `unsupported_capability` | The data needs a capability this version does not have (for example a database written with capabilities this version does not know) | The database cannot be served by this version |
| `completion_recovery_required`, `completion_invalid`, `completion_mismatch` | A generation's completion seal is missing, malformed or does not match its facts | Recover from the last verified backup into a new database |
| `unsupported_postgres_version` | PostgreSQL is not major version 17 | Use PostgreSQL 17 |
| `postgres_backup_tool_unavailable`, `postgres_backup_tool_version_mismatch` | `pg_dump`/`pg_restore` 17 is missing from `PATH` or has another version | Install the PostgreSQL 17 client tools |
| `storage_low` | A checked volume has less free space than its configured reserve | Free space on that volume, then restart |
| `provider_unavailable`, `provider_timeout` | Blockfrost failed or timed out; the request was counted | Temporary: db-sync-alt waits and retries by itself |
| `quota_exhausted` | The daily request ceiling is reached | db-sync-alt waits for the next UTC day; see [costs](costs.md) |
| `request_budget_exhausted` | A job used up its own request budget (10,000 requests by default) | Polling continues with the next UTC day's job. An exhausted acquisition job is not replaced automatically: stop the service, keep the database and find out why one update needed so many requests; never reset accounting |
| `provider_request_rejected` | Blockfrost refused a request (for example a wrong key or network) | Check the key file and that it is a key of the configured network |
| `provider_client_unavailable`, `provider_accounting_unavailable` | The provider client or its request ledger cannot be used (missing, unreadable or corrupt) | Check the key file and the state directory permissions; never delete the ledger |
| `invalid_sync_configuration`, `invalid_sync_job`, `pending_job_missing` | Stored job records or the sync settings are inconsistent | Keep the database for diagnosis; recover from a verified backup |
| `pending_target_changed`, `pending_anchor_changed`, `pending_suffix_base_changed` | A block an unfinished update is anchored to changed at the provider | The service stops without publishing the update. Check the provider and restart; the update is accepted only once the provider's blocks agree again |
| `unsupported_follow_mode` | The database cannot be followed in the configured mode | Run `db-sync-alt upgrade` while the service is stopped |
| `invalid_coverage` | Served positions are inconsistent, or the tip is too low to confirm a target | Check the network and the provider; restart |
| `sql_unavailable` | The reader could not connect or its query of a node-facing relation did not answer | Check the reader's password, `pg_hba.conf`, its connection limit and the database's state |
| `sync_unavailable`, `startup_failed` | The follower or startup could not begin (often a port in use) | Check that port 18080 (or yours) is free |
| `shutdown_timeout` | A worker did not stop within the shutdown time | Check for a blocked database operation; the next start re-verifies |
| `internal_error` | Anything not covered above | Report it with the version and the steps that led to it |

## Command refusals

Administrative commands (`acquire`, `upgrade`, `backup`, `restore`, `verify`,
`status`) print the precise code. The most common ones:

| Code | Meaning |
| --- | --- |
| `destination_not_empty` | The target database already has objects; restores and builds only use an empty database |
| `incomplete_owned_destination` | The database belongs to db-sync-alt but an import or restore did not finish |
| `unowned_database`, `unsupported_database_identity` | The database was not created by db-sync-alt, or by an incompatible version |
| `unsupported_backup_identity`, `backup_checksum_mismatch`, `backup_manifest_checksum_mismatch`, `backup_profile_mismatch` | The backup's format, PostgreSQL major version (17), archive hash, manifest hash or profile does not match |
| `pending_generation_cannot_backup`, `revoked_database_cannot_backup` | Backups are taken only of a ready, unrevoked database without an unfinished update |
| `source_attestation_pair_required` (`upgrade`) | Supply `--source-service` and `--source-attestation` together for external ordinal recovery; ordinary local upgrades need neither |
| `schema_upgrade_failed` (`upgrade`) | Check that the attestation file exists, is readable and contains valid JSON; also inspect database access and upgrade prerequisites. For a database of 0.3.x or earlier it is also printed when another session (a running node, for example) held a lock on `epoch_param` for 5 seconds: stop the node and rerun; the answers already read are kept |
| `upgrade_requires_ready` | Only a ready, unrevoked database of an older schema can be upgraded |
| `epoch_parameters_upgrade_required` | The database was written before epoch parameters (0.3.x or earlier); this version opens it only to verify, show status or upgrade it ([upgrade to schema 5](recovery.md#upgrade-to-schema-5)) |
| `epoch_parameters_source_required` (`upgrade`, `restore`) | Upgrading a database or restoring a backup of 0.3.x or earlier needs each stored epoch's `max_tx_size`: give the Blockfrost settings the service uses (`DBSYNC_PROVIDER_KEY_FILE` and `DBSYNC_PROVIDER_STATE`, or the shared adapter), or `--source-service` with `--source-attestation`; nothing was read or changed |
| `upgrade_pending_generation` | An unfinished update of the earlier release exists; start that release, let it publish, stop it when `db-sync-alt status` shows no pending generation, then upgrade |
| `provider_budget_insufficient` (`upgrade`, `restore`) | The day's remaining Blockfrost budget is smaller than the upgrade's remaining requests (2 per stored epoch); run it after the next UTC day starts. No request was sent, and a restore refuses before it touches the destination |
| `epoch_parameters_changed`, `epoch_parameters_nonce_mismatch`, `epoch_parameters_unavailable`, `invalid_epoch_max_tx_size` (`upgrade`) | The source's two reads of an epoch differed, its nonce is not the stored one, it named another epoch or had no valid size (an integer 0 to 2,147,483,647). Nothing was committed and the answers were discarded; check the source and run the upgrade again |
| `epoch_parameters_source_unavailable` (`upgrade`) | A source request failed; the answers already received are kept and a rerun continues from them |
| `request_budget_exhausted` (`upgrade`) | The upgrade spent twice the requests it needs (4 per stored epoch) on failing requests; its answers were discarded. Check the provider; a rerun starts the reads over |
| `upgrade_required_before_backup` | Only a schema-5 database is backed up; run `db-sync-alt upgrade` first |
| `invalid_epoch_max_tx_size`, `epoch_parameter_receipt_mismatch`, `epoch_parameters_upgrade_invalid` (verification; `invalid_coverage` in the service) | A stored or source epoch size is not a valid integer, differs from the size its receipt or the upgrade record names; keep the database for diagnosis and restore a verified backup |
| `ordinal_source_required`, `ordinal_source_incomplete`, `source_ordinal_conflict` (`upgrade`) | The database holds inputs or output assets without a ledger ordinal; supply both `--source-service dbsync_readonly` and `--source-attestation /run/secrets/source-attestation.json`; the same-network source must retain every required fact ([format](operations.md#build-initial-data-from-your-own-db-sync)) |
| `epoch_end_missing`, `no_served_spend` | A generation lacks the end block of a completed required epoch, or would serve no spend at all (the node then reads spent outputs as unspent); it is not published |
| `serve_projection_mismatch`, `serve_rows_mismatch`, `ordinal_order_invalid`, `node_index_names_invalid` | The served tables do not match the stored facts; keep the database for diagnosis and restore a verified backup |
| `checkpoint_digest_mismatch`, `checkpoint_root_mismatch`, `checkpoint_chain_mismatch` | The facts at the integrity base, or its checkpoint, changed after the checkpoint was written; keep the database for diagnosis and restore a verified backup |
| `generation_before_integrity_base` | A generation older than the integrity base is not verified on its own; verify the canonical generation (`db-sync-alt verify`) |
| `reader_role_missing`, `invalid_reader_role`, `reader_privileges_not_exact`, `reader_requires_current_schema` (`grant-reader`, `restore --reader-role`) | The reader login does not exist, cannot log in, is privileged, the database owner or the connecting user, can do more than read the node's relations, or the database or backup is not of a schema the node can read (schema 5, or a backup of schema 3 or 4, which the restore upgrades) |
| `source_network_mismatch`, `unsupported_source_schema`, `source_version_attestation_mismatch`, `retention_attestation_required`, `unknown_attestation_fields` | The selected network, actual source schema/version or truthful retained-history attestation does not match. Supported pairs are 13.7.1.0/15.48.6 and 13.8.0.0/16.52.6 |
| `network_mismatch`, `network_required` | The database, `--profile-dir` or acquisition profile belongs to another network than `--network`, or a new database was created without a network; a database is always opened as the network it was created for |
| `untrusted_network_profile`, `invalid_network_profile`, `unsupported_network` | A `--profile-dir` copy whose `network.json` differs from the packaged one, a malformed `network.json`, or a network without a packaged profile ([networks](networks.md)) |
| `wrong_provider_network` | Blockfrost `/genesis` reports another network magic, or a shared client points to another host |
| `<network>_credential_required` (`db-sync-alt run`) | The Blockfrost key is missing or its prefix names another network than `--network` |
| `serving_lag_above_node_margin` (service configuration) | `policy.serving_lag_max_blocks` is at or above the node's stability margin; leave it out to use the network profile's limit |
| `resume_namespace_mismatch` | An unfinished build is resumed against a different db-sync database than the one it started from |
| `source_equals_destination` | The db-sync source and the destination are the same database |
| `explicit_blockfrost_target_required` | `acquire --source blockfrost` needs `--target-height` within 1,000 blocks above the ready target |
| `invalid_bridge_checkpoint` (`status`, `run`, service configuration) | The bridge checkpoint is not `block:<n>` (0 to 2,147,483,647, no leading zero) or `tx:<64 lowercase hex>` |
| `provider_metadata_conflict`, `provider_bad_metadata` (service: `invalid_coverage`) | Blockfrost's `/txs/{hash}/metadata` answer for a transaction with an output at the ICS address is malformed, or disagrees with the transaction's own CBOR on whether it carries key 6500973 or on the recipient it names. Nothing is published; check the provider and restart |
| `bridge_metadata_incomplete`, `bridge_metadata_unexpected` (verification; `invalid_coverage` in the service) | An update from Blockfrost lacks the bridge metadata of a transaction with an output at the ICS address, or holds metadata it should not; keep the database for diagnosis and restore a verified backup |

### Relevance filtering refusals

| Code | Action |
|---|---|
| `relevance_parent_unsupported` | Enable filtering only after a compatible, independently verified complete SQL bootstrap and certified continuation lineage. Full scan is available with the option off when no conflicting relevance pending update exists. |
| `relevance_mode_mismatch` | Resume the pending update with its original relevance setting. Do not discard pending data or reset budgets. |
| `relevance_coverage_invalid` | Preserve the database and inspect the declared and stored proofs. A valid missing legacy row is repaired automatically; conflicting or forged proof is refused. |
| `relevance_parent_changed` | Use the exact profile matching the certified parent, and reverify after investigating any unexpected change. |

`/health` and `/status` include the requested mode, effective mode and reason under `relevance`. Turning the option off does not disable validation of optimized generations already in the database.
