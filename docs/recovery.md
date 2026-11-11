# Backup and recovery

The administrative commands below talk to PostgreSQL through a private libpq
service entry (`--service NAME`, resolved with `PGSERVICEFILE` and `PGPASSFILE`).
The state directory of `db-sync-alt run` already contains such files
(`pg_service.conf`, `pgpass`); the Compose image can run the same commands with
the private directory mounted. `pg_dump` and `pg_restore` 17 must be on `PATH`.

**Stop the service first.** Backup, restore, upgrade and acquisition take the
same writer lock as the service and refuse to run next to it (`writer_busy`).

## Status, verify and backup

```sh
db-sync-alt status --service db_sync_alt
db-sync-alt verify --service db_sync_alt
db-sync-alt backup --service db_sync_alt \
  --archive /srv/backups/db-sync-alt-1.dump --manifest /srv/backups/db-sync-alt-1.json
```

`status` reads the database state without changing it and lists one summary
row per generation without its manifest; `SELECT manifest FROM
chain.generations WHERE id = …` reads a manifest. `verify` checks the
database in full from its [integrity base](glossary.md#integrity-checkpoint): it
recomputes the digests of every fact family at the base and compares them with
the base's checkpoint, verifies every update published since the base, the
canonical generation's structure and served tables, and the physical contents.
`backup` verifies the same way, once, then writes an archive (`pg_dump` custom
format) and a manifest with the archive's SHA-256, the profile, the verification
result of the canonical generation and the physical inventory. It prints the
manifest's SHA-256: keep that value apart from the files. Output files are never
overwritten; an archive without its manifest is not a backup. Verification reads
every stored fact once plus the updates published since the last checkpoint, so
its time grows with the amount of data, not with the number of updates.

## Restore into a new, empty database

```sh
db-sync-alt restore --service empty_database \
  --archive /srv/backups/db-sync-alt-1.dump --manifest /srv/backups/db-sync-alt-1.json \
  --expected-manifest-sha256 <manifest sha256>
```

Restore checks the manifest's SHA-256 (when given), the archive's bytes, the
format, the PostgreSQL major version and the profile; it then restores, verifies
the canonical generation (every generation for a backup of format v2 or v1) and
the physical inventory against the manifest, and only then marks the database
ready. A backup of a schema-3 or schema-4 database (formats v2 and v3, such as
the 0.1.0 Preprod snapshot) was written before epoch parameters: the restore then
runs the [upgrade to schema 5](#upgrade-to-schema-5) before the reader is
granted, reading every stored epoch's `max_tx_size` from Blockfrost or from
`--source-service` with `--source-attestation`. Without either it refuses before
touching the destination (`epoch_parameters_source_required`).
`db-sync-alt run --archive … --manifest …` does the same for an empty database,
with the service's Blockfrost settings. Each restore adds a receipt that binds every polling job
to the new database; job identity, counts and back-off are kept. Freshness is
unknown until the first poll.

Restore refuses a database that is not empty. An interrupted restore leaves the
database owned but not ready (`incomplete_restore`): keep it for diagnosis and
restore into another empty database.

If the upgrade step of a restore refuses (for example `epoch_parameters_changed`
or a failed source request), the backup is already restored and verified: the
database is ready but still of the earlier format, and the reader has not been
granted. Do not restore again: fix the cause, run `db-sync-alt upgrade` on that
database, then `db-sync-alt grant-reader` (or start the service, which grants
the reader). The service reports this state as `schema_upgrade_required`. There is no in-place overwrite and no
automatic clean-up.

## Upgrade to schema 5

The service needs schema 5. A database built with `db-sync-alt acquire --create`
starts as schema 1; a database served by db-sync-alt 0.2.x or 0.3.x is schema 4
and one served by 0.1.x is schema 3. Stop the service, upgrade, then start the
service with this version:

```sh
db-sync-alt upgrade --service db_sync_alt
```

`upgrade` verifies the database in full, then adds the objects and guards of
each newer schema, each step in one transaction, and builds the served tables.
The step to schema 4 adds the [integrity checkpoints](glossary.md#integrity-checkpoint),
makes the serving gate require schema 4 and writes the first checkpoint from that
verification, all in one transaction: node queries keep answering from the last
published update while it runs and answer from schema 4 once it commits. Its
output reports the integrity base (`base_generation`) and the checkpoint written
(`checkpoint_generation`). The first verification walks every update published
since the database's first generation, as a 0.1.x start does: on a large
database it takes about as long as one such start. The service then starts from
the last checkpoint. The step to schema 5 serves each epoch's `max_tx_size` in
`epoch_param`, serves `tx_metadata` (empty until the first update from Blockfrost:
the [bridge metadata coverage](connect-a-node.md#bridge-readiness) starts after
the upgraded update, and the output's `bridge.coverage_start` names that block)
and makes the serving gate require schema 5, in one transaction.

### Epoch parameters of a database of 0.3.x or earlier

A database written before this version holds each epoch's nonce but not its
maximum transaction size, which midnight-node 2.1.101 reads
([connect a node](connect-a-node.md#what-the-node-can-read)). The upgrade adds it for every
stored epoch from an authoritative source, never a default:

- Blockfrost, with the settings the service uses (`DBSYNC_PROVIDER_KEY_FILE`
  and `DBSYNC_PROVIDER_STATE`, or `DBSYNC_PROVIDER_ADAPTER=shared` with
  `DBSYNC_SHARED_ROOT`): two `GET /epochs/{n}/parameters` requests per stored
  epoch and nothing else, counted in the same daily ledger as the service.
  `db-sync-alt status` shows the stored epochs and the requests before you
  start; the upgrade refuses before any request if the day's remaining budget
  is smaller (`provider_budget_insufficient`).
- Or a cardano-db-sync of the network, with `--source-service` and
  `--source-attestation` as below: no Blockfrost request.

Every stored epoch is read in two complete passes. Each answer must name the
epoch, carry the stored nonce and a size from 0 to 2,147,483,647, and both
passes must agree; otherwise the upgrade refuses (`epoch_parameters_changed`,
`epoch_parameters_nonce_mismatch`, `invalid_epoch_max_tx_size`). Answers are
kept as they arrive: an interrupted upgrade continues where it stopped, and once
both passes are kept a rerun sends no request, unless the attempt spent its own
request budget (4 per stored epoch) on failing requests: then its answers are
discarded and a rerun reads again (`request_budget_exhausted`). The sizes, the
new fact contract, a record of the sizes and their source, and schema 5 are
committed in one transaction that also verifies the result; any refusal leaves
the database as it was, still served by the release that wrote it.

That transaction holds exclusive locks on the node-facing `epoch_param`, its
served table and the epoch facts while it verifies the whole database, about as
long as one `db-sync-alt verify`: a running node's epoch reads wait until it
commits. Stop the node as well as the service for the upgrade. If another
session holds a lock on those relations, the commit gives up after 5 seconds
and `upgrade` prints `schema_upgrade_failed`; nothing is changed, the answers
are kept and a rerun sends no request. Earlier generations, their
receipts and integrity checkpoints are kept unchanged and verified as they were
sealed; the record binds the sizes added to their epochs, and integrity
checkpoints of later updates include the sizes.

The upgrade refuses while an unfinished update exists
(`upgrade_pending_generation`): start the earlier release again, let it publish,
and stop it when `db-sync-alt status` shows no pending generation.

There is no downgrade: db-sync-alt 0.3.x and earlier refuse a schema-5 database
and 0.1.x refuses schema 4, both with `unsupported_database_identity`
(`invalid_identity` in the service). To go back, take a backup with the
release that serves the database before the upgrade (`db-sync-alt backup`, with
the service stopped), and restore it with that release's `db-sync-alt restore`
into an empty database. A copy made with plain `pg_dump` and `pg_restore` into a
new database or onto another server does not serve until the next UTC day: each
day's polling job is bound to the database that created it, only
`db-sync-alt restore` binds it to a new one, and the service refuses the copy
with `invalid_sync_job`.
With the Compose deployment, run the same command in the image
([Compose](docker.md#upgrade)).
A database whose inputs and output assets were stored without their
[ledger ordinal](glossary.md#ledger-ordinal) (written by a version before
schema 3) also needs the same-network, retained-history cardano-db-sync and a
truthful source attestation, using both options together:

```sh
db-sync-alt upgrade --service db_sync_alt \
  --source-service dbsync_readonly \
  --source-attestation /run/secrets/source-attestation.json
```

Use the [source attestation format](operations.md#build-initial-data-from-your-own-db-sync)
for the actual 13.7.1.0 or 13.8.0.0 source schema/version. Either option without
the other refuses with `source_attestation_pair_required`. A missing, unreadable
or malformed JSON file refuses with `schema_upgrade_failed`, without a
traceback. Ordinary upgrades whose facts already contain ledger ordinals need
neither source option. Take a new backup afterwards.
The service never upgrades by itself. A restore drops all privileges: run
`db-sync-alt grant-reader` for the node's reader role again afterwards, or pass
`--reader-role` to `db-sync-alt restore`, which grants it once the restored
database serves; the service grants it at every start as well. Until then the
reader cannot connect at all: db-sync-alt removes `PUBLIC`'s privileges on its
database, because a node connected to an empty or half-restored database would
find no `tx_in` relation and remember that until it restarts. Create a database
for a restore the same way, so no reader can connect before db-sync-alt runs:

```sql
CREATE DATABASE db_sync_alt OWNER db_sync_alt_owner ALLOW_CONNECTIONS false;
REVOKE ALL ON DATABASE db_sync_alt FROM PUBLIC;
ALTER DATABASE db_sync_alt ALLOW_CONNECTIONS true;
```

The owner login restores without superuser or `CREATEROLE`.

## When something is refused

| Code | What to do |
| --- | --- |
| `writer_busy` | Find and stop the other writer (the service or another command) |
| `missing_backup` | Supply a snapshot or backup, or build initial data from your own db-sync |
| `incomplete_restore`, `incomplete_owned_destination` | Keep the database; restore into a new empty one |
| `backend_identity_mismatch` | Fix the configured endpoint; never copy or edit identity rows |
| `storage_low` | Free space on the checked volume |
| `completion_recovery_required` | Restore the last verified backup into a new database; never recreate missing seals |
| `canonical_conflict`, revoked serving | Stop, keep the database and logs, investigate the source |
| `quota_exhausted`, provider cooldown, corrupt ledger | Wait for the allowance; investigate corruption without deleting the ledger |

All codes: [troubleshooting](troubleshooting.md).
