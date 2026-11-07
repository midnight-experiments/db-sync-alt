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
chain.generations WHERE id = …` reads a manifest. `verify` checks every
ready generation and the physical contents in full. `backup` verifies the same
way, then writes an archive (`pg_dump` custom format) and a manifest with the
archive's SHA-256, the profile and the verification result of every generation.
It prints the manifest's SHA-256: keep that value apart from the files. Output
files are never overwritten; an archive without its manifest is not a backup.
Full verification reads all retained history and takes time on a large database.

## Restore into a new, empty database

```sh
db-sync-alt restore --service empty_database \
  --archive /srv/backups/db-sync-alt-1.dump --manifest /srv/backups/db-sync-alt-1.json \
  --expected-manifest-sha256 <manifest sha256>
```

Restore checks the manifest's SHA-256 (when given), the archive's bytes, the
format, the PostgreSQL major version and the profile; it then restores, verifies
every generation and the physical inventory against the manifest, and only then
marks the database ready. `db-sync-alt run --archive … --manifest …` does the same
for an empty database. Each restore adds a receipt that binds every polling job
to the new database; job identity, counts and back-off are kept. Freshness is
unknown until the first poll.

Restore refuses a database that is not empty. An interrupted restore leaves the
database owned but not ready (`incomplete_restore`): keep it for diagnosis and
restore into another empty database. There is no in-place overwrite and no
automatic clean-up.

## Upgrade to schema 3

The service needs schema 3. A database built with `db-sync-alt acquire --create`
starts as schema 1; with the service stopped:

```sh
db-sync-alt upgrade --service db_sync_alt
db-sync-alt verify --service db_sync_alt
```

`upgrade` verifies the database in full, then adds the objects and guards of
each newer schema, each step in one transaction, and builds the served tables.
A database whose inputs and output assets were stored without their
[ledger ordinal](glossary.md#ledger-ordinal) (written by a version before
schema 3) also needs the read-only cardano-db-sync it can take them from:
`db-sync-alt upgrade --service db_sync_alt --source-service dbsync_readonly`. Take a new backup afterwards.
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
