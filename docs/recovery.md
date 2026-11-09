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
ready. A backup of a schema-3 database (format v2, such as the 0.1.0 Preprod
snapshot) is then upgraded to schema 4, with its first integrity checkpoint
where one can be written, before the reader is granted. `db-sync-alt run --archive … --manifest …` does the same
for an empty database. Each restore adds a receipt that binds every polling job
to the new database; job identity, counts and back-off are kept. Freshness is
unknown until the first poll.

Restore refuses a database that is not empty. An interrupted restore leaves the
database owned but not ready (`incomplete_restore`): keep it for diagnosis and
restore into another empty database. There is no in-place overwrite and no
automatic clean-up.

## Upgrade to schema 4

The service needs schema 4. A database built with `db-sync-alt acquire --create`
starts as schema 1; a database served by db-sync-alt 0.1.x is schema 3. Stop the
service, upgrade, then start the service with this version:

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
the last checkpoint.

There is no downgrade: db-sync-alt 0.1.x refuses a schema-4 database
(`unsupported_database_identity`). To go back, keep a `pg_dump` of the database
taken with the service stopped before the upgrade, and restore it.
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
