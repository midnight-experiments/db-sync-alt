# Compatibility

What db-sync-alt 0.1.0 supports. Anything not listed is refused at startup or
treated as unsupported.

| Component | Supported | Notes |
| --- | --- | --- |
| Cardano network | Preprod (`--network preprod`): supported and tested end to end. Mainnet, Preview: profiles packaged, not tested | The Mainnet and Preview profiles are checked by tests on synthetic data only; no database has been built from their real chains ([networks](networks.md)) |
| Midnight node | SQL queries of `node-1.0.400` | The query set is read from the unchanged upstream sources in `reference/` (commit `3acfd2edecdcd91373b6a506933f4c14ad3f0308`); other node versions work only if they send the same queries |
| Profile | `profiles/preprod-node-1.0.400`, `profiles/mainnet-node-1.0.0`, `profiles/preview-node-1.0.0` | Six configuration files of the network from one midnight-node release and the network's values (`network.json`); see the [glossary](glossary.md#profile) |
| PostgreSQL server | 17, provided by you | Checked on every connection; other major versions are refused with `unsupported_postgres_version`. The Midnight node's reference environment runs its cardano-db-sync database on PostgreSQL 17 as well, so db-sync-alt's database can share that server; it needs a dedicated database closed to `PUBLIC`, a non-superuser owner login and a reader login, and creates no role ([provision PostgreSQL](operations.md#provision-postgresql)) |
| PostgreSQL client tools | `pg_dump` / `pg_restore` 17 | Needed for backups and restores; the version must match the server |
| Python | 3.11 or newer | The container image uses Python 3.12 |
| Python packages | `psycopg[binary]` 3.2.13 | Pinned with hashes in `requirements.lock` |
| cardano-db-sync (initial data only) | 13.7.1.0, schema 15.48.6, `meta.network_name` of the selected network | Must keep `tx_in` and epoch-stake history; read-only access |
| Blockfrost | The selected network's API (`cardano-<network>.blockfrost.io`) | One project key of that network; requests are counted against a daily ceiling ([costs](costs.md)) |
| Docker / Compose (optional) | Compose 2.24.4 or newer | Only for the [Compose deployment](docker.md) and the test lanes |
| Operating system | Linux on amd64 | Tested there; other platforms are untested |

## Database and backup identity

A database created by db-sync-alt is owned by the `db-sync-alt` identity and has
a schema version (1 to 3). The service and the node's reader need schema 3;
`db-sync-alt upgrade` brings a schema-1 or schema-2 database there
([recovery](recovery.md#upgrade-to-schema-3)). Backups of a schema-3 database use
the format `db-sync-alt-postgresql-backup-v2`; backups of schemas 1 and 2
(`db-sync-alt-postgresql-backup-v1`) still restore, then need the upgrade. A
manifest records the format, the PostgreSQL major version, the schema version,
the profile and the verification result of every generation. A backup is
restored only into an empty PostgreSQL 17 database by the same profile; a
manifest that records another major version is refused
(`unsupported_backup_identity`).
