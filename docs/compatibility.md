# Compatibility

What db-sync-alt 0.4.2 supports. Anything not listed is refused at startup or
treated as unsupported.

| Component | Supported | Notes |
| --- | --- | --- |
| Cardano network | Preprod (`--network preprod`): supported and tested end to end. Mainnet, Preview: profiles packaged, not tested | The Mainnet and Preview profiles are checked by tests on synthetic data only; no database has been built from their real chains ([networks](networks.md)) |
| Midnight node | SQL queries of `node-2.1.101` | The query set is read from the unchanged upstream sources in `reference/` (commit `86774edf8cc76345a5c3a4221f3fe80c094b1c33`), including `MAX(epoch_param.max_tx_size)` of the cost-based cNIGHT observation; other node versions work only if they send the same queries |
| Profile | `profiles/preprod-node-1.0.400`, `profiles/mainnet-node-1.0.0`, `profiles/preview-node-1.0.0` | Six configuration files of the network from one midnight-node release and the network's values (`network.json`); see the [glossary](glossary.md#profile) |
| PostgreSQL server | 17, provided by you | Checked on every connection; other major versions are refused with `unsupported_postgres_version`. The Midnight node's reference environment runs its cardano-db-sync database on PostgreSQL 17 as well, so db-sync-alt's database can share that server; it needs a dedicated database closed to `PUBLIC`, a non-superuser owner login and a reader login, and creates no role ([provision PostgreSQL](operations.md#provision-postgresql)) |
| PostgreSQL client tools | `pg_dump` / `pg_restore` 17 | Needed for backups and restores; the version must match the server |
| Python | 3.11 or newer | The container image uses Python 3.12 |
| Python packages | `psycopg[binary]` 3.2.13 | Pinned with hashes in `requirements.lock` |
| cardano-db-sync (initial data only) | 13.7.1.0/schema 15.48.6 or 13.8.0.0/schema 16.52.6; `meta.network_name` of the selected network | Core full-history mode only; required types/domains/nullability (including `epoch_param.max_tx_size`), mode markers and epoch coverage checked; truthful retained `tx_in`/epoch-stake attestation |
| Blockfrost | The selected network's API (`cardano-<network>.blockfrost.io`) | One project key of that network; requests are counted against a daily ceiling ([costs](costs.md)) |
| Docker / Compose (optional) | Compose 2.24.4 or newer | Only for the [Compose deployment](docker.md) and the test lanes |
| Operating system | Linux on amd64 | Tested there; other platforms are untested |

## Database and backup identity

A database created by db-sync-alt is owned by the `db-sync-alt` identity and has
a schema version (1 to 5) and a fact format. The service and the node's reader
need schema 5 with fact format `midnight-compact-cardano-v2`, whose epoch facts
carry `max_tx_size` and which holds bridge metadata (`tx_metadata`) from its
coverage start. It holds a phase-2-invalid transaction as cardano-db-sync 13.8
does: `valid_contract` false, its collateral inputs as `tx_in` and its
collateral return as `tx_out` (at the index after the body outputs); its body
inputs, outputs and mint never took effect and are not held. `db-sync-alt upgrade` brings an older database there
([recovery](recovery.md#upgrade-to-schema-5)); a database of 0.3.x or earlier
(fact format `midnight-compact-cardano-v1`) also needs each stored epoch's size
from Blockfrost or a cardano-db-sync. db-sync-alt 0.3.x and earlier refuse an
upgraded database. Only schema-5 databases are backed up, in the format
`db-sync-alt-postgresql-backup-v4`. Backups of schemas 3 and 4
(`db-sync-alt-postgresql-backup-v2` and `-v3`, such as the 0.1.0 Preprod
snapshot) restore and are upgraded to schema 5 during the restore, with the
same source; backups of schemas 1 and 2 (`db-sync-alt-postgresql-backup-v1`)
restore, then need the upgrade. A manifest records the format, the PostgreSQL
major version, the schema version, the profile, the verification result of the
canonical generation (formats v3 and v4) or of every generation (formats v1 and
v2), and the physical inventory. A backup is
restored only into an empty PostgreSQL 17 database by the same profile; a
manifest that records another major version is refused
(`unsupported_backup_identity`).


## Source schema qualification

Source schema support is separate from the persisted compact fact contract.
The adapter accepts only `(15,48,6)` as 13.7.1.0 and `(16,52,6)` as 13.8.0.0.
The source manifest stores the actual tuple/version. The compact contract and
packaged profiles are unchanged; 13.6, unfinished stage-three indexes and
unknown schemas remain unsupported. `meta.version` describes the creating
binary and is not used to label the current schema.

Required core relations must retain their upstream column types, domains and
nullability. Address-table, consumed-output, pruning and UTxO bootstrap modes
are refused even if ordinary inputs remain. Unknown extra-migration tokens are
refused; empty mode history and `StakeDistrEnded` are allowed. Required epochs
need completed stake progress, a nonce, a valid `max_tx_size` in every
`epoch_param` row (the fact is their maximum) and stake rows. Extraction and
independent verification keep their full checks.

The operator must truthfully attest retained `tx_in` and epoch-stake history,
including the absence of ledger, asset or Plutus filtering. Schema/mode and
nonempty epoch checks cannot prove all semantic history. Use these same source
options with any packaged network profile; source and destination must still
belong to the selected network. Real-source retention and node acceptance
remain deployment qualifications.
