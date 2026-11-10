# Costs

## Blockfrost requests

Measured at the Preprod chain tip:

| Work | Requests per day |
| --- | ---: |
| Polling the chain tip every 20 seconds (tip, confirmed block, periodic usage check) | ≈ 8,700 |
| Publishing new blocks, ≈ 9 requests per block | ≈ 24,700 |
| **Total** | **≈ 33,400** |

Upgrading a database of 0.3.x or earlier to this version reads each stored
epoch's parameters twice: 2 requests per stored epoch, once
([upgrade to schema 5](recovery.md#upgrade-to-schema-5)); `db-sync-alt status`
shows the number before you start. With a cardano-db-sync source it needs none.

Catching up after a pause costs more: every missed block needs its own requests,
and history before the published snapshot is never fetched from Blockfrost (the
snapshot or your own db-sync provides it). Blocks with many relevant
transactions need more requests than empty ones.

### Limits db-sync-alt enforces

| Limit | Default | Where it is kept |
| --- | --- | --- |
| Requests per UTC day for the key | 50,000 attempts | The request ledger, `accounting/usage.json` in the state directory |
| Request rate | 8 per second | The same ledger |
| One day's polling job | 10,000 attempts | The database (`chain.sync_jobs`) |
| One update (acquisition job) | 10,000 attempts | The database |

Every attempt is counted before it is sent, including failures and usage
checks. Restarting the process never resets a count. When the daily ceiling is
reached the service waits for the next UTC day (`quota_exhausted`). All
programs that use the same Blockfrost key must share one ledger; two ledgers for
one key would each believe they have the whole allowance.

### The key

The key is read from a file only your user can read (mode `600`), never from
the command line or a log:

- native: `db-sync-alt run --blockfrost-key-file PATH` (or a hidden prompt with
  a bare `--blockfrost-key`); it is copied into the private state directory;
- Compose: `deploy/setup-compose.sh` asks for the key file's path and copies it
  into the private Compose directory.

## Memory

On Preprod, in RAM + swap (one measurement, not a guarantee):

| Original full stack | GiB | db-sync-alt stack | GiB |
| --- | ---: | --- | ---: |
| Midnight node | 1.81 | Midnight node | 1.81 |
| Cardano node | 6.66 | db-sync-alt | 0.32 |
| cardano-db-sync | 5.35 | PostgreSQL | 0.30 |
| PostgreSQL | 2.13 | | |
| **Total** | **15.94** | **Total** | **2.42** |

## Disk

The Preprod release snapshot (up to block 5,366,232) restores to about 750 MB,
214 MB of it the served tables the node reads. A database that follows the
chain grows with every update: each update's receipts and block membership are
kept as the record of past updates; bounding that record is on the roadmap.
Keep free space for the database, backups, the snapshot you
restore from, and the free-space reserves db-sync-alt checks before writing
(10 GiB on the state directory's filesystem by default).
