# Costs

## Blockfrost requests

Counted from the request receipts of a Preprod deployment at the chain tip with
the default settings (full scan, the `sync` values `db-sync-alt run` writes),
over six whole UTC days, 5 to 10 November 2026: 3,834 to 3,893 blocks and 10,609
to 16,975 transactions a day. The counts leave out the usage checks (`/metrics`);
this version makes one per 10,000 requests counted.

| Work | Requests per day |
| --- | ---: |
| Polling the chain tip every 20 seconds (tip, confirmed block, periodic usage check) | ≈ 8,700 |
| Publishing new blocks, 7.3 to 9.5 requests per block | ≈ 28,600 to 36,800 |
| **Total** | **≈ 36,500 to 45,100** |

Without relevance filtering, a block's cost follows its transactions: about 1.3
`/txs/{hash}/utxos` requests per transaction (its own, and some of the outputs
it spends), one transaction page per 100 transactions, and per update about 5
fixed requests at the tip, where an update holds one or two blocks, or about 9
in a catch-up update of 100 blocks. Measured over 99,333 transactions of that
deployment: 1.29 `/utxos` requests per transaction.

Catching up from the published Preprod snapshot (block 5,366,232) costs about
5.5 requests per block for its 3.6 transactions per block on average, and the
blocks of the last seven days before 17 November 2026 about 6 (4.0 transactions
per block). With about 8,700 of the built-in client's 50,000 requests a day
going to polls, each day of snapshot age takes 1.2 to 1.3 days to catch up
([first run](../README.md#option-a-the-published-preprod-snapshot)).

Bridge metadata adds one request per published transaction with an output at
the ICS address (two with relevance filtering, which repeats every request for
its independent pass). Such transactions are rare.

Each new epoch's stake is read per pool, twice, whatever the number of
delegated stake addresses: about 2 × (pools / 100 + pools without stake) for
the current epoch (≈ 1,300 requests on Mainnet), or about 2 × pools when
catching up an epoch that has already ended (≈ 6,100 on Mainnet).

Upgrading a database of 0.3.x or earlier to this version reads each stored
epoch's parameters twice: 2 requests per stored epoch, once
([upgrade to schema 5](recovery.md#upgrade-to-schema-5)); `db-sync-alt status`
shows the number before you start. With a cardano-db-sync source it needs none.

With relevance filtering, an update makes about 6 fixed requests (the committed
block, the previous witness, the target, one page of up to 100 headers, and the
final target and witness), and each block costs about 3 to 7 more for its two
independent passes (its transaction pages, and the answers of selected and
relevant transactions). A Mainnet catch-up of about 20 blocks per update costs
about 6.5 to 7.5 requests per block, plus about 1 per block for polling.

Catching up after a pause costs more: every missed block needs its own requests,
and history before the published snapshot is never fetched from Blockfrost (the
snapshot or your own db-sync provides it). Blocks with many relevant
transactions need more requests than empty ones.

### Limits db-sync-alt enforces

| Limit | Default | Where it is kept |
| --- | --- | --- |
| Requests per UTC day for the key | 50,000 attempts; with the shared adapter, the limit its client reports (up to 10,000,000) | The request ledger, `accounting/usage.json` in the state directory |
| Request rate of the built-in client | 8 per second | The same ledger |
| Requests of an update in flight | 1 (`provider_concurrency`, up to 8) | `service.json` (`sync`) |
| Pace of an update's requests | None of its own (`provider_max_requests_per_hour`, 1 to 28,800) | `service.json` (`sync`) |
| One day's polling job | 10,000 attempts | The database (`chain.sync_jobs`) |
| One update (acquisition job) | 10,000 attempts per UTC day | The database |

Blockfrost limits each IP address to 10 requests per second, with a burst of
500 that cools off at 10 per second, and answers HTTP 429 above that (418 after
repeated floods); db-sync-alt then waits out the cooldown
(`provider_unavailable`). An external client shared by several programs on one
host should space all their requests below that, for example 8 per second for
the whole host. With several requests in flight
([requests in flight](operations.md#requests-in-flight)), a pace keeps a whole
day under the key's daily limit.

Every attempt is counted before it is sent, including failures and usage
checks. Restarting the process never resets a count. When the daily ceiling is
reached the service waits for the next UTC day (`quota_exhausted`).

Quota decisions use the ledger's count. Once per 10,000 requests counted in the
ledger, and on the first request of a new database, the service asks Blockfrost
for the key's usage (`/metrics`). That adds the requests other hosts made with
the same key. The database keeps the count since the last check, across
restarts and UTC days. Between checks, requests from other hosts show only when
Blockfrost answers HTTP 402, which ends the day at once (`quota_exhausted`).

Each update is sized to its blocks: it takes as many blocks as fit three
quarters of its budget at one request per transaction (two with relevance
filtering), and at least one. With relevance filtering the update then holds
only the blocks whose content fits its key limit
([relevance filtering](mainnet-relevance.md#enable-and-observe)).
An update that still uses up its budget waits for the next UTC day
(`request_budget_exhausted`) and then continues where it stopped, under a new
job record that carries the earlier count. All
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

See [memory and disk](../README.md#memory-and-disk): one dated measurement on a
running Preprod deployment, not a guarantee.

## Disk

The Preprod release snapshot (up to block 5,366,232) restored with 0.1.0 to about
750 MB, 214 MB of it the served tables the node reads. A database that follows
the chain grows with every update: each update's receipts and block membership
are kept as the record of past updates; bounding that record is on the roadmap.
The Preprod database restored from that snapshot on 3 November 2026 held
1.79 GiB (`pg_database_size`) on 17 November 2026, after about 32,700 updates.
The Mainnet database built from cardano-db-sync up to block 14,160,853 held
9.28 GiB on 17 November 2026, at block 14,193,809.
Keep free space for the database, backups, the snapshot you
restore from, and the free-space reserves db-sync-alt checks before writing
(10 GiB on the state directory's filesystem by default).
