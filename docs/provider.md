# Provider

db-sync-alt reads Cardano through the [Blockfrost API](https://blockfrost.io)
of the network, with a key you give it in a file (`--provider-key-file`). The
service sends its own requests, counts them per UTC day and shows the counts in
`/status`. It does not know the daily allowance of your key and does not
enforce one: when the provider says the allowance is spent, the service waits
for the next day.

## The requests

Every stored value comes from a block's transaction CBOR, whose hashes the
follower checks; spends are matched to the outputs the database holds, without
a request.

| Request | When |
| --- | --- |
| `/blocks/{stored tip hash}/next` | Every poll at the tip, every `--poll-interval-seconds` (default 20): one request when nothing changed. While behind: the headers to fetch, 100 a page. |
| `/blocks/latest` | Only when a page of headers is full, after a "not found", and for the first status. |
| `/blocks/{height}` | After a fork at the tip, to find the highest stored block the provider still has: a binary search, at most 12 requests for k = 2,160. Also to read the target again when the provider contradicts it. |
| `/blocks/{hash}/txs/cbor` | Each block that becomes final and has transactions: one page per 100 transactions. None for an empty block. |
| `/epochs/{n}/parameters` | Once per epoch, before its first block is served. |
| `/scripts/datum/{hash}/cbor` | A datum the node reads whose bytes are not in the transaction. Rare. |
| `/txs/{hash}/metadata` | A valid transaction with an output at the ICS address: the bridge metadata. Rare. |
| `/txs/{hash}/utxos` | An output the decoder cannot classify; it is never taken as not relevant. Rare. |
| `/epochs/{n}`, `/pools/extended`, `/pools`, `/pools/retired`, `/pools/{id}`, `/pools/{id}/history`, `/epochs/{n}/stakes/{pool}` | The stake distribution of each new epoch, read in the background while the service is at the tip, and accepted only when its sum equals the epoch's active stake. It never delays an update. |

## Pace and requests in flight

`--requests-per-second` (default 8, at most 10, the provider's published
limit) spaces the requests. `--max-in-flight` (default 4) is the number of
requests in flight at once: while catching up, the transaction pages of the
next blocks are fetched ahead, and used in chain order. Lower both when several
programs share one key.

`/status` shows the pace, the requests in flight, the calls of today and of
each of the 7 days before, and the provider's last refusal. The counts are
stored in the database with every update and at least every 10 minutes, and
survive a restart.

## Catching up

Measured on 2026-11-23: a database restored from a real snapshot file followed
real blocks, on Preprod to the target of a database built later from the full
history, on Mainnet to the tip.

| Measure | Preprod | Mainnet |
| --- | ---: | ---: |
| Blocks | 43,919 | about 75,000 |
| Requests | 35,071 | 68,961 |
| Requests per block | 0.80 | 0.92 |
| Of them, transaction pages | 32,877 | — |
| Of them, header requests | 891 | — |
| Of them, pool requests (the stake of three epochs) | 1,178 | — |
| Of them, epoch requests | 122 | — |
| Time | 2 h 08 min | 4 h 30 min |
| Requests per second | 5 | 4 to 5 |
| Provider timeouts, retried | 2 | 6 |
| Blocks per second | 5.7 | — |
| Peak resident memory of the process | 42,064 KiB | 42 MB |
| Processor time of the process | 39 s | — |

Conditions: release builds, PostgreSQL 17 in a container with 4 CPUs and 4 GiB
on NVMe storage; on Preprod `--max-in-flight 4`. A dash is a value that was not
measured. On Preprod the four counts cover the follower's first run of 35,068
requests; a final run for the last block sent 3 more. The range needed no
datum, metadata or output request. The database then had the content digest of
a database built from the full history at the same horizon, target and tip.

At the tip the follower sends one request per poll and one page request per
block with transactions, plus the epoch requests. Measured on Preprod on
2026-11-23: 498 requests in 2 h 08 min at the tip, which is 233 an hour or
about 5,600 a day.

## Refusals and outages

| Provider answer | What the service does |
| --- | --- |
| 401 or 403 | The key is rejected: the service stops with `provider_key_rejected` and exit status 1. Check the key file. |
| 402 | The key's daily allowance is spent (`quota_exhausted`). Every request waits until 00:02 UTC of the next day, or 5 minutes when the refusal comes in the first 10 minutes of a UTC day. The node keeps reading the database meanwhile. |
| 418 | The key is banned for a while (`provider_banned`). Every request waits for the time in `Retry-After`, at least 1 hour and at most 24 hours. |
| 429 | Too many requests (`provider_rate_limited`). Every request waits for the time in `Retry-After`, 60 seconds when it names none. |
| 5xx, a timeout, a dropped connection | Retried after 1, 2, 4, 8 and 16 seconds and then every 30 seconds (`provider_unavailable`, `provider_timeout`). After `--database-fatal-after-seconds` (default 1,800) without a success, the service ends with exit status 75 and its restart policy starts it again. |
| An answer that contradicts itself or the chain | Nothing is written. The step is retried every 30 seconds without end and named in `/status` ([troubleshooting](troubleshooting.md#answers-the-follower-refuses)). |

A 402, 418 or 429 is waited out until the time it names and never ends the
service: `/status` shows `waiting`, the code and `wait_until`, and
`provider.last_refusal` keeps it after the wait.
