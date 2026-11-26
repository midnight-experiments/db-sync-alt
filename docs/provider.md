# Provider

db-sync-alt reads Cardano through the [Blockfrost API](https://blockfrost.io)
of the network, with a key you give it in a file (`--provider-key-file`). The
service sends its own requests, counts them per UTC day and shows the counts in
`/status`. It does not know the daily allowance of your key and does not
enforce one: when the provider says the allowance is spent, the service waits
for the next day.

## The requests

Events come from a block's transaction CBOR, whose hashes the follower checks;
spends are matched to the outputs the database holds, without a request. Block
headers, epoch parameters and stake distributions are the provider's answers,
checked for consistency: headers by their links, a stake distribution by its
sum and by the epoch not moving during the read
([limitations](limitations.md#one-provider)).

| Request | When |
| --- | --- |
| `/genesis` | Once when the follower starts, and again before a contradiction of the stored target would close the gate (`canonical_conflict`). Its `network_magic` must be the network's: 764824073 for Mainnet, 1 for Preprod. Another ends the service with `provider_network_mismatch` and exit status 1, and leaves the gate open; a provider of another network contradicts the stored chain without anything being wrong with it. |
| `/blocks/{stored tip hash}/next` | Every poll at the tip, every `--poll-interval-seconds` (default 20): one request when nothing changed. While behind: the headers to fetch, 100 a page. |
| `/blocks/latest` | When a page of headers is full, after a "not found", for the first status, and at the tip at most once a minute while a poll finds nothing after the stored tip: the provider's own head, whose age `/health` checks. When that head stands above the stored tip on two such reads in a row while the poll stays empty, the stored tip has left the provider's chain and the fork search runs. |
| `/blocks/{height}` | After a fork at the tip, to find the highest stored block the provider still has: a binary search, at most 12 requests for k = 2,160. Also to read the target again when the provider contradicts it. |
| `/blocks/{hash}/txs/cbor` | Each block that becomes final and has transactions: one page per 100 transactions. None for an empty block. |
| `/epochs/{n}/parameters` | Once per epoch, before its first block is served. |
| `/scripts/datum/{hash}/cbor` | A datum the node reads whose bytes are not in the transaction. Rare. |
| `/txs/{hash}/metadata` | A valid transaction with an output at the ICS address: the bridge metadata. Rare. |
| `/txs/{hash}/utxos` | An output the decoder cannot classify; it is never taken as not relevant. Rare. |
| `/epochs/{n}`, `/pools/extended`, `/pools`, `/pools/retired`, `/pools/{id}`, `/pools/{id}/history`, `/epochs/{n}/stakes/{pool}` | The stake distribution of each epoch without one, read in the background while the service is at the tip ([stake distributions](#stake-distributions)). It never delays an update. |

## Stake distributions

The node selects its committee from the stake distribution of each epoch. The
service reads it in the background, one epoch at a time: the oldest epoch of
the window without a stored distribution whose own retry time has passed. The
epoch of the provider's latest block is read from `/pools/extended`, the pools
retired in the last two epochs and, for a pool with zero stake, the epoch's
listing of that pool. An ended epoch is read from the history of every
registered pool and of the pools retired in the two epochs before it: a
history row of the epoch with stake above zero counts whatever its delegator
count; a row with zero stake counts only when it names a delegator (the pool
then holds stake of amount zero) and is skipped otherwise. A distribution is
stored only when its sum equals the epoch's active stake from `/epochs/{n}`
and the latest block's epoch did not move during the read.

Within a read, a request that fails in transport (a timeout, a dropped
connection, a 5xx) is sent again after the wait the client names, up to 5
times, and a 402, 418 or 429 is waited out until the time it names; one failed
request does not cost the whole read. A read that fails is tried again for
that epoch only: after a transport failure in 1, 2, 4, 8 and 16 seconds and
then every 30 seconds; after a refusal of the answers (a sum that does not match, a pool listed twice, an
invalid row) in 1, 2, 4, 8, 16, 30, 60 and 300 seconds and then every 600
seconds. Meanwhile the newer epochs are read, so one epoch that cannot be read
does not hold back the others. `/status` shows the last failure in
`epochs.stake_failure`, with the epoch, the number of failures, the time of
the next attempt and the error with its cause ([troubleshooting](troubleshooting.md#stake-distributions)),
and the warning `stake_missing:<epoch>` while the target's epoch has had no
stored distribution for more than 10 minutes at the tip.

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

At the tip the follower sends one request per poll, at most one `/blocks/latest`
a minute while the polls find nothing new, one page request per block with
transactions, and the epoch requests. At the default poll of 20 seconds that is
4,320 polls a day and at most 1,440 reads of the head.

## A build from the provider

`rebuild` builds a database without a snapshot file and sends its own
requests ([rebuild](rebuild.md#what-it-reads)): the listings of the profile's
assets and addresses, the mints and burns and the quantity of each asset unit,
the transactions since the horizon with their inputs, the full transaction
lists below the horizon (of each profile address with the outputs of every
transaction in it), then every block of the window as the service reads it.
Measured on Preprod, a build of 34,653 blocks sent 26,653 requests and a
build of a 7-day window (27,296 blocks) 20,331. On Mainnet
a build has not been run; the expected request count for a 7-day window is an
estimate of about 270,000 requests, and the histories of the profile
addresses on top.
Check the daily allowance of the key's plan before such a build
([rebuild](rebuild.md#cost)).

## Refusals and outages

| Provider answer | What the service does |
| --- | --- |
| 401 or 403 | The key is rejected: the service stops with `provider_key_rejected` and exit status 1. Check the key file. |
| 402 | The key's daily allowance is spent (`quota_exhausted`). Every request waits until 00:02 UTC of the next day, or 5 minutes when the refusal comes in the first 10 minutes of a UTC day. The node keeps reading the database meanwhile. |
| 418 | The key is banned for a while (`provider_banned`). Every request waits for the time in `Retry-After`, at least 1 hour and at most 24 hours. |
| 429 | Too many requests (`provider_rate_limited`). Every request waits for the time in `Retry-After`, 60 seconds when it names none. |
| 5xx, a timeout, a dropped connection | Retried after 1, 2, 4, 8 and 16 seconds and then every 30 seconds (`provider_unavailable`, `provider_timeout`). After `--database-fatal-after-seconds` (default 1,800) without a success, the service ends with exit status 75 and its restart policy starts it again. |
| An answer that contradicts itself or the chain, or that the client refuses: not valid for the endpoint, larger than its limit, a 404 where the endpoint must answer, another 4xx or a 3xx | Nothing is written. The step is retried every 30 seconds without end and named in `/status` ([troubleshooting](troubleshooting.md#answers-the-follower-refuses)). |

A 402, 418 or 429 is waited out until the time it names and never ends the
service: `/status` shows `waiting`, the code and `wait_until`, and
`provider.last_refusal` keeps it after the wait.
