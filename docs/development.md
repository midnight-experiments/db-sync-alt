# Development

All checks run in Docker: the host needs Python 3.11 or newer and Docker with
Compose 2.24.4 or newer, but no PostgreSQL and no Python packages. The runner
uses the current Docker context, names every container, image and directory it
creates after a random run id, publishes no database port, and removes its own
resources when it finishes. None of the checks needs a Blockfrost key.

```sh
python3 tools/dev.py lint                     # ruff (pinned version) on the source tree
python3 tools/dev.py unit                     # tests/unit, no database, no network
python3 tools/dev.py integration              # tests/integration against one disposable PostgreSQL 17
python3 tools/dev.py integration --shard 2/3  # one of three balanced parts, as CI runs them
python3 tools/dev.py e2e                      # Compose smoke of the built image
python3 tools/dev.py all                      # lint, unit, integration and e2e
python3 .github/scripts/check_public_source.py
```

To reuse an image you built (the runner keeps an image it did not build):

```sh
docker build -f deploy/Dockerfile -t db-sync-alt:local .
python3 tools/dev.py unit --image db-sync-alt:local
```

## Test layout

| Directory | Needs | Lane |
| --- | --- | --- |
| `tests/unit/` | nothing (runs with `--network none`) | `unit` |
| `tests/integration/` | `DB_SYNC_ALT_TEST_DSN`: a superuser DSN on a disposable PostgreSQL 17 server | `integration` |
| `tests/e2e/` | Docker with Compose and the built image | `e2e` |
| `tests/conformance/` | a frozen corpus and a db-sync-alt database, read as the reader login (real data) | `conformance`, `drills` |
| `tests/fixtures/` | shared builders and data; never run on their own | — |

A lane fails on any skipped test: a test that cannot run in its lane is a
failure, not a skip. The integration lane also passes
`DB_SYNC_ALT_TEST_RESTART_DIR`, a directory through which a test asks the runner
to restart the PostgreSQL server. Never point `DB_SYNC_ALT_TEST_DSN` at a real
database: the tests create and drop databases and roles.

Integration tests start from small synthetic stores that the fixtures write
straight into PostgreSQL (`tests/fixtures/synthetic.py`): a three-generation
db-sync-shaped store (`generations.py`) and a generation 0 with fake Blockfrost
routes for the next update (`blockfrost_generation.py`). Each generation is
checked by the service's own verifier before it is marked ready. Sync tests
compare the service's result with the full Blockfrost update run on a fresh copy
of the same generation 0.

The Compose smoke (`e2e`) seeds a backup, starts PostgreSQL, provisions it as
the operator (owner and reader logins, the database), starts the service with a
synthetic in-process provider, checks health and the verified reader, runs the
node's queries as the reader from a container on the `node` network only, stops
the service and reads again, restarts it and checks again. It uses random loopback
ports above 10000 and private temporary directories, which the Docker daemon
must be able to bind-mount.

## Conformance and drills

Two runs check a database as the node reads it (the reader login and the
connection string midnight-node is given) with real data. They are not part of `all` or CI, and they never skip: without their
inputs they fail.

```sh
PGPASSFILE=/path/to/pgpass python3 tools/dev.py conformance \
  --corpus /path/to/corpus.jsonl.gz --corpus-sha256 <sha256> \
  --dsn 'host=127.0.0.1 port=<port> dbname=<db> user=<reader> sslmode=disable' \
  --report /path/to/out/report.json [--capture /path/to/out/answers.jsonl.gz]
PGPASSFILE=/path/to/pgpass python3 tools/dev.py drills --corpus ... --dsn ... \
  --params /path/to/params.json --control-dir /path/to/control \
  --report /path/to/out/drills.json [--drill NAME ...] [--outage-seconds 600]
```

`conformance` runs every case of a frozen corpus of cardano-db-sync answers
(gzip JSON Lines; the inventory, generation and profile identity are read from
the file) the way midnight-node runs it: the same SQL text, prepared with the
parameter types its SQLx driver binds, binary parameters and binary results,
the ID-bound and asset-resolution helpers run on the endpoint itself. Answers
are compared after the corpus's normalization; surrogate IDs never take part in
a comparison. Catalogue statements the corpus has no case for are run as
supplementary cases with the reason they are or are not on the node's run path
(`tests/conformance/supplement.py`); the run fails unless every one of the
catalogue's statements is exercised. The report lists every family, every
failure and per-family latency. The endpoint must serve exactly the corpus's
generation, which the run checks first.

`drills` runs node queries in states where the data must not be trusted or is
not published: outside the covered epochs, below the coverage start, beyond the
witness, for addresses and assets outside the profile, the bridge query, while
the service restarts, while the updater is stopped, across a publication and
after revocation. A probe may fail (fail-closed) or return the published
answer; any other answer, including empty rows, fails the drill. A query the
node never sends (outside the covered epochs or blocks, or the profile) may also
be answered, as `unreachable`, when the database covers the edges of the node's
domain; while the updater is stopped every probe must keep answering. Drills that
change the deployment ask an external controller through `--control-dir`
(protocol in `tests/conformance/drills.py`).

## Memory

`tools/memory.py` measures a deployment from the host: cgroup v2
`memory.current`, `memory.peak`, `memory.swap.current` and `memory.stat` per
container or systemd unit, the PSS of every PostgreSQL process (by kind, and the
number of client backends) and the PostgreSQL memory settings. It only reads.

```sh
python3 tools/memory.py snapshot --container <service> --pg-container <postgres>
python3 tools/memory.py sample --container <service> --pg-container <postgres> --interval 2 --duration 600 --out samples.tsv
python3 tools/memory.py summary samples.tsv
```

## CI

`.github/workflows/ci.yml` runs, as parallel jobs with read-only permissions and
no secrets: the source check and lint, the unit lane, three integration shards
and the Compose smoke. Shards are balanced on the measured module times in
`tools/dev.py`.

## Lint

`ruff check` with the configuration in `pyproject.toml`: pyflakes and the
`E4`/`E7`/`E9` errors. The code keeps short guard clauses and grouped statements
on one line, so the one-statement-per-line rules are off. Formatting is not
enforced.

## Source check

`.github/scripts/check_public_source.py` checks every file of the tree: no
unexpected top-level paths, symlinks, binaries other than the approved
screenshot (pinned by SHA-256), operational artifacts (`.dump`, `.db`, `.log`,
keys), common secret and personal-path patterns, or broken local Markdown links;
and the pinned hashes of `reference/` and `profiles/` still match. It prints
file names and finding codes, never the matching text. Exact fake credentials
are allowed only where the redaction tests and the connection examples need
them. Check a clean export the same way:

```sh
python3 .github/scripts/check_public_source.py --root /path/to/clean-export
```

## Dependencies

The Python and PostgreSQL base images are pinned by digest; Python packages and
the build backend are pinned with hashes in `requirements.lock`; ruff's version
is pinned in `tools/dev.py`. The image installs the PostgreSQL 17 client tools
from the PostgreSQL apt repository (`apt.postgresql.org`) at build time; apt
verifies the repository with its signing key, kept in `deploy/postgresql-apt.asc`
(fingerprint `B97B 0AFC AA1A 47F0 44F2 44A0 7FCC 7D46 ACCC 4CF8`) and pinned by
SHA-256 in `deploy/Dockerfile`. Two builds are therefore not byte-identical;
record the digest of the image you deploy. Validation targets Linux amd64.

## What the checks cover

| Check | Covers |
| --- | --- |
| Unit | Parsing, the fact contract, cancellation, redaction, provider accounting with fakes, health logic |
| Integration | Store, verification, writer fencing, publication, backup/restore, sync and follow paths, the reader role, the gate and sharing a server, against real PostgreSQL |
| Compose smoke | The built image, configuration, operator provisioning, restore as the owner, health, the node's queries as the reader on the node network, updater stop and restart |
| Source check | Repository contents, pinned upstream inputs, disclosure patterns |

They do not cover a real Blockfrost key, live Preprod data, a real Midnight node,
long-running soak or host reboots.
