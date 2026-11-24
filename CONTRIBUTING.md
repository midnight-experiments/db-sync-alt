# Contributing

Thanks for helping. db-sync-alt is a Rust workspace:

| Crate | What it holds |
| --- | --- |
| `crates/dsa-chain` | No I/O: profiles, positional IDs, the transaction decoder, datum and metadata rendering, block processing and the content digest. |
| `crates/dsa-store` | PostgreSQL: the layout, the gate, restore, snapshot, verify, the keep rule, the follower's writes, conversion and comparison. |
| `crates/dsa-provider` | The Blockfrost client. |
| `crates/dsa-service` | The follower, the status document, the HTTP endpoints, the upkeep and the service. |
| `crates/db-sync-alt` | The binary: the command line and the command wrapper. |
| `crates/dsa-testkit` | Test support only: a transaction builder, a fake provider and generated chains. Never a dependency of the binary. |
| `tools/corpus` | Tools that record and compare real provider answers; not built by default. |

## What you need

Docker. Build and test in the container image the CI uses
(`rust:1.100.0-bookworm`, the toolchain pinned in `rust-toolchain.toml`), so
that nothing depends on the tools of your machine. The checks under
`.github/scripts` are Python 3 scripts, which that image includes. db-sync-alt
itself needs no Python.

## Build and test

From the repository root:

```sh
docker run --rm -v "$PWD":/work -w /work rust:1.100.0-bookworm bash -c '
  cargo fmt --all -- --check &&
  cargo clippy --locked --workspace --all-targets -- -D warnings &&
  cargo test --locked --workspace &&
  cargo build --release --locked -p db-sync-alt'
```

The tests that need PostgreSQL are marked `#[ignore]` and read the connection
settings of a disposable server, PostgreSQL 17 or newer, from
`DSA_TEST_DATABASE`. Never point it at a database you care about: the tests
create and drop databases. CI runs them on PostgreSQL 17, 18 and 19.
Run them against a PostgreSQL container on a private network, without a
published port:

```sh
docker network create dsa-test
docker run -d --name dsa-test-pg --network dsa-test \
  -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=dsa_test postgres:17
docker run --rm --network dsa-test -v "$PWD":/work -w /work \
  -e DSA_TEST_DATABASE='host=dsa-test-pg port=5432 user=postgres dbname=dsa_test' \
  -e DSA_KEEP_CASES=64 rust:1.100.0-bookworm \
  cargo test --locked --workspace -- --ignored \
    --skip keep_rule_realistic_plans --skip convert_scaled_source_cost \
    --skip compare_cli_tls_reader_files_and_certificate_identity
docker rm -f dsa-test-pg
docker network rm dsa-test
```

`DSA_KEEP_CASES` sets how many generated chains the keep-rule tests check; CI
uses 64. The three skipped tests are the long ones CI also skips.
`DSA_TEST_DATABASE_OTHER`, in the same form, names a second disposable server
of another major version: `snapshot_file_moves_between_major_versions` then
moves snapshot files between the two servers in both directions; without it,
that test returns at once. CI gives the jobs of 18 and 19 a second server,
PostgreSQL 17. CI also sets `DSA_TEST_SERVER_MAJOR`, the major version the
server must have, and in the jobs of 18 and 19 `DSA_TEST_REQUIRE_OTHER`, with
which that test fails when no second server is given.

The binary has a hidden command, `follow`, that runs only the follower and
stops at `--stop-at-target`; the tests use it to prove catch-ups, and
operators use `run`.

The CI workflow (`.github/workflows/ci.yml`) runs these and also:

```sh
python3 .github/scripts/check_public_source.py
python3 .github/scripts/test_public_source.py
python3 .github/scripts/test_reference_source.py
python3 .github/scripts/test_third_party_notices.py
python3 .github/scripts/third_party_notices.py --check
```

`check_public_source.py` checks that the tree holds no private path, key or
credential, that pinned upstream files keep their hashes, that every relative
link in the Markdown files and every code in `docs/troubleshooting.md`
resolves, and that no prose file names a version before 0.5.0, an update path
or a comparison with before. When `Cargo.lock` changes, run
`python3 .github/scripts/third_party_notices.py` in the container to write
`THIRD_PARTY_NOTICES.md` and `THIRD_PARTY_LICENSES.txt` again.

## Documentation

The documentation describes the software as it is. Tests keep parts of it
equal to the code: `crates/db-sync-alt/tests/documentation.rs` compares
`docs/configuration.md` with the `--help` of every command, the exit statuses
of `docs/operations.md` with the binary's, and the `/status` fields and
`/health` reason codes with the status document. They also check that no page
and no `--help` names a hidden command or flag, a version before 0.5.0 or an
update path. A change that adds a flag, a field or a code fails these tests until
the page says what it means.

The binary has two commands outside the documented interface, `follow` and
`convert` (with the `compare` flag `--a-capture`, which reads the answers
`convert` captures), used in development and maintenance: `--help` does not
list them and the pages do not describe them.

## Changes

1. Open an issue first for anything larger than a small fix: the problem, the
   expected behaviour and, if you can, a reproduction with synthetic data.
2. Keep changes small and reviewable. Never weaken a check to make a test pass.
3. Add tests for changed behaviour in the crate that changes
   (`crates/*/tests`), and show that each new test fails without the change.
4. Update the documentation the change affects, and add a line to
   [CHANGELOG.md](CHANGELOG.md).
5. In the pull request, say what changed, how you validated it, and its effect
   on the database format, the snapshot file format, the keep rule, the
   provider requests, the node's queries or operations. Mark breaking changes
   clearly.

The files under `reference/` and `profiles/` are pinned copies of upstream
sources; changing them is a deliberate compatibility change with new
provenance, not a regenerated hash. Never put private configuration, keys,
passwords, database dumps, provider answers with keys, or operational logs in
patches or issues.

Maintainer and code owner: [@acedward](https://github.com/acedward). Please
follow the [code of conduct](CODE_OF_CONDUCT.md). Security issues go through
[private reporting](SECURITY.md), not public issues. Contributions are accepted
under the [Apache-2.0](LICENSE) license.
