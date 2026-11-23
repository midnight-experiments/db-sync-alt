# Contributing

Thanks for helping. db-sync-alt is a Rust workspace (`crates/`); build and test
it in a container with the toolchain pinned in `rust-toolchain.toml`.

1. Open an issue first for anything larger than a small fix: describe the
   problem, the expected behavior and, if you can, a reproduction with synthetic
   data.
2. Keep changes small and reviewable. Keep the boundaries explicit: what is
   published, how provider requests are counted, what the reader role can read.
   Never weaken a check to make a test pass.
3. Add tests for changed behavior next to the code in the crate that changes
   (`crates/*/tests`). Tests that need PostgreSQL 17 are marked `#[ignore]` and
   read a disposable server's connection settings from `DSA_TEST_DATABASE`;
   never point it at a real database.
4. Run the checks that CI runs (`.github/workflows/ci.yml`):

   ```sh
   cargo fmt --all -- --check
   cargo clippy --locked --workspace --all-targets -- -D warnings
   cargo test --locked --workspace
   DSA_TEST_DATABASE='host=<test server> user=postgres dbname=dsa_test' \
     cargo test --locked --workspace -- --ignored
   python3 .github/scripts/check_public_source.py
   ```

5. Update the documentation the change affects, and add a line to
   [CHANGELOG.md](CHANGELOG.md).
6. In the pull request, say what changed, how you validated it, and any effect
   on the database schema, backups, provider budgets, the query set or
   operations. Mark breaking changes clearly.

The files under `reference/` and `profiles/` are pinned copies of upstream
sources; changing them is a deliberate compatibility change with new provenance,
not a regenerated hash. Never put private configuration, keys, passwords, real
database exports, provider responses or operational logs in patches or issues.

Maintainer and code owner: [@acedward](https://github.com/acedward). Please
follow the [code of conduct](CODE_OF_CONDUCT.md). Security issues go through
[private reporting](SECURITY.md), not public issues. Contributions are accepted
under the [Apache-2.0](LICENSE) license.
