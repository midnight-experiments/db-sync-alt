# Contributing

Thanks for helping. Start with [architecture](docs/architecture.md) and
[development](docs/development.md); everything runs in Docker.

1. Open an issue first for anything larger than a small fix: describe the
   problem, the expected behavior and, if you can, a reproduction with synthetic
   data.
2. Keep changes small and reviewable. Keep the boundaries explicit: what is
   published, how provider requests are counted, what the reader role can read.
   Never weaken a check to make a test pass.
3. Add tests for changed behavior in the lane that fits it (`tests/unit`,
   `tests/integration`, `tests/e2e`; see [development](docs/development.md)).
   A skipped test fails its lane.
4. Run the checks:

   ```sh
   python3 tools/dev.py all
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
