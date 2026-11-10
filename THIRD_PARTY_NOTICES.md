# Third-party notices

db-sync-alt is licensed under [Apache-2.0](LICENSE). Copyright notices in
source files remain authoritative; this document does not assign ownership or
imply upstream endorsement.

## Midnight node and Partner Chains reference material

Source: [midnightntwrk/midnight-node at
86774edf8cc76345a5c3a4221f3fe80c094b1c33](https://github.com/midnightntwrk/midnight-node/tree/86774edf8cc76345a5c3a4221f3fe80c094b1c33),
tag `node-2.1.101`.

The seven Rust files under `reference/` are unchanged copies of the corresponding
paths at that commit. They supply the runtime SQL catalogue. Their exact hashes
are recorded in [`reference/source-manifest.json`](reference/source-manifest.json); do not edit them as
ordinary application code. Midnight source files retain their Midnight
Foundation notices. The Partner Chains db-sync model is part of the upstream
vendored component; its package declares Apache-2.0.

- [Midnight node license copy](docs/licenses/midnight-node-LICENSE)
- [Partner Chains license copy](docs/licenses/partner-chains-LICENSE)
- [Pinned Partner Chains package declaration](https://github.com/midnightntwrk/midnight-node/blob/86774edf8cc76345a5c3a4221f3fe80c094b1c33/partner-chains/toolkit/data-sources/db-sync/Cargo.toml)

The six configuration files in `profiles/preprod-node-1.0.400/` are unchanged
copies of upstream `res/preprod/` at commit
`3acfd2edecdcd91373b6a506933f4c14ad3f0308` (tag `node-1.0.400`). Their paths and SHA-256
hashes are recorded in that directory's `source-manifest.json`. The license
copies above were taken at that commit; the upstream root and Partner Chains
root had no `NOTICE` file there. Each reference source keeps its own upstream
copyright and license header.

## Installed dependencies

Python, PostgreSQL and the Python packages are installed at build time, not
vendored here; their own licenses apply. Versions are declared in
`pyproject.toml`, `requirements.lock` and `deploy/Dockerfile`. A built image
keeps the upstream license metadata of its components; redistributed images must
preserve it.


## Cardano db-sync migration fixtures

`tests/fixtures/data/upstream-dbsync-migrations.json` retains exact SQL migrations from IntersectMBO/cardano-db-sync, Apache-2.0. Tags 13.7.1.0 (`ef00c76513947b356b81505ca3792b89322bd792`) and 13.8.0.0 (`ccb9c0f4f6168593ec8d8568a6358dbac84d2045`); each file path/content SHA-256 and the original SQL comments are preserved in the fixture. These test inputs are not snapshot data or a live chain.
