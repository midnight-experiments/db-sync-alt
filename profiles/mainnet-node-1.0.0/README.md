# Mainnet profile — Midnight node 1.0.0

The six JSON configuration files are byte-for-byte copies of `res/mainnet/` in
[midnightntwrk/midnight-node at f4a4cd543e6fbdfa34e11671737598f5b40c19fd](https://github.com/midnightntwrk/midnight-node/tree/f4a4cd543e6fbdfa34e11671737598f5b40c19fd/res/mainnet)
(tag `node-1.0.0`). `source-manifest.json` pins their SHA-256 hashes and the
upstream source files that define how they are read. `profile.json` names the
profile and its network. `network.json` holds the network's values with the provenance of each one.

- Cardano network: `mainnet` (network magic 764824073, k = 2160).
- Start cursor: block `13164005`, transaction index `1`.
- cNIGHT unit: policy `0691b2fecca1ac4f53cb6dfb00b7013e561d1f34403b957cbb5af1fa`,
  asset name `NIGHT` (`4e49474854`); ICS and reserve use the same unit.

db-sync-alt compiles its own profile from Midnight node 2.1.101
([compatibility](../../docs/compatibility.md#networks-and-their-profiles)).
These files are test input. `crates/dsa-chain/tests/legacy_profiles.rs` checks
that the compiled Mainnet profile names the same addresses and asset units, the
same start cursor and the same Cardano epoch values as these files: the
full-history source layout that `convert` reads holds rows selected with them.
`crates/db-sync-alt/tests/ledger_content.rs` reads the registered-candidates
address from `registered-candidates-addresses.json`.
