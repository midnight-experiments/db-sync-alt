# Mainnet profile — Midnight node 1.0.0

The six JSON configuration files are byte-for-byte copies of `res/mainnet/` in
[midnightntwrk/midnight-node at f4a4cd543e6fbdfa34e11671737598f5b40c19fd](https://github.com/midnightntwrk/midnight-node/tree/f4a4cd543e6fbdfa34e11671737598f5b40c19fd/res/mainnet)
(tag `node-1.0.0`). `source-manifest.json` pins their SHA-256 hashes and the
upstream source files that define how they are read. `profile.json` names the
profile and its network. `network.json` holds the network's values (see
[networks](../../docs/networks.md)) with the provenance of each one.

- Cardano network: `mainnet` (network magic 764824073, k = 2160).
- Start cursor: block `13164005`, transaction index `1`.
- cNIGHT unit: policy `0691b2fecca1ac4f53cb6dfb00b7013e561d1f34403b957cbb5af1fa`,
  asset name `NIGHT` (`4e49474854`); ICS and reserve use the same unit.

The profile is shipped inside the package and is the default of
`db-sync-alt acquire --network mainnet`. Mainnet has not been built or qualified
end-to-end with this release; see [compatibility](../../docs/compatibility.md).
