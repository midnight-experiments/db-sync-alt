# Preview profile — Midnight node 1.0.0

The six JSON configuration files are byte-for-byte copies of `res/preview/` in
[midnightntwrk/midnight-node at f4a4cd543e6fbdfa34e11671737598f5b40c19fd](https://github.com/midnightntwrk/midnight-node/tree/f4a4cd543e6fbdfa34e11671737598f5b40c19fd/res/preview)
(tag `node-1.0.0`). `source-manifest.json` pins their SHA-256 hashes and the
upstream source files that define how they are read. `profile.json` names the
profile and its network. `network.json` holds the network's values (see
[networks](../../docs/networks.md)) with the provenance of each one.

- Cardano network: `preview` (network magic 2, k = 432, one-day epochs).
- Start cursor: block `0`, transaction index `0`: there is no history before the
  cursor to acquire, and the required epochs start at epoch 0.
- cNIGHT unit: policy `d2dbff622e509dda256fedbd31ef6e9fd98ed49ad91d5c0e07f68af1`,
  empty asset name; the ICS configuration uses the same unit, the reserve
  configuration the asset name `NIGHT`.
- `cnight-config.json` also names a `redemption_validator_address`, which the
  node's queries never read.

The profile is shipped inside the package and is the default of
`db-sync-alt acquire --network preview`. Preview has not been built or qualified
end-to-end with this release; see [compatibility](../../docs/compatibility.md).
