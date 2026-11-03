# Preprod profile — Midnight node 1.0.400

The six JSON configuration files are byte-for-byte copies of `res/preprod/` in
[midnightntwrk/midnight-node at 3acfd2edecdcd91373b6a506933f4c14ad3f0308](https://github.com/midnightntwrk/midnight-node/tree/3acfd2edecdcd91373b6a506933f4c14ad3f0308/res/preprod)
(tag `node-1.0.400`). `source-manifest.json` pins their SHA-256 hashes and the
upstream source files that define how they are read. `profile.json` names the
profile and its network. `network.json` holds the network's values (see
[networks](../../docs/networks.md)) with the provenance of each one.

- Cardano network: `preprod`.
- Start cursor: block `4500197`, transaction index `4`.
- cNIGHT unit: policy `d2dbff622e509dda256fedbd31ef6e9fd98ed49ad91d5c0e07f68af1`,
  empty asset name.
- ICS and reserve unit: the same policy, asset name `NIGHT` (`4e49474854`).
- Asset names in the cNIGHT configuration are literal UTF-8. The node's ICS
  parser tries hexadecimal first; db-sync-alt accepts only unambiguous UTF-8
  auxiliary names and refuses names that also read as hexadecimal, before any
  request.

The profile is shipped inside the package and is the default of
`db-sync-alt acquire --profile-dir`. See the [glossary](../../docs/glossary.md#profile)
and [compatibility](../../docs/compatibility.md).
