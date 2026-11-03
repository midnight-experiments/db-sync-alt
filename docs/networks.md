# Networks

Everything that differs between Cardano networks comes from the network's
profile, a directory under [`profiles/`](../profiles) that ships inside the
package. Code, database checks and the verifier read these values from the
profile; none of them is written into the code.

## Profile directory

| File | Content |
| --- | --- |
| six Midnight configuration files | Byte-for-byte copies of `res/<network>/` of a midnight-node release: `cnight-config.json`, `federated-authority-config.json`, `permissioned-candidates-config.json`, `registered-candidates-addresses.json`, `ics-config.json`, `reserve-config.json` |
| `source-manifest.json` | Release tag, commit and SHA-256 of each configuration file |
| `profile.json` | Profile name and network |
| `network.json` | The network's values (below) and where each one comes from |

A profile given with `--profile-dir` is accepted only when its `network.json`
equals the packaged one of the same network, its configuration files match
their manifest and their digest equals `midnight.config_sha256`.

## `network.json`

Format `db-sync-alt-network-profile-v1`. Every key is required; unknown keys
are refused.

| Key | Meaning |
| --- | --- |
| `network` | Network name, also the value of `--network` |
| `profile` | Profile name stored with every generation (the node query profile it was built for) |
| `algorithm` | Selection algorithm name in the fact contract |
| `cardano.network_magic` | Cardano network magic; Blockfrost `/genesis` must report it |
| `cardano.security_parameter` | Security parameter k: witness W = target T + k, confirmed target = head − k |
| `cardano.active_slots_coefficient`, `cardano.epoch_length`, `cardano.system_start`, `cardano.epochs` | Cardano genesis and epoch configuration, as the node's `res/cfg/<network>.toml` sets them |
| `cardano.stake_address_prefix` | Bech32 prefix of stake addresses in Blockfrost epoch stakes |
| `cardano.dbsync_network_name` | `meta.network_name` a db-sync source must report |
| `midnight.genesis` | Hash of Midnight block 0, derived from the release's `chain-spec-raw.json` |
| `midnight.chain_spec_raw_sha256` | SHA-256 of that `chain-spec-raw.json` |
| `midnight.config_sha256` | Digest of the six configuration files |
| `midnight.start` | Start cursor S (Cardano block number and transaction index) from `cnight-config.json` |
| `midnight.genesis_cardano_reference` | Cardano block that Midnight block 0 references (`cardano-tip.json`) |
| `midnight.block_stability_margin` | The node's default block stability margin |
| `blockfrost.host`, `blockfrost.key_prefix` | Blockfrost API host of the network and the prefix of its project keys |
| `blockfrost.adapter`, `blockfrost.openapi` | Blockfrost adapter identity stored with every Blockfrost generation |
| `node_queries` | midnight-node release whose queries the packaged `reference/` sources pin |
| `health.serving_lag_max_blocks` | Default `/health` block-lag limit; always below the stability margin |
| `provenance` | Source files and their SHA-256 for the values above |

## Packaged networks

| | Mainnet | Preprod | Preview |
| --- | --- | --- | --- |
| Profile | `midnight-mainnet-node-1.0.0` | `midnight-preprod-node-1.0.400` | `midnight-preview-node-1.0.0` |
| Configuration files from | midnight-node `node-1.0.0` | midnight-node `node-1.0.400` (identical in `node-1.0.0`) | midnight-node `node-1.0.0` |
| Cardano network magic | 764824073 | 1 | 2 |
| Security parameter k | 2160 | 2160 | 432 |
| Epoch length (slots of 1 s) | 432,000 | 432,000 | 86,400 |
| Start cursor S | 13,164,005 : 1 | 4,500,197 : 4 | 0 : 0 |
| Stake-address prefix | `stake1` | `stake_test1` | `stake_test1` |
| db-sync `meta.network_name` | `mainnet` | `preprod` | `preview` |
| Blockfrost host | `cardano-mainnet.blockfrost.io` | `cardano-preprod.blockfrost.io` | `cardano-preview.blockfrost.io` |
| Node stability margin / `/health` block-lag limit | 10 / 9 | 10 / 9 | 10 / 9 |
| End-to-end qualified | no | yes | no |

Preprod is built, followed, served and checked against cardano-db-sync end to
end. The Mainnet and Preview profiles are checked by unit and integration tests
on synthetic data only (for example a Preview build from block 0 with k = 432);
neither network has been built from real data with this release.

With S = 0 (Preview) there is no history before the start cursor: a first
generation has no prerequisites range, and the required epochs start at epoch 0
instead of two epochs before S.

## Selecting a network

`db-sync-alt run --network <name>` and `db-sync-alt acquire --network <name>`
take the packaged profile of that network. The service configuration names its
`network`; its `profile` and `/health` block-lag limit default to the
profile's values, and a limit at or above the node's stability margin is
refused. A database stores its network's fact contract when it is created and
is always opened as that network.

db-sync-alt refuses another network's data or credentials:

| Check | Refusal |
| --- | --- |
| Blockfrost project key prefix is not the network's | `<network>_credential_required` (launcher), `provider_configuration_rejected` (service) |
| Blockfrost `/genesis` reports another network magic | `wrong_provider_network` |
| A shared Blockfrost client points to another host | `provider_configuration_rejected` |
| db-sync `meta.network_name` is not the network's | `source_network_mismatch` |
| Database, backup or profile directory of another network | `network_mismatch`, `backup_profile_mismatch`, `invalid_accepted_backup` |
