# Glossary

Terms the other pages use. Each page explains a term on first use or links
here.

## Chain and window

| Term | Meaning |
| --- | --- |
| provider | The Blockfrost API of the network. db-sync-alt reads Cardano blocks from it with a key you give it ([provider](provider.md)). |
| tip | The newest Cardano block the provider reports. |
| k | Cardano's security parameter: 2,160 blocks on Mainnet and Preprod. A block k blocks below the tip is final. |
| target | The newest block whose events are served: the tip minus k. The database stores the headers of the k blocks above the target too, because the node computes its stable block as the newest block minus k. |
| relevant | An output at an address of the [profile](#profile), or holding one of its assets; a spend of such an output; a mint of a profile asset. |
| event | A relevant output, spend or mint, with its transaction and block header. |
| first served | The time this database first served a block's events. A restore counts as first serving every block of its window at that moment. |
| retention | How long a block's events stay after they are first served. Default 7 days, at least 24 hours. |
| horizon | The oldest block whose events are all kept. Blocks below it are pruned. It moves when its first-served time is older than the retention. |
| window | The blocks from the horizon to the target. |
| unspent set | The relevant outputs created below the horizon and not spent below it. Pruning never removes them. |
| governance records | The council, technical-committee and permissioned-candidates outputs a node can still read although they are old or spent. They are kept with their transaction, block and datum. |
| keep rule | The definition of what the database keeps: the window, the unspent set, the governance records, the epoch values of the window, and the asset descriptors these rows refer to. It has a version (2) bound to Midnight node 2.1.101. Everything else is deleted. |
| epoch parameters | The nonce and maximum transaction size of an epoch (`epoch_param`). The node reads them during import; they are stored before the first event of their epoch is served. |
| stake distribution | The stake per pool of an epoch (`epoch_stake`, `pool_hash`). Only operator RPCs of the node read it; it is stored for recent epochs, in the background. |
| update | One step of the follower: new headers, the events of the blocks that became final, and any horizon advance, written in one database transaction. |
| catch-up | Following while the database is more than one batch behind the tip: updates follow each other without a pause. |

## Database

| Term | Meaning |
| --- | --- |
| served tables | The eleven tables the node reads, shaped as cardano-db-sync shapes them: `block`, `tx`, `tx_out`, `tx_in`, `ma_tx_out`, `multi_asset`, `datum`, `epoch_param`, `pool_hash`, `epoch_stake`, `tx_metadata`. They live in schema `serve`; the node reads them through views of the same names in schema `public`. |
| gate | The check every view runs. While the database is not serving (empty, restoring, or closed by a check), every view raises SQLSTATE 55000 (`db_sync_alt_not_serving`), so the node never takes an empty answer for a real one. |
| closed gate | The gate after a check found a fault: `canonical_conflict`, `asset_conservation` or `supply_mismatch`. The node keeps getting 55000 until the database is restored from a snapshot file ([operations](operations.md#a-closed-gate)). |
| positional ID | The `id` of a row in `tx`, `tx_out`, `ma_tx_out` and `tx_in`, computed from the row's position on the chain, so that two databases at the same blocks hold the same IDs. |
| owner login | The PostgreSQL login db-sync-alt runs as. It owns the database and has no superuser rights. |
| reader login | The PostgreSQL login of the Midnight node. It may read the eleven views and call the gate, nothing else. |
| writer lock | A PostgreSQL session lock. Only one process that writes may hold a database; a second one is refused (`writer_busy`). |
| supply check | The comparison of the unspent amount of each fixed-supply asset with the supply recorded in the database. |

## Files

| Term | Meaning |
| --- | --- |
| snapshot file | One file holding a whole database and its description, for publishing and restoring ([snapshots](snapshots.md)). |
| content digest | A SHA-256 over every row the node can read, by natural key. Two databases at the same horizon, target and tip have the same content digest, whether they were restored or followed. |
| state digest | A SHA-256 over the database's record of outputs spent below the horizon, which the node cannot read but the keep rule needs. |

## Profile

The values of one network that db-sync-alt is built with: the Cardano
addresses and asset units the node watches, k, the node's stability margin and
the first block the node reads. They are generated from the configuration files
of Midnight node 2.1.101 and compiled into the binary; `db-sync-alt profile
--network <network>` prints them. A database records the hash of its profile
and is refused by a binary with another one (`profile_hash_mismatch`).
