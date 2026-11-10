# Limitations

- **One query set; one qualified network.** The SQL queries of Midnight node
  2.1.101 only. Cardano Mainnet, Preprod and Preview have network profiles, but
  only Preprod has been built, followed and checked against cardano-db-sync end
  to end; Mainnet and Preview are tested on synthetic data only
  ([networks](networks.md)). Other networks have no profile and are refused.
- **Node configuration must match the profile.** The node's
  `CARDANO_SECURITY_PARAMETER` must equal the profile's k, and its Cardano
  addresses and policies must be the profile's. db-sync-alt cannot see the
  node's settings: a mismatch gives the node empty answers. Given the node's
  values, the service reports a mismatch as a warning
  ([node requirements](connect-a-node.md#node-requirements)); it cannot see
  on-chain governance changes of those addresses.
- **Not a db-sync replacement.** db-sync-alt keeps only the Cardano data the
  profile selects in the relations the pinned Midnight queries read. Other
  assets or addresses and ranges outside the [coverage](glossary.md#coverage)
  are not held; the reader login can read nothing else. Candidate registrations, invalid
  transactions and bridge data are not covered; a database that would need them
  is refused (`unsupported_capability`).
- **Datums db-sync-alt cannot see.** A datum row is added when db-sync-alt reads
  a transaction that carries the preimage: the output's own transaction or a
  spend of it. cardano-db-sync also adds the row when the preimage appears only in
  an unrelated transaction while the output is unspent; db-sync-alt does not.
  The node reads such rows only for registration, governance and D-parameter
  outputs, which carry tokens only their minting policies create. Through
  Blockfrost, a datum hash that matches only some redeemer's data also gets a
  row (Blockfrost answers both kinds), and a preimage already on chain when the
  output is read is stored with it. From a cardano-db-sync source, datums are
  copied as that source stores them.
- **PostgreSQL 17 only**, server and client tools.
- **Initial data is required.** An empty database is filled from a snapshot or
  backup, or from your own db-sync; a Blockfrost key alone cannot fill it.
- **Polling is a target, not a promise.** The 20-second tip poll can be delayed
  by provider rate limits, the request ceiling and local load. A provider pause
  makes `/health` stale without losing the coverage already served; `/status`
  stays 200 while `/health` is 503.
- **Verification costs time.** Startup, backups and `db-sync-alt verify` read
  every stored fact once and the updates published since the last integrity
  checkpoint; their time grows with the amount of stored data. The periodic
  checkpoint pauses publication for that long every 6 hours by default. Backups
  need the writer lock, so the service must be stopped.
- **TLS is your PostgreSQL server's.** db-sync-alt's launcher connects over
  loopback without TLS; the node uses whatever your server offers.
- **Growing history.** Every update adds its receipts and block membership to
  the database; there is no pruning yet ([costs](costs.md#disk)).
- **What the tests cover.** The test suite uses synthetic data and disposable
  PostgreSQL; it does not run a live Blockfrost key, a real Midnight node, a
  long soak or host reboots ([development](development.md)).
