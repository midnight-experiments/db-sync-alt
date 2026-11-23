The compressed corpora pair intact finalized block responses with the SQL rows
of the same blocks, exported independently from a full-history database, and
spent-output origins. Each bundle has an external
compressed SHA256, exact byte/member/count assertions, and a SHA256 per member.
The test decodes each block once and processes it with the block function,
then compares rows and available Pallas facts. Original positional IDs and asset
ordinals survive token filtering. These are selected block contexts, rather than
one consecutive public chain.

The Preprod corpus has six projection blocks and 20 transactions. Block 4318350
is support-only and is excluded from replay and shape counts; its historical
foreign-token observations remain explicit. Original shared positive datum
responses carry qualified capture provenance.

The Mainnet corpus has ten blocks and 114 transactions. It includes three real
phase-2-invalid transactions and their effective collateral rows, stored Byron
outputs, and an asset whose original pre-filter ordinal is 15. Four contextual
datum API responses retain their original timestamps and response hashes. A real
nonempty metadata response containing label 2001 is excluded from bridge-key
6500973 rows. The manifests and tests state measured counts.

These corpora did not witness a pointer output, a raw datum above 10,000 bytes,
or a positive stored bridge-key metadatum. Controlled parser/adapter tests cover
additional implementation cases without presenting them as historical witnesses.
No operator journals, credential files or backup artifacts belong in a bundle.

The pagination input contains 103 intact finalized transactions from several
actual Preprod blocks. It is used to form controlled 100+3 transport pages; it
does not claim a genuine 103-transaction block. Its separate manifest binds its
bytes and original response-file hashes, block heights and transaction indices.

Readers: `tests/corpus.rs` replays `mainnet-corpus.json.gz` and
`preprod-corpus.json.gz` with their manifests; `tests/pages.rs` reads
`pagination-finalized.json` and its manifest; `tests/supplements.rs` reads
`collateral-omission-cbor.json` and `collateral-omission-utxos.json`.
