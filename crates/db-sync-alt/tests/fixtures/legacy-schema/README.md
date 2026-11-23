The full-history source layout that `convert` reads, as SQL. Applied in this
order, the files build an empty source database: `schema.sql`, `schema_v2.sql`,
`schema_v3.sql`, `schema_v3_after_load.sql`, `schema_v4.sql`, `schema_v5.sql`.

- `tests/common/legacy.rs` builds from them the source databases of the
  conversion, comparison, follower, service and command-line tests.
- `tests/database.rs` checks that the served tables of a db-sync-alt database
  have the columns and indexes of this layout.

Do not edit them: `convert` reads databases of exactly this layout.
