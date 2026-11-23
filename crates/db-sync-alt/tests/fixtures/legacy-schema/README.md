The database schema of db-sync-alt 0.4.x, the source that `convert` reads.
The files are byte-identical copies of `src/db_sync_alt/store/` at tag
`v0.4.9`. The tests apply them in this order to build a schema-5 source:
`schema.sql`, `schema_v2.sql`, `schema_v3.sql`, `schema_v3_after_load.sql`,
`schema_v4.sql`, `schema_v5.sql`. Do not edit them: they stand for databases
that already exist.
