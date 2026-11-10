# SQL acquisition version 1 controls

`sql_acquire_v1.py` and `sql_completion_v1.py` are byte-identical version-1 source controls from this Apache-2.0 db-sync-alt repository, tree `58dffa97d052973be34ede988db1a8674578596f`. They are test-only baseline and old-reader controls, never imported by production code.

SHA256:

- Acquisition: `95385711f0b82f44ee2aa493375cf7c5f6de947f26880a27ac6f67eb137265f8`.
- Completion: `76a45636ad34816ffbd4ad0aea54e3a32bdd91b9a0c7647d33d8b895e76042fa`.

Separate version-1 controls prevent the implementation under test from serving as its own baseline or old reader.
