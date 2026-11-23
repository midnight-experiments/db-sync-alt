# Security policy

## Reporting a vulnerability

Report vulnerabilities privately through GitHub:
[report a vulnerability](https://github.com/midnight-experiments/db-sync-alt/security/advisories/new)
(the repository's *Security* tab, *Report a vulnerability*). Do not open a
public issue. The maintainer, [@acedward](https://github.com/acedward), handles
reports.

Please include the affected version or commit, what an attacker can do, and a
minimal reproduction with synthetic data. Never attach Blockfrost keys,
passwords, private configuration or database dumps.

## Supported versions

| Version | Supported |
| --- | --- |
| 0.5.x | yes |

db-sync-alt is experimental; fixes go into the latest version.

## Running it safely

- The secrets are the Blockfrost key file and the PostgreSQL password files.
  Keep them out of source control, readable by the service's user only. They
  are only ever given as file paths, never as flag values.
- Give PostgreSQL TLS, and keep it on a private network. db-sync-alt's database
  accepts no `PUBLIC` connections.
- The reader login is for Midnight nodes only and can read only the node's
  views; never give a node, or anyone else, the owner login db-sync-alt runs
  as. db-sync-alt refuses a reader with more rights.
- `--listen` defaults to `0.0.0.0:18080`. `/health` and `/status` carry no
  secret, but publish them only where you want them read; the Compose
  deployment publishes them on `127.0.0.1` only.
- Snapshot files hold public chain data only. Restore a file only with the
  content digest from where it was published (`--expect-content-digest`).
- Reports, logs and `/status` carry stable codes, never a key, a password, a
  connection string or a provider URL; keep it that way in changes.
