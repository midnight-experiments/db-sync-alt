# Security policy

## Reporting a vulnerability

Report vulnerabilities privately through GitHub:
[report a vulnerability](https://github.com/midnight-experiments/db-sync-alt/security/advisories/new)
(the repository's *Security* tab, *Report a vulnerability*). Do not open a
public issue. The maintainer, [@acedward](https://github.com/acedward), handles
reports.

Please include the affected version or commit, what an attacker can do, and a
minimal reproduction with synthetic data. Never attach Blockfrost keys,
passwords, private configuration, real database dumps or full provider
responses.

## Supported versions

| Version | Supported |
| --- | --- |
| 0.1.x | yes |

db-sync-alt is experimental; fixes go into the latest version.

## Running it safely

- Keep the Blockfrost key, the PostgreSQL passwords, the state directory and
  backups out of source control, with owner-only permissions.
- Keep PostgreSQL on loopback or a private network, or give it TLS; db-sync-alt's
  database accepts no `PUBLIC` connections.
- The reader login is for Midnight nodes only and can read only the node's
  relations; never give a node, or anyone else, the owner login db-sync-alt runs
  as. db-sync-alt refuses a reader with more privileges.
- Status endpoints and errors carry stable codes only, never secrets, URLs or
  raw exception text; keep it that way in changes.
