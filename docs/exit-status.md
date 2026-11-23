# Exit status

Every `db-sync-alt` command ends with one of these statuses. `db-sync-alt --help`
prints the same table.

| Status | Meaning | Examples |
|---|---|---|
| 0 | Success. | `verify` passed; `compare` matched. |
| 1 | The operation failed. | A snapshot file that does not exist; a refused restore; a query error; a stop by SIGINT, SIGTERM or `--overall-timeout-seconds`. |
| 2 | Usage error: an invalid command line or flag value, found before any connection. The error names the flag. | A missing `--database`; a password in the connection string; an unreadable `--password-file`; an unsupported `--network`. |
| 3 | A check found a difference. | `compare` reports `matched: false`; `verify` or `grant-reader --check` refuses the database. |
| 75 | Temporary failure: the database could not be reached or is restarting. The same command may succeed later. | Connection refused or timed out; the server is starting or shutting down; too many connections. |

Each command writes one JSON line to standard error when it starts
(`"event": "started"`) and one when it ends (`"event": "finished"`), with the
`outcome` (`success`, `difference`, `failed`, `temporary_failure`,
`interrupted` or `deadline_exceeded`), the exit status and the duration in
seconds. A failure is reported on its own JSON line before the end line.

On SIGINT, SIGTERM or the deadline a command cancels the statements of its own
database sessions, removes its temporary files and exits with status 1. A
command that changes the database reports `"published": false` when it was
stopped before the commit that publishes, and `"outcome": "unknown"` when that
commit had been sent; run `status` and `verify` to see which state it reached.
