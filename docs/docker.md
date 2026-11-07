# Docker / Compose

An alternative to `db-sync-alt run`: PostgreSQL and db-sync-alt in containers.
You need Docker with Compose 2.24.4 or newer, Python 3.11 or newer on the host
(for the setup script), the snapshot or another db-sync-alt backup with its
manifest, and a Blockfrost key of the backup's network in a private file (the
service is configured for the network named in the backup's manifest). No image is
published; build it from the repository.

## Build the image

From the repository root:

```sh
docker build -f deploy/Dockerfile -t db-sync-alt:local .
```

The base images are pinned by digest and Python packages by hash. The image runs
as an unprivileged user; its default command prints `db-sync-alt --help`.

## Prepare the private configuration

```sh
bash deploy/setup-compose.sh
```

The script asks for the absolute paths of the archive, the manifest and the key
file (not the key itself), checks that all three are readable, and creates
`~/.local/share/db-sync-alt/private` (it refuses to touch an existing one) with
generated passwords for the PostgreSQL superuser, the owner login and the reader
login:

| Path | Mounted into | Holds |
| --- | --- | --- |
| `compose.env` | nothing (read by Compose on the host) | Project name, ports, image, the superuser's password |
| `service/` | the service, read-only | `service.json`, libpq service file and passfile of the owner and reader logins, the Blockfrost key, the backup pair |
| `accounting/` | the service | The Blockfrost request ledger |
| `operator/` | the bootstrap only, read-only | The superuser's connection and the owner and reader passwords |

`compose.env` and `operator/` contain the superuser's password: never paste
rendered Compose configuration into issues or logs. The script uses
`db-sync-alt compose init`, which you can also call yourself for other paths or
ports.

## Start

```sh
env_file=~/.local/share/db-sync-alt/private/compose.env
docker compose -f deploy/compose.yaml --env-file "$env_file" up -d postgres
docker compose -f deploy/compose.yaml --env-file "$env_file" run --rm bootstrap
docker compose -f deploy/compose.yaml --env-file "$env_file" up -d service
```

`bootstrap` (`db-sync-alt compose provision`) is the operator's step for the
bundled PostgreSQL server: as its superuser it creates the owner login
`db_sync_alt_owner`, the reader login `db_sync_alt_reader` (connection limit 40,
statement and idle-in-transaction timeouts of 60 seconds, `search_path` `public`)
and the database `db_sync_alt`, owned by the owner and closed to `PUBLIC` — the
statements of [provision PostgreSQL](operations.md#provision-postgresql). It
refuses to run again. The service runs as the owner login: it restores the
backup into the empty database, verifies it, grants the reader the node's
relations and starts following Blockfrost. A database that is already ready is
resumed, never restored over.

The service publishes HTTP status on `127.0.0.1:18080`
([health](../README.md#health-and-status)). PostgreSQL publishes no port on the
host: it is on the deployment's internal `backend` network (with the service)
and its internal `node` network, which is where a Midnight node connects
([connect a Midnight node](connect-a-node.md)). Attach the node's container to
it, for example `docker network connect <project>_node <node container>` (the
project name is `COMPOSE_PROJECT_NAME` in `compose.env`), or declare it in the
node's Compose file as an external network named `<project>_node`, and connect to
host `postgres`, port 5432, database `db_sync_alt`, user `db_sync_alt_reader`,
with the password in `operator/reader.password`.

## Stop

```sh
docker compose -f deploy/compose.yaml --env-file "$env_file" stop service
```

The node keeps reading the last published update from PostgreSQL while the
service is stopped. The database volume, the private directory and the request
ledger stay. Nothing restarts automatically. For failures use
[backup and recovery](recovery.md); never delete the volume or the ledger to get
past an error.

## Upgrade

A database of an older schema needs `db-sync-alt upgrade` once, with the service
stopped ([upgrade to schema 4](recovery.md#upgrade-to-schema-4)). Build the new
image and set `APP_IMAGE` in `compose.env` to it, then:

```sh
docker compose -f deploy/compose.yaml --env-file "$env_file" stop service
docker compose -f deploy/compose.yaml --env-file "$env_file" run --rm --no-deps service \
  db-sync-alt upgrade --service db_sync_alt_owner
docker compose -f deploy/compose.yaml --env-file "$env_file" up -d service
```

`run` starts a one-off container from the service's image with its private
directory and network; it publishes no port.
