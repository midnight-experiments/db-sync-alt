# Deployment

## The image

`ghcr.io/midnight-experiments/db-sync-alt:<version>` holds the release binary
on a distroless Debian 12 base (the C library and its runtime libraries, CA
certificates and time-zone data), without a shell, a package manager or
PostgreSQL client tools. It runs as user 65532. Its
entry point is `/usr/local/bin/db-sync-alt` and its default command is `run`;
`/var/lib/db-sync-alt/snapshots` belongs to user 65532 for the daily snapshot
files. The license texts are in `/usr/share/doc/db-sync-alt`. The image is
about 33 MB and its binary about 9 MB. `Dockerfile` builds it from the
repository root:

```sh
docker build -f deploy/Dockerfile -t db-sync-alt:local .
```

The service is configured through `DSA_` variables
([configuration](../docs/configuration.md)); secrets are files mounted into the
container. Any command runs the same way, for example
`docker run --rm ghcr.io/midnight-experiments/db-sync-alt:0.5.1 verify …`.
`db-sync-alt health` is the health check: it needs no HTTP client in the image.

## Compose

`compose.yaml` runs db-sync-alt next to its own server, PostgreSQL 17
(db-sync-alt runs on [PostgreSQL 17 or newer](../docs/postgresql.md)):

| Service | What it does |
| --- | --- |
| `postgres` | PostgreSQL 17 over TLS. On its first start it creates the owner login `db_sync_alt_owner`, the node's reader login `db_sync_alt_reader` and the database `db_sync_alt` (`postgres/init.sh`). Nothing is published on the host; the node connects on the `node` network. |
| `db-sync-alt` | The service (`db-sync-alt run`) as the owner login, configured through `DSA_` variables. It restarts after an exit, has a read-only root file system and no capabilities, and answers `/health` and `/status` on port 18080, published on `127.0.0.1` only. Its health check is `db-sync-alt health`. |

Secrets are files, never environment values: Compose mounts them into the
containers under `/run/secrets`. Both containers restart with the Docker daemon
(`restart: unless-stopped`); the database lives in the volume `database`.

Requirements: Docker Engine with Compose 2.24.4 or newer, `openssl`, and a
Blockfrost key file for the network.

### 1. Directories and secrets

Start in the root of this repository, and pick a directory you own, one per
network (`/srv/db-sync-alt/mainnet` here):

```sh
repo=$PWD
dir=/srv/db-sync-alt/mainnet
mkdir -p "$dir/snapshots"
install -d -m 700 "$dir/secrets"
cd "$dir/secrets"
umask 077
for name in postgres owner reader; do openssl rand -hex 24 > "$name.password"; done
cp <your Blockfrost key file> provider.key
```

`postgres.password` is the server's superuser, `owner.password` the login
db-sync-alt runs as, `reader.password` the Midnight node's login.

### 2. TLS certificates

Midnight node 2.1.101 connects to PostgreSQL over TLS only. Create a
certificate authority and a server certificate for the host name `postgres`,
the name both the node and db-sync-alt use on the Compose networks:

```sh
cd "$dir/secrets"
openssl req -x509 -newkey rsa:3072 -nodes -days 3650 -subj '/CN=db-sync-alt PostgreSQL CA' \
  -keyout ../root-ca.key -out root-ca.crt
openssl req -newkey rsa:3072 -nodes -subj '/CN=postgres' -keyout server.key -out server.csr
printf 'subjectAltName=DNS:postgres\nextendedKeyUsage=serverAuth\n' > server.ext
openssl x509 -req -in server.csr -CA root-ca.crt -CAkey ../root-ca.key -CAcreateserial \
  -days 825 -extfile server.ext -out server.crt
```

The containers need `root-ca.crt`, `server.crt` and `server.key`; keep
`root-ca.key` out of the secrets directory, and off the host if you can.
`postgres/start.sh` copies the server's key into a private directory owned by
the server's user at every start, so the files can stay owned by you.

`DSA_TLS_ROOT_CERT` (the flag `--tls-root-cert`) names the certificate
authority that db-sync-alt checks the server against: Compose sets it to
`/run/secrets/root-ca.crt`. With it, db-sync-alt connects only over TLS and
checks that the certificate is for the host name in the connection string. Give
the node the same file as `SSL_ROOT_CERT`.

The server certificate is valid for 825 days. Once it has expired, every new
connection of db-sync-alt and of a node that checks it fails its TLS check:
db-sync-alt ends with exit status 1 at its next connection
(`tls_certificate_invalid`). Renew it before then: replace `server.crt` and
`server.key` and restart the server, `docker compose … restart postgres`.

### 3. Settings

```sh
cd "$repo"
cp deploy/compose.env.example deploy/compose.env
```

Set `DSA_NETWORK`, `DSA_UID` and `DSA_GID` (the owner of the two directories,
`id -u` and `id -g`), `DSA_SECRETS_DIR` and `DSA_SNAPSHOT_HOST_DIR`. The other
`DSA_` lines are optional and passed on only when set. Leave a setting out, or
comment it out: a line such as `DSA_SNAPSHOT=` with nothing after it sets an
empty value, which the service refuses. Check the result without starting
anything:

```sh
docker compose -f deploy/compose.yaml --env-file deploy/compose.env config --quiet
```

### 4. First start

The Compose deployment starts from a snapshot file: on an empty database the
service restores it. Put the file in
`DSA_SNAPSHOT_HOST_DIR`, and set in `compose.env` its path as the service sees
it and its published content digest:

```sh
DSA_SNAPSHOT=/var/lib/db-sync-alt/snapshots/<file>
DSA_EXPECT_CONTENT_DIGEST=<content digest>
```

Then start both services and follow the service's progress:

```sh
docker compose -f deploy/compose.yaml --env-file deploy/compose.env up -d
curl -s http://127.0.0.1:18080/status
```

The two lines may stay in `compose.env`: the file is read only when a restore
is needed, and a start on a serving database does not need it. The snapshot
directory is also the directory of the daily files, and every file of the
network's daily name pattern in it belongs to the rotation, the release file
included: it is removed once `DSA_SNAPSHOT_KEEP` (default 3) newer daily files
exist. Keep a copy elsewhere if you want to keep it.

Without a snapshot file, `rebuild` is run with the binary or the image against
the deployment's PostgreSQL, before the service starts
([rebuild](../docs/rebuild.md)). The server publishes no port on the host, and
the build needs the owner login, its password file, `root-ca.crt` and the
provider key. Until the build has published, the service does not run on the
database and Compose restarts it: it refuses the database
(`snapshot_required` while it is empty, `database_not_serving` once the build
has begun), and while the build runs, which holds the writer lock, it waits
120 seconds for the lock and ends with `writer_busy`.

### 5. Connect a Midnight node

Attach the node's container to the network `<project>_node`, where `<project>`
is `COMPOSE_PROJECT_NAME`: `docker network connect <project>_node <node
container>`, or declare it in the node's Compose file as an external network.
The node connects to host `postgres`, port 5432, database `db_sync_alt`, user
`db_sync_alt_reader`, with the password in `reader.password`, and with
`SSL_ROOT_CERT` set to `root-ca.crt` as mounted in the node's container
([connect a node](../docs/connect-a-node.md)).

### PostgreSQL settings

`compose.yaml` passes these settings to the server. The database is small: it
measured 194 MB on Mainnet right after a restore and 376 MB while holding 25
days of blocks before its first pruning
([PostgreSQL](../docs/postgresql.md#size-and-connections)). On Preprod, two
minutes after a first start, the service container used 4.8 MiB of memory and
the PostgreSQL container 67 MiB (2026-11-23).

| Setting | Value | Why |
| --- | --- | --- |
| `ssl`, `ssl_cert_file`, `ssl_key_file` | `on`, the mounted certificate and key | The node requires TLS. |
| `ssl_min_protocol_version` | `TLSv1.2` | No older protocol. |
| `password_encryption` | `scram-sha-256` | Password hashes of the three logins. |
| `shared_buffers` | `128MB` | The working set of a small database. |
| `effective_cache_size` | `256MB` | Planner estimate of the file cache. |
| `work_mem` | `4MB` | Enough for the keep rule's sorts and hashes. |
| `maintenance_work_mem` | `64MB` | Index builds after a restore and index rebuilds. |
| `max_connections` | `60` | Up to 32 from the node's pools within the reader's limit of 40, a few for db-sync-alt, the superuser's reserve. |

The reader login has a connection limit of 40, statement and
idle-in-transaction timeouts of 60 seconds, `search_path` `public` and
read-only transactions. db-sync-alt grants it the node's views at every start,
and refuses a reader that can do more.

### Stop and back up

`docker compose -f deploy/compose.yaml --env-file deploy/compose.env stop db-sync-alt`
stops only the service; PostgreSQL keeps serving the node the last update.
`down` stops both and keeps the database volume and the snapshot files.

The daily snapshot files in `DSA_SNAPSHOT_HOST_DIR` are the database's backups:
copy them off the host. Every file of the network's daily name pattern in that
directory belongs to the rotation, a release file put there for the first
start included: the newest `DSA_SNAPSHOT_KEEP` stay, older ones are removed.
The first-start lines `DSA_SNAPSHOT` and `DSA_EXPECT_CONTENT_DIGEST` may stay
set. To restore one, see [a closed gate under Compose](#a-closed-gate-under-compose).

### A closed gate under Compose

When a check closes the gate ([a closed gate](../docs/operations.md#a-closed-gate)),
the service stops and refuses to start on that database. Start over from a
snapshot file in a new database; the logins, their passwords and the node's
connection stay as they are:

1. Stop both services: `docker compose -f deploy/compose.yaml --env-file deploy/compose.env down`.
2. Remove the database volume, which deletes the closed database (the snapshot
   files and the secrets stay): `docker volume rm <project>_database`, where
   `<project>` is `COMPOSE_PROJECT_NAME`. Keep a copy of the volume first if
   you want to diagnose the fault.
3. Put the newest daily file, or a published one, in `DSA_SNAPSHOT_HOST_DIR`,
   and set the first-start lines in `compose.env`: `DSA_SNAPSHOT` to its path
   as the service sees it and `DSA_EXPECT_CONTENT_DIGEST` to its content
   digest (for a daily file, `integrity.snapshot.content_digest` of the last
   `/status`; leave the line out if you do not have it).
4. Start again: `docker compose -f deploy/compose.yaml --env-file deploy/compose.env up -d`.
   The server creates the logins and an empty database from the same secrets,
   and the service restores the file and catches up from its target.
