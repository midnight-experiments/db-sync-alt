# Entry point of the bundled PostgreSQL server, run by /bin/sh as root before
# the image's own entry point. PostgreSQL refuses a TLS key that another user
# can read, and the server's user cannot read the operator's secret files, so
# this copies them into a private directory owned by that user.
set -eu
umask 077
chown postgres:postgres /run/db-sync-alt
chmod 0700 /run/db-sync-alt
for name in postgres.password owner.password reader.password server.crt server.key; do
  install -m 0600 -o postgres -g postgres "/run/secrets/$name" "/run/db-sync-alt/$name"
done
exec docker-entrypoint.sh "$@"
