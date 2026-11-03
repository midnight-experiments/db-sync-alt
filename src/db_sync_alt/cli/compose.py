"""Create private Compose configuration, or provision the bundled PostgreSQL as its operator.

`init` never starts services, downloads data, or contacts a provider. `provision` is the operator's
step for the PostgreSQL server bundled in the Compose file: as that server's superuser it creates
the database owner login, the reader login midnight-node connects with (with its defaults) and the
dedicated database. db-sync-alt itself runs as the owner and never creates or alters a role.

Private directory layout: `compose.env` (read by Compose on the host), `service/` (mounted read-only
into the service: configuration, libpq service file and passfile of the owner and the reader,
provider key, recovery backup), `accounting/` (the provider request ledger), `operator/` (mounted
only into the bootstrap container: the superuser's connection and the two login passwords).
"""
import argparse
import json
import os
from pathlib import Path
import secrets
import shutil

from db_sync_alt.resources import service_template

DATABASE = 'db_sync_alt'
OWNER = 'db_sync_alt_owner'
READER = 'db_sync_alt_reader'
READER_SETTINGS = {'statement_timeout': '60s', 'idle_in_transaction_session_timeout': '60s', 'search_path': 'public'}
READER_CONNECTION_LIMIT = 40


def write(path, value):
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'w') as out:out.write(value)


def initialize(args):
    private=Path(args.private).absolute()
    if private.exists() or any(p.is_symlink() for p in private.parents):raise ValueError('fresh_private_directory_required')
    if any(c in str(private) for c in ('\n','\r','$','"',"'")):raise ValueError('unsupported_configuration_path')
    if not 10000<=args.http_port<=65535:raise ValueError('invalid_ports')
    archive,manifest=Path(args.archive).resolve(),Path(args.manifest).resolve()
    if not archive.is_file() or not manifest.is_file():raise ValueError('backup_pair_required')
    # The supplied backup names its network; the service is configured for that network.
    from db_sync_alt.networks import for_identity
    network=for_identity(json.loads(manifest.read_text()).get('profile'))
    if network is None:raise ValueError('backup_profile_mismatch')
    private.mkdir(mode=0o700,parents=True)
    for name in ('service','service/recovery','accounting','operator'):(private/name).mkdir(mode=0o700)
    service,operator=private/'service',private/'operator'
    # Preserve the supplied archive read-only in the operator's private area.
    shutil.copyfile(archive,service/'recovery/state.dump');shutil.copyfile(manifest,service/'recovery/manifest.json')
    for p in (service/'recovery').iterdir():p.chmod(0o600)
    passwords={name:secrets.token_hex(32) for name in ('superuser','owner','reader')}
    write(operator/'owner.password',passwords['owner']+'\n');write(operator/'reader.password',passwords['reader']+'\n')
    write(operator/'pgpass','postgres:5432:postgres:postgres:'+passwords['superuser']+'\n')
    write(operator/'pg_service.conf','[db_sync_alt_operator]\nhost=postgres\nport=5432\ndbname=postgres\nuser=postgres\nconnect_timeout=5\n')
    write(service/'pgpass',''.join('postgres:5432:'+DATABASE+':'+user+':'+passwords[name]+'\n' for name,user in (('owner',OWNER),('reader',READER))))
    write(service/'pg_service.conf',''.join('['+user+']\nhost=postgres\nport=5432\ndbname='+DATABASE+'\nuser='+user+'\nconnect_timeout=5\n' for user in (OWNER,READER)))
    cfg=json.loads(service_template().read_text())
    cfg.update(network=network.network,profile=network.profile,http_host='0.0.0.0',http_port=18080,http_allow_external=True,
               backup_archive='/run/db-sync-alt/recovery/state.dump',backup_manifest='/run/db-sync-alt/recovery/manifest.json',
               reader={'role':READER,'pg_service':READER},pg_service=OWNER)
    cfg['storage_reserves']={'/var/lib/db-sync-alt':10*1024**3}
    write(service/'service.json',json.dumps(cfg,indent=2)+'\n')
    values={'COMPOSE_PROJECT_NAME':'db-sync-alt-'+secrets.token_hex(4),'PRIVATE_DIR':str(private),'APP_UID':str(os.getuid()),'APP_GID':str(os.getgid()),
            'HTTP_PORT':str(args.http_port),'POSTGRES_PASSWORD':passwords['superuser'],'APP_IMAGE':'db-sync-alt:local'}
    write(private/'compose.env',''.join(k+'='+"'"+v+"'"+'\n' for k,v in values.items()))
    print('Private configuration created. Install a Blockfrost '+network.title+' key as service/provider.key (mode 600) before service startup.')


def provisioning_statements(owner_password,reader_password):
    """The operator's SQL, in order: two logins (no privileged attribute), the reader's defaults and
    the dedicated database owned by the owner login, with no access for PUBLIC."""
    from psycopg import sql
    owner,reader,database=sql.Identifier(OWNER),sql.Identifier(READER),sql.Identifier(DATABASE)
    out=[sql.SQL('CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD {}').format(owner,sql.Literal(owner_password)),
         sql.SQL('CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT CONNECTION LIMIT {} PASSWORD {}').format(
             reader,sql.Literal(READER_CONNECTION_LIMIT),sql.Literal(reader_password))]
    out+=[sql.SQL('ALTER ROLE {} SET {} = {}').format(reader,sql.Identifier(name),sql.Literal(value)) for name,value in READER_SETTINGS.items()]
    out+=[sql.SQL('CREATE DATABASE {} OWNER {} ALLOW_CONNECTIONS false').format(database,owner),
          sql.SQL('REVOKE ALL ON DATABASE {} FROM PUBLIC').format(database),
          sql.SQL('ALTER DATABASE {} ALLOW_CONNECTIONS true').format(database)]
    return out


def provision():
    import psycopg
    operator=Path('/run/db-sync-alt-operator')
    owner_password,reader_password=((operator/name).read_text().strip() for name in ('owner.password','reader.password'))
    with psycopg.connect(service='db_sync_alt_operator',autocommit=True) as db:
        if db.execute('SELECT 1 FROM pg_roles WHERE rolname IN (%s,%s) UNION ALL SELECT 1 FROM pg_database WHERE datname=%s',(OWNER,READER,DATABASE)).fetchone():
            raise ValueError('already_provisioned')
        for statement in provisioning_statements(owner_password,reader_password):db.execute(statement)
    print('Database '+DATABASE+', owner login '+OWNER+' and reader login '+READER+' created.')


def main(argv=None):
    p=argparse.ArgumentParser(prog='db-sync-alt compose',description=__doc__,formatter_class=argparse.RawDescriptionHelpFormatter);sub=p.add_subparsers(dest='command',required=True)
    init=sub.add_parser('init');init.add_argument('--private',required=True);init.add_argument('--archive',required=True);init.add_argument('--manifest',required=True);init.add_argument('--http-port',type=int,default=18080)
    sub.add_parser('provision');a=p.parse_args(argv)
    try:initialize(a) if a.command=='init' else provision()
    except Exception:p.exit(2,'Bootstrap failed; check private paths, permissions, supplied backup and database state.\n')
