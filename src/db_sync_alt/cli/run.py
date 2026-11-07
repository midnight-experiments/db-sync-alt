"""Run db-sync-alt in the foreground against an existing local PostgreSQL 17 database.

The operator provides the database, its non-superuser owner login and the reader login
midnight-node connects with; the launcher never creates or alters a role.
"""
import argparse
import contextlib
import fcntl
import getpass
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import secrets
import stat
import sys
from urllib.parse import unquote, urlsplit

from db_sync_alt.networks import names as network_names, network_profile
from db_sync_alt.resources import service_template

DEFAULT_URL = 'postgresql://127.0.0.1:5432/db_sync_alt'
DEFAULT_OWNER = 'db_sync_alt_owner'
DEFAULT_READER = 'db_sync_alt_reader'
PROMPT = object()
STATE_FILES = ('provider.key', 'pgpass', 'pg_service.conf', 'service.json')
IDENTIFIER = r'[A-Za-z_][A-Za-z0-9_-]{0,62}'


class LaunchError(Exception):
    pass


class Parser(argparse.ArgumentParser):
    def error(self, message):
        # argparse may interpolate a credential or a credential-bearing URL.
        raise LaunchError('invalid_arguments; use --help for accepted options')


def parser():
    p = Parser(prog='db-sync-alt run', description=__doc__)
    p.add_argument('--network', required=True, choices=network_names())
    key = p.add_mutually_exclusive_group()
    key.add_argument('--blockfrost-key', nargs='?', const=PROMPT, help='Credential, or hidden prompt when no value is supplied')
    key.add_argument('--blockfrost-key-file', type=Path, help='Private credential file; alternative: DBSYNC_BLOCKFROST_KEY')
    p.add_argument('--database-url', default=DEFAULT_URL, help='One loopback TCP PostgreSQL URL; no query/TLS options (default: %(default)s)')
    p.add_argument('--database-user', help='Override URL username: the database owner login (default: ' + DEFAULT_OWNER + ')')
    password = p.add_mutually_exclusive_group()
    password.add_argument('--database-password', nargs='?', const=PROMPT, help='Override URL password, or hidden prompt')
    password.add_argument('--database-password-file', type=Path, help='Private owner-password file; alternative: DBSYNC_DATABASE_PASSWORD')
    p.add_argument('--reader-role', default=DEFAULT_READER, help='Existing login midnight-node connects with (default: %(default)s)')
    reader = p.add_mutually_exclusive_group()
    reader.add_argument('--reader-password', nargs='?', const=PROMPT, help='Reader password, or hidden prompt')
    reader.add_argument('--reader-password-file', type=Path, help='Private reader-password file; alternative: DBSYNC_READER_PASSWORD')
    p.add_argument('--state-dir', type=Path, default=Path.home()/'.local/share/db-sync-alt/native', help='Durable private state; reuse on every restart (default: %(default)s)')
    p.add_argument('--archive', type=Path, help='Accepted PostgreSQL-native backup for an empty destination')
    p.add_argument('--manifest', type=Path, help='Accepted manifest paired with --archive')
    p.add_argument('--http-port', type=int, default=18080)
    p.add_argument('--node-security-parameter', type=int, help="The Midnight node's CARDANO_SECURITY_PARAMETER, compared with the network's k (report-only)")
    p.add_argument('--node-configuration-dir', type=Path, help="The node's network configuration directory (res/<network>), compared with the profile (report-only)")
    return p


def private(path, directory=False):
    info = path.lstat()
    expected = stat.S_ISDIR if directory else stat.S_ISREG
    if (not expected(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077
            or (not directory and info.st_nlink != 1)):
        raise LaunchError('unsafe_private_state')


def secret_file(path):
    private(path)
    if path.stat().st_size > 8192:
        raise LaunchError('invalid_credential_file')
    return path.read_text().rstrip('\r\n')


def credential(value, filename, env_name, fallback=None, label='PostgreSQL owner password'):
    if value is PROMPT:
        if not sys.stdin.isatty():
            raise LaunchError('interactive_secret_requires_terminal')
        value = getpass.getpass(label+' (hidden): ')
    elif filename is not None:
        value = secret_file(filename)
    elif value is None:
        value = os.environ.get(env_name, fallback)
    if value is not None and (not isinstance(value, str) or len(value) > 8192 or any(c in value for c in '\x00\r\n')):
        raise LaunchError('invalid_credential')
    return value


def database(args):
    try:
        raw = args.database_url
        if any(ord(c) <= 32 for c in raw) or re.search(r'%(?![0-9a-fA-F]{2})', raw):
            raise ValueError()
        url = urlsplit(raw)
        host = url.hostname
        if (url.scheme not in ('postgresql', 'postgres') or not host or url.query or url.fragment
                or '?' in raw or '#' in raw or not ipaddress.ip_address(host).is_loopback):
            raise ValueError()
        port = url.port if url.port is not None else 5432
        name = unquote(url.path[1:]) if url.path.startswith('/') else ''
        user = args.database_user or (unquote(url.username) if url.username else DEFAULT_OWNER)
        # Service-file and pgpass fields cannot inject options; intentionally narrow identifiers.
        if not 1 <= port <= 65535 or not all(re.fullmatch(IDENTIFIER, x) for x in (name, user)):
            raise ValueError()
        password = credential(args.database_password, args.database_password_file,
                              'DBSYNC_DATABASE_PASSWORD', unquote(url.password) if url.password is not None else '')
        return {'host': host, 'port': port, 'dbname': name, 'user': user, 'password': password,
                'sslmode': 'disable', 'gssencmode': 'disable', 'connect_timeout': 2, 'passfile': '/dev/null'}
    except LaunchError:
        raise
    except Exception:
        raise LaunchError('unsupported_database_url; use one literal loopback TCP endpoint without query or TLS options') from None


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def file_digest(path):
    value = hashlib.sha256()
    with path.open('rb') as inp:
        while chunk := inp.read(1024*1024):
            value.update(chunk)
    return value.hexdigest()


def durable(path, value):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'w') as out:
        out.write(value)
        out.flush()
        os.fsync(out.fileno())
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


@contextlib.contextmanager
def controlled_environment():
    # libpq environment is not a second, hidden source of connection identity.
    original = dict(os.environ)
    try:
        for name in list(os.environ):
            if name.startswith('PG'):
                del os.environ[name]
        yield
    finally:
        os.environ.clear()
        os.environ.update(original)


def accepted_backup(args):
    archive, manifest = args.archive, args.manifest
    if bool(archive) != bool(manifest):
        raise LaunchError('backup_pair_required')
    if archive is None:
        if not sys.stdin.isatty():
            raise LaunchError('empty_database_requires_archive_and_manifest')
        archive = Path(input('Accepted backup archive absolute path: '))
        manifest = Path(input('Accepted manifest absolute path: '))
    from db_sync_alt.backup import FORMATS, SERVING_FORMATS, file_hash, safe_file
    from db_sync_alt.store.database import OWNER, POSTGRES_MAJOR
    from db_sync_alt.networks import for_identity
    archive, manifest = safe_file(archive, existing=True), safe_file(manifest, existing=True)
    body = json.loads(manifest.read_text())
    network = for_identity(body.get('profile', {}))
    if (body.get('format') not in SERVING_FORMATS or body.get('postgres_major') != POSTGRES_MAJOR
            or body.get('schema_version') not in FORMATS[body['format']] or body.get('owner') != OWNER
            or body.get('archive') != file_hash(archive) or network is None or network.network != args.network):
        raise LaunchError('invalid_accepted_backup')
    return archive, manifest


def db_identity(db):
    row = db.execute('SELECT d.oid,d.datdba,r.oid FROM pg_database d,pg_roles r WHERE d.datname=current_database() AND r.rolname=current_user').fetchone()
    from db_sync_alt.sources.blockfrost.poller import identity
    cluster, database_oid = identity(db)
    return {'cluster_system_identifier': cluster, 'database_oid': database_oid,
            'database_owner_oid': row[1], 'owner_login_oid': row[2]}


def reader(args, params):
    """The operator's reader login: its name and password (the service connects as it to check serving)."""
    role = args.reader_role
    if not re.fullmatch(IDENTIFIER, role) or role == params['user']:
        raise LaunchError('invalid_reader_role')
    password = credential(args.reader_password, args.reader_password_file, 'DBSYNC_READER_PASSWORD', '', label='PostgreSQL reader password')
    return role, password


def node_values(args):
    """The node's values the service compares with the profile; None when none is given."""
    out = {}
    if args.node_security_parameter is not None:
        out['security_parameter'] = args.node_security_parameter
    if args.node_configuration_dir is not None:
        out['configuration_dir'] = str(args.node_configuration_dir.absolute())
    return out or None


def role_identity(db, role):
    """The reader is a plain login the operator created: never a superuser, owner or privileged role."""
    row = db.execute('SELECT oid,rolcanlogin,rolsuper,rolcreatedb,rolcreaterole,rolreplication,rolbypassrls FROM pg_roles WHERE rolname=%s', (role,)).fetchone()
    if row is None:
        raise LaunchError('reader_role_missing; create the reader login first (see docs/operations.md)')
    if row[1] is not True or any(row[2:]):
        raise LaunchError('reader_role_identity_mismatch')
    return row[0]


def configure(args, params, key, state, db, existing, reader_login=(DEFAULT_READER, '')):
    from db_sync_alt.store.database import empty_destination, PgStore, SCHEMA_VERSION
    from db_sync_alt.backup import check_tool
    from psycopg.conninfo import make_conninfo
    network = network_profile(args.network)
    reader_role, reader_password = reader_login
    identity = dict(db_identity(db), network=args.network, host=params['host'], port=params['port'],
                    database=params['dbname'], user=params['user'], owner_password_sha256=digest(params['password']),
                    reader=reader_role, reader_password_sha256=digest(reader_password),
                    key_sha256=digest(key), http_port=args.http_port, node=node_values(args),
                    provider_adapter=os.environ.get('DBSYNC_PROVIDER_ADAPTER', 'portable'),
                    shared_root=os.environ.get('DBSYNC_SHARED_ROOT', ''), shared_profile=os.environ.get('DBSYNC_SHARED_PROFILE', args.network))
    if identity['provider_adapter'] not in ('portable', 'shared'):
        raise LaunchError('unsupported_provider_adapter')
    if identity['provider_adapter'] == 'shared' and (not Path(identity['shared_root']).is_absolute() or identity['shared_profile'] != args.network):
        raise LaunchError('shared_provider_configuration_required')
    if identity['provider_adapter'] == 'shared':
        from db_sync_alt.sources.blockfrost.provider import SharedClient
        shared = SharedClient(identity['shared_root'], identity['shared_profile'], network=network)
        if not secrets.compare_digest(shared.client.key, key):
            raise LaunchError('shared_provider_credential_mismatch')
    empty = empty_destination(db)
    if not empty:
        with PgStore(conninfo=make_conninfo(**params), readonly=True) as store:
            if (store.state()['schema_version'] != SCHEMA_VERSION or store.lifecycle_state()['revoked']
                    or store.meta('profile').get('network') != args.network):
                raise LaunchError('database_not_ready_for_native_launch')
    if empty:
        check_tool('pg_restore', db)
    if existing:
        private(state/'receipt.json')
        receipt = json.loads((state/'receipt.json').read_text())
        if receipt.get('version') != 2 or receipt.get('identity') != identity:
            raise LaunchError('launch_identity_mismatch')
        if (not isinstance(receipt.get('files'), dict) or set(receipt['files']) != set(STATE_FILES)
                or type(receipt.get('reader_role_oid')) is not int or receipt['reader_role_oid'] <= 0):
            raise LaunchError('invalid_state_receipt')
        for name, expected in receipt['files'].items():
            if not isinstance(expected, str) or not re.fullmatch('[0-9a-f]{64}', expected):
                raise LaunchError('invalid_state_receipt')
            private(state/name)
            if file_digest(state/name) != expected:
                raise LaunchError('private_state_changed')
        if role_identity(db, reader_role) != receipt['reader_role_oid']:
            raise LaunchError('reader_role_identity_mismatch')
        if identity['provider_adapter'] == 'portable':
            private(state/'accounting', True)
            for name in ('usage.json', '.initialized'):
                private(state/'accounting'/name)
        if args.archive or args.manifest:
            # Explicit recovery inputs cannot silently replace the accepted pair.
            cfg = json.loads((state/'service.json').read_text())
            if (not args.archive or not args.manifest or str(args.archive.absolute()) != cfg['backup_archive']
                    or str(args.manifest.absolute()) != cfg['backup_manifest']):
                raise LaunchError('backup_pair_changed')
        return
    archive, manifest = accepted_backup(args) if empty or args.archive or args.manifest else (None, None)
    role_oid = role_identity(db, reader_role)
    # Durable marker precedes the FIRST provider/config mutation.
    # Only a fresh directory containing its lock alone can be removed on preflight failure.
    durable(state/'.initializing', 'native-launcher-v2\n')
    durable(state/'provider.key', key+'\n')
    esc = lambda x: str(x).replace('\\', '\\\\').replace(':', '\\:')
    logins = (('db_sync_alt_owner', params['user'], params['password']), ('db_sync_alt_reader', reader_role, reader_password))
    durable(state/'pgpass', ''.join(':'.join(esc(x) for x in (params['host'], params['port'], params['dbname'], user, password))+'\n'
                                    for _, user, password in logins))
    durable(state/'pg_service.conf', ''.join('['+name+']\n'+''.join(f'{k}={dict(params, user=user)[k]}\n' for k in ('host','port','dbname','user','sslmode','gssencmode','connect_timeout'))
                                             for name, user, _ in logins))
    cfg = json.loads(service_template().read_text())
    cfg.update(network=network.network, profile=network.profile, http_port=args.http_port, backup_archive=str(archive) if archive else None,
               backup_manifest=str(manifest) if manifest else None, storage_reserves={str(state):10*1024**3},
               reader={'role': reader_role, 'pg_service': 'db_sync_alt_reader'}, node=node_values(args))
    durable(state/'service.json', json.dumps(cfg, indent=2)+'\n')
    if identity['provider_adapter'] == 'portable':
        from db_sync_alt.sources.blockfrost.provider import PortableClient
        PortableClient(key, state/'accounting', network=network).status()  # Offline validation/initialization only.
    names = STATE_FILES
    durable(state/'receipt.json', json.dumps({'version':2,'identity':identity,
            'reader_role_oid':role_oid,'files':{name:file_digest(state/name) for name in names}}, indent=2)+'\n')


def launch(args):
    params = database(args)
    reader_login = reader(args, params)
    network = network_profile(args.network)
    key = credential(args.blockfrost_key, args.blockfrost_key_file, 'DBSYNC_BLOCKFROST_KEY',
                     label='Blockfrost ' + network.title + ' API key')
    # A Blockfrost project key names its network in its prefix.
    if not network.valid_key(key):
        raise LaunchError(network.network + '_credential_required')
    if not 10000 <= args.http_port <= 65535 or args.http_port == params['port']:
        raise LaunchError('distinct_valid_ports_required')
    if bool(args.archive) != bool(args.manifest):
        raise LaunchError('backup_pair_required')
    state = args.state_dir.absolute()
    if any(p.is_symlink() for p in (state,*state.parents)) or any(c in str(state) for c in '\x00\r\n'):
        raise LaunchError('unsafe_state_path')
    existing = state.exists()
    if existing:
        private(state, True)
        if not (state/'receipt.json').is_file():
            raise LaunchError('incomplete_private_state; inspect and recover explicitly')
    else:
        state.mkdir(mode=0o700, parents=True)
    fd = os.open(state/'.launcher.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd,'r+') as lock, controlled_environment():
        private(state/'.launcher.lock')
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise LaunchError('launcher_already_running') from None
        try:
            import psycopg
            from db_sync_alt.store.database import POSTGRES_MAJOR, WRITER_LOCK
            with psycopg.connect(**params, autocommit=True) as db:
                if db.info.server_version//10000 != POSTGRES_MAJOR:
                    raise LaunchError('postgresql_17_required')
                if not db.execute('SELECT pg_try_advisory_lock(%s,%s)', WRITER_LOCK).fetchone()[0]:
                    raise LaunchError('database_writer_busy')
                configure(args, params, key, state, db, existing, reader_login)
            os.environ.update(PGSERVICEFILE=str(state/'pg_service.conf'), PGPASSFILE=str(state/'pgpass'),
                              DBSYNC_PROVIDER_KEY_FILE=str(state/'provider.key'), DBSYNC_PROVIDER_STATE=str(state/'accounting'))
            # Explicit shared-adapter selection/root remain intact. Never replace its ledger.
            from db_sync_alt.service.runtime import main as service_main
            print('Starting foreground service; HTTP status on 127.0.0.1:'+str(args.http_port)+'. midnight-node reads PostgreSQL '
                  +params['host']+':'+str(params['port'])+'/'+params['dbname']+' as '+reader_login[0]+'.', flush=True)
            return service_main(['--config',str(state/'service.json')])
        except BaseException:
            if not existing and set(p.name for p in state.iterdir()) == {'.launcher.lock'}:
                # Still holding this new state's lock; no mutation marker exists.
                # Another launcher refuses a receipt-less directory, so cannot adopt it.
                if (state/'.launcher.lock').lstat().st_ino == os.fstat(lock.fileno()).st_ino:
                    (state/'.launcher.lock').unlink()
                    state.rmdir()
            raise


def main(argv=None):
    try:
        return launch(parser().parse_args(argv))
    except LaunchError as error:
        print('Launcher refused: '+str(error), file=sys.stderr)
        return 2
    except (KeyboardInterrupt, EOFError):
        print('Launcher cancelled.', file=sys.stderr)
        return 130
    except Exception:
        # Driver, paths, URL parsing and OS errors may contain credentials.
        print('Launcher failed; check private state, accepted backup, database access and PostgreSQL 17 tools.', file=sys.stderr)
        return 2

