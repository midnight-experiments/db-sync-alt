"""Verified PostgreSQL-native backups and create-only recovery."""
import hashlib
import json
import os
import re
from pathlib import Path
import stat
import subprocess
import uuid

from db_sync_alt.contract import Refusal, canonical
from db_sync_alt.store.database import PgStore, OWNER, POSTGRES_MAJOR, RESTORE_GUARD, SCHEMA_VERSION, SUPPORTED_SCHEMA_VERSIONS, WRITER_LOCK, SERVING_LOCK, apply_privileges, connect, empty_destination
from db_sync_alt.verify.store import verify_store, physical_inventory

BACKUP_FORMAT='db-sync-alt-postgresql-backup-v2'
# Backups of schema 1 and 2 (no served tables) are still restored, then upgraded.
FORMATS={'db-sync-alt-postgresql-backup-v1':(1,2),BACKUP_FORMAT:(3,)}


def safe_file(path,*,existing):
    path=Path(path).absolute()
    for p in (path,*path.parents):
        if p.is_symlink():raise Refusal('unsafe_backup_path')
    if path.exists():
        st=path.stat()
        if not existing or not stat.S_ISREG(st.st_mode) or st.st_nlink!=1:raise Refusal('unsafe_or_existing_backup')
    elif existing:raise Refusal('backup_missing')
    return path


def file_hash(path):
    hasher=hashlib.sha256();count=0
    with open(path,'rb') as handle:
        while chunk:=handle.read(1024*1024):hasher.update(chunk);count+=len(chunk)
    return {'sha256':hasher.hexdigest(),'bytes':count}


def cli_environment(db):
    env=dict(os.environ)
    # Secrets travel only in child environment, never argv or a status/error.
    for k,v in {'PGHOST':db.info.host,'PGPORT':str(db.info.port),'PGUSER':db.info.user,'PGDATABASE':db.info.dbname,'PGPASSWORD':db.pgconn.password.decode(),'PGCONNECT_TIMEOUT':'2'}.items():env[k]=v
    for name in ('PGSERVICE','PGSERVICEFILE','PGOPTIONS'):env.pop(name,None)
    resolved=db.info.get_parameters()
    for key in ('hostaddr','sslmode','sslrootcert','sslcert','sslkey','sslcrl','sslcrldir','sslsni','sslcompression','ssl_min_protocol_version','ssl_max_protocol_version','channel_binding','gssencmode','krbsrvname','gsslib','passfile','target_session_attrs'):
        if key in resolved:env['PG'+key.upper().replace('_','')]=resolved[key]
    return env


def check_tool(executable,db):
    try:result=subprocess.run([executable,'--version'],capture_output=True,text=True,timeout=2,check=False)
    except (OSError,subprocess.TimeoutExpired):raise Refusal('postgres_backup_tool_unavailable') from None
    found=re.search(r'\(PostgreSQL\)\s+(\d+)',result.stdout)
    major=db.info.server_version//10000
    if result.returncode or not found or major!=POSTGRES_MAJOR or int(found[1])!=major:raise Refusal('postgres_backup_tool_version_mismatch')
    return major


def run_cli(argv,db,timeout=600):
    try:result=subprocess.run(argv,env=cli_environment(db),stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,timeout=timeout,check=False)
    except (OSError,subprocess.TimeoutExpired):raise Refusal('postgres_backup_tool_failure') from None
    if result.returncode:raise Refusal('postgres_backup_tool_failure')


def reports(store):return {str(row[0]):verify_store(store,row[0]) for row in store.db.execute("SELECT id FROM chain.generations WHERE status='ready' ORDER BY id").fetchall()}


def backup(archive,manifest,service=None,*,conninfo='',reserve_check=None,pg_dump='pg_dump'):
    archive=safe_file(archive,existing=False);manifest=safe_file(manifest,existing=False)
    if archive==manifest:raise Refusal('backup_paths_overlap')
    if reserve_check:reserve_check()
    temp=archive.parent/('.'+archive.name+'.'+uuid.uuid4().hex+'.partial')
    temp_manifest=manifest.parent/('.'+manifest.name+'.'+uuid.uuid4().hex+'.partial')
    try:
        with PgStore(service,conninfo=conninfo,reserve_check=reserve_check) as store:
            check_tool(pg_dump,store.db)
            if store.pending() is not None:raise Refusal('pending_generation_cannot_backup')
            if store.meta('source_conflict'):raise Refusal('revoked_database_cannot_backup')
            verified=reports(store);physical=physical_inventory(store);state=store.state()
            if state['canonical_ready'] is None:raise Refusal('no_ready_generation')
            with store.db.transaction():
                store.db.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                snapshot=store.db.execute('SELECT pg_export_snapshot()').fetchone()[0]
                fd=os.open(temp,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600);os.close(fd)
                run_cli([pg_dump,'--format=custom','--no-owner','--no-privileges','--schema=chain','--schema=serve','--schema=public','--exclude-table-data=chain.service_state','--snapshot='+snapshot,'--file='+str(temp)],store.db)
            body={'format':next(f for f,versions in FORMATS.items() if state['schema_version'] in versions),'postgres_major':POSTGRES_MAJOR,'schema_version':state['schema_version'],'owner':OWNER,'profile':store.meta('profile'),'canonical_ready':state['canonical_ready'],'publication_serial':state['publication_serial'],'generations':verified,'physical_inventory':physical,'archive':file_hash(temp)}
            fd=os.open(temp_manifest,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
            with os.fdopen(fd,'w') as out:out.write(canonical(body)+'\n');out.flush();os.fsync(out.fileno())
            with open(temp,'rb') as inp:os.fsync(inp.fileno())
            os.link(temp,archive);os.link(temp_manifest,manifest)
            for directory in {archive.parent,manifest.parent}:
                fd=os.open(directory,os.O_RDONLY|os.O_DIRECTORY);os.fsync(fd);os.close(fd)
            return {'status':'backed_up','archive':str(archive),'manifest':str(manifest),'manifest_sha256':file_hash(manifest)['sha256'],'canonical_ready':body['canonical_ready'],'archive_receipt':body['archive']}
    finally:
        temp.unlink(missing_ok=True);temp_manifest.unlink(missing_ok=True)


def restore(archive,manifest,destination_service=None,*,conninfo='',reserve_check=None,pg_restore='pg_restore',expected_manifest_sha256=None,expected_network=None,reader_role=None):
    """Restore into an empty database; `reader_role` (schema 3) gets the node reader's grants again
    once the restored database serves. No other role may connect before (PUBLIC loses CONNECT)."""
    archive=safe_file(archive,existing=True);manifest=safe_file(manifest,existing=True)
    if expected_manifest_sha256 and file_hash(manifest)['sha256']!=expected_manifest_sha256:raise Refusal('backup_manifest_checksum_mismatch')
    try:body=json.loads(manifest.read_text())
    except (ValueError,UnicodeError):raise Refusal('invalid_backup_manifest') from None
    if body.get('format') not in FORMATS or body.get('postgres_major')!=POSTGRES_MAJOR or body.get('schema_version') not in FORMATS[body['format']] or body.get('schema_version') not in SUPPORTED_SCHEMA_VERSIONS or body.get('owner')!=OWNER:raise Refusal('unsupported_backup_identity')
    if reader_role is not None and body['schema_version']!=SCHEMA_VERSION:raise Refusal('reader_requires_current_schema')
    if body.get('archive')!=file_hash(archive):raise Refusal('backup_checksum_mismatch')
    from db_sync_alt.networks import for_identity
    profile=body.get('profile',{})
    network=for_identity(profile)
    if network is None or (expected_network is not None and network.network!=expected_network):raise Refusal('backup_profile_mismatch')
    if reserve_check:reserve_check()
    with connect(destination_service,conninfo=conninfo) as db:
        if not db.execute('SELECT pg_try_advisory_lock(%s,%s)',WRITER_LOCK).fetchone()[0]:raise Refusal('writer_busy')
        if not empty_destination(db):raise Refusal('destination_not_empty')
        check_tool(pg_restore,db)
        # No reader connects while the database is being restored: it would see no tx_in relation.
        from psycopg import sql
        db.execute(sql.SQL('REVOKE ALL ON DATABASE {} FROM PUBLIC').format(sql.Identifier(db.info.dbname)))
        # A crashed restore stays visibly task-owned but unready. No source state
        # row is in the dump, so copied ready generations cannot serve early.
        db.execute('CREATE SCHEMA '+RESTORE_GUARD)
        db.execute('CREATE TABLE '+RESTORE_GUARD+'.identity(owner text,manifest_sha256 text)')
        db.execute('INSERT INTO '+RESTORE_GUARD+'.identity VALUES(%s,%s)',(OWNER,file_hash(manifest)['sha256']))
        # pg_dump explicitly recreates public. It was proven empty above; no
        # CASCADE is allowed, so a concurrent object cannot be silently removed.
        db.execute('DROP SCHEMA public')
        run_cli([pg_restore,'--no-owner','--no-privileges','--exit-on-error','--single-transaction','--dbname='+db.info.dbname,str(archive)],db)
        db.execute("INSERT INTO chain.service_state(id,owner,schema_version,migration_state) VALUES(1,%s,%s,'restoring')",(OWNER,body['schema_version']))
        apply_privileges(db)
        # Transfer ownership to PgStore without dropping the fencing session.
        store=PgStore.__new__(PgStore);store.db=db;store._writer=True;store.readonly=False;store.reserve_check=reserve_check;store.progress_callback=None
        if store.meta('profile')!=profile or store.network!=network or reports(store)!=body.get('generations') or physical_inventory(store)!=body.get('physical_inventory'):raise Refusal('restored_database_mismatch')
        # Explicit one-shot polling may create a poll policy even on schema 1;
        # jobs without a policy marker are left unchanged (the helper skips them).
        from db_sync_alt.sources.blockfrost.poller import bind_restored_poll_policy
        poll_binding=bind_restored_poll_policy(store,file_hash(manifest)['sha256'])
        ready=body['canonical_ready']
        if str(ready) not in body['generations']:raise Refusal('invalid_backup_canonical')
        if store.serves_directly:
            # A restore carries no planner statistics: gather them before the node is served.
            from db_sync_alt.store.serve import TABLES as SERVED
            db.execute('ANALYZE '+','.join('serve.'+t for t in SERVED))
        with db.transaction():
            db.execute('SELECT pg_advisory_xact_lock(%s,%s)',SERVING_LOCK)
            # Observations are runtime state; startup must refresh before healthy.
            db.execute("UPDATE chain.service_state SET canonical_ready=%s,migration_state='ready',publication_serial=%s WHERE id=1",(ready,body['publication_serial']))
            db.execute('DROP SCHEMA '+RESTORE_GUARD+' CASCADE')
        granted=None
        if reader_role is not None:
            # The reader may connect only now that the restored database serves.
            from db_sync_alt.store.reader import grant_reader
            granted=grant_reader(db,reader_role)
        return {'status':'restored',**({'reader':granted} if granted else {}),'canonical_ready':ready,'database_bytes':store.status()['bytes'],'profile':profile,'freshness':'unknown',**({'poll_restore_binding':poll_binding} if poll_binding is not None else {})}


def main(argv=None):
    import argparse
    parser=argparse.ArgumentParser(prog='db-sync-alt',description=__doc__)
    commands=parser.add_subparsers(dest='command',required=True)
    for name in ('backup','restore','verify','status'):
        command=commands.add_parser(name);command.add_argument('--service',required=True)
        if name in ('backup','restore'):
            command.add_argument('--archive',required=True);command.add_argument('--manifest',required=True)
        if name=='restore':
            command.add_argument('--expected-manifest-sha256')
            command.add_argument('--reader-role',help="existing login role midnight-node connects with; granted read access after the restore")
    args=parser.parse_args(argv)
    try:
        if args.command=='backup':result=backup(args.archive,args.manifest,args.service)
        elif args.command=='restore':result=restore(args.archive,args.manifest,args.service,expected_manifest_sha256=args.expected_manifest_sha256,reader_role=args.reader_role)
        else:
            with PgStore(args.service,readonly=True,allow_incomplete=args.command=='status') as store:
                result=store.status() if args.command=='status' else {'generations':reports(store),'physical_inventory':physical_inventory(store)}
        print(canonical(result))
    except Refusal as error:parser.exit(2,canonical({'error':error.code})+'\n')
    except Exception:parser.exit(2,canonical({'error':'postgres_operation_failed'})+'\n')
