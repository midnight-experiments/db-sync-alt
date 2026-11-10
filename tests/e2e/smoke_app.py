"""In-image side of the Compose smoke: seed a backup, serve with a synthetic provider, check, read as the node.

Run as `python tests/e2e/smoke_app.py seed|serve|check|node`. Never talks to an external provider.
"""
import json
import os
from pathlib import Path
import sys
import time
import urllib.request
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))  # run as a script: make `fixtures` importable
import psycopg
from db_sync_alt.store.database import PgStore

KEY='preprod'+'synthetic0000000000000000'

def seed(directory):
    from fixtures.generations import make_store
    from db_sync_alt.store.upgrade import upgrade_store
    from db_sync_alt.backup import backup
    dsn='host=127.0.0.1 user=postgres dbname=smoke_seed'
    with psycopg.connect('host=127.0.0.1 user=postgres dbname=postgres',autocommit=True) as db:db.execute('CREATE DATABASE smoke_seed')
    make_store(dsn)
    with PgStore(conninfo=dsn) as store:upgrade_store(store)
    backup(Path(directory)/'state.dump',Path(directory)/'manifest.json',conninfo=dsn)

def serve():
    import db_sync_alt.sources.blockfrost.sync as pg_sync
    from db_sync_alt.sources.blockfrost.provider import PortableClient
    from fixtures.blockfrost import provider_row
    def transport(path):
        if path=='/metrics':value=[]
        else:
            with PgStore('db_sync_alt_owner',readonly=True) as store:
                gen=store.generation();height=gen['witness_height'] if path=='/blocks/latest' else int(path.rsplit('/',1)[1])
                value=provider_row(store.block_at(gen['id'],height))
        return 200,{},json.dumps(value).encode()
    pg_sync.shared_client=lambda network:PortableClient(KEY,os.environ['DBSYNC_PROVIDER_STATE'],network=network,transport=transport)
    import db_sync_alt.service.runtime as pg_service
    raise SystemExit(pg_service.main(['--config','/run/db-sync-alt/service.json']))

def check():
    deadline=time.monotonic()+60
    while True:
        try:
            with urllib.request.urlopen('http://service:18080/health',timeout=2) as response:
                if response.status==200:
                    status=json.loads(response.read())
                    break
        except Exception:
            if time.monotonic()>deadline:raise RuntimeError('synthetic service did not become healthy') from None
            time.sleep(.5)
    if status['reader']['role']!='db_sync_alt_reader' or status['reader']['warnings']:
        raise RuntimeError('reader not verified as provisioned')
    with PgStore('db_sync_alt_owner',readonly=True) as store:
        if store.state()['schema_version']!=5:raise RuntimeError('schema 5 was not restored')
        if store.db.execute('SELECT rolsuper OR rolcreaterole OR rolcreatedb FROM pg_roles WHERE rolname=current_user').fetchone()[0]:
            raise RuntimeError('the service runs as a privileged role')
    print('Synthetic generated-config Compose restore as the owner, counted polling, health and reader verification passed.')

def node():
    """A node-like client on the node network: midnight-node's own statements, as the reader, straight from PostgreSQL."""
    from db_sync_alt.node_queries import named
    with psycopg.connect(service='db_sync_alt_reader',autocommit=True,prepare_threshold=0) as db:
        if db.execute("select count(*) from information_schema.tables where table_name = 'tx_in'").fetchone()[0]!=1:
            raise RuntimeError('the node would not find tx_in')
        if not db.execute('SELECT EXISTS (SELECT 1 FROM tx_in)').fetchone()[0]:
            raise RuntimeError('the node would cache tx_in as empty')
        row=db.execute(named('get_epoch_nonce','candidates_data_source').replace('$1','%s'),(psycopg.types.numeric.Int4(2),)).fetchone()
        if row is None:raise RuntimeError('the node query returned no nonce')
        # midnight-node 2.1.101's Cost path: the size of the cursor block's epoch.
        from fixtures.generations import START
        size=db.execute(named('get_max_tx_size').replace('$1','%s'),(psycopg.types.numeric.Int4(START),)).fetchone()
        if size!=(12002,):raise RuntimeError('the node query returned no epoch max_tx_size')
        try:db.execute('SELECT * FROM chain.service_state')
        except psycopg.errors.InsufficientPrivilege:pass
        else:raise RuntimeError('the reader reads internal state')
    print('Node-network reader answered the node queries directly from PostgreSQL.')

if __name__=='__main__':
    if sys.argv[1]=='seed':seed(sys.argv[2])
    elif sys.argv[1]=='serve':serve()
    elif sys.argv[1]=='check':check()
    elif sys.argv[1]=='node':node()
    else:raise SystemExit('unknown smoke mode')
