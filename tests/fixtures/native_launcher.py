"""Subprocess fixture: real native launcher, counted synthetic provider only."""
import json
import os
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))  # run as a script: make `fixtures` importable
import db_sync_alt.sources.blockfrost.sync as pg_sync
from db_sync_alt.sources.blockfrost.provider import PortableClient
from db_sync_alt.store.database import PgStore
from db_sync_alt.cli.run import main as launcher_main
from fixtures.blockfrost import provider_row


def client(network):
    def transport(path):
        if path == '/metrics':
            value = []
        else:
            with PgStore('db_sync_alt_owner', readonly=True) as store:
                generation = store.generation()
                height = generation['witness_height'] if path == '/blocks/latest' else int(path.rsplit('/',1)[1])
                value = provider_row(store.block_at(generation['id'],height))
        return 200,{},json.dumps(value).encode()
    return PortableClient(Path(os.environ['DBSYNC_PROVIDER_KEY_FILE']).read_text().strip(),
                          os.environ['DBSYNC_PROVIDER_STATE'],network=network,transport=transport)

pg_sync.shared_client = client
raise SystemExit(launcher_main())
