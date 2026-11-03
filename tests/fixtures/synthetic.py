"""Synthetic generations written straight into a new PostgreSQL store (test support only).

The service creates generations with its acquisition commands. Tests need small,
exactly known histories instead: these helpers create the store, add a generation
row, let the caller write its facts, block membership and receipts with the store's
own methods, check the generation with the service's verifier and mark it ready.
Serving opens only after every generation is ready (`open_serving`).
"""
from psycopg.types.json import Jsonb
from db_sync_alt.contract import TABLES
from db_sync_alt.store.database import PgStore, SERVING_LOCK, bind_row
from db_sync_alt.verify.store import verify_store

DBSYNC = 'cardano-db-sync-postgresql'
# Validation kind of a ready db-sync generation that carries no acquisition seal.
FAMILY_INVENTORY = 'independent-sql-family-inventory-v1'


def create(conninfo, profile_identity, **kwargs):
    """A new owned store on an empty database (still importing) with the profile recorded."""
    kwargs.setdefault('network', profile_identity['network'])
    store = PgStore(conninfo=conninfo, create=True, **kwargs)
    try:
        store.set_meta('profile', profile_identity)
    except BaseException:
        store.close()
        raise
    return store


def begin(store, *, start, target, witness, manifest, parent=None, source=DBSYNC):
    """Add generation `parent + 1` (or 0) as pending and return its id."""
    generation = 0 if parent is None else parent + 1
    store.db.execute("INSERT INTO chain.generations VALUES(%s,%s,'pending',%s,%s,%s,%s,%s,%s)",
                     (generation, parent, source, *start, target, witness, Jsonb(manifest)))
    return generation


def add_blocks(store, generation, rows):
    """Insert block headers (an identical existing header is kept) and bind them to `generation`.

    A bulk shortcut for the thousands of headers a fixture needs; `seal` verifies the
    resulting header chain, anchors and membership with the service's verifier.
    """
    rows = list(rows)
    columns = TABLES['blocks']
    with store.db.cursor() as cursor:
        cursor.executemany('INSERT INTO chain.blocks(' + ','.join(columns) + ',first_generation) VALUES(' + ','.join(['%s'] * (len(columns) + 1)) + ') ON CONFLICT (hash) DO NOTHING',
                           [(*bind_row('blocks', row), generation) for row in rows])
        cursor.executemany('INSERT INTO chain.generation_blocks VALUES(%s,%s,%s)', [(generation, row['height'], row['hash']) for row in rows])


def seal(store, generation, manifest, **validation):
    """Verify the pending generation, record its digests in `manifest` and mark it ready."""
    report = verify_store(store, generation, require_ready=False)
    manifest.update(digests=report['digests'],
                    validation={'kind': FAMILY_INVENTORY, 'digests': report['digests'], **validation})
    store.db.execute("UPDATE chain.generations SET manifest=%s,status='ready' WHERE id=%s", (Jsonb(manifest), generation))
    return verify_store(store, generation)


def open_serving(store, generation):
    """Publish `generation` as the ready generation of a store whose generations are all ready."""
    with store.atomic():
        store.db.execute('SELECT pg_advisory_xact_lock(%s,%s)', SERVING_LOCK)
        store.db.execute("UPDATE chain.service_state SET canonical_ready=%s,migration_state='ready',publication_serial=1 WHERE id=1",
                         (generation,))
