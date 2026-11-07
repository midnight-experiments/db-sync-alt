"""Stores at an older schema (test support only)."""
from db_sync_alt.store.upgrade import _upgrade_v2, _upgrade_v3
from db_sync_alt.verify.store import verify_store


def to_schema3(store):
    """Upgrade a ready schema-1 store to schema 3 only: served tables, no integrity checkpoints,
    the integrity base at the canonical generation. Backups of such a store use format v2."""
    verify_store(store)
    _upgrade_v2(store, store.state())
    _upgrade_v3(store, store.state())
