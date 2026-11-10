"""Stores of the nonce-only fact format, as releases before epoch parameters wrote them (test support only).

Inside `nonce_only()` the service code runs with that release's fact format: the
`midnight-compact-cardano-v1` contract, epoch facts `(epoch, nonce)`, generations without the
`epoch_max_tx_size_v1` capability, schema 4 as the current schema (its serving gate and its
relevance-anchor guard) and no stored `max_tx_size` column. Everything else is the same code, so a
store built here has the shape a nonce-only release gives it: generations, receipts, seals,
integrity checkpoints and relevance anchors. Leaving the context, the same store is what this
release finds when it opens an existing database.
"""
import contextlib
from unittest.mock import patch

from db_sync_alt import backup, contract
from db_sync_alt.sources.blockfrost import adapter, follow
from db_sync_alt.store import database, reader, upgrade
from db_sync_alt.verify import store as verification

# The nonce-only release created chain.epoch_nonces without max_tx_size and its schema-1 view over it.
NONCE_ONLY_DDL = """DROP VIEW public.epoch_param;
ALTER TABLE chain.epoch_nonces DROP COLUMN max_tx_size;
CREATE VIEW public.epoch_param AS SELECT epoch AS epoch_no,nonce FROM chain.epoch_nonces WHERE first_generation<=(SELECT id FROM chain.ready_generation);"""


@contextlib.contextmanager
def nonce_only():
    legacy_contract_for = contract.contract_for
    create = database.PgStore.__init__

    def init(self, *args, **kwargs):
        create(self, *args, **kwargs)
        if kwargs.get('create'):
            self.db.execute(NONCE_ONLY_DDL)

    def contract_for(network, *, legacy=False):
        return legacy_contract_for(network, legacy=True)

    # The nonce-only release neither read nor recorded an epoch's size.
    read_epoch, complete_epoch = adapter.epoch_facts, adapter.complete_inherited_epoch

    def epoch_facts(*args, **kwargs):
        fact, stakes, anchor, receipt = read_epoch(*args, **kwargs)
        fact.pop('max_tx_size'); receipt.pop('max_tx_size')
        return fact, stakes, anchor, receipt

    def complete_inherited_epoch(provider, store, g, epoch, inherited):
        stored = store.epoch_nonce
        with patch.object(store, 'epoch_nonce', lambda e, gen: None if stored(e, gen) is None else {**stored(e, gen), 'max_tx_size': None}):
            receipt = complete_epoch(provider, store, g, epoch, inherited)
        receipt.pop('max_tx_size')
        return receipt

    with contextlib.ExitStack() as stack:
        for module in (contract, database, verification):
            stack.enter_context(patch.object(module, 'FORMAT', contract.LEGACY_FORMAT))
        stack.enter_context(patch.dict(contract.TABLES, {'epoch_nonces': contract.LEGACY_TABLES['epoch_nonces']}))
        stack.enter_context(patch.dict(contract.NUMERIC_FIELDS, {'epoch_nonces': contract.LEGACY_NUMERIC_FIELDS['epoch_nonces']}))
        stack.enter_context(patch.object(contract, 'CAPABILITIES', dict(contract.LEGACY_CAPABILITIES)))
        stack.enter_context(patch.object(contract, 'contract_for', contract_for))
        stack.enter_context(patch.object(database, 'SCHEMA_VERSION', 4))
        # The nonce-only releases know schemas 1 to 4 only, and check that before the contract.
        for module in (database, backup):
            stack.enter_context(patch.object(module, 'SUPPORTED_SCHEMA_VERSIONS', (1, 2, 3, 4)))
        stack.enter_context(patch.object(reader, 'SCHEMA_VERSION', 4))
        stack.enter_context(patch.object(database.PgStore, '__init__', init))
        for module in (adapter, verification):
            stack.enter_context(patch.object(module, 'epoch_size', lambda value: None))
        for module in (adapter, follow):
            stack.enter_context(patch.object(module, 'epoch_facts', epoch_facts))
            stack.enter_context(patch.object(module, 'complete_inherited_epoch', complete_inherited_epoch))
        # The nonce-only release's upgrade ended at schema 4.
        stack.enter_context(patch.object(upgrade, '_upgrade_v5', lambda store, state, checkpoint: None))
        yield

