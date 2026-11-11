"""Explicit, fenced schema upgrades (1 -> 2 -> 3 -> 4 -> 5); never run as an implicit live startup step."""
from pathlib import Path
from psycopg import sql
from db_sync_alt.contract import Refusal

LATEST=5


def _upgrade_v2(store,state):
    from db_sync_alt.store.database import SERVING_LOCK,apply_privileges
    from db_sync_alt.verify.limits import bulk
    with store.atomic():
        # One-time indexed projection/FK construction over the verified base. Each whole-table statement runs
        # under the documented BULK_TIMEOUT; every other statement keeps the session limit (issue 00042).
        store.db.execute('SELECT pg_advisory_xact_lock(%s,%s)',SERVING_LOCK)
        if store.state()!=state:raise Refusal('upgrade_state_changed')
        store.db.execute(Path(__file__).with_name('schema_v2.sql').read_text())
        store.db.execute('INSERT INTO chain.integrity_state(id,base_generation,projection_generation) VALUES(1,%s,%s)',(state['canonical_ready'],state['canonical_ready']))
        with bulk(store.db):
            store.db.execute('INSERT INTO chain.current_membership SELECT * FROM chain.membership(%s)',(state['canonical_ready'],))
            # The served blocks are later built in height ranges of this table.
            store.db.execute('ANALYZE chain.current_membership')
        # Bulk validation preserves the exact final FK without 859k individual
        # trigger lookups during initial projection construction.
        with bulk(store.db):
            store.db.execute('ALTER TABLE chain.current_membership ADD CONSTRAINT current_membership_hash_fkey FOREIGN KEY(hash) REFERENCES chain.blocks(hash)')
        # The fact families this store has: a store written before bridge metadata has no tx_metadata yet.
        families=tuple(store.physical_tables)
        for table in families:
            with bulk(store.db):
                store.db.execute(sql.SQL('CREATE INDEX {} ON chain.{}(first_generation)').format(sql.Identifier('delta_'+table),sql.Identifier(table)))
        for table in (*families,'generations','generation_blocks','checkpoints','provenance','metadata','generation_reuse','membership_delta','pools'):
            store.db.execute(sql.SQL('CREATE TRIGGER immutable_guard BEFORE INSERT OR UPDATE OR DELETE ON chain.{} FOR EACH ROW EXECUTE FUNCTION chain.guard_immutable()').format(sql.Identifier(table)))
        store.db.execute('UPDATE chain.service_state SET schema_version=2 WHERE id=1')
        for table in ('current_membership','integrity_state','service_state'):
            store.db.execute(sql.SQL('CREATE TRIGGER projection_guard BEFORE INSERT OR UPDATE OR DELETE ON chain.{} FOR EACH ROW EXECUTE FUNCTION chain.guard_projection()').format(sql.Identifier(table)))
        for table in (*families,'generations','generation_blocks','checkpoints','provenance','metadata','generation_reuse','membership_delta','pools','current_membership','integrity_state','service_state'):
            store.db.execute(sql.SQL('CREATE TRIGGER no_truncate BEFORE TRUNCATE ON chain.{} EXECUTE FUNCTION chain.guard_truncate()').format(sql.Identifier(table)))
        apply_privileges(store.db)


# What schema 3 defines in chain beyond its tables: anything else a database of an older schema
# carries is removed, so the gated public relations are the only reading surface.
SCHEMA3_FUNCTIONS=('gate','guard_immutable','guard_projection','guard_truncate','membership')
SERVICE_STATE_COLUMNS=('id','owner','schema_version','canonical_ready','revoked','publication_serial','migration_state')


def _normalize_v3(db):
    for function, in db.execute("SELECT 'chain.'||quote_ident(p.proname)||'('||pg_get_function_identity_arguments(p.oid)||')' FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='chain' AND p.proname<>ALL(%s)",(list(SCHEMA3_FUNCTIONS),)).fetchall():
        db.execute('DROP FUNCTION '+function)
    for column, in db.execute("SELECT attname FROM pg_attribute WHERE attrelid='chain.service_state'::regclass AND attnum>0 AND NOT attisdropped AND attname<>ALL(%s)",(list(SERVICE_STATE_COLUMNS),)).fetchall():
        db.execute(sql.SQL('ALTER TABLE chain.service_state DROP COLUMN {}').format(sql.Identifier(column)))


ORDINAL_KEYS={'inputs':('tx_hash','origin_hash','origin_index'),'output_assets':('tx_hash','output_index','policy','name')}
ORDINAL_CONSTRAINTS={
    'inputs':(('inputs_ordinal_range','CHECK(ordinal BETWEEN 0 AND 4095)'),('inputs_transaction_ordinal','UNIQUE(tx_hash,ordinal)')),
    'output_assets':(('output_assets_ordinal_range','CHECK(ordinal BETWEEN 0 AND 4095)'),('output_assets_output_ordinal','UNIQUE(tx_hash,output_index,ordinal)')),
}


def missing_ordinals(db):
    """Keys of the stored inputs and output assets that have no ledger ordinal yet."""
    out={}
    for table,keys in ORDINAL_KEYS.items():
        present=db.execute("SELECT 1 FROM pg_attribute WHERE attrelid=%s::regclass AND attname='ordinal' AND NOT attisdropped",('chain.'+table,)).fetchone()
        where=' WHERE ordinal IS NULL' if present else ''
        out[table]=[tuple(bytes(v) if isinstance(v,memoryview) else v for v in row) for row in db.execute('SELECT '+','.join(keys)+' FROM chain.'+table+where+' ORDER BY '+','.join(keys))]
    return out


def _fill_ordinals(db,found):
    """Write source ordinals into existing fact rows (their immutability guard is lifted for this fenced step only)."""
    for table,values in found.items():
        if not values:continue
        keys=ORDINAL_KEYS[table];types={'tx_hash':'bytea','origin_hash':'bytea','origin_index':'int','output_index':'int','policy':'bytea','name':'bytea'}
        columns=list(zip(*[(*key,ordinal) for key,ordinal in values.items()]))
        db.execute(sql.SQL('ALTER TABLE chain.{} DISABLE TRIGGER immutable_guard').format(sql.Identifier(table)))
        unnest=','.join('%s::'+types[k]+'[]' for k in keys)+',%s::int[]'
        changed=db.execute('UPDATE chain.'+table+' f SET ordinal=v.ordinal FROM unnest('+unnest+') v('+','.join(keys)+',ordinal) WHERE '
                           +' AND '.join('f.'+k+'=v.'+k for k in keys)+' AND f.ordinal IS NULL',[list(c) for c in columns]).rowcount
        db.execute(sql.SQL('ALTER TABLE chain.{} ENABLE TRIGGER immutable_guard').format(sql.Identifier(table)))
        if changed!=len(values):raise Refusal('ordinal_source_incomplete')


def _upgrade_v3(store,state,ordinal_source=None):
    """Ledger ordinals, the served tables and the gate: the database serves a reader role directly."""
    from db_sync_alt.store.database import SERVING_LOCK,PROJECTION_WRITE,ORDINAL_LIMIT,apply_privileges
    from db_sync_alt.store import serve
    missing=missing_ordinals(store.db);found={}
    if any(missing.values()):
        if ordinal_source is None:raise Refusal('ordinal_source_required')
        g=store.generation()
        found=ordinal_source.ordinals(missing['inputs'],missing['output_assets'],{k:g['manifest']['anchors'][k] for k in ('start','target')})
        if any(set(found[t])!=set(missing[t]) for t in missing):raise Refusal('ordinal_source_incomplete')
        if any(type(v) is not int or not 0<=v<ORDINAL_LIMIT for t in found for v in found[t].values()):raise Refusal('invalid_ordinal')
    from db_sync_alt.verify.limits import bulk
    with store.atomic():
        # One-time build of the served tables from the verified projection: the served rows in bounded ranges
        # under the session limit, each whole-table statement under the documented BULK_TIMEOUT (issue 00042).
        store.db.execute('SELECT pg_advisory_xact_lock(%s,%s)',SERVING_LOCK)
        if store.state()!=state:raise Refusal('upgrade_state_changed')
        store.db.execute('SELECT set_config(%s,%s,true)',(PROJECTION_WRITE,'on'))
        for table in ORDINAL_KEYS:
            store.db.execute(sql.SQL('ALTER TABLE chain.{} ADD COLUMN IF NOT EXISTS ordinal smallint').format(sql.Identifier(table)))
        with bulk(store.db):
            _fill_ordinals(store.db,found)
        for table,constraints in ORDINAL_CONSTRAINTS.items():
            with bulk(store.db):
                store.db.execute(sql.SQL('ALTER TABLE chain.{} ALTER COLUMN ordinal SET NOT NULL').format(sql.Identifier(table)))
            for name,body in constraints:
                if not store.db.execute('SELECT 1 FROM pg_constraint WHERE conname=%s AND conrelid=%s::regclass',(name,'chain.'+table)).fetchone():
                    with bulk(store.db):
                        store.db.execute(sql.SQL('ALTER TABLE chain.{} ADD CONSTRAINT {} '+body).format(sql.Identifier(table),sql.Identifier(name)))
        _normalize_v3(store.db)
        store.db.execute(Path(__file__).with_name('schema_v3.sql').read_text())
        counts=serve.build(store.db,state['canonical_ready'])
        with bulk(store.db):
            store.db.execute(Path(__file__).with_name('schema_v3_after_load.sql').read_text())
        store.db.execute('UPDATE chain.service_state SET schema_version=3 WHERE id=1')
        apply_privileges(store.db)
    return {'served_rows':counts,'ordinals_from_source':{t:len(v) for t,v in found.items()}}


def _upgrade_v4(store,state,checkpoint):
    """Integrity checkpoints; the serving gate requires schema 4. One transaction: the table, its guards,
    the gate, the schema version and the first checkpoint (from the full verification `checkpoint`
    holds, when its generation is newer than the integrity base)."""
    from db_sync_alt.store.database import SERVING_LOCK,PROJECTION_WRITE,apply_privileges
    from db_sync_alt.store.incremental import insert_checkpoint,integrity_base
    with store.atomic():
        store.db.execute('SELECT pg_advisory_xact_lock(%s,%s)',SERVING_LOCK)
        if store.state()!=state:raise Refusal('upgrade_state_changed')
        store.db.execute('SELECT set_config(%s,%s,true)',(PROJECTION_WRITE,'on'))
        store.db.execute(Path(__file__).with_name('schema_v4.sql').read_text())
        store.db.execute('UPDATE chain.service_state SET schema_version=4 WHERE id=1')
        written=None
        if checkpoint is not None and checkpoint.generation>integrity_base(store)[0]:
            written=insert_checkpoint(store,checkpoint)['generation']
        apply_privileges(store.db)
    return written


def _relevance_guard_v5(db):
    """The relevance-anchor branch of the immutability guard admits anchors on the serving schema: a
    schema-4 store whose guard already has that branch gets its schema-5 successor."""
    from db_sync_alt.store.database import relevance_anchor_sql
    previous=relevance_anchor_sql(4).split('AS $$',1)[1].split('END $$;',1)[0]
    actual=db.execute("SELECT prosrc FROM pg_proc WHERE oid='chain.guard_immutable()'::regprocedure").fetchone()[0]
    if actual==previous+'END ':
        db.execute(relevance_anchor_sql(5))


def _schema_v5(store):
    """Inside the caller's gated transaction: served epoch parameters, gate and ready view of schema 5."""
    from db_sync_alt.store.database import apply_privileges
    from db_sync_alt.verify.limits import bulk
    # It validates every stored transaction and copies the stored bridge metadata: whole-table statements.
    with bulk(store.db):
        store.db.execute(Path(__file__).with_name('schema_v5.sql').read_text())
    _relevance_guard_v5(store.db)
    store.db.execute('UPDATE chain.service_state SET schema_version=5 WHERE id=1')
    apply_privileges(store.db)


def _upgrade_v5(store,state,checkpoint):
    """Schema 5 of a store whose facts already carry max_tx_size: one transaction for the served
    column, the gate and the version, with the checkpoint `checkpoint` holds when it is newer than the base."""
    from db_sync_alt.store.database import SERVING_LOCK,PROJECTION_WRITE
    from db_sync_alt.store.incremental import insert_checkpoint,integrity_base
    with store.atomic():
        store.db.execute('SELECT pg_advisory_xact_lock(%s,%s)',SERVING_LOCK)
        if store.state()!=state:raise Refusal('upgrade_state_changed')
        store.db.execute('SELECT set_config(%s,%s,true)',(PROJECTION_WRITE,'on'))
        written=None
        if checkpoint is not None and checkpoint.generation>integrity_base(store)[0]:
            written=insert_checkpoint(store,checkpoint)['generation']
        _schema_v5(store)
    return written


def upgrade_store(store,ordinal_source=None,epoch_source=None):
    """Upgrade a ready store to schema 5. A store whose inputs or output assets have no ledger
    ordinal (written before schema 3) needs `ordinal_source`, a read-only db-sync source.

    A nonce-only store (written before epoch parameters) also needs `epoch_source`, an authoritative
    source of every stored epoch's max_tx_size (store/epoch_parameters.py); without one it is
    refused before anything is read or changed. The full verification that starts the upgrade also
    writes the next integrity checkpoint of a store whose facts are already complete."""
    from db_sync_alt.verify.store import verify_store
    from db_sync_alt.verify.limits import analyze_chain
    from db_sync_alt.store.incremental import certify_checkpoint
    store.require_writer()
    state=store.state();legacy=store.legacy_generation(None)
    if state['schema_version']==LATEST and not legacy:return {'schema_version':LATEST,'changed':False}
    if state['schema_version'] not in (1,2,3,4,LATEST) or state['migration_state']!='ready' or state['revoked']:raise Refusal('upgrade_requires_ready')
    if legacy:
        from db_sync_alt.store.epoch_parameters import admit
        admit(store,epoch_source)
    # The store may come from a bulk load or a restore without planner statistics.
    analyze_chain(store.db)
    if state['schema_version']==1:
        # Schema 2 makes the verified canonical generation the integrity base: nothing to checkpoint.
        report=verify_store(store);checkpoint=None
    else:
        checkpoint=certify_checkpoint(store);report=checkpoint.report
    served={}
    if state['schema_version']==1:
        _upgrade_v2(store,state);state=store.state()
    if state['schema_version']==2:
        served=_upgrade_v3(store,state,ordinal_source);state=store.state()
    written=None
    if state['schema_version']==3:
        written=_upgrade_v4(store,state,checkpoint);state=store.state();checkpoint=None
    epochs=None
    if legacy:
        from db_sync_alt.store.epoch_parameters import upgrade_epoch_parameters
        epochs=upgrade_epoch_parameters(store,state,epoch_source)
    else:
        written=_upgrade_v5(store,state,checkpoint) or written
    from db_sync_alt.store import serve
    from db_sync_alt.bridge import report as bridge_report
    # The upgrade steps loaded projection, ordinal and parameter rows.
    analyze_chain(store.db)
    serve.verify(store,store.state()['canonical_ready'])
    # Bridge metadata coverage starts after the upgraded generation: the next Blockfrost generation is the first covered.
    result={'schema_version':LATEST,'changed':True,'base_generation':store.integrity()['base'],'checkpoint_generation':written,'full_verification':report,**served,'bridge':bridge_report(store)}
    if epochs is not None:result['epoch_parameters']=epochs
    if ordinal_source is not None and any(served.get('ordinals_from_source',{}).values()):
        identity=getattr(ordinal_source,'validated_identity',None)
        if identity is not None:result['ordinal_source_identity']=identity
    return result


# What a nonce-only store needs to be upgraded, named in the refusal.
EPOCH_SOURCE_REQUIRED=('--source-service with --source-attestation (a cardano-db-sync of the network), or the '
                       'Blockfrost settings the service uses (DBSYNC_PROVIDER_KEY_FILE and DBSYNC_PROVIDER_STATE, '
                       'or DBSYNC_PROVIDER_ADAPTER=shared with DBSYNC_SHARED_ROOT)')


def epoch_source(network,sql_source=None):
    """The authoritative source of a nonce-only store's epoch parameters: the given cardano-db-sync,
    else the network's accounted Blockfrost client when one is configured, else None."""
    from db_sync_alt.store.epoch_parameters import ProviderEpochSource,SQLEpochSource
    if sql_source is not None:return SQLEpochSource(sql_source)
    from db_sync_alt.sources.blockfrost.provider import ProviderError,configured_client
    try:client=configured_client(network)
    except ProviderError:return None
    return ProviderEpochSource(client,network)


def main(argv=None):
    import argparse
    import json
    from db_sync_alt.store.database import PgStore
    parser=argparse.ArgumentParser(prog='db-sync-alt upgrade',description=__doc__+'\n\nA store written before epoch parameters reads every stored epoch\'s '
                                   'max_tx_size twice from --source-service, or else from Blockfrost (2 requests per stored epoch).')
    parser.add_argument('--service',required=True)
    parser.add_argument('--source-service',help='read-only cardano-db-sync of the same network for missing ledger ordinals and epoch parameters; requires --source-attestation')
    parser.add_argument('--source-attestation',type=Path,help='truthful version/retention JSON, as for acquire; requires --source-service')
    args=parser.parse_args(argv)
    try:
        if bool(args.source_service)!=bool(args.source_attestation):raise Refusal('source_attestation_pair_required')
        with PgStore(args.service,allow_legacy=True) as store:
            source=None
            if args.source_service:
                from db_sync_alt.contract import Profile
                from db_sync_alt.resources import profile_dir
                from db_sync_alt.sources.dbsync.reader import SQLSource
                source=SQLSource(args.source_service,Profile(profile_dir(store.network.network),store.network.network),json.loads(args.source_attestation.read_text()))
            epochs=epoch_source(store.network,source) if store.legacy_format else None
            result=upgrade_store(store,source,epochs)
        print(json.dumps(result,sort_keys=True,default=str))
    except Refusal as e:
        body={'error':e.code}
        if e.code=='epoch_parameters_source_required':body['required']=EPOCH_SOURCE_REQUIRED
        parser.exit(2,json.dumps(body)+'\n')
    except Exception:parser.exit(2,'{"error":"schema_upgrade_failed"}\n')
