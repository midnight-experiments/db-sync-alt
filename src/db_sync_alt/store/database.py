"""PostgreSQL store of the authoritative facts: generations, block membership, receipts and publication."""
import contextlib
import datetime as dt
import hashlib
import json
from pathlib import Path
import re
import uuid

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb
from db_sync_alt import networks
from db_sync_alt.contract import TABLES, LEGACY_TABLES, KEYS, FORMAT, LEGACY_FORMAT, BRIDGE_CAPABILITY, Refusal, canonical, digest, jsonable, metadata_json, validate_numeric

POSTGRES_MAJOR = 17
SCHEMA_VERSION = 5
# Every store session's statement limit: node-facing, serving, poller and writer work. Statements whose cost
# grows with the history run in chunks below it or under a documented limit of their own (verify/limits.py).
WRITER_TIMEOUT = '30s'
SUPPORTED_SCHEMA_VERSIONS = (1, 2, 3, 4, 5)
# Metadata key of the immutable record `db-sync-alt upgrade` writes when it adds the epoch
# parameters to a nonce-only store (see store/upgrade.py).
EPOCH_UPGRADE = 'epoch-parameters-upgrade'
OWNER = 'db-sync-alt'
RESTORE_GUARD = 'db_sync_alt_restore_guard'
PROJECTION_WRITE = 'db_sync_alt.projection_write'
WRITER_LOCK = (36, 1)
SERVING_LOCK = (36, 2)
FACT_ORDER = ('blocks','transactions','datums','outputs','inputs','assets','output_assets','mints','epoch_nonces','epoch_stakes','tx_metadata')
# Facts that also store their ledger ordinal (outside the fact contract): the position of an input
# among all ordinary inputs of its transaction, of an asset among all assets of its output.
ORDINAL_TABLES = ('inputs','output_assets')
ORDINAL_LIMIT = 4096
GENERATION_SUMMARY = ('id','parent','status','source','start_height','start_index','target_height','witness_height')
# An exhausted update's job record is kept unchanged under ARCHIVE + its job ID once its continuation replaces it.
ARCHIVE = 'provider-acquire-'


def exhausted_at(job):
    """When an acquisition job spent its budget (a job of 0.3.x has only its last reservation time)."""
    return job.get('exhausted_at', job.get('reserved_at', job.get('created_at')))


def _utc_day(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 <= value < 2**40:
        return None
    return dt.datetime.fromtimestamp(value, dt.timezone.utc).date()


def continues(old, body):
    """Whether `body` continues the exhausted update `old` on a later UTC day: same parent, target and witness,
    a fresh allotment and every earlier attempt carried in `prior_attempts`. Counts are never reset."""
    since, day = _utc_day(exhausted_at(old)), _utc_day(body.get('created_at'))
    return (old.get('state') == 'exhausted' and old.get('kind') == 'acquire' and body.get('kind') == 'acquire'
            and body.get('resumes') == old.get('job_id') and body.get('state') == 'active' and body.get('attempts') == 0
            and all(body.get(k) == old.get(k) for k in ('parent', 'target', 'witness'))
            and type(old.get('attempts')) is int and type(body.get('prior_attempts')) is int
            and body['prior_attempts'] == old.get('prior_attempts', 0) + old['attempts']
            and since is not None and day is not None and day > since)


def connect(service=None, *, conninfo='', **kwargs):
    if service is not None:
        if not re.fullmatch(r'[a-zA-Z0-9_.-]{1,128}',service):raise Refusal('invalid_destination_service')
        kwargs['service']=service
    connection=psycopg.connect(conninfo,autocommit=True,connect_timeout=2,**kwargs)
    if connection.info.server_version//10000!=POSTGRES_MAJOR:
        connection.close();raise Refusal('unsupported_postgres_version')
    return connection


def empty_destination(db):
    # Namespace dependencies cover functions/operators/collations as well as
    # ordinary relations/types. Do not adopt a function-only user database.
    return not db.execute("""SELECT 1 FROM pg_namespace n
      WHERE n.nspname NOT IN ('public','information_schema') AND n.nspname !~ '^pg_'
      UNION ALL SELECT 1 FROM pg_depend WHERE refclassid='pg_namespace'::regclass
        AND refobjid=COALESCE(to_regnamespace('public'),0::oid)
      UNION ALL SELECT 1 FROM pg_extension WHERE extname<>'plpgsql'
      UNION ALL SELECT 1 FROM pg_largeobject_metadata
      UNION ALL SELECT 1 FROM pg_foreign_server
      UNION ALL SELECT 1 FROM pg_event_trigger
      UNION ALL SELECT 1 FROM pg_publication LIMIT 1""").fetchone()


def relevance_anchor_sql(schema):
    """The immutability guard whose relevance-anchor branch admits anchors on serving schema `schema`."""
    text=Path(__file__).with_name('relevance_anchor.sql').read_text()
    return text.replace('schema_version=5','schema_version='+str(int(schema)))


def apply_privileges(db):
    """Only the owner and the roles db-sync-alt grants may connect or use the database: a reader
    connected to an empty, importing or restoring database would find no tx_in relation and cache that."""
    db.execute(sql.SQL('REVOKE ALL ON DATABASE {} FROM PUBLIC').format(sql.Identifier(db.info.dbname)))
    db.execute('REVOKE CREATE ON SCHEMA public FROM PUBLIC')
    db.execute('REVOKE ALL ON SCHEMA chain FROM PUBLIC')
    db.execute('REVOKE ALL ON ALL TABLES IN SCHEMA chain FROM PUBLIC')
    db.execute('REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA chain FROM PUBLIC')
    if db.execute("SELECT to_regnamespace('serve')").fetchone()[0] is not None:
        db.execute('REVOKE ALL ON SCHEMA serve FROM PUBLIC')
        db.execute('REVOKE ALL ON ALL TABLES IN SCHEMA serve FROM PUBLIC')


def logical_row(table, row):
    out=dict(row)
    for key,value in list(out.items()):
        if isinstance(value,memoryview):out[key]=bytes(value)
    for column in ('value','quantity','amount'):
        if column in out:
            # A datum value is NULL where cardano-db-sync stores NULL (JSON nested past its limit).
            out[column]=(None if out[column] is None else canonical(out[column])) if table=='datums' else str(out[column])
    if table=='tx_metadata' and out.get('json') is not None:out['json']=canonical(out['json'])
    if table=='blocks' and isinstance(out['time'],dt.datetime):out['time']=out['time'].strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    return out


def bind_row(table,row):
    values=[row[c] for c in TABLES[table]]
    if table=='datums' and row['value'] is not None:values[TABLES[table].index('value')]=Jsonb(json.loads(row['value']))
    if table=='tx_metadata' and row['json'] is not None:values[TABLES[table].index('json')]=Jsonb(json.loads(row['json']))
    if table=='blocks':
        raw=row['time']
        parsed=dt.datetime.fromisoformat(raw.replace('Z','+00:00'))
        if parsed.tzinfo is None or parsed.utcoffset()!=dt.timedelta(0) or parsed.strftime('%Y-%m-%dT%H:%M:%S.%fZ')!=raw:raise Refusal('invalid_timestamp')
        values[TABLES[table].index('time')]=parsed.replace(tzinfo=None)
    return values


class PgStore:
    """One owned database of one network: `create` stores the network's fact contract,
    opening finds the network from it. `network` (a name or profile) is required to
    create and, when given to open, must be the database's network.

    A store of the nonce-only fact format (written before epoch parameters) opens only with
    `allow_legacy` (upgrade, verify, status); it never takes new facts."""
    def __init__(self,service=None,*,conninfo='',readonly=False,create=False,reserve_check=None,progress_callback=None,allow_incomplete=False,network=None,allow_legacy=False):
        if readonly and create:raise Refusal('invalid_open_mode')
        if create and network is None:raise Refusal('network_required')
        expected=None if network is None else networks.network_profile(network)
        self.readonly=readonly;self._writer=False;self.reserve_check=reserve_check;self.progress_callback=progress_callback
        self.db=connect(service,conninfo=conninfo)
        try:
            self.db.execute("SET statement_timeout='"+WRITER_TIMEOUT+"'");self.db.execute("SET lock_timeout='2s'")
            if not readonly:
                if not self.db.execute('SELECT pg_try_advisory_lock(%s,%s)',WRITER_LOCK).fetchone()[0]:raise Refusal('writer_busy')
                self._writer=True
            if create:
                if not empty_destination(self.db):raise Refusal('destination_not_empty')
                with self.db.transaction():
                    self.db.execute(Path(__file__).with_name('schema.sql').read_text())
                    self.db.execute("INSERT INTO chain.service_state(id,owner,schema_version,migration_state) VALUES(1,%s,%s,'importing')",(OWNER,1))
                    self.set_meta('contract',expected.contract)
                    apply_privileges(self.db)
            self.db.execute('SET search_path=chain,public,pg_catalog')
            state=self.state()
            if state['owner']!=OWNER or state['schema_version'] not in SUPPORTED_SCHEMA_VERSIONS:raise Refusal('unsupported_database_identity')
            self.bind_format(allow_legacy=allow_legacy)
            if expected is not None and self._network!=expected:raise Refusal('network_mismatch')
            if not (allow_incomplete or create) and state['migration_state']!='ready':raise Refusal('incomplete_owned_destination')
            if readonly:self.db.execute('SET default_transaction_read_only=on')
        except BaseException:
            self.close();raise
    def bind_format(self,*,allow_legacy=False):
        """Read the stored contract: the network, whether it is the nonce-only format and the epoch
        parameter upgrade record, if any."""
        contract=self.meta('contract')
        try:
            self._network=networks.for_contract(contract);self.legacy_format=False
        except Refusal as error:
            if error.code!='unsupported_format':raise
            self._network=networks.for_contract(contract,legacy=True);self.legacy_format=True
        if self.legacy_format and not allow_legacy:raise Refusal('epoch_parameters_upgrade_required')
        record=None if self.legacy_format else self.meta(EPOCH_UPGRADE)
        if record is not None and (not isinstance(record,dict) or type(record.get('generation')) is not int or record['generation']<0):
            raise Refusal('epoch_parameters_upgrade_invalid')
        self.epoch_upgrade=record
        return self._network
    @property
    def network(self):
        """The profile of the network whose fact contract this database holds."""
        found=self.__dict__.get('_network')
        if found is None:found=self.bind_format(allow_legacy=True)
        return found
    @property
    def physical_tables(self):
        """Stored fact families and their columns: the nonce-only store has no max_tx_size and no
        tx_metadata."""
        if self.__dict__.get('_network') is None:self.network
        return LEGACY_TABLES if self.legacy_format else TABLES
    def legacy_generation(self,generation):
        """Whether `generation` was sealed in the nonce-only format: every generation of a nonce-only
        store, and those at or before the epoch parameter upgrade of an upgraded one."""
        if self.__dict__.get('_network') is None:self.network
        record=self.epoch_upgrade
        return self.legacy_format or (record is not None and generation is not None and generation<=record['generation'])
    def sealed_tables(self,generation):
        """The columns `generation`'s digests, seals and proofs were computed over."""
        return LEGACY_TABLES if self.legacy_generation(generation) else TABLES
    def close(self):
        if hasattr(self,'db'):self.db.close()
        self._writer=False
    def __enter__(self):return self
    def __exit__(self,*exc):self.close()
    def state(self):
        if self.db.execute("SELECT to_regclass('chain.service_state')").fetchone()[0] is None:
            marker=self.db.execute('SELECT to_regclass(%s)',(RESTORE_GUARD+'.identity',)).fetchone()[0]
            if marker is not None:
                cols=self.db.execute("SELECT attname,atttypid::regtype::text FROM pg_attribute WHERE attrelid=%s::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum",(RESTORE_GUARD+'.identity',)).fetchall()
                if cols==[('owner','text'),('manifest_sha256','text')]:
                    rows=self.db.execute('SELECT owner,manifest_sha256 FROM '+RESTORE_GUARD+'.identity LIMIT 2').fetchall()
                    if len(rows)==1 and rows[0][0]==OWNER and re.fullmatch('[0-9a-f]{64}',rows[0][1] or ''):raise Refusal('incomplete_owned_destination')
            raise Refusal('unowned_database')
        cur=self.db.execute('SELECT * FROM chain.service_state WHERE id=1');row=cur.fetchone()
        if row is None:raise Refusal('incomplete_owned_destination')
        return dict(zip([c.name for c in cur.description],row))
    def require_writer(self):
        if not self._writer or self.db.closed:raise Refusal('writer_ownership_required')
    def meta(self,key):
        if key=='canonical_ready':return self.state()['canonical_ready']
        if key=='source_conflict':return self.state()['revoked']
        r=self.db.execute('SELECT value FROM chain.metadata WHERE key=%s',(key,)).fetchone();return r[0] if r else None
    def set_meta(self,key,value):
        self.require_writer()
        if key in ('canonical_ready','source_conflict'):raise Refusal('publication_state_requires_fenced_transition')
        self.db.execute('INSERT INTO chain.metadata VALUES(%s,%s) ON CONFLICT(key) DO UPDATE SET value=excluded.value',(key,Jsonb(value)))
    def generation(self,generation=None):
        if generation is None:generation=self.meta('canonical_ready')
        if generation is None:raise Refusal('no_ready_generation')
        cur=self.db.execute('SELECT * FROM chain.generations WHERE id=%s',(generation,));row=cur.fetchone()
        if row is None:raise Refusal('generation_missing')
        return dict(zip([c.name for c in cur.description],row))
    def pending(self):
        row=self.db.execute("SELECT id FROM chain.generations WHERE status='pending'").fetchone()
        return self.generation(row[0]) if row else None
    @contextlib.contextmanager
    def atomic(self):
        self.require_writer()
        if self.db.info.transaction_status!=psycopg.pq.TransactionStatus.IDLE:raise Refusal('nested_store_transaction')
        with self.db.transaction():yield
    def begin_generation(self,*,source,start,target,witness,manifest,parent=None):
        self.require_writer()
        if self.legacy_generation(None):raise Refusal('epoch_parameters_upgrade_required')
        if source not in ('cardano-db-sync-postgresql','blockfrost-cardano'):raise Refusal('unsupported_source')
        if self.pending() or self.meta('source_conflict'):raise Refusal('pending_or_conflicting_lineage')
        latest=self.db.execute('SELECT max(id) FROM chain.generations').fetchone()[0]
        if parent!=latest:raise Refusal('nonlinear_lineage')
        if parent is not None and (self.generation(parent)['status']!='ready' or target<=self.generation(parent)['target_height']):raise Refusal('invalid_parent')
        if witness!=target+self.network.security or target<start[0]:raise Refusal('invalid_bounds')
        gen=0 if latest is None else latest+1
        if source=='cardano-db-sync-postgresql':
            from db_sync_alt.verify.completion import SQL_ADAPTER
            if not isinstance(manifest,dict) or manifest.get('source_identity',{}).get('adapter')!=SQL_ADAPTER:
                raise Refusal('sql_acquisition_contract_required')
        with self.db.transaction():
            self.db.execute("INSERT INTO chain.generations VALUES(%s,%s,'pending',%s,%s,%s,%s,%s,%s)",(gen,parent,source,*start,target,witness,Jsonb(manifest)))
            if source=='cardano-db-sync-postgresql':
                if self.meta('sql_completion_required_from') is None:self.set_meta('sql_completion_required_from',gen)
                self.checkpoint(gen,'sql-acquisition','identity',{'version':1,'initial_manifest_sha256':digest(manifest)})
        return gen
    def require_pending(self,generation):
        self.require_writer()
        if self.__dict__.get('legacy_format'):raise Refusal('epoch_parameters_upgrade_required')
        if generation is None:generation=self.meta('canonical_ready')
        if generation is None:raise Refusal('no_ready_generation')
        # Every mutation needs fresh status, not the potentially large manifest.
        row=self.db.execute('SELECT status FROM chain.generations WHERE id=%s',(generation,)).fetchone()
        if row is None:raise Refusal('generation_missing')
        if row[0]!='pending' or self.meta('source_conflict'):raise Refusal('immutable_generation')
    def fact(self,table,key_values):
        if table not in TABLES:raise Refusal('unknown_fact_table')
        if isinstance(key_values,dict):key_values=tuple(key_values[k] for k in KEYS[table])
        where=' AND '.join(k+'=%s' for k in KEYS[table]);columns=self.physical_tables[table]
        row=self.db.execute('SELECT '+','.join(columns)+' FROM chain.'+table+' WHERE '+where,tuple(key_values)).fetchone()
        return logical_row(table,dict(zip(columns,row))) if row else None
    def insert(self,table,row,generation):
        """Store one fact; inputs and output assets also need their ledger `ordinal` (0..4095)."""
        self.require_pending(generation)
        if table not in TABLES:raise Refusal('unknown_fact_table')
        validate_numeric(table,row)
        if table=='tx_metadata':metadata_json(row)
        ordinal=()
        if table in ORDINAL_TABLES:
            value=row.get('ordinal')
            if type(value) is not int or not 0<=value<ORDINAL_LIMIT:raise Refusal('invalid_ordinal')
            ordinal=(value,)
        expected={c:bytes(row[c]) if isinstance(row[c],memoryview) else row[c] for c in TABLES[table]}
        old=self.fact(table,row)
        if old is not None:
            if old!=expected or (ordinal and self.ordinal(table,row)!=ordinal[0]):raise Refusal('immutable_fact_conflict')
            return False
        if table=='epoch_stakes':self.db.execute('INSERT INTO chain.pools(pool_hash) VALUES(%s) ON CONFLICT DO NOTHING',(row['pool_hash'],))
        columns=TABLES[table]+(('ordinal',) if ordinal else ())
        self.db.execute('INSERT INTO chain.'+table+'('+','.join(columns)+',first_generation) VALUES('+','.join(['%s']*(len(columns)+1))+')',(*bind_row(table,row),*ordinal,generation))
        return True
    def ordinal(self,table,key_values):
        """The stored ledger ordinal of an input or output asset (None if the fact is not stored)."""
        if table not in ORDINAL_TABLES:raise Refusal('unknown_fact_table')
        if isinstance(key_values,dict):key_values=tuple(key_values[k] for k in KEYS[table])
        row=self.db.execute('SELECT ordinal FROM chain.'+table+' WHERE '+' AND '.join(k+'=%s' for k in KEYS[table]),tuple(key_values)).fetchone()
        return row[0] if row else None
    @property
    def supports_incremental(self):return self.state()['schema_version']>=2
    @property
    def serves_directly(self):
        """Schema 3: the node reads the served tables; every publication keeps them current."""
        return self.state()['schema_version']>=3
    @property
    def supports_checkpoints(self):
        """Schema 4: verification starts at the last integrity checkpoint."""
        return self.state()['schema_version']>=4
    def certify_checkpoint(self):
        from db_sync_alt.store.incremental import certify_checkpoint
        return certify_checkpoint(self)
    def verified_checkpoint(self):
        from db_sync_alt.store.incremental import verified_checkpoint
        return verified_checkpoint(self)
    def write_checkpoint(self,capability):
        from db_sync_alt.store.incremental import write_checkpoint
        return write_checkpoint(self,capability)
    def integrity(self):
        """The integrity base and when its checkpoint was written (None: the base has no checkpoint)."""
        from db_sync_alt.store.incremental import integrity_base
        base,checkpoint=integrity_base(self)
        return {'base':base,'checkpointed_at':None if checkpoint is None else checkpoint['checkpointed_at'].timestamp()}
    def current_verified_parent(self):
        from db_sync_alt.store.incremental import current_verified_parent
        return current_verified_parent(self)
    def certify_ready_full(self):
        from db_sync_alt.store.incremental import certify_ready_full
        return certify_ready_full(self)
    def begin_continuation(self,**kwargs):
        from db_sync_alt.store.incremental import begin_continuation
        return begin_continuation(self,**kwargs)
    def resume_continuation(self,generation,parent_capability=None):
        from db_sync_alt.store.incremental import resume_continuation
        return resume_continuation(self,generation,parent_capability)
    def stage_metadata_reuse(self,generation,through_height,source_receipt):
        from db_sync_alt.store.incremental import stage_metadata_reuse
        return stage_metadata_reuse(self,generation,through_height,source_receipt)
    def rebind_pending_witness(self,generation):
        from db_sync_alt.store.incremental import rebind_pending_witness
        return rebind_pending_witness(self,generation)
    def verify_pending_incremental(self,generation):
        from db_sync_alt.store.incremental import verify_pending_incremental
        return verify_pending_incremental(self,generation)
    def finalize_pending_incremental(self,generation,manifest,receipt,ticket):
        from db_sync_alt.store.incremental import finalize_pending_incremental
        return finalize_pending_incremental(self,generation,manifest,receipt,ticket)
    def publish_incremental(self,generation,ticket,*,stop_requested=None):
        from db_sync_alt.store.incremental import publish_incremental
        return publish_incremental(self,generation,ticket,stop_requested=stop_requested)
    def membership_sql(self,generation):
        parts=self.membership_parts(generation)
        return ' UNION ALL '.join(query for query,_ in parts),tuple(arg for _,args in parts for arg in args)
    def membership_parts(self,generation):
        """`generation`'s block bindings as (query, args) parts, each of height and hash; their UNION ALL is
        `membership_sql`. PostgreSQL does not flatten a UNION ALL member with a WHERE clause, so a statement
        that must reach a part's height or hash index uses the parts one by one."""
        if self.supports_incremental:
            current=self.state()['canonical_ready']
            if generation==current:return [('SELECT height,hash FROM chain.current_membership',())]
            reuse=self.db.execute('SELECT through_height FROM chain.generation_reuse WHERE generation=%s',(generation,)).fetchone()
            if reuse and self.generation(generation)['parent']==current:
                return [('SELECT height,hash FROM chain.current_membership WHERE height<=%s',(reuse[0],)),
                        ('SELECT height,hash FROM chain.membership_delta WHERE generation=%s',(generation,))]
            return [('SELECT height,hash FROM chain.membership(%s)',(generation,))]
        return [('''WITH RECURSIVE lineage(id,ceiling) AS (
          SELECT id,witness_height FROM chain.generations WHERE id=%s UNION ALL
          SELECT p.id,p.target_height FROM lineage l JOIN chain.generations c ON c.id=l.id JOIN chain.generations p ON p.id=c.parent
        ) SELECT gb.height,gb.hash FROM lineage l JOIN chain.generation_blocks gb ON gb.generation=l.id AND gb.height<=l.ceiling''',(generation,))]
    def iter_block_membership(self,generation):
        with self.db.cursor(name='membership_'+uuid.uuid4().hex,withhold=True) as cur:
            cur.execute('SELECT height,hash FROM chain.membership(%s) ORDER BY height',(generation,))
            count=0
            for height,h in cur:
                count+=1
                if count%10000==0:self.progress('membership',generation=generation,rows=count)
                yield {'height':height,'hash':bytes(h)}
            self.progress('membership_complete',generation=generation,rows=count)
    def block_at(self,generation,height):
        membership,args=self.membership_sql(generation)
        rows=self.db.execute('SELECT '+','.join('b.'+c for c in TABLES['blocks'])+' FROM ('+membership+') m JOIN chain.blocks b ON b.hash=m.hash WHERE m.height=%s',(*args,height)).fetchall()
        if len(rows)>1:raise Refusal('conflicting_inherited_membership')
        return logical_row('blocks',dict(zip(TABLES['blocks'],rows[0]))) if rows else None
    def has_block_hash(self,generation,h):
        row=self.db.execute('SELECT height FROM chain.blocks WHERE hash=%s',(h,)).fetchone()
        canonical=self.block_at(generation,row[0]) if row else None
        return canonical is not None and canonical['hash']==h
    def bind_block(self,generation,height,h):
        self.require_pending(generation)
        old=self.block_at(generation,height)
        if old:
            if old['hash']!=h:raise Refusal('canonical_block_conflict')
            return False
        table='membership_delta' if self.supports_incremental and self.db.execute('SELECT 1 FROM chain.generation_reuse WHERE generation=%s',(generation,)).fetchone() else 'generation_blocks'
        self.db.execute('INSERT INTO chain.'+table+' VALUES(%s,%s,%s)',(generation,height,h));return True
    def insert_blocks(self,generation,rows):
        """`insert` + `bind_block` for many headers at once, with the same refusals.

        An existing header must be identical (`immutable_fact_conflict`); a height the
        generation already has must hold the same hash (`canonical_block_conflict`).
        Returns the number of headers newly stored.
        """
        self.require_pending(generation)
        columns=TABLES['blocks'];unique={}
        for row in rows:
            validate_numeric('blocks',row)
            expected={c:bytes(row[c]) if isinstance(row[c],memoryview) else row[c] for c in columns}
            if unique.setdefault(expected['hash'],expected)!=expected:raise Refusal('immutable_fact_conflict')
        if not unique:return 0
        stored={}
        for raw in self.db.execute('SELECT '+','.join(columns)+' FROM chain.blocks WHERE hash=ANY(%s)',(list(unique),)):
            old=logical_row('blocks',dict(zip(columns,raw)));stored[old['hash']]=old
        for h,expected in unique.items():
            if h in stored and stored[h]!=expected:raise Refusal('immutable_fact_conflict')
        new=[expected for h,expected in unique.items() if h not in stored]
        if new:
            with self.db.cursor() as cursor:
                cursor.executemany('INSERT INTO chain.blocks('+','.join(columns)+',first_generation) VALUES('+','.join(['%s']*(len(columns)+1))+')',
                                   [(*bind_row('blocks',row),generation) for row in new])
        heights={}
        for expected in unique.values():
            if heights.setdefault(expected['height'],expected['hash'])!=expected['hash']:raise Refusal('canonical_block_conflict')
        membership,args=self.membership_sql(generation)
        bound={}
        for height,h in self.db.execute('SELECT m.height,m.hash FROM ('+membership+') m WHERE m.height=ANY(%s)',(*args,list(heights))):
            if height in bound:raise Refusal('conflicting_inherited_membership')
            bound[height]=bytes(h)
        for height,h in bound.items():
            if heights[height]!=h:raise Refusal('canonical_block_conflict')
        table='membership_delta' if self.supports_incremental and self.db.execute('SELECT 1 FROM chain.generation_reuse WHERE generation=%s',(generation,)).fetchone() else 'generation_blocks'
        bindings=[(generation,height,h) for height,h in heights.items() if height not in bound]
        if bindings:
            with self.db.cursor() as cursor:
                cursor.executemany('INSERT INTO chain.'+table+' VALUES(%s,%s,%s)',bindings)
        return len(new)
    def checkpoint(self,generation,phase,unit,receipt):
        self.require_pending(generation)
        self.db.execute('INSERT INTO chain.checkpoints VALUES(%s,%s,%s,%s)',(generation,phase,str(unit),Jsonb(receipt)))
    def insert_relevance_anchor(self,receipt):
        """Exact ready metadata gate; caller holds one existing exclusive transaction.

        No startup DDL and no trigger disabling. Unknown guard definitions refuse.
        The only installed change is a pinned INSERT-only anchor branch.
        """
        from db_sync_alt.store.incremental import _session
        from db_sync_alt.sources.blockfrost.relevance import validate_anchor
        _session(self)
        if self.db.info.transaction_status!=psycopg.pq.TransactionStatus.INTRANS:raise Refusal('relevance_anchor_transaction_required')
        state=self.state();g=self.generation()
        if state['schema_version']!=SCHEMA_VERSION or state['migration_state']!='ready' or state['revoked'] or self.pending() is not None:raise Refusal('relevance_parent_changed')
        validate_anchor(self,g,[{'receipt':receipt}])
        if self.completed(g['id'],'relevance','anchor'):raise Refusal('relevance_trust_already_initialized')
        old=Path(__file__).with_name('schema_v2.sql').read_text()
        old=old[old.index('CREATE FUNCTION chain.guard_immutable()'):].split('END $$;',1)[0].split('AS $$',1)[1]
        replacement=relevance_anchor_sql(state['schema_version'])
        successor=replacement.split('AS $$',1)[1].split('END $$;',1)[0]
        actual=self.db.execute("SELECT prosrc FROM pg_proc WHERE oid='chain.guard_immutable()'::regprocedure").fetchone()[0]
        if actual not in (old+'END ',successor+'END '):raise Refusal('relevance_anchor_guard_mismatch')
        if actual!=successor+'END ':self.db.execute(replacement)
        self.db.execute('SELECT set_config(%s,%s,true)',('db_sync_alt.relevance_anchor_write',canonical(receipt)))
        try:self.db.execute("INSERT INTO chain.checkpoints VALUES(%s,'relevance','anchor',%s)",(g['id'],Jsonb(receipt)))
        finally:
            if self.db.info.transaction_status==psycopg.pq.TransactionStatus.INTRANS:
                self.db.execute('SELECT set_config(%s,%s,true)',('db_sync_alt.relevance_anchor_write','null'))
    def finalize_pending(self,generation,manifest,receipt,computed_digests):
        from db_sync_alt.verify.completion import prepare_finalization
        self.require_pending(generation)
        final,latest=prepare_finalization(self,generation,manifest,receipt,computed_digests)
        with self.atomic():
            self.require_pending(generation)
            if latest is None:
                self.checkpoint(generation,'final-anchors','complete',receipt)
                self.db.execute('UPDATE chain.generations SET manifest=%s WHERE id=%s',(Jsonb(final),generation))
            else:
                self.db.execute("INSERT INTO chain.checkpoints VALUES(%s,'completion-revalidation','latest',%s) ON CONFLICT(generation,phase,unit) DO UPDATE SET receipt=excluded.receipt",(generation,Jsonb(latest)))
        return final
    def completed(self,generation,phase,unit):return self.db.execute('SELECT 1 FROM chain.checkpoints WHERE generation=%s AND phase=%s AND unit=%s',(generation,phase,str(unit))).fetchone() is not None
    def checkpoint_rows(self,generation=None,phase=None,before=None,unit=None):
        where=[];args=[]
        for key,value,op in [('generation',generation,'='),('phase',phase,'='),('generation',before,'<'),('unit',None if unit is None else str(unit),'=')]:
            if value is not None:where.append(key+op+'%s');args.append(value)
        cur=self.db.execute('SELECT generation,phase,unit,receipt FROM chain.checkpoints'+(' WHERE '+' AND '.join(where) if where else '')+' ORDER BY generation,phase,unit',args)
        return [dict(zip(('generation','phase','unit','receipt'),r)) for r in cur]
    def mark_conflict(self):
        with self.atomic():
            self.db.execute("SET LOCAL lock_timeout='5s'")
            self.db.execute('SELECT pg_advisory_xact_lock(%s,%s)',SERVING_LOCK)
            self.db.execute('UPDATE chain.service_state SET revoked=true,canonical_ready=NULL,publication_serial=publication_serial+1 WHERE id=1')
            self.db.execute("UPDATE chain.generations SET status='source_conflict' WHERE status='pending'")
    def publish(self,generation,manifest):
        from db_sync_alt.verify.store import verify_store,validate_publication,validate_prior_answers
        self.require_pending(generation)
        from db_sync_alt.verify.completion import validate_completion,completion_required
        g=self.generation(generation)
        required=completion_required(self,g)
        validate_completion(self,g,require_seal=required,manifest=manifest)
        if not required:
            self.db.execute('UPDATE chain.generations SET manifest=%s WHERE id=%s',(Jsonb(manifest),generation))
        if self.generation(generation)['manifest'].get('integrity',{}).get('version')==2:raise Refusal('incremental_ticket_required')
        report=verify_store(self,generation,require_ready=False)
        validate_publication(self,self.generation(generation),report['digests'])
        try:validate_prior_answers(self,generation)
        except Refusal:
            self.mark_conflict();raise
        parent=self.generation(generation)['parent']
        if self.supports_incremental:
            # A fully verified child inherits the committed parent prefix plus
            # only its own bindings. Verify the index before reusing that prefix;
            # reuse/delta rows belong to incremental publication only.
            if parent is None or self.state()['canonical_ready']!=parent:
                raise Refusal('publication_parent_changed')
            if self.db.execute('SELECT projection_generation FROM chain.integrity_state WHERE id=1').fetchone()!=(parent,):
                raise Refusal('membership_projection_mismatch')
            if (self.db.execute('SELECT 1 FROM chain.generation_reuse WHERE generation=%s',(generation,)).fetchone()
                    or self.db.execute('SELECT 1 FROM chain.membership_delta WHERE generation=%s',(generation,)).fetchone()):
                raise Refusal('full_publication_shape_invalid')
            from db_sync_alt.verify.store import verify_projection
            verify_projection(self,parent)
            parent_target=self.generation(parent)['target_height']
        with self.atomic():
            self.db.execute("SET LOCAL lock_timeout='5s'")
            self.db.execute('SELECT pg_advisory_xact_lock(%s,%s)',SERVING_LOCK)
            self.require_pending(generation)
            if self.meta('canonical_ready')!=parent:raise Refusal('publication_parent_changed')
            if self.supports_incremental:
                self.db.execute('SELECT set_config(%s,%s,true)',(PROJECTION_WRITE,'on'))
                self.db.execute('DELETE FROM chain.current_membership WHERE height>%s',(parent_target,))
                self.db.execute('INSERT INTO chain.current_membership SELECT height,hash FROM chain.generation_blocks WHERE generation=%s',(generation,))
                self.db.execute('UPDATE chain.integrity_state SET projection_generation=%s WHERE id=1',(generation,))
                if self.serves_directly:
                    from db_sync_alt.store import serve
                    serve.advance(self.db,generation,cutoff=parent_target,bindings='SELECT height,hash FROM chain.generation_blocks WHERE generation=%s',bindings_args=(generation,))
            self.db.execute("UPDATE chain.generations SET status='ready' WHERE id=%s",(generation,))
            self.db.execute("UPDATE chain.service_state SET canonical_ready=%s,migration_state='ready',publication_serial=publication_serial+1 WHERE id=1",(generation,))
        return report
    def rows(self,table,generation,*,batch=1000):
        if table not in TABLES:raise Refusal('unknown_fact_table')
        self.generation(generation)
        if self.db.execute("SELECT 1 FROM chain.generations WHERE id<%s AND status!='ready'",(generation,)).fetchone():raise Refusal('invalid_lineage')
        where='first_generation<=%s';args=[generation]
        if table=='blocks':where+=' AND hash IN(SELECT hash FROM chain.membership(%s))';args.append(generation)
        columns=self.sealed_tables(generation)[table]
        query='SELECT '+','.join(columns)+' FROM chain.'+table+' WHERE '+where+' ORDER BY '+','.join(KEYS[table])
        # Named cursors bound memory; transaction scope belongs to this iterator.
        from db_sync_alt.verify.limits import verification
        with verification(self.db,query,args):
            with self.db.cursor(name='rows_'+uuid.uuid4().hex) as cur:
                cur.itersize=batch;cur.execute(query,args)
                for row in cur:yield logical_row(table,dict(zip(columns,row)))
    def digests(self,generation):
        result={};legacy=self.legacy_generation(generation)
        for table in self.sealed_tables(generation):
            hasher=hashlib.sha256();count=0
            for row in self.rows(table,generation):
                validate_numeric(table,row,legacy=legacy);hasher.update((canonical(jsonable(row))+'\n').encode());count+=1
                if count%10000==0:self.progress('digest',generation=generation,family=table,rows=count)
            result[table]={'count':count,'sha256':hasher.hexdigest()}
            self.progress('digest_family_complete',generation=generation,family=table,rows=count)
        return result
    def lifecycle_state(self):
        """Fresh startup state without materializing historical manifests.

        Keep the diagnostic inspection's state/relation/access refusal order:
        one scalar ID establishes whether its per-generation SELECT privileges
        are needed, LIMIT 0 checks those privileges without transferring a row.
        Size/checkpoint access is still checked, without counting history.
        Full history diagnostics and full verification remain separate.
        """
        state=self.state()
        if self.db.execute('SELECT id FROM chain.generations ORDER BY id LIMIT 1').fetchone() is not None:
            self.db.execute('SELECT * FROM chain.generations LIMIT 0')
        self.db.execute('SELECT pg_database_size(current_database())').fetchone()
        self.db.execute('SELECT 1 FROM chain.checkpoints LIMIT 0')
        return state
    def status(self,checkpoint=None):
        """Database state plus one summary row per generation; manifests stay in PostgreSQL.

        `checkpoint` is the node's bridge checkpoint as the operator states it (bridge.parse_checkpoint);
        the `bridge` section reports the metadata coverage start and whether that checkpoint is supported.

        Generation columns beyond id are read only when history exists, so status of an empty
        database needs only SELECT(id) on chain.generations.
        """
        state=self.state();generations=[]
        if self.db.execute('SELECT id FROM chain.generations ORDER BY id LIMIT 1').fetchone() is not None:
            generations=[dict(zip(GENERATION_SUMMARY,r)) for r in self.db.execute('SELECT '+','.join(GENERATION_SUMMARY)+' FROM chain.generations ORDER BY id')]
        legacy=self.legacy_generation(None)
        result={'format':LEGACY_FORMAT if legacy else FORMAT,**state,'generations':generations,'bytes':self.database_bytes(),'checkpoints':self.db.execute('SELECT count(*) FROM chain.checkpoints').fetchone()[0]}
        if legacy:
            # `db-sync-alt upgrade` reads every stored epoch's parameters twice.
            epochs=self.db.execute('SELECT count(*) FROM chain.epoch_nonces').fetchone()[0]
            result['epoch_parameters_upgrade']={'required':True,'stored_epochs':epochs,'provider_requests':2*epochs}
        if state['schema_version']>=4 and state['canonical_ready'] is not None:
            from db_sync_alt.sources.blockfrost.relevance import history_status
            result['relevance']={'history':history_status(self)}
        if not legacy and 'tx_metadata' in self.physical_tables and state['schema_version']>=SCHEMA_VERSION and state['canonical_ready'] is not None and not state['revoked']:
            from db_sync_alt.bridge import report
            result['bridge']=report(self,checkpoint)
        return result
    def database_bytes(self):return self.db.execute('SELECT pg_database_size(current_database())').fetchone()[0]
    def sync_job(self,job_id):
        row=self.db.execute('SELECT body FROM chain.sync_jobs WHERE id=%s',(job_id,)).fetchone()
        return row[0] if row else None
    def save_sync_job(self,job_id,body):
        self.require_writer()
        if self.db.info.transaction_status!=psycopg.pq.TransactionStatus.IDLE:raise Refusal('sync_job_requires_durable_autocommit')
        if not isinstance(job_id,str) or not 1<=len(job_id)<=256 or not isinstance(body,dict) or len(canonical(body).encode())>65536:raise Refusal('invalid_sync_job')
        if (not isinstance(body.get('job_id'),str) or not 1<=len(body['job_id'])<=128
            or type(body.get('attempts')) is not int or type(body.get('budget')) is not int
            or not 0<=body['attempts']<=body['budget']<=10000 or body['budget']==0
            or body.get('state') not in ('active','completed','exhausted')):raise Refusal('invalid_sync_job')
        old=self.sync_job(job_id)
        if old:
            if old.get('job_id')!=body['job_id']:
                # Only a completed job, or an exhausted update kept unchanged under its own key, is replaced.
                if old.get('state')!='completed' and not (continues(old,body) and self.sync_job(ARCHIVE+old['job_id'])==old):
                    raise Refusal('sync_job_rollover_refused')
            elif body['attempts']<old['attempts'] or body['budget']!=old['budget']:raise Refusal('sync_job_budget_reset')
            elif old.get('state') in ('completed','exhausted') and body['state']=='active':raise Refusal('sync_job_reopen_refused')
        self.db.execute('INSERT INTO chain.sync_jobs VALUES(%s,%s) ON CONFLICT(id) DO UPDATE SET body=excluded.body',(job_id,Jsonb(body)))
    def check_space(self):
        if self.reserve_check:self.reserve_check()
    def progress(self,phase,**fields):
        if self.progress_callback:self.progress_callback(phase,**fields)
    def _facts_where(self,table,where,args):
        columns=self.physical_tables[table]
        return [logical_row(table,dict(zip(columns,r))) for r in self.db.execute('SELECT '+','.join(columns)+' FROM chain.'+table+' WHERE '+where,args)]
    def output_assets(self,tx_hash,output_index,generation=None):return self._facts_where('output_assets','tx_hash=%s AND output_index=%s'+(' AND first_generation<=%s' if generation is not None else ''),(tx_hash,output_index,*(() if generation is None else (generation,))))
    def transaction_outputs(self,tx_hash,generation):return self._facts_where('outputs','tx_hash=%s AND first_generation<=%s',(tx_hash,generation))
    def transaction_inputs(self,tx_hash,generation):return self._facts_where('inputs','tx_hash=%s AND first_generation<=%s',(tx_hash,generation))
    def transaction_mints(self,tx_hash,generation):return self._facts_where('mints','tx_hash=%s AND first_generation<=%s',(tx_hash,generation))
    def recent_transactions(self,generation,limit=8):
        g=self.generation(generation)
        return [dict(zip(('hash','block_hash','tx_index','valid'),r)) for r in self.db.execute('SELECT t.hash,t.block_hash,t.tx_index,t.valid FROM chain.transactions t JOIN chain.blocks b ON b.hash=t.block_hash WHERE t.first_generation<=%s AND b.height<=%s ORDER BY b.height DESC,t.tx_index DESC LIMIT %s',(generation,g['target_height'],limit))]
    def epoch_nonce(self,epoch,generation):
        rows=self._facts_where('epoch_nonces','epoch=%s AND first_generation<=%s',(epoch,generation));return rows[0] if rows else None
    def epoch_stake_total(self,epoch,generation):return int(self.db.execute('SELECT COALESCE(sum(amount),0) FROM chain.epoch_stakes WHERE epoch=%s AND first_generation<=%s',(epoch,generation)).fetchone()[0])
    def bridge_covered(self,generation):
        """Whether `generation` was acquired with bridge metadata (its sealed capability)."""
        row=self.db.execute("SELECT (manifest->'capabilities'->%s)='true'::jsonb FROM chain.generations WHERE id=%s",(BRIDGE_CAPABILITY,generation)).fetchone()
        if row is None:raise Refusal('generation_missing')
        return row[0] is True
    def output_generation(self,tx_hash,output_index):
        """The generation that first stored an output (None if it is not stored)."""
        row=self.db.execute('SELECT first_generation FROM chain.outputs WHERE tx_hash=%s AND output_index=%s',(tx_hash,output_index)).fetchone()
        return None if row is None else row[0]
    def last_metadata_height(self,generation):
        if self.supports_incremental and self.db.execute('SELECT 1 FROM chain.generation_reuse WHERE generation=%s',(generation,)).fetchone():
            # Each part's max() reads the top of its height index; one max() over their UNION ALL reads every inherited height.
            parts=self.membership_parts(generation)
            return self.db.execute('SELECT greatest('+','.join('(SELECT max(height) FROM ('+query+') m)' for query,_ in parts)+')',
                                   tuple(arg for _,args in parts for arg in args)).fetchone()[0]
        return self.db.execute('SELECT max(height) FROM chain.generation_blocks WHERE generation=%s',(generation,)).fetchone()[0]
    def last_event_height(self,generation):return self.db.execute("SELECT max(unit::bigint) FROM chain.checkpoints WHERE generation=%s AND phase='events' AND unit~'^[0-9]+$'",(generation,)).fetchone()[0]


def inspect(service=None,*,conninfo=''):
    with PgStore(service,conninfo=conninfo,readonly=True,allow_incomplete=True,allow_legacy=True) as store:return store.status()
def inspect_lifecycle(service=None,*,conninfo=''):
    with PgStore(service,conninfo=conninfo,readonly=True,allow_incomplete=True) as store:return store.lifecycle_state()
