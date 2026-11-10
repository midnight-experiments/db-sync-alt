"""Fixed, parameterized queries for supported full-history core db-sync sources. Every session is read-only.

The source must report the profile's `meta.network_name`; k comes from the profile."""
import contextlib
import datetime
import re
from db_sync_alt.contract import Refusal, canonical, required_epochs, epoch_size, generation_capabilities
from db_sync_alt.sources.dbsync.compatibility import SOURCE_COLUMNS, source_version, validate_columns, validate_modes, validate_attestation

BLOCK_COLUMNS='b.hash,b.block_no AS height,p.hash AS parent_hash,b.epoch_no AS epoch,b.slot_no AS slot,b.time,b.tx_count'
# Position of an input among all ordinary inputs of its spending transaction, and of an output
# asset among all assets of its output: db-sync's row order, which is ledger order.
INPUT_ORDINAL='(SELECT count(*) FROM tx_in j WHERE j.tx_in_id=i.tx_in_id AND j.id<i.id)::integer'
ASSET_ORDINAL='(SELECT count(*) FROM ma_tx_out k WHERE k.tx_out_id=m.tx_out_id AND k.id<m.id)::integer'

class SQLSource:
    def __init__(self,service,profile,attestation=None):
        if not isinstance(service,str) or not re.fullmatch(r'[A-Za-z0-9_.-]{1,80}',service):raise Refusal('invalid_service_name')
        self.service=service;self.profile=profile;self.attestation=attestation;self.conn=None;self.serial=0;self._asset_ids=None
        self.units=sorted(profile.units);self.policies=[bytes.fromhex(p) for p in sorted({u[:56] for u in profile.units})]
        self.governance=[bytes.fromhex(p) for p in sorted(profile.governance)]
        self.addresses=sorted(profile.addresses)
    @contextlib.contextmanager
    def snapshot(self):
        import psycopg
        from psycopg.rows import dict_row
        if self.conn is not None:raise Refusal('nested_snapshot')
        try:
            self._asset_ids=None
            self.conn=psycopg.connect(service=self.service,connect_timeout=10,autocommit=True,row_factory=dict_row,
                    application_name='db-sync-alt-reader',options='-c default_transaction_read_only=on')
            self.conn.execute('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY')
            self.conn.execute("SET LOCAL statement_timeout='15s'")
            self.conn.execute("SET LOCAL jit=off")
            self.conn.execute("SET LOCAL lock_timeout='1s'")
            self.conn.execute("SET LOCAL idle_in_transaction_session_timeout='60s'")
            if self.one("SELECT current_setting('transaction_read_only') AS read_only")['read_only']!='on':raise Refusal('source_not_read_only')
            yield {'read_only':True}
        except psycopg.errors.QueryCanceled:raise Refusal('source_timeout') from None
        except psycopg.Error:raise Refusal('source_query_failed') from None
        finally:
            if self.conn is not None:
                self.conn.close();self.conn=None
    def one(self,query,params=()):
        r=self.conn.execute(query,params).fetchone()
        if r is None:raise Refusal('source_row_missing')
        return dict(r)
    def stream(self,query,params=(),batch=500):
        self.serial+=1
        with self.conn.cursor(name='read_'+str(self.serial)) as cur:
            cur.execute(query,params)
            while True:
                rows=cur.fetchmany(batch)
                if not rows:return
                for row in rows:yield dict(row)
    def asset_predicate(self,alias='a'):
        return f"({alias}.policy=ANY(%s::bytea[]) AND (encode({alias}.policy,'hex')||encode({alias}.name,'hex')=ANY(%s::text[]) OR {alias}.policy=ANY(%s::bytea[])))"
    def asset_params(self):return (self.policies,self.units,self.governance)
    def output_predicate(self):
        return '(o.address=ANY(%s::text[]) OR EXISTS(SELECT 1 FROM ma_tx_out ma WHERE ma.tx_out_id=o.id AND ma.ident=ANY(%s::bigint[]) OFFSET 0))'
    def output_params(self):
        if self._asset_ids is None:self._asset_ids=[r['id'] for r in self.stream('SELECT a.id FROM multi_asset a WHERE '+self.asset_predicate(),self.asset_params())]
        return (self.addresses,self._asset_ids)
    def anchor(self,height):
        """The block at `height`; `source_id` is the source's row id, used only inside the source binding."""
        r=self.one('SELECT id AS source_id,hash,block_no AS height,epoch_no AS epoch,tx_count FROM block WHERE block_no=%s',(height,))
        return {**r,'hash':bytes(r['hash']).hex()}
    def namespace(self,anchors):
        """The source's row ids of the start and target anchors (deep, immutable blocks)."""
        return {name:self.anchor(anchors[name]['height'])['source_id'] for name in ('start','target')}
    def check_anchors(self,manifest):
        for old in manifest['anchors'].values():
            if old['hash']!=self.anchor(old['height'])['hash']:raise Refusal('source_anchor_conflict')
    def check_source(self):
        row=self.one('SELECT stage_one,stage_two,stage_three FROM schema_version')
        schema=tuple(row[k] for k in ('stage_one','stage_two','stage_three'))
        version=source_version(schema)
        if self.one('SELECT network_name FROM meta')['network_name']!=self.profile.network.dbsync_network_name:raise Refusal('source_network_mismatch')
        rows=self.stream("SELECT a.table_name,a.column_name,a.udt_name,a.domain_name,a.is_nullable,c.relkind "
                         "FROM information_schema.columns a JOIN pg_namespace n ON n.nspname=a.table_schema "
                         "JOIN pg_class c ON c.relnamespace=n.oid AND c.relname=a.table_name "
                         "WHERE a.table_schema='public' AND a.table_name=ANY(%s::text[])",(list(SOURCE_COLUMNS),))
        validate_columns(rows)
        validate_modes(r['token'] for r in self.stream('SELECT token FROM extra_migrations'))
        validate_attestation(self.attestation,version)
        self.validated_identity={'network':self.profile.network.network,'schema':list(schema),'dbsync_version':version}
        return schema,version
    def check_epoch_coverage(self,epochs):
        missing=self.one("SELECT count(*) AS n FROM unnest(%s::bigint[]) e(epoch) WHERE NOT EXISTS "
                         "(SELECT 1 FROM epoch_stake_progress p WHERE p.epoch_no=e.epoch AND p.completed IS TRUE)",(epochs,))['n']
        if missing:raise Refusal('epoch_stake_not_complete')
        missing=self.one("SELECT count(*) AS n FROM unnest(%s::bigint[]) e(epoch) WHERE NOT EXISTS "
                         "(SELECT 1 FROM epoch_param p WHERE p.epoch_no=e.epoch AND p.nonce IS NOT NULL)",(epochs,))['n']
        if missing:raise Refusal('epoch_nonce_unavailable')
        for epoch in epochs:self.epoch_parameters(epoch)
        missing=self.one("SELECT count(*) AS n FROM unnest(%s::bigint[]) e(epoch) WHERE NOT EXISTS "
                         "(SELECT 1 FROM epoch_stake p WHERE p.epoch_no=e.epoch)",(epochs,))['n']
        if missing:raise Refusal('epoch_stake_unavailable')
    def preflight(self,end=None):
        schema,version=self.check_source()
        head=self.one('SELECT block_no FROM block WHERE block_no IS NOT NULL ORDER BY block_no DESC LIMIT 1')['block_no']
        k=self.profile.security
        target=head-k if end is None else end
        if target<self.profile.start[0] or target+k>head:raise Refusal('unconfirmed_target')
        anchors={'start':self.anchor(self.profile.start[0]),'target':self.anchor(target),'witness':self.anchor(target+k)}
        # Only what verification and provenance need; nothing that identifies the source host.
        epochs=required_epochs(anchors['start']['epoch'],anchors['target']['epoch'])
        self.check_epoch_coverage(epochs)
        identity={'network':self.profile.network.network,'schema':list(schema),'dbsync_version':version,
                  'retention':{'tx_in':True,'epoch_stake':True}}
        return {'identity':self.profile.identity,'anchors':anchors,'source_identity':identity,
                'required_epochs':epochs,
                'capabilities':generation_capabilities(),'sampled_head':head}
    def ordinals(self,inputs,output_assets,anchors):
        """Ledger ordinals of already stored facts: `inputs` as (tx, origin tx, origin index) and
        `output_assets` as (tx, output index, policy, name) keys; every key must be found."""
        found={'inputs':{},'output_assets':{}}
        with self.snapshot():
            self.check_source()
            self.check_anchors({'anchors':anchors})
            for start in range(0,len(inputs),5000):
                part=inputs[start:start+5000]
                for r in self.stream('WITH s(tx_hash,origin_hash,origin_index) AS (SELECT * FROM unnest(%s::bytea[],%s::bytea[],%s::integer[])) '
                                     'SELECT s.tx_hash,s.origin_hash,s.origin_index,'+INPUT_ORDINAL+' AS ordinal FROM s JOIN tx t ON t.hash=s.tx_hash '
                                     'JOIN tx ot ON ot.hash=s.origin_hash JOIN tx_in i ON i.tx_in_id=t.id AND i.tx_out_id=ot.id AND i.tx_out_index=s.origin_index',
                                     ([k[0] for k in part],[k[1] for k in part],[k[2] for k in part])):
                    key=(bytes(r['tx_hash']),bytes(r['origin_hash']),r['origin_index'])
                    if found['inputs'].setdefault(key,r['ordinal'])!=r['ordinal']:raise Refusal('source_ordinal_conflict')
            for start in range(0,len(output_assets),5000):
                part=output_assets[start:start+5000]
                for r in self.stream('WITH s(tx_hash,output_index,policy,name) AS (SELECT * FROM unnest(%s::bytea[],%s::integer[],%s::bytea[],%s::bytea[])) '
                                     'SELECT s.tx_hash,s.output_index,s.policy,s.name,'+ASSET_ORDINAL+' AS ordinal FROM s JOIN tx t ON t.hash=s.tx_hash '
                                     'JOIN tx_out o ON o.tx_id=t.id AND o.index=s.output_index JOIN multi_asset a ON a.policy=s.policy AND a.name=s.name '
                                     'JOIN ma_tx_out m ON m.tx_out_id=o.id AND m.ident=a.id',
                                     ([k[0] for k in part],[k[1] for k in part],[k[2] for k in part],[k[3] for k in part])):
                    key=(bytes(r['tx_hash']),r['output_index'],bytes(r['policy']),bytes(r['name']))
                    if found['output_assets'].setdefault(key,r['ordinal'])!=r['ordinal']:raise Refusal('source_ordinal_conflict')
        return found
    @staticmethod
    def block_row(r):
        r=dict(r)
        if r['height'] is None:raise Refusal('dependency_without_height')
        r['time']=r['time'].replace(tzinfo=datetime.timezone.utc).isoformat(timespec='microseconds').replace('+00:00','Z')
        return {k:bytes(v) if isinstance(v,memoryview) else v for k,v in r.items()}
    def blocks(self,lo,hi):
        for r in self.stream('SELECT '+BLOCK_COLUMNS+' FROM block b LEFT JOIN block p ON p.id=b.previous_id WHERE b.block_no BETWEEN %s AND %s ORDER BY b.block_no',(lo,hi)):yield self.block_row(r)
    def select_ids(self,lo,hi,*,independent=False):
        """Discovery only; source IDs are not persisted as semantic identities."""
        ap=self.asset_predicate();op=self.output_predicate()
        if independent:
            # Independent indexed asset/address-led inventory, never seeded from destination IDs.
            cte='WITH selected AS MATERIALIZED (SELECT o.id FROM tx_out o WHERE o.address=ANY(%s::text[]) UNION SELECT ma.tx_out_id FROM multi_asset a JOIN ma_tx_out ma ON ma.ident=a.id WHERE '+ap+') '
            params=(self.addresses,*self.asset_params())
            oq=cte+'SELECT o.id FROM selected s JOIN tx_out o ON o.id=s.id JOIN tx t ON t.id=o.tx_id JOIN block b ON b.id=t.block_id WHERE b.block_no BETWEEN %s AND %s ORDER BY o.id'
            sq=cte+'SELECT i.id FROM selected s JOIN tx_out o ON o.id=s.id JOIN tx_in i ON i.tx_out_id=o.tx_id AND i.tx_out_index=o.index JOIN tx t ON t.id=i.tx_in_id JOIN block b ON b.id=t.block_id WHERE b.block_no BETWEEN %s AND %s ORDER BY i.id'
            opms=(*params,lo,hi)
        else:
            oq='SELECT o.id FROM block b JOIN tx t ON t.block_id=b.id JOIN tx_out o ON o.tx_id=t.id WHERE b.block_no BETWEEN %s AND %s AND '+op+' ORDER BY o.id'
            sq='SELECT i.id FROM block b JOIN tx t ON t.block_id=b.id JOIN tx_in i ON i.tx_in_id=t.id JOIN tx_out o ON o.tx_id=i.tx_out_id AND o.index=i.tx_out_index WHERE b.block_no BETWEEN %s AND %s AND '+op+' ORDER BY i.id'
            opms=(lo,hi,*self.output_params())
        # Sparse identities only, bounded by an explicit maximum; facts remain streamed.
        def ids(q,p):
            out=[]
            for r in self.stream(q,p):
                out.append(r['id'])
                if len(out)>100000:raise Refusal('work_unit_too_dense')
            return out
        outputs=ids(oq,opms);spends=ids(sq,opms)
        mq='SELECT DISTINCT t.id FROM multi_asset a JOIN ma_tx_mint m ON m.ident=a.id JOIN tx t ON t.id=m.tx_id JOIN block b ON b.id=t.block_id WHERE '+ap+' AND b.block_no BETWEEN %s AND %s ORDER BY t.id'
        mints=ids(mq,(*self.asset_params(),lo,hi))
        return outputs,spends,mints
    def facts(self,outputs,spends,mints):
        # Dependency closure expands only selected sparse IDs. Fixed parameter arrays, no dynamic values in SQL.
        allout=set(outputs);txids=set(mints)
        if spends:
            for r in self.stream('SELECT o.id,i.tx_in_id FROM tx_in i JOIN tx_out o ON o.tx_id=i.tx_out_id AND o.index=i.tx_out_index WHERE i.id=ANY(%s::bigint[])',(spends,)):allout.add(r['id']);txids.add(r['tx_in_id'])
        outs=sorted(allout)
        if outs:
            for r in self.stream('SELECT DISTINCT tx_id FROM tx_out WHERE id=ANY(%s::bigint[])',(outs,)):txids.add(r['tx_id'])
        txs=sorted(txids)
        if not txs:return
        for r in self.stream('SELECT DISTINCT '+BLOCK_COLUMNS+' FROM tx t JOIN block b ON b.id=t.block_id LEFT JOIN block p ON p.id=b.previous_id WHERE t.id=ANY(%s::bigint[])',(txs,)):yield 'blocks',self.block_row(r)
        # A phase-2-invalid transaction's tx_in and tx_out rows are its collateral inputs and return.
        for r in self.stream('SELECT t.hash,b.hash AS block_hash,t.block_index AS tx_index,t.valid_contract AS valid FROM tx t JOIN block b ON b.id=t.block_id WHERE t.id=ANY(%s::bigint[]) ORDER BY b.block_no,t.block_index',(txs,)):
            r['valid']=1 if r['valid'] is True else 0;yield 'transactions',r
        # A datum hash without a datum row is a preimage not on chain yet: the output keeps the hash.
        # An inline datum is always in the source, under the output's own hash.
        missing=self.one('SELECT count(*) AS n FROM tx_out o LEFT JOIN datum di ON di.id=o.inline_datum_id WHERE o.id=ANY(%s::bigint[]) AND o.inline_datum_id IS NOT NULL AND (di.id IS NULL OR di.hash IS DISTINCT FROM o.data_hash)',(outs,))['n']
        if missing:raise Refusal('missing_or_conflicting_datum')
        for r in self.stream('SELECT DISTINCT d.hash,d.value FROM tx_out o JOIN datum d ON d.hash=o.data_hash WHERE o.id=ANY(%s::bigint[])',(outs,)):
            r['value']=None if r['value'] is None else canonical(r['value']);yield 'datums',r
        for r in self.stream('SELECT t.hash AS tx_hash,o.index AS output_index,o.address,o.value::text AS value,o.data_hash AS datum_hash FROM tx_out o JOIN tx t ON t.id=o.tx_id WHERE o.id=ANY(%s::bigint[]) ORDER BY t.hash,o.index',(outs,)):yield 'outputs',r
        for r in self.stream('SELECT DISTINCT a.policy,a.name FROM multi_asset a WHERE '+self.asset_predicate()+' AND (EXISTS(SELECT 1 FROM ma_tx_out m WHERE m.ident=a.id AND m.tx_out_id=ANY(%s::bigint[])) OR EXISTS(SELECT 1 FROM ma_tx_mint m WHERE m.ident=a.id AND m.tx_id=ANY(%s::bigint[])))',(*self.asset_params(),outs,mints)):yield 'assets',r
        # Ordinals: db-sync's row order within the output / spending transaction, which is ledger order.
        for r in self.stream('SELECT t.hash AS tx_hash,o.index AS output_index,a.policy,a.name,m.quantity::text AS quantity,'+ASSET_ORDINAL+' AS ordinal FROM ma_tx_out m JOIN multi_asset a ON a.id=m.ident JOIN tx_out o ON o.id=m.tx_out_id JOIN tx t ON t.id=o.tx_id WHERE o.id=ANY(%s::bigint[]) AND '+self.asset_predicate(),(outs,*self.asset_params())):yield 'output_assets',r
        for r in self.stream('SELECT t.hash AS tx_hash,ot.hash AS origin_hash,i.tx_out_index AS origin_index,'+INPUT_ORDINAL+' AS ordinal FROM tx_in i JOIN tx t ON t.id=i.tx_in_id JOIN tx ot ON ot.id=i.tx_out_id WHERE i.id=ANY(%s::bigint[])',(spends,)):yield 'inputs',r
        for r in self.stream('SELECT t.hash AS tx_hash,a.policy,a.name,sum(m.quantity)::text AS quantity FROM ma_tx_mint m JOIN multi_asset a ON a.id=m.ident JOIN tx t ON t.id=m.tx_id WHERE t.id=ANY(%s::bigint[]) AND '+self.asset_predicate()+' GROUP BY t.hash,a.policy,a.name',(mints,*self.asset_params())):yield 'mints',r
    def epoch_parameters(self,epoch):
        """The epoch's nonce and max_tx_size from all of its epoch_param rows.

        The node reads MAX(max_tx_size) over the epoch's rows, so unequal valid sizes are allowed
        and their maximum is the fact. Every row is validated first: a NULL, negative or
        out-of-range size is never hidden behind a valid larger one. The nonce must be present
        and the same in every row."""
        row=self.one("SELECT count(*) AS n,count(nonce) AS nonces,count(DISTINCT nonce) AS distinct_nonces,"
                     "min(encode(nonce,'hex')) AS nonce,max(max_tx_size)::bigint AS max_tx_size,"
                     "coalesce(bool_or(max_tx_size IS NULL OR max_tx_size<0 OR max_tx_size>2147483647),false) AS invalid "
                     "FROM epoch_param WHERE epoch_no=%s",(epoch,))
        if not row['n']:raise Refusal('epoch_parameters_unavailable')
        if row['nonces']!=row['n'] or row['distinct_nonces']!=1:raise Refusal('epoch_nonce_unavailable')
        if row['invalid'] is not False:raise Refusal('invalid_epoch_max_tx_size')
        nonce=bytes.fromhex(row['nonce'])
        if len(nonce)!=32:raise Refusal('epoch_nonce_unavailable')
        return {'epoch':epoch,'nonce':nonce,'max_tx_size':epoch_size(row['max_tx_size'])}
    def epochs(self,epoch,target):
        if self.one('SELECT completed FROM epoch_stake_progress WHERE epoch_no=%s',(epoch,))['completed'] is not True:raise Refusal('epoch_stake_not_complete')
        yield 'epoch_nonces',self.epoch_parameters(epoch)
        count=0
        for r in self.stream('SELECT s.epoch_no AS epoch,p.hash_raw AS pool_hash,sum(s.amount)::text AS amount FROM epoch_stake s JOIN pool_hash p ON p.id=s.pool_id WHERE s.epoch_no=%s GROUP BY s.epoch_no,p.hash_raw ORDER BY p.hash_raw',(epoch,)):
            count+=1;yield 'epoch_stakes',r
        if not count:raise Refusal('epoch_stake_unavailable')
        # Exact historical epoch-end anchors only when completed, never current epoch's moving tip.
        end=self.one('SELECT max(block_no) AS height FROM block WHERE epoch_no=%s',(epoch,))['height']
        if end is None:raise Refusal('epoch_blocks_unavailable')
        if end<=target:
            for b in self.blocks(end,end):yield 'blocks',b
