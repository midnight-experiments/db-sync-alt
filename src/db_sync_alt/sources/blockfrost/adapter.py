"""Blockfrost adapter: fact extraction and the generation update.

OpenAPI contract pin: 1d4d5d8bac8f0c641eea5c6f7b1f3fbbed8d1505. The adapter identity
stored with each generation comes from the network profile.
Requests go through an accounted provider (`sources.blockfrost.sync.AccountedProvider`).
"""
import copy
import datetime as dt
import hashlib
import logging
import re
from urllib.parse import urlencode
from db_sync_alt.sources.blockfrost.cbor import decode_transaction, decode_datum
from db_sync_alt.sources.blockfrost import metadata
from db_sync_alt.bridge import covered as bridge_covered
from db_sync_alt.contract import TABLES, Refusal, canonical, digest, jsonable, required_epochs, epoch_size, generation_capabilities

OPENAPI='1d4d5d8bac8f0c641eea5c6f7b1f3fbbed8d1505'

def diagnostic_context(provider, stage, **fields):
    """Optional operational callback; providers without it are unaffected."""
    try:
        callback = getattr(provider, 'set_failure_context', None)
        if callback is not None:
            callback(stage, **fields)
    except Exception:
        logging.getLogger(__name__).warning('failure_context_unavailable')


def hexbytes(v,size=None):
    if not isinstance(v,str) or len(v)%2 or re.fullmatch('[0-9a-f]*',v) is None or (size is not None and len(v)!=size*2):raise Refusal('provider_invalid_hex')
    return bytes.fromhex(v)

def integer(v,*,signed=False):
    if type(v) is int:r=v
    elif isinstance(v,str) and re.fullmatch('-?(0|[1-9][0-9]*)',v) and str(int(v))==v:r=int(v)
    else:raise Refusal('provider_invalid_integer')
    if not signed and r<0:raise Refusal('provider_negative_quantity')
    return r

class ProviderClient:
    """Provider base: subclasses implement `get(path)`, `get_optional(path)` (None for HTTP 404) and `drain()`; `pages` walks ascending pages."""
    def pages(self,path):
        page=1
        while True:
            rows=self.get(path+'?'+urlencode({'count':100,'page':page,'order':'asc'}))
            if not isinstance(rows,list) or len(rows)>100:raise Refusal('provider_bad_page')
            yield rows
            if len(rows)<100:return
            page+=1
            if page>10000:raise Refusal('provider_page_limit')

_BECH32='qpzry9x8gf2tvdw0s3jn54khce6mua7l'
def pool_hash(s):
    if not isinstance(s,str) or not s.startswith('pool1') or s.lower()!=s or len(s)>90:raise Refusal('provider_invalid_pool')
    hrp,payload=s.rsplit('1',1)
    try:data=[_BECH32.index(x) for x in payload]
    except ValueError:raise Refusal('provider_invalid_pool') from None
    chk=1
    for v in [ord(x)>>5 for x in hrp]+[0]+[ord(x)&31 for x in hrp]+data:
        top=chk>>25;chk=(chk&0x1ffffff)<<5^v
        for i,g in enumerate((0x3b6a57b2,0x26508e6d,0x1ea119fa,0x3d4233dd,0x2a1462b3)):
            if (top>>i)&1:chk^=g
    if len(data)<6 or chk!=1:raise Refusal('provider_invalid_pool_checksum')
    acc=bits=0;raw=[]
    for v in data[:-6]:
        acc=(acc<<5)|v;bits+=5
        while bits>=8:bits-=8;raw.append((acc>>bits)&255)
    if bits>=5 or ((acc<<(8-bits))&255) or len(raw)!=28:raise Refusal('provider_invalid_pool_padding')
    return bytes(raw)

def block_row(r):
    if not isinstance(r,dict):raise Refusal('provider_bad_block')
    try:
        return {'hash':hexbytes(r['hash'],32),'height':integer(r['height']),'parent_hash':hexbytes(r['previous_block'],32) if r['previous_block'] is not None else None,
                'epoch':integer(r['epoch']),'slot':integer(r['slot']), 'time':dt.datetime.fromtimestamp(integer(r['time']),dt.timezone.utc).isoformat(timespec='microseconds').replace('+00:00','Z'), 'tx_count':integer(r['tx_count'])}
    except (KeyError,OverflowError,ValueError,TypeError):raise Refusal('provider_bad_block') from None

def amounts(row):
    try:
        items=row['amount'];lovelace=None;assets={}
        if not isinstance(items,list):raise Refusal('provider_bad_amounts')
        for x in items:
            unit=x['unit'];qty=integer(x['quantity'])
            if unit=='lovelace':
                if lovelace is not None:raise Refusal('provider_duplicate_asset')
                lovelace=qty
            else:
                raw=hexbytes(unit)
                if not 28<=len(raw)<=60 or qty<=0:raise Refusal('provider_bad_asset')
                key=(raw[:28],raw[28:])
                if key in assets:raise Refusal('provider_duplicate_asset')
                assets[key]=qty
        if lovelace is None:raise Refusal('provider_missing_lovelace')
        return str(lovelace),assets
    except (KeyError,TypeError):raise Refusal('provider_bad_amounts') from None

def output_value(row):
    value,assets=amounts(row)
    if not isinstance(row.get('address'),str) or not row['address']:raise Refusal('provider_bad_address')
    if 'data_hash' not in row:raise Refusal('provider_missing_datum_field')
    datum=hexbytes(row['data_hash'],32) if row['data_hash'] is not None else None
    inline=row.get('inline_datum')
    if inline is not None:
        raw=hexbytes(inline)
        if datum is None or hashlib.blake2b(raw,digest_size=32).digest()!=datum:raise Refusal('provider_inline_datum_hash_mismatch')
    return {'address':row['address'],'value':value,'datum_hash':datum},assets

def ledger_ordinals(keys):
    """Position of each key in ledger order: inputs by (origin hash, index), output assets by
    (policy, name), both bytewise; equal to cardano-db-sync's row order. Pass ALL inputs of the
    transaction or ALL assets of the output, not only the stored ones."""
    return {key:position for position,key in enumerate(sorted(keys))}

def classify_inputs(rows,decoded,resolve_origin=None):
    """Validate exact body membership, including same outpoint ordinary+collateral."""
    if not isinstance(rows,list):raise Refusal('provider_bad_inputs')
    roles={k:set(decoded[k]) for k in ('ordinary','collateral','reference')};union=set().union(*roles.values());seen={k:set() for k in roles};logical={};role_rows=set()
    for r in rows:
        try:k=(hexbytes(r['tx_hash'],32),integer(r['output_index']))
        except (KeyError,TypeError):raise Refusal('provider_bad_input') from None
        if k not in union:raise Refusal('provider_input_membership_mismatch')
        if 'collateral' in r and type(r['collateral']) is not bool:raise Refusal('provider_bad_input_role')
        if 'reference' in r and type(r['reference']) is not bool:raise Refusal('provider_bad_input_role')
        if r.get('collateral') is True:
            if r.get('reference') is True:raise Refusal('provider_ambiguous_input_role')
            role='collateral'
        elif r.get('reference') is True:role='reference'
        elif r.get('reference') is False:role='ordinary'
        else:
            candidates=[x for x in ('ordinary','reference') if k in roles[x]]
            if not candidates and k in roles['collateral'] and 'collateral' not in r:candidates=['collateral']
            if len(candidates)!=1:raise Refusal('provider_ambiguous_input_role')
            role=candidates[0]
        if k not in roles[role] or (role,k) in role_rows:raise Refusal('provider_input_membership_mismatch')
        role_rows.add((role,k));seen[role].add(k);value,assets=output_value(r)
        logical.setdefault(k,{})[role]=(value,assets,r)
    if any(seen[k]!=roles[k] for k in roles):raise Refusal('provider_missing_input_membership')
    for k,records in logical.items():
        values=list(records.values())
        if any(v[:2]!=values[0][:2] for v in values[1:]):
            # Blockfrost collateral-role rows may omit ALL native assets. Never
            # infer that permission from a mismatch alone: prove the complete
            # ordinary/reference row against the original output first.
            if resolve_origin is None or 'collateral' not in records:raise Refusal('provider_conflicting_input_rows')
            original=output_value(resolve_origin(*k))
            for role,(value,assets,_) in records.items():
                if value!=original[0] or (assets!=original[1] and not (role=='collateral' and assets=={})):raise Refusal('provider_conflicting_input_rows')
    return {k:logical[k]['ordinary'][2] for k in roles['ordinary']}

def fetch_original_output(provider,h,index):
    response=provider.get('/txs/'+h.hex()+'/utxos')
    if response.get('hash')!=h.hex():raise Refusal('provider_origin_hash_mismatch')
    outputs=response.get('outputs')
    if not isinstance(outputs,list):raise Refusal('provider_origin_missing')
    matches=[r for r in outputs if r.get('output_index')==index]
    if len(matches)!=1 or matches[0].get('collateral') is True:raise Refusal('provider_origin_missing')
    return matches[0]


def ordinary_outputs(rows,decoded):
    if not isinstance(rows,list):raise Refusal('provider_missing_outputs')
    count=decoded['output_count'];expected=set(range(count+(1 if decoded['collateral_return'] else 0)));seen=set();normal=[]
    for row in rows:
        index=integer(row.get('output_index'))
        if index not in expected or index in seen:raise Refusal('provider_output_count_mismatch')
        seen.add(index);collateral=index==count
        if 'collateral' in row and (type(row['collateral']) is not bool or row['collateral']!=collateral):raise Refusal('provider_output_role_mismatch')
        if not collateral:normal.append(row)
    if seen!=expected:raise Refusal('provider_output_count_mismatch')
    return normal


def collateral_inputs(rows,decoded):
    """The outputs a phase-2-invalid transaction spent: exactly its collateral inputs. cardano-db-sync stores
    them as the transaction's tx_in rows, so Blockfrost lists them like ordinary inputs, with every asset."""
    if not isinstance(rows,list):raise Refusal('provider_bad_inputs')
    spent={}
    for r in rows:
        try:k=(hexbytes(r['tx_hash'],32),integer(r['output_index']))
        except (KeyError,TypeError):raise Refusal('provider_bad_input') from None
        if any(role in r and type(r[role]) is not bool for role in ('collateral','reference')):raise Refusal('provider_bad_input_role')
        if k not in decoded['collateral'] or k in spent or r.get('collateral') is True or r.get('reference') is True:raise Refusal('provider_input_membership_mismatch')
        output_value(r);spent[k]=r
    if set(spent)!=decoded['collateral']:raise Refusal('provider_missing_input_membership')
    return spent


def collateral_return(rows,decoded):
    """The outputs a phase-2-invalid transaction created: its collateral return alone, at index = the number
    of body outputs, or none. cardano-db-sync stores it as the transaction's tx_out row (not collateral)."""
    if not isinstance(rows,list):raise Refusal('provider_missing_outputs')
    expected={decoded['output_count']} if decoded['collateral_return'] else set();seen=set()
    for row in rows:
        index=integer(row.get('output_index'))
        if index not in expected or index in seen:raise Refusal('provider_output_count_mismatch')
        if 'collateral' in row and row['collateral'] is not False:raise Refusal('provider_output_role_mismatch')
        seen.add(index)
    if seen!=expected:raise Refusal('provider_output_count_mismatch')
    return list(rows)


def ledger_effects(response,decoded,resolve_origin=None):
    """What a transaction did on chain, from its `/txs/{hash}/utxos` rows checked against its CBOR: the
    outputs it spent (outpoint to row), the outputs it created (rows) and its mint. A phase-2-invalid
    transaction spent only its collateral and created only its collateral return; its body inputs, outputs
    and mint never took effect, and cardano-db-sync stores none of them."""
    if decoded['valid']:
        return classify_inputs(response.get('inputs'),decoded,resolve_origin),ordinary_outputs(response.get('outputs'),decoded),decoded['mints']
    return collateral_inputs(response.get('inputs'),decoded),collateral_return(response.get('outputs'),decoded),()


def inventory(provider,block):
    diagnostic_context(provider,'events.inventory',block_height=block.get('height'),block_hash=block.get('hash'))
    if block['tx_count']==0:return []
    if block['tx_count']>10000:raise Refusal('provider_block_transaction_limit')
    txs=[];seen=set();body_bytes=0;raw_bytes=0
    for rows in provider.pages('/blocks/'+block['hash'].hex()+'/txs/cbor'):
        for row in rows:
            diagnostic_context(provider,'events.inventory',block_height=block.get('height'),block_hash=block.get('hash'),transaction_index=len(txs),transaction_hash=row.get('tx_hash') if isinstance(row,dict) else None)
            if not isinstance(row,dict):raise Refusal('provider_bad_inventory')
            try:d=decode_transaction(row['cbor'],row['tx_hash'])
            except KeyError:raise Refusal('provider_bad_inventory') from None
            if d['hash'] in seen:raise Refusal('provider_duplicate_transaction')
            body_bytes+=d['body_bytes']
            raw_bytes+=len(row['cbor'])//2
            if body_bytes>16*1024*1024 or raw_bytes>16*1024*1024:raise Refusal('provider_block_decode_limit')
            seen.add(d['hash']);txs.append(d)
            if len(txs)>block['tx_count']:raise Refusal('provider_transaction_count_mismatch')
        diagnostic_context(provider,'events.inventory',block_height=block.get('height'),block_hash=block.get('hash'))
    if len(txs)!=block['tx_count']:raise Refusal('provider_transaction_count_mismatch')
    return txs

# Retired pools can still hold stake in an epoch: its distribution is the snapshot taken one epoch earlier,
# before that boundary reaps retiring pools. Pools retired at E - 2 or later are read for epoch E.
RETIRED_WINDOW = 2
MAX_POOLS = 100000


def _pool_rows(rows):
    if not isinstance(rows, list) or len(rows) > 100:raise Refusal('provider_bad_page')
    return rows


def retired_pools(provider, epoch):
    """Pool IDs retired at epoch `epoch - RETIRED_WINDOW` or later, newest first."""
    out = []
    for page in range(1, 10001):
        rows = _pool_rows(provider.get('/pools/retired?' + urlencode({'count': 100, 'page': page, 'order': 'desc'})))
        for r in rows:
            if not isinstance(r, dict):raise Refusal('provider_duplicate_or_invalid_stake')
            if integer(r.get('epoch')) < epoch - RETIRED_WINDOW:return out
            pool_hash(r.get('pool_id'));out.append(r['pool_id'])
        if len(rows) < 100:return out
    raise Refusal('provider_page_limit')


def _add(pools, pool_id, amount):
    key = pool_hash(pool_id)
    if key in pools:raise Refusal('provider_duplicate_or_invalid_stake')
    pools[key] = (pool_id, amount)
    if len(pools) > MAX_POOLS:raise Refusal('provider_epoch_size_limit')


def current_pool_stakes(provider, epoch, *, stake_prefix):
    """Per-pool stake of the current epoch: `/pools/extended` and the retired window through `/pools/{id}`, each
    the sum of the epoch's epoch_stake rows of that pool. A pool with sum 0 holds rows only if the epoch's per-address
    listing of that pool is non-empty."""
    pools = {}
    for rows in provider.pages('/pools/extended'):
        for r in rows:
            if not isinstance(r, dict):raise Refusal('provider_duplicate_or_invalid_stake')
            _add(pools, r.get('pool_id'), integer(r.get('active_stake')))
    for pool_id in retired_pools(provider, epoch):
        if pool_hash(pool_id) in pools:continue  # retired, then registered again
        r = provider.get('/pools/' + pool_id)
        if not isinstance(r, dict) or r.get('pool_id') != pool_id:raise Refusal('provider_duplicate_or_invalid_stake')
        _add(pools, pool_id, integer(r.get('active_stake')))
    out = {}
    for key, (pool_id, amount) in pools.items():
        if amount == 0:
            rows = _pool_rows(provider.get('/epochs/' + str(epoch) + '/stakes/' + pool_id + '?count=1&page=1'))
            if not rows:continue
            if not isinstance(rows[0], dict) or not isinstance(rows[0].get('stake_address'), str) or not rows[0]['stake_address'].startswith(stake_prefix):
                raise Refusal('provider_duplicate_or_invalid_stake')
        out[key] = amount
    return out


def historical_pool_stakes(provider, epoch):
    """Per-pool stake of a completed epoch: every registered pool and the retired window, from each pool's history
    (a row exists exactly where the pool has epoch_stake rows in that epoch)."""
    candidates = {}
    for rows in provider.pages('/pools'):
        for pool_id in rows:
            _add(candidates, pool_id, None)
    for pool_id in retired_pools(provider, epoch):
        if pool_hash(pool_id) not in candidates:_add(candidates, pool_id, None)
    out = {}
    for key, (pool_id, _) in sorted(candidates.items()):
        for page in range(1, 10001):
            rows = _pool_rows(provider.get('/pools/' + pool_id + '/history?' + urlencode({'count': 100, 'page': page, 'order': 'desc'})))
            done = len(rows) < 100
            for r in rows:
                if not isinstance(r, dict):raise Refusal('provider_duplicate_or_invalid_stake')
                row_epoch = integer(r.get('epoch'))
                if row_epoch == epoch:
                    if integer(r.get('delegators_count')) < 1:raise Refusal('provider_duplicate_or_invalid_stake')
                    out[key] = integer(r.get('active_stake'))
                if row_epoch <= epoch:done = True;break
            if done:break
        else:raise Refusal('provider_page_limit')
    return out


def epoch_facts(provider,epoch,target=None,*,stake_prefix):
    """Two complete per-pool reads of an epoch's stake distribution, with total/nonce/max_tx_size/end-anchor equality.

    The facts are the SQL path's per-pool sums. Requests grow with the number of pools, never with the number of
    delegated addresses: the current epoch (the latest block's) through `/pools/extended`, a completed one through each
    pool's history."""
    def header():
        summary=provider.get('/epochs/'+str(epoch));params=provider.get('/epochs/'+str(epoch)+'/parameters')
        if summary.get('epoch')!=epoch or params.get('epoch')!=epoch or summary.get('active_stake') is None or params.get('nonce') is None:raise Refusal('provider_epoch_not_ready')
        total=integer(summary['active_stake'])
        if total<=0:raise Refusal('provider_empty_epoch_unqualified')
        nonce=hexbytes(params['nonce'],32);size=epoch_size(params.get('max_tx_size'))
        end=provider.get('/epochs/'+str(epoch)+'/blocks?count=1&page=1&order=desc')
        if not isinstance(end,list) or len(end)!=1:raise Refusal('provider_epoch_not_ready')
        anchor=block_row(provider.get('/blocks/'+end[0]))
        if anchor['epoch']!=epoch:raise Refusal('provider_epoch_anchor_mismatch')
        # A target inside the current/historical epoch has no accepted epoch
        # end yet. Pin its confirmed target block, never the moving epoch tip.
        completed=target is None or anchor['height']<=target
        if target is not None and anchor['height']>target:
            anchor=block_row(provider.get('/blocks/'+str(target)))
            if anchor['height']!=target or anchor['epoch']!=epoch:raise Refusal('provider_epoch_anchor_mismatch')
        # The pool endpoints' "current epoch" is the latest block's epoch: read it the same way.
        current=block_row(provider.get('/blocks/latest'))['epoch']
        if current<epoch:raise Refusal('provider_epoch_not_ready')
        return total,{'epoch':epoch,'nonce':nonce,'max_tx_size':size},anchor,completed,current
    first=header();reads=[]
    source='pools-current' if first[4]==epoch else 'pools-history'
    for _ in range(2):
        reads.append(current_pool_stakes(provider,epoch,stake_prefix=stake_prefix) if source=='pools-current' else historical_pool_stakes(provider,epoch))
    # A boundary crossed during the reads moves the latest block's epoch: that is a wait (then the completed
    # epoch is read from the pools' histories), checked before the sums, which such a read cannot match.
    last=header()
    if first!=last:raise Refusal('provider_epoch_changed')
    for pools in reads:
        if not pools or sum(pools.values())!=first[0]:raise Refusal('provider_epoch_total_mismatch')
    if reads[0]!=reads[1]:raise Refusal('provider_epoch_changed')
    pools=reads[0]
    return first[1],[{'epoch':epoch,'pool_hash':k,'amount':str(v)} for k,v in sorted(pools.items())],first[2],{'max_tx_size':first[1]['max_tx_size'],'stake_source':source,'stake_pools':len(pools),'stake_digest':digest({k.hex():str(v) for k,v in pools.items()}),'active_stake':str(first[0]),'epoch_end':{'height':first[2]['height'],'hash':first[2]['hash'].hex()} if first[3] else None,'epoch_anchor':{'height':first[2]['height'],'hash':first[2]['hash'].hex(),'kind':'completed-end' if first[3] else 'confirmed-target'}}

class BlockfrostSource:
    def __init__(self,provider,profile,store,generation,stop_requested,*,audit=False,coverage=None,known=None,relevance_enabled=False):
        self.provider=provider;self.profile=profile;self.store=store;self.stop_requested=stop_requested;self.gen=generation
        g=store.generation(generation);self.target=g['target_height']
        # A covered generation stores bridge metadata and every asset of its ICS and reserve outputs.
        self.bridge=bridge_covered(g['manifest'])
        self.cache={};self.facts=[]
        from db_sync_alt.sources.blockfrost.relevance import pending_certificate,Discovery,CertifiedReadView
        self.coverage=coverage if relevance_enabled and coverage is not None else (pending_certificate(store,profile,generation) if relevance_enabled else None)
        if relevance_enabled and self.coverage is None:raise Refusal('relevance_parent_unsupported')
        self.discovery=None;self.auditor=None
        if self.coverage is not None:
            parent=store.generation(generation)['parent'];known=known if known is not None else {table:set() for table in TABLES}
            self.known=known;self.store=CertifiedReadView(store,parent,known)
            self.discovery=Discovery(store,profile,parent,independent=audit)
            if not audit:self.auditor=BlockfrostSource(provider,profile,store,generation,stop_requested,audit=True,coverage=self.coverage,known=known,relevance_enabled=True)
    def add(self,table,row):self.facts.append((table,row))
    def has(self,table,where,params):return self.store.fact(table,params) is not None
    def block(self,h):
        existing=self.store.fact('blocks',(h,)) if self.store.has_block_hash(self.gen,h) else None
        if existing:return {k:existing[k] for k in ('hash','height','parent_hash','epoch','slot','time','tx_count')}
        r=block_row(self.provider.get('/blocks/'+h.hex()))
        if r['hash']!=h or r['height']>self.target:raise Refusal('provider_origin_block_mismatch')
        self.add('blocks',r);return r
    def transaction(self,h,expected_block=None,expected_index=None,expected_valid=None):
        if h in self.cache:return self.cache[h]
        r=self.provider.get('/txs/'+h.hex())
        if r.get('hash')!=h.hex() or type(r.get('valid_contract')) is not bool or (expected_valid is not None and r['valid_contract']!=expected_valid):raise Refusal('provider_transaction_mismatch')
        block=self.block(hexbytes(r.get('block'),32));index=integer(r.get('index'))
        if integer(r.get('block_height'))!=block['height'] or index>=block['tx_count'] or (expected_block is not None and block['hash']!=expected_block) or (expected_index is not None and index!=expected_index):raise Refusal('provider_transaction_position_mismatch')
        self.add('transactions',{'hash':h,'block_hash':block['hash'],'tx_index':index,'valid':1 if r['valid_contract'] else 0});self.cache[h]=r;return r
    def datum(self,h,inline=None):
        """Store datum `h` as cardano-db-sync 13.8 does, from its CBOR bound to `h`; `inline` is the
        output's own datum CBOR.

        No preimage on chain (Blockfrost 404): no datum, the output keeps its hash. The value is
        `decode_datum`'s, never Blockfrost's JSON: that rounds integers beyond 2^53 and serves
        db-sync's NULL as `{}`.
        """
        if self.has('datums','hash=?',(h,)) or any(t=='datums' and r['hash']==h for t,r in self.facts):return
        if inline is None:
            found=self.provider.get_optional('/scripts/datum/'+h.hex()+'/cbor')
            if found is None:return
            inline=found.get('cbor') if isinstance(found,dict) else None
        raw=hexbytes(inline)
        if hashlib.blake2b(raw,digest_size=32).digest()!=h:raise Refusal('provider_datum_hash_mismatch')
        value=decode_datum(raw)
        self.add('datums',{'hash':h,'value':None if value is None else canonical(value)})
    def output(self,h,row):
        val,assets=output_value(row);idx=integer(row['output_index'])
        if val['datum_hash'] is not None:self.datum(val['datum_hash'],row.get('inline_datum'))
        self.add('outputs',{'tx_hash':h,'output_index':idx,**val})
        ordinals=ledger_ordinals(assets)
        for (p,n),q in self.profile.kept_assets(val['address'],assets,covered=self.bridge).items():
            self.add('assets',{'policy':p,'name':n});self.add('output_assets',{'tx_hash':h,'output_index':idx,'policy':p,'name':n,'quantity':str(q),'ordinal':ordinals[(p,n)]})
    def origin(self,h,index,observed):
        old=self.store.fact('outputs',(h,index))
        if old:
            val,assets=output_value(observed)
            if any(old[k]!=v for k,v in val.items()):raise Refusal('provider_origin_conflict')
            expected={(bytes(r['policy']),bytes(r['name'])):int(r['quantity']) for r in self.store.output_assets(h,index)}
            # A stored output is compared under the rule of the generation that stored it.
            covered=self.store.bridge_covered(self.store.output_generation(h,index))
            if expected!=self.profile.kept_assets(old['address'],assets,covered=covered):raise Refusal('provider_origin_asset_conflict')
            # A Plutus spend of a hash-only output carries its preimage: a datum missing until now is stored here.
            if old['datum_hash'] is not None:self.datum(old['datum_hash'],observed.get('inline_datum'))
            return
        self.transaction(h)
        response=self.provider.get('/txs/'+h.hex()+'/utxos')
        if response.get('hash')!=h.hex():raise Refusal('provider_origin_hash_mismatch')
        # A created output: a valid transaction's collateral return (listed as collateral) never exists.
        matches=[x for x in response.get('outputs',[]) if x.get('output_index')==index]
        if len(matches)!=1 or matches[0].get('collateral') is True or output_value(matches[0])!=output_value(observed):raise Refusal('provider_origin_conflict')
        self.output(h,matches[0])
    def _collect(self,block):
        self.cache={};self.facts=[];txs=inventory(self.provider,block)
        if self.discovery:self.discovery.begin()
        for index,decoded in enumerate(txs):
            if self.stop_requested():raise Refusal('interrupted')
            h=decoded['hash']
            diagnostic_context(self.provider,'events.transaction',block_height=block.get('height'),block_hash=block.get('hash'),transaction_index=index,transaction_hash=h)
            if self.discovery and not self.discovery.select(decoded):continue
            response=self.provider.get('/txs/'+h.hex()+'/utxos')
            if response.get('hash')!=h.hex():raise Refusal('provider_utxo_hash_mismatch')
            spent,created,minted=ledger_effects(response,decoded,lambda origin,index:fetch_original_output(self.provider,origin,index))
            if self.discovery:
                from db_sync_alt.sources.blockfrost.relevance import compare_outputs
                compare_outputs(decoded,response['outputs'],self.profile.network.network)
                self.discovery.advance(decoded,created)
            relevant_out=[x for x in created if self.profile.relevant(output_value(x)[0]['address'],output_value(x)[1])]
            relevant_in=[(k,x) for k,x in spent.items() if self.profile.relevant(output_value(x)[0]['address'],output_value(x)[1])]
            if self.discovery and any(not self.discovery.contains(key) for key,_ in relevant_in):raise Refusal('relevance_parent_incomplete')
            mints=[m for m in minted if self.profile.required(m[0],m[1])]
            if not (relevant_out or relevant_in or mints):continue
            self.transaction(h,block['hash'],index,decoded['valid'])
            for x in relevant_out:self.output(h,x)
            ordinals=ledger_ordinals(spent)
            for (origin,oi),x in relevant_in:
                self.origin(origin,oi,x);self.add('inputs',{'tx_hash':h,'origin_hash':origin,'origin_index':oi,'ordinal':ordinals[(origin,oi)]})
            for p,n,q in mints:
                self.add('assets',{'policy':p,'name':n});self.add('mints',{'tx_hash':h,'policy':p,'name':n,'quantity':str(q)})
            # The bridge query reads the metadata of transactions with an output at the ICS address.
            # cardano-db-sync stores none for a phase-2-invalid transaction.
            if self.bridge and any(output_value(x)[0]['address']==self.profile.ics for x in relevant_out):
                self.add('tx_metadata',metadata.fetch(self.provider,decoded) if decoded['valid'] else metadata.fact(h,False,None))
        return self.facts,{'transactions_scanned':len(txs),'facts':len(self.facts)},txs
    def block_facts(self,block):
        facts,stats,txs=self._collect(block)
        if self.auditor is not None:
            from db_sync_alt.sources.blockfrost.relevance import inventory as fact_inventory,KIND,MAX_KEYS,MAX_PROOF
            expected,_,independent=self.auditor._collect(block)
            tx_hashes=[row['hash'] for row in txs]
            if tx_hashes!=[row['hash'] for row in independent]:raise Refusal('independent_provider_inventory_changed')
            actual=fact_inventory(facts);expected=fact_inventory(expected)
            if actual!=expected:raise Refusal('independent_provider_mismatch')
            proof={'kind':KIND,'version':1,'parent_ref':self.coverage['parent_ref'],'inventory':expected,
                   'transactions':len(txs),'transaction_hashes_sha256':digest(tx_hashes),
                   'created_outputs':jsonable(sorted(self.auditor.discovery.block_created))}
            if len(canonical(proof).encode())>MAX_PROOF:raise Refusal('relevance_work_unit_limit')
            for table,entry in expected.items():self.known[table].update(canonical(k) for k in entry['keys'])
            if sum(map(len,self.known.values()))>MAX_KEYS:raise Refusal('relevance_work_unit_limit')
            stats['relevance']=proof
        return facts,stats

def complete_inherited_epoch(provider,store,g,epoch,inherited):
    """Qualify the new completed end; already certified stake facts stay inherited."""
    summary=provider.get('/epochs/'+str(epoch));params=provider.get('/epochs/'+str(epoch)+'/parameters')
    nonce=store.epoch_nonce(epoch,g['parent'])
    total=store.epoch_stake_total(epoch,g['parent'])
    if (summary.get('epoch')!=epoch or params.get('epoch')!=epoch or summary.get('active_stake') is None or integer(summary['active_stake'])!=total
            or nonce is None or hexbytes(params.get('nonce'),32)!=nonce['nonce'] or epoch_size(params.get('max_tx_size'))!=nonce['max_tx_size']):raise Refusal('provider_inherited_epoch_conflict')
    rows=provider.get('/epochs/'+str(epoch)+'/blocks?count=1&page=1&order=desc')
    if not isinstance(rows,list) or len(rows)!=1:raise Refusal('provider_epoch_not_ready')
    end=block_row(provider.get('/blocks/'+rows[0]))
    expected=store.block_at(g['id'],end['height']);successor=store.block_at(g['id'],end['height']+1)
    if end['epoch']!=epoch or end['height']>g['target_height'] or expected is None or expected['hash']!=end['hash'] or successor is None or successor['parent_hash']!=end['hash'] or successor['epoch']!=epoch+1:raise Refusal('provider_epoch_end_not_qualified')
    final=block_row(provider.get('/blocks/'+str(end['height'])))
    if final!=end:raise Refusal('provider_epoch_changed')
    return {'epoch':epoch,'max_tx_size':nonce['max_tx_size'],'epoch_end':{'height':end['height'],'hash':end['hash'].hex()},'stake_inherited_from':inherited,'active_stake':str(total),'source_kind':'blockfrost-cardano','provider':provider.drain()}


def check_anchor(provider,store,anchor,*,committed):
    diagnostic_context(provider,'anchors',block_height=anchor['height'],block_hash=anchor['hash'])
    row=block_row(provider.get('/blocks/'+str(anchor['height'])))
    if row['hash'].hex()!=anchor['hash']:
        if committed:store.mark_conflict()
        raise Refusal('committed_source_conflict' if committed else 'pending_anchor_changed')
    return row

def handoff_overlap(provider,store,profile,parent):
    """Bounded, nonempty semantic overlap, independent of provider row IDs."""
    rows=store.recent_transactions(parent['id'],limit=8)
    checked=0
    for transaction in rows:
        h,block,index,valid=(transaction[k] for k in ('hash','block_hash','tx_index','valid'))
        diagnostic_context(provider,'handoff',block_hash=block,transaction_index=index,transaction_hash=h)
        raw=provider.get('/txs/'+h.hex()+'/cbor');decoded=decode_transaction(raw.get('cbor'),h.hex())
        u=provider.get('/txs/'+h.hex()+'/utxos')
        if u.get('hash')!=h.hex() or decoded['valid']!=(valid==1):raise Refusal('provider_handoff_fact_conflict')
        spent,created,minted=ledger_effects(u,decoded,lambda origin,index:fetch_original_output(provider,origin,index))
        expected={};covered={}
        for out in store.transaction_outputs(h,parent['id']):
            assets={(bytes(r['policy']),bytes(r['name'])):int(r['quantity']) for r in store.output_assets(h,out['output_index'],parent['id'])}
            expected[out['output_index']]=({k:out[k] for k in ('address','value','datum_hash')},assets)
            covered[out['output_index']]=store.bridge_covered(store.output_generation(h,out['output_index']))
        seen={}
        for out in created:
            val,assets=output_value(out);index=integer(out['output_index'])
            if profile.relevant(val['address'],assets):seen[index]=(val,profile.kept_assets(val['address'],assets,covered=covered.get(index,False)))
        ins={k for k,v in spent.items() if profile.relevant(output_value(v)[0]['address'],output_value(v)[1])}
        expected_ins={(bytes(r['origin_hash']),r['origin_index']) for r in store.transaction_inputs(h,parent['id'])}
        mint={ (p,n):q for p,n,q in minted if profile.required(p,n)}
        expected_mint={(bytes(r['policy']),bytes(r['name'])):int(r['quantity']) for r in store.transaction_mints(h,parent['id'])}
        stored=store.fact('tx_metadata',(h,)) if 'tx_metadata' in store.physical_tables else None
        metadata_agrees=stored is None or (metadata.stored_agrees(decoded,stored) if decoded['valid'] else stored['present']==0)
        if seen!=expected or ins!=expected_ins or mint!=expected_mint or not metadata_agrees:raise Refusal('provider_handoff_fact_conflict')
        checked+=1
    if not checked:raise Refusal('provider_nonempty_handoff_unqualified')
    return checked


def update(store,provider,profile,end_height=None,*,hooks,relevance_enabled=False):
    from db_sync_alt.verify.completion import validate_completion
    if relevance_enabled:raise Refusal('relevance_parent_unsupported')
    pending=store.pending()
    if pending and pending['manifest'].get('relevance_coverage') is not None:raise Refusal('relevance_mode_mismatch')
    if pending:validate_completion(store,pending)
    ready=store.generation()
    diagnostic_context(provider,'verification',generation=(pending or ready)['id'])
    hooks.update(phase='verifying',committed=ready['target_height']);hooks.verify(store,ready['id'])
    if store.meta('profile')!=profile.identity:raise Refusal('provider_profile_mismatch')
    diagnostic_context(provider,'genesis')
    genesis=provider.get('/genesis')
    if genesis.get('network_magic')!=profile.network.magic:raise Refusal('wrong_provider_network')
    # The client pins the network's host; stable source anchors bind this
    # lineage to the exact trusted chain rather than a similar network.
    diagnostic_context(provider,'anchors')
    for name in ('start','target'):check_anchor(provider,store,ready['manifest']['anchors'][name],committed=True)
    diagnostic_context(provider,'handoff')
    try:handoff_overlap(provider,store,profile,ready)
    except Refusal as exc:
        if exc.code=='provider_handoff_fact_conflict':store.mark_conflict()
        raise
    diagnostic_context(provider,'anchors')
    pending=store.pending()
    if pending:
        if pending['source']!='blockfrost-cardano' or pending['manifest']['source_identity']!=profile.network.source_identity:raise Refusal('resume_adapter_mismatch')
        if end_height is not None and end_height!=pending['target_height']:raise Refusal('resume_target_mismatch')
        gen=pending['id'];manifest=pending['manifest']
    else:
        k=profile.security
        head=block_row(provider.get('/blocks/latest'));target=head['height']-k if end_height is None else end_height
        if target<=ready['target_height']:raise Refusal('no_confirmed_progress')
        if target>head['height']-k:raise Refusal('target_not_confirmed')
        targetrow=block_row(provider.get('/blocks/'+str(target)));witnessrow=block_row(provider.get('/blocks/'+str(target+k)))
        if targetrow['height']!=target or witnessrow['height']!=target+k:raise Refusal('provider_target_mismatch')
        startepoch=store.block_at(ready['id'],ready['start_height'])['epoch']
        manifest={'identity':profile.identity,'contracts':digest(profile.contract),'source_identity':profile.network.source_identity,
                  'anchors':{'start':ready['manifest']['anchors']['start'],'target':{'height':target,'hash':targetrow['hash'].hex()},'witness':{'height':target+k,'hash':witnessrow['hash'].hex()}},
                  'required_epochs':required_epochs(startepoch,targetrow['epoch']),'capabilities':generation_capabilities(bridge_metadata=True),'parent_digest':digest(ready['manifest']['digests'])}
        with store.atomic():
            gen=store.begin_generation(source='blockfrost-cardano',start=profile.start,target=target,witness=target+k,manifest=manifest,parent=ready['id'])
            store.checkpoint(gen,'metadata','inherited',{'lo':ready['start_height'],'hi':ready['target_height'],'parent':ready['id'],'parent_digest':manifest['parent_digest'],'source_kind':'generation-inheritance','inherited_source_kind':ready['source']})
            store.checkpoint(gen,'handoff','initial',provider.drain())
    g=store.generation(gen)
    diagnostic_context(provider,'anchors',generation=gen)
    hooks.sample(store,target=g['target_height'],committed=ready['target_height'])
    processed_rows=0
    # Resume accepts neither a different target nor a stale pending fork. The
    # previous ready witness can change; only committed anchors are invariant.
    for name in ('target','witness'):check_anchor(provider,store,manifest['anchors'][name],committed=False)
    last=max(ready['target_height'],store.last_metadata_height(gen) or ready['target_height'])
    while last<g['witness_height']:
        if hooks.stopped():raise Refusal('interrupted')
        hooks.check_space(store)
        hooks.update(phase='metadata',range_start=last+1,range_end=min(last+100,g['witness_height']))
        diagnostic_context(provider,'metadata',block_height=last+1)
        previous=store.block_at(gen,last)['hash']
        batch=provider.get('/blocks/'+bytes(previous).hex()+'/next?count=100&page=1')
        if not isinstance(batch,list) or not batch:raise Refusal('provider_metadata_gap')
        rows=[]
        for raw in batch:
            row=block_row(raw)
            if row['height']!=last+len(rows)+1 or row['parent_hash']!=(rows[-1]['hash'] if rows else previous):raise Refusal('provider_metadata_parent_conflict')
            if row['height']>g['witness_height']:break
            rows.append(row)
        if not rows:raise Refusal('provider_metadata_gap')
        diagnostic_context(provider,'metadata.commit',block_height=last+1)
        with store.atomic():
            for row in rows:store.insert('blocks',row,gen);store.bind_block(gen,row['height'],row['hash'])
            store.checkpoint(gen,'metadata',str(last+1),{'lo':last+1,'hi':rows[-1]['height'],'source_kind':'blockfrost-cardano','provider':provider.drain()})
        last=rows[-1]['height']
        processed_rows+=len(rows)
        hooks.sample(store,phase='metadata',committed=last,rows=processed_rows)
    source=BlockfrostSource(provider,profile,store,gen,stop_requested=hooks.stopped)
    committed_events=store.last_event_height(gen)
    hooks.update(phase='events',committed=max(ready['target_height'],committed_events or ready['target_height']))
    for height in range(ready['target_height']+1,g['target_height']+1):
        if store.completed(gen,'events',height):continue
        if hooks.stopped():raise Refusal('interrupted')
        hooks.check_space(store)
        hooks.update(phase='events',range_start=height,range_end=height)
        diagnostic_context(provider,'events',block_height=height)
        block=store.block_at(gen,height)
        if block is None:raise Refusal('provider_metadata_missing')
        try:
            facts,stats=source.block_facts(block)
            # Fact order is dependency ordered at commit, with outputs before spends.
            order=['blocks','transactions','datums','assets','outputs','output_assets','mints','inputs','tx_metadata']
            diagnostic_context(provider,'events.commit',block_height=height,block_hash=block['hash'])
            with store.atomic():
                for table in order:
                    for t,f in facts:
                        if t==table:
                            diagnostic_context(provider,'events.commit',block_height=height,block_hash=block['hash'],transaction_hash=f.get('tx_hash',f.get('hash') if t=='transactions' else None),transaction_index=f.get('tx_index'))
                            store.insert(t,f,gen)
                            if t=='blocks':store.bind_block(gen,f['height'],f['hash'])
                store.checkpoint(gen,'events',height,{'lo':height,'hi':height,'source_kind':'blockfrost-cardano',**stats,'provider':provider.drain()})
        except Refusal as exc:
            if exc.code in ('provider_origin_conflict','provider_origin_asset_conflict','immutable_fact_conflict','canonical_block_conflict'):store.mark_conflict()
            raise
        processed_rows+=len(facts)
        hooks.sample(store,phase='events',committed=height,rows=processed_rows)
    store.block_at(gen,g['target_height'])['epoch']  # the target header must be stored
    next_epoch=store.block_at(gen,g['target_height']+1)['epoch']
    for epoch in manifest['required_epochs']:
        if store.completed(gen,'epochs',epoch):continue
        diagnostic_context(provider,'epochs',epoch=epoch)
        hooks.update(phase='epochs',range_start=epoch,range_end=epoch)
        prior=store.checkpoint_rows(phase='epochs',unit=epoch,before=gen)
        inherited=max(prior,key=lambda row:row['generation']) if prior else None
        if inherited:
            old=inherited['receipt']
            if old.get('epoch_end') is not None or epoch>=next_epoch:continue
            if hooks.stopped():raise Refusal('interrupted')
            try:receipt=complete_inherited_epoch(provider,store,g,epoch,inherited['generation'])
            except Refusal as exc:
                if exc.code=='provider_inherited_epoch_conflict':store.mark_conflict()
                raise
            diagnostic_context(provider,'epochs.commit',epoch=epoch)
            with store.atomic():store.checkpoint(gen,'epochs',epoch,receipt)
            hooks.sample(store,phase='epochs',committed=epoch,rows=processed_rows)
            continue
        if hooks.stopped():raise Refusal('interrupted')
        nonce,stakes,anchor,receipt=epoch_facts(provider,epoch,g['target_height'],stake_prefix=profile.network.stake_prefix)
        diagnostic_context(provider,'epochs.commit',epoch=epoch)
        with store.atomic():
            store.insert('epoch_nonces',nonce,gen)
            for row in stakes:store.insert('epoch_stakes',row,gen)
            if anchor['height']<=g['target_height']:store.insert('blocks',anchor,gen);store.bind_block(gen,anchor['height'],anchor['hash'])
            store.checkpoint(gen,'epochs',epoch,{'epoch':epoch,'source_kind':'blockfrost-cardano',**receipt,'provider':provider.drain()})
        processed_rows+=len(stakes)+1
        hooks.sample(store,phase='epochs',committed=epoch,rows=processed_rows)
    diagnostic_context(provider,'anchors')
    for name in ('start','target'):check_anchor(provider,store,ready['manifest']['anchors'][name],committed=True)
    for name in ('target','witness'):check_anchor(provider,store,manifest['anchors'][name],committed=False)
    diagnostic_context(provider,'verification')
    hooks.update(phase='verifying',waiting=True)
    report=hooks.verify(store,gen,require_ready=False)
    manifest=copy.deepcopy(manifest)
    manifest.update(digests=report['digests'],validation={'kind':'provider-enumeration','independent_sql':'required-external-qualification'},provider_attempts_this_invocation=provider.attempts)
    diagnostic_context(provider,'finalization')
    manifest=store.finalize_pending(gen,manifest,provider.drain(),report['digests'])
    diagnostic_context(provider,'publication')
    hooks.before_publish()
    store.publish(gen,manifest)
    diagnostic_context(provider,'verification')
    report=hooks.verify(store,gen)
    hooks.sample(store,phase='complete',waiting=False,committed=g['target_height'],rows=processed_rows)
    return report
