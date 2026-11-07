"""Versioned SQL prerequisite coverage; chunks are not completion seals."""
from db_sync_alt.contract import Refusal,digest

SOURCE='cardano-db-sync-postgresql'
ADAPTER='db-sync-direct-pg-v1'
CHUNK_KEYS={'version','kind','binding','source_kind','read_only','lo','hi','rows'}
SEAL_KEYS={'version','kind','binding','source_kind','read_only','lo','hi','chunk_count','chunks_sha256'}

def _invalid():raise Refusal('prerequisites_receipt_invalid')
def acquisition_version(g):
    m=g['manifest']
    if g['source']!=SOURCE or m.get('source_identity',{}).get('adapter')!=ADAPTER:return 0
    a=m.get('acquisition',{});v=a.get('version');w=a.get('window_blocks')
    if type(v) is not int or v not in (1,2) or type(w) is not int or not 1<=w<=10000:_invalid()
    return v

def prerequisite_binding(g):
    m=g['manifest']
    return digest({'generation':g['id'],'parent':g['parent'],'source':g['source'],
        'source_identity':m['source_identity'],'identity':m['identity'],'anchors':m['anchors'],
        'acquisition':m['acquisition'],'start':[g['start_height'],g['start_index']],
        'target':g['target_height'],'witness':g['witness_height']})

def _base(g,kind,lo,hi):
    return {'version':1,'kind':kind,'binding':prerequisite_binding(g),'source_kind':g['source'],
            'read_only':True,'lo':lo,'hi':hi}

def chunk_receipt(g,lo,hi,rows):return {**_base(g,'prerequisites-chunk',lo,hi),'rows':rows}
def seal_receipt(g,chunks):
    return {**_base(g,'prerequisites-complete',0,g['start_height']-1),
            'chunk_count':len(chunks),'chunks_sha256':digest(chunks)}

def prerequisite_coverage(store,g,*,require_complete=True):
    """Return a validated contiguous prefix; completion requires a distinct seal.

    Legacy admitted pre-adapter histories keep their old receipt semantics.
    Qualified v1 whole-range receipts remain valid without manifest mutation.
    """
    version=acquisition_version(g);start=g['start_height'];root=g['parent'] is None and start>0
    rows=store.checkpoint_rows(generation=g['id'],phase='prerequisites')
    if version==0:
        complete=not root or store.completed(g['id'],'prerequisites','complete')
        if require_complete and not complete:raise Refusal('prerequisites_incomplete')
        return {'version':0,'position':start if complete else 0,'chunks':[],'complete':complete}
    if not root:
        if rows:_invalid()
        return {'version':version,'position':0,'chunks':[],'complete':True}
    if version==1:
        if not rows:
            if require_complete:raise Refusal('prerequisites_incomplete')
            return {'version':1,'position':0,'chunks':[],'complete':False}
        if len(rows)!=1 or rows[0]['unit']!='complete':_invalid()
        row=rows[0];r=row['receipt']
        if not isinstance(r,dict) or set(r)!={'lo','hi','read_only','rows','source_kind'}:_invalid()
        if type(r['lo']) is not int or type(r['hi']) is not int or (r['lo'],r['hi'])!=(0,start-1):_invalid()
        if r['read_only'] is not True or r['source_kind']!=g['source'] or type(r['rows']) is not int or r['rows']<0:_invalid()
        return {'version':1,'position':start,'chunks':[],'complete':True}
    window=g['manifest']['acquisition']['window_blocks'];binding=prerequisite_binding(g);chunks=[];seal=None
    for row in rows:
        r=row.get('receipt');unit=row.get('unit')
        if not isinstance(r,dict) or row.get('generation')!=g['id'] or row.get('phase')!='prerequisites':_invalid()
        if type(r.get('version')) is not int or r['version']!=1 or r.get('read_only') is not True or r.get('source_kind')!=g['source'] or r.get('binding')!=binding:_invalid()
        if unit=='complete':
            if seal is not None or set(r)!=SEAL_KEYS or r['kind']!='prerequisites-complete':_invalid()
            seal=r;continue
        if set(r)!=CHUNK_KEYS or r['kind']!='prerequisites-chunk':_invalid()
        lo,hi=r['lo'],r['hi']
        if type(lo) is not int or type(hi) is not int or lo<0 or hi<lo or hi>=start or hi-lo+1>window or unit!=f'{lo}:{hi}':_invalid()
        if type(r['rows']) is not int or r['rows']<0:_invalid()
        chunks.append({'unit':unit,'receipt':r})
    chunks.sort(key=lambda c:c['receipt']['lo']);position=0
    for chunk in chunks:
        if chunk['receipt']['lo']!=position:_invalid()
        position=chunk['receipt']['hi']+1
    if seal is not None:
        if position!=start or type(seal.get('lo')) is not int or type(seal.get('hi')) is not int or type(seal.get('chunk_count')) is not int or seal!=seal_receipt(g,chunks):_invalid()
    if require_complete and seal is None:raise Refusal('prerequisites_incomplete')
    return {'version':2,'position':position,'chunks':chunks,'complete':seal is not None}
