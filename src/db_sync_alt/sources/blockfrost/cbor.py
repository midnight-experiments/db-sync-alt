"""Bounded Cardano transaction decoder; identity always hashes original body bytes.

Contract: Alonzo/Babbage/Conway four-element transaction envelopes, ledger CDDL
4eb427eaf58127b26079f2148466dd889bb84f47. This extracts discovery facts, not a
replacement for consensus validation by the trusted Cardano index provider.
"""
from dataclasses import dataclass
import hashlib
import re
from db_sync_alt.contract import Refusal

@dataclass(frozen=True)
class Tag:
    number: int
    value: object

@dataclass
class MapEntries:
    pairs: list

_NO_KEY=object()

class _TagParts(list):
    """A tag being read: its number, then its content."""

def _attach(frame,value):
    built=frame[0]
    if isinstance(built,MapEntries):
        if frame[2] is _NO_KEY:frame[2]=value
        else:built.pairs.append((frame[2],value));frame[2]=_NO_KEY
    else:built.append(value)

class Decoder:
    def __init__(self, data, *, max_bytes=4*1024*1024, max_depth=64, max_items=200000, maps_as_pairs=False):
        if len(data)>max_bytes: raise Refusal('cbor_size_limit')
        self.data=data;self.pos=0;self.depth=max_depth;self.remaining=max_items;self.maps_as_pairs=maps_as_pairs
    def take(self,n):
        if n<0 or self.pos+n>len(self.data):raise Refusal('cbor_truncated')
        r=self.data[self.pos:self.pos+n];self.pos+=n;return r
    def head(self):
        b=self.take(1)[0];m=b>>5;a=b&31
        if a<24:return m,a
        if a==31:return m,None
        if a not in (24,25,26,27):raise Refusal('cbor_reserved_encoding')
        n=int.from_bytes(self.take(1<<(a-24)),'big')
        # Confirmed transactions may use non-preferred argument widths. Keep
        # their original bytes for body hashing; do not impose encoder rules.
        # Major7 payloads are floats/extended simple values, not arguments that
        # may be reinterpreted as the direct false/true/null values below.
        if m==7:raise Refusal('cbor_unsupported_simple')
        return m,n
    def item(self,depth=0):
        if depth>self.depth:raise Refusal('cbor_depth_limit')
        self.remaining-=1
        if self.remaining<0:raise Refusal('cbor_item_limit')
        m,n=self.head()
        if m in (0,1):
            if n is None:raise Refusal('cbor_invalid_integer')
            return n if m==0 else -1-n
        if m in (2,3):
            if n is None:
                parts=[]
                while self.data[self.pos:self.pos+1]!=b'\xff':
                    if self.pos>=len(self.data) or self.data[self.pos]>>5!=m or self.data[self.pos]&31==31:raise Refusal('cbor_invalid_chunks')
                    parts.append(self.item(depth+1))
                self.pos+=1;return (b'' if m==2 else '').join(parts)
            raw=self.take(n)
            if m==2:return raw
            try:return raw.decode('utf-8')
            except UnicodeDecodeError:raise Refusal('cbor_invalid_utf8') from None
        if m in (4,5):
            if n is not None and n*(2 if m==5 else 1)>self.remaining:raise Refusal('cbor_item_limit')
            result=[] if m==4 else (MapEntries([]) if self.maps_as_pairs else {});seen=set();i=0
            while n is None or i<n:
                if n is None and self.data[self.pos:self.pos+1]==b'\xff':self.pos+=1;break
                v=self.item(depth+1)
                if m==4:result.append(v)
                else:
                    # Ledger map keys are ints/bytes (Plutus maps can use arrays).
                    k=key_identity(v)
                    if k in seen:raise Refusal('cbor_duplicate_map_key')
                    seen.add(k)
                    try:hash(v);stored=v
                    except TypeError:stored=k
                    decoded=self.item(depth+1)
                    if self.maps_as_pairs:result.pairs.append((v,decoded))
                    else:result[stored]=decoded
                i+=1
            return result
        if m==6:
            if n is None:raise Refusal('cbor_invalid_tag')
            return Tag(n,self.item(depth+1))
        if m==7 and n in (20,21,22):return {20:False,21:True,22:None}[n]
        raise Refusal('cbor_unsupported_simple')
    def skip(self):
        """Read past one item without building it; return its (start, end) byte span.

        For data no fact depends on: a transaction's witness set and auxiliary
        data. Plutus data and metadata may nest to any depth below the
        transaction size and repeat map keys, so neither refuses here: an
        explicit stack replaces recursion, and the input length and the shared
        item budget bound the work. Leaves go through `item`, so malformed
        heads, truncation, string chunks, UTF-8 and simple values refuse with
        the same reason codes as everywhere else.
        """
        start=self.pos;todo=[1]  # per open container: items left, or -1/-2 (array/map groups) until a break
        while todo:
            if todo[-1]<0:
                if self.data[self.pos:self.pos+1]==b'\xff':self.pos+=1;todo.pop()
                else:todo.append(-todo[-1])
                continue
            if todo[-1]==0:todo.pop();continue
            todo[-1]-=1;at=self.pos;self.remaining-=1
            if self.remaining<0:raise Refusal('cbor_item_limit')
            m,n=self.head()
            if m not in (4,5,6):self.pos=at;self.remaining+=1;self.item();continue
            if m==6:
                if n is None:raise Refusal('cbor_invalid_tag')
                todo.append(1)
            elif n is None:todo.append(-1 if m==4 else -2)
            else:
                if n*(2 if m==5 else 1)>self.remaining:raise Refusal('cbor_item_limit')
                todo.append(n*(2 if m==5 else 1))
        return start,self.pos
    def value(self):
        """Read one item of any depth and build it, with an explicit stack.

        For Plutus data, which may nest to any depth below the transaction size
        and repeat map keys: maps keep every pair in order (`MapEntries`), and
        neither depth nor keys refuse. Leaves go through `item`, as in `skip`.
        """
        holder=[];frames=[[holder,1,_NO_KEY]]  # per open container: [built value, items left or None until a break, pending map key]
        while True:
            frame=frames[-1];built,left,key=frame
            if left==0 or left is None and key is _NO_KEY and self.data[self.pos:self.pos+1]==b'\xff':
                if left is None:self.pos+=1
                frames.pop()
                if not frames:return holder[0]
                _attach(frames[-1],Tag(built[0],built[1]) if isinstance(built,_TagParts) else built);continue
            if left is not None:frame[1]=left-1
            at=self.pos;self.remaining-=1
            if self.remaining<0:raise Refusal('cbor_item_limit')
            m,n=self.head()
            if m in (4,5):
                if n is not None and n*(2 if m==5 else 1)>self.remaining:raise Refusal('cbor_item_limit')
                frames.append([[] if m==4 else MapEntries([]),None if n is None else n*(2 if m==5 else 1),_NO_KEY])
            elif m==6:
                if n is None:raise Refusal('cbor_invalid_tag')
                frames.append([_TagParts([n]),1,_NO_KEY])
            else:
                self.pos=at;self.remaining+=1;_attach(frame,self.item())

def key_identity(v):
    if type(v) in (int,bytes,str):return (type(v).__name__,v)
    if isinstance(v,Tag):return ('tag',v.number,key_identity(v.value))
    if isinstance(v,list):return ('list',tuple(key_identity(x) for x in v))
    if isinstance(v,MapEntries):return ('map',tuple(sorted(((key_identity(k),key_identity(x)) for k,x in v.pairs),key=repr)))
    if isinstance(v,dict):return ('map',tuple(sorted(((key_identity(k),key_identity(x)) for k,x in v.items()),key=repr)))
    raise Refusal('cbor_unsupported_map_key')

def uint(v):return type(v) is int and 0<=v<=0xffffffffffffffff

def inputs(value):
    if isinstance(value,Tag):
        if value.number!=258:raise Refusal('cbor_input_tag')
        value=value.value
    if not isinstance(value,list):raise Refusal('cbor_input_set')
    refs=set()
    for x in value:
        if not isinstance(x,list) or len(x)!=2 or not isinstance(x[0],bytes) or len(x[0])!=32 or not uint(x[1]) or x[1]>65535:raise Refusal('cbor_input_shape')
        k=(x[0],x[1])
        if k in refs:raise Refusal('cbor_duplicate_input')
        refs.add(k)
    return frozenset(refs)

def output_details(output):
    """Exact supported value/datum fields, or None: unknown is never negative evidence.

    The existing envelope/shape checks remain authoritative. Older or unfamiliar
    address/datum/script encodings retain the accounted full provider path.
    """
    address=output[0];value=output[1];datum=None;safe=True;datum_known=True
    if not isinstance(address,bytes) or not address:return None
    if isinstance(output,list):
        if len(output)==3:
            if output[2] is not None and (not isinstance(output[2],bytes) or len(output[2])!=32):return None
            datum=output[2]
    else:
        option=output.get(2)
        if option is not None:
            if not isinstance(option,list) or len(option)!=2 or type(option[0]) is not int:safe=False;datum_known=False
            elif option[0]==0 and isinstance(option[1],bytes) and len(option[1])==32:datum=option[1]
            elif option[0]==1 and isinstance(option[1],Tag) and option[1].number==24 and isinstance(option[1].value,bytes):
                try:decode_datum(option[1].value)
                except Refusal:safe=False
                datum=hashlib.blake2b(option[1].value,digest_size=32).digest()
            else:safe=False;datum_known=False
        script=output.get(3)
        # Script-reference language decoding is not qualified by this filter.
        # Even a structurally plausible reference keeps the full provider path.
        if script is not None:safe=False
    if uint(value):coin=value;multi={}
    elif isinstance(value,list) and len(value)==2 and uint(value[0]) and isinstance(value[1],dict):coin,multi=value
    else:return None
    assets=[]
    for policy,names in multi.items():
        if not isinstance(policy,bytes) or len(policy)!=28 or not isinstance(names,dict) or not names:return None
        for name,quantity in names.items():
            if not isinstance(name,bytes) or len(name)>32 or not uint(quantity) or quantity==0:return None
            assets.append((policy,name,quantity))
    return {'address':address,'value':str(coin),'assets':tuple(sorted(assets)),'datum_hash':datum,'classification_safe':safe,'datum_known':datum_known}

def decode_transaction(raw_hex,expected_hash):
    """Discovery facts of one transaction whose original body bytes hash to `expected_hash`.

    The witness set (checked to be a map) and auxiliary data are only skipped
    (`Decoder.skip`); `auxiliary_data_span` is the auxiliary data's byte span
    in the transaction. A reader of one metadata label decodes just that value
    from those bytes with an explicit depth bound, and treats an over-deep or
    undecodable value as absent, never as a refusal: anyone can put such
    metadata on chain.
    """
    if not isinstance(raw_hex,str) or len(raw_hex)>8*1024*1024 or len(raw_hex)%2 or re.fullmatch('[0-9a-f]+',raw_hex) is None:raise Refusal('cbor_invalid_hex')
    if not isinstance(expected_hash,str) or re.fullmatch('[0-9a-f]{64}',expected_hash) is None:raise Refusal('cbor_invalid_hash')
    raw=bytes.fromhex(raw_hex);d=Decoder(raw);m,n=d.head()
    if m!=4 or n not in (4,None):raise Refusal('cbor_unsupported_envelope')
    start=d.pos;body=d.item(1);end=d.pos
    actual=hashlib.blake2b(raw[start:end],digest_size=32).hexdigest()
    if actual!=expected_hash:raise Refusal('cbor_body_hash_mismatch')
    witness_head=d.data[d.pos:d.pos+1];d.skip();valid=d.item(1);auxiliary=d.skip()
    if n is None and d.take(1)!=b'\xff':raise Refusal('cbor_envelope_arity')
    if d.pos!=len(raw):raise Refusal('cbor_trailing_bytes')
    if not isinstance(body,dict) or not {0,1,2}.issubset(body) or any(type(k) is not int or k not in set(range(23))-{10,12} for k in body):raise Refusal('cbor_unsupported_body')
    if witness_head[0]>>5!=5 or type(valid) is not bool or not uint(body[2]):raise Refusal('cbor_unsupported_envelope')
    if not isinstance(body[1],list):raise Refusal('cbor_output_shape')
    for o in body[1]:
        if not ((isinstance(o,list) and len(o) in (2,3)) or (isinstance(o,dict) and {0,1}.issubset(o) and set(o)<={0,1,2,3})):raise Refusal('cbor_output_shape')
    mint=body.get(9,{})
    if not isinstance(mint,dict):raise Refusal('cbor_mint_shape')
    facts=[]
    for policy,names in mint.items():
        if not isinstance(policy,bytes) or len(policy)!=28 or not isinstance(names,dict) or not names:raise Refusal('cbor_mint_shape')
        for name,amount in names.items():
            if not isinstance(name,bytes) or len(name)>32 or type(amount) is not int or amount==0 or not -(1<<63)<=amount<(1<<63):raise Refusal('cbor_mint_shape')
            facts.append((policy,name,amount))
    returned=body.get(16)
    supported_return=isinstance(returned,list) and len(returned) in (2,3) or isinstance(returned,dict) and {0,1}.issubset(returned) and set(returned)<={0,1,2,3}
    return {'hash':bytes.fromhex(actual),'valid':valid,'ordinary':inputs(body[0]),'collateral':inputs(body.get(13,[])),
            'reference':inputs(body.get(18,[])),'mints':tuple(sorted(facts)),'output_count':len(body[1]),'collateral_return':16 in body,'body_bytes':end-start,
            'outputs':tuple(output_details(o) for o in body[1]),'collateral_output':output_details(returned) if supported_return else None,
            'auxiliary_data_span':auxiliary,'auxiliary_data':None if raw[auxiliary[0]:auxiliary[1]]==b'\xf6' else raw[auxiliary[0]:auxiliary[1]],
            'auxiliary_data_hash':body.get(7)}


# What `bridge_metadatum` finds for one metadata label: no such label, the label's metadatum (with the
# recipient bytes the node would read from its JSON rendering, or None), or nothing certain.
ABSENT='absent'
PRESENT='present'
UNKNOWN='unknown'
# A one-element array of one (possibly chunked) string: the deepest shape that can be a recipient.
RECIPIENT_DEPTH=2


def bridge_metadatum(aux,label):
    """(ABSENT|PRESENT|UNKNOWN, recipient) for metadata `label` in auxiliary data bytes `aux`.

    Only that label's value is decoded, with an explicit depth bound: an over-deep or undecodable
    value, or any shape but a one-element array holding bytes or a `0x`-hexadecimal text, is a
    metadatum without a recipient, never a refusal. cardano-db-sync renders bytes as `0x` + hex,
    which the node decodes back to the same bytes. Other labels and the scripts are only skipped.
    A structure this reader does not recognise, a repeated label with different readings or a
    malformed item gives UNKNOWN: anyone can put such data on chain."""
    if aux is None:return ABSENT,None
    try:
        d=Decoder(aux,max_bytes=len(aux)+1,max_items=len(aux)+1)
        m,n=d.head()
        if m==6 and n==259:
            m,n=d.head()
            if m!=5:return UNKNOWN,None
            found=False;i=0
            while n is None or i<n:
                if n is None and d.data[d.pos:d.pos+1]==b'\xff':d.pos+=1;break
                key=d.item(1);i+=1
                if key==0 and not found:found=True;break
                d.skip()
            if not found:return ABSENT,None
            m,n=d.head()
        elif m==4:
            if n is not None and n<1:return UNKNOWN,None
            m,n=d.head()
        elif m!=5:return UNKNOWN,None
        if m!=5:return UNKNOWN,None
        readings=set();i=0
        while n is None or i<n:
            if n is None and d.data[d.pos:d.pos+1]==b'\xff':break
            key=d.item(1);i+=1
            if type(key) is not int:return UNKNOWN,None
            value_start,value_end=d.skip()
            if key==label:readings.add(_recipient(aux[value_start:value_end]))
    except Refusal:
        return UNKNOWN,None
    if not readings:return ABSENT,None
    if len(readings)!=1:return UNKNOWN,None
    return PRESENT,readings.pop()


def _recipient(raw):
    """The recipient bytes of one metadatum's CBOR, or None when it cannot be one."""
    try:
        d=Decoder(raw,max_bytes=len(raw)+1,max_depth=RECIPIENT_DEPTH,max_items=len(raw)+1)
        value=d.item()
    except Refusal:
        return None
    if not isinstance(value,list) or len(value)!=1:return None
    element=value[0]
    if isinstance(element,bytes):return element
    if isinstance(element,str) and element.startswith('0x') and re.fullmatch('(?:[0-9a-fA-F]{2})*',element[2:]):return bytes.fromhex(element[2:])
    return None


# cardano-db-sync 13.8 stores a datum's JSON as NULL when it nests deeper than this, counting one
# level per object or array (`jsonbNestingLimit`, `jsonbNestingExceeded`).
DATUM_JSON_NESTING_LIMIT=2000

def decode_datum(raw):
    """cardano-db-sync's `datum.value` for the Plutus data `raw`: its detailed-schema JSON, or None
    (SQL NULL) when that JSON nests deeper than DATUM_JSON_NESTING_LIMIT.

    The ledger accepts Plutus data of any depth and maps that repeat a key, so neither refuses;
    malformed CBOR and shapes that are not Plutus data do. Built bottom-up with explicit stacks.
    """
    d=Decoder(raw);value=d.value()
    if d.pos!=len(raw):raise Refusal('cbor_datum_trailing_bytes')
    work=[(None,value)];done=[]  # (None, item) to convert; (kind, count) to assemble the last `count` results
    while work:
        kind,x=work.pop()
        if kind=='list':
            parts=_take(done,x);done.append(({'list':[p for p,_ in parts]},2+max((n for _,n in parts),default=0)))
        elif kind=='map':
            parts=_take(done,2*x);pairs=[{'k':parts[i][0],'v':parts[i+1][0]} for i in range(0,len(parts),2)]
            done.append(({'map':pairs},3+max(n for _,n in parts) if parts else 2))
        elif kind is not None:
            parts=_take(done,x[1]);done.append(({'constructor':x[0],'fields':[p for p,_ in parts]},2+max((n for _,n in parts),default=0)))
        elif type(x) is int:done.append(({'int':x},1))
        elif isinstance(x,bytes):done.append(({'bytes':x.hex()},1))
        elif isinstance(x,list):work.append(('list',len(x)));work.extend((None,v) for v in reversed(x))
        elif isinstance(x,MapEntries):
            work.append(('map',len(x.pairs)))
            for k,v in reversed(x.pairs):work.append((None,v));work.append((None,k))
        elif isinstance(x,Tag):
            if x.number in (2,3) and isinstance(x.value,bytes):
                n=int.from_bytes(x.value,'big');done.append(({'int':n if x.number==2 else -1-n},1));continue
            if 121<=x.number<=127:constructor=x.number-121;fields=x.value
            elif 1280<=x.number<=1400:constructor=x.number-1280+7;fields=x.value
            elif x.number==102 and isinstance(x.value,list) and len(x.value)==2:constructor,fields=x.value
            else:raise Refusal('cbor_datum_tag')
            if not uint(constructor) or not isinstance(fields,list):raise Refusal('cbor_datum_constructor')
            work.append(('constructor',(constructor,len(fields))));work.extend((None,v) for v in reversed(fields))
        else:raise Refusal('cbor_datum_shape')
    json_value,nesting=done[0]
    return None if nesting>DATUM_JSON_NESTING_LIMIT else json_value

def _take(stack,count):
    parts=stack[len(stack)-count:];del stack[len(stack)-count:];return parts
