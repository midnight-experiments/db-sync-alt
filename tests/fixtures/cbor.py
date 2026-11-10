"""Cardano CBOR builders and captured Blockfrost CBOR samples for tests."""
import hashlib
from pathlib import Path
from db_sync_alt.sources.blockfrost.cbor import Tag


F=Path(__file__).parent/'data'/'blockfrost'

def enc(x):
    def head(m,n):
        if n<24:return bytes([m*32+n])
        for a,size in ((24,1),(25,2),(26,4),(27,8)):
            if n<1<<(8*size):return bytes([m*32+a])+n.to_bytes(size,'big')
    if x is True:return b'\xf5'
    if x is False:return b'\xf4'
    if x is None:return b'\xf6'
    if type(x) is int:return head(0,x) if x>=0 else head(1,-1-x)
    if isinstance(x,bytes):return head(2,len(x))+x
    if isinstance(x,str):return head(3,len(x.encode()))+x.encode()
    if isinstance(x,list):return head(4,len(x))+b''.join(enc(v) for v in x)
    if isinstance(x,dict):return head(5,len(x))+b''.join(enc(k)+enc(v) for k,v in x.items())
    if isinstance(x,Tag):return head(6,x.number)+enc(x.value)
    raise TypeError(x)

def tx(body,valid=True,auxiliary=b'\xf6'):
    """(hex transaction, body hash); `auxiliary` is the encoded auxiliary data, null by default."""
    raw=enc(body);return (b'\x84'+raw+enc({})+enc(valid)+auxiliary).hex(),hashlib.blake2b(raw,digest_size=32).hexdigest()

def body():return {0:[[b'a'*32,0]],1:[],2:3}
