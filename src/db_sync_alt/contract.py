"""Versioned, provider-independent fact contract. No storage side effects; network values come from the profile."""
from pathlib import Path
import hashlib
import json
import re

FORMAT = 'midnight-compact-cardano-v2'
# The nonce-only format of stores written before epoch parameters. Such a store is still
# recognised and verified as it was sealed, and `db-sync-alt upgrade` moves it to FORMAT.
LEGACY_FORMAT = 'midnight-compact-cardano-v1'
EPOCH_CAPABILITY = 'epoch_max_tx_size_v1'
# Bridge metadata: midnight-node 2.1.101 reads tx_metadata.json of this key for the transactions that
# produce an output at the ICS address. A generation acquired with that metadata carries the capability.
BRIDGE_METADATA_KEY = 6500973
BRIDGE_CAPABILITY = 'bridge_metadata_v1'
# Registered candidates are not served: the registered-candidates address is not a profile address, so the
# node's registration RPCs read no registrations. Node 2.1.101 selects committees without registrations.
CANDIDATES_CAPABILITY = 'registered_candidates_unserved'
SCHEMA_VERSION = (15, 48, 6)
PROFILE_FILES = ('cnight-config', 'federated-authority-config', 'permissioned-candidates-config',
                 'registered-candidates-addresses', 'ics-config', 'reserve-config')
# Ordered columns; all hash/policy/name fields are bytes; quantities are decimal TEXT.
TABLES = {
 'blocks': ('hash','height','parent_hash','epoch','slot','time','tx_count'),
 'transactions': ('hash','block_hash','tx_index','valid'),
 'outputs': ('tx_hash','output_index','address','value','datum_hash'),
 'inputs': ('tx_hash','origin_hash','origin_index'),
 'assets': ('policy','name'),
 'output_assets': ('tx_hash','output_index','policy','name','quantity'),
 'mints': ('tx_hash','policy','name','quantity'),
 'datums': ('hash','value'),
 'epoch_nonces': ('epoch','nonce','max_tx_size'),
 'epoch_stakes': ('epoch','pool_hash','amount'),
 # One row per transaction with an output at the ICS address in a covered block: `present` 1 with the
 # metadatum of BRIDGE_METADATA_KEY as canonical JSON text (or None for a source NULL), 0 without one.
 'tx_metadata': ('tx_hash','present','json'),
}
KEYS = {'blocks':('hash',),'transactions':('hash',),'outputs':('tx_hash','output_index'),
 'inputs':('tx_hash','origin_hash','origin_index'),'assets':('policy','name'),
 'output_assets':('tx_hash','output_index','policy','name'),'mints':('tx_hash','policy','name'),
 'datums':('hash',),'epoch_nonces':('epoch',),'epoch_stakes':('epoch','pool_hash'),'tx_metadata':('tx_hash',)}
LEGACY_TABLES = {**{k:v for k,v in TABLES.items() if k!='tx_metadata'}, 'epoch_nonces': ('epoch','nonce')}
LEGACY_KEYS = {k:v for k,v in KEYS.items() if k in LEGACY_TABLES}

class Refusal(Exception):
    """Safe machine-readable code, never source driver text or secrets."""
    def __init__(self, code):
        self.code = code
        super().__init__(code)

# These bounds are the existing PostgreSQL projection domains. Enforce them
# at ingestion and during streaming verification, including hand-edited files.
# `valid` is cardano-db-sync's valid_contract: 0 for a phase-2-invalid transaction, which, as in
# cardano-db-sync, holds only its collateral inputs (inputs) and its collateral return (outputs).
NUMERIC_FIELDS={
 'blocks':{'height':(0,2**31-1),'epoch':(0,2**31-1),'slot':(0,2**63-1),'tx_count':(0,2**63-1)},
 'transactions':{'tx_index':(0,2**31-1),'valid':(0,1)},
 'outputs':{'output_index':(0,32767),'value':(0,2**64-1)},
 'inputs':{'origin_index':(0,32767)},
 'output_assets':{'output_index':(0,32767),'quantity':(0,2**64-1)},
 'mints':{'quantity':(-(10**20-1),10**20-1)},
 'epoch_nonces':{'epoch':(0,2**31-1),'max_tx_size':(0,2**31-1)},
 'epoch_stakes':{'epoch':(0,2**31-1),'amount':(0,2**64-1)},
 'tx_metadata':{'present':(0,1)},
}
LEGACY_NUMERIC_FIELDS={**NUMERIC_FIELDS,'transactions':{'tx_index':(0,2**31-1),'valid':(1,1)},'epoch_nonces':{'epoch':(0,2**31-1)}}
def validate_numeric(table,row,*,legacy=False):
    for column,(lower,upper) in (LEGACY_NUMERIC_FIELDS if legacy else NUMERIC_FIELDS).get(table,{}).items():
        if column not in row:raise Refusal('invalid_numeric_domain')
        value=row[column]
        if column in ('value','quantity','amount'):
            if not isinstance(value,str) or not re.fullmatch(r'-?(0|[1-9][0-9]*)',value) or len(value)>21:raise Refusal('invalid_numeric_domain')
            parsed=int(value)
            if str(parsed)!=value:raise Refusal('invalid_numeric_domain')
        else:
            if type(value) is not int:raise Refusal('invalid_numeric_domain')
            parsed=value
        if not lower<=parsed<=upper:raise Refusal('invalid_numeric_domain')

def epoch_size(value):
    """An epoch's maximum transaction size as the source states it: an integer 0..2147483647,
    zero included. Never a protocol default, never coerced from another type."""
    if type(value) is not int or not 0<=value<=2**31-1:raise Refusal('invalid_epoch_max_tx_size')
    return value

def metadata_json(row):
    """A tx_metadata fact's `json`: None, or canonical JSON text; None whenever `present` is 0."""
    value=row.get('json')
    if value is None:return None
    if row.get('present')!=1 or not isinstance(value,str):raise Refusal('invalid_bridge_metadata')
    try:parsed=json.loads(value)
    except (ValueError,RecursionError):raise Refusal('invalid_bridge_metadata') from None
    if canonical(parsed)!=value:raise Refusal('invalid_bridge_metadata')
    return value

# Manifest capabilities of a generation: the nonce-only format had no epoch parameters and no bridge
# metadata, and its `candidates_empty` stated an empty registered-candidates address. `bridge_metadata_v1`
# is true exactly for a generation whose event range was acquired with bridge metadata (Blockfrost); a
# cardano-db-sync acquisition never reads tx_metadata.
CAPABILITIES={CANDIDATES_CAPABILITY:True,EPOCH_CAPABILITY:True,BRIDGE_CAPABILITY:False}
LEGACY_CAPABILITIES={'bridge_qualified':False,'candidates_empty':True}
def generation_capabilities(*,legacy=False,bridge_metadata=False):
    out=dict(LEGACY_CAPABILITIES if legacy else CAPABILITIES)
    if BRIDGE_CAPABILITY in out:out[BRIDGE_CAPABILITY]=bridge_metadata is True
    return out

def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False)

def jsonable(value):
    if isinstance(value, (bytes, bytearray, memoryview)): return bytes(value).hex()
    if isinstance(value, dict): return {k:jsonable(v) for k,v in value.items()}
    if isinstance(value, (tuple,list)): return [jsonable(v) for v in value]
    return value

def digest(value): return hashlib.sha256(canonical(jsonable(value)).encode()).hexdigest()

class Profile:
    """A packaged acquisition profile: the network's values and the Midnight configuration files.

    `network` names the expected network (a name or a NetworkProfile); a profile of
    another network is refused.
    """
    def __init__(self, directory, network=None):
        from db_sync_alt import networks
        self.directory = Path(directory).resolve(strict=True)
        local = networks.load(self.directory)
        if network is not None and local.network != (network if isinstance(network, str) else network.network):
            raise Refusal('network_mismatch')
        self.network = networks.network_profile(local)
        self.configs = {n:json.loads((self.directory/(n+'.json')).read_text()) for n in PROFILE_FILES}
        info=json.loads((self.directory/'profile.json').read_text())
        if info.get('network') != self.network.network: raise Refusal('unsupported_network')
        if info.get('name') != self.network.profile: raise Refusal('unsupported_profile')
        manifest=json.loads((self.directory/'source-manifest.json').read_text())
        if manifest.get('commit') != self.network.raw['provenance']['midnight_configs']['commit']: raise Refusal('unsupported_profile')
        for n in PROFILE_FILES:
            if hashlib.sha256((self.directory/(n+'.json')).read_bytes()).hexdigest()!=manifest['files'][n+'.json']['sha256']:
                raise Refusal('profile_fingerprint_mismatch')
        if digest(self.configs)!=self.network.config_sha256: raise Refusal('untrusted_profile_configuration')
        cn=self.configs['cnight-config']; self.start=(cn['next_cardano_position']['block_number'],cn['next_cardano_position']['tx_index_in_block'])
        if self.start!=self.network.start: raise Refusal('unsupported_cursor')
        a=cn['addresses']; self.night=a['cnight_policy_id']+a['cnight_asset_name'].encode().hex()
        self.mapping=a['mapping_validator_address']
        alphabet='qpzry9x8gf2tvdw0s3jn54khce6mua7l'; acc=bits=0; raw=[]
        for c in self.mapping.rsplit('1',1)[1][:-6]:
            acc=(acc<<5)|alphabet.index(c);bits+=5
            while bits>=8:bits-=8;raw.append((acc>>bits)&255)
        self.auth=bytes(raw)[1:29].hex()
        self.permissioned=self.configs['permissioned-candidates-config']['permissioned_candidates_policy_id'].removeprefix('0x')
        self.governance={v['policy_id'] for v in self.configs['federated-authority-config'].values()}
        # The registered-candidates address is not a profile address: node 2.1.101 selects committees without
        # registrations, so an output there is kept only for a profile asset, as at any other address.
        self.candidate=self.configs['registered-candidates-addresses']['committee_candidates_address']
        self.addresses={self.mapping}|{v['address'] for v in self.configs['federated-authority-config'].values()}
        self.units={self.night,self.auth,self.permissioned}|self.governance
        for key,address in [('ics-config','illiquid_circulation_supply_validator_address'),('reserve-config','reserve_validator_address')]:
            c=self.configs[key];self.addresses.add(c[address]);self.units.add(c['asset']['policy_id'].removeprefix('0x')+c['asset']['asset_name'].encode().hex())
        # The bridge reads the ICS and reserve addresses: from bridge coverage on, their outputs keep every asset.
        self.ics=self.configs['ics-config']['illiquid_circulation_supply_validator_address']
        self.bridge_addresses=frozenset({self.ics,self.configs['reserve-config']['reserve_validator_address']})
        self.identity={'network':self.network.network,'config_sha256':digest(self.configs),'genesis':self.network.genesis,'configs':self.configs,'profile':info['name']}
    @property
    def security(self):
        """The network's security parameter k: witness = target + k."""
        return self.network.security
    @property
    def contract(self):
        return contract_for(self.network)
    def required(self, policy, name):
        unit=bytes(policy).hex()+bytes(name).hex()
        return unit in self.units or unit[:56] in self.governance
    def relevant(self,address,assets):
        return address in self.addresses or any(self.required(p,n) for p,n in assets)
    def kept_assets(self,address,assets,*,covered):
        """The assets of an output that are stored: profile units, or every asset of an ICS or reserve
        output stored by a bridge-covered generation. `assets` maps (policy, name) to a quantity."""
        if covered and address in self.bridge_addresses:return dict(assets)
        return {k:v for k,v in assets.items() if self.required(*k)}

def required_epochs(start_epoch, target_epoch):
    """Epochs whose nonce, stakes and end block a generation holds: from two before the
    start cursor's epoch (never below epoch 0) through the target's epoch."""
    return list(range(max(0, start_epoch - 2), target_epoch + 1))


def contract_for(network,*,legacy=False):
    """The fact contract of a network: stored when a database is created, its digest in every manifest.
    `legacy` gives the nonce-only contract of stores written before epoch parameters."""
    contract={'block_membership':'ancestor-committed-prefix-v1','format':LEGACY_FORMAT if legacy else FORMAT,'algorithm':network.algorithm,'security':network.security,
              'tables':LEGACY_TABLES if legacy else TABLES,'keys':LEGACY_KEYS if legacy else KEYS,
              'quantities':'canonical decimal TEXT','hashes':'raw bytes','timestamps':'UTC ISO-8601 microseconds',
              'datum':'canonical semantic JSON','schema_version':SCHEMA_VERSION}
    if legacy:contract['bridge_qualified']=False
    else:
        contract['datum']=('canonical semantic JSON; NULL where cardano-db-sync 13.8 stores NULL (JSON nested deeper than 2000 levels);'
                           ' an output datum hash without a preimage on chain has no datum')
        # The node reads MAX(max_tx_size) of the cursor block's epoch: every source row of the
        # epoch is validated and their maximum is the fact.
        contract['epoch_parameters']={'version':1,'capability':EPOCH_CAPABILITY,
                                      'max_tx_size':'maximum of the validated per-epoch source values, integers 0..2147483647'}
        # Forward-only bridge readiness: tx_metadata of the bridge key for every transaction with an
        # output at the ICS address, from the first generation with the capability on.
        contract['bridge_metadata']={'version':1,'capability':BRIDGE_CAPABILITY,'key':BRIDGE_METADATA_KEY,
                                     'transactions':'every transaction with an output at the ICS address in a covered block',
                                     'json':'Blockfrost json_metadata as jsonb, integral numbers as integers; NULL for a NULL, a U+0000 or a nesting deeper than 2000'}
    return contract
