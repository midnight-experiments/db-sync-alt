"""One-shot direct PostgreSQL acquisition with explicit source selection."""
import argparse
import json
from pathlib import Path
import re
import shutil
import signal
import sys
import threading
from types import SimpleNamespace
import psycopg
from db_sync_alt.networks import names as network_names
from db_sync_alt.resources import profile_dir
from db_sync_alt.contract import Profile, Refusal, canonical
from db_sync_alt.store.database import PgStore
from db_sync_alt.errors import LifecycleFailure, TransientSyncFailure


def blockfrost_once(store, profile, *, target_height=None, request_budget=None,
                    stop_event=None, client=None, relevance_enabled=False):
    from db_sync_alt.sources.blockfrost.sync import SyncDriver
    from db_sync_alt.service.health import HealthState
    stop_event = stop_event or threading.Event()
    ready = store.generation(); pending = store.pending()
    if pending and pending['source'] != 'blockfrost-cardano':
        raise Refusal('resume_source_mismatch')
    if pending:
        if target_height is not None and target_height != pending['target_height']:
            raise Refusal('resume_target_mismatch')
        target_height = pending['target_height']
    if type(target_height) is not int or not ready['target_height'] < target_height <= ready['target_height']+1000:
        raise Refusal('explicit_blockfrost_target_required')
    active = store.sync_job('provider-active')
    if active and active['state'] != 'completed':
        if active['target'] != target_height:raise Refusal('resume_target_mismatch')
        if request_budget is not None and request_budget != active['budget']:raise Refusal('sync_job_budget_reset')
        request_budget = active['budget']
    options={'max_batch_blocks':target_height-ready['target_height'],
             'publication_interval_seconds':1,'acquisition_request_budget':request_budget or 10000,'relevance_enabled':relevance_enabled}
    # Keep an existing current-day poll allowance as it is; never reset it.
    import datetime as dt
    day=dt.datetime.now(dt.timezone.utc).date().isoformat()
    poll=store.sync_job('provider-poll-'+day)
    if poll:options['daily_poll_request_budget']=poll['budget']
    health=HealthState(service_version='0.4.2',schema_version=1,network=profile.network.network,profile=profile.identity['config_sha256'])
    serial=store.state()['publication_serial']
    if store.supports_incremental:store.certify_ready_full()
    SyncDriver(SimpleNamespace(sync=options),client=client,profile=profile,fixed_target=target_height).run_once(store,health,stop_event)
    if stop_event.is_set():raise Refusal('interrupted')
    if store.generation()['target_height'] != target_height or store.state()['publication_serial'] != serial+1:
        raise Refusal('publication_not_completed')
    return {'source':'blockfrost','generation':store.generation()['id'],'target':target_height,
            'witness':store.generation()['witness_height'],'publication_serial':serial+1}


def parser():
    p=argparse.ArgumentParser(prog='db-sync-alt acquire',description=__doc__)
    p.add_argument('--source',required=True,choices=['db-sync','blockfrost'])
    p.add_argument('--destination-service',required=True)
    p.add_argument('--network',required=True,choices=network_names(),help='Cardano network of the destination database')
    p.add_argument('--profile-dir',type=Path,help="Copy of the network's packaged acquisition profile (default: the packaged profile)")
    p.add_argument('--create',action='store_true')
    p.add_argument('--target-height',type=int)
    p.add_argument('--window-blocks',type=int,default=1000)
    p.add_argument('--source-service')
    p.add_argument('--source-attestation',type=Path)
    p.add_argument('--request-budget',type=int)
    p.add_argument('--relevance',action=argparse.BooleanOptionalAction,dest='relevance_enabled',default=False,help='Enable coverage-certified relevance filtering for Blockfrost following (default: off)')
    p.add_argument('--reserve',action='append',default=[],metavar='PATH:BYTES',
                   help='Free space that must stay on the volume of PATH, in bytes; repeat for each volume (default /:10737418240, 10 GiB)')
    return p


def main(argv=None):
    args=parser().parse_args(argv);stop=threading.Event()
    for sig in (signal.SIGINT,signal.SIGTERM):signal.signal(sig,lambda *_:stop.set())
    def progress(phase,**fields):
        if stop.is_set():raise Refusal('interrupted')
        print(canonical({'phase':phase,**fields}),file=sys.stderr,flush=True)
    try:
        if args.source=='db-sync' and (not args.source_service or not args.source_attestation or args.request_budget is not None or args.relevance_enabled):
            raise Refusal('invalid_source_configuration')
        if args.source=='blockfrost' and (args.create or args.source_service or args.source_attestation):
            raise Refusal('blockfrost_requires_ready_destination')
        reserves={}
        for item in args.reserve or ['/:10737418240']:
            path,floor=item.rsplit(':',1)
            if not Path(path).is_absolute() or int(floor)<=0:raise Refusal('invalid_storage_reserve')
            reserves[path]=int(floor)
        def reserve():
            if any(shutil.disk_usage(path).free<floor for path,floor in reserves.items()):raise Refusal('storage_low')
        reserve();profile=Profile(args.profile_dir or profile_dir(args.network),args.network)
        with PgStore(args.destination_service,create=args.create,allow_incomplete=args.source=='db-sync',
                     reserve_check=reserve,progress_callback=progress,network=profile.network) as store:
            if args.source=='db-sync':
                from db_sync_alt.sources.dbsync.acquire import CountedSQLSource,acquire
                if args.source_attestation.stat().st_size>65536:raise Refusal('invalid_attestation_file')
                attestation=json.loads(args.source_attestation.read_text())
                source=CountedSQLSource(args.source_service,profile,attestation)
                report=acquire(source,store,profile,target_height=args.target_height,
                    window_blocks=args.window_blocks,stop_event=stop)
            else:
                report=blockfrost_once(store,profile,target_height=args.target_height,
                    request_budget=args.request_budget,stop_event=stop,relevance_enabled=args.relevance_enabled)
            for warning in report.get('warnings', []):
                # Said on stderr as well: the operator acts on it now, not when reading the report.
                print(canonical({'warning':warning,**report.get('bridge_coverage_ended',{})}),file=sys.stderr)
            print(canonical(report));return 0
    except TransientSyncFailure as error:
        print(canonical({'error_code':error.code,'retry_at':error.retry_at}),file=sys.stderr);return 75
    except (Refusal,LifecycleFailure) as error:
        code=error.code if re.fullmatch('[a-z_]+',error.code) else 'acquisition_failed'
        print(canonical({'error_code':code}),file=sys.stderr);return 2
    except (OSError,ValueError,KeyError,TypeError,psycopg.Error):
        print(canonical({'error_code':'invalid_configuration_or_database'}),file=sys.stderr);return 2

