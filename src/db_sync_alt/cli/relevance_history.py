"""Explicit one-time maintenance; deliberately absent from the public command list."""
import argparse
import sys
from pathlib import Path
from db_sync_alt.contract import Profile,Refusal,canonical
from db_sync_alt.resources import profile_dir
from db_sync_alt.store.database import PgStore
from db_sync_alt.sources.blockfrost.relevance import trust_history

def main(argv=None):
    parser=argparse.ArgumentParser(prog='db-sync-alt _trust-relevance-history')
    parser.add_argument('--destination-service',required=True)
    parser.add_argument('--network',required=True)
    parser.add_argument('--profile-dir',type=Path)
    args=parser.parse_args(argv)
    try:
        profile=Profile(args.profile_dir or profile_dir(args.network))
        if profile.network.network!=args.network:raise Refusal('relevance_parent_changed')
        if profile.network.network=='mainnet':raise Refusal('relevance_trust_mainnet_refused')
        with PgStore(args.destination_service,network=args.network) as store:
            result=trust_history(store,profile)
        print(canonical(result));return 0
    except Refusal as error:
        print(error.code,file=sys.stderr);return 1
