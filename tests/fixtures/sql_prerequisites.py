"""Assertions on actual acquired v2 roots used by combined feature fixtures."""
import json
from db_sync_alt.verify.prerequisites import prerequisite_coverage


def assert_v2_root(store):
    g=store.generation();a=g['manifest']['acquisition'];state=prerequisite_coverage(store,g)
    assert g['parent'] is None and g['source']=='cardano-db-sync-postgresql'
    assert a=={'version':2,'window_blocks':10000}
    assert state['complete'] and state['position']==g['start_height']
    assert len(state['chunks'])==(g['start_height']+9999)//10000
    print('SQL_V2_ROOT '+json.dumps({'generation':g['id'],'version':2,'window_blocks':10000,'start':g['start_height'],'position':state['position'],'chunk_count':len(state['chunks']),'coverage_complete':True,'dbsync_version':g['manifest']['source_identity']['dbsync_version']}),flush=True)
