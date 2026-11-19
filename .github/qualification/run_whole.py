#!/usr/bin/env python3
"""Hosted Docker only. Own generated PG clusters; never operator data."""
import hashlib,json,os,re,shutil,subprocess,sys,time,threading
from pathlib import Path
ROOT=Path(os.environ['GITHUB_WORKSPACE']);OUT=Path(os.environ['RUNNER_TEMP'])/'whole-evidence';OUT.mkdir(exist_ok=True)
CONFIG=json.loads((ROOT/'.github/qualification/config.json').read_text());PIN=CONFIG['source'];MODE=CONFIG['mode'];DIVISOR=CONFIG['divisor'];assert re.fullmatch('[0-9a-f]{40}',PIN)
TARGET=CONFIG.get('target','whole');FOCUS=TARGET in ['index','selection'];TEST={'index':'a7_index_generated','selection':'a7_select_generated'}.get(TARGET,'a7_whole_generated');HARNESS={'index':'a7_index.rs','selection':'a7_select.rs'}.get(TARGET,'a7_whole.rs')
RUST='rust:1.100.0-bookworm@sha256:ade39a710ef85f3506cffcfec3e8a24ba090badef1dff2fb798f71d878833fea'
PG='postgres:17.11-bookworm@sha256:67f41722b7a8cbdb868a44a4995c846eddfdc2973bccb291ce937dce88ad5675'
PREFIX='dsa-whole-'+os.environ['GITHUB_RUN_ID'];NETWORK=PREFIX+'-net';NAMES={k:PREFIX+'-'+k for k in ['source','destination','restored','worker']};VOLUMES={k:PREFIX+'-'+k+'-data' for k in ['source','destination','restored']};IDS={};STOP=threading.Event()
def call(args,**kw):return subprocess.run(args,check=True,text=True,**kw)
def output(args):return subprocess.check_output(args,text=True,stderr=subprocess.STDOUT)
def docker(*args):return output(['docker',*args])
def save(name,value):(OUT/name).write_text(json.dumps(value,indent=2)+'\n')
def inspect(name):return json.loads(docker('inspect',name))[0]
def cg(name):
 d=inspect(name);assert d['Id']==IDS[name] and d['State']['Running'];p=Path('/proc')/str(d['State']['Pid'])/'cgroup';line=[s for s in p.read_text().splitlines() if s.startswith('0::')][0];path=Path('/sys/fs/cgroup')/line[3:].lstrip('/');assert d['Id'] in str(path);return path

def stat_file(path):
 try:return path.read_text()
 except OSError:return None

def io_stat(path):
 rows={}
 for line in (path/'io.stat').read_text().splitlines():
  parts=line.split();rows[parts[0]]={k:int(v) for k,v in (x.split('=') for x in parts[1:])}
 return rows

def set_caps(name,read,write):
 path=cg(name);stats=io_stat(path);devices=[device for device,row in stats.items() if row.get('wbytes',0)>0];assert devices,('no actual physical IO devices',name,stats)
 for device in devices:
  line=f'{device} rbps={read if read else "max"} wbps={write} riops=max wiops=max\n';call(['sudo','tee',str(path/'io.max')],input=line,stdout=subprocess.DEVNULL)
 return {'cgroup':str(path),'devices':devices,'io_max':(path/'io.max').read_text(),'memory_max':(path/'memory.max').read_text(),'cpu_max':(path/'cpu.max').read_text()}

def probe(name,read,write):
 admitted=set_caps(name,read,write);results=[]
 for kind in ['write']+(['read'] if read else []):
  before=io_stat(cg(name));begin=time.monotonic()
  cmd='dd if=/dev/zero of=/var/lib/postgresql/data/whole-qualification-probe bs=1048576 count=16 oflag=direct conv=fsync status=none' if kind=='write' else 'dd if=/var/lib/postgresql/data/whole-qualification-probe of=/dev/null bs=1048576 count=16 iflag=direct status=none'
  call(['docker','exec','-u','postgres',name,'sh','-c',cmd],timeout=(600 if FOCUS else 90));elapsed=time.monotonic()-begin;after=io_stat(cg(name));limit=write if kind=='write' else read;minimum=16*1048576/limit*0.85
  assert elapsed>=minimum,(name,kind,'throttle not observed',elapsed,minimum,admitted)
  results.append({'kind':kind,'bytes':16*1048576,'seconds':elapsed,'observed_bytes_per_second':16*1048576/elapsed,'before':before,'after':after})
 call(['docker','exec','-u','postgres',name,'rm','--','/var/lib/postgresql/data/whole-qualification-probe'])
 return {'admission':admitted,'probes':results}

PG_QUERY="""SELECT json_build_object('at',clock_timestamp(),'database',current_database(),'database_bytes',pg_database_size(current_database()),'temporary_relations_bytes',coalesce((SELECT sum(pg_total_relation_size(c.oid)) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname LIKE 'pg_temp_%' AND c.relkind IN('r','m')),0),'index_progress',(SELECT json_agg(row_to_json(i)) FROM pg_stat_progress_create_index i WHERE datname=current_database()),'stats',(SELECT row_to_json(d) FROM pg_stat_database d WHERE datname=current_database()),'sessions',(SELECT json_agg(json_build_object('pid',pid,'application_name',application_name,'state',state,'wait_type',wait_event_type,'wait',wait_event,'xact_start',xact_start,'backend_xmin',backend_xmin,'query_start',query_start,'state_change',state_change,'idle_seconds',CASE WHEN state LIKE 'idle%' THEN extract(epoch FROM clock_timestamp()-state_change) ELSE 0 END,'query',left(query,16384))) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()))"""
def process_memory(init_pid,owner):
 rows=[];pending=[init_pid];seen=set()
 while pending and len(seen)<32:
  pid=pending.pop()
  if pid in seen:continue
  seen.add(pid);proc=Path('/proc')/str(pid)
  try:
   if owner not in (proc/'cgroup').read_text():continue
   status={k:v.strip() for k,v in (line.split(':',1) for line in (proc/'status').read_text().splitlines())}
   rows.append({'pid':pid,'comm':status.get('Name'),'VmRSS':status.get('VmRSS'),'VmHWM':status.get('VmHWM'),'PPid':status.get('PPid')});pending.extend(int(p) for p in (proc/'task'/str(pid)/'children').read_text().split())
  except OSError:pass
 return rows

def monitor():
 with (OUT/'telemetry.jsonl').open('w') as log:
  offset=0;phase=None
  while not STOP.wait(2):
   record={'unix':time.time(),'containers':{}}
   try:record['active_command']=json.loads((OUT/'active-command.json').read_text())
   except (OSError,ValueError):pass
   try:
    with (OUT/'convert.stderr').open() as events:
     events.seek(offset);lines=events.readlines();offset=events.tell()
    parsed=[]
    for line in lines:
     try:
      event=json.loads(line);parsed.append(event)
      if 'event' in event:phase=event['event']
     except ValueError:pass
    record['product_events_observed']=parsed;record['latest_product_event']=phase
   except OSError:pass
   databases={}
   try:databases=json.loads((OUT/'databases.json').read_text())
   except (OSError,ValueError):pass
   for role in ['source','destination','restored','worker']:
    name=NAMES[role]
    if name not in IDS:continue
    data={}
    try:
     d=inspect(name);data['state']=d['State'];data['memory_bytes']=d['HostConfig']['Memory'];data['cpu_nanos']=d['HostConfig']['NanoCpus']
     if d['State']['Running']:
      path=cg(name);data['memory_current']=stat_file(path/'memory.current');data['memory_peak']=stat_file(path/'memory.peak');data['io_stat']=io_stat(path);data['io_max']=stat_file(path/'io.max');data['cpu_stat']=stat_file(path/'cpu.stat');data['memory_stat']=stat_file(path/'memory.stat');data['memory_events']=stat_file(path/'memory.events');data['io_pressure']=stat_file(path/'io.pressure')
      if role=='worker':data['process_memory']=process_memory(d['State']['Pid'],d['Id'])
      if role!='worker':
       data['volume_bytes']=int(docker('exec',name,'du','-sb','/var/lib/postgresql/data').split()[0]);data['volume_allocated_bytes']=int(docker('exec',name,'du','-sB1','/var/lib/postgresql/data').split()[0]);data['wal_bytes']=int(docker('exec',name,'du','-sb','/var/lib/postgresql/data/pg_wal').split()[0]);database=databases.get(role)
       if database:
        query=docker('exec','-e','PGOPTIONS=-c statement_timeout=1500',name,'psql','-XAt','-v','ON_ERROR_STOP=1','-U','postgres','-d',database,'-c',PG_QUERY).strip();data['database']=json.loads(query)
    except Exception as error:data['observation_error']=type(error).__name__+':'+str(error)[:400]
    record['containers'][role]=data
   record['evidence_bytes']=sum(p.stat().st_size for p in OUT.rglob('*') if p.is_file())
   log.write(json.dumps(record)+'\n');log.flush()

def wait_postgres(name):
 for _ in range(120):
  if subprocess.run(['docker','exec',name,'pg_isready','-U','postgres'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode==0:return
  time.sleep(.5)
 raise RuntimeError('PostgreSQL did not become ready')

thread=None
try:
 # Product files are the explicit untouched pin, with only one opt-in test copy.
 assert output(['git','-C',str(ROOT/'product'),'rev-parse','HEAD']).strip()==PIN
 shutil.copy2(ROOT/'.github/qualification'/HARNESS,ROOT/'product/crates/db-sync-alt/tests'/(TEST+'.rs'))
 if TARGET=='selection':
  for name in ['a7_candidate_indexes.sql','a7_closure.sql']:shutil.copy2(ROOT/'.github/qualification'/name,ROOT/'product/crates/db-sync-alt/tests'/name)
 tracked=output(['git','-C',str(ROOT/'product'),'diff','--name-only']);assert tracked==''
 save('source.json',{'product_head':PIN,'wrapper_head':os.environ['GITHUB_SHA'],'product_tracked_diff':tracked,'injected_test_sha256':hashlib.sha256((ROOT/'.github/qualification'/HARNESS).read_bytes()).hexdigest(),'config':CONFIG,'synthetic':True,'injected_sql_sha256':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in (ROOT/'.github/qualification').glob('a7_*.sql')} })
 call(['docker','pull',RUST]);call(['docker','pull',PG]);save('images.json',json.loads(docker('image','inspect',RUST,PG)))
 target=Path(os.environ['RUNNER_TEMP'])/'whole-target';target.mkdir(exist_ok=True)
 build=['docker','run','--rm','--name',PREFIX+'-build','-e','CARGO_TARGET_DIR=/work/target','-e','CARGO_PROFILE_RELEASE_DEBUG=0','--mount',f'type=bind,src={ROOT}/product,dst=/work/product','--mount',f'type=bind,src={target},dst=/work/target','-w','/work/product',RUST,'cargo','test','--locked','--release','-p','db-sync-alt','--test',TEST,'--no-run','--message-format=json']
 with (OUT/'build.stdout').open('w') as stdout,(OUT/'build.stderr').open('w') as stderr:call(build,stdout=stdout,stderr=stderr)
 binaries=[json.loads(line) for line in (OUT/'build.stdout').read_text().splitlines() if line.startswith('{')];binary=[r['executable'] for r in binaries if r.get('reason')=='compiler-artifact' and r['target']['name']==TEST and r.get('executable')][0]
 save('binaries.json',{'test_binary':binary,'cli_sha256':hashlib.sha256((target/'release/db-sync-alt').read_bytes()).hexdigest(),'test_sha256':hashlib.sha256((target/Path(binary).relative_to('/work/target')).read_bytes()).hexdigest()})
 docker('network','create',NETWORK)
 for role in ['source','destination','restored']:
  volume=VOLUMES[role];docker('volume','create',volume)
  IDS[NAMES[role]]=docker('run','-d','--name',NAMES[role],'--network',NETWORK,'--network-alias',role,'--cpus','1','--memory','1g','--memory-swap','1g','--shm-size','256m','-e','POSTGRES_HOST_AUTH_METHOD=trust','--mount',f'type=volume,src={volume},dst=/var/lib/postgresql/data',PG,'-c','max_wal_size=256MB','-c','min_wal_size=32MB','-c','checkpoint_timeout=30s','-c','track_activity_query_size=16384','-c','log_min_duration_statement=1000','-c','log_temp_files=0').strip();wait_postgres(NAMES[role])
 save('startup-inspect.json',[inspect(NAMES[r]) for r in ['source','destination','restored']])
 # Destination is slowed from its first write. Population itself is source-only;
 # source write cap is positively installed after generated setup/checkpoint.
 admission={} if FOCUS else {role:probe(NAMES[role],4*1048576,2*1048576) for role in ['destination','restored']};save('destination-io-admission.json',admission)
 worker=['docker','run','-d','--user',f'{os.getuid()}:{os.getgid()}','--name',NAMES['worker'],'--network',NETWORK,'--cpus','1','--memory','1g','--memory-swap','1g','--mount',f'type=bind,src={ROOT}/product,dst=/work/product,readonly','--mount',f'type=bind,src={target},dst=/work/target,readonly','--mount',f'type=bind,src={OUT},dst=/evidence','-w','/work/product','-e',('DSA_TEST_DATABASE=host=destination port=5432 user=postgres dbname=postgres sslmode=disable' if FOCUS else 'DSA_TEST_DATABASE=host=source port=5432 user=postgres dbname=postgres sslmode=disable'),'-e','DSA_DEST_DATABASE=host=destination port=5432 user=postgres dbname=postgres sslmode=disable','-e','DSA_RESTORED_DATABASE=host=restored port=5432 user=postgres dbname=postgres sslmode=disable','-e','DSA_QUALIFICATION_OUT=/evidence','-e',f'DSA_WHOLE_DIVISOR={DIVISOR}','-e',f'DSA_WHOLE_MODE={MODE}',RUST,binary,'--ignored','--exact',({'index':'private_index_focus','selection':'selection_focus'}.get(TARGET,'whole_conversion_qualification')),'--nocapture']
 IDS[NAMES['worker']]=output(worker).strip();thread=threading.Thread(target=monitor,daemon=True);thread.start();admitted=False;reclaimed_nonce=None
 while inspect(NAMES['worker'])['State']['Running']:
  if not admitted and (OUT/'fixture-ready.json').exists():
   admission.update({'destination':probe(NAMES['destination'],4*1048576,2*1048576)}) if FOCUS else admission.update({'source':probe(NAMES['source'],None,2*1048576)});save('io-admission.json',admission);save('baseline-volumes.json',{role:{'allocated_bytes':int(docker('exec',NAMES[role],'du','-sB1','/var/lib/postgresql/data').split()[0]),'apparent_bytes':int(docker('exec',NAMES[role],'du','-sb','/var/lib/postgresql/data').split()[0])} for role in ['source','destination','restored']});save('io-ready.json',{'ready':True,'at':time.time(),'all_caps_positively_probed':True});admitted=True
  time.sleep(1)
 save('worker-exit.json',inspect(NAMES['worker'])['State']);print(docker('logs',NAMES['worker']),flush=True);assert inspect(NAMES['worker'])['State']['ExitCode']==0,'qualification binary failed'
 save(('index-experiment-success.json' if FOCUS else 'qualification-success.json'),{'source':PIN,'config':CONFIG,'admitted':admitted,'generated_only':True,'historical_acceptance':False})
finally:
 STOP.set()
 if thread:thread.join(timeout=30)
 for role,name in NAMES.items():
  if name not in IDS:continue
  try:
   save(role+'-final-inspect.json',inspect(name));(OUT/(role+'-docker.log')).write_text(docker('logs',name))
  except Exception:pass
  try:docker('stop','-t','120',name)
  except Exception:pass
  try:docker('rm',name)
  except Exception:pass
 for volume in VOLUMES.values():
  try:docker('volume','rm',volume)
  except Exception:pass
 try:docker('network','rm',NETWORK)
 except Exception:pass
 # Do not upload full generated DBs, test executables or bulk snapshots.
 for name in ['source.capture.json','converted.snapshot','explicit.snapshot']:
  path=OUT/name
  if path.exists():
   try:save(name+'.manifest.json',{'bytes':path.stat().st_size,'sha256':hashlib.sha256(path.read_bytes()).hexdigest()});path.unlink()
   except OSError as error:save(name+'.retention-error.json',{'error':str(error)});call(['sudo','chmod','u+r,g+r,o+r',str(path)]);path.unlink()
 try:
  points=[json.loads(line) for line in (OUT/'telemetry.jsonl').read_text().splitlines()]
  maxima={};source_released=[];source_held=[];actual_copy_queries={};simultaneous=[];idle_maxima={};baseline=json.loads((OUT/'baseline-volumes.json').read_text())
  for point in points:
   peak=point.get('evidence_bytes',0);incremental=peak
   for role,data in point['containers'].items():
    current=maxima.setdefault(role,{})
    for key in ['volume_bytes','volume_allocated_bytes','wal_bytes','memory_current','memory_peak']:
     if data.get(key) is not None:current[key]=max(current.get(key,0),int(data[key]))
    peak+=data.get('volume_allocated_bytes',0)
    if role=='source':incremental+=max(0,data.get('volume_allocated_bytes',0)-baseline['source']['allocated_bytes'])
    elif role in ['destination','restored']:incremental+=data.get('volume_allocated_bytes',0)
    database=data.get('database') or {}
    for key in ['database_bytes','temporary_relations_bytes']:
     if database.get(key) is not None:current[key]=max(current.get(key,0),database[key])
    for session in database.get('sessions') or []:
     idle_maxima[role]=max(idle_maxima.get(role,0),float(session.get('idle_seconds') or 0))
     if role=='source' and session.get('application_name')=='db-sync-alt-convert-source':
      row={'unix':point['unix'],'phase':point.get('latest_product_event'),'active_command':point.get('active_command'),**session}
      (source_held if session.get('backend_xmin') is not None else source_released).append(row)
     query=session.get('query','')
     if query.startswith('COPY (WITH RECURSIVE page'):
      actual_copy_queries.setdefault(role,query)
   simultaneous.append({'unix':point['unix'],'allocated_pgdata_plus_evidence_bytes':peak,'incremental_work_bytes_above_existing_source':incremental})
  events=[]
  for line in ((OUT/'convert.stderr').read_text() if (OUT/'convert.stderr').exists() else '').splitlines():
   try:events.append(json.loads(line))
   except ValueError:pass
  pages=[event for event in events if event.get('event')=='source_page_finished']
  families=[event for event in events if event.get('event')=='source_family_finished']
  save('resource-summary.json',{'scope':'sampled generated qualification, not real archive','sample_period_seconds':2,'containers_maxima':maxima,'peak_simultaneous_allocated_pgdata_plus_evidence':max(simultaneous,key=lambda p:p['allocated_pgdata_plus_evidence_bytes']) if simultaneous else None,'peak_incremental_work_bytes_above_existing_source':max(simultaneous,key=lambda p:p['incremental_work_bytes_above_existing_source']) if simultaneous else None,'maximum_sampled_idle_seconds_by_role':idle_maxima,'source_snapshot_held_samples':len(source_held),'source_released_samples':len(source_released),'source_released_during_local_selection':[r for r in source_released if str(r['phase'] or '').startswith('conversion_local_selection')],'actual_recursive_copy_queries_observed':actual_copy_queries,'raw_families':families,'page_count':len(pages),'maximum_source_page_seconds':max((r['source_seconds'] for r in pages),default=None),'maximum_destination_write_seconds':max((r['destination_write_seconds'] for r in pages),default=None),'sampled_peak_may_miss_sub2second_spikes':True})
 except Exception as error:save('resource-summary-error.json',{'error':repr(error)})
 save('SHA256SUMS.json',{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in OUT.iterdir() if p.is_file() and p.name!='SHA256SUMS.json'})
