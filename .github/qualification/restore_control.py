"""Generated-only paired real-CLI COPY stall; no source population or conversion."""
import hashlib
import json
import subprocess
import threading
import time
from pathlib import Path


def socket_queue(text, client_port, target_ip):
    for line in text.splitlines():
        fields = line.split()
        if len(fields) >= 5 and fields[0] == 'ESTAB':
            if fields[3].endswith(':' + str(client_port)) and fields[4] == target_ip + ':5432':
                return int(fields[2])
    return None


def run(g):
    root, out, config = g['ROOT'], g['OUT'], g['CONFIG']
    docker, output, save, inspect = (g[k] for k in ['docker', 'output', 'save', 'inspect'])
    pg, rust, prefix = (g[k] for k in ['PG', 'RUST', 'PREFIX'])
    names, ids = g['NAMES'], g['IDS']
    assert output(['git', '-C', str(root/'product'), 'rev-parse', 'HEAD']).strip() == g['PIN']
    assert output(['git', '-C', str(root/'product'), 'diff', '--name-only']) == ''
    save('source.json', {'product_head':g['PIN'], 'wrapper_head':g['os'].environ['GITHUB_SHA'], 'product_tracked_diff':'', 'config':config, 'synthetic':True, 'scope':'direct paired CLI restore only, no source generation/conversion'})
    reused = root/'reuse'
    seal = json.loads((reused/'SHA256SUMS.json').read_text())
    for name, digest in seal.items():
        assert Path(name).name == name and hashlib.sha256((reused/name).read_bytes()).hexdigest() == digest
    for name, key in [('representative.snapshot','reuse_snapshot_sha256'), ('source.capture.json','reuse_capture_sha256')]:
        assert hashlib.sha256((reused/name).read_bytes()).hexdigest() == config[key]
        g['shutil'].copyfile(reused/name, out/name)
    original = json.loads((reused/'mini-conversion.json').read_text())['verified']
    save('reuse-origin.json', {'run':config['reuse_run'], 'verified_manifest_files':len(seal), 'no_source_generation':True, 'no_source_conversion':True, 'expected_verified':original})
    g['call'](['docker','pull',rust]);g['call'](['docker','pull',pg])
    save('images.json', json.loads(docker('image','inspect',rust,pg)))
    target = Path(g['os'].environ['RUNNER_TEMP'])/'control-target';target.mkdir(exist_ok=True)
    args = ['docker','run','--rm','--name',prefix+'-build','-e','CARGO_TARGET_DIR=/work/target','-e','CARGO_PROFILE_RELEASE_DEBUG=0','--mount',f'type=bind,src={root}/product,dst=/work/product,readonly','--mount',f'type=bind,src={target},dst=/work/target','-w','/work/product',rust,'cargo','build','--locked','--release','-p','db-sync-alt','--bin','db-sync-alt']
    with (out/'build.stdout').open('w') as stdout, (out/'build.stderr').open('w') as stderr:
        g['call'](args, stdout=stdout, stderr=stderr)
    cli = '/work/target/release/db-sync-alt'
    for verb in ['--version','restore','verify','status']:
        command = [cli,verb] + ([] if verb == '--version' else ['--help'])
        text = output(['docker','run','--rm','--mount',f'type=bind,src={target},dst=/work/target,readonly',rust,*command])
        (out/(verb.replace('--','')+'-preflight.txt')).write_text(text)
        if verb == '--version':assert text.strip() == 'db-sync-alt 0.5.0'
        else:
            assert '--statement-timeout-seconds' in text and '--idle-transaction-timeout-seconds' in text
            if verb == 'restore':assert '--file' in text
    save('binaries.json', {'cli_sha256':hashlib.sha256((target/'release/db-sync-alt').read_bytes()).hexdigest(), 'direct_real_cli_preflight':True})
    docker('network','create',g['NETWORK'])
    ids[names['worker']] = docker('run','-d','--user',f'{g["os"].getuid()}:{g["os"].getgid()}','--name',names['worker'],'--network',g['NETWORK'],'--cpus','1','--memory','1g','--memory-swap','1g','--mount',f'type=bind,src={target},dst=/work/target,readonly','--mount',f'type=bind,src={out},dst=/evidence',rust,'sleep','infinity').strip()
    g['thread'] = threading.Thread(target=g['monitor'],daemon=True);g['thread'].start()
    results = []
    for label, tcp in [('controlled-default30',None), ('controlled-owner600',600)]:
        target_info = g['fresh_restore_target'](label)
        save('baseline-volumes.json', {'source':{'allocated_bytes':0}, 'restored':{'allocated_bytes':int(docker('exec',names['restored'],'du','-sB1','/var/lib/postgresql/data').split()[0])}})
        name = 'restore_control';owner = 'restore_control_owner'
        docker('exec',names['restored'],'psql','-XAt','-v','ON_ERROR_STOP=1','-U','postgres','-d','postgres','-c',f'CREATE ROLE {owner} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS')
        docker('exec',names['restored'],'psql','-XAt','-v','ON_ERROR_STOP=1','-U','postgres','-d','postgres','-c',f'CREATE DATABASE {name} OWNER {owner}')
        save('databases.json', {'restored':name})
        trace_start = g['start_socket_trace'](label)
        dsn = f'host=restored port=5432 user={owner} dbname={name} sslmode=disable' + (f' tcp_user_timeout={tcp}' if tcp else '')
        base = ['docker','exec',names['worker'],cli]
        common = ['--database',dsn,'--network','mainnet','--statement-timeout-seconds','600','--idle-transaction-timeout-seconds','7200']
        started = time.time();save('active-command.json',{'name':label,'started_unix':started})
        with (out/(label+'.stdout')).open('w') as stdout, (out/(label+'.stderr')).open('w') as stderr:
            process = subprocess.Popen([*base,'restore',*common,'--file','/evidence/representative.snapshot'],stdout=stdout,stderr=stderr)
            control = {'label':label,'container_id':ids[names['restored']], 'statement_seconds':600, 'requested_tcp_ms':30000 if tcp is None else 600000, 'scope':'controlled own COPY stall, not original failure cause'}
            released = threading.Event();watchdog = None
            def query(sql):
                raw = output(['docker','exec','-e','PGOPTIONS=-c statement_timeout=3000',names['restored'],'psql','-XAt','-v','ON_ERROR_STOP=1','-U','postgres','-d','postgres','-c',sql])
                return json.loads(raw)
            def identity(pid):
                return output(['docker','exec',names['restored'],'cat',f'/proc/{pid}/stat']).split(') ',1)[1].split()[19]
            def signal_backend(signal):
                assert inspect(names['restored'])['Id'] == control['container_id']
                pid = control['backend']['pid'];assert identity(pid) == control['os_start_ticks']
                g['call'](['docker','exec','--user','postgres',names['restored'],'sh','-c','kill -'+signal+' "$1"','signal-backend',str(pid)],timeout=10,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
            try:
                deadline = time.monotonic()+120
                while process.poll() is None and time.monotonic()<deadline:
                    rows = query(f"SELECT coalesce(json_agg(json_build_object('pid',pid,'backend_start',backend_start,'xact_start',xact_start,'client_addr',client_addr,'client_port',client_port,'query',query,'application_name',application_name,'backend_type',backend_type)), '[]'::json) FROM pg_stat_activity WHERE datname='{name}' AND application_name='db-sync-alt' AND backend_type='client backend' AND state='active' AND query ~ '^COPY serve\\.tx FROM STDIN'")
                    if rows:
                        assert len(rows)==1;control['backend']=rows[0];break
                    time.sleep(.1)
                assert 'backend' in control,'actual COPY serve.tx not observed'
                assert control['backend']['client_addr']==trace_start['worker_ip']
                control['os_start_ticks']=identity(control['backend']['pid'])
                signal_backend('STOP');control['stopped_unix']=time.time();control['release_deadline_monotonic']=time.monotonic()+90
                def resume():
                    while not released.wait(.05):
                        if time.monotonic() >= control['release_deadline_monotonic']:break
                    try:
                        signal_backend('CONT');control['continued_unix']=time.time();control['continued']=True
                    except Exception as error:control['resume_error']=repr(error)
                    released.set()
                watchdog=threading.Thread(target=resume,daemon=True);watchdog.start()
                recheck=query(f"SELECT coalesce(json_agg(json_build_object('pid',pid,'backend_start',backend_start,'xact_start',xact_start,'client_addr',client_addr,'client_port',client_port,'query',query,'application_name',application_name,'backend_type',backend_type)), '[]'::json) FROM pg_stat_activity WHERE pid={control['backend']['pid']}")
                assert recheck==[control['backend']],'COPY backend/family raced before STOP'
                confirm_deadline=time.monotonic()+15
                while time.monotonic()<confirm_deadline:
                    worker=inspect(names['worker']);assert worker['Id']==ids[names['worker']]
                    sockets=output(['sudo','nsenter','-t',str(worker['State']['Pid']),'-n','ss','-tin'])
                    pending=socket_queue(sockets,control['backend']['client_port'],trace_start['target_ip'])
                    zero=g['TRACE']['counts'].get('last_zero_window',{})
                    if pending and zero.get('client_port')==control['backend']['client_port'] and zero.get('observed_unix',0)>=control['stopped_unix']:
                        control['zero_window_confirmed_unix']=time.time();control['pending_send_bytes']=pending;control['confirmed_socket_info']=sockets;control['confirmed_zero_window']=zero
                        control['release_deadline_monotonic']=time.monotonic()+45;break
                    time.sleep(.1)
                assert 'zero_window_confirmed_unix' in control,'no verified zero-window plus pending data'
                code=process.wait(timeout=1200)
                # Even a default30 failure must leave the independent hold/release intact.
                watchdog.join(timeout=100);assert not watchdog.is_alive() and control.get('continued')
            except BaseException as error:
                control['control_error']=repr(error);released.set()
                if watchdog:watchdog.join(timeout=15)
                if process.poll() is None:
                    assert inspect(names['worker'])['Id']==ids[names['worker']]
                    docker('kill',names['worker']);process.wait(timeout=15)
                code=process.returncode
            finally:
                released.set()
                if watchdog:watchdog.join(timeout=15)
                save(label+'-control.json',control)
        finished=time.time()
        # Observe actual backend/transaction and writer advisory lock disappearance before state.
        drain_deadline=time.monotonic()+180;remaining=True
        while time.monotonic()<drain_deadline:
            pid=control.get('backend',{}).get('pid',0)
            remaining=query(f"SELECT json_build_object('backend',EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid={pid}),'writer_lock',EXISTS(SELECT 1 FROM pg_locks WHERE pid={pid} AND locktype='advisory' AND granted))")
            if not remaining['backend'] and not remaining['writer_lock']:break
            time.sleep(.25)
        control['backend_release']=remaining;control['backend_release_observed_unix']=time.time();save(label+'-control.json',control)
        if 'control_error' in control:
            raise RuntimeError('controlled trial failed safely; receipt retained, no mechanism claim')
        trace=g['stop_socket_trace']()
        lines=(out/(label+'.stderr')).read_text().splitlines();events=[]
        for line in lines:
            try:events.append(json.loads(line))
            except ValueError:pass
        safe_errno=[e for e in events if e.get('event')=='postgres_connection_failed']
        state_raw=output(['docker','exec',names['restored'],'psql','-XAt','-v','ON_ERROR_STOP=1','-U','postgres','-d',name,'-c',"SELECT to_jsonb(p) FROM dsa.position p"])
        state=json.loads(state_raw)
        item={'label':label,'exit_code':code,'seconds':finished-started,'control':control,'trace':trace,'connection_failures':safe_errno,'state':state,'fresh_target':target_info,'socket_limits':[e for e in events if e.get('event')=='postgres_socket_limits']}
        if code==0:
            verified=subprocess.run([*base,'verify',*common],text=True,capture_output=True,timeout=600)
            (out/(label+'-verify.stdout')).write_text(verified.stdout);(out/(label+'-verify.stderr')).write_text(verified.stderr)
            value=json.loads(verified.stdout);item['verified']=value
            assert verified.returncode==0 and value['content_digest']==original['content_digest'] and value['state_digest']==original['state_digest']
        else:
            counts={}
            for table in ['block','tx','tx_out','tx_in','ma_tx_out','multi_asset','datum','epoch_param','pool_hash','epoch_stake','tx_metadata']:
                counts[table]=int(output(['docker','exec',names['restored'],'psql','-XAt','-U','postgres','-d',name,'-c',f'SELECT count(*) FROM serve.{table}']).strip())
            item['failed_rows']=counts
        save(label+'-result.json',item);results.append(item)
        print(json.dumps({'event':'controlled_restore_finished','label':label,'exit_code':code,'seconds':item['seconds'],'control_error':control.get('control_error'),'connection_failures':safe_errno}),flush=True)
    def errno110(value):
        if isinstance(value,dict):return value.get('os_error')==110 or value.get('kind')=='TimedOut' or any(errno110(v) for v in value.values())
        if isinstance(value,list):return any(errno110(v) for v in value)
        return False
    negative,positive=results
    for item in results:
        c=item['control'];assert 'control_error' not in c and c['continued'] and c['continued_unix']-c['zero_window_confirmed_unix']>=44.5
        assert c['backend_release']=={'backend':False,'writer_lock':False}
        assert item['socket_limits'][0]['tcp_user_timeout_ms']==c['requested_tcp_ms']
    assert negative['exit_code']!=0 and errno110(negative['connection_failures']) and negative['trace']['tcp_abort_on_timeout_delta']>0
    assert not negative['state']['serving'] and all(n==0 for n in negative['failed_rows'].values())
    assert positive['exit_code']==0 and positive['state']['serving']
    save('controlled-result.json',{'status':'PASS_CONTROLLED_MECHANISM_ONLY','variants':results,'original_failure_cause_proven':False,'source_generation':False,'source_conversion':False,'generated_only':True})
