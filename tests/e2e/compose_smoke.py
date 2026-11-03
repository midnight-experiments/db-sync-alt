"""Host side of the Compose smoke: provision, restore, start, health, the node's direct reads as the reader,
updater outage and restart, with synthetic data only."""
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import uuid
ROOT=Path(__file__).resolve().parents[2]

def docker(*args, **kwargs):return subprocess.run(['docker',*args],check=True,**kwargs)

def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1',0));port=s.getsockname()[1]
    if port<10000:return free_port()
    return port

def smoke(image,pg_image):
    token=uuid.uuid4().hex[:12];seed='db-sync-alt-seed-'+token;started=False;compose=None
    with tempfile.TemporaryDirectory(prefix='db-sync-alt-smoke-') as tmp:
        root=Path(tmp);out=root/'seed';out.mkdir();private=root/'private'
        try:
            docker('run','--detach','--name',seed,'--network','none','--memory','2g','--cpus','2','--tmpfs','/var/lib/postgresql/data:rw','-e','POSTGRES_HOST_AUTH_METHOD=trust',pg_image,stdout=subprocess.DEVNULL);started=True
            for _ in range(60):
                if subprocess.run(['docker','exec',seed,'pg_isready','-h','127.0.0.1','-U','postgres'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode==0:break
                time.sleep(1)
            else:raise RuntimeError('seed PostgreSQL did not start')
            docker('run','--rm','--network','container:'+seed,'--memory','2g','--cpus','2','--user',f'{os.getuid()}:{os.getgid()}','-v',str(out)+':/smoke',image,'python','tests/e2e/smoke_app.py','seed','/smoke')
            docker('rm','-f',seed,stdout=subprocess.DEVNULL);started=False
            subprocess.run([sys.executable,'-m','db_sync_alt','compose','init','--private',str(private),'--archive',str(out/'state.dump'),'--manifest',str(out/'manifest.json'),'--http-port',str(free_port())],check=True,env=dict(os.environ,PYTHONPATH=str(ROOT/'src')))
            override=root/'override.yaml'
            # The smoke provider is an in-process synthetic transport: the service stays on the internal
            # backend network (Compose !override). `check` reads the service's health and the owner's store;
            # `node` is a node-like client on the node network only, reading PostgreSQL as the reader.
            mounts='    volumes: [\''+str(private/'service')+':/run/db-sync-alt:ro\']\n    environment: {PGSERVICEFILE: /run/db-sync-alt/pg_service.conf, PGPASSFILE: /run/db-sync-alt/pgpass}\n'
            tool='    image: '+image+'\n    user: "'+str(os.getuid())+':'+str(os.getgid())+'"\n    read_only: true\n    profiles: [tools]\n    mem_limit: 2g\n    cpus: 2\n'
            override.write_text('services:\n  postgres:\n    mem_limit: 2g\n    cpus: 2\n  service:\n    image: '+image+'\n    command: [python, tests/e2e/smoke_app.py, serve]\n    networks: !override [backend]\n    mem_limit: 2g\n    cpus: 2\n  bootstrap:\n    image: '+image+'\n    mem_limit: 2g\n    cpus: 2\n'
                                '  check:\n'+tool+'    command: [python, tests/e2e/smoke_app.py, check]\n    networks: [backend]\n'+mounts+
                                '  node:\n'+tool+'    command: [python, tests/e2e/smoke_app.py, node]\n    networks: [node]\n'+mounts)
            compose=['compose','-p','db-sync-alt-smoke-'+token,'-f',str(ROOT/'deploy/compose.yaml'),'-f',str(override),'--env-file',str(private/'compose.env')]
            docker(*compose,'config','--quiet')
            docker(*compose,'up','-d','postgres')
            docker(*compose,'run','--rm','bootstrap')
            docker(*compose,'up','-d','service')
            docker(*compose,'run','--rm','check')
            docker(*compose,'run','--rm','node')
            # The node keeps reading while the updater is stopped, and after it restarts.
            docker(*compose,'stop','service')
            docker(*compose,'run','--rm','node')
            docker(*compose,'start','service')
            docker(*compose,'run','--rm','check')
            docker(*compose,'run','--rm','node')
        except Exception:
            if compose:subprocess.run(['docker',*compose,'logs','--tail','25','service'],check=False)
            raise
        finally:
            if started:subprocess.run(['docker','rm','-f',seed],stdout=subprocess.DEVNULL,check=False)
            if compose:subprocess.run(['docker',*compose,'down','--volumes','--remove-orphans'],check=False)
if __name__=='__main__':smoke(sys.argv[1],sys.argv[2])
