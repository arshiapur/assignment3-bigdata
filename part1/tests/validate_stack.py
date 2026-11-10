"""Controlled full-stack test with checkpoint restart and real Kibana captures."""
import argparse, json, os, subprocess, sys, time
from pathlib import Path
from urllib.parse import urlsplit
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from run_experiment import api, prepare, snapshot, export, utc
p=argparse.ArgumentParser()
for key,default in [('run-id',None),('work-dir',None),('bootstrap','127.0.0.1:19092'),('elasticsearch','http://127.0.0.1:9200'),('kibana','http://127.0.0.1:5601'),('chromium',None)]:
    p.add_argument('--'+key,default=default,required=key in ('run-id','work-dir'))
a=p.parse_args();a.input_topic='articles-'+a.run_id;a.output_topic='entity-counts-'+a.run_id
a.feed='CONTROLLED TEXT FIXTURE';a.controlled=True
work=Path(a.work_dir).resolve();work.mkdir(parents=True,exist_ok=True);a.work_dir=str(work)
root=Path(__file__).resolve().parents[1]
for attempt in range(180):
    try:
        status=api(a.kibana,'GET','/api/status');api(a.elasticsearch,'GET','/_cluster/health')
        if status['status']['overall']['level']=='available':break
    except Exception:pass
    if attempt==179:raise RuntimeError('Services not ready after 180 seconds')
    time.sleep(1)
print('Services available',flush=True)
dashboard=prepare(a)
subprocess.run([sys.executable,str(root/'tests/producer_fixture.py'),'--bootstrap',a.bootstrap,'--topic','articles-producer-'+a.run_id,'--state',str(work/'producer-fixture')],check=True)
env=dict(os.environ,PYSPARK_PYTHON=sys.executable);options=[]
for protocol in ('http','https'):
    value=os.environ.get(protocol.upper()+'_PROXY') or os.environ.get(protocol+'_proxy')
    if value:
        proxy=urlsplit(value)
        if proxy.hostname and not proxy.username:options.extend([f'-D{protocol}.proxyHost={proxy.hostname}',f'-D{protocol}.proxyPort={proxy.port or 80}'])
env['SPARK_SUBMIT_OPTS']=env.get('SPARK_SUBMIT_OPTS','')+' '+' '.join(options)
command=[str(Path(sys.executable).parent/'spark-submit'),'--master','local[2]','--packages','org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1','--conf','spark.jars.ivy='+str(work/'ivy'),'--conf','spark.sql.shuffle.partitions=2',str(root/'pipeline.py'),'stream','--bootstrap',a.bootstrap,'--input-topic',a.input_topic,'--output-topic',a.output_topic,'--run-id',a.run_id,'--checkpoint',str(work/'checkpoint')]
started=time.monotonic()
for phase in (1,2):
    with (work/f'spark-phase-{phase}.log').open('w') as log:
        child=subprocess.Popen(command,env=env,stdout=log,stderr=subprocess.STDOUT)
        try:
            test=[sys.executable,str(root/'tests/integration.py'),'--run-id',a.run_id,'--bootstrap',a.bootstrap,'--elasticsearch',a.elasticsearch]
            if phase==2:test.append('--restart-phase')
            subprocess.run(test,check=True)
            meta,rows=snapshot(a);export(a,phase,time.monotonic()-started,meta,rows,dashboard)
            print(f'Phase {phase} passed with rendered Kibana bars',flush=True)
        finally:
            if child.poll() is None:child.terminate()
            try:child.wait(timeout=30)
            except subprocess.TimeoutExpired:child.kill();child.wait()
(work/'PASS.json').write_text(json.dumps({'completed_at':utc(),'run_id':a.run_id,'controlled':True,'checks':['RSS acknowledgement and SQLite restart deduplication','entity occurrence counts','duplicate IDs across microbatches','checkpoint restart','real Kibana SVG bars']},indent=2))
