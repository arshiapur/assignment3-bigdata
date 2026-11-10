"""Verify replaying a cumulative snapshot cannot inflate latest or history counts."""
import argparse,json,sys,time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from kafka import KafkaProducer
from run_experiment import api,snapshot
p=argparse.ArgumentParser()
p.add_argument('--run-id',required=True)
p.add_argument('--bootstrap',default='127.0.0.1:19092')
p.add_argument('--elasticsearch',default='http://127.0.0.1:9200')
a=p.parse_args()
meta,rows=snapshot(a)
query={'query':{'term':{'run_id':a.run_id}}}
before=api(a.elasticsearch,'POST','/entities-history/_count',json=query)['count']
producer=KafkaProducer(bootstrap_servers=a.bootstrap,acks='all',value_serializer=lambda value:json.dumps(value).encode())
try:
    for _ in range(2):
        for row in rows:producer.send('entity-counts-'+a.run_id,value=row).get(timeout=60)
finally:producer.close()
time.sleep(5)
api(a.elasticsearch,'POST','/entities-history,entities-latest/_refresh')
after=api(a.elasticsearch,'POST','/entities-history/_count',json=query)['count']
latest=api(a.elasticsearch,'POST','/entities-latest/_search',json={**query,'size':len(rows)+1})['hits']['hits']
expected={(r['entity'],r['label']):r['count'] for r in rows}
actual={(h['_source']['entity'],h['_source']['label']):h['_source']['count'] for h in latest}
assert before==after,(before,after)
assert expected==actual,(expected,actual)
assert all(h['_source']['version']==meta['batch_id']+1 for h in latest)
print(json.dumps({'passed':True,'history_documents_before':before,'history_documents_after':after,'latest_counts_unchanged':True}))
