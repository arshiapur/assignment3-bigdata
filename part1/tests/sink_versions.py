"""Check stale cumulative snapshots cannot replace a newer latest document."""
import argparse
import json
from pathlib import Path
import sys
import time
from urllib.parse import quote
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from run_experiment import api, utc

p = argparse.ArgumentParser()
p.add_argument('--run-id', required=True, help='Fresh ID for this controlled sink-only test')
p.add_argument('--bootstrap', default='127.0.0.1:19092')
p.add_argument('--elasticsearch', default='http://127.0.0.1:9200')
a = p.parse_args()
topic = 'entity-counts-' + a.run_id
admin = KafkaAdminClient(bootstrap_servers=a.bootstrap)
try:
    admin.create_topics([NewTopic(topic, 1, 1)])
finally:
    admin.close()
producer = KafkaProducer(bootstrap_servers=a.bootstrap, acks='all',
                         value_serializer=lambda value: json.dumps(value).encode())
document = quote(a.run_id + '|microsoft|ORG', safe='')

def send(version, count):
    producer.send(topic, value=dict(entity='microsoft', label='ORG', count=count,
                  run_id=a.run_id, batch_id=version - 1, version=version,
                  snapshot_size=1, observed_at=utc())).get(timeout=60)

def wait_for(version, count):
    for attempt in range(60):
        try:
            result = api(a.elasticsearch, 'GET', '/entities-latest/_doc/' + document)
            if result['_version'] == version and result['_source']['count'] == count:
                return
        except Exception:
            pass
        time.sleep(1)
    raise AssertionError(f'Latest document did not reach version={version}, count={count}')

try:
    send(1, 3)
    wait_for(1, 3)
    send(2, 5)
    wait_for(2, 5)
    send(1, 3)  # stale event arrives after the newer count has been indexed
    send(2, 5)  # equal-version retry must be idempotent
    time.sleep(5)
    wait_for(2, 5)
    api(a.elasticsearch, 'POST', '/entities-history,entities-latest/_refresh')
    history = api(a.elasticsearch, 'POST', '/entities-history/_count',
                  json={'query': {'term': {'run_id': a.run_id}}})['count']
    assert history == 2, history
    print(json.dumps({'completed_at': utc(), 'controlled': True,
                      'passed': True, 'latest_count': 5, 'latest_version': 2,
                      'history_documents': history, 'stale_version_rejected': True}))
finally:
    producer.close()
