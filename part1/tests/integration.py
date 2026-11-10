"""Controlled full-stack smoke check. Start Spark and prepare services first; see README."""
import argparse
import json
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from kafka import KafkaProducer
from run_experiment import snapshot

p = argparse.ArgumentParser()
p.add_argument('--run-id', required=True)
p.add_argument('--bootstrap', default='127.0.0.1:19092')
p.add_argument('--elasticsearch', default='http://127.0.0.1:9200')
p.add_argument('--restart-phase', action='store_true')
a = p.parse_args()
producer = KafkaProducer(bootstrap_servers=a.bootstrap, acks='all', value_serializer=lambda v: json.dumps(v).encode())
def send(identity, text):
    producer.send('articles-' + a.run_id, value={'id': identity, 'text': text}).get(timeout=60)
def check(expected):
    end = time.monotonic() + 180
    last = None
    while time.monotonic() < end:
        try:
            _, rows = snapshot(a)
            last = {}
            for row in rows:
                last[row['entity']] = last.get(row['entity'], 0) + row['count']
            if last == expected:
                print(json.dumps(last, sort_keys=True)); return
        except Exception as exc:
            last = str(exc)
        time.sleep(3)
    raise AssertionError(f'Expected {expected}; observed {last}')
try:
    text = 'Barack Obama visited Paris. Barack Obama visited France.'
    baseline = {'barack obama': 2, 'paris': 1, 'france': 1, 'microsoft': 1, 'london': 1}
    if not a.restart_phase:
        send('smoke-1', text); send('smoke-1', text)
        send('smoke-2', 'Microsoft opened an office in London.')
        check(baseline)
        send('smoke-1', text); send('smoke-3', 'Microsoft is in London.')
        check({**baseline, 'microsoft': 2, 'london': 2})
    else:
        send('smoke-1', text); send('smoke-4', 'Microsoft is in London.')
        check({**baseline, 'microsoft': 3, 'london': 3})
finally:
    producer.close()
