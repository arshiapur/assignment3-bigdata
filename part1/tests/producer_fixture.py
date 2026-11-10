"""Real Kafka acknowledgement + local RSS fixture + SQLite restart-dedup check."""
import argparse
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
import sys
from threading import Thread
from types import SimpleNamespace
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from kafka import KafkaConsumer
from pipeline import produce

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = b'<rss version="2.0"><channel><title>CONTROLLED TEST</title><item><guid>fixture-1</guid><title>Microsoft in London</title><description>&lt;p&gt;Barack Obama visits Paris.&lt;/p&gt;</description><link>https://example.invalid/fixture-1</link></item></channel></rss>'
        self.send_response(200); self.send_header('Content-Type', 'application/rss+xml'); self.end_headers(); self.wfile.write(body)
    def log_message(self, *args):
        pass

p = argparse.ArgumentParser()
p.add_argument('--bootstrap', default='127.0.0.1:19092')
p.add_argument('--topic', required=True)
p.add_argument('--state', required=True)
a = p.parse_args()
server = HTTPServer(('127.0.0.1', 0), Handler)
thread = Thread(target=server.serve_forever, daemon=True); thread.start()
try:
    args = SimpleNamespace(bootstrap=a.bootstrap, input_topic=a.topic, state=a.state, feed=f'http://127.0.0.1:{server.server_port}/feed.xml', once=True, poll_seconds=60)
    produce(args); produce(args)
    records = [json.loads(line) for line in (Path(a.state) / 'articles.jsonl').read_text().splitlines()]
    polls = [json.loads(line) for line in (Path(a.state) / 'polls.jsonl').read_text().splitlines()]
    assert len(records) == 1, records
    assert [poll['acknowledged'] for poll in polls] == [1, 0], polls
    assert records[0]['text'] == 'Microsoft in London Barack Obama visits Paris.'
    consumer = KafkaConsumer(a.topic, bootstrap_servers=a.bootstrap, auto_offset_reset='earliest', group_id=None, consumer_timeout_ms=10000, value_deserializer=lambda value: json.loads(value))
    try:
        messages = list(consumer)
        assert len(messages) == 1 and messages[0].value == records[0]
    finally:
        consumer.close()
    print('PASS: local RSS -> acknowledged Kafka, clean text, audit logs, SQLite restart deduplication')
finally:
    server.shutdown(); server.server_close(); thread.join()
