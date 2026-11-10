"""Run the actual experiment twice and verify checkpoint/capture preservation."""
import argparse
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
from pathlib import Path
import subprocess
import sys
from threading import Thread
import time
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from run_experiment import api, utc

p = argparse.ArgumentParser()
p.add_argument('--run-id', required=True)
p.add_argument('--work-dir', required=True)
p.add_argument('--chromium')
p.add_argument('--ivy-cache')
a = p.parse_args()
work = Path(a.work_dir).resolve()
if work.exists():
    p.error('use a fresh work directory for this test')
work.mkdir(parents=True)

class Fixture(BaseHTTPRequestHandler):
    def do_GET(self):
        body = b'<rss version="2.0"><channel><title>CONTROLLED TEST</title><item><guid>resume-fixture-1</guid><title>Microsoft in London.</title><description>Controlled fixture.</description></item></channel></rss>'
        self.send_response(200)
        self.send_header('Content-Type', 'application/rss+xml')
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *args):
        pass

for attempt in range(180):
    try:
        status = api('http://127.0.0.1:5601', 'GET', '/api/status')
        if status['status']['overall']['level'] == 'available':
            break
    except Exception:
        pass
    if attempt == 179:
        raise RuntimeError('Local services did not become available')
    time.sleep(1)
server = HTTPServer(('127.0.0.1', 0), Fixture)
thread = Thread(target=server.serve_forever, daemon=True)
thread.start()
script = Path(__file__).resolve().parents[1] / 'run_experiment.py'
command = [sys.executable, str(script), '--run-id', a.run_id, '--work-dir', str(work),
           '--controlled', '--feed', f'http://127.0.0.1:{server.server_port}/feed.xml',
           '--intervals', '45', '--poll-seconds', '2']
for key in ('chromium', 'ivy_cache'):
    value = getattr(a, key)
    if value:
        command.extend(['--' + key.replace('_', '-'), value])
try:
    before = None
    for phase in (1, 2):
        with (work / f'runner-phase-{phase}.log').open('w') as log:
            subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True)
        observed = json.loads((work / f'snapshot-{phase:02d}.json').read_text())
        counts = {r['entity']: r['count'] for r in observed['rows']}
        assert counts == {'microsoft': 1, 'london': 1}, counts
        assert observed['kibana_render_verified']
        if phase == 1:
            before = {path.name: path.read_bytes() for path in work.glob('snapshot-01*')}
        else:
            assert before == {path.name: path.read_bytes() for path in work.glob('snapshot-01*')}
        print(f'Runner invocation {phase} passed', flush=True)
    report = (work / 'REPORT.md').read_text()
    assert 'snapshot-01.png' in report and 'snapshot-02.png' in report
    articles = (work / 'producer/articles.jsonl').read_text().splitlines()
    assert len(articles) == 1
    (work / 'PASS.json').write_text(json.dumps({
        'completed_at': utc(), 'controlled': True,
        'checks': ['two real runner invocations with same run/topics/checkpoint',
                   'producer restart does not republish the fixture',
                   'checkpoint preserves entity counts',
                   'original PNG/CSV/JSON/Kibana screenshot bytes unchanged',
                   'second capture uses a new number',
                   'report includes captures from both sessions']}, indent=2))
finally:
    server.shutdown()
    server.server_close()
    thread.join()
