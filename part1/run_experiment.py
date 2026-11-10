"""Prepare services and capture complete indexed snapshots on elapsed-time deadlines."""
import argparse
import csv
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from datetime import datetime, timezone

import requests


def utc():
    return datetime.now(timezone.utc).isoformat()


def previous_observations(work, run_id):
    """Reserve every existing capture number, including interrupted exports."""
    observations = []
    last_number = 0
    for path in sorted(work.glob('snapshot-*.json')):
        try:
            number = int(path.stem.removeprefix('snapshot-'))
        except ValueError:
            continue
        last_number = max(last_number, number)
        try:
            observation = json.loads(path.read_text())
        except (ValueError, OSError):
            continue
        if observation.get('run_id') != run_id:
            raise ValueError(f'{path.name} belongs to a different run')
        prefix = work / f'snapshot-{number:02d}'
        if not observation.get('kibana_render_verified') or not all(
            Path(str(prefix) + suffix).is_file()
            for suffix in ('.csv', '.png', '-kibana.png')
        ):
            continue
        observation['snapshot_number'] = number
        observations.append(observation)
    observations.sort(key=lambda observation: observation['snapshot_number'])
    return observations, last_number + 1


def api(base, method, path, **kwargs):
    response = requests.request(method, base.rstrip('/') + path, timeout=60, **kwargs)
    response.raise_for_status()
    return response.json() if response.content else {}


def prepare(a):
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError
    admin = KafkaAdminClient(bootstrap_servers=a.bootstrap)
    try:
        for topic in (a.input_topic, a.output_topic):
            try:
                admin.create_topics([NewTopic(topic, 1, 1)])
            except TopicAlreadyExistsError:
                pass
    finally:
        admin.close()
    props = {key: {'type': 'keyword'} for key in ('entity', 'label', 'run_id')}
    props.update({key: {'type': 'long'} for key in ('count', 'batch_id', 'version', 'snapshot_size')})
    props['observed_at'] = {'type': 'date'}
    api(a.elasticsearch, 'PUT', '/_index_template/part1', json={'index_patterns': ['entities-latest', 'entities-history'], 'template': {'mappings': {'properties': props}}})
    # Index template applies when Logstash first creates the indices.
    for index in ('entities-latest', 'entities-history'):
        response = requests.head(a.elasticsearch.rstrip('/') + '/' + index, timeout=30)
        if response.status_code == 404:
            api(a.elasticsearch, 'PUT', '/' + index, json={'mappings': {'properties': props}})
        else:
            response.raise_for_status()
            api(a.elasticsearch, 'PUT', '/' + index + '/_mapping', json={'properties': props})
    headers = {'kbn-xsrf': 'true'}
    api(a.kibana, 'POST', '/api/data_views/data_view', headers=headers, json={'data_view': {'id': 'part1-latest', 'title': 'entities-latest'}, 'override': True})
    spec = {
        '$schema': 'https://vega.github.io/schema/vega/v5.json',
        'padding': 10,
        'config': {'kibana': {'renderer': 'svg'}},
        'data': [{'name': 'entities', 'url': {'index': 'entities-latest', 'body': {'size': 0, 'query': {'term': {'run_id': a.run_id}}, 'aggs': {'top_entities': {'terms': {'field': 'entity', 'size': 10, 'order': {'total': 'desc'}}, 'aggs': {'total': {'sum': {'field': 'count'}}}}}}}, 'format': {'property': 'aggregations.top_entities.buckets'}}],
        'scales': [
            {'name': 'x', 'type': 'linear', 'domain': {'data': 'entities', 'field': 'total.value'}, 'range': 'width', 'zero': True},
            {'name': 'y', 'type': 'band', 'domain': {'data': 'entities', 'field': 'key'}, 'range': 'height', 'padding': 0.15}],
        'axes': [{'orient': 'bottom', 'scale': 'x', 'title': 'Cumulative occurrences', 'tickMinStep': 1, 'format': 'd'}, {'orient': 'left', 'scale': 'y'}],
        'marks': [{'type': 'rect', 'from': {'data': 'entities'}, 'encode': {'enter': {'x': {'value': 0}, 'x2': {'scale': 'x', 'field': 'total.value'}, 'y': {'scale': 'y', 'field': 'key'}, 'height': {'scale': 'y', 'band': 1}, 'fill': {'value': '#247ba0'}}}}]
    }
    vis_id = 'part1-' + a.run_id
    api(a.kibana, 'POST', '/api/saved_objects/visualization/' + vis_id + '?overwrite=true', headers=headers, json={'attributes': {'title': 'Top 10 entities: ' + a.run_id, 'visState': json.dumps({'title': 'Named entities', 'type': 'vega', 'params': {'spec': json.dumps(spec)}}), 'uiStateJSON': '{}', 'kibanaSavedObjectMeta': {'searchSourceJSON': '{}'}}})
    panel = {'panelIndex': '1', 'type': 'visualization', 'panelRefName': 'panel_0', 'embeddableConfig': {}, 'gridData': {'x': 0, 'y': 0, 'w': 48, 'h': 24, 'i': '1'}, 'version': '8.17.0'}
    api(a.kibana, 'POST', '/api/saved_objects/dashboard/' + vis_id + '?overwrite=true', headers=headers, json={'attributes': {'title': 'Part 1 ' + a.run_id, 'panelsJSON': json.dumps([panel]), 'optionsJSON': '{}', 'kibanaSavedObjectMeta': {'searchSourceJSON': '{}'}}, 'references': [{'name': 'panel_0', 'type': 'visualization', 'id': vis_id}]})
    return a.kibana.rstrip('/') + '/app/dashboards#/view/' + vis_id


def snapshot(a):
    query = {'term': {'run_id': a.run_id}}
    latest = api(a.elasticsearch, 'POST', '/entities-history/_search', json={'size': 1, 'query': query, 'sort': [{'batch_id': 'desc'}]})
    hits = latest['hits']['hits']
    if not hits:
        raise RuntimeError('No indexed snapshot yet')
    meta = hits[0]['_source']
    batch_query = {'bool': {'filter': [query, {'term': {'batch_id': meta['batch_id']}}]}}
    # Scroll rather than silently truncating a snapshot at Elasticsearch's result window.
    page = api(a.elasticsearch, 'POST', '/entities-history/_search?scroll=1m', json={'size': 1000, 'query': batch_query, 'sort': ['_doc']})
    rows = []
    scroll_id = page.get('_scroll_id')
    try:
        while page['hits']['hits']:
            rows.extend(hit['_source'] for hit in page['hits']['hits'])
            page = api(a.elasticsearch, 'POST', '/_search/scroll', json={'scroll': '1m', 'scroll_id': scroll_id})
            scroll_id = page.get('_scroll_id', scroll_id)
    finally:
        if scroll_id:
            api(a.elasticsearch, 'DELETE', '/_search/scroll', json={'scroll_id': [scroll_id]})
    if len(rows) != meta['snapshot_size']:
        raise RuntimeError(f"Batch {meta['batch_id']} incomplete: {len(rows)}/{meta['snapshot_size']}")
    return meta, rows


def export(a, number, elapsed, meta, rows, dashboard):
    cache = Path(a.work_dir) / '.cache'
    cache.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault('MPLCONFIGDIR', str(cache / 'matplotlib'))
    os.environ.setdefault('XDG_CACHE_HOME', str(cache))
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from playwright.sync_api import sync_playwright
    prefix = Path(a.work_dir) / f'snapshot-{number:02d}'
    totals = {}
    for row in rows:
        totals[row['entity']] = totals.get(row['entity'], 0) + row['count']
    top = sorted(totals.items(), key=lambda v: (-v[1], v[0]))[:10]
    payload = {'captured_at': utc(), 'elapsed_seconds': elapsed, 'snapshot_number': number, 'session_started_at': getattr(a, 'session_started_at', None), 'run_id': a.run_id, 'batch_id': meta['batch_id'], 'source': a.feed, 'controlled': a.controlled, 'rows': rows}
    prefix.with_suffix('.json').write_text(json.dumps(payload, indent=2))
    with prefix.with_suffix('.csv').open('w', newline='') as f:
        writer = csv.writer(f); writer.writerow(['entity', 'label', 'count', 'batch_id'])
        writer.writerows((r['entity'], r['label'], r['count'], r['batch_id']) for r in rows)
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh([r[0] for r in reversed(top)], [r[1] for r in reversed(top)])
    ax.set_xlabel('Cumulative occurrences')
    ax.set_title(f"{'CONTROLLED TEST' if a.controlled else 'RSS observation'} — {elapsed:.1f} seconds")
    from matplotlib.ticker import MaxNLocator
    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    fig.tight_layout(); fig.savefig(prefix.with_suffix('.png')); plt.close(fig)
    with sync_playwright() as p:
        kwargs = {'headless': True}
        if a.chromium:
            kwargs['executable_path'] = a.chromium
        browser = p.chromium.launch(**kwargs)
        try:
            page = browser.new_page(viewport={'width': 1440, 'height': 1000})
            page.goto(dashboard, wait_until='domcontentloaded')
            # Require actual rendered Vega marks with positive geometry, not just a canvas.
            page.wait_for_function("""() => [...document.querySelectorAll('svg .mark-rect path, svg .mark-rect rect')].filter(e => {const b=e.getBBox();return b.width>0 && b.height>0}).length > 0""", timeout=120000)
            dismiss = page.get_by_role('button', name='Dismiss', exact=True)
            if dismiss.count() and dismiss.first.is_visible():
                dismiss.first.click()
            page.screenshot(path=str(prefix) + '-kibana.png', full_page=True)
            payload['kibana_captured_at'] = utc()
            payload['kibana_render_verified'] = True
            prefix.with_suffix('.json').write_text(json.dumps(payload, indent=2))
        finally:
            browser.close()
    return payload


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--run-id', required=True)
    p.add_argument('--work-dir', required=True)
    p.add_argument('--bootstrap', default='127.0.0.1:19092')
    p.add_argument('--elasticsearch', default='http://127.0.0.1:9200')
    p.add_argument('--kibana', default='http://127.0.0.1:5601')
    p.add_argument('--feed', default='https://feeds.bbci.co.uk/news/world/rss.xml')
    p.add_argument('--intervals', default='900,1800,2700,3600', help='Elapsed seconds from runner start')
    p.add_argument('--poll-seconds', type=float, default=60)
    p.add_argument('--trigger', default='15 seconds')
    p.add_argument('--chromium')
    p.add_argument('--ivy-cache', help='Writable Spark connector cache (default: WORK_DIR/ivy)')
    p.add_argument('--controlled', action='store_true')
    p.add_argument('--prepare-only', action='store_true')
    a = p.parse_args()
    import re
    if not re.fullmatch(r'[A-Za-z0-9_-]+', a.run_id):
        p.error('run-id must contain only letters, digits, underscores and hyphens')
    intervals = [float(x) for x in a.intervals.split(',')]
    if intervals != sorted(set(intervals)) or intervals[0] <= 0:
        p.error('intervals must be positive and strictly increasing')
    a.input_topic = 'articles-' + a.run_id
    a.output_topic = 'entity-counts-' + a.run_id
    work = Path(a.work_dir).resolve(); work.mkdir(parents=True, exist_ok=True)
    a.work_dir = str(work)
    manifest = {'run_id': a.run_id, 'feed': a.feed, 'input_topic': a.input_topic, 'output_topic': a.output_topic, 'controlled': a.controlled}
    manifest_file = work / 'run.json'
    if manifest_file.exists() and json.loads(manifest_file.read_text()) != manifest:
        p.error('existing work directory belongs to a different experiment')
    manifest_file.write_text(json.dumps(manifest, indent=2))
    try:
        observations, first_number = previous_observations(work, a.run_id)
    except ValueError as exc:
        p.error(str(exc))
    dashboard = prepare(a)
    print(dashboard, flush=True)
    if a.prepare_only:
        return
    script = str(Path(__file__).with_name('pipeline.py').resolve())
    common = ['--bootstrap', a.bootstrap, '--input-topic', a.input_topic]
    spark = str(Path(sys.executable).parent / 'spark-submit')
    spark_options = ['--conf', 'spark.jars.ivy=' + str(Path(a.ivy_cache).resolve() if a.ivy_cache else work / 'ivy')]
    commands = [
        [spark, *spark_options, '--master', 'local[2]', '--packages', 'org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1', '--conf', 'spark.sql.shuffle.partitions=2', script, 'stream', *common, '--output-topic', a.output_topic, '--run-id', a.run_id, '--checkpoint', str(work / 'checkpoint'), '--trigger', a.trigger],
        [sys.executable, script, 'produce', *common, '--feed', a.feed, '--state', str(work / 'producer'), '--poll-seconds', str(a.poll_seconds)]]
    child_env = dict(os.environ, PYSPARK_PYTHON=sys.executable)
    # Java/Ivy does not automatically honor HTTP(S)_PROXY. Preserve CA trust.
    from urllib.parse import urlsplit
    java_proxy = []
    for protocol in ('http', 'https'):
        proxy = os.environ.get(protocol.upper() + '_PROXY') or os.environ.get(protocol + '_proxy')
        if proxy:
            parsed = urlsplit(proxy)
            if parsed.hostname and not parsed.username:
                java_proxy.extend([f'-D{protocol}.proxyHost={parsed.hostname}', f'-D{protocol}.proxyPort={parsed.port or 80}'])
    if java_proxy:
        child_env['SPARK_SUBMIT_OPTS'] = child_env.get('SPARK_SUBMIT_OPTS', '') + ' ' + ' '.join(java_proxy)
    children = []; logs = []; start = time.monotonic(); started_at = utc()
    a.session_started_at = started_at
    try:
        for name, command in zip(('spark', 'producer'), commands):
            log = (work / (name + '.log')).open('a'); logs.append(log)
            children.append(subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, env=child_env))
        for number, deadline in enumerate(intervals, first_number):
            while time.monotonic() - start < deadline:
                if any(child.poll() is not None for child in children):
                    raise RuntimeError('A pipeline child exited; inspect logs')
                time.sleep(min(1, max(0, deadline - (time.monotonic() - start))))
            # Retry a partially indexed batch for at most two minutes; record actual elapsed time.
            for attempt in range(25):
                try:
                    meta, rows = snapshot(a); break
                except (RuntimeError, requests.HTTPError):
                    if attempt == 24:
                        raise
                    time.sleep(5)
            observations.append(export(a, number, time.monotonic() - start, meta, rows, dashboard))
            write_report(work, started_at, observations, a)
    finally:
        for child in children:
            if child.poll() is None:
                child.terminate()
        for child in children:
            try:
                child.wait(timeout=30)
            except subprocess.TimeoutExpired:
                child.kill(); child.wait()
        for log in logs:
            log.close()


def write_report(work, started_at, observations, a):
    lines = ['# ' + ('CONTROLLED TEST ONLY' if a.controlled else 'RSS observations'), '', f'Source: {a.feed}', f'Latest runner invocation started: {started_at}', '', 'Each elapsed time is measured from its own runner invocation, including startup and indexing delays. Absolute timestamps distinguish resumed sessions. Counts are cumulative, not sliding-window counts. The first RSS poll includes existing feed backlog.', '']
    for position, observation in enumerate(observations, 1):
        number = observation.get('snapshot_number', position)
        totals = {}
        for row in observation['rows']:
            totals[row['entity']] = totals.get(row['entity'], 0) + row['count']
        leaders = sorted(totals.items(), key=lambda x: (-x[1], x[0]))[:10]
        lines += [f"## Observation {number}: {observation['elapsed_seconds']:.1f} seconds", f"Captured: {observation['captured_at']}; batch {observation['batch_id']}", f"Runner invocation started: {observation.get('session_started_at') or 'not recorded; use absolute capture timestamp'}", '', f'![Top entities](snapshot-{number:02d}.png)', '', f'![Actual Kibana dashboard](snapshot-{number:02d}-kibana.png)', '', 'Leaders: ' + ', '.join(f'{e} ({n})' for e, n in leaders), '']
    audit = work / 'producer' / 'articles.jsonl'
    acknowledged = len(audit.read_text().splitlines()) if audit.exists() else 0
    lines += [f'Audit contains {acknowledged} acknowledged article records across this work directory. This is not a processed-article total; Spark and Elasticsearch can lag.', '', 'Interpretation requires reviewing the source articles: high counts indicate repeated mentions in sampled titles/summaries, not sentiment or public opinion. spaCy errors, entity aliases, repeated title/summary text, source coverage, initial backlog and processing lag limit the result. See producer/polls.jsonl to distinguish first-poll backlog from subsequent discoveries.']
    (work / 'REPORT.md').write_text('\n'.join(lines) + '\n')

if __name__ == '__main__':
    main()
