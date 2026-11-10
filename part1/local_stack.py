"""Optional local-binary fallback when Docker storage is unavailable."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import time


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--runtime-dir', required=True, help='Contains kafka/, elasticsearch/, kibana/, logstash/ distributions')
    p.add_argument('--data-dir', required=True, help='Persistent configuration, state and logs')
    a = p.parse_args()
    runtime = Path(a.runtime_dir).resolve(); data = Path(a.data_dir).resolve()
    data.mkdir(parents=True, exist_ok=True)
    kafka_config = data / 'kafka.properties'
    kafka_config.write_text('\n'.join([
        'process.roles=broker,controller', 'node.id=1',
        'controller.quorum.voters=1@127.0.0.1:19093',
        'listeners=PLAINTEXT://127.0.0.1:19092,CONTROLLER://127.0.0.1:19093',
        'advertised.listeners=PLAINTEXT://127.0.0.1:19092',
        'controller.listener.names=CONTROLLER',
        'listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT',
        'inter.broker.listener.name=PLAINTEXT',
        'offsets.topic.replication.factor=1', 'transaction.state.log.replication.factor=1',
        'transaction.state.log.min.isr=1', 'num.partitions=1',
        'log.dirs=' + str(data / 'kafka-data')]) + '\n')
    storage = str(runtime / 'kafka/bin/kafka-storage.sh')
    if not (data / 'kafka-data/meta.properties').exists():
        cluster_id = subprocess.check_output([storage, 'random-uuid'], text=True).strip()
        subprocess.run([storage, 'format', '-t', cluster_id, '-c', str(kafka_config)], check=True)
    es_config = data / 'es-config'
    if not es_config.exists():
        shutil.copytree(runtime / 'elasticsearch/config', es_config)
    (es_config / 'elasticsearch.yml').write_text('\n'.join([
        'cluster.name: part1-local', 'discovery.type: single-node',
        'xpack.security.enabled: false', 'network.host: 127.0.0.1',
        'http.port: 9200', 'path.data: ' + json.dumps(str(data / 'es-data')),
        'path.logs: ' + json.dumps(str(data / 'es-logs'))]) + '\n')
    kibana_config = data / 'kibana.yml'
    kibana_config.write_text('\n'.join([
        'server.host: 127.0.0.1', 'server.port: 5601',
        'elasticsearch.hosts: ["http://127.0.0.1:9200"]',
        'path.data: ' + json.dumps(str(data / 'kibana-data')),
        'telemetry.enabled: false']) + '\n')
    logstash_config = data / 'logstash-config'; logstash_config.mkdir(exist_ok=True)
    source = Path(__file__).resolve().parent / 'config'
    (logstash_config / 'logstash.yml').write_text('http.host: 127.0.0.1\nxpack.monitoring.enabled: false\nqueue.type: persisted\n')
    for name in ('jvm.options', 'log4j2.properties'):
        shutil.copyfile(runtime / 'logstash/config' / name, logstash_config / name)
    conf = (source / 'logstash.conf').read_text().replace('kafka:9092', '127.0.0.1:19092').replace('http://elasticsearch:9200', 'http://127.0.0.1:9200')
    (data / 'logstash.conf').write_text(conf)
    common = dict(os.environ)
    # All service-to-service traffic is local; retain inherited proxy/CA settings.
    commands = [
        ('kafka', [str(runtime / 'kafka/bin/kafka-server-start.sh'), str(kafka_config)], dict(common, KAFKA_HEAP_OPTS='-Xms256m -Xmx512m')),
        ('elasticsearch', [str(runtime / 'elasticsearch/bin/elasticsearch')], dict(common, ES_PATH_CONF=str(es_config), ES_JAVA_OPTS='-Xms512m -Xmx512m')),
        ('kibana', [str(runtime / 'kibana/bin/kibana'), '--config', str(kibana_config)], dict(common, NODE_OPTIONS='--max-old-space-size=1024')),
        ('logstash', [str(runtime / 'logstash/bin/logstash'), '--path.settings', str(logstash_config), '--path.data', str(data / 'logstash-data'), '-f', str(data / 'logstash.conf')], dict(common, LS_JAVA_OPTS='-Xms256m -Xmx512m'))]
    children = []; logs = []
    try:
        for name, command, env in commands:
            log = (data / (name + '.log')).open('a'); logs.append(log)
            children.append(subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, env=env))
        print('Services started; inspect logs and health endpoints before experiments.', flush=True)
        while True:
            for (name, _, _), child in zip(commands, children):
                if child.poll() is not None:
                    raise RuntimeError(name + ' exited; inspect ' + str(data / (name + '.log')))
            time.sleep(1)
    except KeyboardInterrupt:
        pass
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

if __name__ == '__main__':
    main()
