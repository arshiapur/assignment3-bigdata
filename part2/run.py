"""Launch matching Spark 4 / Scala 2.13 GraphFrames without machine-specific paths."""
import argparse
import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlsplit

p = argparse.ArgumentParser()
p.add_argument('--ivy-cache', required=True, help='Writable Maven/Ivy dependency cache')
p.add_argument('--master', default='local[2]')
p.add_argument('--driver-memory', default='2g')
a, remaining = p.parse_known_args()
env = dict(os.environ, PYSPARK_PYTHON=sys.executable)
options = []
for protocol in ('http', 'https'):
    proxy = os.environ.get(protocol.upper() + '_PROXY') or os.environ.get(protocol + '_proxy')
    if proxy:
        parsed = urlsplit(proxy)
        if parsed.hostname and not parsed.username:
            options += [f'-D{protocol}.proxyHost={parsed.hostname}', f'-D{protocol}.proxyPort={parsed.port or 80}']
env['SPARK_SUBMIT_OPTS'] = env.get('SPARK_SUBMIT_OPTS', '') + ' ' + ' '.join(options)
command = [str(Path(sys.executable).parent / 'spark-submit'), '--master', a.master,
           '--driver-memory', a.driver_memory, '--packages', 'io.graphframes:graphframes-spark4_2.13:0.10.0',
           '--conf', 'spark.jars.ivy=' + str(Path(a.ivy_cache).resolve()),
           str(Path(__file__).with_name('analysis.py').resolve()), *remaining]
raise SystemExit(subprocess.call(command, env=env))
