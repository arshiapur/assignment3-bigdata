from concurrent.futures import ThreadPoolExecutor
import hashlib
from pathlib import Path
import subprocess
import argparse
p=argparse.ArgumentParser()
p.add_argument('--runtime-dir',required=True)
a=p.parse_args()
base=Path(a.runtime_dir).resolve()
base.mkdir(parents=True,exist_ok=True)
items=[
('kafka','https://archive.apache.org/dist/kafka/3.9.1/kafka_2.13-3.9.1.tgz','.sha512'),
('elasticsearch','https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-8.17.0-linux-x86_64.tar.gz','.sha512'),
('kibana','https://artifacts.elastic.co/downloads/kibana/kibana-8.17.0-linux-x86_64.tar.gz','.sha512'),
('logstash','https://artifacts.elastic.co/downloads/logstash/logstash-8.17.0-linux-x86_64.tar.gz','.sha512')]
def fetch(item):
    name,url,suffix=item
    target=base/(name+'.tar.gz')
    subprocess.run(['curl','--fail','--location','--retry','2','--max-time','900','--silent','--show-error',url,'-o',str(target)],check=True)
    digest=subprocess.check_output(['curl','--fail','--location','--max-time','30','--silent','--show-error',url+suffix],text=True)
    actual=hashlib.file_digest(target.open('rb'),'sha512').hexdigest()
    if actual.lower() not in ''.join(digest.split()).lower():
        raise RuntimeError('Checksum mismatch for '+name)
    print(name+' checksum verified',flush=True)
    dest=base/name;dest.mkdir(exist_ok=True)
    subprocess.run(['tar','-xzf',str(target),'-C',str(dest),'--strip-components=1'],check=True)
    target.unlink()
    print(name+' extracted',flush=True)
with ThreadPoolExecutor(max_workers=4) as pool:
    list(pool.map(fetch,items))
