"""BBC RSS -> Kafka -> Spark cumulative named entities -> Kafka."""
import argparse
from datetime import datetime, timezone
import hashlib
from html.parser import HTMLParser
import json
from pathlib import Path
import sqlite3
import time

LABELS = {'PERSON', 'ORG', 'GPE', 'LOC', 'NORP', 'FAC', 'EVENT', 'PRODUCT', 'WORK_OF_ART', 'LAW', 'LANGUAGE'}
_MODEL = None

def now():
    return datetime.now(timezone.utc).isoformat()

class Text(HTMLParser):
    def __init__(self):
        super().__init__(); self.parts = []
    def handle_data(self, data):
        self.parts.append(data)

def clean(value):
    parser = Text(); parser.feed(value)
    return ' '.join(' '.join(parser.parts).split())

def entities(value):
    global _MODEL
    if _MODEL is None:
        import spacy
        _MODEL = spacy.load('en_core_web_sm')
    return [(" ".join(e.text.casefold().split()), e.label_) for e in _MODEL(value).ents if e.label_ in LABELS]

def append(path, value):
    with path.open('a', encoding='utf-8') as f:
        f.write(json.dumps(value) + '\n')

def produce(a):
    import feedparser
    import requests
    from kafka import KafkaProducer
    state = Path(a.state); state.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(state / 'seen.sqlite3')
    db.execute('CREATE TABLE IF NOT EXISTS seen (id TEXT PRIMARY KEY)')
    producer = KafkaProducer(bootstrap_servers=a.bootstrap, acks='all', value_serializer=lambda v: json.dumps(v).encode())
    try:
        while True:
            started = now(); sent = 0
            try:
                response = requests.get(a.feed, timeout=30); response.raise_for_status()
                feed = feedparser.parse(response.content)
                if feed.bozo and not feed.entries:
                    raise ValueError(str(feed.bozo_exception))
                for entry in feed.entries:
                    identity = entry.get('id') or entry.get('link')
                    if not identity:
                        continue
                    article_id = hashlib.sha256(identity.encode()).hexdigest()
                    if db.execute('SELECT 1 FROM seen WHERE id=?', (article_id,)).fetchone():
                        continue
                    article = dict(id=article_id, text=clean(entry.get('title', '') + ' ' + entry.get('summary', '')), source=a.feed, url=entry.get('link', ''), published_at=entry.get('published', ''), ingested_at=now())
                    producer.send(a.input_topic, key=article_id.encode(), value=article).get(timeout=60)
                    append(state / 'articles.jsonl', article)
                    db.execute('INSERT OR IGNORE INTO seen VALUES (?)', (article_id,)); db.commit(); sent += 1
                append(state / 'polls.jsonl', dict(started_at=started, completed_at=now(), entries=len(feed.entries), acknowledged=sent))
            except Exception as exc:
                append(state / 'polls.jsonl', dict(started_at=started, completed_at=now(), acknowledged=sent, error=str(exc)))
                if a.once:
                    raise
            if a.once:
                break
            time.sleep(a.poll_seconds)
    finally:
        producer.close(); db.close()

def stream(a):
    from pyspark.sql import SparkSession, functions as F, types as T
    spark = SparkSession.builder.appName('Part1NamedEntities').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    schema = T.StructType([T.StructField('id', T.StringType()), T.StructField('text', T.StringType())])
    ner = F.udf(entities, T.ArrayType(T.StructType([T.StructField('entity', T.StringType()), T.StructField('label', T.StringType())])))
    raw = spark.readStream.format('kafka').option('kafka.bootstrap.servers', a.bootstrap).option('subscribe', a.input_topic).option('startingOffsets', 'earliest').load()
    articles = raw.select(F.from_json(F.col('value').cast('string'), schema).alias('a')).select('a.*').filter('id IS NOT NULL AND text IS NOT NULL').dropDuplicates(['id'])
    counts = articles.select(F.explode(ner('text')).alias('e')).select('e.*').groupBy('entity', 'label').count()
    def publish(batch, batch_id):
        batch.persist()
        try:
            size = batch.count()
            snapshot = batch.withColumn('run_id', F.lit(a.run_id)).withColumn('batch_id', F.lit(batch_id)).withColumn('version', F.lit(batch_id + 1)).withColumn('snapshot_size', F.lit(size)).withColumn('observed_at', F.lit(now()))
            snapshot.select(F.to_json(F.struct('*')).alias('value')).write.format('kafka').option('kafka.bootstrap.servers', a.bootstrap).option('topic', a.output_topic).save()
        finally:
            batch.unpersist()
    counts.writeStream.outputMode('complete').foreachBatch(publish).option('checkpointLocation', a.checkpoint).trigger(processingTime=a.trigger).start().awaitTermination()

def main():
    p = argparse.ArgumentParser()
    p.add_argument('mode', choices=['produce', 'stream'])
    p.add_argument('--bootstrap', default='127.0.0.1:19092')
    p.add_argument('--input-topic', required=True)
    p.add_argument('--output-topic')
    p.add_argument('--run-id')
    p.add_argument('--feed', default='https://feeds.bbci.co.uk/news/world/rss.xml')
    p.add_argument('--state', default='data/producer')
    p.add_argument('--checkpoint', default='data/checkpoint')
    p.add_argument('--poll-seconds', type=float, default=60)
    p.add_argument('--trigger', default='15 seconds')
    p.add_argument('--once', action='store_true')
    a = p.parse_args()
    if a.mode == 'stream' and not (a.run_id and a.output_topic):
        p.error('stream requires --run-id and --output-topic')
    (produce if a.mode == 'produce' else stream)(a)

if __name__ == '__main__':
    main()
