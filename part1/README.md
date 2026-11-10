# CS 6350 Assignment 3 — Part 1 reconstruction

This archive reconstructs the project from the supplied conversation specification.
The prior environment's implementation, plots and screenshots were not available.
The instructor ZIP is preserved verbatim under `reference/`. The newly reconstructed
code is not a byte-for-byte recovery. Part 2 is excluded.

The prior session collected **controlled tests only**. This resumed session can
access BBC RSS and collected a separate real-source experiment under
`results/bbc-20261110a/`. See REPORT.md for actual observation timestamps and
limitations. New controlled validation is under `validation/smoke-20261110a/`.
The original pasted context and assignment are preserved under `reference/`.

## Prerequisites and install

Use Linux/WSL/macOS with Docker Compose v2, Python 3.12, Java 21 and adequate memory
(roughly 8 GB or more available for the local stack). Confirm `java -version`,
`python3.12 --version`, and `docker compose version`. On Linux Elasticsearch may
require `sudo sysctl -w vm.max_map_count=262144` according to your host policy.

Run these commands from this directory:

```bash
./setup.sh
source .venv/bin/activate
python -m spacy validate
docker compose up -d
curl --fail http://127.0.0.1:9200/_cluster/health
curl --fail http://127.0.0.1:5601/api/status
```

Wait until Elasticsearch and Kibana report ready, and inspect
`docker compose logs kafka logstash` for startup failures before starting a run.
These checks do not themselves verify the pipeline. Compose uses persistent volumes
and loopback host ports. Security is disabled for local development services;
container traffic uses the private Compose network. Do not expose these services
publicly. Kafka uses 19092 on the host, kafka:9092 within Compose.

`requirements.txt` pins direct versions from the supplied context; it is not a
resolved transitive lock. A previous lock was not provided, so none is invented.
To record your actual installed environment, run `python -m pip freeze >
requirements.lock.txt` after a successful installation. Keep TLS verification enabled.
The Spark connector is Scala 2.13 / Spark 4.0.1, not the instructor's old 2.12 example.

## Live experiment (optional future collection)

```bash
python run_experiment.py --run-id bbc-001 --work-dir results/bbc-001 \
  --intervals 900,1800,2700,3600
```

This command actually contacts BBC and starts a new collection. The default source is BBC World News RSS, polled every 60 seconds.
No full article pages are downloaded. Initial feed entries are backlog, not newly
published stories during the observation period. An unchanged article ID is not
published again after acknowledgement. A crash between acknowledgement and the
SQLite commit can replay an article; Spark's persistent ID deduplication handles it.

The runner prepares topics, index mappings, a data view, saved Vega visualization
and dashboard; starts producer/Spark with logs; captures observations at elapsed
seconds from runner start; and stops its own children. Delays are recorded as actual
elapsed times. It writes PNG/CSV/JSON snapshots, actual rendered Kibana screenshots,
run.json and REPORT.md. Incomplete indexed batches are retried rather than exported.
No data/empty snapshots or render failures cause an error; inspect the logs.
A screenshot may show a newer latest snapshot than the historical batch exported
alongside it if indexing advances during capture. CSV/JSON identify the exact batch.
Review and expand the generated report's interpretation before submission.

Use `--chromium /path/to/chromium` for an existing browser. Use `--feed URL` for
another RSS source; explain the source change. For local fixtures always use
`--controlled` and store outputs under `validation/`, never present them as live data.
Use `python run_experiment.py --help` and `python pipeline.py --help` for all options.
State, checkpoints, work directories and service endpoints are configurable.

For a resume, use the same run ID, work directory, feed and topics. Elapsed time in
each runner invocation restarts at zero; inspect audit timestamps for gaps. Use a
fresh run ID and directory for each independent experiment. Do not run two Spark
queries concurrently against the same checkpoint. Do not delete checkpoints to
resume, or mix an old SQLite state with fresh input topics.

If Spark cannot resolve dependencies through a managed proxy, configure Java's
`https.proxyHost` and `https.proxyPort` from your environment and a writable
`spark.jars.ivy` cache via spark-submit options. Do not disable TLS validation or
put proxy credentials in source files. See Spark logs for the actual failure.

## Controlled verification

```bash
python -m unittest discover -s tests -v
python run_experiment.py --run-id smoke-001 --work-dir validation/smoke-001 --prepare-only --controlled
spark-submit --master 'local[2]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 \
  --conf spark.sql.shuffle.partitions=2 \
  pipeline.py stream --input-topic articles-smoke-001 \
  --output-topic entity-counts-smoke-001 --run-id smoke-001 \
  --checkpoint validation/smoke-001/checkpoint
```

Leave Spark running and, in another activated terminal, execute:

```bash
python tests/integration.py --run-id smoke-001
```

Then stop only that Spark process, restart it with the identical command/checkpoint,
and run `python tests/integration.py --run-id smoke-001 --restart-phase`.
Expected counts are checked against indexed historical snapshots. The test waits
for separate stages before introducing subsequent articles. Use a new run ID on a
fresh test attempt. These full-stack checks passed in the resumed session; see validation/STATUS.md.

## Counting and storage

spaCy en_core_web_sm is loaded once per worker. Accepted labels are PERSON, ORG,
GPE, LOC, NORP, FAC, EVENT, PRODUCT, WORK_OF_ART, LAW, LANGUAGE; dates and quantities
are excluded. Entity whitespace is normalized and text case-folded. Occurrences,
including repeated mentions in one article, are counted cumulatively across unique
article IDs. The duplicate-ID state and aggregation state grow without a retention
bound: appropriate for a bounded coursework run, requiring redesign for production.

Spark emits complete count snapshots with entity, label, count, run_id, batch_id,
version=batch_id+1, snapshot_size and observed_at. Logstash upserts latest documents
using external_gte versions and stores one historical document per batch/entity/label.
Retries do not add counts. Never sum cumulative counts across historical batches.
The top-10 view sums latest counts across labels for each entity text, within one run.

Shutdown: `docker compose stop`. Restart: `docker compose up -d`.
`docker compose down` retains volumes. `docker compose down -v` destroys stored data;
use it only when intentionally discarding all local experiments.

The instructor requests a cover sheet and student names; add these yourself before
academic submission. Review the actual observation intervals and results in REPORT.md before submission.

## Optional local-binary fallback

If Docker cannot extract images in a constrained environment, Linux x86_64 users
can run the same versions directly. The downloader verifies SHA-512 checksums from
the official distribution hosts before extraction. Run services as an ordinary user.

```bash
python scripts/download_services.py --runtime-dir runtime
python local_stack.py --runtime-dir runtime --data-dir services
```

Keep that terminal running; use a second activated terminal for the experiments.
The fallback binds Kafka/Elasticsearch/Kibana to the same loopback endpoints as
Compose and writes persistent data and service logs under the selected data directory.
Do not run it alongside Compose on the same ports. Ctrl+C stops its child services.
It is a development setup with security disabled, as in Compose.

The experiment runner configures a writable Ivy cache inside the work directory;
`--ivy-cache PATH` overrides it. It derives Java HTTP/HTTPS proxy host/port options
from inherited proxy URLs without embedded user credentials, and preserves the
inherited trust configuration and existing SPARK_SUBMIT_OPTS.

To verify producer restart deduplication against real Kafka using a local RSS fixture:

```bash
python tests/producer_fixture.py --topic articles-producer-fixture-001 \
  --state validation/producer-fixture-001
```

Use a fresh topic and state directory for each attempt. This is a controlled test.

To automate the controlled producer/count/checkpoint/browser checks after services
are ready, use a fresh run ID:

```bash
python tests/validate_stack.py --run-id smoke-002 --work-dir validation/smoke-002
python tests/sink_replay.py --run-id smoke-002
```

The recorded BBC development run uses actual 2/4/6-minute targets. For a full-length
coursework run, the default command captures at 15/30/45/60 minutes. An unchanged
feed and unchanged plots are valid; initial stories are not republished to change
the chart. The UI's global time picker is not applied to the Vega query: it shows
cumulative counts for the selected run ID, including its initial backlog.
