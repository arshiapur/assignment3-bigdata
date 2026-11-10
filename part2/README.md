# CS 6350 Assignment 3 — Part 2

This project uses **Spark GraphFrames** for all five required analyses on the public
SNAP Wiki-Vote social network. The complete graph is used; there is no sampling.
Code: `analysis.py`. Combined query output: `results/wiki-vote/results.txt` and
`results/wiki-vote/results.json`. Interpretation: `REPORT.md`.

## Requirements and installation

Use Linux, WSL or macOS with Python 3.12 and Java 21. The tested versions are:

- PySpark 4.0.1 / Scala 2.13
- graphframes-py 0.10.0 and matching JVM package
  `io.graphframes:graphframes-spark4_2.13:0.10.0`
- NetworkX 3.4.2 for independent validation, not for producing assignment results

```bash
cd part2
./setup.sh
source .venv/bin/activate
java -version
python -m pip check
```

setup.sh installs `requirements.lock.txt`, which records the tested dependency
versions including the validation tools. `VENV_DIR` can choose another environment
location. Keep the Python and JVM GraphFrames versions aligned; the Spark 3/Scala
2.12 package is incompatible with this Spark 4 setup. `typing-extensions` is pinned
explicitly because the GraphFrames wrapper imports it.

There is no source compilation step: spark-submit runs the Python code and resolves
the precompiled GraphFrames JVM package. The first run needs network access to Maven
Central (and its configured mirrors) and a writable Ivy cache. run.py translates
inherited credential-free HTTP(S)_PROXY host/port settings for Java/Ivy while keeping
TLS verification and inherited trust settings enabled. Existing SPARK_SUBMIT_OPTS
are preserved. Do not print or commit credentials.

## Data and reproducible execution

The original compressed SNAP edge list and provenance are included under `data/`.
To download and verify the same snapshot again:

```bash
python download_data.py --output data/wiki-Vote.txt.gz --metadata data/provenance.json
```

The downloader uses HTTPS, verifies the pinned SHA-256 and checks the gzip CRC by
reading to EOF. The hash identifies the observed snapshot, not a publisher signature.
Source: https://snap.stanford.edu/data/wiki-Vote.html

From this directory, run:

```bash
python run.py --ivy-cache cache/ivy \
  --input data/wiki-Vote.txt.gz \
  --output results/wiki-vote-new \
  --checkpoint checkpoints/wiki-vote-new \
  --expected-vertices 7115 --expected-edges 103689
```

Use a **fresh output and checkpoint directory** for an independent analysis. The
program refuses to overwrite a nonempty output directory. All input/output,
checkpoint and dependency-cache paths are arguments; no machine-specific paths
are embedded in the source. The included recorded results use `results/wiki-vote/`.
Run `python run.py --help` and `python analysis.py --help` for launcher/analysis options.
Launcher options include `--master 'local[2]'` and `--driver-memory 2g`; analysis
options include `--partitions 2`, `--pagerank-iterations 20`,
`--reset-probability 0.15`, `--dataset-name`, and `--relationship`.

The equivalent direct submission, after activating the environment, is:

```bash
spark-submit --master 'local[2]' --driver-memory 2g \
  --packages io.graphframes:graphframes-spark4_2.13:0.10.0 \
  --conf spark.jars.ivy=cache/ivy \
  analysis.py --input data/wiki-Vote.txt.gz \
  --output results/wiki-vote-new --checkpoint checkpoints/wiki-vote-new
```

Use run.py when Java needs the inherited proxy settings translated automatically.
Local-driver output and checkpoint directories must be writable. The initial
archive may contain .gz input; Spark decompresses it directly.

## Parsing, graph structure and query definitions

The parser skips blank lines and `#` comments, accepts whitespace-separated integer
`src dst` IDs, and rejects malformed records and values outside signed 64-bit range.
Vertices are the distinct endpoints. An optional `--vertices PATH` adds IDs from a
one-ID-per-line file, including isolated vertices.

The property graph has vertex fields `id: long, dataset: string` and edge fields
`src: long, dst: long, relationship: string`. Duplicate directed edges and self-loops
are removed; all endpoint vertices are retained even if their only edge was a loop.
The cleaning counts are recorded in results.json.

Wiki-Vote is **directed**: A→B means A voted on B's adminship. Leave `--undirected`
OFF for this dataset. For a different undirected relationship input, pass
`--undirected`; it creates both A→B and B→A and deduplicates the resulting arcs.

| PDF requirement | GraphFrames operation | Output |
| --- | --- | --- |
| 2.3(a), five highest outdegrees | `graph.outDegrees` | `a_outdegree.csv` |
| 2.3(b), five highest indegrees | `graph.inDegrees` | `b_indegree.csv` |
| 2.3(c), five highest PageRank values | `graph.pageRank` | `c_pagerank.csv` |
| 2.3(d), five largest components | `graph.connectedComponents` | `d_connected_components.csv` |
| 2.3(e), five highest triangle counts | `triangleCount` | `e_triangles.csv` |

All these files are in the chosen output directory and are combined in results.txt
and results.json. Degrees include zero-degree vertices via left joins. PageRank
uses 20 iterations and reset probability 0.15 (link-follow probability 0.85).
GraphX's finite-iteration algorithm starts at 1 per vertex, applies
`reset + (1-reset) * incoming_rank` each iteration, and normalizes at the end so the
scores sum to the vertex count. `pagerank_probability` is also exported and sums to
1; it is not a separate ranking. This is not the same finite-iteration recurrence
as adding redistributed dangling mass every iteration.

Connected components mean **weak** components: edge direction is ignored for
connectivity. They are ranked by vertex count. The output component ID is the
minimum original vertex ID in that component, for stable interpretation.
Triangles ignore direction and count simple undirected 3-cliques: reciprocal arcs
are collapsed before invoking GraphFrames triangleCount. Each triangle contributes
one to each of its three vertices, so the sum of vertex counts is three times the
graph's triangle total. This does not count only directed cycles.

Ties use ascending numeric vertex/component ID. The assignment allows random ties;
this deterministic choice is reproducible. For a graph with fewer than five nodes
or components, all available entries are output. The full Wiki-Vote graph has enough
components for all five requested component rows.

## Validation

```bash
python -m unittest discover -s tests -v
python tests/verify_results.py --input data/wiki-Vote.txt.gz \
  --results results/wiki-vote --output validation/wiki-vote-checks.json
```

The independent validator checks every exported node's indegree, outdegree,
triangle count, component membership and finite-iteration PageRank, as well as all
five top-result tables and graph totals. NetworkX provides independent graph
operations, with a scalar implementation of the documented GraphX PageRank
recurrence. It does not produce the submitted GraphFrames results.

Controlled graph tests (use fresh output/checkpoint directories if rerunning):

```bash
python run.py --ivy-cache cache/ivy \
  --input tests/fixtures/edges.txt --vertices tests/fixtures/vertices.txt \
  --output validation/directed-new --checkpoint checkpoints/directed-new \
  --dataset-name 'CONTROLLED DIRECTED FIXTURE' --expected-vertices 7 --expected-edges 6
python tests/verify_results.py --input tests/fixtures/edges.txt \
  --vertices tests/fixtures/vertices.txt --results validation/directed-new \
  --output validation/directed-new-checks.json
python run.py --ivy-cache cache/ivy \
  --input tests/fixtures/edges.txt --vertices tests/fixtures/vertices.txt --undirected \
  --output validation/undirected-new --checkpoint checkpoints/undirected-new \
  --dataset-name 'CONTROLLED UNDIRECTED FIXTURE' --expected-vertices 7 --expected-edges 10
python tests/verify_results.py --input tests/fixtures/edges.txt \
  --vertices tests/fixtures/vertices.txt --undirected --results validation/undirected-new \
  --output validation/undirected-new-checks.json
```

The fixtures exercise direction, reverse-edge conversion, duplicate edges,
self-loops, an isolated vertex, zero degrees, multiple components and a known
triangle. Validation evidence and logs are under validation/.

## Submission contents

The supplied CS6350_CoverPage.docx is the official template, with assignment number
and references filled. Complete its student-name and late-day fields before
submission. The unmodified original and download provenance are under reference/.

Submit analysis.py and the launcher/setup/dependency files, results.txt (or
results.json), REPORT.md, this README, and the instructor's cover sheet with student
names/details. The all-vertex CSV output is under `vertex_metrics/`; component sizes
are under `component_sizes/`. These are standard Spark CSV directories containing
part files and _SUCCESS markers. No Docker, Kafka, Elasticsearch or Kibana is needed
for Part 2. Part 1 remains separate.
