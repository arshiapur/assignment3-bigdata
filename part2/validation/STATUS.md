# Part 2 validation

The submitted analysis results were produced by Spark GraphFrames 0.10.0 on PySpark
4.0.1/Scala 2.13, Python 3.12 and Java 21. Actual versions are in versions.json.

Passed:

1. Three strict-parser unit tests: comments/whitespace, malformed/wrong-column IDs,
   signed-64-bit bounds and the optional isolated-vertex list.
2. Directed fixture: 7 vertices, 6 cleaned directed edges, 3 weak components and
   one undirected triangle. Duplicate records and a self-loop are removed; an
   explicit isolated vertex and zero-degree nodes are retained. Every vertex's
   degrees, PageRank, component and triangle count passed independent checks.
3. Undirected fixture: reverse edges yield 10 directed arcs from 5 simple undirected
   relationships. All vertex values and five top-result tables passed independent
   checks. Reciprocal input relationships are not doubled again by mistake.
4. Complete SNAP Wiki-Vote analysis: 7,115 nodes, 103,689 directed edges, 24 weak
   components and 608,389 undirected triangles. No source edges or vertices were
   sampled away. The largest weak component has 7,066 nodes.
5. Every one of the 7,115 vertices' in/out degrees, triangle counts and component
   memberships matched NetworkX. All five top-result tables matched. Every
   PageRank value matched a scalar implementation of the documented GraphX
   20-iteration recurrence, with maximum absolute error about 8.03e-13.
6. Dataset compressed SHA-256 matched the pinned snapshot, and gzip CRC verified.
   Vertex, edge, largest-component and total triangle counts match the published
   SNAP dataset statistics.
7. Python dependency consistency, syntax and shell setup checks passed.

Evidence:

- directed-fixture-checks.json and directed-fixture/ result files
- undirected-fixture-checks.json and undirected-fixture/ result files
- wiki-vote-checks.json and ../results/wiki-vote/ result files
- parser-tests.log, versions.json and the actual *-spark.log files

NetworkX is only the independent reference; assignment results are computed by
GraphFrames APIs. The source is historical Wikipedia voting data through January
2008. Part 1's live-news data and controlled tests are not used in this analysis.
No Docker or external account/service credentials are needed for Part 2.

The official cover sheet was fetched and its DOCX structure verified. The supplied
copy has assignment number 3 and references filled. Student names and late-day usage
remain blank; they must be completed before submission. The original template and
its SHA-256 provenance are retained under reference/.
