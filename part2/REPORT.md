# Part 2 — Wikipedia voting network analysis

## Dataset and method

The public SNAP Wiki-Vote social network records votes on Wikipedia administrator nominations through January 2008. A directed edge A→B means user A voted on user B’s nomination. The file contains anonymous numeric user IDs, not names or vote polarity. The complete published edge list was used, not a sample or only its largest component.

Source: https://snap.stanford.edu/data/wiki-Vote.html. Download: https://snap.stanford.edu/data/wiki-Vote.txt.gz. The downloaded snapshot’s SHA-256 is `7d3e53626e14b8b09fb3b396bece9d481ad606bd64ceab066349ff57d4ada7fc`. Provenance and gzip integrity verification are recorded in data/provenance.json.

The run completed 2026-11-10T05:04:40.816913+00:00 (November 09, 2026 at 11:04:40 PM CST). This is the execution time, not the historical dataset period. The measured algorithm stage took 93.39 seconds, excluding input preparation and final file exports.

The implementation uses PySpark 4.0.1, Scala 2.13, Java 21 and GraphFrames 0.10.0. The parser reads two integer IDs per data line, skips blank/comment lines and rejects malformed or out-of-range IDs. Vertex properties are id and dataset; edge properties are src, dst and relationship. Duplicate directed records and self-loops are removed while retaining endpoint vertices; both removal counts were zero for this file. Missing degree rows are filled with zero. All paths are configurable.

PageRank uses 20 iterations and reset probability 0.15. The GraphFrames/GraphX scores sum to the vertex count; normalized probabilities are also exported. Components are weakly connected, with minimum source vertex ID used as a reproducible component representative. Triangles are counted on a simple undirected projection, collapsing reciprocal arcs. Ties are broken by ascending numeric ID.

## Graph totals

| Quantity | Observed | SNAP published value, where available |
| --- | ---: | ---: |
| Vertices | 7,115 | 7,115 |
| Directed edges | 103,689 | 103,689 |
| Simple undirected relationships | 100,762 | — |
| Weak components | 24 | — |
| Largest weak component | 7,066 | 7,066 |
| Undirected triangles | 608,389 | 608,389 |

The source page’s prose mentions 7,066 users and 103,663 votes; its full-file statistics list 7,115 nodes and 103,689 edges. The former figures match the largest weak component. This analysis uses the complete file and matches the full-file statistics.

## a. Top five nodes by outdegree

| id | outDegree |
| ---: | ---: |
| 2565 | 893 |
| 766 | 773 |
| 11 | 743 |
| 457 | 732 |
| 2688 | 618 |

## b. Top five nodes by indegree

| id | inDegree |
| ---: | ---: |
| 4037 | 457 |
| 15 | 361 |
| 2398 | 340 |
| 2625 | 331 |
| 1297 | 309 |

## c. Top five nodes by PageRank

| id | pagerank | pagerank_probability |
| ---: | ---: | ---: |
| 4037 | 32.7799186786 | 0.00460715652546 |
| 15 | 26.1826646118 | 0.00367992475218 |
| 6634 | 25.5257481567 | 0.00358759636777 |
| 2625 | 23.3640801848 | 0.00328377795992 |
| 2398 | 18.5615043682 | 0.00260878487255 |

## d. Top five largest weak components

| component_id | size |
| ---: | ---: |
| 3 | 7066 |
| 7031 | 3 |
| 7465 | 3 |
| 8074 | 3 |
| 2304 | 2 |

## e. Top five nodes by triangle count

| id | triangle_count |
| ---: | ---: |
| 2565 | 30940 |
| 1549 | 22003 |
| 766 | 18204 |
| 1166 | 17361 |
| 2688 | 14220 |

## Interpretation

Node 2565 has the most outgoing votes (893), so it was a particularly active voter in this data. Node 4037 has the largest incoming vote count (457) and highest PageRank (32.7799, or probability 0.00460716). These results are consistent with a user receiving many links and occupying a structurally important position. Node 6634 ranks third by PageRank despite not being among the five largest indegrees, illustrating that the source of incoming links matters as well as their number.

The largest weak component contains 7,066 of 7,115 users (99.31%). The other 23 components contain only 49 users together; the largest three of those have three users each. Most participants therefore belong to one broad connected voting community, although weak connectivity does not imply that directed paths exist between every pair.

Node 2565 also has the largest triangle count (30,940). Many pairs of its neighbors are connected in the direction-independent projection. This is consistent with participation in a dense voting neighborhood, but its large neighborhood also creates many opportunities for triangles; raw triangle counts are not normalized clustering coefficients. The five leading triangle counts and degree/rank leaders show concentrated participation rather than a uniformly connected network.

These results make sense because they agree with the published network totals and with independent checks on every vertex. Votes are not marked positive/negative in this edge list, so high indegree or PageRank does not establish approval, promotion success, personal reputation or influence outside this historical network. The data is an old, specific sample of Wikipedia governance; it is not representative of all present-day users. Anonymous IDs are not resolved to identities.

## Validation and deliverables

All 7,115 vertices’ indegrees, outdegrees, triangle counts and weak-component memberships matched NetworkX. All finite-iteration PageRank values matched an independent scalar implementation of GraphX’s recurrence; the maximum absolute score difference was 8.03e-13. All five top-result tables also matched. This checks the submitted GraphFrames computation rather than generating its results with NetworkX.

Directed and undirected fixtures independently passed. They exercise a known triangle, reciprocal edge, duplicate input record, self-loop, disconnected pair and explicit isolated vertex. The undirected conversion produced 10 directed arcs from five simple relationships. Strict-parser unit tests passed. Validation evidence and actual Spark logs are under validation/.

The requested combined output is [results.txt](results/wiki-vote/results.txt), with structured [results.json](results/wiki-vote/results.json) and five top-result CSVs. The per-vertex and component-size CSV directories retain the full analysis for validation. [README.md](README.md) provides installation and execution instructions. The official cover-sheet template is included; student details and late-day usage must be supplied by the student.

## References

- SNAP Wiki-Vote dataset and published statistics: https://snap.stanford.edu/data/wiki-Vote.html
- J. Leskovec, D. Huttenlocher, J. Kleinberg. *Signed Networks in Social Media*. CHI 2010. Dataset citation provided by SNAP.
- J. Leskovec, D. Huttenlocher, J. Kleinberg. *Predicting Positive and Negative Links in Online Social Networks*. WWW 2010. Dataset citation provided by SNAP.
- GraphFrames user guide: https://graphframes.io/04-user-guide/05-traversals.html
- Spark 4.0.1 GraphX PageRank implementation: https://github.com/apache/spark/blob/v4.0.1/graphx/src/main/scala/org/apache/spark/graphx/lib/PageRank.scala
- NetworkX validation library: https://networkx.org/documentation/stable/
