"""Independently verify every exported vertex using NetworkX and scalar PageRank."""
import argparse
from collections import Counter
import csv
from datetime import datetime, timezone
import gzip
import json
import math
from pathlib import Path
import networkx as nx


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--input', required=True)
    p.add_argument('--results', required=True)
    p.add_argument('--vertices')
    p.add_argument('--undirected', action='store_true')
    p.add_argument('--output', required=True)
    a = p.parse_args()
    graph = nx.DiGraph()
    opener = gzip.open if a.input.endswith('.gz') else open
    with opener(a.input, 'rt') as file:
        for line in file:
            if not line.strip() or line.lstrip().startswith('#'):
                continue
            u, v = map(int, line.split())
            graph.add_nodes_from([u, v])
            if u != v:
                graph.add_edge(u, v)
    if a.vertices:
        with open(a.vertices) as file:
            for line in file:
                if line.strip() and not line.lstrip().startswith('#'):
                    graph.add_node(int(line.strip()))
    if a.undirected:
        graph = graph.to_undirected().to_directed()
    undirected = graph.to_undirected()
    triangles = nx.triangles(undirected)
    components = list(nx.connected_components(undirected))
    membership = {v: min(component) for component in components for v in component}
    result_dir = Path(a.results)
    result = json.loads((result_dir / 'results.json').read_text())
    rows = []
    for path in sorted((result_dir / 'vertex_metrics').glob('part-*.csv')):
        with path.open(newline='') as file:
            rows.extend(csv.DictReader(file))
    assert len(rows) == len(graph), (len(rows), len(graph))
    assert len({int(row['id']) for row in rows}) == len(graph)
    reset = result['parameters']['reset_probability']
    iterations = result['parameters']['pagerank_iterations']
    # Match GraphX's documented finite-iteration recurrence, including sinks:
    # start at 1; add reset to every vertex each iteration; normalize once at end.
    ranks = dict.fromkeys(graph, 1.0)
    for _ in range(iterations):
        updated = dict.fromkeys(graph, reset)
        for u, v in graph.edges:
            updated[v] += (1 - reset) * ranks[u] / graph.out_degree(u)
        ranks = updated
    rank_sum = sum(ranks.values())
    ranks = {v: rank * len(graph) / rank_sum for v, rank in ranks.items()}
    maximum_error = 0.0
    for row in rows:
        vertex = int(row['id'])
        assert int(row['outDegree']) == graph.out_degree(vertex), ('outdegree', vertex)
        assert int(row['inDegree']) == graph.in_degree(vertex), ('indegree', vertex)
        assert int(row['triangle_count']) == triangles[vertex], ('triangle', vertex)
        assert int(row['component_id']) == membership[vertex], ('component', vertex)
        error = abs(float(row['pagerank']) - ranks[vertex])
        maximum_error = max(maximum_error, error)
        assert math.isclose(float(row['pagerank']), ranks[vertex], rel_tol=1e-9, abs_tol=1e-9), ('pagerank', vertex, row['pagerank'], ranks[vertex])
        assert math.isclose(float(row['pagerank_probability']), float(row['pagerank']) / len(graph), rel_tol=1e-9, abs_tol=1e-12)
    def top(values, key):
        return [{'id': vertex, key: value} for vertex, value in sorted(values.items(), key=lambda pair: (-pair[1], pair[0]))[:5]]
    queries = result['queries']
    assert queries['a_outdegree'] == top(dict(graph.out_degree()), 'outDegree')
    assert queries['b_indegree'] == top(dict(graph.in_degree()), 'inDegree')
    assert queries['e_triangles'] == top(triangles, 'triangle_count')
    expected_components = sorted([{'component_id': min(c), 'size': len(c)} for c in components], key=lambda c: (-c['size'], c['component_id']))[:5]
    assert queries['d_connected_components'] == expected_components
    assert [row['id'] for row in queries['c_pagerank']] == [v for v, _ in sorted(ranks.items(), key=lambda pair: (-pair[1], pair[0]))[:5]]
    stats = result['graph_statistics']
    assert stats['vertices'] == len(graph)
    assert stats['directed_edges'] == graph.number_of_edges()
    assert stats['weak_components'] == len(components)
    assert stats['total_undirected_triangles'] == sum(triangles.values()) // 3
    report = dict(passed=True, completed_at=datetime.now(timezone.utc).isoformat(),
                  vertices_checked=len(rows), directed_edges=graph.number_of_edges(),
                  weak_components=len(components), total_undirected_triangles=sum(triangles.values()) // 3,
                  max_absolute_pagerank_error=maximum_error,
                  checks=['all vertex indegrees/outdegrees', 'all vertex triangle counts',
                          'all component memberships', 'all finite-iteration GraphX PageRank values',
                          'all five top-result tables', 'graph totals'],
                  reference='NetworkX graph/triangles/components; independent scalar GraphX PageRank recurrence')
    Path(a.output).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
