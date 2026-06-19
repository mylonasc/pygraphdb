"""Small benchmark suite for PyGraphDB.

Run with:

    uv run python benchmarks.py --nodes 10000 --edges 50000

The benchmarks are intentionally dependency-light and report operations per
second for common graph workloads. They are not a substitute for production
profiling, but they provide a repeatable baseline while the engine evolves.
"""

import argparse
import random
import time

from graphdb import Edge, GraphDB, Node
from kvstores import InMemoryKVStore
from serializers import MessagePackSerializer


def timed(label, func):
    start = time.perf_counter()
    result = func()
    elapsed = time.perf_counter() - start
    print(f"{label}: {elapsed:.4f}s")
    return result, elapsed


def build_graph(num_nodes, num_edges, seed):
    random.seed(seed)
    graph = GraphDB(InMemoryKVStore(), MessagePackSerializer())

    def insert_nodes():
        for index in range(num_nodes):
            graph.put_node(
                Node(
                    f"n{index}",
                    labels=["Node", f"Group{index % 10}"],
                    properties={"group": index % 10, "rank": index},
                )
            )

    def insert_edges():
        for index in range(num_edges):
            source = f"n{random.randrange(num_nodes)}"
            target = f"n{random.randrange(num_nodes)}"
            graph.put_edge(
                Edge(
                    f"e{index}",
                    source=source,
                    target=target,
                    type=f"T{index % 5}",
                    properties={"weight": index % 100},
                )
            )

    _, node_time = timed(f"insert_nodes ({num_nodes})", insert_nodes)
    _, edge_time = timed(f"insert_edges ({num_edges})", insert_edges)
    print(f"node_insert_rate: {num_nodes / node_time:,.0f} ops/s")
    print(f"edge_insert_rate: {num_edges / edge_time:,.0f} ops/s")
    return graph


def run_queries(graph, num_nodes, samples, seed):
    random.seed(seed)
    sample_nodes = [f"n{random.randrange(num_nodes)}" for _ in range(samples)]

    def neighbor_lookup():
        total = 0
        for node_id in sample_nodes:
            total += len(graph.neighbors(node_id, "out"))
        return total

    def label_lookup():
        return sum(len(graph.nodes_by_label(f"Group{i}")) for i in range(10))

    def property_lookup():
        return sum(len(graph.find_nodes(properties={"group": i})) for i in range(10))

    def edge_type_lookup():
        return sum(len(graph.edges_by_type(f"T{i}")) for i in range(5))

    def bfs_lookup():
        total = 0
        for node_id in sample_nodes[: max(1, samples // 20)]:
            total += len(graph.bfs(node_id, direction="out", max_depth=2))
        return total

    _, neighbor_time = timed(f"neighbor_lookup ({samples})", neighbor_lookup)
    timed("label_lookup", label_lookup)
    timed("property_lookup", property_lookup)
    timed("edge_type_lookup", edge_type_lookup)
    timed("bfs_depth_2", bfs_lookup)
    print(f"neighbor_lookup_rate: {samples / neighbor_time:,.0f} ops/s")


def main():
    parser = argparse.ArgumentParser(description="Run PyGraphDB benchmarks")
    parser.add_argument("--nodes", type=int, default=10_000)
    parser.add_argument("--edges", type=int, default=50_000)
    parser.add_argument("--samples", type=int, default=5_000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    graph = build_graph(args.nodes, args.edges, args.seed)
    run_queries(graph, args.nodes, args.samples, args.seed)


if __name__ == "__main__":
    main()
