

import sys
sys.path.append('./src')
from pygraphdb.kvstores import LevelDBStore, LMDBStore
from pygraphdb.sampling import SamplingHop, SamplingPattern
from pygraphdb.serializers import JSONSerializer, MessagePackSerializer, PickleSerializer, ProtobufSerializer
from pygraphdb.graphdb import GraphDB, Node, Edge

import builtins
from contextlib import contextmanager
import random
import shutil
import tempfile
import unittest 
import abc


@contextmanager
def blocked_import(package_name):
    original_import = builtins.__import__

    def import_hook(name, globals=None, locals=None, fromlist=(), level=0):
        if name == package_name or name.startswith(f"{package_name}."):
            raise ImportError(f"blocked import: {package_name}")
        return original_import(name, globals, locals, fromlist, level)

    builtins.__import__ = import_hook
    try:
        yield
    finally:
        builtins.__import__ = original_import


class OptionalDependencyTests(unittest.TestCase):
    def assert_missing_dependency_error(self, callable_obj, package_name):
        with self.assertRaisesRegex(
            ImportError,
            f"Missing optional dependency '{package_name}'.*python -m pip install {package_name}.*uv add {package_name}",
        ):
            callable_obj()

    def test_lmdb_store_reports_missing_lmdb_when_used(self):
        with blocked_import("lmdb"):
            self.assert_missing_dependency_error(lambda: LMDBStore(), "lmdb")

    def test_leveldb_store_reports_missing_plyvel_when_used(self):
        with blocked_import("plyvel"):
            self.assert_missing_dependency_error(lambda: LevelDBStore(), "plyvel")

    def test_messagepack_serializer_reports_missing_msgpack_when_used(self):
        with blocked_import("msgpack"):
            self.assert_missing_dependency_error(
                lambda: MessagePackSerializer().serialize({"name": "Alice"}),
                "msgpack",
            )

    def test_protobuf_serializer_reports_missing_protobuf_when_used(self):
        with blocked_import("google.protobuf"):
            self.assert_missing_dependency_error(
                lambda: ProtobufSerializer().serialize({"name": "Alice"}),
                "protobuf",
            )


class SerializerTests(unittest.TestCase):
    def serializer_round_trip_cases(self):
        return [
            PickleSerializer(),
            JSONSerializer(),
            MessagePackSerializer(),
            ProtobufSerializer(),
        ]

    def test_serializers_round_trip_json_like_dicts(self):
        payload = {
            "id": "alice",
            "properties": {
                "name": "Alice",
                "age": 30,
                "score": 9.5,
                "active": True,
                "tags": ["person", "employee"],
                "metadata": {"department": "Engineering"},
            },
        }

        for serializer in self.serializer_round_trip_cases():
            with self.subTest(serializer=serializer.__class__.__name__):
                self.assertEqual(serializer.deserialize(serializer.serialize(payload)), payload)

    def test_binary_serializers_round_trip_bytes(self):
        payload = {
            "edge_ids": [b"edge-1", b"edge-2"],
            "properties": {"raw": b"\x00\x01\x02"},
        }

        for serializer in [PickleSerializer(), MessagePackSerializer(), ProtobufSerializer()]:
            with self.subTest(serializer=serializer.__class__.__name__):
                self.assertEqual(serializer.deserialize(serializer.serialize(payload)), payload)


class SerializerGraphDBTests(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="graphdb_serializer_test_")

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_graphdb_round_trip_with_messagepack_serializer(self):
        graph_db = GraphDB(LMDBStore(path=self.test_dir), MessagePackSerializer())
        try:
            node_a = Node(node_id="alice", properties={"name": "Alice", "age": 30})
            node_b = Node(node_id="bob", properties={"name": "Bob"})
            edge = Edge(edge_id="alice-bob", source=node_a.get_id, target=node_b.get_id, properties={"relation": "friend"})

            graph_db.put_node(node_a)
            graph_db.put_node(node_b)
            graph_db.put_edge(edge)

            self.assertEqual(graph_db.get_node(b"alice").properties, node_a.properties)
            self.assertEqual(graph_db.get_edge(b"alice-bob").properties, edge.properties)
            self.assertEqual(graph_db.get_adjacency_list(b"alice", direction="any"), ["alice-bob"])
        finally:
            graph_db.close()

    def test_graphdb_round_trip_with_protobuf_serializer(self):
        graph_db = GraphDB(LMDBStore(path=self.test_dir), ProtobufSerializer())
        try:
            node_a = Node(node_id="alice", properties={"name": "Alice", "age": 30})
            node_b = Node(node_id="bob", properties={"name": "Bob"})
            edge = Edge(edge_id="alice-bob", source=node_a.get_id, target=node_b.get_id, properties={"relation": "friend"})

            graph_db.put_node(node_a)
            graph_db.put_node(node_b)
            graph_db.put_edge(edge)

            self.assertEqual(graph_db.get_node(b"alice").properties, node_a.properties)
            self.assertEqual(graph_db.get_edge(b"alice-bob").properties, edge.properties)
            self.assertEqual(graph_db.get_adjacency_list(b"alice", direction="any"), ["alice-bob"])
        finally:
            graph_db.close()


class SamplingConfigTests(unittest.TestCase):
    def test_sampling_hop_round_trips_dict_config(self):
        hop = SamplingHop.from_dict({"edge_type": "drug-to-protein", "direction": "out", "sample_size": 2})

        self.assertEqual(hop.edge_type, "drug-to-protein")
        self.assertEqual(hop.direction, "out")
        self.assertEqual(hop.sample_size, 2)
        self.assertEqual(
            hop.to_dict(),
            {"edge_type": "drug-to-protein", "direction": "out", "sample_size": 2},
        )

    def test_sampling_pattern_normalizes_dicts_and_hops(self):
        pattern = SamplingPattern([
            SamplingHop("drug-to-protein", sample_size=2),
            {"edge_type": "protein-to-disease", "direction": "out", "sample_size": 1},
        ])

        self.assertEqual(len(pattern), 2)
        self.assertEqual(pattern.hops[0].edge_type, "drug-to-protein")
        self.assertEqual(pattern.hops[1].sample_size, 1)

    def test_sampling_hop_validates_values(self):
        with self.assertRaisesRegex(ValueError, "direction"):
            SamplingHop("drug-to-protein", direction="sideways")
        with self.assertRaisesRegex(ValueError, "sample_size"):
            SamplingHop("drug-to-protein", sample_size=0)

class AbstractGraphDBBase(unittest.TestCase):
    """
    Base test case for GraphDB. Subclasses implement get_store() to return either an LMDB or LevelDB store.
    This ensures we run the same tests for both backends.
    """

    def get_store(self, path: str):
        """Should return an instance of the KVStore (LMDBStore or LevelDBStore)."""
        return LMDBStore(path=path)

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="graphdb_test_")
        self.serializer = PickleSerializer()  # or JSONSerializer()
        self.store = self.get_store(self.test_dir)
        self.graph_db = GraphDB(self.store, self.serializer)

    def tearDown(self):
        # Close the DB.
        self.graph_db.close()
        # Remove temp directory.
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_single_node(self):
        # 1. Create a node
        node_a = Node(properties={"name": "Alice", "age": 30})
        self.graph_db.put_node(node_a)

        # 2. Retrieve the node
        fetched = self.graph_db.get_node(node_a.get_id_bytes)
        self.assertIsNotNone(fetched)
        self.assertEqual(fetched.properties["name"], "Alice")
        self.assertEqual(fetched.properties["age"], 30)

        # 3. Delete the node
        self.graph_db.delete_node(node_a.get_id_bytes)
        deleted = self.graph_db.get_node(node_a.get_id_bytes)
        self.assertIsNone(deleted)

    def test_single_edge(self):
        # Create two nodes
        node_a = Node(properties={"name": "Alice"})
        node_b = Node(properties={"name": "Bob"})
        self.graph_db.put_node(node_a)
        self.graph_db.put_node(node_b)

        # Create an edge between them
        edge_ab = Edge(source=node_a.get_id, target=node_b.get_id, properties={"relation": "friend"})
        self.graph_db.put_edge(edge_ab)

        # Retrieve the edge
        fetched_edge = self.graph_db.get_edge(edge_ab.get_id_bytes)
        self.assertIsNotNone(fetched_edge)
        self.assertEqual(fetched_edge.properties["relation"], "friend")
        self.assertEqual(fetched_edge.source, node_a.get_id)
        self.assertEqual(fetched_edge.target, node_b.get_id)

        # Delete the edge
        self.graph_db.delete_edge(edge_ab.get_id_bytes)
        deleted_edge = self.graph_db.get_edge(edge_ab.get_id_bytes)
        self.assertIsNone(deleted_edge)

    def test_bfs_simple(self):
        """
        Verify BFS works with adjacency-based graph structure.
        We'll create a small triangle graph: A-B, B-C, A-C
        BFS from A should visit A, B, C (in BFS order).
        """
        node_a = Node(properties={"label": "A"})
        node_b = Node(properties={"label": "B"})
        node_c = Node(properties={"label": "C"})

        self.graph_db.put_node(node_a)
        self.graph_db.put_node(node_b)
        self.graph_db.put_node(node_c)

        # Edges
        edge_ab = Edge(source=node_a.get_id, target=node_b.get_id)
        edge_bc = Edge(source=node_b.get_id, target=node_c.get_id)
        edge_ac = Edge(source=node_a.get_id, target=node_c.get_id)

        self.graph_db.put_edge(edge_ab)
        self.graph_db.put_edge(edge_bc)
        self.graph_db.put_edge(edge_ac)

        # BFS from A
        bfs_result = self.graph_db.bfs(node_a.get_id_bytes)
        # BFS typically visits in the order [A, B, C], but the exact order can vary
        # We'll check that BFS visited all 3 exactly once.
        self.assertEqual(set(bfs_result), {node_a.get_id_bytes, node_b.get_id_bytes, node_c.get_id_bytes})
        self.assertEqual(len(bfs_result), 3, "BFS should visit exactly 3 nodes")


    def test_bulk_nodes(self):
        """Example test for multi-node put/get if your GraphDB supports it."""
        # Suppose we define a hypothetical put_nodes() and get_nodes() in GraphDB.
        # If you haven't implemented these, consider them placeholders.

        # Create multiple nodes
        nodes = [Node(properties={"name": f"User{i}"}) for i in range(5)]

        # We'll store them individually in this example, but you might have a real put_nodes method.
        for n in nodes:
            self.graph_db.put_node(n)

        # Bulk retrieval (if your GraphDB has get_nodes):
        # If not, we just do a loop.
        retrieved = self.graph_db.get_nodes([n.get_id_bytes for n in nodes])
        # retrieved = [self.graph_db.get_node(n.get_id_bytes) for n in nodes]

        # Check the results
        for i, r in enumerate(retrieved):
            self.assertIsNotNone(r)
            self.assertEqual(r.properties["name"], f"User{i}")

        # Cleanup
        for n in nodes:
            self.graph_db.delete_node(n.get_id_bytes)
        for n in nodes:
            self.assertIsNone(self.graph_db.get_node(n.get_id_bytes))

    def test_bulk_edges(self):
        """Example test for multi-edge put/get if your GraphDB supports it."""
        # Create some nodes
        node_a = Node(properties={"label": "A"})
        node_b = Node(properties={"label": "B"})
        node_c = Node(properties={"label": "C"})
        self.graph_db.put_node(node_a)
        self.graph_db.put_node(node_b)
        self.graph_db.put_node(node_c)

        edges = [
            Edge(source=node_a.get_id, target=node_b.get_id, properties={"weight": 1}),
            Edge(source=node_b.get_id, target=node_c.get_id, properties={"weight": 2}),
            Edge(source=node_a.get_id, target=node_c.get_id, properties={"weight": 3})
        ]

        for e in edges:
            self.graph_db.put_edge(e)

        # Hypothetical get_edges method
        fetched_edges = [self.graph_db.get_edge(e.get_id_bytes) for e in edges]
        for i, e in enumerate(fetched_edges):
            self.assertIsNotNone(e)
            self.assertIn("weight", e.properties)

        # Cleanup
        for e in edges:
            self.graph_db.delete_edge(e.get_id_bytes)
        for e in edges:
            self.assertIsNone(self.graph_db.get_edge(e.get_id_bytes))

    def test_put_edges_bulk(self):
        """
        Verify bulk insertion of multiple edges, with adjacency updates in one pass.
        We'll create 3 nodes, then 3 edges in bulk, then check adjacency and BFS.
        """
        # Create 3 nodes
        node_a = Node(properties={"label": "A"})
        node_b = Node(properties={"label": "B"})
        node_c = Node(properties={"label": "C"})
        self.graph_db.put_node(node_a)
        self.graph_db.put_node(node_b)
        self.graph_db.put_node(node_c)

        # Suppose we have a 'put_edges_bulk' in GraphDB
        if not hasattr(self.graph_db, "put_edges_bulk"):
            self.skipTest("GraphDB does not implement put_edges_bulk")

        edge_ab = Edge(source=node_a.get_id, target=node_b.get_id, properties={"weight": 1})
        edge_bc = Edge(source=node_b.get_id, target=node_c.get_id, properties={"weight": 2})
        edge_ac = Edge(source=node_a.get_id, target=node_c.get_id, properties={"weight": 3})

        # Insert edges in one bulk call
        self.graph_db.put_edges_bulk([edge_ab, edge_bc, edge_ac])

        # Check adjacency
        adj_a = set(self.graph_db.get_adjacency_list(node_a.get_id_bytes,direction = 'any'))
        adj_b = set(self.graph_db.get_adjacency_list(node_b.get_id_bytes, direction = 'any'))
        adj_c = set(self.graph_db.get_adjacency_list(node_c.get_id_bytes, direction = 'any'))

        # Each node is connected to the 2 edges that link it
        self.assertIn(edge_ab.get_id_bytes, adj_a)
        self.assertIn(edge_ac.get_id_bytes, adj_a)
        self.assertIn(edge_ab.get_id_bytes, adj_b)
        self.assertIn(edge_bc.get_id_bytes, adj_b)
        self.assertIn(edge_bc.get_id_bytes, adj_c)
        self.assertIn(edge_ac.get_id_bytes, adj_c)

        # BFS from A
        # bfs_result = self.graph_db.bfs(node_a.get_id_bytes)
        # print(bfs_result)
        # self.assertEqual(set(bfs_result), {node_a.get_id_bytes, node_b.get_id_bytes, node_c.get_id_bytes})
        # self.assertEqual(len(bfs_result), 3, "BFS should visit exactly 3 nodes")


class TypedTraversalBase(unittest.TestCase):
    def get_store(self, path: str):
        return LMDBStore(path=path)

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="typed_graphdb_test_")
        self.graph_db = GraphDB(self.get_store(self.test_dir), PickleSerializer())

    def tearDown(self):
        self.graph_db.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def populate_typed_graph(self):
        nodes = [
            Node(node_id="drug-1", properties={"kind": "drug"}),
            Node(node_id="drug-2", properties={"kind": "drug"}),
            Node(node_id="protein-1", properties={"kind": "protein"}),
            Node(node_id="protein-2", properties={"kind": "protein"}),
            Node(node_id="protein-3", properties={"kind": "protein"}),
            Node(node_id="disease-1", properties={"kind": "disease"}),
            Node(node_id="disease-2", properties={"kind": "disease"}),
            Node(node_id="disease-3", properties={"kind": "disease"}),
        ]
        for node in nodes:
            self.graph_db.put_node(node)

        edges = [
            Edge(edge_id="d1-p1", source="drug-1", target="protein-1", properties={"type": "drug-to-protein"}),
            Edge(edge_id="d1-p2", source="drug-1", target="protein-2", properties={"type": "drug-to-protein"}),
            Edge(edge_id="d1-disease", source="drug-1", target="disease-1", properties={"type": "drug-to-disease"}),
            Edge(edge_id="d2-p3", source="drug-2", target="protein-3", properties={"type": "drug-to-protein"}),
            Edge(edge_id="p1-dis1", source="protein-1", target="disease-1", properties={"type": "protein-to-disease"}),
            Edge(edge_id="p1-dis2", source="protein-1", target="disease-2", properties={"type": "protein-to-disease"}),
            Edge(edge_id="p2-dis3", source="protein-2", target="disease-3", properties={"type": "protein-to-disease"}),
        ]
        self.graph_db.put_edges_bulk(edges)
        return edges

    def test_typed_adjacency_filters_type_and_direction(self):
        self.populate_typed_graph()

        self.assertEqual(
            set(self.graph_db.neighbors_by_edge_type("drug-1", "drug-to-protein", direction="out")),
            {b"protein-1", b"protein-2"},
        )
        self.assertEqual(
            self.graph_db.neighbors_by_edge_type("drug-1", "drug-to-protein", direction="in"),
            [],
        )
        self.assertEqual(
            self.graph_db.neighbors_by_edge_type("protein-1", "drug-to-protein", direction="in"),
            [b"drug-1"],
        )
        self.assertEqual(
            self.graph_db.neighbors_by_edge_type("drug-1", "drug-to-disease", direction="out"),
            [b"disease-1"],
        )

    def test_deleting_typed_edge_removes_typed_adjacency(self):
        self.populate_typed_graph()

        self.graph_db.delete_edge(b"d1-p1")

        self.assertEqual(
            self.graph_db.neighbors_by_edge_type("drug-1", "drug-to-protein", direction="out"),
            [b"protein-2"],
        )
        self.assertEqual(
            self.graph_db.neighbors_by_edge_type("protein-1", "drug-to-protein", direction="in"),
            [],
        )

    def test_replacing_typed_edge_removes_stale_typed_adjacency(self):
        self.populate_typed_graph()

        self.graph_db.put_edge(
            Edge(edge_id="d1-p1", source="drug-1", target="disease-2", properties={"type": "drug-to-disease"})
        )

        self.assertEqual(
            self.graph_db.neighbors_by_edge_type("drug-1", "drug-to-protein", direction="out"),
            [b"protein-2"],
        )
        self.assertEqual(
            set(self.graph_db.neighbors_by_edge_type("drug-1", "drug-to-disease", direction="out")),
            {b"disease-1", b"disease-2"},
        )

    def test_sample_neighbors_uses_typed_frontier(self):
        self.populate_typed_graph()

        sample = self.graph_db.sample_neighbors(
            "drug-1",
            "drug-to-protein",
            direction="out",
            sample_size=1,
            rng=random.Random(7),
        )

        self.assertEqual(len(sample), 1)
        self.assertEqual(sample[0]["edge_type"], "drug-to-protein")
        self.assertIn(sample[0]["neighbor_id"], {b"protein-1", b"protein-2"})

    def test_sample_typed_paths_respects_edge_type_sequence(self):
        self.populate_typed_graph()

        paths = self.graph_db.sample_typed_paths(
            ["drug-1", "drug-2"],
            SamplingPattern([
                SamplingHop("drug-to-protein", direction="out", sample_size=2),
                SamplingHop("protein-to-disease", direction="out", sample_size=1),
            ]),
            rng=random.Random(3),
        )

        self.assertTrue(paths)
        for sampled_path in paths:
            self.assertEqual(len(sampled_path["path"]), 2)
            self.assertEqual(sampled_path["path"][0]["edge_type"], "drug-to-protein")
            self.assertEqual(sampled_path["path"][1]["edge_type"], "protein-to-disease")
            self.assertTrue(sampled_path["path"][0]["target_id"].startswith(b"protein-"))
            self.assertTrue(sampled_path["path"][1]["target_id"].startswith(b"disease-"))

    def test_sample_typed_subgraph_materializes_sampled_records(self):
        self.populate_typed_graph()

        subgraph = self.graph_db.sample_typed_subgraph(
            ["drug-1"],
            [
                {"edge_type": "drug-to-protein", "direction": "out", "sample_size": 1},
                {"edge_type": "protein-to-disease", "direction": "out", "sample_size": 1},
            ],
            rng=random.Random(11),
        )

        self.assertIn(b"drug-1", subgraph["nodes"])
        self.assertTrue(subgraph["edges"])
        self.assertTrue(subgraph["paths"])
        self.assertTrue(all(node is not None for node in subgraph["nodes"].values()))
        self.assertTrue(all(edge is not None for edge in subgraph["edges"].values()))

    def test_rebuild_typed_adjacency_from_edge_records(self):
        for node_id in ["drug-1", "protein-1"]:
            self.graph_db.put_node(Node(node_id=node_id))
        edge = Edge(edge_id="d1-p1", source="drug-1", target="protein-1", properties={"type": "drug-to-protein"})
        self.graph_db.store.put_edge(edge.get_id_bytes, self.graph_db.entity_serializer.serialize(edge, "Edge"))

        self.assertEqual(self.graph_db.neighbors_by_edge_type("drug-1", "drug-to-protein", direction="out"), [])

        self.assertEqual(self.graph_db.rebuild_typed_adjacency(), 1)
        self.assertEqual(
            self.graph_db.neighbors_by_edge_type("drug-1", "drug-to-protein", direction="out"),
            [b"protein-1"],
        )


class TestTypedTraversalWithLMDB(TypedTraversalBase):
    def get_store(self, path: str):
        return LMDBStore(path=path)


class TestTypedTraversalWithLevelDB(TypedTraversalBase):
    def get_store(self, path: str):
        return LevelDBStore(path=path)

class TestGraphDBWithLMDB(AbstractGraphDBBase):
    def get_store(self, path: str):
        # Return LMDBStore pointing to a temporary directory.
        return LMDBStore(path=path)


class TestGraphDBWithLevelDB(AbstractGraphDBBase):
    def get_store(self, path: str):
        # Return LevelDBStore pointing to a temporary directory.
        return LevelDBStore(path=path)


if __name__ == "__main__":
    unittest.main()
