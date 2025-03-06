

import sys
sys.path.append('./src')
from kvstores import LevelDBStore, LMDBStore
from serializers import PickleSerializer, JSONSerializer
from graphdb import GraphDB, Node, Edge

import shutil
import tempfile
import unittest 
import abc

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

