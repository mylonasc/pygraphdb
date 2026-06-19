from threading import Thread
import shutil
import tempfile
import unittest
import random

from graphdb import ConstraintError, Edge, Node, NodeNotFoundError, GraphDB
from kvstores import InMemoryKVStore, LMDBStore
from serializers import MessagePackSerializer


class FailingOnceAdjacencyStore(InMemoryKVStore):
    def __init__(self):
        super().__init__()
        self.fail_next_adjacency_write = False

    def put_adjacency(self, node_id: bytes, value: bytes) -> None:
        if self.fail_next_adjacency_write:
            self.fail_next_adjacency_write = False
            raise RuntimeError("injected adjacency failure")
        super().put_adjacency(node_id, value)

    def add_adjacency_edge(self, source_id: str, target_id: str, edge_id: str) -> None:
        if self.fail_next_adjacency_write:
            self.fail_next_adjacency_write = False
            raise RuntimeError("injected adjacency failure")
        super().add_adjacency_edge(source_id, target_id, edge_id)


class GraphDBTest(unittest.TestCase):
    def setUp(self):
        self.graph = GraphDB(InMemoryKVStore(), MessagePackSerializer())

    def test_nodes_round_trip_with_labels_and_properties(self):
        self.graph.put_node(Node("a", labels=["Person"], properties={"name": "Alice", "age": 30}))

        node = self.graph.get_node("a")

        self.assertEqual(node.get_id, "a")
        self.assertEqual(node.labels, frozenset({"Person"}))
        self.assertEqual(node.properties, {"name": "Alice", "age": 30})

    def test_edge_requires_existing_endpoints(self):
        self.graph.put_node(Node("a"))

        with self.assertRaises(NodeNotFoundError):
            self.graph.put_edge(Edge("e1", source="a", target="missing", type="KNOWS"))

    def test_put_edge_maintains_directional_adjacency(self):
        self.graph.put_node(Node("a"))
        self.graph.put_node(Node("b"))
        self.graph.put_edge(Edge("e1", source="a", target="b", type="KNOWS"))

        self.assertEqual(self.graph.get_adjacency_list("a", "out"), ["e1"])
        self.assertEqual(self.graph.get_adjacency_list("a", "in"), [])
        self.assertEqual(self.graph.get_adjacency_list("b", "in"), ["e1"])
        self.assertEqual(self.graph.neighbors("a", "out"), ["b"])
        self.assertEqual(self.graph.neighbors("b", "in"), ["a"])

    def test_edge_upsert_is_idempotent_and_moves_adjacency(self):
        for node_id in ["a", "b", "c"]:
            self.graph.put_node(Node(node_id))

        self.graph.put_edge(Edge("e1", source="a", target="b"))
        self.graph.put_edge(Edge("e1", source="a", target="b"))
        self.graph.put_edge(Edge("e1", source="a", target="c"))

        self.assertEqual(self.graph.get_adjacency_list("a", "out"), ["e1"])
        self.assertEqual(self.graph.get_adjacency_list("b", "in"), [])
        self.assertEqual(self.graph.get_adjacency_list("c", "in"), ["e1"])

    def test_delete_edge_cleans_adjacency(self):
        self.graph.put_node(Node("a"))
        self.graph.put_node(Node("b"))
        self.graph.put_edge(Edge("e1", source="a", target="b"))

        self.graph.delete_edge("e1")

        self.assertIsNone(self.graph.get_edge("e1"))
        self.assertEqual(self.graph.get_adjacency_list("a", "out"), [])
        self.assertEqual(self.graph.get_adjacency_list("b", "in"), [])

    def test_delete_node_restrict_and_detach(self):
        self.graph.put_node(Node("a"))
        self.graph.put_node(Node("b"))
        self.graph.put_edge(Edge("e1", source="a", target="b"))

        with self.assertRaises(ConstraintError):
            self.graph.delete_node("a")

        self.graph.delete_node("a", mode="detach")

        self.assertIsNone(self.graph.get_node("a"))
        self.assertIsNone(self.graph.get_edge("e1"))
        self.assertEqual(self.graph.get_adjacency_list("b", "in"), [])

    def test_bfs_uses_deque_and_respects_direction(self):
        for node_id in ["a", "b", "c"]:
            self.graph.put_node(Node(node_id))
        self.graph.put_edge(Edge("ab", source="a", target="b"))
        self.graph.put_edge(Edge("bc", source="b", target="c"))

        self.assertEqual(self.graph.bfs("a", direction="out"), ["a", "b", "c"])
        self.assertEqual(self.graph.bfs("c", direction="out"), ["c"])
        self.assertEqual(self.graph.bfs("c", direction="in"), ["c", "b", "a"])

    def test_property_graph_lookup_api(self):
        self.graph.put_node(Node("a", labels=["Person"], properties={"name": "Alice", "age": 30}))
        self.graph.put_node(Node("b", labels=["Person", "Employee"], properties={"name": "Bob", "age": 40}))
        self.graph.put_node(Node("c", labels=["Company"], properties={"name": "Acme"}))
        self.graph.put_edge(Edge("e1", source="a", target="b", type="KNOWS", properties={"since": 2020}))
        self.graph.put_edge(Edge("e2", source="b", target="c", type="WORKS_AT", properties={"role": "Engineer"}))

        self.assertEqual([n.get_id for n in self.graph.find_nodes(labels=["Person"])], ["a", "b"])
        self.assertEqual([n.get_id for n in self.graph.find_nodes(labels=["Employee"])], ["b"])
        self.assertEqual([n.get_id for n in self.graph.find_nodes(properties={"name": "Acme"})], ["c"])
        self.assertEqual([n.get_id for n in self.graph.find_nodes(predicate=lambda n: n.properties.get("age", 0) >= 35)], ["b"])
        self.assertEqual([e.get_id for e in self.graph.find_edges(type="KNOWS")], ["e1"])
        self.assertEqual([e.get_id for e in self.graph.find_edges(source="b")], ["e2"])
        self.assertEqual([e.get_id for e in self.graph.find_edges(properties={"role": "Engineer"})], ["e2"])

    def test_bulk_gets_preserve_order_and_missing_values(self):
        self.graph.put_nodes([Node("a"), Node("b")])

        nodes = self.graph.get_nodes(["b", "missing", "a"])

        self.assertEqual([node.get_id if node else None for node in nodes], ["b", None, "a"])

    def test_indexes_are_updated_on_upsert_and_delete(self):
        self.graph.put_node(Node("a", labels=["Person"], properties={"name": "Alice"}))
        self.graph.put_node(Node("a", labels=["Company"], properties={"name": "Acme"}))

        self.assertEqual(self.graph.find_nodes(labels=["Person"]), [])
        self.assertEqual([n.get_id for n in self.graph.find_nodes(labels=["Company"])], ["a"])
        self.assertEqual(self.graph.find_nodes(properties={"name": "Alice"}), [])
        self.assertEqual([n.get_id for n in self.graph.find_nodes(properties={"name": "Acme"})], ["a"])

        self.graph.delete_node("a")

        self.assertEqual(self.graph.find_nodes(labels=["Company"]), [])

    def test_edge_indexes_are_updated_on_upsert_and_delete(self):
        for node_id in ["a", "b"]:
            self.graph.put_node(Node(node_id))
        self.graph.put_edge(Edge("e1", source="a", target="b", type="KNOWS", properties={"since": 2020}))
        self.graph.put_edge(Edge("e1", source="a", target="b", type="LIKES", properties={"since": 2021}))

        self.assertEqual(self.graph.find_edges(type="KNOWS"), [])
        self.assertEqual([e.get_id for e in self.graph.find_edges(type="LIKES")], ["e1"])
        self.assertEqual(self.graph.find_edges(properties={"since": 2020}), [])
        self.assertEqual([e.get_id for e in self.graph.find_edges(properties={"since": 2021})], ["e1"])

        self.graph.delete_edge("e1")

        self.assertEqual(self.graph.find_edges(type="LIKES"), [])

    def test_failed_edge_write_rolls_back_partial_state(self):
        store = FailingOnceAdjacencyStore()
        graph = GraphDB(store, MessagePackSerializer())
        graph.put_node(Node("a"))
        graph.put_node(Node("b"))
        store.fail_next_adjacency_write = True

        with self.assertRaises(RuntimeError):
            graph.put_edge(Edge("e1", source="a", target="b"))

        self.assertIsNone(graph.get_edge("e1"))
        self.assertEqual(graph.get_adjacency_list("a", "out"), [])
        self.assertEqual(graph.get_adjacency_list("b", "in"), [])

    def test_concurrent_edge_inserts_do_not_lose_adjacency_updates(self):
        graph = GraphDB(InMemoryKVStore(), MessagePackSerializer())
        graph.put_node(Node("source"))
        for index in range(50):
            graph.put_node(Node(f"target-{index}"))

        def insert_edge(index):
            graph.put_edge(Edge(f"edge-{index}", source="source", target=f"target-{index}"))

        threads = [Thread(target=insert_edge, args=(index,)) for index in range(50)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(len(graph.get_adjacency_list("source", "out")), 50)
        self.assertEqual(len(graph.neighbors("source", "out")), 50)
        self.assertEqual(len(graph.find_edges(source="source")), 50)

    def test_in_memory_store_uses_per_edge_adjacency_records(self):
        store = InMemoryKVStore()
        graph = GraphDB(store, MessagePackSerializer())
        graph.put_node(Node("a"))
        graph.put_node(Node("b"))

        graph.put_edge(Edge("e1", source="a", target="b"))

        self.assertEqual(store.adjacency, {})
        self.assertEqual(store.out_adjacency, {"a": {"e1": "b"}})
        self.assertEqual(store.in_adjacency, {"b": {"e1": "a"}})
        self.assertEqual(graph.neighbors("a", "out"), ["b"])

    def test_common_property_graph_operations(self):
        self.graph.put_node(Node("a", labels=["Person"], properties={"name": "Alice"}))
        self.graph.put_node(Node("b", labels=["Person"], properties={"name": "Bob"}))
        self.graph.put_edge(Edge("e1", source="a", target="b", type="KNOWS", properties={"since": 2020}))

        self.assertTrue(self.graph.has_node("a"))
        self.assertTrue(self.graph.has_edge("e1"))
        self.assertEqual(self.graph.count_nodes(), 2)
        self.assertEqual(self.graph.count_edges(), 1)
        self.assertEqual([n.get_id for n in self.graph.nodes_by_label("Person")], ["a", "b"])
        self.assertEqual([e.get_id for e in self.graph.edges_by_type("KNOWS")], ["e1"])

        self.graph.add_label("a", "Employee")
        self.assertEqual([n.get_id for n in self.graph.nodes_by_label("Employee")], ["a"])
        self.graph.remove_label("a", "Employee")
        self.assertEqual(self.graph.nodes_by_label("Employee"), [])

        self.graph.set_node_property("a", "age", 30)
        self.assertEqual(self.graph.node_properties("a")["age"], 30)
        self.assertEqual([n.get_id for n in self.graph.find_nodes(properties={"age": 30})], ["a"])
        self.graph.remove_node_property("a", "age")
        self.assertEqual(self.graph.find_nodes(properties={"age": 30}), [])

        self.graph.set_edge_property("e1", "weight", 1.5)
        self.assertEqual(self.graph.edge_properties("e1")["weight"], 1.5)
        self.assertEqual([e.get_id for e in self.graph.find_edges(properties={"weight": 1.5})], ["e1"])
        self.graph.remove_edge_property("e1", "weight")
        self.assertEqual(self.graph.find_edges(properties={"weight": 1.5}), [])

    def test_rename_label_and_edge_type(self):
        self.graph.put_node(Node("a", labels=["OldLabel"]))
        self.graph.put_node(Node("b", labels=["OldLabel"]))
        self.graph.put_edge(Edge("e1", source="a", target="b", type="OLD_TYPE"))

        self.assertEqual(self.graph.rename_label("OldLabel", "NewLabel"), 2)
        self.assertEqual([n.get_id for n in self.graph.nodes_by_label("NewLabel")], ["a", "b"])
        self.assertEqual(self.graph.nodes_by_label("OldLabel"), [])

        self.assertEqual(self.graph.rename_edge_type("OLD_TYPE", "NEW_TYPE"), 1)
        self.assertEqual([e.get_id for e in self.graph.edges_by_type("NEW_TYPE")], ["e1"])
        self.assertEqual(self.graph.edges_by_type("OLD_TYPE"), [])

    def test_integrity_check_and_rebuild_indexes(self):
        self.graph.put_node(Node("a", labels=["Person"], properties={"name": "Alice"}))
        self.graph.put_node(Node("b"))
        self.graph.put_edge(Edge("e1", source="a", target="b", type="KNOWS"))

        self.assertTrue(self.graph.check_integrity()["ok"])
        self.graph.store.clear_indexes()
        self.assertEqual(self.graph.find_nodes(labels=["Person"]), [])
        self.assertEqual(self.graph.neighbors("a", "out"), [])

        self.graph.rebuild_indexes()

        self.assertEqual([n.get_id for n in self.graph.find_nodes(labels=["Person"])], ["a"])
        self.assertEqual(self.graph.neighbors("a", "out"), ["b"])
        self.assertTrue(self.graph.check_integrity()["ok"])

    def test_randomized_mutations_preserve_integrity(self):
        random.seed(7)
        for index in range(20):
            self.graph.put_node(Node(f"n{index}", labels=["Node"], properties={"group": index % 3}))
        live_edges = set()
        for index in range(100):
            source = f"n{random.randrange(20)}"
            target = f"n{random.randrange(20)}"
            edge_id = f"e{index}"
            self.graph.put_edge(Edge(edge_id, source=source, target=target, type="LINK", properties={"i": index}))
            live_edges.add(edge_id)
            if index % 5 == 0 and live_edges:
                deleted = sorted(live_edges)[0]
                self.graph.delete_edge(deleted)
                live_edges.discard(deleted)
            self.assertTrue(self.graph.check_integrity()["ok"])

        self.assertEqual(self.graph.count_edges(type="LINK"), len(live_edges))

    def test_compact_hook_for_in_memory_store_is_noop(self):
        self.assertIsNone(self.graph.compact())


class LMDBGraphDBTest(unittest.TestCase):
    def setUp(self):
        try:
            import lmdb  # noqa: F401
        except ModuleNotFoundError:
            self.skipTest("lmdb extra is not installed")
        self.test_dir = tempfile.mkdtemp(prefix="pygraphdb_lmdb_")
        self.graph = GraphDB(LMDBStore(path=self.test_dir), MessagePackSerializer())

    def tearDown(self):
        if hasattr(self, "graph"):
            self.graph.close()
        if hasattr(self, "test_dir"):
            shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_lmdb_persists_adjacency_and_indexes(self):
        self.graph.put_node(Node("a", labels=["Person"], properties={"name": "Alice"}))
        self.graph.put_node(Node("b", labels=["Person"], properties={"name": "Bob"}))
        self.graph.put_edge(Edge("e1", source="a", target="b", type="KNOWS", properties={"since": 2020}))

        self.assertEqual(self.graph.neighbors("a", "out"), ["b"])
        self.assertEqual([node.get_id for node in self.graph.find_nodes(labels=["Person"])], ["a", "b"])
        self.assertEqual([edge.get_id for edge in self.graph.find_edges(type="KNOWS")], ["e1"])

        self.graph.close()
        self.graph = GraphDB(LMDBStore(path=self.test_dir), MessagePackSerializer())

        self.assertEqual(self.graph.neighbors("a", "out"), ["b"])
        self.assertEqual([node.get_id for node in self.graph.find_nodes(properties={"name": "Alice"})], ["a"])
        self.assertEqual([edge.get_id for edge in self.graph.find_edges(properties={"since": 2020})], ["e1"])

    def test_lmdb_edge_upsert_and_delete_update_persisted_adjacency(self):
        for node_id in ["a", "b", "c"]:
            self.graph.put_node(Node(node_id))

        self.graph.put_edge(Edge("e1", source="a", target="b", type="FIRST"))
        self.graph.put_edge(Edge("e1", source="a", target="c", type="SECOND"))

        self.assertEqual(self.graph.neighbors("a", "out"), ["c"])
        self.assertEqual(self.graph.neighbors("b", "in"), [])
        self.assertEqual(self.graph.neighbors("c", "in"), ["a"])
        self.assertEqual(self.graph.find_edges(type="FIRST"), [])
        self.assertEqual([edge.get_id for edge in self.graph.find_edges(type="SECOND")], ["e1"])

        self.graph.delete_edge("e1")

        self.assertEqual(self.graph.neighbors("a", "out"), [])
        self.assertEqual(self.graph.neighbors("c", "in"), [])
        self.assertEqual(self.graph.find_edges(type="SECOND"), [])


if __name__ == "__main__":
    unittest.main()
