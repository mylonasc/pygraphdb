import unittest

from graphdb import ConstraintError, Edge, Node, NodeNotFoundError, GraphDB
from kvstores import InMemoryKVStore
from serializers import PickleSerializer


class FailingOnceAdjacencyStore(InMemoryKVStore):
    def __init__(self):
        super().__init__()
        self.fail_next_adjacency_write = False

    def put_adjacency(self, node_id: bytes, value: bytes) -> None:
        if self.fail_next_adjacency_write:
            self.fail_next_adjacency_write = False
            raise RuntimeError("injected adjacency failure")
        super().put_adjacency(node_id, value)


class GraphDBTest(unittest.TestCase):
    def setUp(self):
        self.graph = GraphDB(InMemoryKVStore(), PickleSerializer())

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
        graph = GraphDB(store, PickleSerializer())
        graph.put_node(Node("a"))
        graph.put_node(Node("b"))
        store.fail_next_adjacency_write = True

        with self.assertRaises(RuntimeError):
            graph.put_edge(Edge("e1", source="a", target="b"))

        self.assertIsNone(graph.get_edge("e1"))
        self.assertEqual(graph.get_adjacency_list("a", "out"), [])
        self.assertEqual(graph.get_adjacency_list("b", "in"), [])


if __name__ == "__main__":
    unittest.main()
