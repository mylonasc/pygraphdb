Quickstart
==========

Create a Graph
--------------

.. code-block:: python

   from pygraphdb.graphdb import Edge, GraphDB, Node
   from pygraphdb.kvstores import LMDBStore
   from pygraphdb.serializers import PickleSerializer

   graph_db = GraphDB(LMDBStore(path="quickstart_lmdb"), PickleSerializer())

   alice = Node(node_id="alice", properties={"name": "Alice", "age": 30})
   bob = Node(node_id="bob", properties={"name": "Bob", "age": 25})

   graph_db.put_node(alice)
   graph_db.put_node(bob)

   edge = Edge(
       edge_id="alice-bob",
       source=alice.get_id,
       target=bob.get_id,
       properties={"type": "friend", "weight": 0.9},
   )
   graph_db.put_edge(edge)

   print(graph_db.get_node(b"alice").to_dict())
   print(graph_db.get_edge(b"alice-bob").to_dict())

   graph_db.close()

Bulk Insert Edges
-----------------

``put_edges_bulk`` stores many edge records and updates adjacency indexes in one
operation.

.. code-block:: python

   nodes = [Node(node_id=f"user-{idx}") for idx in range(4)]
   for node in nodes:
       graph_db.put_node(node)

   edges = [
       Edge(edge_id="u0-u1", source="user-0", target="user-1", properties={"type": "follows"}),
       Edge(edge_id="u0-u2", source="user-0", target="user-2", properties={"type": "follows"}),
       Edge(edge_id="u2-u3", source="user-2", target="user-3", properties={"type": "follows"}),
   ]
   graph_db.put_edges_bulk(edges)

For append-only ingestion where edge IDs are known to be new, skip replacement
checks to avoid one existing-edge read per edge:

.. code-block:: python

   graph_db.put_edges_bulk(edges, check_existing=False)

Fetch Nodes in Bulk
-------------------

.. code-block:: python

   fetched = graph_db.get_nodes([b"user-0", b"user-1", b"missing"])
   for node in fetched:
       print(None if node is None else node.get_id)

Traverse With BFS
-----------------

.. code-block:: python

   visited = graph_db.bfs(b"user-0", direction="any")
   print(visited)

Use Stable IDs
--------------

Stable IDs make notebooks, tests, and serialized records easier to inspect.

.. code-block:: python

   drug = Node(node_id="drug-1", properties={"kind": "drug", "name": "Aspirin"})
   protein = Node(node_id="protein-1", properties={"kind": "protein"})
   edge = Edge(
       edge_id="drug-1-protein-1",
       source=drug.get_id,
       target=protein.get_id,
       properties={"type": "drug-to-protein"},
   )
