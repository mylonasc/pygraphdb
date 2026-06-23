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

Columnar Ingestion
------------------

``ingest_nodes_arrow`` and ``ingest_edges_arrow`` accept Arrow-like columns or
plain Python sequences. The first implementation requires caller-provided
serialized ``node_value`` and ``edge_value`` payloads so existing serializer
behavior remains unchanged.

.. code-block:: python

   nodes = [
       Node(node_id="drug-1", properties={"kind": "drug"}),
       Node(node_id="protein-1", properties={"kind": "protein"}),
   ]
   graph_db.ingest_nodes_arrow(
       [node.get_id for node in nodes],
       [graph_db.serialize_node_value(node) for node in nodes],
   )

   edge = Edge(
       edge_id="d1-p1",
       source="drug-1",
       target="protein-1",
       properties={"type": "drug-to-protein", "score": 0.9},
   )
   graph_db.ingest_edges_arrow(
       [edge.get_id],
       [edge.source],
       [edge.target],
       [edge.get_type],
       [graph_db.serialize_edge_value(edge)],
       append_only=True,
   )

Polars users can use ``ingest_nodes_polars`` and ``ingest_edges_polars`` with
``node_value`` and ``edge_value`` binary columns. With ``PyRexStore`` and
``pyrex-rocksdb>=0.3.0a0``, these methods use native RocksDB columnar batch
writes when available. Other stores use the Python bulk fallback.

Traverse With BFS
-----------------

.. code-block:: python

   visited = graph_db.bfs(b"user-0", direction="any")
   print(visited)

Query With Cypher
-----------------

``GraphDB.query`` supports an initial read-only Cypher subset for anchored typed
traversal and typed path sampling.

.. code-block:: python

   result = graph_db.query(
       'MATCH (drug {id: "drug-1"})-[:drug-to-protein]->(protein) RETURN drug, protein'
   )

   for record in result:
       print(record["drug"].get_id, record["protein"].get_id)

Multi-hop typed traversal is supported when each hop is outgoing and has an edge
type:

.. code-block:: python

   result = graph_db.query(
       'MATCH (drug {id: "drug-1"})-[:drug-to-protein]->(protein)-[:protein-to-disease]->(disease) RETURN drug, protein, disease'
   )

See :doc:`cypher` for the full supported subset and current limitations.

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
