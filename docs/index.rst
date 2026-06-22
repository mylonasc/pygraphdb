PyGraphDB Documentation
=======================

PyGraphDB is a pure Python graph database toolkit for attributed graphs. It
stores nodes, edges, adjacency lists, and typed adjacency indexes on key-value
backends such as LMDB and LevelDB.

The documentation focuses on practical examples: creating graphs, choosing a
backend, serializing properties, traversing typed edges, and sampling subgraphs.

.. toctree::
   :maxdepth: 2
   :caption: User Guide

   installation
   quickstart
   storage-backends
   serializers
   typed-sampling
   performance
   notebooks

.. toctree::
   :maxdepth: 2
   :caption: API Reference

   api

Quick Example
-------------

.. code-block:: python

   from pygraphdb.graphdb import Edge, GraphDB, Node
   from pygraphdb.kvstores import LMDBStore
   from pygraphdb.serializers import PickleSerializer

   graph_db = GraphDB(LMDBStore(path="graph_lmdb_example"), PickleSerializer())

   alice = Node(node_id="alice", properties={"kind": "person"})
   bob = Node(node_id="bob", properties={"kind": "person"})
   graph_db.put_node(alice)
   graph_db.put_node(bob)

   graph_db.put_edge(Edge(
       edge_id="alice-knows-bob",
       source="alice",
       target="bob",
       properties={"type": "knows", "since": 2024},
   ))

   print(graph_db.get_node(b"alice").properties)
   print(graph_db.neighbors_by_edge_type("alice", "knows", direction="out"))

   graph_db.close()
