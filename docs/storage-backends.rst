Storage Backends
================

PyGraphDB separates graph logic from storage. ``GraphDB`` receives a key-value
store instance and a serializer instance.

LMDB Backend
------------

Use ``LMDBStore`` for a mature embedded backend with named sub-databases.

.. code-block:: python

   from pygraphdb.graphdb import GraphDB
   from pygraphdb.kvstores import LMDBStore
   from pygraphdb.serializers import PickleSerializer

   store = LMDBStore(path="graph_lmdb", map_size=2**30)
   graph_db = GraphDB(store, PickleSerializer())

LMDB keeps separate databases for nodes, edges, adjacency, and typed adjacency.
Increase ``map_size`` when loading large graphs.

LevelDB Backend
---------------

Use ``LevelDBStore`` when you want LevelDB through ``plyvel``.

.. code-block:: python

   from pygraphdb.graphdb import GraphDB
   from pygraphdb.kvstores import LevelDBStore
   from pygraphdb.serializers import PickleSerializer

   store = LevelDBStore(path="graph_leveldb")
   graph_db = GraphDB(store, PickleSerializer())

``plyvel`` requires compatible CPython wheels or local LevelDB build tooling. If
installation fails on Python 3.14 or a free-threaded interpreter, create a Python
3.12 environment and install ``pygraphdb[leveldb]`` there.

Backend Selection Pattern
-------------------------

.. code-block:: python

   from pathlib import Path

   from pygraphdb.graphdb import GraphDB
   from pygraphdb.kvstores import LMDBStore, LevelDBStore
   from pygraphdb.serializers import PickleSerializer

   def open_graph(path: str, backend: str = "lmdb") -> GraphDB:
       Path(path).parent.mkdir(parents=True, exist_ok=True)
       if backend == "lmdb":
           store = LMDBStore(path=path, map_size=2**30)
       elif backend == "leveldb":
           store = LevelDBStore(path=path)
       else:
           raise ValueError(f"unknown backend: {backend}")
       return GraphDB(store, PickleSerializer())

Cleanup
-------

Always close stores when a script or notebook cell is finished with them.

.. code-block:: python

   graph_db = GraphDB(LMDBStore(path="example_lmdb"), PickleSerializer())
   try:
       graph_db.put_node(Node(node_id="n1"))
   finally:
       graph_db.close()
