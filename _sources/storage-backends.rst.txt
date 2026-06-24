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

LMDB keeps separate databases for nodes, edges, adjacency, typed adjacency, and
sorted indexes. Increase ``map_size`` when loading large graphs.

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

RocksDB Backend
---------------

Use ``PyRexStore`` for RocksDB through the optional ``pyrex-rocksdb`` package.
This backend uses one physical RocksDB database with prefixed keys and exposes
several RocksDB tuning knobs.

.. code-block:: python

   from pygraphdb.graphdb import GraphDB
   from pygraphdb.kvstores import PyRexStore
   from pygraphdb.serializers import PickleSerializer

   store = PyRexStore(
       path="graph_rocksdb",
       parallelism=4,
       max_background_jobs=4,
       write_buffer_size=64 * 1024 * 1024,
       bloom_bits_per_key=10,
   )
   graph_db = GraphDB(store, PickleSerializer())

``disable_wal=True`` can be useful for bulk-loading experiments, but it weakens
durability and should not be used as a safe default.

When installed with ``pyrex-rocksdb>=0.3.0a0``, ``PyRexStore`` can use PyRex's
native ``write_columnar_batch`` API through ``GraphDB.ingest_nodes_arrow`` and
``GraphDB.ingest_edges_arrow``. The columnar methods currently require
caller-provided serialized ``node_value`` and ``edge_value`` payloads and edge
ingestion is append-only.

Sorted Indexes
--------------

All backends implement a small sorted index interface used by labels,
relationship type catalogs, and explicit exact-match property indexes:

- ``put_index_entry(index_name, key_parts, value)``
- ``put_index_entries_bulk(entries)``
- ``delete_index_entry(index_name, key_parts, value)``
- ``iter_index_prefix(index_name, key_parts)``

These indexes are prefix-scanned by the backend rather than by deserializing all
nodes or edges. Current high-level indexes include:

- ``node_label`` for ``Node.labels`` and ``GraphDB.nodes_by_label``.
- ``node_property`` for explicitly registered node properties.
- ``edge_type`` for ``edge.properties["type"]`` and ``GraphDB.edges_by_type``.
- ``edge_property`` for explicitly registered edge properties.

Property indexes are intentionally explicit. Register them only for predicates
you expect to use frequently:

.. code-block:: python

   graph_db.create_node_property_index("name")
   graph_db.create_edge_property_index("score")

Backend Selection Pattern
-------------------------

.. code-block:: python

   from pathlib import Path

   from pygraphdb.graphdb import GraphDB
   from pygraphdb.kvstores import LMDBStore, LevelDBStore, PyRexStore
   from pygraphdb.serializers import PickleSerializer

   def open_graph(path: str, backend: str = "lmdb") -> GraphDB:
       Path(path).parent.mkdir(parents=True, exist_ok=True)
       if backend == "lmdb":
           store = LMDBStore(path=path, map_size=2**30)
       elif backend == "leveldb":
           store = LevelDBStore(path=path)
       elif backend == "rocksdb":
           store = PyRexStore(path=path)
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
