
<img src="https://raw.githubusercontent.com/mylonasc/pygraphdb/refs/heads/main/assets/coverage_badge.svg">

# PyGraphDB 

A pure python GraphDB for attributed graphs. 

Documentation: https://mylonasc.github.io/pygraphdb/

# Installation

## With uv

From this repository:

```sh
uv sync
```

Install the package from a local checkout into another project:

```sh
uv add /path/to/pygraphdb
```

For editable development installs:

```sh
uv add --editable /path/to/pygraphdb
```

Install directly from the Git repository:

```sh
uv add git+https://github.com/mylonasc/pygraphdb.git
```

Install optional backends or serializers only when you need them:

```sh
uv add "/path/to/pygraphdb[lmdb,msgpack,protobuf]"
uv add "/path/to/pygraphdb[fast-ingest]"
uv add "git+https://github.com/mylonasc/pygraphdb.git#egg=pygraphdb[all]"
```

## With pip

From this repository:

```sh
python -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install .
```

Install the package from a local checkout into another project:

```sh
python -m pip install /path/to/pygraphdb
```

For editable development installs:

```sh
python -m pip install -e /path/to/pygraphdb
```

Install directly from the Git repository:

```sh
python -m pip install git+https://github.com/mylonasc/pygraphdb.git
```

Install optional backends or serializers only when you need them:

```sh
python -m pip install "/path/to/pygraphdb[lmdb,msgpack,protobuf]"
python -m pip install "/path/to/pygraphdb[fast-ingest]"
python -m pip install "pygraphdb[all] @ git+https://github.com/mylonasc/pygraphdb.git"
```

Available extras are `lmdb`, `leveldb`, `rocksdb`, `arrow`, `polars`, `fast-ingest`, `msgpack`, `protobuf`, `bloom`, `docs`, `coverage`, `dev`, and `all`. Optional packages are imported only when the corresponding backend, serializer, or ingestion helper is used. If one is missing, PyGraphDB raises an error naming the missing package and the install command.

After installation, import modules through the `pygraphdb` package, for example `pygraphdb.graphdb`, `pygraphdb.kvstores`, and `pygraphdb.serializers`.

# Coverage badge

Regenerate the test coverage badge with:

```sh
python scripts/update_coverage_badge.py
```

The script runs `pytest` through `coverage`, computes total coverage for `src/pygraphdb`, and updates `assets/coverage_badge.svg`.

# Example usage

```python
# 1. Choose a store and serializer
from pygraphdb.kvstores import LMDBStore
from pygraphdb.graphdb import GraphDB, Node, Edge
from pygraphdb.serializers import PickleSerializer
lmdb_store = LMDBStore(path='graph_lmdb_example')
serializer = PickleSerializer()

# 2. Create the GraphDB 
graph_db = GraphDB(lmdb_store, serializer)

# 3. Create and put a Node
node_a = Node(properties={'name': 'Alice', 'age': 30})
graph_db.put_node(node_a)

# 4. Create and put another Node
node_b = Node(properties={'name': 'Bob', 'age': 25})
graph_db.put_node(node_b)

# 5. Create an Edge between them
edge_ab = Edge(source=node_a.get_id, target=node_b.get_id, properties={'relation': 'friend'})
graph_db.put_edge(edge_ab)

# 6. Retrieve a node
fetched_node_a = graph_db.get_node(node_a.get_id_bytes)
print("Fetched Node A:", fetched_node_a.to_dict())

# 7. Retrieve an edge
fetched_edge_ab = graph_db.get_edge(edge_ab.get_id_bytes)
print("Fetched Edge A->B:", fetched_edge_ab.to_dict())

# 8. Cleanup
graph_db.close()
```

# Labels, indexes, and Cypher

PyGraphDB stores native node labels and maintains sorted indexes for labels, relationship types, and explicitly registered exact-match properties. These indexes are designed to support query execution without scanning and deserializing every node or edge.

```python
from pygraphdb.graphdb import Edge, GraphDB, Node

graph_db.put_node(Node(node_id="drug-1", labels=["Drug"], properties={"name": "Aspirin"}))
graph_db.put_node(Node(node_id="protein-1", labels=["Protein"], properties={"name": "PTGS1"}))
graph_db.put_edge(Edge(
    edge_id="d1-p1",
    source="drug-1",
    target="protein-1",
    properties={"type": "drug-to-protein", "score": 0.9},
))

graph_db.create_node_property_index("name")
graph_db.create_edge_property_index("score")

graph_db.nodes_by_label("Drug")
graph_db.nodes_by_property("name", "Aspirin")
graph_db.edges_by_type("drug-to-protein")
graph_db.edges_by_property("score", 0.9)

result = graph_db.query('MATCH (drug:Drug {name: "Aspirin"}) RETURN drug')
```

# Typed sampled traversal

Store edge types in `edge.properties['type']`. PyGraphDB maintains typed adjacency indexes for these edges, so traversals can scan only the requested edge type instead of loading all incident edges and filtering in Python.

```python
import random

from pygraphdb.graphdb import Edge, GraphDB, Node
from pygraphdb.kvstores import LMDBStore
from pygraphdb.serializers import PickleSerializer

graph_db = GraphDB(LMDBStore(path='typed_graph_lmdb'), PickleSerializer())

graph_db.put_node(Node(node_id='drug-1'))
graph_db.put_node(Node(node_id='protein-1'))
graph_db.put_node(Node(node_id='disease-1'))

graph_db.put_edge(Edge(
    edge_id='drug-1-protein-1',
    source='drug-1',
    target='protein-1',
    properties={'type': 'drug-to-protein'},
))
graph_db.put_edge(Edge(
    edge_id='protein-1-disease-1',
    source='protein-1',
    target='disease-1',
    properties={'type': 'protein-to-disease'},
))

paths = graph_db.sample_typed_paths(
    seed_ids=['drug-1'],
    pattern=[
        {'edge_type': 'drug-to-protein', 'direction': 'out', 'sample_size': 10},
        {'edge_type': 'protein-to-disease', 'direction': 'out', 'sample_size': 10},
    ],
    rng=random.Random(7),
)

subgraph = graph_db.sample_typed_subgraph(
    seed_ids=['drug-1'],
    pattern=[
        {'edge_type': 'drug-to-protein', 'direction': 'out', 'sample_size': 10},
        {'edge_type': 'protein-to-disease', 'direction': 'out', 'sample_size': 10},
    ],
)

graph_db.close()
```

Useful typed traversal methods:

```python
graph_db.neighbors_by_edge_type('drug-1', 'drug-to-protein', direction='out')
graph_db.edges_by_edge_type('drug-1', 'drug-to-protein', direction='out')
graph_db.sample_neighbors('drug-1', 'drug-to-protein', direction='out', sample_size=10)
graph_db.sample_typed_paths(seed_ids, pattern)
graph_db.sample_typed_subgraph(seed_ids, pattern)
graph_db.rebuild_typed_adjacency()
```

# Columnar ingestion

Version `0.2.0a0` adds serialized Arrow/Polars-style columnar ingestion for attributed nodes and typed edges. The first implementation requires caller-provided serialized `node_value` and `edge_value` payloads so `get_node` and `get_edge` continue to use the configured serializer without a migration step.

With `PyRexStore` and `pyrex-rocksdb>=0.3.0a0`, these APIs use PyRex's native `write_columnar_batch` method when available. LMDB, LevelDB, and older PyRex runtimes use the existing Python bulk-write fallback.

```python
from pygraphdb.graphdb import Edge, GraphDB, Node
from pygraphdb.kvstores import PyRexStore
from pygraphdb.serializers import PickleSerializer

graph_db = GraphDB(PyRexStore(path="graph_rocksdb"), PickleSerializer())

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

graph_db.neighbors_by_edge_type("drug-1", "drug-to-protein")
graph_db.close()
```

Polars users can call `ingest_nodes_polars` and `ingest_edges_polars` with binary `node_value` and `edge_value` columns. See `notebooks/05_columnar_ingestion_benchmark.ipynb` for a runnable comparison against LevelDB object-batch ingestion.
