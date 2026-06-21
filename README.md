
<img src="https://raw.githubusercontent.com/mylonasc/pygraphdb/refs/heads/main/assets/coverage_badge.svg">

# PyGraphDB 

A pure python GraphDB for attributed graphs. 

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
python -m pip install "pygraphdb[all] @ git+https://github.com/mylonasc/pygraphdb.git"
```

Available extras are `lmdb`, `leveldb`, `msgpack`, `protobuf`, `bloom`, and `all`. Optional packages are imported only when the corresponding backend or serializer is used. If one is missing, PyGraphDB raises an error naming the missing package and the install command.

After installation, import modules through the `pygraphdb` package, for example `pygraphdb.graphdb`, `pygraphdb.kvstores`, and `pygraphdb.serializers`.

# Coverage badge

Regenerate the test coverage badge with:

```sh
python scripts/update_coverage_badge.py
```

The script runs `unittest` through `coverage`, computes total coverage for `src/pygraphdb`, and updates `assets/coverage_badge.svg`.

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
