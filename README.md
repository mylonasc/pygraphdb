
<img src="https://raw.githubusercontent.com/mylonasc/pygraphdb/refs/heads/main/assets/coverage_badge.svg">

# PyGraphDB 

A pure python GraphDB for attributed graphs. 

# Development

```sh
uv sync
uv sync --extra lmdb      # optional LMDB backend
uv sync --extra leveldb   # optional LevelDB backend
uv build
```

# Example usage

```python
# 1. Choose a store and serializer
from kvstores import InMemoryKVStore
from graphdb import GraphDB, Node, Edge
from serializers import PickleSerializer

store = InMemoryKVStore()
serializer = PickleSerializer()

# 2. Create the GraphDB 
graph_db = GraphDB(store, serializer)

# 3. Create and put a Node
node_a = Node('alice', labels=['Person'], properties={'name': 'Alice', 'age': 30})
graph_db.put_node(node_a)

# 4. Create and put another Node
node_b = Node('bob', labels=['Person'], properties={'name': 'Bob', 'age': 25})
graph_db.put_node(node_b)

# 5. Create an Edge between them
edge_ab = Edge('alice-knows-bob', source=node_a.get_id, target=node_b.get_id, type='KNOWS')
graph_db.put_edge(edge_ab)

# 6. Retrieve a node
fetched_node_a = graph_db.get_node('alice')
print("Fetched Node A:", fetched_node_a.to_dict())

# 7. Property graph lookups and traversal
print(graph_db.find_nodes(labels=['Person']))
print(graph_db.find_edges(type='KNOWS', source='alice'))
print(graph_db.neighbors('alice', direction='out'))

# 8. Cleanup
graph_db.close()
```
