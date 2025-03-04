# PyGraphDB 

A pure python GraphDB for attributed graphs. 

# Example usage

```python
# 1. Choose a store and serializer
import sys
sys.path.append('../src')
from kvstores import LMDBStore
from graphdb import GraphDB, Node, Edge
from serializers import PickleSerializer
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
