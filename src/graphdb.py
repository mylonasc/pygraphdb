
import pickle
import json
import os
import uuid
from kvstores import KVStore
from serializers import Serializer

# =========================================
#  Node and Edge Models
# =========================================

class Node:
    def __init__(self, node_id=None, properties=None):
        """If no node_id is provided, generate a UUID."""
        self._id = node_id or str(uuid.uuid4())
        self.properties = properties or {}

    @property
    def get_id(self):
        """Unique identifier for this node."""
        return self._id
    
    def to_dict(self):
        """Convert to a dictionary form for serialization."""
        return {
            'id': self._id,
            'properties': self.properties
        }

    @classmethod
    def from_dict(cls, data: dict):
        """Factory from dictionary."""
        return cls(node_id=data['id'], properties=data['properties'])


class Edge:
    def __init__(self, edge_id=None, source=None, target=None, properties=None):
        """If no edge_id is provided, generate a UUID."""
        self._id = edge_id or str(uuid.uuid4())
        self.source = source  # node_id or Node instance
        self.target = target  # node_id or Node instance
        self.properties = properties or {}
        
    @property
    def get_id(self):
        """Unique identifier for this edge."""
        return self._id
    
    def to_dict(self):
        """Convert to a dictionary for serialization."""
        return {
            'id': self._id,
            'source': self.source if isinstance(self.source, str) else self.source.get_id,
            'target': self.target if isinstance(self.target, str) else self.target.get_id,
            'properties': self.properties
        }
    
    @classmethod
    def from_dict(cls, data: dict):
        """Factory from dictionary."""
        return cls(edge_id=data['id'],
                   source=data['source'],
                   target=data['target'],
                   properties=data['properties'])

class GraphDB:
    """High-level interface to manage Node/Edge storing, retrieval, and indexing."""
    def __init__(self, store: KVStore, serializer: Serializer):
        """
        :param store: An instance of KVStore (e.g. LMDBStore or LevelDBStore),
                      which implements specialized node/edge methods
        :param serializer: A serializer instance (PickleSerializer, JSONSerializer, etc.)
        """
        self.store = store
        self.serializer = serializer

    # -----------
    # Node Methods
    # -----------
    def put_node(self, node: Node):
        node_dict = node.to_dict()
        value = self.serializer.serialize(node_dict)
        self.store.put_node(node.get_id, value)

    def get_node(self, node_id) -> Node:
        data = self.store.get_node(node_id)
        if data:
            node_dict = self.serializer.deserialize(data)
            return Node.from_dict(node_dict)
        else:
            return None

    def delete_node(self, node_id):
        self.store.delete_node(node_id)

    # -----------
    # Edge Methods
    # -----------
    def put_edge(self, edge: Edge):
        edge_dict = edge.to_dict()
        value = self.serializer.serialize(edge_dict)
        self.store.put_edge(edge.get_id, value)

    def get_edge(self, edge_id) -> Edge:
        data = self.store.get_edge(edge_id)
        if data:
            edge_dict = self.serializer.deserialize(data)
            return Edge.from_dict(edge_dict)
        else:
            return None

    def delete_edge(self, edge_id):
        self.store.delete_edge(edge_id)

    # -----------
    # Example Range Query
    #  (Implementation depends on how you store indexes in the KVStore)
    # -----------
    def range_query_nodes(self, property_name: str, start_val, end_val):
        """Example stub: You might rely on the underlying store to handle indexing for nodes."""
        start_key = f"IDX:N:{property_name}:{start_val}:".encode('utf-8')
        end_key = f"IDX:N:{property_name}:{end_val}:\xff".encode('utf-8')
        for k, v in self.store.range_iter(start_key, end_key):
            # parse node id from k, retrieve node
            parts = k.decode('utf-8').split(':')
            node_id = parts[-1]
            yield self.get_node(node_id)

    # --------------------------------
    # Merge-based update (single node)
    # --------------------------------
    def update_node(self, node_id: str, new_data: dict, merge_func) -> Node:
        """
        Fetch existing node (if any). If none found, treat as new or handle gracefully.
        merge_func(old_node_dict, new_data_dict) -> merged_properties (dict)
        """
        old_node = self.get_node(node_id)
        if old_node is None:
            # Decide if we create a new node or return None
            old_node = Node(node_id=node_id, properties={})
        # merged_properties is a dict that results from the user’s custom logic
        merged_props = merge_func(old_node.properties, new_data)
        old_node.properties = merged_props
        self.put_node(old_node)
        return old_node

    def update_edge(self, edge_id: str, new_data: dict, merge_func) -> Edge:
        """
        Similar approach for edges. The new_data might include new properties,
        or you might also allow changing source/target if that makes sense.
        """
        old_edge = self.get_edge(edge_id)
        if old_edge is None:
            # treat as new or handle differently
            old_edge = Edge(edge_id=edge_id, source=None, target=None, properties={})
        merged_props = merge_func(old_edge.properties, new_data)
        old_edge.properties = merged_props
        self.put_edge(old_edge)
        return old_edge

    # --------------------------------
    # Bulk put of nodes
    # --------------------------------
    def put_nodes(self, nodes: list[Node]):
        """
        Convert each Node to bytes using the serializer, then call store.put_nodes_bulk(...)
        """
        to_store = {}
        for n in nodes:
            to_store[n.get_id] = self.serializer.serialize(n.to_dict())
        self.store.put_nodes_bulk(to_store)

    def get_nodes(self, node_ids: list[str]) -> list[Node]:
        """
        Use store.get_nodes_bulk(...) and deserialize each one.
        Return a list of Node (in the same order as node_ids, or possibly just all found).
        """
        results = []
        raw_dict = self.store.get_nodes_bulk(node_ids)  # dict[node_id, bytes]
        for node_id in node_ids:
            raw_data = raw_dict.get(node_id)
            if raw_data is not None:
                node_dict = self.serializer.deserialize(raw_data)
                results.append(Node.from_dict(node_dict))
            else:
                results.append(None)  # or skip it
        return results

    def close(self):
        self.store.close()
        
