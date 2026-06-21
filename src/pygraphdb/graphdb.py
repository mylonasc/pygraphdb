
from __future__ import annotations

import pickle
import json
import os
import random
import uuid
from typing import TYPE_CHECKING, List, Optional, Union

if TYPE_CHECKING:
    from .kvstores import KVStore
    from .serializers import Serializer

import datetime
import struct

def datetime_to_bytes(dt: datetime.datetime, tzinfo = datetime.timezone.utc) -> bytes:
    """Convert a datetime >= 1970-01-01 to big-endian 64-bit microseconds since epoch."""
    # Make sure `dt` is in UTC, or at least consistently handled.
    # (If dt has no tzinfo, Python treats it as local time for .timestamp().)
    epoch = datetime.datetime(1970, 1, 1, tzinfo=tzinfo)
    delta = dt - epoch
    # Convert to integer microseconds
    microseconds = int(delta.total_seconds() * 1_000_000)
    # Pack as an unsigned 64-bit integer in big-endian order
    return struct.pack('>Q', microseconds)

def bytes_to_datetime(b: bytes, tzinfo = datetime.timezone.utc) -> datetime.datetime:
    """Inverse of datetime_to_bytes for datetimes >= 1970-01-01."""
    epoch = datetime.datetime(1970, 1, 1, tzinfo=tzinfo)
    (microseconds,) = struct.unpack('>Q', b)
    return epoch + datetime.timedelta(microseconds=microseconds)


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
    
    @property
    def get_id_bytes(self):
        return self._id.encode('utf-8')

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
    
    @property
    def get_id_bytes(self):
        return self._id.encode('utf-8')

    @property
    def get_type(self):
        return self.properties.get('type')

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

class TimeIndexedEdge(Edge):
    def __init__(self, timestamp_dat, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.timestamp_dat = timestamp_dat
        self.id_string = self.get_id

    @property
    def get_id_bytes(self):
        b2 = self.id_string.encode('utf-8')
        sep = b':'
        b1 = datetime_to_bytes(self.timestamp_dat)
        return b1 + sep + b2

    def to_dict(self):
        """Convert to a dictionary for serialization."""
        return {
            'timestamp_dat' : self.timestamp_dat,
            'id': self._id,
            'source': self.source if isinstance(self.source, str) else self.source.get_id,
            'target': self.target if isinstance(self.target, str) else self.target.get_id,
            'properties': self.properties
        }
    
    @classmethod
    def from_dict(cls, data: dict):
        """Factory from dictionary."""
        return cls(timestamp_dat = data['timestamp_dat'],
                   edge_id=data['id'],
                   source=data['source'],
                   target=data['target'],
                   properties=data['properties'])

class GraphEntityDictSerializer:
    _ent_type_encoder = {
        'Edge' : lambda x : x.to_dict(),
        'Node' : lambda x : x.to_dict(),
        'AdjacencyList' : lambda x : x
    }

    _ent_type_decoder = {
        'Edge' : lambda x : Edge.from_dict(x),
        'Node' : lambda x : Node.from_dict(x),
        'AdjacencyList' : lambda x : x
    }
    
    def __init__(self, serializer : Serializer):
        self.serializer = serializer
    
    def serialize(self, entity, entity_type : str):
        enc_obj = self._ent_type_encoder[entity_type](entity)
        return self.serializer.serialize(enc_obj)
    
    def deserialize(self, val, entity_type : str):
        """ Deserializer (conditional on entity type)

        Args:
            val: bytes containing the data
            entity_type : (str) is Edge, Node, AdjacencyList
        """
        deser_val = self.serializer.deserialize(val)
        return self._ent_type_decoder[entity_type](deser_val)


class GraphDB:
    """High-level interface to manage Node/Edge storing, retrieval, and indexing."""
    def __init__(
            self, 
            store: KVStore, 
            serializer: Serializer
        ):
        """
        :param store: An instance of KVStore (e.g. LMDBStore or LevelDBStore),
                      which implements specialized node/edge methods
        :param serializer: A serializer instance (PickleSerializer, JSONSerializer, etc.)
        """
        self.store = store
        self.serializer = serializer
        self.entity_serializer = GraphEntityDictSerializer(
            self.serializer
        )

    # -----------
    # Node Methods
    # -----------
    def put_node(self, node: Node):
        value = self.entity_serializer.serialize(node, 'Node')
        self.store.put_node(node.get_id_bytes, value)

    def get_node(self, node_id) -> Node:
        data = self.store.get_node(node_id)
        if data:
            # node_dict = self.serializer.deserialize(data)
            # return Node.from_dict(node_dict)
            return self.entity_serializer.deserialize(data,'Node')
        else:
            return None

    def delete_node(self, node_id):
        self.store.delete_node(node_id)

    def node_key_to_bytes(self, node_key):
        if isinstance(node_key, bytes):
            return node_key
        return node_key.encode('utf-8')

    def edge_key_to_bytes(self, edge_key):
        if isinstance(edge_key, bytes):
            return edge_key
        return edge_key.encode('utf-8')

    def key_to_string(self, key):
        if isinstance(key, bytes):
            return key.decode('utf-8')
        return key

    def edge_type(self, edge: Edge):
        return edge.get_type

    # -----------
    # Edge Methods
    # -----------
    def put_edge(self, edge: Edge, update_adjacency = True):
        # edge_dict = edge.to_dict()
        old_edge = self.get_edge(edge.get_id_bytes)
        if old_edge is not None:
            self._delete_typed_adjacency_for_edge(old_edge)
        value = self.entity_serializer.serialize(edge,'Edge')
        self.store.put_edge(edge.get_id_bytes, value)
        self._put_typed_adjacency_for_edge(edge)
        if update_adjacency:
            # Get the edge lists from the adjacency store, 
            # and if they exist update them, if they don't exist 
            # create them.
            for dict_flag, io_node in zip(['source','target'], (edge.source, edge.target)): 
                io_node_key = self.node_key_to_bytes(io_node)
                adj_list = self.store.get_adjacency(io_node_key)
                new_edge_list = {dict_flag : [edge.get_id]}
                if adj_list is None:
                    serialized_adj_list = self.entity_serializer.serialize(new_edge_list,'AdjacencyList')
                    self.store.put_adjacency(io_node_key, serialized_adj_list)
                else:
                    adj_edge_list = self.entity_serializer.deserialize(adj_list,'AdjacencyList')
                    adj_edge_list.setdefault(dict_flag, []).append(edge.get_id)
                    self.store.put_adjacency(io_node_key, self.serializer.serialize(adj_edge_list))

    def _put_typed_adjacency_for_edge(self, edge: Edge):
        edge_type = self.edge_type(edge)
        if edge_type is None:
            return
        self.store.put_typed_adjacency(
            self.node_key_to_bytes(edge.source),
            self.node_key_to_bytes(edge.target),
            edge_type,
            edge.get_id_bytes,
        )

    def _delete_typed_adjacency_for_edge(self, edge: Edge):
        edge_type = self.edge_type(edge)
        if edge_type is None:
            return
        self.store.delete_typed_adjacency(
            self.node_key_to_bytes(edge.source),
            self.node_key_to_bytes(edge.target),
            edge_type,
            edge.get_id_bytes,
        )

    def get_typed_adjacency(self, node_id, edge_type: str, direction: str = 'out'):
        """Return typed adjacency records with clean direction semantics.

        `out` means source -> target, `in` means target -> source, and `any`
        returns the union of both directions.
        """
        if direction not in {'out', 'in', 'any'}:
            raise ValueError("direction must be 'out', 'in', or 'any'")

        node_id_bytes = self.node_key_to_bytes(node_id)
        directions = ['out', 'in'] if direction == 'any' else [direction]
        records = []
        for current_direction in directions:
            for edge_id, neighbor_id in self.store.iter_typed_adjacency(node_id_bytes, edge_type, current_direction):
                if current_direction == 'out':
                    source_id = node_id_bytes
                    target_id = neighbor_id
                else:
                    source_id = neighbor_id
                    target_id = node_id_bytes
                records.append({
                    'edge_id': edge_id,
                    'neighbor_id': neighbor_id,
                    'source_id': source_id,
                    'target_id': target_id,
                    'edge_type': edge_type,
                    'direction': current_direction,
                })
        return records

    def neighbors_by_edge_type(self, node_id, edge_type: str, direction: str = 'out'):
        return [record['neighbor_id'] for record in self.get_typed_adjacency(node_id, edge_type, direction)]

    def edges_by_edge_type(self, node_id, edge_type: str, direction: str = 'out'):
        return [record['edge_id'] for record in self.get_typed_adjacency(node_id, edge_type, direction)]

    def sample_neighbors(self, node_id, edge_type: str, direction: str = 'out', sample_size: int = 10, rng=None):
        rng = rng or random
        sample = []
        seen = 0
        for record in self.get_typed_adjacency(node_id, edge_type, direction):
            seen += 1
            if len(sample) < sample_size:
                sample.append(record)
                continue
            replacement_idx = rng.randrange(seen)
            if replacement_idx < sample_size:
                sample[replacement_idx] = record
        return sample

    def sample_typed_paths(self, seed_ids, pattern: list[dict], rng=None):
        rng = rng or random
        paths = []

        for seed_id in seed_ids:
            seed_id_bytes = self.node_key_to_bytes(seed_id)
            frontier = [{'seed': seed_id_bytes, 'path': [], 'current_node_id': seed_id_bytes}]
            for hop in pattern:
                next_frontier = []
                edge_type = hop['edge_type']
                direction = hop.get('direction', 'out')
                sample_size = hop.get('sample_size', 10)
                for partial in frontier:
                    sampled_records = self.sample_neighbors(
                        partial['current_node_id'],
                        edge_type,
                        direction=direction,
                        sample_size=sample_size,
                        rng=rng,
                    )
                    for record in sampled_records:
                        next_frontier.append({
                            'seed': partial['seed'],
                            'path': partial['path'] + [record],
                            'current_node_id': record['neighbor_id'],
                        })
                frontier = next_frontier
                if not frontier:
                    break
            for partial in frontier:
                paths.append({'seed': partial['seed'], 'path': partial['path']})
        return paths

    def sample_typed_subgraph(self, seed_ids, pattern: list[dict], rng=None):
        paths = self.sample_typed_paths(seed_ids, pattern, rng=rng)
        node_ids = {self.node_key_to_bytes(seed_id) for seed_id in seed_ids}
        edge_ids = set()
        for sampled_path in paths:
            node_ids.add(sampled_path['seed'])
            for hop in sampled_path['path']:
                node_ids.add(hop['source_id'])
                node_ids.add(hop['target_id'])
                edge_ids.add(hop['edge_id'])

        return {
            'nodes': {node_id: self.get_node(node_id) for node_id in node_ids},
            'edges': {edge_id: self.get_edge(edge_id) for edge_id in edge_ids},
            'paths': paths,
        }

    def rebuild_typed_adjacency(self):
        rebuilt = 0
        for edge_id in self.store.get_edge_keys_generator():
            edge = self.get_edge(edge_id)
            if edge is None or self.edge_type(edge) is None:
                continue
            self._put_typed_adjacency_for_edge(edge)
            rebuilt += 1
        return rebuilt

    def get_edge(self, edge_id) -> Edge:
        data = self.store.get_edge(edge_id)
        if data:
            return self.entity_serializer.deserialize(data,'Edge')
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
            to_store[n.get_id_bytes] = self.entity_serializer.serialize(n, 'Node')
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
    def get_node_keys_generator(self, num_nodes = None, key_offset = None):
        return self.store.get_node_keys_generator(num_nodes, key_offset)

    # -----------------------
    # Adjacency Management
    # -----------------------
    
    # def _append_edge_to_adjacency(self, node_id: str, edge_id: str):
    #     """
    #     Internal helper: read the adjacency list for node_id,
    #     append edge_id if not present, and write it back.
    #     """
    #     edges_list = self.get_adjacency_list(node_id)
    #     if edge_id not in edges_list:
    #         edges_list.append(edge_id)
    #         self.put_adjacency_list(node_id, edges_list)

    def get_adjacency_list(self, node_id: bytes,direction = 'forward', return_raw = False) -> list[str]:
        """
        Returns the list of edge IDs connected to node_id.
        If none found, returns an empty list.
        
        Args:
          node_id: a string representing the node_id
          direction : 'forward', 'backward' or 'any' -> controls whether the source, target, or un-directed adjacency of the node will be returned. 
          return_raw : if this flag is true it will return the data as they are stored (e.g., a dictionary of 'source' and 'target' lists. )
        """
        
        raw = self.store.get_adjacency(node_id)
        if raw is None:
            return []
        adj = self.serializer.deserialize(raw)
        
        if return_raw:
            return adj

        if direction == 'forward':
            return adj.get('target',[]) 
        if direction == 'backward':
            return adj.get('source',[])
        if direction == 'any' : 
            _s = adj.get('source',[])
            _t = adj.get('target',[])
            return list(set(_s).union(set(_t)))
        
    def put_adjacency_list(self, node_id: str, edges_list: list[str]):
        """Stores the adjacency list for node_id."""
        raw = self.serializer.serialize(edges_list)
        self.store.put_adjacency(node_id, raw)

    # -----------------------
    # Deletion (Optional)
    # -----------------------
    def delete_edge(self, edge_id: str, edge_key_serializer= lambda x:  x.encode('utf-8')):
        """
        Removes the edge from the edge store, and from adjacency
        of both source and target nodes. If either node doesn't exist,
        we skip gracefully.
        """
        e = self.get_edge(edge_id)
        if not e:
            return  # Edge not found

        self._delete_typed_adjacency_for_edge(e)

        # Remove edge_id from adjacency of source
        _source_edge_bytes = edge_key_serializer(e.source)
        self._remove_edge_from_adjacency(_source_edge_bytes, edge_id)
        # Remove edge_id from adjacency of target
        if e.target != e.source:
            _target_edge_bytes = edge_key_serializer(e.target)
            self._remove_edge_from_adjacency(_target_edge_bytes, edge_id)

        # If your store had a 'delete_edge' method, you'd call it here.
        # We'll assume you add that to KVStore if needed, e.g.:
        self.store.delete_edge(edge_id)

    def _remove_edge_from_adjacency(self, node_id: str, edge_id: str):

        
        adj_list = self.get_adjacency_list(node_id,return_raw = True)
        _changed = False
        for _dir in ['source', 'target']:
            if _dir in adj_list:
                if edge_id in adj_list[_dir]:
                    adj_list[_dir].remove(edge_id)
                    _changed = True
        if _changed:
            self.put_adjacency_list(node_id, adj_list)

    # -----------------------
    # BFS Example
    # -----------------------
    def bfs(self, start_node_id: bytes, direction = 'any', edge_key_serializer = lambda x : x.encode('utf-8'), node_key_serializer = lambda x : x.encode('utf-8')) -> list[str]:
        """
        Returns a list of node_ids in BFS order starting from `start_node_id`.
        Demonstrates how adjacency is used for graph traversal.
        """
        visited = set()
        queue = [start_node_id]
        result = []
        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)
            result.append(current)

            # 1) get adjacency list for current
            edges_list = self.get_adjacency_list(current, direction = direction)
            
            # 2) for each edge in adjacency, find the other node
            for e_id in edges_list:
                _ser_edge_key = edge_key_serializer(e_id)
                edge_obj = self.get_edge(_ser_edge_key)
                if not edge_obj:
                    continue
                _source_node = node_key_serializer(edge_obj.source)
                neighbor = (
                    node_key_serializer(edge_obj.target) if _source_node == current else _source_node
                )
                if neighbor not in visited:
                    queue.append(neighbor)

        return result

    def put_edges_bulk(self, edges: List[Edge]):
        # 1) Build a dict[edge_id, bytes] to store all edges in one go
        edge_dict = {}
        # 2) Accumulate adjacency changes in memory: node_id -> set(edge_ids)
        adjacency_accumulator = {} # the keys are "nodes" and the values are sets of edges for where the nodes appear as source or destinations (separately). 
        # adjacency_accumulator_target = {}

        for e in edges:
            old_edge = self.get_edge(e.get_id_bytes)
            if old_edge is not None:
                self._delete_typed_adjacency_for_edge(old_edge)
            e_bytes = self.entity_serializer.serialize(e, 'Edge')
            _source = self.node_key_to_bytes(e.source)
            _target = self.node_key_to_bytes(e.target)
            edge_dict[e.get_id_bytes] = e_bytes
            # adjacency accum update
            adjacency_accumulator.setdefault(_source, {'target' : [], 'source' : []})
            adjacency_accumulator[_source]['source'].append(e.get_id_bytes)
            adjacency_accumulator.setdefault(_target, {'target' : [], 'source' : []})
            adjacency_accumulator[_target]['target'].append(e.get_id_bytes)
            
        # 3) Use the store's put_edges_bulk
        self.store.put_edges_bulk(edge_dict)
        for e in edges:
            self._put_typed_adjacency_for_edge(e)


        # 4) Build adjacency dict so we do one read+write per node
        #    node_id -> final adjacency (existing + new edges)
        final_adjacency = {}

        # For each node in adjacency_accumulator, fetch old adjacency,
        # union with new edges, and store in final_adjacency dict
        try:
            for node_id, new_edges_source_target in adjacency_accumulator.items():

                # raw_adj = self.store.get_adjacency(node_key_serializer(node_id))
                raw_adj = self.store.get_adjacency(node_id)
                # raw_adj = self.store.get_adjacency(node_id)
                if raw_adj is None:
                    old_edges = {'source' : set(),'target' : set()}
                else:
                    old_edges = self.serializer.deserialize(raw_adj)
                    if 'target' not in old_edges:
                        old_edges['target'] = set()
                    
                    if 'source' not in old_edges:
                        old_edges['source'] = set()
                source_edges = old_edges['source']
                target_edges = old_edges['target']
                if 'source' in new_edges_source_target:
                    source_edges = set(source_edges).union(new_edges_source_target['source'])
                if 'target' in new_edges_source_target:
                    target_edges = set(target_edges).union(new_edges_source_target['target'])
                # Here we cast to list because some objects do not support set serialization. 
                new_adj_value = {'source' : list(source_edges),'target' : list(target_edges)}
                final_adjacency[node_id] = self.entity_serializer.serialize(new_adj_value,'AdjacencyList')
        except:
            return adjacency_accumulator, source_edges, target_edges

        # 5) One batch write for adjacency
        self.store.put_adjacency_bulk(final_adjacency)

    def close(self):
        self.store.close()
        
