from contextlib import contextmanager
from threading import RLock
from typing import Optional, Dict, List, Union
import os


def _pack_long_int(int_val):
    return struct.pack('<L', int_val)

def _unpack_long_int(int_val):
    return struct.unpack('<L', int_val)[0]


class KVStore:
    """Abstract storage contract used by :class:`graphdb.GraphDB`.

    A backend stores serialized node, edge, and adjacency records under bytes
    keys. GraphDB owns graph-level semantics; backends provide durable or
    in-memory key-value operations and optional secondary-index hooks.

    Implementations should treat missing deletes as no-ops and missing reads as
    ``None``.
    """

    # The basic K/V methods:
    @contextmanager
    def read_transaction(self):
        """Open a backend read transaction.

        Backends that do not support explicit transactions may use this no-op
        implementation. Transaction-aware graph operations call this hook so
        backends can provide snapshot reads where available.
        """
        yield self

    @contextmanager
    def write_transaction(self):
        """Open a backend write transaction.

        Backends that do not support explicit transactions may use this no-op
        implementation. Correct durable backends should override this so graph
        mutations can update nodes, edges, adjacency, and indexes atomically.
        """
        yield self

    def put(self, key: bytes, value: bytes):
        raise NotImplementedError

    def get(self, key: bytes) -> bytes:
        raise NotImplementedError

    def delete(self, key: bytes):
        raise NotImplementedError

    def range_iter(self, start_key: bytes, end_key: bytes):
        """Iterate over keys from start_key to end_key (inclusive)."""
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    # The specialized node/edge methods:
    def put_node(self, node_id: str, value: bytes):
        """Store a node (serialized)."""
        raise NotImplementedError

    def get_node(self, node_id: str) -> bytes:
        """Retrieve a node by ID."""
        raise NotImplementedError

    def delete_node(self, node_id: str):
        """Delete a node."""
        raise NotImplementedError

    def put_edge(self, edge_id: str, value: bytes):
        """Store an edge (serialized)."""
        raise NotImplementedError

    def get_edge(self, edge_id: str) -> bytes:
        """Retrieve an edge."""
        raise NotImplementedError

    def delete_edge(self, edge_id: str):
        """Delete an edge."""
        raise NotImplementedError

    def put_nodes_bulk(self, keys_and_values: dict[str, bytes]):
        """
        Store multiple node (serialized) values in a single batch/transaction if possible.
        """
        raise NotImplementedError

    def get_nodes_bulk(self, node_ids: list[str]) -> dict[str, bytes]:
        raise NotImplementedError

    def put_edges_bulk(self, keys_and_values: dict[str, bytes]):
        raise NotImplementedError
    
    def get_edges_bulk(self, edge_ids: list[str]) -> dict[str, bytes]:
        raise NotImplementedError

    def put_adjacency(self, node_id: bytes, value: bytes) -> None:
        raise NotImplementedError

    def get_adjacency(self, node_id: bytes) -> Optional[bytes]:
        raise NotImplementedError

    def put_adjacency_bulk(self, adj_dict: Dict[bytes, bytes]) -> None:
        raise NotImplementedError

    def get_adjacency_bulk(self, node_ids: List[bytes]) -> Dict[bytes, bytes]:
        raise NotImplementedError

    def get_node_keys_generator(self, num_nodes=None, key_offset=None):
        raise NotImplementedError

    def get_edge_keys_generator(self, num_edges=None, key_offset=None):
        raise NotImplementedError


class InMemoryKVStore(KVStore):
    """Deterministic in-memory backend for tests and small embedded graphs.

    The store keeps separate dictionaries for node records, edge records, and
    adjacency records. It also maintains exact-match indexes for node labels,
    node properties, edge types, and edge properties. These indexes are optional
    hooks consumed by :class:`graphdb.GraphDB`; backends that do not implement
    them still work via scans.

    Example:
        >>> from graphdb import Edge, GraphDB, Node
        >>> from serializers import PickleSerializer
        >>> graph = GraphDB(InMemoryKVStore(), PickleSerializer())
        >>> graph.put_node(Node("a", labels=["Person"]))
        <graphdb.Node object at ...>
        >>> graph.find_nodes(labels=["Person"])[0].get_id
        'a'
    """

    def __init__(self):
        """Create an empty in-memory store."""
        self._lock = RLock()
        self.nodes = {}
        self.edges = {}
        self.adjacency = {}
        self.out_adjacency = {}
        self.in_adjacency = {}
        self.node_labels = {}
        self.node_properties = {}
        self.edge_types = {}
        self.edge_properties = {}

    @contextmanager
    def read_transaction(self):
        """Acquire the store lock for a consistent in-memory read."""
        with self._lock:
            yield self

    @contextmanager
    def write_transaction(self):
        """Acquire the store lock for an atomic in-memory mutation."""
        with self._lock:
            yield self

    def put(self, key: bytes, value: bytes):
        """Store a raw key-value pair in the node namespace.

        Args:
            key: Bytes key.
            value: Serialized value.
        """
        with self._lock:
            self.nodes[key] = value

    def get(self, key: bytes) -> bytes:
        """Fetch a raw key-value pair from the node namespace.

        Args:
            key: Bytes key.

        Returns:
            Serialized value or ``None`` if missing.
        """
        with self._lock:
            return self.nodes.get(key)

    def delete(self, key: bytes):
        """Delete a raw key-value pair from the node namespace.

        Args:
            key: Bytes key to delete.
        """
        with self._lock:
            self.nodes.pop(key, None)

    def range_iter(self, start_key: bytes, end_key: bytes):
        """Iterate raw node-namespace keys in an inclusive byte range.

        Args:
            start_key: Inclusive lower bound.
            end_key: Inclusive upper bound.

        Yields:
            ``(key, value)`` pairs in sorted key order.
        """
        with self._lock:
            items = [(key, self.nodes[key]) for key in sorted(self.nodes) if start_key <= key <= end_key]
        for item in items:
            yield item

    def close(self):
        """Close the store.

        The in-memory backend has no external resources, so this is a no-op.
        """
        pass

    def put_node(self, node_id: bytes, value: bytes):
        """Store a serialized node record.

        Args:
            node_id: Node key.
            value: Serialized node value.
        """
        with self._lock:
            self.nodes[node_id] = value

    def get_node(self, node_id: bytes) -> bytes:
        """Fetch a serialized node record.

        Args:
            node_id: Node key.

        Returns:
            Serialized node value or ``None`` if missing.
        """
        with self._lock:
            return self.nodes.get(node_id)

    def delete_node(self, node_id: bytes):
        """Delete a node record.

        Args:
            node_id: Node key.
        """
        with self._lock:
            self.nodes.pop(node_id, None)

    def put_edge(self, edge_id: bytes, value: bytes):
        """Store a serialized edge record.

        Args:
            edge_id: Edge key.
            value: Serialized edge value.
        """
        with self._lock:
            self.edges[edge_id] = value

    def get_edge(self, edge_id: bytes) -> bytes:
        """Fetch a serialized edge record.

        Args:
            edge_id: Edge key.

        Returns:
            Serialized edge value or ``None`` if missing.
        """
        with self._lock:
            return self.edges.get(edge_id)

    def delete_edge(self, edge_id: bytes):
        """Delete an edge record.

        Args:
            edge_id: Edge key.
        """
        with self._lock:
            self.edges.pop(edge_id, None)

    def put_nodes_bulk(self, keys_and_values: dict[bytes, bytes]):
        """Store multiple serialized node records.

        Args:
            keys_and_values: Mapping of node keys to serialized node values.
        """
        with self._lock:
            self.nodes.update(keys_and_values)

    def get_nodes_bulk(self, node_ids: list[bytes]) -> dict[bytes, bytes]:
        """Fetch multiple serialized node records.

        Args:
            node_ids: Node keys to fetch.

        Returns:
            Mapping for IDs that exist. Missing IDs are omitted.
        """
        with self._lock:
            return {node_id: self.nodes[node_id] for node_id in node_ids if node_id in self.nodes}

    def put_edges_bulk(self, keys_and_values: dict[bytes, bytes]):
        """Store multiple serialized edge records.

        Args:
            keys_and_values: Mapping of edge keys to serialized edge values.
        """
        with self._lock:
            self.edges.update(keys_and_values)

    def get_edges_bulk(self, edge_ids: list[bytes]) -> dict[bytes, bytes]:
        """Fetch multiple serialized edge records.

        Args:
            edge_ids: Edge keys to fetch.

        Returns:
            Mapping for IDs that exist. Missing IDs are omitted.
        """
        with self._lock:
            return {edge_id: self.edges[edge_id] for edge_id in edge_ids if edge_id in self.edges}

    def put_adjacency(self, node_id: bytes, value: bytes) -> None:
        """Store a serialized adjacency record for a node.

        Args:
            node_id: Node key.
            value: Serialized adjacency value.
        """
        with self._lock:
            self.adjacency[node_id] = value

    def get_adjacency(self, node_id: bytes) -> Optional[bytes]:
        """Fetch a serialized adjacency record.

        Args:
            node_id: Node key.

        Returns:
            Serialized adjacency value or ``None`` if missing.
        """
        with self._lock:
            return self.adjacency.get(node_id)

    def put_adjacency_bulk(self, adj_dict: Dict[bytes, bytes]) -> None:
        """Store multiple serialized adjacency records.

        Args:
            adj_dict: Mapping of node keys to serialized adjacency values.
        """
        with self._lock:
            self.adjacency.update(adj_dict)

    def get_adjacency_bulk(self, node_ids: List[bytes]) -> Dict[bytes, bytes]:
        """Fetch multiple serialized adjacency records.

        Args:
            node_ids: Node keys to fetch.

        Returns:
            Mapping for IDs that exist. Missing IDs are omitted.
        """
        with self._lock:
            return {node_id: self.adjacency[node_id] for node_id in node_ids if node_id in self.adjacency}

    def get_node_keys_generator(self, num_nodes=None, key_offset=None):
        """Yield node keys in sorted order.

        Args:
            num_nodes: Optional maximum number of keys to yield.
            key_offset: Optional inclusive starting key.

        Yields:
            Node keys as bytes.
        """
        with self._lock:
            keys = sorted(self.nodes)
        yielded = 0
        for key in keys:
            if key_offset is not None and key < key_offset:
                continue
            yield key
            yielded += 1
            if num_nodes is not None and yielded == num_nodes:
                break

    def get_edge_keys_generator(self, num_edges=None, key_offset=None):
        """Yield edge keys in sorted order.

        Args:
            num_edges: Optional maximum number of keys to yield.
            key_offset: Optional inclusive starting key.

        Yields:
            Edge keys as bytes.
        """
        with self._lock:
            keys = sorted(self.edges)
        yielded = 0
        for key in keys:
            if key_offset is not None and key < key_offset:
                continue
            yield key
            yielded += 1
            if num_edges is not None and yielded == num_edges:
                break

    def index_node(self, node, old_node=None):
        """Add a node to label/property indexes.

        Args:
            node: Node to index.
            old_node: Previous version of the same node, if this is an upsert.
                It is removed from indexes before the new version is added.
        """
        with self._lock:
            if old_node is not None:
                self.unindex_node(old_node)
            node_id = node.get_id
            for label in node.labels:
                self.node_labels.setdefault(label, set()).add(node_id)
            for key, value in node.properties.items():
                try:
                    self.node_properties.setdefault((key, value), set()).add(node_id)
                except TypeError:
                    continue

    def unindex_node(self, node):
        """Remove a node from label/property indexes.

        Args:
            node: Node to remove from indexes.
        """
        with self._lock:
            node_id = node.get_id
            for label in node.labels:
                ids = self.node_labels.get(label)
                if ids is not None:
                    ids.discard(node_id)
                    if not ids:
                        self.node_labels.pop(label, None)
            for key, value in node.properties.items():
                try:
                    ids = self.node_properties.get((key, value))
                except TypeError:
                    continue
                if ids is not None:
                    ids.discard(node_id)
                    if not ids:
                        self.node_properties.pop((key, value), None)

    def node_candidates(self, labels=None, properties=None):
        """Return candidate node IDs for exact label/property filters.

        Args:
            labels: Labels that candidate nodes must contain.
            properties: Exact property matches for candidate nodes.

        Returns:
            Sorted node IDs when at least one usable indexed filter exists.
            ``None`` means the caller should fall back to a full scan.
        """
        with self._lock:
            sets = []
            for label in labels or []:
                sets.append(set(self.node_labels.get(label, set())))
            for key, value in (properties or {}).items():
                try:
                    sets.append(set(self.node_properties.get((key, value), set())))
                except TypeError:
                    return None
            if not sets:
                return None
            return sorted(set.intersection(*sets))

    def index_edge(self, edge, old_edge=None):
        """Add an edge to type/property indexes.

        Args:
            edge: Edge to index.
            old_edge: Previous version of the same edge, if this is an upsert.
                It is removed from indexes before the new version is added.
        """
        with self._lock:
            if old_edge is not None:
                self.unindex_edge(old_edge)
            edge_id = edge.get_id
            if edge.type is not None:
                self.edge_types.setdefault(edge.type, set()).add(edge_id)
            for key, value in edge.properties.items():
                try:
                    self.edge_properties.setdefault((key, value), set()).add(edge_id)
                except TypeError:
                    continue

    def unindex_edge(self, edge):
        """Remove an edge from type/property indexes.

        Args:
            edge: Edge to remove from indexes.
        """
        with self._lock:
            edge_id = edge.get_id
            if edge.type is not None:
                ids = self.edge_types.get(edge.type)
                if ids is not None:
                    ids.discard(edge_id)
                    if not ids:
                        self.edge_types.pop(edge.type, None)
            for key, value in edge.properties.items():
                try:
                    ids = self.edge_properties.get((key, value))
                except TypeError:
                    continue
                if ids is not None:
                    ids.discard(edge_id)
                    if not ids:
                        self.edge_properties.pop((key, value), None)

    def edge_candidates(self, type=None, properties=None):
        """Return candidate edge IDs for exact type/property filters.

        Args:
            type: Required edge type, or ``None``.
            properties: Exact property matches for candidate edges.

        Returns:
            Sorted edge IDs when at least one usable indexed filter exists.
            ``None`` means the caller should fall back to a full scan.
        """
        with self._lock:
            sets = []
            if type is not None:
                sets.append(set(self.edge_types.get(type, set())))
            for key, value in (properties or {}).items():
                try:
                    sets.append(set(self.edge_properties.get((key, value), set())))
                except TypeError:
                    return None
            if not sets:
                return None
            return sorted(set.intersection(*sets))

    def add_adjacency_edge(self, source_id: str, target_id: str, edge_id: str) -> None:
        """Add one directed adjacency entry without rewriting a node blob."""
        with self._lock:
            self.out_adjacency.setdefault(source_id, {})[edge_id] = target_id
            self.in_adjacency.setdefault(target_id, {})[edge_id] = source_id

    def remove_adjacency_edge(self, source_id: str, target_id: str, edge_id: str) -> None:
        """Remove one directed adjacency entry."""
        with self._lock:
            out_edges = self.out_adjacency.get(source_id)
            if out_edges is not None:
                out_edges.pop(edge_id, None)
                if not out_edges:
                    self.out_adjacency.pop(source_id, None)
            in_edges = self.in_adjacency.get(target_id)
            if in_edges is not None:
                in_edges.pop(edge_id, None)
                if not in_edges:
                    self.in_adjacency.pop(target_id, None)

    def adjacency_edge_ids(self, node_id: str, direction: str = "out") -> list[str]:
        """Return adjacent edge IDs from per-edge adjacency records."""
        with self._lock:
            if direction in {"out", "forward"}:
                return sorted(self.out_adjacency.get(node_id, {}))
            if direction in {"in", "backward"}:
                return sorted(self.in_adjacency.get(node_id, {}))
            if direction in {"both", "any"}:
                return sorted(set(self.out_adjacency.get(node_id, {})) | set(self.in_adjacency.get(node_id, {})))
        raise ValueError("direction must be 'out', 'in', or 'both'")

    def neighbor_ids(self, node_id: str, direction: str = "out") -> list[str]:
        """Return neighboring node IDs without reading edge records."""
        with self._lock:
            if direction in {"out", "forward"}:
                return sorted(set(self.out_adjacency.get(node_id, {}).values()))
            if direction in {"in", "backward"}:
                return sorted(set(self.in_adjacency.get(node_id, {}).values()))
            if direction in {"both", "any"}:
                return sorted(
                    set(self.out_adjacency.get(node_id, {}).values())
                    | set(self.in_adjacency.get(node_id, {}).values())
                )
        raise ValueError("direction must be 'out', 'in', or 'both'")

    def clear_indexes(self) -> None:
        """Clear all secondary indexes and optimized adjacency records."""
        with self._lock:
            self.node_labels.clear()
            self.node_properties.clear()
            self.edge_types.clear()
            self.edge_properties.clear()
            self.out_adjacency.clear()
            self.in_adjacency.clear()

    def compact(self, destination_path=None):
        """No-op compaction hook for the in-memory backend."""
        return None

# =========================================
# LMDB Implementation
# =========================================
import struct
class SimpleKV:
    def __init__(self, db_path):
        self.db_path = db_path
        self.max_key_idx = None
        
    def get_num_keys(self):
        _b = self.get('num_keys'.encode('utf-8'))
        num_keys = struct.unpack('<L',_b)
        return num_keys
    
    def put_num_keys(self, num_keys):
        num_keys_bytes = struct.pack('<L',num_keys)
        _b = self.put('num_keys'.encode('utf-8'), num_keys_bytes)

    def put(self, key : bytes, value : bytes):
        with self.env.begin(write=True, db=self.db_path) as txn:
            txn.put(key, value)

    def get(self, key):
        with self.env.begin(write=False, db=self.db_path) as txn:
            return txn.get(key)
    
    def encode_db_key(self, key):
        """If the key exists, it will return the existing key.
        if the key does not exist, it will add it to the KV store with a new increment, 
        and return that.
        """
        v = self.get(key.encode('utf-8'))
        if v is None:
            num_keys = self.get_num_keys()
            num_keys += 1
            self.put_num_keys(num_keys)
            return num_keys
        
    def decode_db_key(self, key):
        v = self.get(key.encode('utf-8'))
        return v
        

class LMDBStore(KVStore):
    def __init__(self, path='graph_lmdb', map_size=10_485_760, map_id = True, map_keys = False):
        try:
            import lmdb
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError("LMDBStore requires the 'lmdb' extra: uv sync --extra lmdb") from exc

        """
        Creates/opens an LMDB environment with three named sub-databases:
          - b'nodes' for node data
          - b'edges' for edge data
          - b'adj'   for adjacency lists
        """
        max_dbs = 6
        if map_keys:
            max_dbs += 2
        self._txn_lock = RLock()
        self._active_txn = None
        self.env = lmdb.open(path, map_size=map_size, subdir=True, max_dbs=max_dbs)
        self.nodes_db = self.env.open_db(b'nodes')
        self.edges_db = self.env.open_db(b'edges')
        self.adj_db   = self.env.open_db(b'adj')
        self.out_adj_db = self.env.open_db(b'out_adj')
        self.in_adj_db = self.env.open_db(b'in_adj')
        self.index_db = self.env.open_db(b'indexes')

        if map_keys:
            self.node_key_encdec_db = self.env.open_db(b'node_key_db')
            self.edge_key_encdec_db = self.env.open_db(b'edge_key_db')
            self.node_key_encdec = SimpleKV(b'node_key_db')
            self.edge_key_encdec = SimpleKV(b'edge_key_db')

    @contextmanager
    def read_transaction(self):
        """Open a read transaction or reuse the active write transaction."""
        if self._active_txn is not None:
            yield self
            return
        with self._txn_lock:
            with self.env.begin(write=False) as txn:
                self._active_txn = txn
                try:
                    yield self
                finally:
                    self._active_txn = None

    @contextmanager
    def write_transaction(self):
        """Open one LMDB write transaction for a graph mutation."""
        if self._active_txn is not None:
            yield self
            return
        with self._txn_lock:
            with self.env.begin(write=True) as txn:
                self._active_txn = txn
                try:
                    yield self
                finally:
                    self._active_txn = None

    @contextmanager
    def _transaction(self, write=False):
        if self._active_txn is not None:
            yield self._active_txn
        else:
            with self.env.begin(write=write) as txn:
                yield txn

    def _scan_prefix(self, db, prefix: bytes):
        with self._transaction(write=False) as txn:
            with txn.cursor(db=db) as cursor:
                if not cursor.set_range(prefix):
                    return []
                return [(k, v) for k, v in cursor if k.startswith(prefix)]

    @staticmethod
    def _part(value) -> bytes:
        return str(value).encode('utf-8')

    @staticmethod
    def _prop_value(value) -> bytes:
        return f"{type(value).__name__}:{repr(value)}".encode('utf-8')

    @staticmethod
    def _index_key(*parts) -> bytes:
        return b'\x00'.join(part if isinstance(part, bytes) else str(part).encode('utf-8') for part in parts)

    # -- Basic methods (not used by GraphDB if we rely on specialized node/edge methods below)
    def put(self, key: bytes, value: bytes):
        # For LMDB, we'd need to decide which db to put to. This might remain unused.
        pass

    def get(self, key: bytes) -> bytes:
        # Not used directly.
        return None

    def delete(self, key: bytes):
        pass

    def range_iter(self, start_key: bytes, end_key: bytes):
        # For demonstration, let us assume node range queries share a single sub-DB.
        # This might need refining.
        with self._transaction(write=False) as txn:
            cursor = txn.cursor(db=self.nodes_db)
            if not cursor.set_range(start_key):
                return
            for k, v in cursor:
                if k > end_key:
                    break
                yield k, v

    def close(self):
        self.env.close()

    # -- Specialized node methods
    def put_node(self, node_id: bytes, value: bytes):
        with self._transaction(write=True) as txn:
            txn.put(node_id, value, db=self.nodes_db)

    def get_node(self, node_id: bytes) -> bytes:
        with self._transaction(write=False) as txn:
            return txn.get(node_id, db=self.nodes_db)

    def delete_node(self, node_id: bytes):
        with self._transaction(write=True) as txn:
            txn.delete(node_id, db=self.nodes_db)

    # -- Specialized edge methods
    def put_edge(self, edge_id: bytes, value: bytes):
        with self._transaction(write=True) as txn:
            txn.put(edge_id, value, db=self.edges_db)

    def get_edge(self, edge_id: str) -> bytes:
        with self._transaction(write=False) as txn:
            return txn.get(edge_id, db=self.edges_db)

    def delete_edge(self, edge_id: str):
        with self._transaction(write=True) as txn:
            txn.delete(edge_id, db=self.edges_db)

    def put_nodes_bulk(self, keys_and_values: dict[bytes, bytes]):
        """Write a batch of nodes in a single transaction."""
        with self._transaction(write=True) as txn:
            for k, v in keys_and_values.items():
                txn.put(k, v, db=self.nodes_db)
            # for node_id, val in keys_and_values.items():
            #     txn.put(node_id, val)
    
    def get_nodes_bulk(self, node_ids: list[bytes]) -> dict[bytes, bytes]:
        """Retrieve multiple nodes in one read transaction."""
        results = {}
        with self._transaction(write=False) as txn:
            for node_id in node_ids:
                data = txn.get(node_id, db=self.nodes_db)
                if data is not None:
                    results[node_id] = data

        return results
    
    def put_edges_bulk(self, keys_and_values: dict[bytes, bytes]):
        with self._transaction(write=True) as txn:
            for edge_id, val in keys_and_values.items():
                txn.put(edge_id, val, db=self.edges_db)
    
    def get_edges_bulk(self, edge_ids: list[bytes]) -> dict[bytes, bytes]:
        results = {}
        with self._transaction(write=False) as txn:
            for edge_id in edge_ids:
                data = txn.get(edge_id, db=self.edges_db)
                if data is not None:
                    results[edge_id] = data
        return results
    
    def get_node_keys_generator(self, num_nodes = None, key_offset = None):
        yielded = 0
        with self._transaction(write=False) as txn:
            with txn.cursor(db=self.nodes_db) as c:
                if key_offset is not None:
                    c.set_range(key_offset)
                for k, _ in c:
                    yield k
                    yielded += 1
                    if num_nodes is not None and yielded == num_nodes:
                        break

    def get_edge_keys_generator(self, num_edges = None, key_offset = None):
        yielded = 0
        with self._transaction(write=False) as txn:
            with txn.cursor(db=self.edges_db) as c:
                if key_offset is not None:
                    c.set_range(key_offset)
                for k, _ in c:
                    yield k
                    yielded += 1
                    if num_edges is not None and yielded == num_edges:
                        break

    # ----- Adjacency Methods -----
    def put_adjacency(self, node_id: bytes, value: bytes) -> None:
        with self._transaction(write=True) as txn:
            txn.put(node_id, value, db=self.adj_db)

    def get_adjacency(self, node_id: Union[bytes, str]) -> Optional[bytes]:
        with self._transaction(write=False) as txn:
            if isinstance(node_id, bytes):
                return txn.get(node_id, db=self.adj_db)
            else:
                raise Exception('Get adjacency requires the bytes! (serialized data)')

    # -------------------------------------------------------------------------
    # Bulk Write: Adjacency
    # -------------------------------------------------------------------------
    def put_adjacency_bulk(self, adj_dict: Dict[bytes, bytes]) -> None:
        """
        Insert/update multiple adjacency lists in one transaction.
        :param adj_dict: a dict mapping node_id -> serialized adjacency (list of edges)
        """
        with self._transaction(write=True) as txn:
            for node_id, val in adj_dict.items():
                txn.put(node_id, val, db=self.adj_db)

    # -------------------------------------------------------------------------
    # Bulk Read: Adjacency
    # -------------------------------------------------------------------------
    def get_adjacency_bulk(self, node_ids: List[bytes]) -> Dict[bytes, bytes]:
        """
        Retrieve multiple adjacency lists in a single read transaction.
        Returns a dict { node_id: serialized adjacency } for all found items.
        """
        results = {}
        with self._transaction(write=False) as txn:
            for node_id in node_ids:
                data = txn.get(node_id, db=self.adj_db)
                if data is not None:
                    results[node_id] = data
        return results

    def add_adjacency_edge(self, source_id: str, target_id: str, edge_id: str) -> None:
        source = self._part(source_id)
        target = self._part(target_id)
        edge = self._part(edge_id)
        with self._transaction(write=True) as txn:
            txn.put(self._index_key(b'out', source, edge), target, db=self.out_adj_db)
            txn.put(self._index_key(b'in', target, edge), source, db=self.in_adj_db)

    def remove_adjacency_edge(self, source_id: str, target_id: str, edge_id: str) -> None:
        source = self._part(source_id)
        target = self._part(target_id)
        edge = self._part(edge_id)
        with self._transaction(write=True) as txn:
            txn.delete(self._index_key(b'out', source, edge), db=self.out_adj_db)
            txn.delete(self._index_key(b'in', target, edge), db=self.in_adj_db)

    def adjacency_edge_ids(self, node_id: str, direction: str = "out") -> list[str]:
        node = self._part(node_id)
        if direction in {"out", "forward"}:
            prefix = self._index_key(b'out', node) + b'\x00'
            return sorted(k[len(prefix):].decode('utf-8') for k, _ in self._scan_prefix(self.out_adj_db, prefix))
        if direction in {"in", "backward"}:
            prefix = self._index_key(b'in', node) + b'\x00'
            return sorted(k[len(prefix):].decode('utf-8') for k, _ in self._scan_prefix(self.in_adj_db, prefix))
        if direction in {"both", "any"}:
            return sorted(set(self.adjacency_edge_ids(node_id, "out")) | set(self.adjacency_edge_ids(node_id, "in")))
        raise ValueError("direction must be 'out', 'in', or 'both'")

    def neighbor_ids(self, node_id: str, direction: str = "out") -> list[str]:
        node = self._part(node_id)
        if direction in {"out", "forward"}:
            prefix = self._index_key(b'out', node) + b'\x00'
            return sorted(set(v.decode('utf-8') for _, v in self._scan_prefix(self.out_adj_db, prefix)))
        if direction in {"in", "backward"}:
            prefix = self._index_key(b'in', node) + b'\x00'
            return sorted(set(v.decode('utf-8') for _, v in self._scan_prefix(self.in_adj_db, prefix)))
        if direction in {"both", "any"}:
            return sorted(set(self.neighbor_ids(node_id, "out")) | set(self.neighbor_ids(node_id, "in")))
        raise ValueError("direction must be 'out', 'in', or 'both'")

    def index_node(self, node, old_node=None):
        with self._transaction(write=True) as txn:
            if old_node is not None:
                self.unindex_node(old_node)
            node_id = self._part(node.get_id)
            for label in node.labels:
                txn.put(self._index_key(b'nl', self._part(label), node_id), b'', db=self.index_db)
            for key, value in node.properties.items():
                try:
                    hash(value)
                except TypeError:
                    continue
                txn.put(self._index_key(b'np', self._part(key), self._prop_value(value), node_id), b'', db=self.index_db)

    def unindex_node(self, node):
        with self._transaction(write=True) as txn:
            node_id = self._part(node.get_id)
            for label in node.labels:
                txn.delete(self._index_key(b'nl', self._part(label), node_id), db=self.index_db)
            for key, value in node.properties.items():
                try:
                    hash(value)
                except TypeError:
                    continue
                txn.delete(self._index_key(b'np', self._part(key), self._prop_value(value), node_id), db=self.index_db)

    def node_candidates(self, labels=None, properties=None):
        sets = []
        for label in labels or []:
            prefix = self._index_key(b'nl', self._part(label)) + b'\x00'
            sets.append({k[len(prefix):].decode('utf-8') for k, _ in self._scan_prefix(self.index_db, prefix)})
        for key, value in (properties or {}).items():
            try:
                hash(value)
            except TypeError:
                return None
            prefix = self._index_key(b'np', self._part(key), self._prop_value(value)) + b'\x00'
            sets.append({k[len(prefix):].decode('utf-8') for k, _ in self._scan_prefix(self.index_db, prefix)})
        if not sets:
            return None
        return sorted(set.intersection(*sets))

    def index_edge(self, edge, old_edge=None):
        with self._transaction(write=True) as txn:
            if old_edge is not None:
                self.unindex_edge(old_edge)
            edge_id = self._part(edge.get_id)
            if edge.type is not None:
                txn.put(self._index_key(b'et', self._part(edge.type), edge_id), b'', db=self.index_db)
            for key, value in edge.properties.items():
                try:
                    hash(value)
                except TypeError:
                    continue
                txn.put(self._index_key(b'ep', self._part(key), self._prop_value(value), edge_id), b'', db=self.index_db)

    def unindex_edge(self, edge):
        with self._transaction(write=True) as txn:
            edge_id = self._part(edge.get_id)
            if edge.type is not None:
                txn.delete(self._index_key(b'et', self._part(edge.type), edge_id), db=self.index_db)
            for key, value in edge.properties.items():
                try:
                    hash(value)
                except TypeError:
                    continue
                txn.delete(self._index_key(b'ep', self._part(key), self._prop_value(value), edge_id), db=self.index_db)

    def edge_candidates(self, type=None, properties=None):
        sets = []
        if type is not None:
            prefix = self._index_key(b'et', self._part(type)) + b'\x00'
            sets.append({k[len(prefix):].decode('utf-8') for k, _ in self._scan_prefix(self.index_db, prefix)})
        for key, value in (properties or {}).items():
            try:
                hash(value)
            except TypeError:
                return None
            prefix = self._index_key(b'ep', self._part(key), self._prop_value(value)) + b'\x00'
            sets.append({k[len(prefix):].decode('utf-8') for k, _ in self._scan_prefix(self.index_db, prefix)})
        if not sets:
            return None
        return sorted(set.intersection(*sets))

    def clear_indexes(self) -> None:
        with self._transaction(write=True) as txn:
            for db in (self.index_db, self.out_adj_db, self.in_adj_db):
                with txn.cursor(db=db) as cursor:
                    if cursor.first():
                        while True:
                            cursor.delete()
                            if not cursor.next():
                                break

    def compact(self, destination_path=None):
        if destination_path is None:
            raise ValueError("LMDB compaction requires a destination_path")
        self.env.copy(destination_path, compact=True)
        return destination_path


# =========================================
# LevelDB Implementation
# =========================================

class LevelDBStore(KVStore):
    def __init__(self, path='graph_leveldb'):
        try:
            import plyvel
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError("LevelDBStore requires the 'leveldb' extra: uv sync --extra leveldb") from exc

        """Create or open a LevelDB store. We'll store nodes/edges by prefix."""
        
        self.db_paths = {'nodes' : os.path.join('nodes'), 'edges': os.path.join('edges'), 'adjacency' : os.path.join('adjacency')}
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
        self.db_nodes = plyvel.DB(os.path.join(path, 'nodes'), create_if_missing=True)
        self.db_edges = plyvel.DB(os.path.join(path, 'edges'), create_if_missing=True)
        self.db_adj   = plyvel.DB(os.path.join(path, 'adjacency'), create_if_missing=True)
        self.db_dict = {
            'nodes' : self.db_nodes,
            'edges' : self.db_edges,
            'adjacency' : self.db_adj
        }
    # def put(self, key: bytes, value: bytes):
    #     self.db.put(key, value)

    # def get(self, key: bytes) -> bytes:
    #     return self.db.get(key)

    # def delete(self, key: bytes):
    #     self.db.delete(key)
    def get_db_path(self, db_string = 'nodes'):
        return self.db_paths[db_string]
    
    def range_iter(self, start_key: bytes, end_key: bytes):
        with self.db_nodes.iterator(start=start_key, stop=end_key) as it:
            for k, v in it:
                yield k, v

    def get_db_iterator(self, which_db = 'nodes'):
        with self.db_dict[which_db].iterator() as it:
            for k, v in it:
                yield k, v

    def get_node_keys_iterator(self):
        return self.get_db_iterator(which_db='nodes')

    def get_node_keys_generator(self, num_nodes = None, key_offset = None):
        yielded = 0
        with self.db_nodes.iterator(start=key_offset) as it:
            for k, _ in it:
                yield k
                yielded += 1
                if num_nodes is not None and yielded == num_nodes:
                    break

    def get_edge_keys_generator(self, num_edges = None, key_offset = None):
        yielded = 0
        with self.db_edges.iterator(start=key_offset) as it:
            for k, _ in it:
                yield k
                yielded += 1
                if num_edges is not None and yielded == num_edges:
                    break


    # -- Specialized methods for nodes
    def put_node(self, node_id: bytes, value: bytes):
        self.db_nodes.put(node_id,value)

    def get_node(self, node_id: bytes) -> bytes:
        return self.db_nodes.get(node_id)

    def delete_node(self, node_id: bytes):
        self.db_nodes.delete(node_id)

    # -- Specialized methods for edges
    def put_edge(self, edge_id: bytes, value: bytes):
        self.db_edges.put(edge_id, value)

    def get_edge(self, edge_id: bytes) -> bytes:
        return self.db_edges.get(edge_id)

    def delete_edge(self, edge_id: str):
        self.db_edges.delete(edge_id)

    def put_nodes_bulk(self, keys_and_values: dict[bytes, bytes]):
        """Use a WriteBatch for atomic bulk updates."""
        with self.db_nodes.write_batch() as wb:
            for node_id, val in keys_and_values.items():
                # s = node_id.decode()
                # wb.put(f"N:{s}".encode('utf-8'), val)
                wb.put(node_id, val)

    def get_nodes_bulk(self, node_ids: list[bytes]) -> dict[bytes, bytes]:
        results = {}
        for node_id in node_ids:
            data = self.db_nodes.get(node_id)
            if data is not None:
                results[node_id] = data
        return results
    
    def put_edges_bulk(self, keys_and_values: dict[bytes, bytes]):
        with self.db_edges.write_batch() as wb:
            for edge_id, val in keys_and_values.items():
                wb.put(edge_id, val)
    
    def get_edges_bulk(self, edge_ids: list[bytes]) -> dict[bytes, bytes]:
        results = {}
        for edge_id in edge_ids:
            # s = edge_id.decode()
            # key = f"E:{s}".encode('utf-8')
            data = self.db_edges.get(edge_id)
            if data is not None:
                results[edge_id] = data
        return results
    

    # ----- Adjacency Methods -----
    def put_adjacency(self, node_id: bytes, value: bytes) -> None:
        self.db_adj.put(node_id, value)

    def get_adjacency(self, node_id: bytes) -> Optional[bytes]:
        return self.db_adj.get(node_id)
    
    # -------------------------------------------------------------------------
    # Bulk Write: Adjacency
    # -------------------------------------------------------------------------
    def put_adjacency_bulk(self, adj_dict: Dict[str, bytes]) -> None:
        """
        Insert/update multiple adjacency lists in one write batch.
        :param adj_dict: a dict mapping node_id -> serialized adjacency
        """
        with self.db_adj.write_batch() as wb:
            for node_id, val in adj_dict.items():
                wb.put(node_id, val)

    # -------------------------------------------------------------------------
    # Bulk Read: Adjacency
    # -------------------------------------------------------------------------
    def get_adjacency_bulk(self, node_ids: List[bytes]) -> Dict[bytes, bytes]:
        results = {}
        for node_id in node_ids:
            # s = node_id.decode()
            # key = f"A:{s}".encode('utf-8')
            data = self.db_adj.get(node_id)
            if data is not None:
                results[node_id] = data
        return results

    def close(self):
        self.db_adj.close()
        self.db_edges.close()
        self.db_nodes.close()

class SimpleIndexCounterKVStore:
    """This is to help with lowering storage requirements 
    for edge and node keys, by casting them to long ints. 

    It makes use of the struct.pack and struct.unpack functions 
    and a simple counter (also stored in the medatadata) to count the number of  
    keys (and hence the index) already entered. 
    """
    def __init__(self, dbenv = None, db_path = b'nodes'):
        self.db_path = db_path
        self.env = dbenv
        self.kvdb = self.env.open_db(self.db_path)        
        self.max_key_idx = None
        
    def get_num_keys(self):
        _b = self.get('num_keys'.encode('utf-8'))
        if _b is None:
            return 0
        num_keys = struct.unpack('<L',_b)[0]
        return num_keys
    
    def put_num_keys(self, num_keys):
        num_keys_bytes = struct.pack('<L',num_keys)
        _b = self.put('num_keys'.encode('utf-8'), num_keys_bytes)

    def put(self, key : bytes, value : bytes):
        with self.env.begin(write=True, db=self.kvdb) as txn:
            txn.put(key, value)

    def get(self, key):
        with self.env.begin(write=False, db=self.kvdb) as txn:
            vv = txn.get(key)
            return vv
    
    def encode_db_key(self, key):
        """If the key exists, it will return the existing key.
        if the key does not exist, it will add it to the KV store with a new increment, 
        and return that.
        """
        _enc_key = key.encode('utf-8')
        v = self.get(_enc_key)
        if v is None:
            num_keys = self.get_num_keys()
            num_keys += 1
            k_idx = num_keys
            self.put_num_keys(num_keys)
            self.put(_enc_key, _pack_long_int(num_keys))
            return num_keys
        return _unpack_long_int(v)
    def decode_db_key(self, key):
        v = self.get(key.encode('utf-8'))
