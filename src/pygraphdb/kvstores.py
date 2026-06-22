from typing import Optional, Dict, List, Union
import os
import struct


_TYPED_ADJ_SEP = b"\x1f"


def _missing_dependency_error(package_name, install_name=None, feature_name=None):
    """Build a consistent optional dependency error.

    Examples:
        >>> "lmdb" in str(_missing_dependency_error("lmdb"))
        True
    """
    install_name = install_name or package_name
    feature_name = feature_name or package_name
    return ImportError(
        f"Missing optional dependency '{package_name}' required for {feature_name}. "
        f"Install it with `python -m pip install {install_name}` or `uv add {install_name}`."
    )


def _pack_long_int(int_val):
    """Pack an integer as little-endian unsigned long bytes.

    Examples:
        >>> _unpack_long_int(_pack_long_int(3))
        3
    """
    return struct.pack('<L', int_val)

def _unpack_long_int(int_val):
    """Unpack little-endian unsigned long bytes.

    Examples:
        >>> _unpack_long_int(_pack_long_int(3))
        3
    """
    return struct.unpack('<L', int_val)[0]


def _typed_adjacency_key(direction: str, node_id: bytes, edge_type: str, edge_id: bytes = b"") -> bytes:
    """Build a typed adjacency key.

    Examples:
        >>> _typed_adjacency_key("out", b"drug-1", "drug-to-protein", b"e1")
        b'out\\x1fdrug-1\\x1fdrug-to-protein\\x1fe1'
    """
    edge_type_bytes = edge_type.encode("utf-8")
    return _TYPED_ADJ_SEP.join([direction.encode("utf-8"), node_id, edge_type_bytes, edge_id])


def _typed_adjacency_prefix(direction: str, node_id: bytes, edge_type: str) -> bytes:
    """Build the key prefix for a typed adjacency scan.

    Examples:
        >>> _typed_adjacency_prefix("out", b"drug-1", "drug-to-protein").startswith(b"out")
        True
    """
    return _typed_adjacency_key(direction, node_id, edge_type)


class KVStore:
    """Abstract interface for a simple key-value store."""

    # The basic K/V methods:
    def put(self, key: bytes, value: bytes):
        """Store a raw key/value pair."""
        raise NotImplementedError

    def get(self, key: bytes) -> bytes:
        """Return a raw value by key."""
        raise NotImplementedError

    def delete(self, key: bytes):
        """Delete a raw key/value pair."""
        raise NotImplementedError

    def range_iter(self, start_key: bytes, end_key: bytes):
        """Iterate over keys from start_key to end_key (inclusive)."""
        raise NotImplementedError

    def close(self):
        """Close any resources owned by the store."""
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
        """Retrieve multiple serialized nodes by key."""
        raise NotImplementedError

    def put_edges_bulk(self, keys_and_values: dict[str, bytes]):
        """Store multiple serialized edges."""
        raise NotImplementedError
    
    def get_edges_bulk(self, edge_ids: list[str]) -> dict[str, bytes]:
        """Retrieve multiple serialized edges by key."""
        raise NotImplementedError

    def put_typed_adjacency(self, source_id: bytes, target_id: bytes, edge_type: str, edge_id: bytes):
        """Store typed adjacency records for an edge."""
        raise NotImplementedError

    def put_typed_adjacency_bulk(self, records: list[tuple[bytes, bytes, str, bytes]]):
        """Store typed adjacency records for multiple edges."""
        for source_id, target_id, edge_type, edge_id in records:
            self.put_typed_adjacency(source_id, target_id, edge_type, edge_id)

    def delete_typed_adjacency(self, source_id: bytes, target_id: bytes, edge_type: str, edge_id: bytes):
        """Delete typed adjacency records for an edge."""
        raise NotImplementedError

    def iter_typed_adjacency(self, node_id: bytes, edge_type: str, direction: str = "out"):
        """Yield typed adjacency records for a node and edge type."""
        raise NotImplementedError

# =========================================
# LMDB Implementation
# =========================================
class SimpleKV:
    """Small LMDB-backed helper for metadata key/value access.

    Args:
        db_path: LMDB database handle or name used by transactions.
    """

    def __init__(self, db_path):
        """Initialize the helper with an LMDB database path or handle."""
        self.db_path = db_path
        self.max_key_idx = None
        
    def get_num_keys(self):
        """Return the stored key counter."""
        _b = self.get('num_keys'.encode('utf-8'))
        num_keys = struct.unpack('<L',_b)
        return num_keys
    
    def put_num_keys(self, num_keys):
        """Store the key counter."""
        num_keys_bytes = struct.pack('<L',num_keys)
        _b = self.put('num_keys'.encode('utf-8'), num_keys_bytes)

    def put(self, key : bytes, value : bytes):
        """Write a metadata key/value pair."""
        with self.env.begin(write=True, db=self.db_path) as txn:
            txn.put(key, value)

    def get(self, key):
        """Read a metadata value by key."""
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
        """Return the encoded database key for a user key."""
        v = self.get(key.encode('utf-8'))
        return v
        

class LMDBStore(KVStore):
    """LMDB implementation of the PyGraphDB key-value store.

    Examples:
        >>> store = LMDBStore(path="/tmp/example_graph_lmdb")  # doctest: +SKIP
    """

    def __init__(self, path='graph_lmdb', map_size=10_485_760, map_id = True, map_keys = False):
        """
        Creates/opens an LMDB environment with three named sub-databases:
          - b'nodes' for node data
          - b'edges' for edge data
          - b'adj'   for adjacency lists
        """
        try:
            import lmdb
        except ImportError as exc:
            raise _missing_dependency_error("lmdb", feature_name="LMDBStore") from exc

        max_dbs = 4
        if map_keys:
            max_dbs += 2
        self.env = lmdb.open(path, map_size=map_size, subdir=True, max_dbs=max_dbs)
        self.nodes_db = self.env.open_db(b'nodes')
        self.edges_db = self.env.open_db(b'edges')
        self.adj_db   = self.env.open_db(b'adj')
        self.typed_adj_db = self.env.open_db(b'typed_adj')

        if map_keys:
            self.node_key_encdec_db = self.env.open_db(b'node_key_db')
            self.edge_key_encdec_db = self.env.open_db(b'edge_key_db')
            self.node_key_encdec = SimpleKV(b'node_key_db')
            self.edge_key_encdec = SimpleKV(b'edge_key_db')

    # -- Basic methods (not used by GraphDB if we rely on specialized node/edge methods below)
    def put(self, key: bytes, value: bytes):
        """Placeholder generic put; graph code uses specialized methods."""
        # For LMDB, we'd need to decide which db to put to. This might remain unused.
        pass

    def get(self, key: bytes) -> bytes:
        """Placeholder generic get; graph code uses specialized methods."""
        # Not used directly.
        return None

    def delete(self, key: bytes):
        """Placeholder generic delete; graph code uses specialized methods."""
        pass

    def range_iter(self, start_key: bytes, end_key: bytes):
        """Yield node records whose keys fall within an inclusive range."""
        # For demonstration, let us assume node range queries share a single sub-DB.
        # This might need refining.
        with self.env.begin(write=False, db=self.nodes_db) as txn:
            cursor = txn.cursor()
            if not cursor.set_range(start_key):
                return
            for k, v in cursor:
                if k > end_key:
                    break
                yield k, v

    def close(self):
        """Close the LMDB environment."""
        self.env.close()

    # -- Specialized node methods
    def put_node(self, node_id: bytes, value: bytes):
        """Store a serialized node by byte key."""
        with self.env.begin(write=True, db=self.nodes_db) as txn:
            txn.put(node_id, value)

    def get_node(self, node_id: bytes) -> bytes:
        """Return serialized node bytes by key, or ``None``."""
        with self.env.begin(write=False, db=self.nodes_db) as txn:
            return txn.get(node_id)

    def delete_node(self, node_id: bytes):
        """Delete a node by byte key."""
        with self.env.begin(write=True, db=self.nodes_db) as txn:
            txn.delete(node_id)

    # -- Specialized edge methods
    def put_edge(self, edge_id: bytes, value: bytes):
        """Store a serialized edge by byte key."""
        with self.env.begin(write=True, db=self.edges_db) as txn:
            txn.put(edge_id, value)

    def get_edge(self, edge_id: str) -> bytes:
        """Return serialized edge bytes by key, or ``None``."""
        with self.env.begin(write=False, db=self.edges_db) as txn:
            return txn.get(edge_id)

    def delete_edge(self, edge_id: str):
        """Delete an edge by byte key."""
        with self.env.begin(write=True, db=self.edges_db) as txn:
            txn.delete(edge_id)

    def put_nodes_bulk(self, keys_and_values: dict[bytes, bytes]):
        """Write a batch of nodes in a single transaction."""
        with self.env.begin(write=True, db=self.nodes_db) as txn:
            for node_id, val in keys_and_values.items():
                txn.put(node_id, val)
    
    def get_nodes_bulk(self, node_ids: list[bytes]) -> dict[bytes, bytes]:
        """Retrieve multiple nodes in one read transaction."""
        results = {}
        with self.env.begin(write=False, db=self.nodes_db) as txn:
            with txn.cursor() as c:
            # for node_id in node_ids:
                data = c.getmulti(node_ids)
                if data is not None:
                    results.update({k : v for k, v in data})

        return results
    
    def put_edges_bulk(self, keys_and_values: dict[bytes, bytes]):
        """Store many serialized edges in one transaction."""
        with self.env.begin(write=True, db=self.edges_db) as txn:
            for edge_id, val in keys_and_values.items():
                txn.put(edge_id, val)
    
    def get_edges_bulk(self, edge_ids: list[bytes]) -> dict[bytes, bytes]:
        """Return serialized edges for the requested keys."""
        results = {}
        with self.env.begin(write=False, db=self.edges_db) as txn:
            for edge_id in edge_ids:
                data = txn.get(edge_id)
                if data is not None:
                    results[edge_id] = data
        return results

    def get_edge_keys_generator(self, num_edges = None, key_offset = None):
        """Yield edge keys from the edge database."""
        yielded = 0
        with self.env.begin(write=False, db=self.edges_db) as txn:
            with txn.cursor() as c:
                if key_offset is not None:
                    c.set_range(key_offset)
                for k, _ in c:
                    yield k
                    yielded += 1
                    if num_edges is not None and yielded == num_edges:
                        break
    
    def get_node_keys_generator(self, num_nodes = None, key_offset = None):
        """Yield node keys from the node database."""
        yielded = 0
        with self.env.begin(write = False, db = self.nodes_db) as txn:
            with txn.cursor() as c:
                if key_offset is not None:
                    c.set_range(key_offset)
                for k, _ in c:
                    yield k
                    yielded += 1
                    if num_nodes is not None and yielded == num_nodes:
                        break

    # ----- Adjacency Methods -----
    def put_adjacency(self, node_id: bytes, value: bytes) -> None:
        """Store a serialized adjacency list for a node."""
        with self.env.begin(write=True, db=self.adj_db) as txn:
            txn.put(node_id, value)

    def get_adjacency(self, node_id: Union[bytes, str]) -> Optional[bytes]:
        """Return a serialized adjacency list for a node."""
        with self.env.begin(write=False, db=self.adj_db) as txn:
            if isinstance(node_id, bytes):
                return txn.get(node_id)
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
        with self.env.begin(write=True, db=self.adj_db) as txn:
            for node_id, val in adj_dict.items():
                txn.put(node_id, val)

    # -------------------------------------------------------------------------
    # Bulk Read: Adjacency
    # -------------------------------------------------------------------------
    def get_adjacency_bulk(self, node_ids: List[bytes]) -> Dict[bytes, bytes]:
        """
        Retrieve multiple adjacency lists in a single read transaction.
        Returns a dict { node_id: serialized adjacency } for all found items.
        """
        results = {}
        with self.env.begin(write=False, db=self.adj_db) as txn:
            for node_id in node_ids:
                data = txn.get(node_id)
                if data is not None:
                    results[node_id] = data
        return results

    # ----- Typed Adjacency Methods -----
    def put_typed_adjacency(self, source_id: bytes, target_id: bytes, edge_type: str, edge_id: bytes):
        """Store forward and reverse typed adjacency records."""
        with self.env.begin(write=True, db=self.typed_adj_db) as txn:
            txn.put(_typed_adjacency_key("out", source_id, edge_type, edge_id), target_id)
            txn.put(_typed_adjacency_key("in", target_id, edge_type, edge_id), source_id)

    def put_typed_adjacency_bulk(self, records: list[tuple[bytes, bytes, str, bytes]]):
        """Store many typed adjacency records in one transaction."""
        with self.env.begin(write=True, db=self.typed_adj_db) as txn:
            for source_id, target_id, edge_type, edge_id in records:
                txn.put(_typed_adjacency_key("out", source_id, edge_type, edge_id), target_id)
                txn.put(_typed_adjacency_key("in", target_id, edge_type, edge_id), source_id)

    def delete_typed_adjacency(self, source_id: bytes, target_id: bytes, edge_type: str, edge_id: bytes):
        """Delete forward and reverse typed adjacency records."""
        with self.env.begin(write=True, db=self.typed_adj_db) as txn:
            txn.delete(_typed_adjacency_key("out", source_id, edge_type, edge_id))
            txn.delete(_typed_adjacency_key("in", target_id, edge_type, edge_id))

    def iter_typed_adjacency(self, node_id: bytes, edge_type: str, direction: str = "out"):
        """Yield typed adjacency ``(edge_id, neighbor_id)`` pairs."""
        prefix = _typed_adjacency_prefix(direction, node_id, edge_type)
        with self.env.begin(write=False, db=self.typed_adj_db) as txn:
            cursor = txn.cursor()
            if not cursor.set_range(prefix):
                return
            for key, neighbor_id in cursor:
                if not key.startswith(prefix):
                    break
                edge_id = key[len(prefix):]
                yield edge_id, neighbor_id


# =========================================
# LevelDB Implementation
# =========================================

class LevelDBStore(KVStore):
    """LevelDB implementation backed by ``plyvel``.

    Args:
        path: Directory that will contain the LevelDB sub-databases.

    Examples:
        >>> store = LevelDBStore(path="/tmp/example_graph_leveldb")  # doctest: +SKIP
    """

    def __init__(self, path='graph_leveldb'):
        """Create or open a LevelDB store. We'll store nodes/edges by prefix."""
        try:
            import plyvel
        except ImportError as exc:
            raise _missing_dependency_error("plyvel", feature_name="LevelDBStore") from exc
        
        self.db_paths = {'nodes' : os.path.join('nodes'), 'edges': os.path.join('edges'), 'adjacency' : os.path.join('adjacency'), 'typed_adjacency': os.path.join('typed_adjacency')}
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
        self.db_nodes = plyvel.DB(os.path.join(path, 'nodes'), create_if_missing=True)
        self.db_edges = plyvel.DB(os.path.join(path, 'edges'), create_if_missing=True)
        self.db_adj   = plyvel.DB(os.path.join(path, 'adjacency'), create_if_missing=True)
        self.db_typed_adj = plyvel.DB(os.path.join(path, 'typed_adjacency'), create_if_missing=True)
        self.db_dict = {
            'nodes' : self.db_nodes,
            'edges' : self.db_edges,
            'adjacency' : self.db_adj,
            'typed_adjacency': self.db_typed_adj,
        }
    # def put(self, key: bytes, value: bytes):
    #     self.db.put(key, value)

    # def get(self, key: bytes) -> bytes:
    #     return self.db.get(key)

    # def delete(self, key: bytes):
    #     self.db.delete(key)
    def get_db_path(self, db_string = 'nodes'):
        """Return the relative path for a named LevelDB database.

        Examples:
            >>> LevelDBStore.get_db_path.__name__
            'get_db_path'
        """
        return self.db_paths[db_string]
    
    def range_iter(self, start_key: bytes, end_key: bytes):
        """Yield records whose keys fall within a range.

        Note:
            This generic iterator is not used by the main graph APIs.
        """
        with self.db.iterator(start=start_key, stop=end_key) as it:
            for k, v in it:
                yield k, v

    def get_db_iterator(self, which_db = 'nodes'):
        """Yield all records from a named sub-database."""
        with self.db_dict[which_db].iterator() as it:
            for k, v in it:
                yield k, v

    def get_node_keys_iterator(self):
        """Yield node database records."""
        return self.get_db_iterator(which_db='nodes')

    def get_node_keys_generator(self, num_nodes = None, key_offset = None):
        """Yield node keys from the node database."""
        yielded = 0
        with self.db_nodes.iterator(start=key_offset) as it:
            for k, _ in it:
                yield k
                yielded += 1
                if num_nodes is not None and yielded == num_nodes:
                    break

    def get_edge_keys_generator(self, num_edges = None, key_offset = None):
        """Yield edge keys from the edge database."""
        yielded = 0
        with self.db_edges.iterator(start=key_offset) as it:
            for k, _ in it:
                yield k
                yielded += 1
                if num_edges is not None and yielded == num_edges:
                    break


    # -- Specialized methods for nodes
    def put_node(self, node_id: bytes, value: bytes):
        """Store a serialized node by byte key."""
        self.db_nodes.put(node_id,value)

    def get_node(self, node_id: bytes) -> bytes:
        """Return serialized node bytes by key, or ``None``."""
        return self.db_nodes.get(node_id)

    def delete_node(self, node_id: bytes):
        """Delete a node by byte key."""
        self.db_nodes.delete(node_id)

    # -- Specialized methods for edges
    def put_edge(self, edge_id: bytes, value: bytes):
        """Store a serialized edge by byte key."""
        self.db_edges.put(edge_id, value)

    def get_edge(self, edge_id: bytes) -> bytes:
        """Return serialized edge bytes by key, or ``None``."""
        return self.db_edges.get(edge_id)

    def delete_edge(self, edge_id: str):
        """Delete an edge by byte key."""
        self.db_edges.delete(edge_id)

    def put_nodes_bulk(self, keys_and_values: dict[bytes, bytes]):
        """Use a WriteBatch for atomic bulk updates."""
        with self.db_nodes.write_batch() as wb:
            for node_id, val in keys_and_values.items():
                # s = node_id.decode()
                # wb.put(f"N:{s}".encode('utf-8'), val)
                wb.put(node_id, val)

    def get_nodes_bulk(self, node_ids: list[bytes]) -> dict[bytes, bytes]:
        """Return serialized nodes for the requested keys."""
        results = {}
        for node_id in node_ids:
            data = self.db_nodes.get(node_id)
            if data is not None:
                results[node_id] = data
        return results
    
    def put_edges_bulk(self, keys_and_values: dict[bytes, bytes]):
        """Store many serialized edges in one write batch."""
        with self.db_edges.write_batch() as wb:
            for edge_id, val in keys_and_values.items():
                wb.put(edge_id, val)
    
    def get_edges_bulk(self, edge_ids: list[bytes]) -> dict[bytes, bytes]:
        """Return serialized edges for the requested keys."""
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
        """Store a serialized adjacency list for a node."""
        self.db_adj.put(node_id, value)

    def get_adjacency(self, node_id: bytes) -> Optional[bytes]:
        """Return a serialized adjacency list for a node."""
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
        """Return serialized adjacency lists for the requested nodes."""
        results = {}
        for node_id in node_ids:
            # s = node_id.decode()
            # key = f"A:{s}".encode('utf-8')
            data = self.db_adj.get(node_id)
            if data is not None:
                results[node_id] = data
        return results

    # ----- Typed Adjacency Methods -----
    def put_typed_adjacency(self, source_id: bytes, target_id: bytes, edge_type: str, edge_id: bytes):
        """Store forward and reverse typed adjacency records."""
        with self.db_typed_adj.write_batch() as wb:
            wb.put(_typed_adjacency_key("out", source_id, edge_type, edge_id), target_id)
            wb.put(_typed_adjacency_key("in", target_id, edge_type, edge_id), source_id)

    def put_typed_adjacency_bulk(self, records: list[tuple[bytes, bytes, str, bytes]]):
        """Store many typed adjacency records in one write batch."""
        with self.db_typed_adj.write_batch() as wb:
            for source_id, target_id, edge_type, edge_id in records:
                wb.put(_typed_adjacency_key("out", source_id, edge_type, edge_id), target_id)
                wb.put(_typed_adjacency_key("in", target_id, edge_type, edge_id), source_id)

    def delete_typed_adjacency(self, source_id: bytes, target_id: bytes, edge_type: str, edge_id: bytes):
        """Delete forward and reverse typed adjacency records."""
        with self.db_typed_adj.write_batch() as wb:
            wb.delete(_typed_adjacency_key("out", source_id, edge_type, edge_id))
            wb.delete(_typed_adjacency_key("in", target_id, edge_type, edge_id))

    def iter_typed_adjacency(self, node_id: bytes, edge_type: str, direction: str = "out"):
        """Yield typed adjacency ``(edge_id, neighbor_id)`` pairs."""
        prefix = _typed_adjacency_prefix(direction, node_id, edge_type)
        with self.db_typed_adj.iterator(prefix=prefix) as it:
            for key, neighbor_id in it:
                edge_id = key[len(prefix):]
                yield edge_id, neighbor_id

    def close(self):
        """Close all LevelDB sub-databases."""
        self.db_typed_adj.close()
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
        """Initialize an index counter helper.

        Args:
            dbenv: LMDB environment.
            db_path: Named LMDB database for the counter mapping.
        """
        self.db_path = db_path
        self.env = dbenv
        self.kvdb = self.env.open_db(self.db_path)        
        self.max_key_idx = None
        
    def get_num_keys(self):
        """Return the number of keys already assigned."""
        _b = self.get('num_keys'.encode('utf-8'))
        if _b is None:
            return 0
        num_keys = struct.unpack('<L',_b)[0]
        return num_keys
    
    def put_num_keys(self, num_keys):
        """Persist the number of keys already assigned."""
        num_keys_bytes = struct.pack('<L',num_keys)
        _b = self.put('num_keys'.encode('utf-8'), num_keys_bytes)

    def put(self, key : bytes, value : bytes):
        """Store a counter metadata key/value pair."""
        with self.env.begin(write=True, db=self.kvdb) as txn:
            txn.put(key, value)

    def get(self, key):
        """Read a counter metadata value by key."""
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
        """Return the stored encoded key bytes for a user key."""
        v = self.get(key.encode('utf-8'))
