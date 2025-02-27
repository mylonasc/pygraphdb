import lmdb
import plyvel
from typing import Optional, Dict, List
import os


def _pack_long_int(int_val):
    return struct.pack('<L', int_val)

def _unpack_long_int(int_val):
    return struct.unpack('<L', int_val)[0]


class KVStore:
    """Abstract interface for a simple key-value store."""

    # The basic K/V methods:
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
            k_idx = num_keys
            self.put_num_keys(num_keys)
            return num_keys
    def decode_db_key(self, key):
        v = self.get(key.encode('utf-8'))
        


class LMDBStore(KVStore):
    def __init__(self, path='graph_lmdb', map_size=10_485_760, map_id = True, map_keys = False):
        """
        Creates/opens an LMDB environment with three named sub-databases:
          - b'nodes' for node data
          - b'edges' for edge data
          - b'adj'   for adjacency lists
        """
        max_dbs = 3
        if map_keys:
            map_dbs += 2
        self.env = lmdb.open(path, map_size=map_size, subdir=True, max_dbs=max_dbs)
        self.nodes_db = self.env.open_db(b'nodes')
        self.edges_db = self.env.open_db(b'edges')
        self.adj_db   = self.env.open_db(b'adj')        

        if map_keys:
            self.node_key_encdec_db = self.env.open_db(b'node_key_db')
            self.edge_key_encdec_db = self.env.open_db(b'edge_key_db')
            self.node_key_encdec = SimpleKV(b'node_key_db')
            self.node_key_encdec = SimpleKV(b'edge_key_db')
            


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
        with self.env.begin(write=False, db=self.nodes_db) as txn:
            cursor = txn.cursor()
            if not cursor.set_range(start_key):
                return
            for k, v in cursor:
                if k > end_key:
                    break
                yield k, v

    def close(self):
        self.env.close()

    # -- Specialized node methods
    def put_node(self, node_id: str, value: bytes):
        with self.env.begin(write=True, db=self.nodes_db) as txn:
            txn.put(node_id.encode('utf-8'), value)

    def get_node(self, node_id: str) -> bytes:
        with self.env.begin(write=False, db=self.nodes_db) as txn:
            return txn.get(node_id.encode('utf-8'))

    def delete_node(self, node_id: str):
        with self.env.begin(write=True, db=self.nodes_db) as txn:
            txn.delete(node_id.encode('utf-8'))

    # -- Specialized edge methods
    def put_edge(self, edge_id: str, value: bytes):
        with self.env.begin(write=True, db=self.edges_db) as txn:
            txn.put(edge_id.encode('utf-8'), value)

    def get_edge(self, edge_id: str) -> bytes:
        with self.env.begin(write=False, db=self.edges_db) as txn:
            return txn.get(edge_id.encode('utf-8'))

    def delete_edge(self, edge_id: str):
        with self.env.begin(write=True, db=self.edges_db) as txn:
            txn.delete(edge_id.encode('utf-8'))

    def put_nodes_bulk(self, keys_and_values: dict[str, bytes]):
        """Write a batch of nodes in a single transaction."""
        with self.env.begin(write=True, db=self.nodes_db) as txn:
            for node_id, val in keys_and_values.items():
                txn.put(node_id.encode('utf-8'), val)
    
    def get_nodes_bulk(self, node_ids: list[str]) -> dict[str, bytes]:
        """Retrieve multiple nodes in one read transaction."""
        results = {}
        with self.env.begin(write=False, db=self.nodes_db) as txn:
            for node_id in node_ids:
                data = txn.get(node_id.encode('utf-8'))
                if data is not None:
                    results[node_id] = data
        return results
    
    def put_edges_bulk(self, keys_and_values: dict[str, bytes]):
        with self.env.begin(write=True, db=self.edges_db) as txn:
            for edge_id, val in keys_and_values.items():
                txn.put(edge_id.encode('utf-8'), val)
    
    def get_edges_bulk(self, edge_ids: list[str]) -> dict[str, bytes]:
        results = {}
        with self.env.begin(write=False, db=self.edges_db) as txn:
            for edge_id in edge_ids:
                data = txn.get(edge_id.encode('utf-8'))
                if data is not None:
                    results[edge_id] = data
        return results
    
    # ----- Adjacency Methods -----
    def put_adjacency(self, node_id: str, value: bytes) -> None:
        with self.env.begin(write=True, db=self.adj_db) as txn:
            txn.put(node_id.encode('utf-8'), value)

    def get_adjacency(self, node_id: str) -> Optional[bytes]:
        with self.env.begin(write=False, db=self.adj_db) as txn:
            return txn.get(node_id.encode('utf-8'))

    # -------------------------------------------------------------------------
    # Bulk Write: Adjacency
    # -------------------------------------------------------------------------
    def put_adjacency_bulk(self, adj_dict: Dict[str, bytes]) -> None:
        """
        Insert/update multiple adjacency lists in one transaction.
        :param adj_dict: a dict mapping node_id -> serialized adjacency (list of edges)
        """
        with self.env.begin(write=True, db=self.adj_db) as txn:
            for node_id, val in adj_dict.items():
                txn.put(node_id.encode('utf-8'), val)

    # -------------------------------------------------------------------------
    # Bulk Read: Adjacency
    # -------------------------------------------------------------------------
    def get_adjacency_bulk(self, node_ids: List[str]) -> Dict[str, bytes]:
        """
        Retrieve multiple adjacency lists in a single read transaction.
        Returns a dict { node_id: serialized adjacency } for all found items.
        """
        results = {}
        with self.env.begin(write=False, db=self.adj_db) as txn:
            for node_id in node_ids:
                data = txn.get(node_id.encode('utf-8'))
                if data is not None:
                    results[node_id] = data
        return results


# =========================================
# LevelDB Implementation
# =========================================

class LevelDBStore(KVStore):
    def __init__(self, path='graph_leveldb'):
        """Create or open a LevelDB store. We'll store nodes/edges by prefix."""
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
        self.db = plyvel.DB(path, create_if_missing=True)

    def put(self, key: bytes, value: bytes):
        self.db.put(key, value)

    def get(self, key: bytes) -> bytes:
        return self.db.get(key)

    def delete(self, key: bytes):
        self.db.delete(key)

    def range_iter(self, start_key: bytes, end_key: bytes):
        with self.db.iterator(start=start_key, stop=end_key) as it:
            for k, v in it:
                yield k, v

    def close(self):
        self.db.close()

    # -- Specialized methods for nodes
    def put_node(self, node_id: str, value: bytes):
        key = f"N:{node_id}".encode('utf-8')
        self.db.put(key, value)

    def get_node(self, node_id: str) -> bytes:
        key = f"N:{node_id}".encode('utf-8')
        return self.db.get(key)

    def delete_node(self, node_id: str):
        key = f"N:{node_id}".encode('utf-8')
        self.db.delete(key)

    # -- Specialized methods for edges
    def put_edge(self, edge_id: str, value: bytes):
        key = f"E:{edge_id}".encode('utf-8')
        self.db.put(key, value)

    def get_edge(self, edge_id: str) -> bytes:
        key = f"E:{edge_id}".encode('utf-8')
        return self.db.get(key)

    def delete_edge(self, edge_id: str):
        key = f"E:{edge_id}".encode('utf-8')
        self.db.delete(key)

    def put_nodes_bulk(self, keys_and_values: dict[str, bytes]):
        """Use a WriteBatch for atomic bulk updates."""
        with self.db.write_batch() as wb:
            for node_id, val in keys_and_values.items():
                wb.put(f"N:{node_id}".encode('utf-8'), val)
    
    def get_nodes_bulk(self, node_ids: list[str]) -> dict[str, bytes]:
        results = {}
        for node_id in node_ids:
            key = f"N:{node_id}".encode('utf-8')
            data = self.db.get(key)
            if data is not None:
                results[node_id] = data
        return results
    
    def put_edges_bulk(self, keys_and_values: dict[str, bytes]):
        with self.db.write_batch() as wb:
            for edge_id, val in keys_and_values.items():
                wb.put(f"E:{edge_id}".encode('utf-8'), val)
    
    def get_edges_bulk(self, edge_ids: list[str]) -> dict[str, bytes]:
        results = {}
        for edge_id in edge_ids:
            key = f"E:{edge_id}".encode('utf-8')
            data = self.db.get(key)
            if data is not None:
                results[edge_id] = data
        return results
    

    # ----- Adjacency Methods -----
    def put_adjacency(self, node_id: str, value: bytes) -> None:
        key = f"A:{node_id}".encode('utf-8')
        self.db.put(key, value)

    def get_adjacency(self, node_id: str) -> Optional[bytes]:
        key = f"A:{node_id}".encode('utf-8')
        return self.db.get(key)

    
    # -------------------------------------------------------------------------
    # Bulk Write: Adjacency
    # -------------------------------------------------------------------------
    def put_adjacency_bulk(self, adj_dict: Dict[str, bytes]) -> None:
        """
        Insert/update multiple adjacency lists in one write batch.
        :param adj_dict: a dict mapping node_id -> serialized adjacency
        """
        with self.db.write_batch() as wb:
            for node_id, val in adj_dict.items():
                key = f"A:{node_id}".encode('utf-8')
                wb.put(key, val)

    # -------------------------------------------------------------------------
    # Bulk Read: Adjacency
    # -------------------------------------------------------------------------
    def get_adjacency_bulk(self, node_ids: List[str]) -> Dict[str, bytes]:
        results = {}
        for node_id in node_ids:
            key = f"A:{node_id}".encode('utf-8')
            data = self.db.get(key)
            if data is not None:
                results[node_id] = data
        return results


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
