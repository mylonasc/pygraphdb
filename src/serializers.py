# =========================================
# 1) Serializer Interfaces and Implementations
# =========================================
import pickle
import json
import msgpack

class Serializer:
    """Abstract serializer interface for graph records.

    Implementations convert serializer-friendly dictionaries to bytes and back.
    GraphDB uses this interface for nodes, edges, and adjacency records.
    """

    def serialize(self, obj: dict) -> bytes:
        """Serialize a dictionary to bytes.

        Args:
            obj: Dictionary to serialize.

        Returns:
            Serialized bytes.

        Raises:
            NotImplementedError: Always raised by the abstract base class.
        """
        raise NotImplementedError
    
    def deserialize(self, data: bytes) -> dict:
        """Deserialize bytes into a dictionary.

        Args:
            data: Serialized bytes.

        Returns:
            Deserialized dictionary.

        Raises:
            NotImplementedError: Always raised by the abstract base class.
        """
        raise NotImplementedError


class PickleSerializer(Serializer):
    """Serializer backed by Python's pickle module.

    Pickle supports arbitrary Python objects, which is convenient for tests and
    trusted local data. Do not use it for untrusted bytes.

    Example:
        >>> serializer = PickleSerializer()
        >>> serializer.deserialize(serializer.serialize({"name": "Alice"}))
        {'name': 'Alice'}
    """

    def serialize(self, obj: dict) -> bytes:
        """Serialize a dictionary with ``pickle.dumps``.

        Args:
            obj: Dictionary to serialize.

        Returns:
            Pickle-encoded bytes.
        """
        return pickle.dumps(obj)
    
    def deserialize(self, data: bytes) -> dict:
        """Deserialize bytes with ``pickle.loads``.

        Args:
            data: Pickle-encoded bytes.

        Returns:
            Deserialized dictionary.
        """
        return pickle.loads(data)


class MessagePackSerializer(Serializer):
    """Serializer backed by MessagePack.

    MessagePack is a compact, well-supported binary format that safely handles
    standard data types without executing code during deserialization. It is the
    recommended serializer for persistent graph data.

    Example:
        >>> serializer = MessagePackSerializer()
        >>> serializer.deserialize(serializer.serialize({"name": "Alice"}))
        {'name': 'Alice'}
    """

    def serialize(self, obj: dict) -> bytes:
        """Serialize a dictionary with MessagePack.

        Args:
            obj: MessagePack-serializable dictionary.

        Returns:
            MessagePack encoded bytes.
        """
        return msgpack.packb(obj, use_bin_type=True)

    def deserialize(self, data: bytes) -> dict:
        """Deserialize MessagePack bytes into a dictionary.

        Args:
            data: MessagePack encoded bytes.

        Returns:
            Deserialized dictionary.
        """
        return msgpack.unpackb(data, raw=False)


class JSONSerializer(Serializer):
    """Serializer backed by JSON.

    JSON is safer and more portable than pickle, but only supports JSON-native
    value types in properties and graph records.

    Example:
        >>> serializer = JSONSerializer()
        >>> serializer.deserialize(serializer.serialize({"name": "Alice"}))
        {'name': 'Alice'}
    """

    def serialize(self, obj: dict) -> bytes:
        """Serialize a dictionary with ``json.dumps`` and UTF-8 encoding.

        Args:
            obj: JSON-serializable dictionary.

        Returns:
            UTF-8 encoded JSON bytes.
        """
        return json.dumps(obj).encode('utf-8')
    
    def deserialize(self, data: bytes) -> dict:
        """Deserialize UTF-8 JSON bytes into a dictionary.

        Args:
            data: UTF-8 encoded JSON bytes.

        Returns:
            Deserialized dictionary.
        """
        return json.loads(data.decode('utf-8'))
