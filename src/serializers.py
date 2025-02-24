# =========================================
# 1) Serializer Interfaces and Implementations
# =========================================
import pickle
import json

class Serializer:
    """Abstract base for serialization/deserialization."""
    def serialize(self, obj: dict) -> bytes:
        raise NotImplementedError
    
    def deserialize(self, data: bytes) -> dict:
        raise NotImplementedError


class PickleSerializer(Serializer):
    """Uses Python's pickle for serialization."""
    def serialize(self, obj: dict) -> bytes:
        return pickle.dumps(obj)
    
    def deserialize(self, data: bytes) -> dict:
        return pickle.loads(data)


class JSONSerializer(Serializer):
    """Uses JSON for serialization."""
    def serialize(self, obj: dict) -> bytes:
        return json.dumps(obj).encode('utf-8')
    
    def deserialize(self, data: bytes) -> dict:
        return json.loads(data.decode('utf-8'))