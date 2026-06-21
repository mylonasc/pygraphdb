# =========================================
# 1) Serializer Interfaces and Implementations
# =========================================
import pickle
import json
import base64


def _missing_dependency_error(package_name, install_name=None, feature_name=None):
    install_name = install_name or package_name
    feature_name = feature_name or package_name
    return ImportError(
        f"Missing optional dependency '{package_name}' required for {feature_name}. "
        f"Install it with `python -m pip install {install_name}` or `uv add {install_name}`."
    )

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


class MessagePackSerializer(Serializer):
    """Uses MessagePack for serialization."""
    def serialize(self, obj: dict) -> bytes:
        try:
            import msgpack
        except ImportError as exc:
            raise _missing_dependency_error("msgpack", feature_name="MessagePackSerializer") from exc
        return msgpack.packb(obj, use_bin_type=True)

    def deserialize(self, data: bytes) -> dict:
        try:
            import msgpack
        except ImportError as exc:
            raise _missing_dependency_error("msgpack", feature_name="MessagePackSerializer") from exc
        return msgpack.unpackb(data, raw=False)


class ProtobufSerializer(Serializer):
    """Uses google.protobuf Struct for JSON-like dictionaries.

    Struct does not have native integer or bytes types. This serializer tags those
    values before encoding so Python dictionaries round-trip without losing them.
    """

    _TYPE_KEY = "__pygraphdb_type__"
    _VALUE_KEY = "value"

    def serialize(self, obj: dict) -> bytes:
        try:
            from google.protobuf import json_format, struct_pb2
        except ImportError as exc:
            raise _missing_dependency_error("protobuf", feature_name="ProtobufSerializer") from exc

        message = struct_pb2.Struct()
        json_format.ParseDict(self._to_struct_compatible(obj), message)
        return message.SerializeToString()

    def deserialize(self, data: bytes) -> dict:
        try:
            from google.protobuf import json_format, struct_pb2
        except ImportError as exc:
            raise _missing_dependency_error("protobuf", feature_name="ProtobufSerializer") from exc

        message = struct_pb2.Struct()
        message.ParseFromString(data)
        return self._from_struct_compatible(json_format.MessageToDict(message))

    def _to_struct_compatible(self, obj):
        if isinstance(obj, bytes):
            return {
                self._TYPE_KEY: "bytes",
                self._VALUE_KEY: base64.b64encode(obj).decode("ascii"),
            }
        if isinstance(obj, int) and not isinstance(obj, bool):
            return {
                self._TYPE_KEY: "int",
                self._VALUE_KEY: str(obj),
            }
        if isinstance(obj, dict):
            return {key: self._to_struct_compatible(value) for key, value in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [self._to_struct_compatible(value) for value in obj]
        return obj

    def _from_struct_compatible(self, obj):
        if isinstance(obj, dict):
            if set(obj) == {self._TYPE_KEY, self._VALUE_KEY}:
                value_type = obj[self._TYPE_KEY]
                value = obj[self._VALUE_KEY]
                if value_type == "bytes":
                    return base64.b64decode(value.encode("ascii"))
                if value_type == "int":
                    return int(value)
            return {key: self._from_struct_compatible(value) for key, value in obj.items()}
        if isinstance(obj, list):
            return [self._from_struct_compatible(value) for value in obj]
        return obj
