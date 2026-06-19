import datetime
import struct
import uuid
from collections import deque
from typing import Callable, Iterable, Iterator, Optional, Union

from kvstores import KVStore
from serializers import Serializer


def datetime_to_bytes(dt: datetime.datetime, tzinfo=datetime.timezone.utc) -> bytes:
    """Encode a datetime as sortable big-endian microseconds since Unix epoch.

    Args:
        dt: Datetime to encode. It must not be earlier than 1970-01-01 for the
            unsigned 64-bit representation to be valid.
        tzinfo: Timezone used for the Unix epoch reference.

    Returns:
        An 8-byte big-endian representation that sorts chronologically.

    Example:
        >>> encoded = datetime_to_bytes(datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc))
        >>> bytes_to_datetime(encoded)
        datetime.datetime(2024, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)
    """
    epoch = datetime.datetime(1970, 1, 1, tzinfo=tzinfo)
    delta = dt - epoch
    microseconds = int(delta.total_seconds() * 1_000_000)
    return struct.pack(">Q", microseconds)


def bytes_to_datetime(b: bytes, tzinfo=datetime.timezone.utc) -> datetime.datetime:
    """Decode bytes produced by :func:`datetime_to_bytes`.

    Args:
        b: Eight bytes containing big-endian microseconds since Unix epoch.
        tzinfo: Timezone used for the returned datetime.

    Returns:
        The decoded datetime.
    """
    epoch = datetime.datetime(1970, 1, 1, tzinfo=tzinfo)
    (microseconds,) = struct.unpack(">Q", b)
    return epoch + datetime.timedelta(microseconds=microseconds)


class GraphDBError(Exception):
    """Base exception for graph database errors."""


class NodeNotFoundError(GraphDBError):
    """Raised when an operation references a missing node."""


class EdgeNotFoundError(GraphDBError):
    """Raised when an operation references a missing edge."""


class ConstraintError(GraphDBError):
    """Raised when an operation would violate graph integrity."""


class Node:
    """A property-graph node.

    Nodes have a stable string ID, zero or more labels, and arbitrary property
    values supported by the configured serializer.

    Args:
        node_id: Optional external ID. If omitted, a UUID string is generated.
        labels: Optional iterable of labels, such as ``"Person"`` or
            ``"Company"``.
        properties: Optional mapping of property names to values.

    Example:
        >>> node = Node("alice", labels=["Person"], properties={"age": 30})
        >>> node.get_id
        'alice'
        >>> "Person" in node.labels
        True
    """

    def __init__(self, node_id=None, labels: Optional[Iterable[str]] = None, properties=None):
        self._id = str(node_id or uuid.uuid4())
        self.labels = frozenset(labels or [])
        self.properties = dict(properties or {})

    @property
    def get_id(self):
        return self._id

    @property
    def get_id_bytes(self):
        return self._id.encode("utf-8")

    def to_dict(self):
        """Convert the node to a serializer-friendly dictionary.

        Returns:
            A dictionary containing ``id``, sorted ``labels``, and ``properties``.
        """
        return {
            "id": self._id,
            "labels": sorted(self.labels),
            "properties": self.properties,
        }

    @classmethod
    def from_dict(cls, data: dict):
        """Create a node from a serialized dictionary.

        Args:
            data: Dictionary produced by :meth:`to_dict`.

        Returns:
            A reconstructed node.
        """
        return cls(
            node_id=data["id"],
            labels=data.get("labels", []),
            properties=data.get("properties", {}),
        )


class Edge:
    """A directed property-graph edge.

    Edges connect a source node to a target node and may have a type and
    arbitrary properties. The graph validates endpoint existence when the edge
    is inserted unless validation is explicitly disabled.

    Args:
        edge_id: Optional external ID. If omitted, a UUID string is generated.
        source: Source node ID or :class:`Node` instance.
        target: Target node ID or :class:`Node` instance.
        type: Optional relationship type, such as ``"KNOWS"``.
        properties: Optional mapping of property names to values.

    Example:
        >>> edge = Edge("e1", source="alice", target="bob", type="KNOWS")
        >>> edge.source, edge.target, edge.type
        ('alice', 'bob', 'KNOWS')
    """

    def __init__(self, edge_id=None, source=None, target=None, type: Optional[str] = None, properties=None):
        self._id = str(edge_id or uuid.uuid4())
        self.source = _id_to_str(source) if source is not None else None
        self.target = _id_to_str(target) if target is not None else None
        self.type = type
        self.properties = dict(properties or {})

    @property
    def get_id(self):
        return self._id

    @property
    def get_id_bytes(self):
        return self._id.encode("utf-8")

    def to_dict(self):
        """Convert the edge to a serializer-friendly dictionary.

        Returns:
            A dictionary containing ID, endpoints, type, and properties.
        """
        return {
            "id": self._id,
            "source": self.source,
            "target": self.target,
            "type": self.type,
            "properties": self.properties,
        }

    @classmethod
    def from_dict(cls, data: dict):
        """Create an edge from a serialized dictionary.

        Args:
            data: Dictionary produced by :meth:`to_dict`.

        Returns:
            A reconstructed edge.
        """
        return cls(
            edge_id=data["id"],
            source=data["source"],
            target=data["target"],
            type=data.get("type"),
            properties=data.get("properties", {}),
        )


class TimeIndexedEdge(Edge):
    """Edge whose storage key is prefixed by a sortable timestamp.

    This is a low-level helper for time-ordered edge storage. It is not used by
    the default graph APIs for temporal indexing yet.

    Args:
        timestamp_dat: Datetime used as the key prefix.
        *args: Positional arguments forwarded to :class:`Edge`.
        **kwargs: Keyword arguments forwarded to :class:`Edge`.

    Example:
        >>> ts = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
        >>> edge = TimeIndexedEdge(ts, edge_id="e1", source="a", target="b")
        >>> edge.get_id_bytes.startswith(datetime_to_bytes(ts))
        True
    """

    def __init__(self, timestamp_dat, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.timestamp_dat = timestamp_dat

    @property
    def get_id_bytes(self):
        return datetime_to_bytes(self.timestamp_dat) + b":" + self.get_id.encode("utf-8")

    def to_dict(self):
        data = super().to_dict()
        data["timestamp_dat"] = self.timestamp_dat
        return data

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            timestamp_dat=data["timestamp_dat"],
            edge_id=data["id"],
            source=data["source"],
            target=data["target"],
            type=data.get("type"),
            properties=data.get("properties", {}),
        )


class GraphEntityDictSerializer:
    """Adapter that serializes graph entities through a dictionary format.

    Args:
        serializer: Concrete serializer used for the final bytes conversion.

    Example:
        >>> from serializers import PickleSerializer
        >>> adapter = GraphEntityDictSerializer(PickleSerializer())
        >>> raw = adapter.serialize(Node("a"), "Node")
        >>> adapter.deserialize(raw, "Node").get_id
        'a'
    """

    _ent_type_encoder = {
        "Edge": lambda x: x.to_dict(),
        "TimeIndexedEdge": lambda x: x.to_dict(),
        "Node": lambda x: x.to_dict(),
        "AdjacencyList": lambda x: {k: sorted(v) for k, v in x.items()},
    }

    _ent_type_decoder = {
        "Edge": lambda x: Edge.from_dict(x),
        "TimeIndexedEdge": lambda x: TimeIndexedEdge.from_dict(x),
        "Node": lambda x: Node.from_dict(x),
        "AdjacencyList": lambda x: {"out": set(x.get("out", [])), "in": set(x.get("in", []))},
    }

    def __init__(self, serializer: Serializer):
        self.serializer = serializer

    def serialize(self, entity, entity_type: str):
        """Serialize a supported graph entity.

        Args:
            entity: Entity instance or adjacency dictionary to serialize.
            entity_type: One of ``"Node"``, ``"Edge"``, ``"TimeIndexedEdge"``,
                or ``"AdjacencyList"``.

        Returns:
            Serialized bytes.
        """
        return self.serializer.serialize(self._ent_type_encoder[entity_type](entity))

    def deserialize(self, val, entity_type: str):
        """Deserialize bytes into a supported graph entity.

        Args:
            val: Serialized bytes.
            entity_type: Type discriminator used to reconstruct the value.

        Returns:
            Reconstructed graph entity or adjacency dictionary.
        """
        return self._ent_type_decoder[entity_type](self.serializer.deserialize(val))


def _id_to_str(value) -> str:
    if isinstance(value, Node) or isinstance(value, Edge):
        return value.get_id
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _id_to_key(value) -> bytes:
    if isinstance(value, bytes):
        return value
    return _id_to_str(value).encode("utf-8")


class GraphDB:
    """Property graph API over a key-value graph store.

    Public APIs accept and return string IDs. Bytes are accepted for backward
    compatibility, but are normalized at the boundary.

    Args:
        store: Storage backend implementing :class:`kvstores.KVStore`.
        serializer: Serializer used for nodes, edges, and adjacency records.

    Example:
        >>> from kvstores import InMemoryKVStore
        >>> from serializers import PickleSerializer
        >>> graph = GraphDB(InMemoryKVStore(), PickleSerializer())
        >>> graph.put_node(Node("alice", labels=["Person"]))
        <graphdb.Node object at ...>
        >>> graph.put_node(Node("bob", labels=["Person"]))
        <graphdb.Node object at ...>
        >>> graph.put_edge(Edge("e1", source="alice", target="bob", type="KNOWS"))
        <graphdb.Edge object at ...>
        >>> graph.neighbors("alice")
        ['bob']
    """

    def __init__(self, store: KVStore, serializer: Serializer):
        self.store = store
        self.serializer = serializer
        self.entity_serializer = GraphEntityDictSerializer(serializer)

    def put_node(self, node: Node):
        """Insert or replace a node.

        Replacing a node updates any backend-maintained node indexes. Existing
        incident edges are preserved.

        Args:
            node: Node to store.

        Returns:
            The stored node.

        Example:
            >>> graph = GraphDB(InMemoryKVStore(), PickleSerializer())
            >>> graph.put_node(Node("alice", labels=["Person"]))
            <graphdb.Node object at ...>
            >>> graph.get_node("alice").labels
            frozenset({'Person'})
        """
        with self.store.write_transaction():
            old_node = self.get_node(node.get_id)
            self.store.put_node(node.get_id_bytes, self.entity_serializer.serialize(node, "Node"))
            if hasattr(self.store, "index_node"):
                self.store.index_node(node, old_node=old_node)
            return node

    add_node = put_node

    def get_node(self, node_id) -> Optional[Node]:
        """Fetch a node by ID.

        Args:
            node_id: Node ID as ``str`` or UTF-8 ``bytes``.

        Returns:
            The node, or ``None`` when it does not exist.
        """
        with self.store.read_transaction():
            data = self.store.get_node(_id_to_key(node_id))
            return self.entity_serializer.deserialize(data, "Node") if data is not None else None

    def has_node(self, node_id) -> bool:
        """Return true if a node exists."""
        return self.get_node(node_id) is not None

    def require_node(self, node_id) -> Node:
        """Fetch a node or raise if it is missing.

        Args:
            node_id: Node ID as ``str`` or UTF-8 ``bytes``.

        Returns:
            The existing node.

        Raises:
            NodeNotFoundError: If the node does not exist.
        """
        node = self.get_node(node_id)
        if node is None:
            raise NodeNotFoundError(f"node not found: {_id_to_str(node_id)}")
        return node

    def update_node(self, node_id, properties: Optional[dict] = None, labels: Optional[Iterable[str]] = None) -> Node:
        """Update a node's properties and optionally replace its labels.

        Args:
            node_id: Node to update.
            properties: Properties to merge into the existing property mapping.
            labels: Replacement label set. Pass ``None`` to leave labels
                unchanged.

        Returns:
            The updated node.

        Raises:
            NodeNotFoundError: If the node does not exist.
        """
        node = self.require_node(node_id)
        if properties:
            node.properties.update(properties)
        if labels is not None:
            node.labels = frozenset(labels)
        return self.put_node(node)

    def add_label(self, node_id, label: str) -> Node:
        """Add one label to a node and update indexes."""
        node = self.require_node(node_id)
        node.labels = frozenset(set(node.labels) | {label})
        return self.put_node(node)

    def remove_label(self, node_id, label: str) -> Node:
        """Remove one label from a node and update indexes."""
        node = self.require_node(node_id)
        node.labels = frozenset(set(node.labels) - {label})
        return self.put_node(node)

    def rename_label(self, old_label: str, new_label: str) -> int:
        """Rename a node label across all matching nodes.

        Returns:
            Number of updated nodes.
        """
        updated = 0
        with self.store.write_transaction():
            for node in list(self.find_nodes(labels=[old_label])):
                labels = set(node.labels)
                labels.discard(old_label)
                labels.add(new_label)
                node.labels = frozenset(labels)
                self.put_node(node)
                updated += 1
        return updated

    def node_properties(self, node_id) -> dict:
        """Return a copy of a node's property mapping."""
        return dict(self.require_node(node_id).properties)

    def set_node_property(self, node_id, key: str, value) -> Node:
        """Set one node property and update indexes."""
        node = self.require_node(node_id)
        node.properties[key] = value
        return self.put_node(node)

    def remove_node_property(self, node_id, key: str) -> Node:
        """Remove one node property if present and update indexes."""
        node = self.require_node(node_id)
        node.properties.pop(key, None)
        return self.put_node(node)

    def delete_node(self, node_id, mode: str = "restrict") -> None:
        """Delete a node using explicit incident-edge semantics.

        Args:
            node_id: Node to delete.
            mode: Deletion policy. ``"restrict"`` raises if incident edges
                exist. ``"detach"`` and ``"cascade"`` delete incident edges
                before deleting the node.

        Raises:
            ConstraintError: If ``mode="restrict"`` and incident edges exist.
            NodeNotFoundError: If the node does not exist.
            ValueError: If ``mode`` is not supported.

        Example:
            >>> graph = GraphDB(InMemoryKVStore(), PickleSerializer())
            >>> graph.put_node(Node("a")); graph.put_node(Node("b"))
            <graphdb.Node object at ...>
            <graphdb.Node object at ...>
            >>> graph.put_edge(Edge("e1", "a", "b"))
            <graphdb.Edge object at ...>
            >>> graph.delete_node("a", mode="detach")
            >>> graph.get_edge("e1") is None
            True
        """
        with self.store.write_transaction():
            node_id_str = _id_to_str(node_id)
            self.require_node(node_id_str)
            incident = self.incident_edges(node_id_str)
            if incident and mode == "restrict":
                raise ConstraintError(f"node has incident edges: {node_id_str}")
            if mode not in {"restrict", "detach", "cascade"}:
                raise ValueError("mode must be 'restrict', 'detach', or 'cascade'")
            if mode in {"detach", "cascade"}:
                for edge in list(incident):
                    self.delete_edge(edge.get_id)
            if hasattr(self.store, "unindex_node"):
                self.store.unindex_node(self.require_node(node_id_str))
            self.store.delete_node(_id_to_key(node_id_str))
            self._put_adjacency(node_id_str, {"out": set(), "in": set()})

    def put_nodes(self, nodes: Iterable[Node]):
        """Insert or replace multiple nodes.

        Args:
            nodes: Iterable of nodes to store.

        Note:
            Nodes are currently written through :meth:`put_node` so index
            maintenance and replacement behavior are identical to single-node
            writes.
        """
        for node in nodes:
            self.put_node(node)

    def get_nodes(self, node_ids: Iterable) -> list[Optional[Node]]:
        """Fetch multiple nodes while preserving requested order.

        Args:
            node_ids: Iterable of node IDs.

        Returns:
            List containing each node or ``None`` for missing IDs.
        """
        keys = [_id_to_key(node_id) for node_id in node_ids]
        raw = self.store.get_nodes_bulk(keys)
        return [self.entity_serializer.deserialize(raw[key], "Node") if key in raw else None for key in keys]

    def put_edge(self, edge: Edge, validate: bool = True):
        """Insert or replace an edge inside one backend write transaction."""
        with self.store.write_transaction():
            return self._put_edge(edge, validate=validate)

    def _put_edge(self, edge: Edge, validate: bool = True):
        """Insert or replace an edge and maintain adjacency/index records.

        Edge upserts are idempotent. If an existing edge changes endpoints, old
        adjacency entries are removed before new entries are written. On failure
        during adjacency/index maintenance, best-effort rollback restores prior
        edge and adjacency state.

        Args:
            edge: Edge to store.
            validate: If true, require source and target nodes to exist.

        Returns:
            The stored edge.

        Raises:
            ValueError: If source or target is missing from the edge object.
            NodeNotFoundError: If endpoint validation fails.

        Example:
            >>> graph = GraphDB(InMemoryKVStore(), PickleSerializer())
            >>> graph.put_node(Node("a")); graph.put_node(Node("b"))
            <graphdb.Node object at ...>
            <graphdb.Node object at ...>
            >>> graph.put_edge(Edge("ab", source="a", target="b", type="LINKS"))
            <graphdb.Edge object at ...>
            >>> graph.out_degree("a"), graph.in_degree("b")
            (1, 1)
        """
        if edge.source is None or edge.target is None:
            raise ValueError("edge source and target are required")
        if validate:
            self.require_node(edge.source)
            self.require_node(edge.target)

        existing = self.get_edge(edge.get_id)
        old_adjacency = {
            edge.source: self._get_adjacency(edge.source),
            edge.target: self._get_adjacency(edge.target),
        }
        if existing is not None:
            old_adjacency[existing.source] = self._get_adjacency(existing.source)
            old_adjacency[existing.target] = self._get_adjacency(existing.target)

        try:
            if existing is not None:
                if hasattr(self.store, "remove_adjacency_edge"):
                    self.store.remove_adjacency_edge(existing.source, existing.target, existing.get_id)
                else:
                    self._remove_edge_from_adjacency(existing.source, existing.get_id, "out")
                    self._remove_edge_from_adjacency(existing.target, existing.get_id, "in")

            self.store.put_edge(edge.get_id_bytes, self.entity_serializer.serialize(edge, "Edge"))
            if hasattr(self.store, "index_edge"):
                self.store.index_edge(edge, old_edge=existing)
            if hasattr(self.store, "add_adjacency_edge"):
                self.store.add_adjacency_edge(edge.source, edge.target, edge.get_id)
            else:
                self._add_edge_to_adjacency(edge.source, edge.get_id, "out")
                self._add_edge_to_adjacency(edge.target, edge.get_id, "in")
        except Exception:
            if hasattr(self.store, "remove_adjacency_edge"):
                self.store.remove_adjacency_edge(edge.source, edge.target, edge.get_id)
            if existing is None:
                self.store.delete_edge(edge.get_id_bytes)
                if hasattr(self.store, "unindex_edge"):
                    self.store.unindex_edge(edge)
            else:
                self.store.put_edge(existing.get_id_bytes, self.entity_serializer.serialize(existing, "Edge"))
                if hasattr(self.store, "index_edge"):
                    self.store.index_edge(existing, old_edge=edge)
                if hasattr(self.store, "add_adjacency_edge"):
                    self.store.add_adjacency_edge(existing.source, existing.target, existing.get_id)
            if not hasattr(self.store, "add_adjacency_edge"):
                for node_id, adjacency in old_adjacency.items():
                    self._put_adjacency(node_id, adjacency)
            raise
        return edge

    add_edge = put_edge

    def get_edge(self, edge_id) -> Optional[Edge]:
        """Fetch an edge by ID.

        Args:
            edge_id: Edge ID as ``str`` or UTF-8 ``bytes``.

        Returns:
            The edge, or ``None`` when it does not exist.
        """
        data = self.store.get_edge(_id_to_key(edge_id))
        return self.entity_serializer.deserialize(data, "Edge") if data is not None else None

    def has_edge(self, edge_id) -> bool:
        """Return true if an edge exists."""
        return self.get_edge(edge_id) is not None

    def require_edge(self, edge_id) -> Edge:
        """Fetch an edge or raise if it is missing.

        Args:
            edge_id: Edge ID as ``str`` or UTF-8 ``bytes``.

        Returns:
            The existing edge.

        Raises:
            EdgeNotFoundError: If the edge does not exist.
        """
        edge = self.get_edge(edge_id)
        if edge is None:
            raise EdgeNotFoundError(f"edge not found: {_id_to_str(edge_id)}")
        return edge

    def update_edge(self, edge_id, properties: Optional[dict] = None, source=None, target=None, type: Optional[str] = None) -> Edge:
        """Update an edge's properties, endpoints, and type.

        Args:
            edge_id: Edge to update.
            properties: Properties to merge into the existing property mapping.
            source: Optional replacement source node ID.
            target: Optional replacement target node ID.
            type: Optional replacement edge type.

        Returns:
            The updated edge.

        Raises:
            EdgeNotFoundError: If the edge does not exist.
            NodeNotFoundError: If a replacement endpoint does not exist.
        """
        edge = self.require_edge(edge_id)
        if properties:
            edge.properties.update(properties)
        if source is not None:
            edge.source = _id_to_str(source)
        if target is not None:
            edge.target = _id_to_str(target)
        if type is not None:
            edge.type = type
        return self.put_edge(edge)

    def edge_properties(self, edge_id) -> dict:
        """Return a copy of an edge's property mapping."""
        return dict(self.require_edge(edge_id).properties)

    def set_edge_property(self, edge_id, key: str, value) -> Edge:
        """Set one edge property and update indexes."""
        edge = self.require_edge(edge_id)
        edge.properties[key] = value
        return self.put_edge(edge)

    def remove_edge_property(self, edge_id, key: str) -> Edge:
        """Remove one edge property if present and update indexes."""
        edge = self.require_edge(edge_id)
        edge.properties.pop(key, None)
        return self.put_edge(edge)

    def rename_edge_type(self, old_type: str, new_type: str) -> int:
        """Rename an edge type across all matching edges.

        Returns:
            Number of updated edges.
        """
        updated = 0
        with self.store.write_transaction():
            for edge in list(self.find_edges(type=old_type)):
                edge.type = new_type
                self.put_edge(edge)
                updated += 1
        return updated

    def delete_edge(self, edge_id) -> None:
        """Delete an edge inside one backend write transaction."""
        with self.store.write_transaction():
            self._delete_edge(edge_id)

    def _delete_edge(self, edge_id) -> None:
        """Delete an edge and remove it from endpoint adjacency lists.

        Missing edges are treated as a no-op. If backend deletion fails after
        adjacency/index changes, best-effort rollback restores the previous
        state.

        Args:
            edge_id: Edge ID as ``str`` or UTF-8 ``bytes``.
        """
        edge = self.get_edge(edge_id)
        if edge is None:
            return
        old_adjacency = {
            edge.source: self._get_adjacency(edge.source),
            edge.target: self._get_adjacency(edge.target),
        }
        try:
            if hasattr(self.store, "remove_adjacency_edge"):
                self.store.remove_adjacency_edge(edge.source, edge.target, edge.get_id)
            else:
                self._remove_edge_from_adjacency(edge.source, edge.get_id, "out")
                self._remove_edge_from_adjacency(edge.target, edge.get_id, "in")
            if hasattr(self.store, "unindex_edge"):
                self.store.unindex_edge(edge)
            self.store.delete_edge(edge.get_id_bytes)
        except Exception:
            self.store.put_edge(edge.get_id_bytes, self.entity_serializer.serialize(edge, "Edge"))
            if hasattr(self.store, "index_edge"):
                self.store.index_edge(edge)
            if hasattr(self.store, "add_adjacency_edge"):
                self.store.add_adjacency_edge(edge.source, edge.target, edge.get_id)
            else:
                for node_id, adjacency in old_adjacency.items():
                    self._put_adjacency(node_id, adjacency)
            raise

    def put_edges_bulk(self, edges: Iterable[Edge], validate: bool = True):
        """Insert or replace multiple edges.

        Args:
            edges: Iterable of edges to store.
            validate: If true, each edge must reference existing endpoints.

        Note:
            Edges are currently written through :meth:`put_edge` to preserve the
            same validation, rollback, and index-maintenance semantics.
        """
        for edge in edges:
            self.put_edge(edge, validate=validate)

    def get_edges(self, edge_ids: Iterable) -> list[Optional[Edge]]:
        """Fetch multiple edges while preserving requested order.

        Args:
            edge_ids: Iterable of edge IDs.

        Returns:
            List containing each edge or ``None`` for missing IDs.
        """
        keys = [_id_to_key(edge_id) for edge_id in edge_ids]
        raw = self.store.get_edges_bulk(keys)
        return [self.entity_serializer.deserialize(raw[key], "Edge") if key in raw else None for key in keys]

    def get_adjacency_list(self, node_id, direction="out", return_raw=False) -> Union[list[str], dict[str, set[str]]]:
        """Return edge IDs adjacent to a node.

        Args:
            node_id: Node ID.
            direction: ``"out"``/``"forward"``, ``"in"``/``"backward"``, or
                ``"both"``/``"any"``.
            return_raw: If true, return the raw ``{"out": set, "in": set}``
                adjacency mapping.

        Returns:
            Sorted edge IDs for the requested direction, or the raw adjacency
            mapping when ``return_raw`` is true.

        Raises:
            ValueError: If ``direction`` is unsupported.
        """
        node_id_str = _id_to_str(node_id)
        if hasattr(self.store, "adjacency_edge_ids"):
            if return_raw:
                return {
                    "out": set(self.store.adjacency_edge_ids(node_id_str, "out")),
                    "in": set(self.store.adjacency_edge_ids(node_id_str, "in")),
                }
            return self.store.adjacency_edge_ids(node_id_str, direction)
        adjacency = self._get_adjacency(node_id)
        if return_raw:
            return adjacency
        if direction in {"out", "forward"}:
            return sorted(adjacency["out"])
        if direction in {"in", "backward"}:
            return sorted(adjacency["in"])
        if direction in {"both", "any"}:
            return sorted(adjacency["out"] | adjacency["in"])
        raise ValueError("direction must be 'out', 'in', or 'both'")

    def out_edges(self, node_id) -> list[Edge]:
        """Return outgoing edges for a node.

        Args:
            node_id: Node ID.

        Returns:
            List of edges whose source is ``node_id``.
        """
        return [edge for edge in self.get_edges(self.get_adjacency_list(node_id, "out")) if edge is not None]

    def in_edges(self, node_id) -> list[Edge]:
        """Return incoming edges for a node.

        Args:
            node_id: Node ID.

        Returns:
            List of edges whose target is ``node_id``.
        """
        return [edge for edge in self.get_edges(self.get_adjacency_list(node_id, "in")) if edge is not None]

    def incident_edges(self, node_id) -> list[Edge]:
        """Return all incoming and outgoing edges for a node.

        Args:
            node_id: Node ID.

        Returns:
            List of unique incident edges.
        """
        return [edge for edge in self.get_edges(self.get_adjacency_list(node_id, "both")) if edge is not None]

    def neighbors(self, node_id, direction="out") -> list[str]:
        """Return neighboring node IDs.

        Args:
            node_id: Node ID.
            direction: ``"out"`` for targets of outgoing edges, ``"in"`` for
                sources of incoming edges, or ``"both"`` for either direction.

        Returns:
            Sorted unique neighboring node IDs.

        Example:
            >>> graph = GraphDB(InMemoryKVStore(), PickleSerializer())
            >>> graph.put_node(Node("a")); graph.put_node(Node("b"))
            <graphdb.Node object at ...>
            <graphdb.Node object at ...>
            >>> graph.put_edge(Edge("e1", "a", "b"))
            <graphdb.Edge object at ...>
            >>> graph.neighbors("a", direction="out")
            ['b']
        """
        node_id_str = _id_to_str(node_id)
        if hasattr(self.store, "neighbor_ids"):
            return self.store.neighbor_ids(node_id_str, direction=direction)
        edges = self.incident_edges(node_id_str) if direction in {"both", "any"} else (
            self.out_edges(node_id_str) if direction in {"out", "forward"} else self.in_edges(node_id_str)
        )
        result = set()
        for edge in edges:
            if direction in {"out", "forward"}:
                result.add(edge.target)
            elif direction in {"in", "backward"}:
                result.add(edge.source)
            else:
                result.add(edge.target if edge.source == node_id_str else edge.source)
        return sorted(result)

    def degree(self, node_id, direction="both") -> int:
        """Return the number of adjacent edges for a node.

        Args:
            node_id: Node ID.
            direction: ``"out"``, ``"in"``, or ``"both"``.

        Returns:
            Number of adjacent edges in the requested direction.
        """
        return len(self.get_adjacency_list(node_id, direction))

    def out_degree(self, node_id) -> int:
        """Return the outgoing degree for a node.

        Args:
            node_id: Node ID.

        Returns:
            Number of outgoing edges.
        """
        return self.degree(node_id, "out")

    def in_degree(self, node_id) -> int:
        """Return the incoming degree for a node.

        Args:
            node_id: Node ID.

        Returns:
            Number of incoming edges.
        """
        return self.degree(node_id, "in")

    def count_nodes(self, labels: Optional[Iterable[str]] = None, properties: Optional[dict] = None) -> int:
        """Count nodes, optionally using label/property filters."""
        if labels or properties:
            return len(self.find_nodes(labels=labels, properties=properties))
        return sum(1 for _ in self.iter_nodes())

    def count_edges(self, type: Optional[str] = None, source=None, target=None, properties: Optional[dict] = None) -> int:
        """Count edges, optionally using type/endpoint/property filters."""
        if type is not None or source is not None or target is not None or properties:
            return len(self.find_edges(type=type, source=source, target=target, properties=properties))
        return sum(1 for _ in self.iter_edges())

    def nodes_by_label(self, label: str) -> list[Node]:
        """Return nodes containing a label."""
        return self.find_nodes(labels=[label])

    def edges_by_type(self, type: str) -> list[Edge]:
        """Return edges with an edge type."""
        return self.find_edges(type=type)

    def bfs(self, start_node_id, direction="out", max_depth: Optional[int] = None) -> list[str]:
        """Traverse reachable nodes with breadth-first search.

        Args:
            start_node_id: Node where traversal starts.
            direction: Edge direction to follow: ``"out"``, ``"in"``, or
                ``"both"``.
            max_depth: Optional maximum hop depth. ``None`` traverses until no
                new reachable nodes remain.

        Returns:
            Node IDs in BFS visit order.

        Raises:
            NodeNotFoundError: If the start node does not exist.

        Example:
            >>> graph = GraphDB(InMemoryKVStore(), PickleSerializer())
            >>> [graph.put_node(Node(node_id)) for node_id in ["a", "b", "c"]]
            [<graphdb.Node object at ...>, <graphdb.Node object at ...>, <graphdb.Node object at ...>]
            >>> graph.put_edge(Edge("ab", "a", "b")); graph.put_edge(Edge("bc", "b", "c"))
            <graphdb.Edge object at ...>
            <graphdb.Edge object at ...>
            >>> graph.bfs("a")
            ['a', 'b', 'c']
        """
        start = _id_to_str(start_node_id)
        self.require_node(start)
        visited = set()
        queue = deque([(start, 0)])
        result = []
        while queue:
            current, depth = queue.popleft()
            if current in visited:
                continue
            visited.add(current)
            result.append(current)
            if max_depth is not None and depth >= max_depth:
                continue
            for neighbor in self.neighbors(current, direction=direction):
                if neighbor not in visited:
                    queue.append((neighbor, depth + 1))
        return result

    def iter_nodes(self) -> Iterator[Node]:
        """Iterate over all nodes in backend key order.

        Yields:
            Stored nodes.
        """
        for key in self.store.get_node_keys_generator():
            node = self.get_node(key)
            if node is not None:
                yield node

    def iter_edges(self) -> Iterator[Edge]:
        """Iterate over all edges in backend key order.

        Yields:
            Stored edges.
        """
        for key in self.store.get_edge_keys_generator():
            edge = self.get_edge(key)
            if edge is not None:
                yield edge

    def find_nodes(
        self,
        labels: Optional[Iterable[str]] = None,
        properties: Optional[dict] = None,
        predicate: Optional[Callable[[Node], bool]] = None,
    ) -> list[Node]:
        """Find nodes by labels, exact property matches, and a predicate.

        Backends may provide candidate sets for label/property filters. The
        graph still applies all filters after candidate selection, so results
        are correct even when a backend falls back to scans.

        Args:
            labels: Labels that every returned node must contain.
            properties: Exact property matches required on returned nodes.
            predicate: Optional callable that receives a node and returns true
                when it should be included.

        Returns:
            Matching nodes.

        Example:
            >>> graph = GraphDB(InMemoryKVStore(), PickleSerializer())
            >>> graph.put_node(Node("alice", labels=["Person"], properties={"age": 30}))
            <graphdb.Node object at ...>
            >>> [node.get_id for node in graph.find_nodes(labels=["Person"])]
            ['alice']
        """
        wanted_labels = set(labels or [])
        wanted_props = properties or {}
        result = []
        candidates = None
        if hasattr(self.store, "node_candidates"):
            candidates = self.store.node_candidates(labels=wanted_labels, properties=wanted_props)
        nodes = self.get_nodes(candidates) if candidates is not None else self.iter_nodes()
        for node in nodes:
            if node is None:
                continue
            if wanted_labels and not wanted_labels.issubset(node.labels):
                continue
            if any(node.properties.get(k) != v for k, v in wanted_props.items()):
                continue
            if predicate is not None and not predicate(node):
                continue
            result.append(node)
        return result

    def find_edges(
        self,
        type: Optional[str] = None,
        source=None,
        target=None,
        properties: Optional[dict] = None,
        predicate: Optional[Callable[[Edge], bool]] = None,
    ) -> list[Edge]:
        """Find edges by type, endpoints, exact properties, and predicate.

        Args:
            type: Required edge type. ``None`` accepts any type.
            source: Optional source node ID filter.
            target: Optional target node ID filter.
            properties: Exact property matches required on returned edges.
            predicate: Optional callable that receives an edge and returns true
                when it should be included.

        Returns:
            Matching edges.

        Example:
            >>> graph = GraphDB(InMemoryKVStore(), PickleSerializer())
            >>> graph.put_node(Node("a")); graph.put_node(Node("b"))
            <graphdb.Node object at ...>
            <graphdb.Node object at ...>
            >>> graph.put_edge(Edge("e1", "a", "b", type="KNOWS"))
            <graphdb.Edge object at ...>
            >>> [edge.get_id for edge in graph.find_edges(type="KNOWS", source="a")]
            ['e1']
        """
        source_id = _id_to_str(source) if source is not None else None
        target_id = _id_to_str(target) if target is not None else None
        wanted_props = properties or {}
        result = []
        candidate_ids = None
        if hasattr(self.store, "edge_candidates"):
            candidate_ids = self.store.edge_candidates(type=type, properties=wanted_props)
        if source_id is not None:
            edges = self.out_edges(source_id)
            if candidate_ids is not None:
                candidate_ids = set(candidate_ids)
                edges = [edge for edge in edges if edge.get_id in candidate_ids]
        else:
            edges = self.get_edges(candidate_ids) if candidate_ids is not None else self.iter_edges()
        for edge in edges:
            if edge is None:
                continue
            if type is not None and edge.type != type:
                continue
            if source_id is not None and edge.source != source_id:
                continue
            if target_id is not None and edge.target != target_id:
                continue
            if any(edge.properties.get(k) != v for k, v in wanted_props.items()):
                continue
            if predicate is not None and not predicate(edge):
                continue
            result.append(edge)
        return result

    def check_integrity(self) -> dict:
        """Check node, edge, and adjacency consistency.

        Returns:
            Dictionary with ``ok`` and ``errors`` keys.
        """
        errors = []
        node_ids = {node.get_id for node in self.iter_nodes()}
        edge_ids = set()
        for edge in self.iter_edges():
            edge_ids.add(edge.get_id)
            if edge.source not in node_ids:
                errors.append(f"edge {edge.get_id} has missing source {edge.source}")
            if edge.target not in node_ids:
                errors.append(f"edge {edge.get_id} has missing target {edge.target}")
            if edge.get_id not in self.get_adjacency_list(edge.source, "out"):
                errors.append(f"edge {edge.get_id} missing from source adjacency")
            if edge.get_id not in self.get_adjacency_list(edge.target, "in"):
                errors.append(f"edge {edge.get_id} missing from target adjacency")
        for node_id in node_ids:
            for edge_id in self.get_adjacency_list(node_id, "both"):
                if edge_id not in edge_ids:
                    errors.append(f"node {node_id} references missing edge {edge_id}")
        return {"ok": not errors, "errors": errors}

    def rebuild_indexes(self) -> None:
        """Rebuild backend-maintained indexes and optimized adjacency records."""
        with self.store.write_transaction():
            if hasattr(self.store, "clear_indexes"):
                self.store.clear_indexes()
            for node in self.iter_nodes():
                if hasattr(self.store, "index_node"):
                    self.store.index_node(node)
            for edge in self.iter_edges():
                if hasattr(self.store, "index_edge"):
                    self.store.index_edge(edge)
                if hasattr(self.store, "add_adjacency_edge"):
                    self.store.add_adjacency_edge(edge.source, edge.target, edge.get_id)

    def compact(self, destination_path: Optional[str] = None):
        """Compact the underlying store when the backend supports it."""
        if not hasattr(self.store, "compact"):
            return None
        return self.store.compact(destination_path)

    def _get_adjacency(self, node_id) -> dict[str, set[str]]:
        raw = self.store.get_adjacency(_id_to_key(node_id))
        if raw is None:
            return {"out": set(), "in": set()}
        return self.entity_serializer.deserialize(raw, "AdjacencyList")

    def _put_adjacency(self, node_id, adjacency: dict[str, set[str]]) -> None:
        self.store.put_adjacency(_id_to_key(node_id), self.entity_serializer.serialize(adjacency, "AdjacencyList"))

    def _add_edge_to_adjacency(self, node_id, edge_id: str, direction: str) -> None:
        adjacency = self._get_adjacency(node_id)
        adjacency[direction].add(_id_to_str(edge_id))
        self._put_adjacency(node_id, adjacency)

    def _remove_edge_from_adjacency(self, node_id, edge_id: str, direction: str) -> None:
        adjacency = self._get_adjacency(node_id)
        adjacency[direction].discard(_id_to_str(edge_id))
        self._put_adjacency(node_id, adjacency)

    def close(self):
        """Close the underlying storage backend.

        Example:
            >>> graph = GraphDB(InMemoryKVStore(), PickleSerializer())
            >>> graph.close()
        """
        self.store.close()
