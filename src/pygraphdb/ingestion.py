"""Columnar ingestion containers for PyGraphDB."""

from __future__ import annotations

from dataclasses import dataclass


def _missing_dependency_error(package_name, install_name=None, feature_name=None):
    """Build a consistent optional dependency error."""
    install_name = install_name or package_name
    feature_name = feature_name or package_name
    return ImportError(
        f"Missing optional dependency '{package_name}' required for {feature_name}. "
        f"Install it with `python -m pip install {install_name}` or `uv add {install_name}`."
    )


def _column_to_list(column, name: str):
    """Convert a Python or Arrow-like column to a list and reject nulls."""
    if hasattr(column, "to_pylist"):
        values = column.to_pylist()
    else:
        values = list(column)
    if any(value is None for value in values):
        raise ValueError(f"{name} contains null values")
    return values


def _to_bytes(value, name: str) -> bytes:
    """Normalize a string/bytes identifier to bytes."""
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, str):
        return value.encode("utf-8")
    raise TypeError(f"{name} values must be str or bytes-like, got {type(value).__name__}")


def _to_payload_bytes(value, name: str) -> bytes:
    """Normalize a serialized payload value to bytes."""
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    raise TypeError(f"{name} values must be bytes-like serialized payloads, got {type(value).__name__}")


def _validate_equal_lengths(columns: dict[str, list]) -> int:
    """Validate that all columns have the same length."""
    lengths = {name: len(values) for name, values in columns.items()}
    unique_lengths = set(lengths.values())
    if len(unique_lengths) != 1:
        details = ", ".join(f"{name}={length}" for name, length in lengths.items())
        raise ValueError(f"column lengths must match: {details}")
    return next(iter(unique_lengths), 0)


@dataclass(frozen=True)
class NodeList:
    """Columnar nodes with caller-provided serialized node values."""

    node_ids: list[bytes]
    node_values: list[bytes]

    @classmethod
    def from_arrow(cls, node_ids, node_values):
        """Create a node list from Arrow-like or Python columns."""
        if node_values is None:
            raise ValueError("node_values is required for columnar node ingestion")
        raw_node_ids = _column_to_list(node_ids, "node_ids")
        raw_node_values = _column_to_list(node_values, "node_values")
        _validate_equal_lengths({"node_ids": raw_node_ids, "node_values": raw_node_values})
        return cls(
            node_ids=[_to_bytes(value, "node_ids") for value in raw_node_ids],
            node_values=[_to_payload_bytes(value, "node_values") for value in raw_node_values],
        )

    @classmethod
    def from_polars(cls, df, *, node_id="node_id", node_value="node_value"):
        """Create a node list from a Polars DataFrame."""
        try:
            import polars as pl
        except ImportError as exc:
            raise _missing_dependency_error("polars", feature_name="NodeList.from_polars") from exc
        if not isinstance(df, pl.DataFrame):
            raise TypeError("df must be a polars.DataFrame")
        missing = [column for column in [node_id, node_value] if column not in df.columns]
        if missing:
            raise ValueError(f"missing required columns: {', '.join(missing)}")
        return cls.from_arrow(df[node_id].to_arrow(), df[node_value].to_arrow())

    def chunks(self, chunk_size: int):
        """Yield fixed-size ``NodeList`` chunks."""
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        for start in range(0, len(self.node_ids), chunk_size):
            end = start + chunk_size
            yield NodeList(self.node_ids[start:end], self.node_values[start:end])


@dataclass(frozen=True)
class EdgeList:
    """Columnar typed edges with caller-provided serialized edge values."""

    edge_ids: list[bytes]
    sources: list[bytes]
    targets: list[bytes]
    edge_types: list[str]
    edge_values: list[bytes]

    @classmethod
    def from_arrow(cls, edge_ids, sources, targets, edge_types, edge_values):
        """Create an edge list from Arrow-like or Python columns."""
        if edge_values is None:
            raise ValueError("edge_values is required for columnar edge ingestion")
        raw_edge_ids = _column_to_list(edge_ids, "edge_ids")
        raw_sources = _column_to_list(sources, "sources")
        raw_targets = _column_to_list(targets, "targets")
        raw_edge_types = _column_to_list(edge_types, "edge_types")
        raw_edge_values = _column_to_list(edge_values, "edge_values")
        _validate_equal_lengths(
            {
                "edge_ids": raw_edge_ids,
                "sources": raw_sources,
                "targets": raw_targets,
                "edge_types": raw_edge_types,
                "edge_values": raw_edge_values,
            }
        )
        normalized_edge_types = []
        for value in raw_edge_types:
            if isinstance(value, bytes):
                normalized_edge_types.append(value.decode("utf-8"))
            elif isinstance(value, str):
                normalized_edge_types.append(value)
            else:
                raise TypeError(f"edge_types values must be str or bytes, got {type(value).__name__}")
        return cls(
            edge_ids=[_to_bytes(value, "edge_ids") for value in raw_edge_ids],
            sources=[_to_bytes(value, "sources") for value in raw_sources],
            targets=[_to_bytes(value, "targets") for value in raw_targets],
            edge_types=normalized_edge_types,
            edge_values=[_to_payload_bytes(value, "edge_values") for value in raw_edge_values],
        )

    @classmethod
    def from_polars(
        cls,
        df,
        *,
        edge_id="edge_id",
        source="source",
        target="target",
        edge_type="edge_type",
        edge_value="edge_value",
    ):
        """Create an edge list from a Polars DataFrame."""
        try:
            import polars as pl
        except ImportError as exc:
            raise _missing_dependency_error("polars", feature_name="EdgeList.from_polars") from exc
        if not isinstance(df, pl.DataFrame):
            raise TypeError("df must be a polars.DataFrame")
        required = [edge_id, source, target, edge_type, edge_value]
        missing = [column for column in required if column not in df.columns]
        if missing:
            raise ValueError(f"missing required columns: {', '.join(missing)}")
        return cls.from_arrow(
            df[edge_id].to_arrow(),
            df[source].to_arrow(),
            df[target].to_arrow(),
            df[edge_type].to_arrow(),
            df[edge_value].to_arrow(),
        )

    def chunks(self, chunk_size: int):
        """Yield fixed-size ``EdgeList`` chunks."""
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        for start in range(0, len(self.edge_ids), chunk_size):
            end = start + chunk_size
            yield EdgeList(
                self.edge_ids[start:end],
                self.sources[start:end],
                self.targets[start:end],
                self.edge_types[start:end],
                self.edge_values[start:end],
            )
