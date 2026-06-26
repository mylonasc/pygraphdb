"""Logical planning for the PyGraphDB Cypher subset."""

from __future__ import annotations

from dataclasses import dataclass

from .cypher_ast import MatchQuery, NodeScanQuery, SampleTypedPathsCall, TraversalHop


@dataclass(frozen=True)
class LogicalPlan:
    """Ordered logical operators for a parsed query."""

    operators: tuple[object, ...]


@dataclass(frozen=True)
class NodeByIdSeek:
    """Seek one node by its identity value."""

    source_id: str
    variable: str


@dataclass(frozen=True)
class NodeLabelScan:
    """Scan node IDs from the label index."""

    label: str
    variable: str
    labels: tuple[str, ...] = ()


@dataclass(frozen=True)
class NodePropertySeek:
    """Seek node IDs from an exact property index."""

    property_name: str
    property_value: object


@dataclass(frozen=True)
class FilterNodeProperty:
    """Filter bound nodes by exact property value."""

    variable: str
    property_name: str
    property_value: object


@dataclass(frozen=True)
class FilterExpression:
    """Filter rows by a boolean expression."""

    expression: object


@dataclass(frozen=True)
class Expand:
    """Expand rows through one typed relationship hop."""

    hop: TraversalHop


@dataclass(frozen=True)
class Project:
    """Project result columns."""

    returns: tuple[str, ...]


@dataclass(frozen=True)
class Limit:
    """Limit result rows."""

    limit: int


@dataclass(frozen=True)
class ProcedureCall:
    """Call a built-in procedure."""

    name: str


def plan_query(parsed) -> LogicalPlan:
    """Create a simple logical plan for the currently supported query types."""
    if isinstance(parsed, NodeScanQuery):
        return _plan_node_scan(parsed)
    if isinstance(parsed, MatchQuery):
        return _plan_match(parsed)
    if isinstance(parsed, SampleTypedPathsCall):
        operators = [ProcedureCall("pg.sample_typed_paths"), Project(parsed.returns)]
        if parsed.limit is not None:
            operators.append(Limit(parsed.limit))
        return LogicalPlan(tuple(operators))
    raise TypeError(f"unsupported parsed query type: {type(parsed).__name__}")


def _plan_node_scan(parsed: NodeScanQuery) -> LogicalPlan:
    operators: list[object] = [NodeLabelScan(parsed.label, parsed.variable, parsed.labels or (parsed.label,))]
    if parsed.property_name is not None:
        operators.append(NodePropertySeek(parsed.property_name, parsed.property_value))
        operators.append(FilterNodeProperty(parsed.variable, parsed.property_name, parsed.property_value))
    if parsed.where is not None:
        operators.append(FilterExpression(parsed.where))
    operators.append(Project(parsed.returns))
    if parsed.limit is not None:
        operators.append(Limit(parsed.limit))
    return LogicalPlan(tuple(operators))


def _plan_match(parsed: MatchQuery) -> LogicalPlan:
    operators: list[object] = [NodeByIdSeek(parsed.source_id, parsed.source_var)]
    operators.extend(Expand(hop) for hop in parsed.hops)
    if parsed.where is not None:
        operators.append(FilterExpression(parsed.where))
    operators.append(Project(parsed.returns))
    if parsed.limit is not None:
        operators.append(Limit(parsed.limit))
    return LogicalPlan(tuple(operators))
