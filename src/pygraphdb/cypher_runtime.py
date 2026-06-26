"""Streaming runtime operators for the PyGraphDB Cypher subset."""

from __future__ import annotations

from dataclasses import dataclass, field
from itertools import islice

from .cypher_ast import AndExpression, ComparisonExpression, MatchQuery, NodeScanQuery, Parameter


@dataclass
class QueryContext:
    """Runtime state shared by physical operators during one query."""

    graph: object
    parameters: dict[str, object] = field(default_factory=dict)
    node_cache: dict[bytes, object] = field(default_factory=dict)
    edge_cache: dict[bytes, object] = field(default_factory=dict)

    def node_key_to_bytes(self, node_key):
        return self.graph.node_key_to_bytes(node_key)

    def get_node(self, node_id: bytes):
        if node_id not in self.node_cache:
            self.node_cache[node_id] = self.graph.get_node(node_id)
        return self.node_cache[node_id]

    def get_edge(self, edge_id: bytes):
        if edge_id not in self.edge_cache:
            self.edge_cache[edge_id] = self.graph.get_edge(edge_id)
        return self.edge_cache[edge_id]

    def resolve(self, value):
        if isinstance(value, Parameter):
            if value.name not in self.parameters:
                raise ValueError(f"Missing Cypher parameter: ${value.name}")
            return self.parameters[value.name]
        return value


def execute_match(parsed: MatchQuery, context: QueryContext) -> list[dict[str, object]]:
    """Execute an anchored typed traversal plan and return projected records."""
    rows = anchored_node_seek(context, parsed.source_id, parsed.source_var)
    for hop in parsed.hops:
        rows = expand_typed(context, rows, hop)
        if parsed.limit is not None and parsed.where is None:
            rows = limit_rows(rows, parsed.limit)
    if parsed.where is not None:
        rows = filter_expression(rows, parsed.where, context)
    return list(project_rows(rows, parsed.returns, limit=parsed.limit))


def execute_node_scan(parsed: NodeScanQuery, context: QueryContext) -> list[dict[str, object]]:
    """Execute a label scan plan and return projected records."""
    node_ids = node_scan_ids(parsed, context)
    rows = hydrate_node_ids(context, node_ids, parsed.variable)
    if parsed.property_name is not None:
        property_value = context.resolve(parsed.property_value)
        rows = filter_node_property(rows, parsed.variable, parsed.property_name, property_value)
    if parsed.where is not None:
        rows = filter_expression(rows, parsed.where, context)
    return list(project_rows(rows, parsed.returns, limit=parsed.limit))


def anchored_node_seek(context: QueryContext, source_id: str, source_var: str):
    """Yield one initial row for an ID lookup when the source node exists."""
    source_id_bytes = context.node_key_to_bytes(source_id)
    source_node = context.get_node(source_id_bytes)
    if source_node is None:
        return
    yield {
        "current_node_id": source_id_bytes,
        "bindings": {source_var: source_node},
    }


def node_scan_ids(parsed: NodeScanQuery, context: QueryContext):
    """Yield node IDs for a label scan, using an exact property index when available."""
    if parsed.limit == 0:
        return iter(())
    label_ids = _node_ids_for_labels(parsed, context)
    if parsed.property_name is None or parsed.property_name not in context.graph.indexed_node_properties:
        return label_ids

    property_value = context.resolve(parsed.property_value)
    property_ids = set(context.graph.iter_node_ids_by_property(parsed.property_name, property_value))
    return iter(sorted(set(label_ids).intersection(property_ids)))


def _node_ids_for_labels(parsed: NodeScanQuery, context: QueryContext):
    labels = parsed.labels or (parsed.label,)
    if len(labels) == 1:
        return context.graph.iter_node_ids_by_label(labels[0])
    matching_ids = None
    for label in labels:
        current_ids = set(context.graph.iter_node_ids_by_label(label))
        matching_ids = current_ids if matching_ids is None else matching_ids.intersection(current_ids)
    return iter(sorted(matching_ids or set()))


def hydrate_node_ids(context: QueryContext, node_ids, variable: str):
    """Hydrate node IDs into binding rows."""
    for node_id in node_ids:
        node = context.get_node(node_id)
        if node is None:
            continue
        yield {
            "current_node_id": node_id,
            "bindings": {variable: node},
        }


def filter_node_property(rows, variable: str, property_name: str, property_value):
    """Yield rows whose bound node has an exact property value."""
    for row in rows:
        node = row["bindings"][variable]
        if node.properties.get(property_name) == property_value:
            yield row


def filter_expression(rows, expression, context: QueryContext):
    """Yield rows that satisfy a supported boolean expression."""
    for row in rows:
        if evaluate_expression(expression, row["bindings"], context):
            yield row


def evaluate_expression(expression, bindings: dict[str, object], context: QueryContext) -> bool:
    """Evaluate a supported expression against one row of bindings."""
    if isinstance(expression, AndExpression):
        return all(evaluate_expression(part, bindings, context) for part in expression.expressions)
    if not isinstance(expression, ComparisonExpression):
        raise ValueError(f"Unsupported expression type: {type(expression).__name__}")
    left_value = project_value(bindings, f"{expression.left.variable}.{expression.left.property_name}")
    right_value = context.resolve(expression.right)
    operator = expression.operator
    if operator == "=":
        return left_value == right_value
    if operator in {"!=", "<>"}:
        return left_value != right_value
    if left_value is None or right_value is None:
        return False
    if operator == "<":
        return left_value < right_value
    if operator == "<=":
        return left_value <= right_value
    if operator == ">":
        return left_value > right_value
    if operator == ">=":
        return left_value >= right_value
    raise ValueError(f"Unsupported comparison operator: {operator}")


def expand_typed(context: QueryContext, rows, hop):
    """Expand rows through one typed relationship hop."""
    for row in rows:
        for edge_type in hop.edge_types or (hop.edge_type,):
            for adjacency in context.graph.iter_typed_adjacency(
                row["current_node_id"],
                edge_type,
                direction=hop.direction,
            ):
                target_node = context.get_node(adjacency["neighbor_id"])
                if target_node is None:
                    continue
                bindings = dict(row["bindings"])
                if hop.target_var in bindings and not same_entity(bindings[hop.target_var], target_node):
                    continue
                bindings[hop.target_var] = target_node
                if hop.rel_var is not None:
                    edge = context.get_edge(adjacency["edge_id"])
                    if edge is None:
                        continue
                    if hop.rel_var in bindings and not same_entity(bindings[hop.rel_var], edge):
                        continue
                    bindings[hop.rel_var] = edge
                yield {
                    "current_node_id": adjacency["neighbor_id"],
                    "bindings": bindings,
                }


def limit_rows(rows, limit: int | None):
    """Limit a streaming row source."""
    if limit is None:
        return rows
    return islice(rows, limit)


def project_rows(rows, returns: tuple[str, ...], limit: int | None = None):
    """Project binding rows into result records."""
    limited_rows = limit_rows(rows, limit)
    for row in limited_rows:
        yield {column: project_value(row["bindings"], column) for column in returns}


def project_value(bindings: dict[str, object], return_item: str):
    """Project one return item from variable bindings."""
    variable, _, property_name = return_item.partition(".")
    value = bindings[variable]
    if not property_name:
        return value
    if property_name == "id" and hasattr(value, "get_id"):
        return value.get_id
    if property_name == "labels" and hasattr(value, "labels"):
        return value.labels
    if property_name in {"source", "target"} and hasattr(value, property_name):
        return getattr(value, property_name)
    properties = getattr(value, "properties", {})
    if property_name in properties:
        return properties[property_name]
    return None


def same_entity(left, right) -> bool:
    """Return whether two graph entities represent the same stored entity."""
    left_id = getattr(left, "get_id", None)
    right_id = getattr(right, "get_id", None)
    if left_id is not None and right_id is not None:
        return left_id == right_id
    return left == right
