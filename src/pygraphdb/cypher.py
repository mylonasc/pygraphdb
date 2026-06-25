"""Minimal read-only Cypher support for PyGraphDB.

The supported subset maps directly to existing typed adjacency and sampling APIs:

    MATCH (a {id: "node-id"})-[:TYPE1]->(b)<-[:TYPE2]-(c) RETURN a, b, c
    CALL pg.sample_typed_paths(["node-id"], [{"edge_type": "TYPE", "sample_size": 2}]) YIELD path RETURN path
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
import re


_IDENTIFIER = r"[A-Za-z_][A-Za-z0-9_]*"
_IDENTIFIER_RE = re.compile(rf"^{_IDENTIFIER}$")
_ANCHOR_RE = re.compile(
    rf"^\s*\((?P<source_var>{_IDENTIFIER})\s*"
    rf"\{{\s*id\s*:\s*(?P<quote>['\"])(?P<source_id>.*?)(?P=quote)\s*\}}\)"
)
_OUT_HOP_RE = re.compile(
    rf"^\s*-\s*\[(?:(?P<rel_var>{_IDENTIFIER})\s*)?:(?P<edge_type>[^\]\s]+)\]\s*->\s*"
    rf"\((?P<target_var>{_IDENTIFIER})\)"
)
_IN_HOP_RE = re.compile(
    rf"^\s*<-\s*\[(?:(?P<rel_var>{_IDENTIFIER})\s*)?:(?P<edge_type>[^\]\s]+)\]\s*-\s*"
    rf"\((?P<target_var>{_IDENTIFIER})\)"
)
_ANY_HOP_RE = re.compile(
    rf"^\s*-\s*\[(?:(?P<rel_var>{_IDENTIFIER})\s*)?:(?P<edge_type>[^\]\s]+)\]\s*-\s*"
    rf"\((?P<target_var>{_IDENTIFIER})\)"
)
_RETURN_RE = re.compile(
    rf"^\s*RETURN\s+(?P<returns>{_IDENTIFIER}(?:\s*,\s*{_IDENTIFIER})*)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_NODE_SCAN_RE = re.compile(
    rf"^\s*MATCH\s+\((?P<var>{_IDENTIFIER}):(?P<label>{_IDENTIFIER})(?:\s*\{{\s*(?P<property>{_IDENTIFIER})\s*:\s*(?P<value>.*?)\s*\}})?\)\s+"
    rf"RETURN\s+(?P<returns>{_IDENTIFIER}(?:\s*,\s*{_IDENTIFIER})*)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_CALL_SAMPLE_RE = re.compile(
    r"^\s*CALL\s+pg\.sample_typed_paths\s*\((?P<args>.*)\)\s+"
    r"YIELD\s+path\s+RETURN\s+path\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)


@dataclass(frozen=True)
class TraversalHop:
    """One typed relationship expansion in a parsed ``MATCH`` pattern."""

    rel_var: str | None
    edge_type: str
    target_var: str
    direction: str = "out"


@dataclass(frozen=True)
class MatchQuery:
    """Parsed anchored typed path query."""

    source_var: str
    source_id: str
    hops: tuple[TraversalHop, ...]
    returns: tuple[str, ...]


@dataclass(frozen=True)
class SampleTypedPathsCall:
    """Parsed ``pg.sample_typed_paths`` procedure call."""

    seed_ids: list[str]
    pattern: list[dict[str, object]]
    returns: tuple[str, ...] = ("path",)


@dataclass(frozen=True)
class NodeScanQuery:
    """Parsed indexed node label scan query."""

    variable: str
    label: str
    property_name: str | None
    property_value: object
    returns: tuple[str, ...]


@dataclass(frozen=True)
class QueryResult:
    """Tabular query result returned by ``GraphDB.query``.

    ``columns`` contains projected column names in return order. ``records`` is
    a list of dictionaries keyed by column name.

    Examples:
        >>> result = QueryResult(columns=("n",), records=[{"n": "node"}])
        >>> len(result)
        1
        >>> list(result)[0]["n"]
        'node'
    """

    columns: tuple[str, ...]
    records: list[dict[str, object]]

    def __iter__(self):
        """Iterate over result records."""
        return iter(self.records)

    def __len__(self):
        """Return the number of result records."""
        return len(self.records)


def parse(query: str) -> MatchQuery | SampleTypedPathsCall | NodeScanQuery:
    """Parse the supported Cypher subset.

    Args:
        query: Cypher query text.

    Returns:
        Parsed query object.

    Raises:
        ValueError: If the query is outside the supported subset.

    Examples:
        >>> parse('MATCH (n:Drug) RETURN n').label
        'Drug'
    """
    stripped = query.strip()
    if re.match(r"^CALL\s+pg\.sample_typed_paths\b", stripped, re.IGNORECASE):
        return _parse_sample_typed_paths(stripped)
    node_scan = _parse_node_scan(stripped)
    if node_scan is not None:
        return node_scan
    return _parse_match(stripped)


def execute(graph, query: str) -> QueryResult:
    """Execute a supported Cypher query against a ``GraphDB`` instance.

    Args:
        graph: ``GraphDB`` instance used for indexed lookups and traversal.
        query: Cypher query text.

    Returns:
        ``QueryResult`` with projected records.

    Examples:
        >>> execute(graph_db, 'MATCH (n:Drug) RETURN n')  # doctest: +SKIP
    """
    parsed = parse(query)
    if isinstance(parsed, SampleTypedPathsCall):
        paths = graph.sample_typed_paths(parsed.seed_ids, parsed.pattern)
        return QueryResult(
            columns=parsed.returns,
            records=[{"path": path} for path in paths],
        )
    if isinstance(parsed, NodeScanQuery):
        return _execute_node_scan(graph, parsed)
    return _execute_match(graph, parsed)


def _parse_node_scan(query: str) -> NodeScanQuery | None:
    match = _NODE_SCAN_RE.match(query)
    if match is None:
        return None
    returns = _parse_returns(match.group("returns"))
    variable = match.group("var")
    unknown = [name for name in returns if name != variable]
    if unknown:
        raise ValueError(f"RETURN references unbound variable(s): {', '.join(unknown)}")
    property_value = None
    if match.group("property") is not None:
        property_value = ast.literal_eval(match.group("value"))
    return NodeScanQuery(
        variable=variable,
        label=match.group("label"),
        property_name=match.group("property"),
        property_value=property_value,
        returns=returns,
    )


def _parse_match(query: str) -> MatchQuery:
    if not query.upper().startswith("MATCH "):
        raise _unsupported_query_error()

    remainder = query[5:]
    anchor_match = _ANCHOR_RE.match(remainder)
    if anchor_match is None:
        raise _unsupported_query_error()

    source_var = anchor_match.group("source_var")
    source_id = anchor_match.group("source_id")
    remainder = remainder[anchor_match.end():]
    hops = []
    while True:
        hop_match, direction = _match_hop(remainder)
        if hop_match is None or direction is None:
            break
        hops.append(
            TraversalHop(
                rel_var=hop_match.group("rel_var"),
                edge_type=hop_match.group("edge_type"),
                target_var=hop_match.group("target_var"),
                direction=direction,
            )
        )
        remainder = remainder[hop_match.end():]

    if not hops:
        raise _unsupported_query_error()
    return_match = _RETURN_RE.match(remainder)
    if return_match is None:
        raise _unsupported_query_error()

    parsed = MatchQuery(
        source_var=source_var,
        source_id=source_id,
        hops=tuple(hops),
        returns=_parse_returns(return_match.group("returns")),
    )
    _validate_match_returns(parsed)
    return parsed


def _parse_sample_typed_paths(query: str) -> SampleTypedPathsCall:
    match = _CALL_SAMPLE_RE.match(query)
    if match is None:
        raise _unsupported_query_error()
    seed_ids, pattern = _parse_call_arguments(match.group("args"))
    if not isinstance(seed_ids, list) or not all(isinstance(seed_id, str) for seed_id in seed_ids):
        raise ValueError("pg.sample_typed_paths seed IDs must be a list of strings")
    if not isinstance(pattern, list) or not all(isinstance(hop, dict) for hop in pattern):
        raise ValueError("pg.sample_typed_paths pattern must be a list of dictionaries")
    return SampleTypedPathsCall(seed_ids=seed_ids, pattern=pattern)


def _execute_match(graph, parsed: MatchQuery) -> QueryResult:
    source_id = graph.node_key_to_bytes(parsed.source_id)
    source_node = graph.get_node(source_id)
    if source_node is None:
        return QueryResult(columns=parsed.returns, records=[])

    frontier = [
        {
            "current_node_id": source_id,
            "bindings": {parsed.source_var: source_node},
        }
    ]
    for hop in parsed.hops:
        next_frontier = []
        for partial in frontier:
            for adjacency in graph.iter_typed_adjacency(
                partial["current_node_id"],
                hop.edge_type,
                direction=hop.direction,
            ):
                target_node = graph.get_node(adjacency["neighbor_id"])
                if target_node is None:
                    continue
                bindings = dict(partial["bindings"])
                bindings[hop.target_var] = target_node
                if hop.rel_var is not None:
                    bindings[hop.rel_var] = graph.get_edge(adjacency["edge_id"])
                next_frontier.append(
                    {
                        "current_node_id": adjacency["neighbor_id"],
                        "bindings": bindings,
                    }
                )
        frontier = next_frontier
        if not frontier:
            break

    records = [
        {column: partial["bindings"][column] for column in parsed.returns}
        for partial in frontier
    ]
    return QueryResult(columns=parsed.returns, records=records)


def _execute_node_scan(graph, parsed: NodeScanQuery) -> QueryResult:
    label_ids = set(graph.iter_node_ids_by_label(parsed.label))
    if parsed.property_name is not None and parsed.property_name in graph.indexed_node_properties:
        property_ids = set(graph.iter_node_ids_by_property(parsed.property_name, parsed.property_value))
        node_ids = label_ids.intersection(property_ids)
    else:
        node_ids = label_ids

    records = []
    for node_id in node_ids:
        node = graph.get_node(node_id)
        if node is None:
            continue
        if parsed.property_name is not None and node.properties.get(parsed.property_name) != parsed.property_value:
            continue
        records.append({parsed.variable: node})
    return QueryResult(columns=parsed.returns, records=records)


def _parse_returns(return_text: str) -> tuple[str, ...]:
    return tuple(part.strip() for part in return_text.split(","))


def _match_hop(remainder: str):
    for pattern, direction in (
        (_IN_HOP_RE, "in"),
        (_OUT_HOP_RE, "out"),
        (_ANY_HOP_RE, "any"),
    ):
        match = pattern.match(remainder)
        if match is not None:
            return match, direction
    return None, None


def _validate_match_returns(parsed: MatchQuery) -> None:
    bound_variables = {parsed.source_var}
    for hop in parsed.hops:
        bound_variables.add(hop.target_var)
        if hop.rel_var is not None:
            bound_variables.add(hop.rel_var)
    unknown = [name for name in parsed.returns if name not in bound_variables]
    if unknown:
        raise ValueError(f"RETURN references unbound variable(s): {', '.join(unknown)}")


def _parse_call_arguments(args_text: str):
    parts = _split_top_level_args(args_text)
    if len(parts) != 2:
        raise ValueError("pg.sample_typed_paths expects seed IDs and a sampling pattern")
    return ast.literal_eval(parts[0]), ast.literal_eval(parts[1])


def _split_top_level_args(args_text: str) -> list[str]:
    parts = []
    start = 0
    depth = 0
    quote = None
    escape = False
    for index, char in enumerate(args_text):
        if quote is not None:
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == quote:
                quote = None
            continue
        if char in {'"', "'"}:
            quote = char
            continue
        if char in "[({":
            depth += 1
            continue
        if char in "])}":
            depth -= 1
            continue
        if char == "," and depth == 0:
            parts.append(args_text[start:index].strip())
            start = index + 1
    parts.append(args_text[start:].strip())
    return parts


def _unsupported_query_error() -> ValueError:
    return ValueError(
        "Unsupported Cypher query. Supported subset: "
        'MATCH (a {id: "node-id"})-[:TYPE1]->(b)<-[:TYPE2]-(c) RETURN a, b, c; '
        'MATCH (n:Label) RETURN n; '
        'CALL pg.sample_typed_paths(["node-id"], [{"edge_type": "TYPE", "sample_size": 2}]) YIELD path RETURN path'
    )
