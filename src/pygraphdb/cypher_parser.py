"""Parser for the current PyGraphDB Cypher subset.

This module intentionally keeps the accepted language small while separating
query syntax from execution. It is the migration point for a fuller Cypher
frontend in later phases.
"""

from __future__ import annotations

import ast
import re

from .cypher_ast import AndExpression, ComparisonExpression, MatchQuery, NodeScanQuery, Parameter, PropertyRef, SampleTypedPathsCall, TraversalHop


_IDENTIFIER = r"[A-Za-z_][A-Za-z0-9_]*"
_PARAMETER_RE = re.compile(rf"^\$(?P<name>{_IDENTIFIER})$")
_RETURN_ITEM = rf"{_IDENTIFIER}(?:\.{_IDENTIFIER})?"
_RETURN_ITEMS = rf"{_RETURN_ITEM}(?:\s*,\s*{_RETURN_ITEM})*"
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
    rf"^\s*(?:WHERE\s+(?P<where>.*?)\s+)?RETURN\s+(?P<returns>{_RETURN_ITEMS})(?:\s+LIMIT\s+(?P<limit>\d+))?\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_NODE_SCAN_RE = re.compile(
    rf"^\s*MATCH\s+\((?P<var>{_IDENTIFIER})(?P<labels>:{_IDENTIFIER}(?::{_IDENTIFIER})*)(?:\s*\{{\s*(?P<property>{_IDENTIFIER})\s*:\s*(?P<value>.*?)\s*\}})?\)\s+"
    rf"(?:WHERE\s+(?P<where>.*?)\s+)?"
    rf"RETURN\s+(?P<returns>{_RETURN_ITEMS})(?:\s+LIMIT\s+(?P<limit>\d+))?\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_COMPARISON_RE = re.compile(
    rf"^\s*(?P<variable>{_IDENTIFIER})\.(?P<property>{_IDENTIFIER})\s*"
    rf"(?P<operator>=|<>|!=|<=|>=|<|>)\s*(?P<value>.*?)\s*$",
    re.IGNORECASE | re.DOTALL,
)
_CALL_SAMPLE_RE = re.compile(
    r"^\s*CALL\s+pg\.sample_typed_paths\s*\((?P<args>.*)\)\s+"
    r"YIELD\s+path\s+RETURN\s+path(?:\s+LIMIT\s+(?P<limit>\d+))?\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)


def parse(query: str) -> MatchQuery | SampleTypedPathsCall | NodeScanQuery:
    """Parse the supported Cypher subset into AST objects."""
    stripped = query.strip()
    if re.match(r"^CALL\s+pg\.sample_typed_paths\b", stripped, re.IGNORECASE):
        return _parse_sample_typed_paths(stripped)
    node_scan = _parse_node_scan(stripped)
    if node_scan is not None:
        return node_scan
    return _parse_match(stripped)


def _parse_node_scan(query: str) -> NodeScanQuery | None:
    match = _NODE_SCAN_RE.match(query)
    if match is None:
        return None
    returns = _parse_returns(match.group("returns"))
    variable = match.group("var")
    unknown = [name for name in _return_variables(returns) if name != variable]
    if unknown:
        raise ValueError(f"RETURN references unbound variable(s): {', '.join(unknown)}")
    labels = tuple(part for part in match.group("labels").split(":") if part)
    property_value = None
    if match.group("property") is not None:
        property_value = parse_literal(match.group("value"))
    where = None
    if match.group("where") is not None:
        where = _parse_where_expression(match.group("where"), {variable})
    return NodeScanQuery(
        variable=variable,
        label=labels[0],
        property_name=match.group("property"),
        property_value=property_value,
        returns=returns,
        limit=_parse_limit(match.group("limit")),
        where=where,
        labels=labels,
    )


def _parse_match(query: str) -> MatchQuery:
    if not query.upper().startswith("MATCH "):
        raise unsupported_query_error()

    remainder = query[5:]
    anchor_match = _ANCHOR_RE.match(remainder)
    if anchor_match is None:
        raise unsupported_query_error()

    source_var = anchor_match.group("source_var")
    source_id = anchor_match.group("source_id")
    remainder = remainder[anchor_match.end():]
    hops = []
    while True:
        hop_match, direction = _match_hop(remainder)
        if hop_match is None or direction is None:
            break
        edge_types = tuple(edge_type for edge_type in hop_match.group("edge_type").split("|") if edge_type)
        hops.append(
            TraversalHop(
                rel_var=hop_match.group("rel_var"),
                edge_type=edge_types[0],
                target_var=hop_match.group("target_var"),
                direction=direction,
                edge_types=edge_types,
            )
        )
        remainder = remainder[hop_match.end():]

    if not hops:
        raise unsupported_query_error()
    return_match = _RETURN_RE.match(remainder)
    if return_match is None:
        raise unsupported_query_error()

    bound_variables = _match_bound_variables(source_var, hops)
    where = None
    if return_match.group("where") is not None:
        where = _parse_where_expression(return_match.group("where"), bound_variables)

    parsed = MatchQuery(
        source_var=source_var,
        source_id=source_id,
        hops=tuple(hops),
        returns=_parse_returns(return_match.group("returns")),
        limit=_parse_limit(return_match.group("limit")),
        where=where,
    )
    _validate_match_returns(parsed)
    return parsed


def _parse_sample_typed_paths(query: str) -> SampleTypedPathsCall:
    match = _CALL_SAMPLE_RE.match(query)
    if match is None:
        raise unsupported_query_error()
    seed_ids, pattern = _parse_call_arguments(match.group("args"))
    if not isinstance(seed_ids, list) or not all(isinstance(seed_id, str) for seed_id in seed_ids):
        raise ValueError("pg.sample_typed_paths seed IDs must be a list of strings")
    if not isinstance(pattern, list) or not all(isinstance(hop, dict) for hop in pattern):
        raise ValueError("pg.sample_typed_paths pattern must be a list of dictionaries")
    return SampleTypedPathsCall(seed_ids=seed_ids, pattern=pattern, limit=_parse_limit(match.groupdict().get("limit")))


def parse_literal(literal_text: str):
    """Parse a Cypher literal or parameter reference supported by this subset."""
    literal_text = literal_text.strip()
    parameter_match = _PARAMETER_RE.match(literal_text)
    if parameter_match is not None:
        return Parameter(parameter_match.group("name"))
    lowered = literal_text.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered == "null":
        return None
    try:
        return ast.literal_eval(literal_text)
    except (SyntaxError, ValueError) as exc:
        raise ValueError(f"Invalid Cypher literal: {literal_text}") from exc


def _parse_where_expression(expression_text: str, bound_variables: set[str]):
    parts = _split_top_level_keyword(expression_text, "AND")
    if len(parts) > 1:
        return AndExpression(tuple(_parse_comparison(part, bound_variables) for part in parts))
    return _parse_comparison(expression_text, bound_variables)


def _parse_comparison(expression_text: str, bound_variables: set[str]) -> ComparisonExpression:
    match = _COMPARISON_RE.match(expression_text)
    if match is None:
        raise ValueError(f"Unsupported WHERE expression: {expression_text}")
    variable = match.group("variable")
    if variable not in bound_variables:
        raise ValueError(f"WHERE references unbound variable: {variable}")
    return ComparisonExpression(
        left=PropertyRef(variable=variable, property_name=match.group("property")),
        operator=match.group("operator"),
        right=parse_literal(match.group("value")),
    )


def _parse_returns(return_text: str) -> tuple[str, ...]:
    return tuple(part.strip() for part in return_text.split(","))


def _parse_limit(limit_text: str | None) -> int | None:
    if limit_text is None:
        return None
    limit = int(limit_text)
    if limit < 0:
        raise ValueError("LIMIT must be non-negative")
    return limit


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
    bound_variables = _match_bound_variables(parsed.source_var, parsed.hops)
    unknown = [name for name in _return_variables(parsed.returns) if name not in bound_variables]
    if unknown:
        raise ValueError(f"RETURN references unbound variable(s): {', '.join(unknown)}")


def _match_bound_variables(source_var: str, hops: list[TraversalHop] | tuple[TraversalHop, ...]) -> set[str]:
    bound_variables = {source_var}
    for hop in hops:
        bound_variables.add(hop.target_var)
        if hop.rel_var is not None:
            bound_variables.add(hop.rel_var)
    return bound_variables


def _return_variables(returns: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(item.split(".", 1)[0] for item in returns)


def _parse_call_arguments(args_text: str):
    parts = split_top_level_args(args_text)
    if len(parts) != 2:
        raise ValueError("pg.sample_typed_paths expects seed IDs and a sampling pattern")
    return ast.literal_eval(parts[0]), ast.literal_eval(parts[1])


def split_top_level_args(args_text: str) -> list[str]:
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


def _split_top_level_keyword(text: str, keyword: str) -> list[str]:
    parts = []
    start = 0
    quote = None
    escape = False
    pattern = re.compile(rf"\b{re.escape(keyword)}\b", re.IGNORECASE)
    index = 0
    while index < len(text):
        char = text[index]
        if quote is not None:
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == quote:
                quote = None
            index += 1
            continue
        if char in {'"', "'"}:
            quote = char
            index += 1
            continue
        match = pattern.match(text, index)
        if match is not None:
            parts.append(text[start:index].strip())
            start = match.end()
            index = match.end()
            continue
        index += 1
    parts.append(text[start:].strip())
    return parts


def unsupported_query_error() -> ValueError:
    return ValueError(
        "Unsupported Cypher query. Supported subset: "
        'MATCH (a {id: "node-id"})-[:TYPE1]->(b)<-[:TYPE2]-(c) RETURN a, b, c; '
        'MATCH (n:Label) RETURN n; '
        'CALL pg.sample_typed_paths(["node-id"], [{"edge_type": "TYPE", "sample_size": 2}]) YIELD path RETURN path'
    )
