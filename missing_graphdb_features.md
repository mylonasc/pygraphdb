# Missing GraphDB Features for openCypher Support

This file tracks property-graph features that PyGraphDB does not yet model directly, but that matter for an openCypher query engine.

## Implemented Performance Foundations

- Native node labels: `Node(labels=[...])` is serialized with each node.
- Label index: `node_label` sorted index supports prefix scans for `GraphDB.nodes_by_label(...)` and `MATCH (n:Label) RETURN n`.
- Relationship type catalog: `edge_type` sorted index supports `GraphDB.edges_by_type(...)` without scanning all edge records.
- Explicit exact-match property indexes: `create_node_property_index(...)` and `create_edge_property_index(...)` rebuild and register exact-match indexes.
- Generic sorted index primitives: backends implement `put_index_entry`, `put_index_entries_bulk`, `delete_index_entry`, and `iter_index_prefix`.
- Cypher indexed label scans: `MATCH (n:Label) RETURN n` uses the label index.
- Cypher indexed label plus property filtering: `MATCH (n:Label {name: "Aspirin"}) RETURN n` intersects with a registered property index when available, otherwise filters within the label scan.

## Graph Model Gaps

- Multiple node labels in Cypher syntax: the node model supports multiple labels, but Cypher parsing currently accepts one label per node pattern.
- Relationship type as schema field: typed traversal currently uses `edge.properties["type"]`; there is no dedicated `Edge.type` field.
- Range property indexes: exact-match property indexes exist, but range indexes and ordered comparisons are not implemented.
- Schema metadata: there are no constraints, uniqueness declarations, required properties, or type metadata.
- Stable property typing rules: serializers preserve Python values, but query comparison semantics are not yet defined across serializers.
- Persisted index definitions: property indexes are explicit per `GraphDB` instance; index definitions are not yet stored as metadata for automatic re-open behavior.

## Query Execution Gaps

- Planner: there is no cost-based planner for choosing between ID lookup, adjacency expansion, label indexes, property indexes, and relationship type indexes.
- Predicate evaluation: there is no shared expression evaluator for `WHERE` property predicates.
- Projection evaluation: there is no shared projection evaluator for `RETURN n`, `RETURN n.id`, or `RETURN n.name`.
- Multi-pattern joins: current APIs support direct traversal, but not general joins across multiple `MATCH` patterns.
- Variable-length paths: BFS exists for legacy untyped traversal, but Cypher-style `*min..max` typed paths are not implemented.
- Aggregation: no support for `count`, `collect`, `sum`, `avg`, grouping, or ordering.
- Mutation queries: no `CREATE`, `MERGE`, `SET`, `DELETE`, or `REMOVE` query support.

## Sampling Gaps

- Sampling is not standard openCypher, so PyGraphDB exposes it through `CALL pg.sample_typed_paths(...)` rather than standard syntax.
- Sampling result shape is query-row compatible for `path`, but not yet decomposed into separately projectable node/edge variables.
- Sampling reproducibility should define how query-level seeds map to the existing `rng` parameters.

## Recommended Compatibility Decisions

- Prefer native `Node.labels` over property-based label conventions such as `kind`.
- Keep relationship type backed by `edge.properties["type"]` for now, because it matches existing typed adjacency indexes.
- Use explicit exact-match property indexes for performance-sensitive predicates; avoid silently indexing every property.
- Treat node identity as `n.id` and edge identity as `r.id` in query expressions.
