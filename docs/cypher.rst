Cypher Queries
==============

PyGraphDB includes an initial read-only Cypher API through
``GraphDB.query(cypher)``. The current implementation is intentionally small and
maps directly to features that already have efficient database APIs: anchored
typed traversal and typed path sampling.

Relationship types are read from ``edge.properties["type"]``. Native node labels
are not implemented yet, so patterns such as ``(n:Drug)`` are not supported.

Supported Feature Matrix
------------------------

The table below distinguishes features available through the Python database API
from features exposed through the Cypher API.

.. list-table:: Current DB API and Cypher API support
   :header-rows: 1
   :widths: 32 18 18 32

   * - Feature
     - DB API
     - Cypher API
     - Notes
   * - Node and edge property storage
     - Yes
     - Partial
     - Cypher can return bound ``Node`` and ``Edge`` objects, but property projections such as ``RETURN n.name`` are not implemented yet.
   * - Native node labels
     - No
     - No
     - Labels are tracked as a missing graph-model feature. Use node properties such as ``kind`` for now.
   * - Dedicated relationship type field
     - Partial
     - Partial
     - Typed traversal uses ``edge.properties["type"]`` instead of a dedicated ``Edge.type`` field.
   * - Anchored one-hop typed traversal
     - Yes
     - Yes
     - DB API uses ``iter_typed_adjacency`` or ``neighbors_by_edge_type``. Cypher supports ``MATCH (a {id: "..."})-[:TYPE]->(b)``.
   * - Anchored multi-hop typed traversal
     - Yes
     - Yes
     - Cypher supports repeated outgoing typed hops from an anchored start node.
   * - Reverse typed traversal
     - Yes
     - No
     - DB API supports ``direction="in"``. Cypher support for ``<-[:TYPE]-`` is not implemented yet.
   * - Undirected typed traversal
     - Yes
     - No
     - DB API supports ``direction="any"``. Cypher support for ``-[:TYPE]-`` is not implemented yet.
   * - Untyped BFS traversal
     - Yes
     - No
     - Available as ``GraphDB.bfs`` over legacy adjacency lists.
   * - Single-hop typed neighbor sampling
     - Yes
     - No
     - Available as ``GraphDB.sample_neighbors``.
   * - Multi-hop typed path sampling
     - Yes
     - Yes
     - Cypher exposes this through ``CALL pg.sample_typed_paths(...) YIELD path RETURN path``.
   * - Materialized sampled subgraph
     - Yes
     - No
     - Available as ``GraphDB.sample_typed_subgraph``.
   * - Property filtering with ``WHERE``
     - No shared evaluator
     - No
     - Property indexes and query predicate evaluation are future work.
   * - Property indexes
     - No
     - No
     - Property predicates currently require future full-scan/filter support or indexes.
   * - Mutating Cypher queries
     - DB mutations exist
     - No
     - Use ``put_node``, ``put_edge``, ``put_edges_bulk``, and ingestion APIs directly.

Anchored One-Hop Traversal
--------------------------

Use ``GraphDB.query`` for an anchored outgoing typed traversal. The start node
must be constrained by ``id``.

.. code-block:: python

   result = graph_db.query(
       'MATCH (d {id: "drug-1"})-[:drug-to-protein]->(p) RETURN d, p'
   )

   for record in result:
       print(record["d"].get_id, record["p"].get_id)

The result object exposes ``columns`` and ``records``:

.. code-block:: python

   print(result.columns)  # ("d", "p")
   print(len(result))

Relationship Variables
----------------------

Relationship variables can be bound and returned.

.. code-block:: python

   result = graph_db.query(
       'MATCH (d {id: "drug-1"})-[r:drug-to-disease]->(x) RETURN d, r, x'
   )

   for record in result:
       print(record["r"].get_id, record["r"].properties)

Anchored Multi-Hop Traversal
----------------------------

Cypher supports repeated outgoing typed hops from the anchored start node.

.. code-block:: python

   result = graph_db.query(
       'MATCH (d {id: "drug-1"})-[:drug-to-protein]->(p)-[:protein-to-disease]->(x) RETURN d, p, x'
   )

   for record in result:
       print(record["d"].get_id, record["p"].get_id, record["x"].get_id)

Relationship variables can be used across multiple hops as well.

.. code-block:: python

   result = graph_db.query(
       'MATCH (d {id: "drug-1"})-[r1:drug-to-protein]->(p)-[r2:protein-to-disease]->(x) RETURN r1, r2, x'
   )

Sampling Procedure
------------------

Typed path sampling is exposed as a PyGraphDB-specific procedure call. This is
not standard openCypher syntax; it delegates to ``GraphDB.sample_typed_paths``.

.. code-block:: python

   result = graph_db.query(
       'CALL pg.sample_typed_paths(["drug-1"], '
       '[{"edge_type": "drug-to-protein", "direction": "out", "sample_size": 2}, '
       '{"edge_type": "protein-to-disease", "direction": "out", "sample_size": 1}]) '
       'YIELD path RETURN path'
   )

   for record in result:
       print(record["path"])

Current Cypher Limitations
--------------------------

Unsupported Cypher features raise ``ValueError`` with a message describing the
supported subset. The current Cypher API does not yet support:

- Node labels such as ``(n:Drug)``.
- Reverse or undirected patterns such as ``<-[:TYPE]-`` or ``-[:TYPE]-``.
- Unanchored scans such as ``MATCH (n) RETURN n``.
- ``WHERE`` predicates.
- Property projections such as ``RETURN n.name``.
- ``LIMIT``, ``ORDER BY``, aggregation, joins across separate patterns, or mutation clauses.
