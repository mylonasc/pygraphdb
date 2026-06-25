Cypher Queries
==============

PyGraphDB includes an initial read-only Cypher API through
``GraphDB.query(cypher)``. The current implementation is intentionally small and
maps directly to features that already have efficient database APIs: indexed
label scans, anchored typed traversal, and typed path sampling.

Relationship types are read from ``edge.properties["type"]``. Node labels are
stored natively through ``Node(labels=[...])`` and maintained in a sorted label
index.

Supported Feature Matrix
------------------------

The table below distinguishes features available through the Python database API
from features exposed through the Cypher API.

Legend: ✅ supported, 🟡 partially supported, ❌ not supported.

.. list-table:: Current DB API and Cypher API support
   :header-rows: 1
   :widths: 32 18 18 32

   * - Feature
     - DB API
     - Cypher API
     - Notes
   * - Node and edge property storage
     - ✅
     - 🟡
     - Cypher can return bound ``Node`` and ``Edge`` objects, but property projections such as ``RETURN n.name`` are not implemented yet.
   * - Native node labels
     - ✅
     - ✅
     - DB API supports ``Node(labels=[...])`` and ``nodes_by_label``. Cypher supports ``MATCH (n:Label) RETURN n``.
   * - Exact-match node property indexes
     - ✅
     - 🟡
     - DB API supports explicit indexes via ``create_node_property_index``. Cypher uses them for ``MATCH (n:Label {name: "..."}) RETURN n`` when registered.
   * - Exact-match edge property indexes
     - ✅
     - ❌
     - DB API supports explicit indexes via ``create_edge_property_index``. Cypher edge property predicates are not implemented yet.
   * - Dedicated relationship type field
     - 🟡
     - 🟡
     - Typed traversal uses ``edge.properties["type"]`` instead of a dedicated ``Edge.type`` field.
   * - Relationship type catalog
     - ✅
     - ❌
     - DB API supports ``edges_by_type``. Cypher does not yet support unanchored ``MATCH ()-[:TYPE]->()`` scans.
   * - Anchored one-hop typed traversal
     - ✅
     - ✅
     - DB API uses ``iter_typed_adjacency`` or ``neighbors_by_edge_type``. Cypher supports ``MATCH (a {id: "..."})-[:TYPE]->(b)``.
   * - Anchored multi-hop typed traversal
     - ✅
     - ✅
     - Cypher supports repeated outgoing typed hops from an anchored start node.
   * - Reverse typed traversal
     - ✅
     - ✅
     - DB API supports ``direction="in"``. Cypher supports ``<-[:TYPE]-`` from an anchored node.
   * - Undirected typed traversal
     - ✅
     - ✅
     - DB API supports ``direction="any"``. Cypher supports ``-[:TYPE]-`` from an anchored node.
   * - Untyped BFS traversal
     - ✅
     - ❌
     - Available as ``GraphDB.bfs`` over legacy adjacency lists.
   * - Single-hop typed neighbor sampling
     - ✅
     - ❌
     - Available as ``GraphDB.sample_neighbors``.
   * - Multi-hop typed path sampling
     - ✅
     - ✅
     - Cypher exposes this through ``CALL pg.sample_typed_paths(...) YIELD path RETURN path``.
   * - Materialized sampled subgraph
     - ✅
     - ❌
     - Available as ``GraphDB.sample_typed_subgraph``.
   * - Property filtering with ``WHERE``
     - 🟡
     - ❌
     - DB API has exact-match index lookup helpers, but Cypher ``WHERE`` parsing is future work.
   * - Mutating Cypher queries
     - ✅
     - ❌
     - Use ``put_node``, ``put_edge``, ``put_edges_bulk``, and ingestion APIs directly.

Indexed Label Scans
-------------------

Create nodes with native labels, then query by label without scanning every node.

.. code-block:: python

   graph_db.put_node(Node(node_id="drug-1", labels=["Drug"], properties={"name": "Aspirin"}))

   result = graph_db.query('MATCH (d:Drug) RETURN d')

   for record in result:
       print(record["d"].get_id)

Indexed Label and Property Lookup
---------------------------------

Exact-match property indexes are explicit. Register an index before relying on
it for performance-sensitive lookup.

.. code-block:: python

   graph_db.create_node_property_index("name")

   result = graph_db.query('MATCH (d:Drug {name: "Aspirin"}) RETURN d')

   for record in result:
       print(record["d"].properties["name"])

If a property index is not registered, Cypher still restricts the search to the
label index and then filters decoded nodes in Python.

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

Reverse and Undirected Traversal
--------------------------------

Anchored typed traversals can follow outgoing, incoming, or either-direction
relationships.

.. code-block:: python

   incoming = graph_db.query(
      'MATCH (p {id: "protein-1"})<-[:drug-to-protein]-(d) RETURN p, d'
   )

   undirected = graph_db.query(
      'MATCH (p {id: "protein-1"})-[:drug-to-protein]-(n) RETURN n'
   )

Direction can vary by hop:

.. code-block:: python

   result = graph_db.query(
      'MATCH (x {id: "disease-1"})<-[:protein-to-disease]-(p)<-[:drug-to-protein]-(d) RETURN x, p, d'
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

- Multiple labels in one node pattern, such as ``(n:Drug:Approved)``.
- Unanchored all-node scans such as ``MATCH (n) RETURN n``.
- ``WHERE`` predicates.
- Property projections such as ``RETURN n.name``.
- ``LIMIT``, ``ORDER BY``, aggregation, joins across separate patterns, or mutation clauses.
