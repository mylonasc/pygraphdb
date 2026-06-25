ArcadeDB vs PyGraphDB Benchmarks
================================

This page records a local embedded ArcadeDB comparison against pygraphdb using
the RocksDB/PyRex backend. The benchmark script is
``scripts/benchmark_arcadedb_vs_pygraphdb.py``.

Benchmark Setup
---------------

Command:

.. code-block:: sh

   uv run --with arcadedb-embedded python scripts/benchmark_arcadedb_vs_pygraphdb.py \
      --engines pygraphdb arcadedb \
      --workloads columnar_ingest star_traversal bfs_depth typed_path rocksdb_compaction \
      --nodes 1000 \
      --edges 3000 \
      --batch-size 1000 \
      --iterations 5 \
      --repetitions 10 \
      --compaction-keys 1000 \
      --compaction-passes 2 \
      --arcadedb-heap-size 1g \
      --output-dir benchmark_results/arcadedb_embedded_10x_20260625

Outputs:

``benchmark_results/arcadedb_embedded_10x_20260625/arcadedb_vs_pygraphdb_results.csv``
   Per-run raw rows.

``benchmark_results/arcadedb_embedded_10x_20260625/arcadedb_vs_pygraphdb_summary.csv``
   Mean and sample standard deviation by engine and workload.

The run used Python ``3.11.14`` on Linux
``6.17.0-35-generic-x86_64``. ArcadeDB used the ``arcadedb-embedded`` package
with a ``1g`` JVM heap. The timings below are mean +/- sample standard deviation
over 10 repetitions. They include first-run Python/JVM warm-up costs, which is
why the standard deviation is high for some ingest-heavy workloads.

Overall Results
---------------

Lower total time is better.

.. list-table::
   :header-rows: 1

   * - Workload
     - PyGraphDB/RocksDB
     - ArcadeDB embedded
     - Relative result
   * - ``columnar_ingest``
     - 0.0358 +/- 0.0507 s
     - 0.0506 +/- 0.0620 s
     - PyGraphDB 1.41x faster
   * - ``star_traversal``
     - 0.0383 +/- 0.0014 s
     - 0.0333 +/- 0.0154 s
     - ArcadeDB 1.15x faster
   * - ``bfs_depth``
     - 0.0303 +/- 0.0022 s
     - 0.0366 +/- 0.0141 s
     - PyGraphDB 1.21x faster
   * - ``typed_path``
     - 0.0293 +/- 0.0023 s
     - 0.0404 +/- 0.0052 s
     - PyGraphDB 1.38x faster
   * - ``rocksdb_compaction``
     - 0.0022 +/- 0.0002 s
     - Not applicable
     - PyGraphDB only

Ingestion Results
-----------------

These timings include graph creation and loading. For ArcadeDB this uses embedded
``GraphBatch``. For pygraphdb, ``columnar_ingest`` uses Arrow/RocksDB columnar
ingestion; the traversal workloads use object ingestion so the graph can be
queried immediately afterward.

.. list-table::
   :header-rows: 1

   * - Workload
     - PyGraphDB/RocksDB
     - ArcadeDB embedded
     - Relative result
   * - ``columnar_ingest``
     - 0.0358 +/- 0.0507 s
     - 0.0506 +/- 0.0620 s
     - PyGraphDB 1.41x faster
   * - ``star_traversal``
     - 0.0286 +/- 0.0012 s
     - 0.0273 +/- 0.0046 s
     - ArcadeDB 1.05x faster
   * - ``bfs_depth``
     - 0.0297 +/- 0.0022 s
     - 0.0322 +/- 0.0083 s
     - PyGraphDB 1.08x faster
   * - ``typed_path``
     - 0.0292 +/- 0.0023 s
     - 0.0359 +/- 0.0053 s
     - PyGraphDB 1.23x faster

Query Results
-------------

These timings exclude ingestion and measure only the repeated query/traversal
portion. ``columnar_ingest`` has no query phase.

.. list-table::
   :header-rows: 1

   * - Workload
     - PyGraphDB/RocksDB
     - ArcadeDB embedded
     - Relative result
   * - ``star_traversal``
     - 0.0097 +/- 0.0003 s
     - 0.0060 +/- 0.0130 s
     - ArcadeDB 1.61x faster
   * - ``bfs_depth``
     - 0.0006 +/- 0.0000 s
     - 0.0044 +/- 0.0061 s
     - PyGraphDB 7.71x faster
   * - ``typed_path``
     - 0.0001 +/- 0.0000 s
     - 0.0045 +/- 0.0029 s
     - PyGraphDB 39.08x faster
   * - ``rocksdb_compaction``
     - 0.0022 +/- 0.0002 s
     - Not applicable
     - PyGraphDB only

Interpretation
--------------

``columnar_ingest``
   PyGraphDB/RocksDB was 1.41x faster on total time. This workload exercises the
   serialized Arrow ingestion path and RocksDB's native ``write_columnar_batch``
   support when available. ArcadeDB used embedded ``GraphBatch`` and still
   performed in the same order of magnitude for this small graph.

``star_traversal``
   ArcadeDB was 1.15x faster overall and 1.61x faster in the query phase. This
   is the workload that most directly benefits from ArcadeDB's native
   vertex-local adjacency representation. The total-time advantage is smaller
   than the query advantage because both systems still pay graph-loading costs.

``bfs_depth``
   PyGraphDB/RocksDB was 1.21x faster overall and 7.71x faster in the query
   phase. In this synthetic shape, pygraphdb's typed-adjacency prefix scans were
   faster than ArcadeDB's SQL ``MATCH`` query execution for the bounded traversal.

``typed_path``
   PyGraphDB/RocksDB was 1.38x faster overall and 39.08x faster in the query
   phase. The result favors pygraphdb's direct typed-adjacency iteration for this
   tiny two-hop pattern. It should not be generalized to complex graph patterns
   where ArcadeDB's query optimizer has more room to help.

``rocksdb_compaction``
   This workload is intentionally pygraphdb/RocksDB-only. It directly writes a
   repeated permuted overwrite pattern into RocksDB to exercise LSM compaction
   behavior, so there is no equivalent ArcadeDB property-graph result.

Important Caveats
-----------------

These are small local smoke benchmarks, not a full database benchmark campaign.
They are useful for catching regressions and showing workload-specific behavior,
but larger graphs, more repetitions, warm-up exclusion, pinned CPU frequency,
and isolated disks are needed before drawing broad conclusions.

The first repetition includes one-time costs such as JVM startup for ArcadeDB and
Python module initialization for pygraphdb's optional ingestion stack. The script
reports standard deviation so this warm-up effect is visible rather than hidden.
