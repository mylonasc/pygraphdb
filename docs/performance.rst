Performance and RocksDB Tuning
==============================

PyGraphDB includes a benchmark script for ingestion and typed sampling, plus a
RocksDB tuning campaign script for the optional PyRex backend.

Install Optional Dependencies
-----------------------------

.. code-block:: sh

   python -m pip install -e ".[leveldb,rocksdb,fast-ingest]"

Run Backend Benchmarks
----------------------

Compare the standard backends on the same append-only workload:

.. code-block:: sh

   python benchmarks.py --backend leveldb --nodes 20000 --edges 100000 --batch-size 10000 --append-only
   python benchmarks.py --backend rocksdb --nodes 20000 --edges 100000 --batch-size 10000 --append-only

Tune RocksDB
------------

Run a small RocksDB tuning matrix against the LevelDB baseline:

.. code-block:: sh

   python scripts/tune_rocksdb.py --nodes 20000 --edges 100000 --batch-size 10000

Benchmarks
----------

Graph Ingestion, BFS, and Sampling Matrix
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``scripts/benchmark_matrix.py`` to compare LevelDB and RocksDB across graph
sizes, core counts, and ingestion paths:

.. code-block:: sh

   uv run python scripts/benchmark_matrix.py \
      --sizes 10000 100000 1000000 \
      --cores 1 2 4 \
      --backends leveldb rocksdb \
      --ingestion-modes object arrow polars \
      --rocksdb-configs parallel-buffer64mb-bloom10 \
      --chunk-size 100000 \
      --samples 1000 \
      --sample-size 5 \
      --bfs-limit 100000 \
      --output-dir benchmark_results/matrix_YYYYMMDD

The matrix writes ``matrix_results.csv`` and ``matrix_results.jsonl``. It closes
and reopens the database before traversal workloads so BFS and sampling do not
measure only Python-side object state. BFS uses typed adjacency records so the
same traversal can run after object, Arrow, and Polars ingestion.

The ingestion modes do not have identical semantics:

``object``
   Uses ``put_nodes`` and ``put_edges_bulk``. It writes edge records, typed
   adjacency, relationship indexes, and legacy adjacency blobs.

``arrow`` and ``polars``
   Use append-only columnar ingestion. They write edge records, typed adjacency,
   and relationship indexes, but intentionally skip legacy adjacency blob
   rewrites. LevelDB uses the generic append-only fallback; only RocksDB can use
   PyRex's native ``write_columnar_batch`` fast path when available.

Compaction-Pressure Benchmark
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``scripts/benchmark_rocksdb_compaction.py`` for a workload designed to show
where RocksDB's background compaction parallelism helps. The benchmark repeatedly
overwrites the same key set using a permuted key order. This creates overlapping
SST ranges and sustained LSM compaction pressure.

.. code-block:: sh

   uv run python scripts/benchmark_rocksdb_compaction.py \
      --configs leveldb rocksdb-p1-bg1-smallbuf rocksdb-p4-bg4-smallbuf rocksdb-p8-bg8-smallbuf rocksdb-p4-bg4-largebuf \
      --keys 250000 \
      --passes 6 \
      --batch-size 5000 \
      --value-size 1024 \
      --write-buffer-size 2097152 \
      --output-dir benchmark_results/compaction_pressure_YYYYMMDD

The script writes ``compaction_pressure_results.csv`` and
``compaction_pressure_results.jsonl``. PyRex does not currently expose RocksDB
properties such as compaction-pending, level sizes, or statistics, so the script
records SST/log file counts and sizes as indirect evidence of flush and
compaction behavior.

A local run on 2026-06-25 produced:

================================= =========== ===================== =================== =============
Configuration                     Backend     Initial write rate    Overwrite avg rate  Final SSTs
================================= =========== ===================== =================== =============
LevelDB                           LevelDB     329,433 writes/s      114,663 writes/s    30
RocksDB p1/bg1 small buffer       RocksDB     694,105 writes/s      262,405 writes/s    14
RocksDB p4/bg4 small buffer       RocksDB     1,008,871 writes/s    749,948 writes/s    47
RocksDB p8/bg8 small buffer       RocksDB     987,248 writes/s      772,436 writes/s    17
RocksDB p4/bg4 large buffer       RocksDB     1,088,475 writes/s    1,132,815 writes/s  7
================================= =========== ===================== =================== =============

This is the benchmark case where the RocksDB-backed database performs clearly
better. The workload creates many overlapping sorted runs; RocksDB can flush and
compact them with multiple background jobs, while LevelDB has a much narrower
background-compaction model. Larger write buffers also help RocksDB by reducing
flush frequency and creating fewer, larger compaction inputs.

This result should not be generalized to every graph workload. Append-only graph
ingestion with Python object construction, serialization, key construction, and
index maintenance can be dominated by Python-side overhead. In those cases,
RocksDB's compaction parallelism is not necessarily the bottleneck, so LevelDB
can appear competitive or faster.

ArcadeDB Comparison Benchmark
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``scripts/benchmark_arcadedb_vs_pygraphdb.py`` to compare the
RocksDB-backed pygraphdb path with ArcadeDB's native property graph engine. The
ArcadeDB side uses the ``arcadedb-embedded`` Python package, which runs ArcadeDB
in-process through JPype/JVM bindings and does not require a separate server.
The suite is intentionally mixed: some workloads favor RocksDB/PyRex write
paths, while others favor ArcadeDB's index-free adjacency traversal.

Run a pygraphdb-only smoke test:

.. code-block:: sh

   uv run python scripts/benchmark_arcadedb_vs_pygraphdb.py \
      --engines pygraphdb \
      --nodes 10000 \
      --edges 50000 \
      --iterations 25 \
      --output-dir benchmark_results/arcadedb_vs_pygraphdb_YYYYMMDD

To include embedded ArcadeDB without adding it to the project dependencies, use
``uv --with``:

.. code-block:: sh

   uv run --with arcadedb-embedded python scripts/benchmark_arcadedb_vs_pygraphdb.py \
      --engines pygraphdb arcadedb \
      --workloads columnar_ingest star_traversal bfs_depth typed_path rocksdb_compaction \
      --nodes 100000 \
      --edges 500000 \
      --batch-size 100000 \
      --iterations 100 \
      --repetitions 10 \
      --arcadedb-heap-size 4g \
      --arcadedb-parallel 4 \
      --output-dir benchmark_results/arcadedb_vs_pygraphdb_YYYYMMDD

The script writes ``arcadedb_vs_pygraphdb_results.csv`` and
``arcadedb_vs_pygraphdb_results.jsonl`` with raw per-repetition rows, plus
``arcadedb_vs_pygraphdb_summary.csv`` and
``arcadedb_vs_pygraphdb_summary.jsonl`` with mean and sample standard deviation
by engine and workload. If ``arcadedb-embedded`` is not installed, ArcadeDB rows
are emitted as ``status=skipped`` and pygraphdb rows still run.

The workloads are:

``columnar_ingest``
   Compares pygraphdb's serialized Arrow ingestion and RocksDB native
   ``write_columnar_batch`` fast path, when available, with ArcadeDB's embedded
   ``GraphBatch`` importer. Both load attributed vertices and typed graph edges.

``star_traversal``
   Builds one high-degree hub and repeatedly expands outgoing ``RelA``
   neighbors. This is a case where ArcadeDB's vertex-local edge segments and
   index-free adjacency can be competitive or faster than prefix scans over a KV
   store.

``bfs_depth``
   Runs bounded-depth traversal from ``n0``. pygraphdb uses typed adjacency
   records; ArcadeDB uses SQL ``MATCH`` over outgoing ``RelA`` edges.

``typed_path``
   Repeatedly follows a typed ``RelA`` then ``RelB`` path. This exercises
   property-graph typed edge expansion rather than raw key/value writes.

``rocksdb_compaction``
   Runs a permuted repeated-overwrite workload directly against the pygraphdb
   RocksDB store. ArcadeDB rows are marked not applicable because this workload
   targets raw LSM compaction behavior rather than a property-graph operation.

Interpret results by workload instead of expecting one global winner. RocksDB is
expected to show its strength on compaction-sensitive overwrites and append-only
columnar ingestion. ArcadeDB is expected to be strongest when the query can start
from an indexed vertex and then stay on native adjacency chains for hub, BFS, or
typed-path expansion.

Columnar Ingestion Benchmark
----------------------------

PyGraphDB ``0.2.0a0`` adds serialized Arrow/Polars columnar ingestion for
attributed nodes and typed edges. With ``PyRexStore`` and
``pyrex-rocksdb>=0.3.0a0``, the store uses PyRex's native
``write_columnar_batch`` API when available.

The new APIs require caller-provided serialized ``node_value`` and
``edge_value`` payload columns. This keeps ``get_node`` and ``get_edge``
compatible with the configured serializer and avoids a storage-format migration.
Edge columnar ingestion is append-only in this first release and writes edge
records plus typed adjacency records; it intentionally skips legacy adjacency
blob rewrites.

See ``notebooks/05_columnar_ingestion_benchmark.ipynb`` for a runnable
comparison. A local run on ``10,000`` nodes and ``50,000`` edges with batch size
``10,000`` produced:

============================== =============== ================
Mode                           Node rate       Edge insert rate
============================== =============== ================
LevelDB object batch           1,110,296/s     167,463 edges/s
RocksDB Arrow columnar native  1,035,690/s     265,050 edges/s
RocksDB Polars columnar native 929,044/s       250,517 edges/s
============================== =============== ================

The edge-ingestion speedup was roughly ``1.5x`` to ``1.6x`` over the LevelDB
object-batch baseline for this small benchmark. Larger append-only workloads
with already-serialized columnar payloads are expected to benefit more because
the native path avoids per-row backend batch calls.

Initial Local Findings
----------------------

An initial campaign on ``10,000`` nodes and ``50,000`` edges found that the
current RocksDB backend is functional but does not yet beat LevelDB on append-only
edge ingestion:

=============================== ================ =====================
Configuration                   Edge insert rate Neighbor sample rate
=============================== ================ =====================
LevelDB                         168,217 edges/s  159,546 seeds/s
RocksDB default                 121,789 edges/s  178,704 seeds/s
RocksDB Bloom filter            128,184 edges/s  183,307 seeds/s
RocksDB parallelism + buffer    125,531 edges/s  187,759 seeds/s
RocksDB no WAL                  127,135 edges/s  183,348 seeds/s
=============================== ================ =====================

The first profile of the best RocksDB run showed substantial Python-side time in
serialization, key construction, string encoding, and adjacency blob merging.
That suggests the next major RocksDB improvement is not just more RocksDB tuning;
it is reducing Python overhead and moving toward prefix-key adjacency records
instead of rewriting serialized adjacency blobs.

The script writes:

``benchmark_results/rocksdb_tuning_results.json``
   Full command output and parsed metrics.

``benchmark_results/rocksdb_tuning_results.csv``
   A compact table for comparing rates across configurations.

To collect a ``cProfile`` file for the best RocksDB configuration:

.. code-block:: sh

   python scripts/tune_rocksdb.py --profile

RocksDB Knobs Exposed by PyRexStore
-----------------------------------

``parallelism``
   Calls RocksDB's ``increase_parallelism`` to raise background thread counts.

``max_background_jobs``
   Sets the maximum number of RocksDB background jobs.

``write_buffer_size``
   Sets write buffer sizes for the database and column-family options.

``bloom_bits_per_key``
   Enables a block-based Bloom filter with the requested bits per key.

``disable_wal``
   Disables the RocksDB write-ahead log. This can improve ingestion throughput
   in some workloads, but it weakens durability and should be treated as a bulk
   loading mode, not a safe default.
