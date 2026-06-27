Performance and Benchmarks
==========================

PyGraphDB includes benchmark scripts for ingestion, traversal, sampling, RocksDB
tuning, and an optional ArcadeDB comparison. Treat the included local results as
directional examples, not universal claims.

Install Benchmark Dependencies
------------------------------

.. code-block:: sh

   python -m pip install -e ".[leveldb,rocksdb,fast-ingest]"

Backend Benchmarks
------------------

Use ``benchmarks.py`` for a quick backend comparison on the same append-only
workload.

.. code-block:: sh

   python benchmarks.py --backend leveldb --nodes 20000 --edges 100000 --batch-size 10000 --append-only
   python benchmarks.py --backend rocksdb --nodes 20000 --edges 100000 --batch-size 10000 --append-only

Use ``scripts/benchmark_matrix.py`` for larger matrix runs across graph sizes,
backends, core counts, and ingestion modes.

.. code-block:: sh

   uv run python scripts/benchmark_matrix.py \
      --sizes 10000 100000 1000000 \
      --cores 1 2 4 \
      --backends leveldb rocksdb \
      --ingestion-modes object arrow polars \
      --chunk-size 100000 \
      --samples 1000 \
      --sample-size 5 \
      --output-dir benchmark_results/matrix_YYYYMMDD

The matrix writes CSV and JSONL outputs and reopens the database before traversal
workloads so traversal does not only measure Python-side object state.

Columnar Ingestion Benchmarks
-----------------------------

Columnar ingestion accepts already-serialized node and edge payloads. With
``PyRexStore`` and ``pyrex-rocksdb>=0.3.0a0``, RocksDB can use PyRex's native
``write_columnar_batch`` path.

See ``notebooks/05_columnar_ingestion_benchmark.ipynb`` for a runnable example.
A local run on 10,000 nodes and 50,000 edges with batch size 10,000 produced:

============================== =============== ================
Mode                           Node rate       Edge insert rate
============================== =============== ================
LevelDB object batch           1,110,296/s     167,463 edges/s
RocksDB Arrow columnar native  1,035,690/s     265,050 edges/s
RocksDB Polars columnar native 929,044/s       250,517 edges/s
============================== =============== ================

Larger append-only workloads with pre-serialized columnar payloads are expected
to benefit more than small runs dominated by Python object construction.

RocksDB Tuning and Compaction Benchmarks
----------------------------------------

Use ``scripts/tune_rocksdb.py`` for a small RocksDB tuning matrix against a
LevelDB baseline.

.. code-block:: sh

   python scripts/tune_rocksdb.py --nodes 20000 --edges 100000 --batch-size 10000

Use ``scripts/benchmark_rocksdb_compaction.py`` for a repeated-overwrite workload
that creates compaction pressure.

.. code-block:: sh

   uv run python scripts/benchmark_rocksdb_compaction.py \
      --configs leveldb rocksdb-p1-bg1-smallbuf rocksdb-p4-bg4-smallbuf rocksdb-p8-bg8-smallbuf rocksdb-p4-bg4-largebuf \
      --keys 250000 \
      --passes 6 \
      --batch-size 5000 \
      --value-size 1024 \
      --output-dir benchmark_results/compaction_pressure_YYYYMMDD

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

This workload favors RocksDB because it creates overlapping sorted runs that can
benefit from background compaction parallelism. It should not be generalized to
all graph workloads.

ArcadeDB Comparison Benchmarks
------------------------------

Use ``scripts/benchmark_arcadedb_vs_pygraphdb.py`` to compare PyGraphDB with the
optional embedded ArcadeDB package. ArcadeDB is not required for normal PyGraphDB
use.

Run a PyGraphDB-only smoke test:

.. code-block:: sh

   uv run python scripts/benchmark_arcadedb_vs_pygraphdb.py \
      --engines pygraphdb \
      --nodes 10000 \
      --edges 50000 \
      --iterations 25 \
      --output-dir benchmark_results/arcadedb_vs_pygraphdb_YYYYMMDD

Include embedded ArcadeDB with ``uv --with``:

.. code-block:: sh

   uv run --with arcadedb-embedded python scripts/benchmark_arcadedb_vs_pygraphdb.py \
      --engines pygraphdb arcadedb \
      --workloads columnar_ingest star_traversal bfs_depth typed_path rocksdb_compaction \
      --nodes 100000 \
      --edges 500000 \
      --batch-size 100000 \
      --iterations 100 \
      --repetitions 10 \
      --output-dir benchmark_results/arcadedb_vs_pygraphdb_YYYYMMDD

The script writes raw rows and summary files grouped by engine and workload. If
``arcadedb-embedded`` is not installed, ArcadeDB rows are marked skipped and
PyGraphDB rows still run.

Representative small local results from 2026-06-25:

=================== ================= ================= ================
Workload            PyGraphDB/RocksDB ArcadeDB embedded Relative result
=================== ================= ================= ================
columnar_ingest     0.0358 s          0.0506 s          PyGraphDB 1.41x faster
star_traversal      0.0383 s          0.0333 s          ArcadeDB 1.15x faster
bfs_depth           0.0303 s          0.0366 s          PyGraphDB 1.21x faster
typed_path          0.0293 s          0.0404 s          PyGraphDB 1.38x faster
rocksdb_compaction  0.0022 s          Not applicable    PyGraphDB only
=================== ================= ================= ================

Interpret these results by workload. RocksDB/PyGraphDB tends to show strength on
append-only columnar ingestion and compaction-sensitive raw writes. ArcadeDB can
be strongest when queries start from an indexed vertex and stay on native
adjacency chains.

Benchmark Caveats
-----------------

- Local benchmark results depend on graph shape, storage device, CPU settings,
  Python version, backend versions, and warm-up behavior.
- Small graphs can be dominated by Python object construction, serialization, and
  key construction rather than backend I/O.
- Prefer raw CSV/JSONL outputs for comparisons and keep benchmark parameters with
  published results.
