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
