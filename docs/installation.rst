Installation
============

Python Version
--------------

PyGraphDB targets Python 3.9 through 3.13. The LevelDB backend uses ``plyvel``;
at the time of writing, ``plyvel`` wheels are not available for Python 3.14 or
free-threaded Python builds. Use Python 3.12 or 3.13 for notebooks that need the
LevelDB backend.

Install With uv
---------------

From a local checkout:

.. code-block:: sh

   uv sync

For editable development:

.. code-block:: sh

   uv add --editable /path/to/pygraphdb

Install optional features only when you need them:

.. code-block:: sh

   uv add "/path/to/pygraphdb[lmdb,msgpack,protobuf]"
   uv add "/path/to/pygraphdb[leveldb]"

Install With pip
----------------

.. code-block:: sh

   python -m venv .venv
   . .venv/bin/activate
   python -m pip install --upgrade pip
   python -m pip install .

Install optional backends and serializers:

.. code-block:: sh

   python -m pip install ".[lmdb]"
   python -m pip install ".[leveldb]"
   python -m pip install ".[msgpack,protobuf]"
   python -m pip install ".[all]"

Install From GitHub
-------------------

.. code-block:: sh

   python -m pip install "pygraphdb[all] @ git+https://github.com/mylonasc/pygraphdb.git"

Notebook Kernel Example
-----------------------

Use a Python version supported by your selected backend and register it with
Jupyter:

.. code-block:: sh

   uv python install 3.12
   uv venv --python 3.12
   uv sync --extra leveldb --extra lmdb --extra msgpack --extra protobuf
   uv add --dev ipykernel
   .venv/bin/python -m ipykernel install --user --name pygraphdb-py312 --display-name "PyGraphDB Python 3.12"

Optional Dependencies
---------------------

``lmdb``
   LMDB key-value backend.

``leveldb``
   LevelDB key-value backend through ``plyvel``.

``rocksdb``
   RocksDB key-value backend through ``pyrex-rocksdb``.

``msgpack``
   MessagePack serializer.

``protobuf``
   Protobuf Struct serializer for JSON-like dictionaries.

``bloom``
   Bloom-filter support through ``pybloom-live``.

``docs``
   Sphinx documentation build dependencies.
