***************************
Administrator Documentation
***************************

When databroker is imported, it discovers catalogs available on the system.
User can list the discovered catalogs by importing a special global ``catalog``
object and listing its entries.

.. code:: python

   from databroker import catalog
   list(catalog)

This is the easiest way to users to discover what catalogs are available.
Catalogs can be provided in one of three ways:

1. Old-style "databroker v0.x" configuration files, for backward-compatibility
2. Intake-style catalog YAML files
3. Python packages that advertise catalogs via the ``intake.catalogs``
   entrypoint

Old-style databroker config
===========================

DataBroker v0.x used a custom YAML-based configuration file structure. See
:doc:`v1/configuration`. For backward-compatibility, configuration files
specifying MongoDB storage will be discovered and included in
``databroker.catalog``.

Migrating sqlite or HDF5 storage
--------------------------------

``databroker.v0`` interfaces with storage in MongoDB, sqlite, and HDF5.
``databroker.v1`` and ``databroker.v2`` drop support for sqlite and HDF5 and
add support for JSONL (newline-delimited JSON) and msgpack. For binary
file-based storage, we recommend using msgpack. Data can be migrated from
sqlite or HDF5 to msgpack like so:

.. code-block:: python

   import databroker.v0
   import suitcase.msgpack

   old = databroker.v0.Broker.named(YOUR_BROKER_NAMED)
   # Loop through every run in the old Broker.
   for run in old():
       # Load all the documents out of this run from their existing format and
       # write them into one file located at
       # `<DESTINATION_DIRECTORY>/<uid>.msgpack`.
       suitcase.msgpack.export(run.documents(), DESTINATION_DIRECTORY)

Create a configuration file somewhere in intake's search path, either at a
system location or user location. Identify these like so:

.. code:: python

   from intake.catalog.default import user_data_dir, global_data_dir 

   user_data_dir()
   global_data_dir()

.. code:: yaml

   sources:
     YOUR_BROKER_NAME:
       driver: bluesky-msgpack-catalog
       paths:
         - "DESTINATION_DIRECTORY/*.msgpack"

Now ``list(databroker.catalog)`` should include an entry ``YOUR_BROKER_NAME``.

Python packages
===============

To distribute catalogs to users, it may be more convenient to provide an
installable Python or conda package, rather than place files at specific
locations on their system. Intake uses 
