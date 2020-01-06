.. currentmodule:: databroker.core

***********************
Developer Documentation
***********************

Design
======

Intake Concepts
---------------

Intake has a notion of Catalogs. Catalogs are roughly dict-like. Iterating over
a Catalog yields the names of its entries, which are strings. Iterating over
``catalog.items()`` yields ``(name, Entry)`` pairs. An Entry is roughly like
a ``functools.partial`` with metadata and intake-specific semantics. When an
Entry is opened, by calling it ``entry.get()`` or, equivalently and more
succinctly, ``entry()``, it returns its content. The content could be another
Catalog or a DataSource.

Calling ``.read()`` on a DataSource returns some in-memory representation, such
as a numpy array, pandas DataFrame, or xarray.Dataset. Calling ``.to_dask()``
return the "lazy" dask-backed equivalent structure.

DataBroker Concepts
-------------------

DataBroker represents a Bluesky "Event Stream", a logical table of data, as a
DataSource, :class:`BlueskyEventStream`. Calling
:meth:`BlueskyEventStream.read` returns an xarray Dataset backed by numpy
arrays; calling :meth:`BlueskyEventStream.to_dask` returns an xarray Dataset
backed by dask arrays.

DataBroker represents a Bluesky Run, sometimes loosely referred to as a "scan",
as a Catalog of Event Streams, :class:`BlueskyRun`. For example, the entries in
a :class:`BlueskyRun` might have the names ``'primary'`` and ``'baseline'``.
The entries always contain instances of :class:`BlueskyEventStream`.
:class:`BlueskyRun` extends the standard Catalog interface with a special
method :meth:BlueskyRun.canonical`. This returns a generator that yields
``(name, doc)`` pairs, recreating the stream of documents that would have been
emitted during data acquisition. (This is akin to ``Header.documents()`` in
DataBroker v0.x.)

:class:`BlueskyEventStream` and :class:`BlueskyRun` should never be
instantiated by the user. They have complex signatures, and they are agnostic
to the storage mechanism; they could be backed by objects in memory, files, or
databases.

Continuing to move up the hierarchy, we get to catalogs whose Entries contain
:class:`BlueskyRun` instances. Each entry's name is the corresponding RunStart
``uid``. The Catalogs at this level of the hierarchy include:

.. currentmodule:: databroker

* :class:`_drivers.jsonl.BlueskyJSONLCatalog`
* :class:`_drivers.msgpack.BlueskyMsgpackCatalog`
* :class:`_drivers.mongo_normalized.BlueskyMongoCatalog`
* :class:`_drivers.mongo_embedded.BlueskyMongoCatalog`

Notice that these are located in an internal package, ``_drivers``.  Except for
testing purposes, they should never be directly imported. They should be
accessed by their name from intake's driver registry as in:

.. code:: python

   import intake
   cls = intake.registry['bluesky-jsonl-catalog']

At some point in the future, once the internal APIs stabilize, these classes
and their specific dependencies (msgpack, pymongo, etc.) will be moved out of
databroker into separate packages. Avoid directly importing from ``_drivers``
so that this change will not break your code.

Scaling Intake Catalogs
-----------------------

To make Catalogs scale to tens of thousands of entries, override the methods:

* ``__iter__``
* ``__getitem__``
* ``__contains__``
* ``__len__``

A simple intake Catalog populates an internal dictionary, ``Catalog._entries``,
mapping entry names to :class:`LocalCatalogEntry` objects. This approach does
not scale to catalogs with large number of entries, where merely populating the
keys of the ``Catalog._entries`` dict is expensive. To customize the type of
``_entries`` override :meth:`Catalog._make_entries_container` and return a
dict-*like* object. This object must support iteration (looping through part or
all of the catalog in order) and random access (requesting a specific entry by
name) by implementing ``__iter__`` and ``__getitem__`` respectively.

It should also implement ``__contains__`` because, similarly, if
``__contains__`` is specifically implemented, Python will iterate through all the
entries and check each in turn. In this case, it is likely more efficient to
implement a ``__contains__`` method that uses ``__getitem__`` to determine
whether a given key is contained.

Finally, the Catalog itself should implement ``__len__``. If it is not
implemented, intake may obtain a Catalog's length by iterating through it
entirely, which may be costly. If a more efficient approach is possible (e.g. a
COUNT query) it should be implemented.

.. toctree::

   reference
