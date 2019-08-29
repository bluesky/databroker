****************************
API and Design Documentation
****************************

Design
======

Large Catalogs in Intake
------------------------

A simple intake Catalog populates an internal dictionary, ``Catalog._entries``,
mapping entry names to :class:`LocalCatalogEntry` objects. This approach does
not scale to large catalogs. Instead, we override
:meth:`Catalog._make_entries_container` and return a dict-*like* object. This
object must support iteration (looping through part of all of the catalog in
order) and random access (requesting a specific entry by name) by implementing
``__iter__`` and ``__getitem__`` respectively.

It should also implement ``__contains__`` because, similarly, if
``__contains__`` is specifically implemented, Python will iterate through all the
entries and check each in turn. In this case, it is likely more efficient to
implement a ``__contains__`` method that uses ``__getitem__`` to determine
whether a given key is contained.

Finally, the Catalog itself should implement ``__len__``. If it is not
implemented, intake may obtain a Catalog's length by iterating through it
entirely, which may be costly. If a more efficient approach is possible (e.g. a
COUNT query) it should be implemented.

Integration with BlueskyRun
---------------------------

For each file format / backend (MongoDB, newline-delimited JSON, directory of
TIFFs, etc.) one needs to write a single custom catalog. Its entries, created
dynamically as described above, should be ``LocalCatalogEntry`` objects that
identify ``intake_bluesky.core.BlueskyRun`` as their driver. See below for the
arguments that they would provide to the entry so that it can instantiate the
catalog when called upon.

Core
====

.. autoclass:: intake_bluesky.core.BlueskyRun
   :members:

.. autoclass:: intake_bluesky.core.RemoteBlueskyRun
   :members:

.. autoclass:: intake_bluesky.core.BlueskyEventStream
   :members:

.. autofunction:: intake_bluesky.core.documents_to_xarray

.. autofunction:: intake_bluesky.core.parse_handler_registry

Backend-Specific Catalogs
=========================

.. note::

   These drivers are currently being developed in intake-bluesky itself, but
   will eventually be split out into separate repositories to isolate
   dependencies and release cycles. This will be done once the interface with
   core is deemed stable.

.. autoclass:: intake_bluesky.mongo_normalized.BlueskyMongoCatalog
   :members:

.. autoclass:: intake_bluesky.jsonl.BlueskyJSONLCatalog
   :members:
