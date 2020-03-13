*************
API Reference
*************

Core
====

.. autoclass:: databroker.core.Document
   :members:

.. autoclass:: databroker.core.BlueskyRun
   :members:

.. autoclass:: databroker.core.RemoteBlueskyRun
   :members:

.. autoclass:: databroker.core.BlueskyEventStream
   :members:

.. autofunction:: databroker.core.discover_handlers

.. autofunction:: databroker.core.parse_handler_registry

.. autofunction:: databroker.core.parse_transforms

.. autoclass:: databroker.v2.Broker
   :members:

Utils
=====

.. autofunction:: databroker.utils.catalog_search_path

.. autofunction:: databroker.v2.temp

.. autofunction:: databroker.v1.temp

Backend-Specific Catalogs
=========================

.. note::

   These drivers are currently being developed in databroker itself, but
   will eventually be split out into separate repositories to isolate
   dependencies and release cycles. This will be done once the internal
   interfaces are stable.

.. autoclass:: databroker._drivers.jsonl.BlueskyJSONLCatalog
   :members:

.. autoclass:: databroker._drivers.mongo_embedded.BlueskyMongoCatalog
   :members:

.. autoclass:: databroker._drivers.mongo_normalized.BlueskyMongoCatalog
   :members:

.. autoclass:: databroker._drivers.msgpack.BlueskyMsgpackCatalog
   :members:
