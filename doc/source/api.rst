*****************
API Documentation
*****************

We use the :class:`Broker` to pose queries for saved data sets ("runs"). A
search query returns a lazy-loaded iterable of :class:`Header` objects.
Each :class:`Header` encapsulates the metadata for one run. It provides
convenient methods for exploring that metadata and loading the full data.

.. currentmodule:: databroker

The Broker object
-----------------

Making a Broker
===============

You can instantiate a :class:`Broker` by passing it a dictionary of
configuration or by providing the name of a configuration file on disk.

.. autosummary::
   :toctree: _as_gen
   :nosignatures:

   Broker.from_config
   Broker.named

Searching
=========

.. autosummary::
   :toctree: _as_gen
   :nosignatures:

   Broker.__call__
   Broker.__getitem__

Loading Data
============

.. autosummary::
   :toctree: _as_gen
   :nosignatures:

   Broker.get_documents
   Broker.get_events
   Broker.get_table
   Broker.get_images
   Broker.restream
   Broker.process
   Broker.fill_event

Saving Data
===========

.. autosummary::
   :toctree: _as_gen
   :nosignatures:

   Broker.insert

Configuring Filters and Aliases
===============================

.. autosummary::
   :toctree: _as_gen
   :nosignatures:

   Broker.add_filter
   Broker.clear_filters
   Broker.alias
   Broker.dynamic_alias

This current list of filters and aliases is accessible via the attributes
:class:`Broker.filters` and :class:`Broker.aliases` respectively.

Export Data to Another Broker
=============================

.. autosummary::
   :toctree: _as_gen
   :nosignatures:

   Broker.export
   Broker.export_size
   Broker.get_resource_uids

Advanced: Controlling the Return Type
=====================================

.. currentmodule:: databroker.broker

The attribute :class:`Broker.prepare_hook` is a function with the signature
``f(name, doc)`` that is applied to every document on its way out.

By default :class:`Broker.prepare_hook` is set to
:func:`wrap_in_deprecated_doct`. The resultant objects issue warnings if
users attempt to access items with dot access like ``event.data`` instead of
dict-style lookup like ``event['data']``. To restore the previous behavior (i.e.
suppress the warnings on dot access), set :class:`Broker.prepare_hook` to
:func:`wrap_in_doct`.

To obtain plain dictionaries, set :class:`Broker.prepare_hook` to
``lambda name, doc: copy.deepcopy(doc)``. (Copying of is recommended because
the underlying objects are cached and mutable.)

In a future release of databroker, the default return type may be changed to
plain dictionaries for simplicity and improved performance.

.. autosummary::
   :toctree: _as_gen
   :nosignatures:

    wrap_in_deprecated_doct
    wrap_in_doct

.. currentmodule:: databroker

The Header object
-----------------

Metadata
========

The :class:`Header` bundles together the metadata of a run, accessible via the
attributes corresponding to the underlying documents:

* :class:`Header.start`
* :class:`Header.stop`
* :class:`Header.descriptors`

The information in these documents is a lot to navigate. Convenience methods
make it easier to extract some key information:

.. autosummary::
   :toctree: _as_gen
   :nosignatures:

   Header.fields
   Header.devices
   Header.config_data

Data
====

The :class:`Header` provides various methods for loading the 'Event' documents,
which may be large. They all access the same data, presented in various ways
for convenience.

.. autosummary::
   :toctree: _as_gen
   :nosignatures:

   Header.documents
   Header.events
   Header.table

All of the above accept an argument called ``stream_name``, which distinguishes
concurrently-collected stream of data. (Typical names include 'primary' and
'baseline'.) A list of all a header's stream names is accessible via the
attribute :class:`Header.stream_names`, a list.

To requrest data from *all* event streams at once, use the special constant
:data:`databroker.ALL`.

.. _configuration_utilities:

Configuration Utilities
-----------------------

.. autosummary::
   :toctree: _as_gen
   :nosignatures:

   list_configs
   lookup_config

See also the Broker methods :meth:`Broker.from_config` and
:meth:`Broker.named`.
