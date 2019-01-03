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

.. autosummary::
   :toctree: generated
   :nosignatures:

   Broker

Making a Broker
===============

You can instantiate a :class:`Broker` by passing it a dictionary of
configuration or by providing the name of a configuration file on disk.

.. autosummary::
   :toctree: generated
   :nosignatures:

   Broker.from_config
   Broker.named

Click the links the table above for details and examples.

You can also add *EventSource* instances to a *Broker* once it
is constructed

.. autosummary::
   :toctree: generated
   :nosignatures:

   Broker.add_event_source
   Broker.event_sources_by_name

Searching
=========

.. autosummary::
   :toctree: generated
   :nosignatures:

   Broker.__call__
   Broker.__getitem__

For some :class:`Broker` instance named ``db``, ``db()`` invokes
:meth:`Broker.__call__` and ``db[]`` invokes :meth:`Broker.__getitem__`.
Again, click the links the table above for details and examples.

Loading Data
============

These methods are an older way to access data, like this:

.. code-block:: python

   header = db[-1]
   db.get_table(header)

The newer :class:`Header` methods, :ref:`documented later on this page
<header_api>` are more convenient.

.. code-block:: python

   header = db[-1]
   header.table()

(Notice that we only had to type ``db`` once.) However, these are
still useful to loading data from *multiple* headers at once, which
the new methods cannot do:

.. code-block:: python

   headers = db[-10:]  # the ten most recent runs
   db.get_table(headers)

.. autosummary::
   :toctree: generated
   :nosignatures:

   Broker.get_documents
   Broker.get_events
   Broker.get_table
   Broker.get_images
   Broker.restream
   Broker.process
   Broker.fill_events
   Broker.fill_table


The broker also has a number of methods to introspect headers:

.. autosummary::
   :toctree: generated
   :nosignatures:

   Broker.get_fields
   Broker.stream_names_given_header


Saving Data
===========

.. autosummary::
   :toctree: generated
   :nosignatures:

   Broker.insert

Configuring Filters and Aliases
===============================

.. autosummary::
   :toctree: generated
   :nosignatures:

   Broker.add_filter
   Broker.clear_filters
   Broker.alias
   Broker.dynamic_alias

This current list of filters and aliases is accessible via the attributes
:class:`Broker.filters` and :class:`Broker.aliases` respectively. Again, click
the links in the table for examples.

Export Data to Another Broker
=============================

.. autosummary::
   :toctree: generated
   :nosignatures:

   Broker.export
   Broker.export_size
   Broker.get_resource_uids


.. _controlling_return_type:

Advanced: Controlling the Return Type
=====================================

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
   :toctree: generated
   :nosignatures:

    wrap_in_deprecated_doct
    wrap_in_doct

.. _header_api:

The Header object
-----------------

.. autosummary::
   :toctree: generated
   :nosignatures:

   Header

Metadata
========

The :class:`Header` bundles together the metadata of a run, accessible via the
attributes corresponding to the underlying documents:


* :class:`Header.start`
* :class:`Header.stop`

.. autosummary::
   :toctree: generated
   :nosignatures:

   Header.descriptors

Measurements are organized into "streams" of asynchronously collected data. The
names of all the streams are listed in the attribute
:attr:`Header.stream_names`.

.. note::

    It helps to understand how data and metadata are organized in our document
    model. This is covered well in `this section of the bluesky documentation
    <https://nsls-ii.github.io/bluesky/documents.html>`_.

The information in these documents is a lot to navigate. Convenience methods
make it easier to extract some key information:

.. autosummary::
   :toctree: generated
   :nosignatures:

   Header.fields
   Header.devices
   Header.config_data
   Header.stream_names

Data
====

The :class:`Header` provides various methods for loading the 'Event' documents,
which may be large. They all access the same data, presented in various ways
for convenience.

.. autosummary::
   :toctree: generated
   :nosignatures:

   Header.table
   Header.data
   Header.documents
   Header.events

All of the above accept an argument called ``stream_name``, which distinguishes
concurrently-collected stream of data. (Typical names include 'primary' and
'baseline'.) A list of all a header's stream names is accessible via the
attribute :class:`Header.stream_names`, a list.

To request data from *all* event streams at once, use the special constant
:data:`databroker.ALL`.


``dict`` compatibility
======================

:class:`Header` objects duck-type as dictionaries to maintain back-compatibility.


.. autosummary::
   :toctree: generated
   :nosignatures:

   Header.items
   Header.keys
   Header.values
   Header.get

Additionally we maintain compatibility with :class:`~doct.Doct`

.. autosummary::
   :toctree: generated
   :nosignatures:

   Header.to_name_dict_pair

Constructor
===========

There is a helper class method to build a :class:`Header` instance given
a :class:`Broker` instance and a run start.

.. autosummary::
   :toctree: generated
   :nosignatures:

   Header.from_run_start


.. _configuration_utilities:

Configuration Utilities
-----------------------

.. autosummary::
   :toctree: generated
   :nosignatures:

   list_configs
   lookup_config
   temp_config
   Broker.get_config

See also the Broker methods :meth:`Broker.from_config` and
:meth:`Broker.named`.

Internals
---------

.. autosummary::
   :toctree: generated
   :nosignatures:

   Broker.reg
   Broker.fetch_external

Deprecated
----------

.. autosummary::
   :toctree: generated
   :nosignatures:

   Broker.stream
   Header.stream
   Broker.fill_event
   Broker.fs
   Broker.mds
