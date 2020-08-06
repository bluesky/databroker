******************
User Documentation
******************

.. important::

   DataBroker release 1.0 includes support for old-style "v1" usage and
   new-style "v2" usage.  This section addresses databroker's new "v2" usage.
   It is still under development and subject to change in response to user
   feedback.

   For the stable usage "v1" usage, see :ref:`v1_index`. See
   :ref:`transition_plan` for more information.

.. ipython:: python
   :suppress:

   import os
   os.makedirs('data', exist_ok=True)
   from bluesky import RunEngine
   RE = RunEngine()
   from bluesky.plans import scan
   from ophyd.sim import img, motor, motor1, motor2
   from suitcase.jsonl import Serializer
   from bluesky.preprocessors import SupplementalData
   sd = SupplementalData(baseline=[motor1, motor2])
   RE.preprocessors.append(sd)
   RE.md['proposal_id'] = 12345
   for _ in range(5):
       with Serializer('data') as serializer:
           uid, = RE(scan([img], motor, -1, 1, 3), serializer)
   RE.md['proposal_id'] = 6789
   for _ in range(7):
       with Serializer('data') as serializer:
           RE(scan([img], motor, -1, 1, 3), serializer)
   serializer.close()
   from intake.catalog.local import YAMLFileCatalog
   csx = YAMLFileCatalog('source/_catalogs/csx.yml')
   import databroker
   # Monkey-patch to override databroker.catalog so we can directly
   # add examples instead of taking the trouble to create and then clean up
   # config files or Python packages of catalogs.
   from intake.catalog.base import Catalog
   databroker.catalog = Catalog()
   databroker.catalog._entries['csx'] = csx
   for name in ('chx', 'isr', 'xpd', 'sst', 'bmm', 'lix'):
       databroker.catalog._entries[name] = Catalog()

Walkthrough
===========

Find a Catalog
--------------

When databroker is first imported, it searches for Catalogs on your system,
typically provided by a Python package or configuration file that you or an
administrator installed.

.. ipython:: python

   from databroker import catalog
   list(catalog)

Each entry is a Catalog that databroker discovered on our system. In this
example, we find Catalogs corresponding to different instruments/beamlines. We
can access a subcatalog with square brackets, like accessing an item in a
dictionary.

.. ipython:: python

   catalog['csx']

List the entries in the 'csx' Catalog.

.. ipython:: python

   list(catalog['csx'])

We see Catalogs for raw data and processed data. Let's access the raw one
and assign it to a variable for convenience.

.. ipython:: python

   raw = catalog['csx']['raw']

This Catalog contains all the raw data taken at CSX. It contains many entries,
as we can see by checking ``len(raw)`` so listing it would take awhile.
Instead, we'll look up entries by name or by search.

.. note::

   As an alternative to ``list(...)``, try using tab-completion to view your
   options. Typing ``catalog['`` and then hitting the TAB key will list the
   available entries.

   Also, these shortcuts can save a little typing.

   .. code:: python

      # These three lines are equivalent.
      catalog['csx']['raw']
      catalog['csx', 'raw']
      catalog.csx.raw  # only works if the entry names are valid Python identifiers

Look up a Run by ID
-------------------

Suppose you know the unique ID of a run (a.k.a "scan") that we want to access. Note
that the first several characters will do; usually 6-8 are enough to uniquely
identify a given run.

.. ipython:: python

   run = raw[uid]  # where uid is some string like '17531ace'

Each run also has a ``scan_id``. The ``scan_id`` is usually easier to remember
(it's a counting number, not a random string) but it may not be globally
unique. If there are collisions, you'll get the most recent match, so the
unique ID is better as a long-term reference.

.. ipython:: python

   run = raw[1]

Search for Runs
---------------

Suppose you want to sift through multiple runs to examine a range of datasets.

.. ipython:: python

   query = {'proposal_id': 12345}  # or, equivalently, dict(proposal_id=12345)
   search_results = raw.search(query)

The result, ``search_results``, is itself a Catalog.

.. ipython:: python

   search_results

We can quickly check how many results it contains

.. ipython:: python

   len(search_results)

and, if we want, list them.

.. ipython:: python

   list(search_results)

Because searching on a Catalog returns another Catalog, we refine our search
by searching ``search_results``. In this example we'll use a helper,
:class:`~databroker.queries.TimeRange`, to build our query.

.. ipython:: python

   from databroker.queries import TimeRange

   query = TimeRange(since='2019-09-01', until='2040')
   search_results.search(query)

Other sophisticated queries are possible, such as filtering for scans that
include *greater than* 50 points.

.. code:: python

    search_results.search({'num_points': {'$gt': 50}})

See MongoQuerySelectors_ for more.

Once we have a result catalog that we are happy with we can list the entries
via ``list(search_results)``, access them individually by names as in
``search_results[SOME_UID]`` or loop through them:

.. ipython:: python

   for uid, run in search_results.items():
       # Do stuff
       ...

Access Data
-----------

Suppose we have a run of interest.

.. ipython:: python

   run = raw[uid]

A given run contains multiple logical tables. The number of these tables and
their names varies by the particular experiment, but two common ones are

* 'primary', the main data of interest, such as a time series of images
* 'baseline', readings taken at the beginning and end of the run for alignment
  and sanity-check purposes

To explore a run, we can open its entry by calling it like a function with no
arguments:

.. ipython:: python

    run()  # or, equivalently, run.get()

We can also use tab-completion, as in ``entry['`` TAB, to see the contents.
That is, the Run is yet another Catalog, and its contents are the logical
tables of data. Finally, let's get one of these tables.

.. ipython:: python

   ds = run.primary.read()
   ds

This is an xarray.Dataset. You can access specific columns

.. ipython:: python

   ds['img']

do mathematical operations

.. ipython:: python

   ds.mean()

make quick plots

.. ipython:: python

   @savefig ds_motor_plot.png
   ds['motor'].plot()

and much more. See the documentation on xarray_.

If the data is large, it can be convenient to access it lazily, deferring the
actual loading network or disk I/O. To do this, replace ``read()`` with
``to_dask()``. You still get back an xarray.Dataset, but it contains
placeholders that will fetch the data in chunks and only as needed, rather than
greedily pulling all the data into memory from the start.

.. ipython:: python

   ds = run.primary.to_dask()
   ds

See the documentation on dask_.

TODO: This is displaying numpy arrays, not dask. Illustrating dask here might
require standing up a server.

Explore Metadata
----------------

Everything recorded at the start of the run is in ``run.metadata['start']``.

.. ipython:: python

    run.metadata['start']

Information only knowable at the end, like the exit status (success, abort,
fail) is stored in ``run.metadata['stop']``.

.. ipython:: python

    run.metadata['stop']

The v1 API stored metadata about devices involved and their configuration,
accessed using ``descriptors``, this is roughly equivalent to what is available
in ``primary.metadata``. It is quite large, 

.. ipython:: python

    run.primary.metadata

It is a little flatter with a different layout than was returned by the v1 API.

Replay Document Stream
----------------------

Bluesky is built around a streaming-friendly representation of data and
metadata. (See event-model_.) To access the run---effectively replaying the
chronological stream of documents that were emitted during data
acquisition---use the ``canonical()`` method.

.. ipython:: python

   run.canonical(fill='yes')

This generator yields ``(name, doc)`` pairs and can be fed into streaming
visualization, processing, and serialization tools that consume this
representation, such as those provided by bluesky.

The keyword argument ``fill`` is required. Its allowed values are ``'yes'``
(numpy arrays)`, ``'no'`` (Datum IDs), and ``'delayed'`` (dask arrays, still
under development).

.. _MongoQuerySelectors: https://docs.mongodb.com/v3.2/reference/operator/query/#query-selectors
.. _xarray: https://xarray.pydata.org/en/stable/
.. _dask: https://docs.dask.org/en/latest/
.. _event-model: https://blueskyproject.io/event-model/
