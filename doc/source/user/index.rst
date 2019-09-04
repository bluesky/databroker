******************
User Documentation
******************

.. important::

   This documentation covers databroker's new "v2" usage. If you are working
   with older databroker code, see :doc:`../v1/index`.

.. ipython:: python
   :suppress:

   import os
   os.makedirs('data', exist_ok=True)
   from bluesky import RunEngine
   RE = RunEngine()
   from bluesky.plans import scan
   from ophyd.sim import det, motor, motor1, motor2
   from suitcase.jsonl import Serializer
   from bluesky.preprocessors import SupplementalData
   sd = SupplementalData(baseline=[motor1, motor2])
   RE.preprocessors.append(sd)
   RE.md['proposal_id'] = 12345
   for _ in range(5):
       with Serializer('data') as serializer:
           uid, = RE(scan([det], motor, -1, 1, 3), serializer)
   RE.md['proposal_id'] = 6789
   for _ in range(7):
       with Serializer('data') as serializer:
           RE(scan([det], motor, -1, 1, 3), serializer)
   serializer.close()
   from intake.catalog.local import YAMLFileCatalog
   csx = YAMLFileCatalog('source/_catalogs/csx.yml')
   import databroker
   from intake.catalog.base import Catalog
   databroker.cat = Catalog()
   databroker.cat._entries['csx'] = csx
   for name in ('chx', 'isr', 'xpd', 'sst', 'bmm', 'lix'):
       databroker.cat._entries[name] = Catalog()

Walkthrough
===========

Find a Catalog
--------------

When databroker is first imported, it searches for Catalogs on your system,
typically provided by a Python package that you or an administrator
installed. List the available Catalogs.

.. ipython:: python

   from databroker import cat
   list(cat)

Each entry is a Catalog that databroker found on our system. In this example,
we find Catalogs corresponding to different instruments/beamlines. We can
access a subcatalog with square brackets, like accessing an item in a
dictionary.

.. ipython:: python

   cat['csx']

List the entries in the 'csx' Catalog.

.. ipython:: python

   list(cat['csx'])

We see Catalogs for raw data and processed data. Let's access the raw one
and assign it to a variable for convenience.

.. ipython:: python

   raw = cat['csx']['raw']

This Catalog contains all the raw data taken at CSX. It contains many entries,
as we can see by checking ``len(raw)`` so listing it would take awhile.
Instead, we'll look up entries by name or by search.

.. note::

   As an alternative to ``list(...)``, try using tab-completion to view your
   options. Typing ``cat['<TAB>`` will list the available entries.

   Also, these shortcuts can save a little typing.

   .. code:: python

      # These three lines are equivalent.
      cat['csx']['raw']
      cat['csx', 'raw']
      cat.csx.raw  # only works if the entry names are valid Python identifiers

Look up a Run by ID
-------------------

Suppose know the unique ID of a run (a.k.a "scan") that we want to access. Note
that the first several characters will do; usually 6-8 are enough to uniquely
identify a given entry.

.. ipython:: python

   entry = raw[uid]  # where uid is some string like '17531ace'

Each run also has a ``scan_id``. The ``scan_id`` is usually easier to remember
(it's a counting number, not a random string) but it may not be globally
unique. If there are collisions, you'll get the most recent match, so the
unique ID is better as a long-term reference.

.. ipython:: python

   entry = raw[1]

Search for Runs
---------------

Suppose you want to sift through multiple runs to examine a range of datasets.

.. ipython:: python

   from databroker.queries import TimeRange

   search_results = raw.search(TimeRange(since='2019-09-01', until='2019-09-07'))

We can quickly check how many results that returned

.. ipython:: python

   len(search_results)

and, if we want, list them.

.. ipython:: python

   list(search_results)

Notice that ``search_results`` is itself a Catalog. We can search on the search
results to narrow them further.

.. ipython:: python

   search_results2 = search_results.search({'proposal_id': 12345})

where ``search()`` is passed a dictionary mapping search terms to values.

.. note:: 

   Sophisticated queries are possible, such as filtering for scans that include
   *greater than* 50 points.

   .. code:: python

      search_results.search({'num_points': {'$gt': 50}})

   See MongoQuerySelectors_ for more.

Once we have a result set that we are happy with we can list them and access
them individually or we can loop through them:

.. ipython:: python

   for uid, entry in search_results2.items():
       # Do stuff
       ...

Access Data as an Xarray
------------------------

Suppose we have a run of interest.

.. ipython:: python

   entry = raw[uid]

A given run contains multiple logical tables. The number of these tables and
their names varies by the particular experiment, but two common ones are

* 'primary', the main data of interest, such an a time series of images
* 'baseline', readings taken at the beginning and end of the run for alignment
  and sanity-check purposes

To explore a run, we can open it by calling it like a function with no
arguments:

.. ipython:: python

    entry()  # or, equivalently, entry.get()

We can also use tab-completion, as in ``entry['<TAB>``, to see the contents.
That is, the Run is yet another Catalog, and its contents are the logical
tables of data. Finally, let's get one of these tables.

.. ipython:: python

   ds = entry['primary'].read()
   ds

This is an xarray.Dataset. You can access specific columns

.. ipython:: python

   ds['det']

do mathematical operations

.. ipython:: python

   ds.mean()

make quick plots

.. ipython:: python

   @savefig ds_motor_plot.png
   ds['motor'].plot()

and much more. See the documentation on xarray_.

Access Data as Bluesky Documents
--------------------------------

.. ipython:: python

   entry.canonical(fill='yes')

This generator yields ``(name, doc)`` pairs and can be fed into streaming
visualization, processing, and serialization tools that consume this
representation, such as those provided by bluesky. This is the same
representation that was emitted when the data was first acquired, so the user
can apply the same streaming pipelines to data while it is being acquired and
after it is saved.

.. _MongoQuerySelectors: https://docs.mongodb.com/v3.2/reference/operator/query/#query-selectors
.. _xarray: https://xarray.pydata.org/en/stable/
