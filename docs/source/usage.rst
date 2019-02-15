=====
Usage
=====

`Launch Binder <https://mybinder.org/v2/gh/NSLS-II/intake-bluesky-demos/master>`_
to explore some interactive examples.

*This is a quick example to give a feel for how it would work to acquire
data using bluesky and access it using intake.*

Acquire data. In this case we'll serialize the stream of "documents" generated
by bluesky to simple text files. See the Binder link above for an example with
a live MongoDB.

.. ipython:: python
   :suppress:

   import logging
   logging.getLogger('bluesky').setLevel('WARNING')
   import os
   os.makedirs('data', exist_ok=True)

.. ipython:: python

    from bluesky import RunEngine
    RE = RunEngine()
    from bluesky.plans import scan
    from ophyd.sim import det, motor
    from suitcase.jsonl import Serializer
    serializer = Serializer('data')
    uid, = RE(scan([det], motor, -1, 1, 3), serializer)
    serializer.close()

We'll access the data using intake locally. See the Binder link above for an
example using a HTTP server, where the user may not have direct access to the
file system where the data files are stored.

.. ipython:: python

    from intake_bluesky.jsonl import BlueskyJSONLCatalog
    import glob
    catalog = BlueskyJSONLCatalog(glob.glob('data/*.jsonl'))

Read Data as Xarray
-------------------

.. ipython:: python

    catalog[uid]
    catalog[uid]()
    catalog[uid]()['primary']
    catalog[uid]()['primary']()
    catalog[uid]()['primary']().read()

The calls accept optional arguments. For example, we can filter by field name
to avoid reading data that we do not need.

.. ipython:: python

    catalog[uid]().primary(include=['det']).read()

If the defaults suffice, intake allows the user to elide the calls, supporting
this more succinct expression:

.. ipython:: python

    catalog[uid].primary.read()

Our simple scan produced one logical table ("event stream") named 'primary'. In
general experiments can produce multiple tables with different time-bases. They
can be merged into one xarray Dataset like so:

.. ipython:: python

    run = catalog[uid]()
    run
    import xarray
    xarray.merge(run[key].read() for key in run)

Read Data as Bluesky "Documents"
--------------------------------

.. ipython:: python

   catalog[uid].read_canonical()

This generator yields ``(name, doc)`` pairs and can be fed into streaming
visualization, processing, and serialization tools that consume this
representation, such as those provided by bluesky. This is the same
representation that was emitted when the data was first acquired, so the user
can apply the same streaming pipelines to data while it is being acquired and
after it is saved.

Search
------

The :meth:`Catalog.search` method returns another Catalog with a subset of the
original Catalog's entries. The search results can be searched in turn. Here we
search for data acquired using the 'scan' experiment plan, and then narrow that
to results from the last 60 seconds.

.. ipython:: python

    catalog2 = catalog.search({'plan_name': 'scan'})
    list(catalog2)
    import time
    catalog3 = catalog2.search({'time': {'$gt': time.time() - 60}})
    list(catalog3)

This is accomplished using `mongoquery <https://pypi.org/project/mongoquery/>`_,
which provides a MongoDB-like query language for querying Python collections.
For Catalogs backed by a real MongoDB instance, as in the Binder example linked
above, the full MongoDB query language is supported.

The Catalogs carry the composite search query as internal state.

.. ipython:: python

    catalog._query
    catalog2._query
    catalog3._query
