Get Data from a Run
===================

Set up for Tutorial
-------------------

Start your favorite interactive Python environment, such as ``ipython`` or
``jupyter lab``.

For this tutorial, we'll use a catalog of publicly available, openly licensed
sample data. This utility downloads it and makes it discoverable to databroker.

.. ipython:: python

   import databroker.tutorial_utils
   databroker.tutorial_utils.fetch_BMM_example()

Access the catalog as assign it to a variable for convenience.

.. ipython:: python

   import databroker
   catalog = databroker.catalog['bluesky-tutorial-BMM']

Let's take a Run from this Catalog.

.. ipython:: python

   run = catalog[23463]

What's in the Run?
------------------

The Run's "pretty display", shown by IPython and Jupyter (and some other
similar tools), shows us a summary.

.. ipython:: python

   run

Each run contains logical "tables" of data called *streams*. We can see them in
the summary above, and we iterate over them programmatically with a ``for``
loop or with ``list``.

.. ipython:: python

   list(run)

Get the data
------------

Access a stream by name.

.. ipython:: python

   ds = run.primary.read()
   ds

Access columns, as in ``ds["I0"]``.

.. ipython:: python

   ds["I0"].head()  # Just show the first couple elements.

Do math on columns.

.. ipython:: python

   normed = ds["I0"] / ds["It"]
   normed.head()  # Just show the first couple elements.

These xarray objects bundle a numpy (or numpy-like) array with some additional
metadata and coordinates. To access the underlying array directly, use the
``data`` accessor.

.. ipython:: python

   type(ds["I0"])
   type(ds["I0"].data)

Handle large data sets.
-----------------------

Do a basic to_dask() example here but link to another tutorial specifically
with large data. Maybe generate this rather than downloading it, akin to 
scikit-learn ``make_*`` vs ``fetch_*``.
