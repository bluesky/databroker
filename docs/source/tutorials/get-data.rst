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

Access a stream by name. This returns an `xarray`_ Dataset.

.. ipython:: python

   ds = run.primary.read()
   ds

Access columns, as in ``ds["I0"]``. This returns an `xarray`_ DataArray.

.. ipython:: python

   ds["I0"].head()  # Just show the first couple elements.

Do math on columns.

.. ipython:: python

   normed = ds["I0"] / ds["It"]
   normed.head()  # Just show the first couple elements.

These `xarray`_ objects bundle a numpy (or numpy-like) array with some
additional metadata and coordinates. To access the underlying array directly,
use the ``data`` accessor.

.. ipython:: python

   type(ds["I0"])
   type(ds["I0"].data)

Handle large data
-----------------

The example data we have been using has no large arrays in it. For this section
we will download a second Catalog with one Run in it that contains image
data.

.. ipython:: python

   import databroker.tutorial_utils
   databroker.tutorial_utils.fetch_RSOXS_example()

Access the catalog as assign it to a variable for convenience.

.. ipython:: python

   import databroker
   run = databroker.catalog['bluesky-tutorial-RSOXS']['777b44a']

The method ``run.primary.read()`` method reads all the data from the "primary"
stream from storage into memory. This can be inconvenient if:

1. The data is so large it does not all fit into memory (RAM) at once.
2. You don't actually need some of the columns for your analysis.

The ``read()`` operation could prompt a ``MemoryError`` (1) or take longer than
necessary by givng an `xarray`_ backed by large numpy arrays that we don't
actually need for our present purpose (2).

Instead, we can summon up an `xarray`_ backed by *placeholders* (`dask`_
arrays). Large arrays are not immediately read from storage into memory. They
will only be read if and when we actually need them. Further, they can be read
progressively in chunks, enabling us to process datasets that are too large to
fit into memory all at once.

.. ipython:: python

   lazy_ds = run.primary.to_dask()

Compare the columns of ``lazy_ds`` and ``ds``. Notice, below, that the "lazy"
variant contains ``<dask.array ...>`` and the original contains
``<numpy.ndarray ...>``.

.. ipython:: python

   ds = run.primary.read()  # Load non-lazy version for comparison.
   ds["Synced_saxs_image"]
   lazy_ds["Synced_saxs_image"]

We can examine the dask array directly by using that ``data`` accessor.

.. ipython:: python

   lazy_ds["Synced_saxs_image"].data

As an example of what's possible, we can subtract from this image series the
mean of an image series taken while the shutter was closed ("dark" images).

.. ipython:: python

   corrected = run.primary.to_dask()["Synced_saxs_image"] - run.dark.to_dask()["Synced_saxs_image"].mean("time")

At this point, *no data has yet been read*. We are still working with
placeholders, building up an expression of work to be done in the future.
If we attempt to plot it or otherwise hand it off to code that will treat it as
normal array, the data will be loaded and processed (in chunks) and finally
give us a normal numpy array as a result. We can force it to do so explicltly
by calling ``.compute()``.

If we subselect the data, only the relevant chunk(s) will be loaded. This can
save a lot of time and memory.

.. ipython:: python

   # Take a slice from the middle.
   corrected[100, 0, :, :].compute()

For more, see the dask documentation. A good entry point is the example
covering `Dask Arrays`_.

.. _xarray: https://xarray.pydata.org/

.. _dask: https://dask.org/

.. _Dask Arrays: https://examples.dask.org/array.html
