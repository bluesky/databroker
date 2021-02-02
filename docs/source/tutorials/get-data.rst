Get Data from a Run
===================

In this tutorial we will:

* Load all the data from a small Run and do some basic math and visualization.
* Load and visualize just a slice of data from a 1 GB dataset, without loading
  the whole dataset.

Set up for Tutorial
-------------------

Before you begin, install ``databroker`` and ``databroker-pack``, following the
:doc:`install`.

Start your favorite interactive Python environment, such as ``ipython`` or
``jupyter lab``.

For this tutorial, we'll use a catalog of publicly available, openly licensed
sample data. Specifically, it is high-quality transmission XAS data from all
over the periodical table.

This utility downloads it and makes it discoverable to Databroker.

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

The Run's "pretty display", shown by IPython and Jupyter and some other
similar tools, shows us a summary.

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

Visualize them. There are couple ways to do this.

.. code:: python

   # The plot() method on xarray.DataArray
   ds["I0"].plot()

.. plot::

   import databroker

   run = databroker.catalog['bluesky-tutorial-BMM'][23463]
   ds = run.primary.read()
   ds["I0"].plot()

.. code:: python

   # The plot accessor on xarray.Dataset 
   ds.plot.scatter(x="dcm_energy", y="I0")


.. plot::

   import databroker

   run = databroker.catalog['bluesky-tutorial-BMM'][23463]
   ds = run.primary.read()

   # The plot accessor on xarray.Dataset 
   ds.plot.scatter(x="dcm_energy", y="I0")


.. code:: python

   # Using matplotlib directly
   import matplotlib.pyplot as plt
   import numpy

   plt.plot(ds["dcm_energy"], numpy.log(ds["It"] / ds["I0"]))
   plt.xlabel("dcm_energy")
   plt.ylabel("log(It / I0)")

.. plot::

   import databroker
   import matplotlib.pyplot as plt
   import numpy

   run = databroker.catalog['bluesky-tutorial-BMM'][23463]
   ds = run.primary.read()

   plt.plot(ds["dcm_energy"], numpy.log(ds["It"] / ds["I0"]))
   plt.xlabel("dcm_energy")
   plt.ylabel("log(It / I0)")

These `xarray`_ DataArray objects bundle a numpy (or numpy-like) array with
some additional metadata and coordinates. To access the underlying array
directly, use the ``data`` attribute.

.. ipython:: python

   type(ds["I0"])
   type(ds["I0"].data)

Looking again at this Run

.. ipython:: python

   run

we see it has a second stream, "baseline". Reading that, we notice that columns
it contains, its dimensions, and its coordinates are different from the ones in
"primary". That's why it's in a different stream.  The "baseline" stream is a
conventional name for snapshots taken at the very beginning and end of a
procedure. We see a long list of instruments with two data points each---before
and after.

.. ipython:: python

   run.baseline.read()

Different Runs can have different streams, but "primary" and "baseline" are the
two most common.

With that, we have accessed all the data from this run.

Handle large data
-----------------

The example data we have been using so far has no large arrays in it. For this
section we will download a second Catalog with one Run in it that contains
image data. It's 1 GB (uncompressed), which is large enough to exercise the
tools involved. These same techniques scale to much larger datasets.

The large arrays require an extra reader, which we can get from the package
``area-detector-handlers`` using pip on conda.

.. code:: bash

   pip install area-detector-handlers
   # or...
   conda install -c nsls2forge area-detector-handlers

Scientificaly, this is Resonant Soft X-ray Scattering (RSoXS) data. (`Details`_.)

.. ipython:: python

   import databroker.tutorial_utils
   databroker.tutorial_utils.fetch_RSOXS_example()

Access the new Catalog and assign this Run to a variable.

.. ipython:: python

   import databroker
   run = databroker.catalog['bluesky-tutorial-RSOXS']['777b44a']

In the previous example, we used ``run.primary.read()`` at this point. That
method reads all the data from the "primary" stream from storage into memory.
This can be inconvenient if:

1. The data is so large it does not all fit into memory (RAM) at once. Reading
   it would prompt a ``MemoryError`` (best case) or cause Python to crash
   (worst case).
2. You only need a subset of the data for your analysis. Reading all of it
   would waste time.

In these situations, we can summon up an `xarray`_ backed by *placeholders*
(`dask`_ arrays). These act like normal numpy arrays in many respects, but
internally they divide the data up intelligently into chunks. They only load
the each chunk if and when it is actually needed for a computation.

.. ipython:: python

   lazy_ds = run.primary.to_dask()

Comparing ``lazy_ds["Synced_waxs_image"].data`` to ``ds["I0"].data`` from the
previous section, we see that the "lazy" variant contains ``<dask.array ...>``
and the original contains ordinary numpy ``array``.

.. ipython:: python

   ds["I0"].head().data  # array
   lazy_ds["Synced_waxs_image"].data  # dask.array, a placeholder

As an example of what's possible, we can subtract from this image series the
mean of an image series taken while the shutter was closed ("dark" images).

.. ipython:: python

   corrected = run.primary.to_dask()["Synced_waxs_image"] - run.dark.to_dask()["Synced_waxs_image"].mean("time")
   corrected
   middle_image = corrected[64, 0, :, :]  # Pull out a 2D slice.
   middle_image

At this point, *no data has yet been read*. We are still working with
placeholders, building up an expression of work to be done in the future.
Finally, when we plot it or otherwise hand it off to code that will treat it as
normal array, the data will be loaded and processed (in chunks) and finally
give us a normal numpy array as a result. When only a sub-slice of the data is
actually used---as is the case in this example---only the relevant chunk(s)
will ever be loaded. This can save a lot of time and memory.

.. code:: python

   import matplotlib.pyplot as plt
   from matplotlib.colors import LogNorm

   # Plot a slice from the middle as an image with a log-scaled color transfer.
   plt.imshow(middle_image, norm=LogNorm(), origin='lower')

.. plot::

   import databroker
   import matplotlib.pyplot as plt
   from matplotlib.colors import LogNorm

   run = databroker.catalog['bluesky-tutorial-RSOXS']['777b44a']
   corrected = run.primary.to_dask()["Synced_waxs_image"] - run.dark.to_dask()["Synced_waxs_image"].mean("time")
   middle_image = corrected[64, 0, :, :]  # Pull out a 2D slice.
   plt.imshow(middle_image, norm=LogNorm(), origin='lower')

We can force that processing to happen explicitly by calling ``.compute()``.

.. ipython:: python

   middle_image.compute()

Notice that we now see ``array`` in there instead of
``<dask.array>``. This is how we know that it's a normal array in memory, not a
placeholder for future work.

For more, see the `xarray`_ documentation and the `dask`_ documentation. A good
entry point is the example covering `Dask Arrays`_.

.. _xarray: https://xarray.pydata.org/

.. _dask: https://dask.org/

.. _Dask Arrays: https://examples.dask.org/array.html

.. _Details: https://github.com/bluesky/data-samples/blob/master/catalogs/RSOXS/README.md
