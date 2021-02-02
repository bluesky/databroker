Export Data
===========

In this tutorial we will export data from a Run to files. We will do this in
two ways:

* The simple way, using methods like ``to_csv`` provided by standard scientific
  Python tools
* The "streaming" way, using Bluesky's Suitcases

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

Simple Export
-------------

Export to CSV or Excel
^^^^^^^^^^^^^^^^^^^^^^

CSV can be suitable small amounts of scalar data. It's not fast and it's not
particularly good way to store numeric data or rich metadata---but it is
universally understood and human-readable.

Here, we look at the columns in the primary stream and choose some to export to
CSV.

.. ipython:: python

   ds = run.primary.read()
   ds
   columns = ["I0", "It", "Ir", "dcm_energy"]  # columns we want to export
   df = ds[columns].to_dataframe()
   df
   # Setting index=False omits the "time" index on the left from the output.
   df.to_csv("data.csv", index=False)

If you target is to get data into Excel, note that you can write Excel files
directly. This requires an additional dependency that you may not already have
installed.

.. code:: python

   # Install Excel writer used by pandas using pip...
   pip install openpyxl
   # or conda...
   conda install -c conda-forge openpyxl

.. ipython:: python

   df.to_excel("data.xlsx", index=False)

Both of these methods have a large number of options to customize the output.
Use ``df.to_csv?`` (IPython, Jupyter) or ``help(df.to_csv)`` to learn more.
Likesie for ``df.to_excel``.

If you have many runs to do in batch, you may use the metadata to automatically
generate filenames. It is strongly recommended to include part of the globally
unique id, ``uid``, at the end to ensure that names do not clash and overwrite.

.. ipython:: python

   columns = ["I0", "It", "Ir", "dcm_energy"]
   results = catalog.search({"XDI.Element.symbol": "Mn"})
   for uid, run in results.items():
       ds = run.primary.read()
       df = ds[columns].to_dataframe()
       # Generate filename from metadata.
       md = run.metadata["start"]
       filename = f'Mn-spectra-{md["scan_id"]}-{md["uid"]:.8}.csv'
       df.to_csv(filename, index=False)
       print(f"Saved {filename}")

Export to HDF5
^^^^^^^^^^^^^^

HDF5 is suitable for image data. It is understood by most data analysis
software.

.. note::

   This example uses h5py.

   .. code::
   
      conda install h5py
   
      # or...
   
      pip install h5py

.. ipython:: python

   import h5py

   ds = run.primary.read()
   columns = ["I0", "It", "Ir", "dcm_energy"]  # columns we want to export
   with h5py.File("data.h5", "w") as file:
       for column in columns:
           file[column] = df[column]

Streaming Export
----------------

A tool built for streaming export can be used on both saved data (as we'll do
here) and on live-streaming data during data acquisition.

.. note::

   This example uses suitcase-csv.

   .. code::
   
      conda install -c nsls2forge suitcase-csv
   
      # or...
   
      pip install suitcase-csv

.. ipython:: python

   import suitcase.csv
   artifacts = suitcase.csv.export(run.documents(fill="yes"), "output_directory")
   artifacts

Note that this operates on the entire `run` and all of its streams. When a Run
contains multiple streams, multiple CSV files will be created. This is why it
acceps a path to a *directory* rather than a path to a single file. Any data
that does well-suited to the format (e.g. image data in this case) is omitted
for the export.

See `Suitcase`_ for a list of supported formats and more information.

.. _Suitcase: https://blueskyproject.io/suitcase

.. ipython:: python
   :suppress:

   # Clean up
   !rm data.csv
   !rm -rf Mn-spectra*
   !rm data.xlsx
   !rm data.h5
   !rm -rf output_directory
