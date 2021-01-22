How to store analysis results
=============================

*I want to access analysis results using Databroker.*

.. important::

   This is very new work and should be treated as experimental.

Databroker is designed to hold processed and analyzed data as well as raw data.
Currently, it is most commonly used for the former, but it was designed for
both from the start.

When collecting raw data, we can rely on Ophyd and the RunEngine to organize
our data and metadata in a structure recognizable to Databroker. When capturing
analysis results, we have to do some of that works ourselves.

#. Install ``bluesky-live``. If you have a recent version of databroker
   (v1.2.0 or higher) then you already have it; it's a requirement of
   databroker's.

   .. code:: bash

      pip install bluesky-live

#. Organize your data into a BlueskyRun. If your data can be represented as
   single table (i.e. a spreadsheet) with dictionary of metadata, then you can
   use this simplified interace.

   .. code:: python

      import bluesky_live.run_builder import build_simple_run

      run = buiLd_simple_run({'x': [1, 2, 3], 'y': [4, 5, 6]}, metadata={'sample': 'Cu'})    

   Here, our "table" is given as a dict of lists, but the following are also accepted:

   * dict of numpy arrays
   * pandas DataFrame
   * xarray Dataset

   This is approach is equivalent:

   .. code:: python

      import bluesky_live.run_builder import RunBuilder

      with RunBuilder(metadata={'sample': 'Cu'}) as builder:
          builder.add_stream("primary", data={'x': [1, 2, 3], 'y': [10, 20, 30]})
      run = builder.get_run()

   and, unlike ``build_simple_run``, it extends to multiple streams (i.e.
   tables or spreadsheets), as in

   .. code:: python

      with RunBuilder(metadata={'sample': 'Cu'}) as builder:
          builder.add_stream("primary", data={'x': [1, 2, 3], 'y': [10, 20, 30]})
          builder.add_stream("baseline", data={'A': [-1, -1], 'B': [250, 250]})
      run = builder.get_run()

#. Store your BlueskyRun.

   .. code:: python

      for name, doc in run.documents():
          catalog.v1.insert(name, doc)

.. note:: 

   You may notice that we are falling back to the ``v1`` API here, where for
   all other things we show and recommend the new ``v2`` API. This is because
   we are still `discussing the design`_ for this in v2. Until that is sorted
   out, this is the officially-recommended solution.

   It uses `Suitcase`_ internally to do the writing.

.. _discussing the design: https://github.com/bluesky/databroker/issues/605

.. _Suitcase: https://blueskyproject.io/suitcase/
