.. ipython:: python
   :suppress:

   # Do this so Quick Start does not include example-generation.
   from metadatastore.utils.testing import mds_setup
   from filestore.utils.testing import fs_setup
   mds_setup()
   fs_setup()
   from dataportal.examples.sample_data import temperature_ramp
   from metadatastore.api import insert_run_start, insert_beamline_config

   rs = insert_run_start(time=0., scan_id=1, uid='a5fbde',
                         owner='nedbrainard', beamline_id='example',
                         beamline_config=insert_beamline_config({}, time=0.))
   temperature_ramp.run(run_start=rs)
   rs = insert_run_start(time=1., scan_id=2,
                         owner='nedbrainard', beamline_id='example',
                         beamline_config=insert_beamline_config({}, time=0.))
   temperature_ramp.run(run_start=rs)
   rs = insert_run_start(time=2., scan_id=3,
                         owner='nedbrainard', beamline_id='example',
                         beamline_config=insert_beamline_config({}, time=0.))
   temperature_ramp.run(run_start=rs)
   rs = insert_run_start(time=3., scan_id=4,
                         owner='nedbrainard', beamline_id='example',
                         beamline_config=insert_beamline_config({}, time=0.))
   temperature_ramp.run(run_start=rs)
   rs = insert_run_start(time=4., scan_id=5,
                         owner='nedbrainard', beamline_id='example',
                         beamline_config=insert_beamline_config({}, time=0.))
   temperature_ramp.run(run_start=rs)

*****************
DataBroker Basics
*****************

What is the Data Broker?
========================

The Data Broker prodives one interface for retrieving data from all sources.

You, the user, don't have to know where the data is stored or in what format
it is stored. The Data Broker returns all the data in one simply-structured
bundle. All measurements are given as standard Python data types (integer,
float, or string) or numpy arrays.

Quick Start
===========

This demonstrates the basic usage with minimal explanation. To understand what
is being done, read the next section.

.. ipython:: python

   from dataportal.broker import DataBroker
   from dataportal.muxer import DataMuxer

   header = DataBroker[-1]  # get most recent run
   events = DataBroker.fetch_events(header)
   dm = DataMuxer.from_events(events)
   dm.sources  # to review list of data sources

You can plot individual data sources against time...

.. ipython:: python

   dm['Tsam'].plot()  # or the name of any data source

Or bin the data to plot sources against each other...

.. ipython:: python

   binned_data = dm.bin_on('point_det')
   binned_data
   binned_data.plot(x='Tsam', y='point_det')

And you can easily export to common formats. Among them:

.. ipython:: python

   binned_data.to_csv('myfile.csv')
   binned_data.to_excel('myfile.xlsx')


Basic Example: Plotting The Most Recent Scan
============================================

Looking at a Scan
-----------------

Let's inspect the most recent run. To get the Nth most recent run,
type ``DataBroker[-N]``.

.. ipython:: python

   from dataportal.broker import DataBroker
  
   header = DataBroker[-1]

What we get is a Header, a dictionary-like (for C programmers, struct-like)
object with all the information pertaining to a run.

.. ipython:: python

   header

We can view its complete contents with ``print`` or, equivalently, 
``str(header)``.

.. ipython:: python

   print header

You can access the contents like a Python dictionary

.. ipython:: python

   header['owner']

or, equivalently, an attribute. In IPython, use tab-completion to explore.

.. ipython:: python

   header.owner

Getting the Data in its Rawest Form
-----------------------------------

The Header does not contain any of the actual measurements from a run. To get
the data itself, pass ``header`` (or a list of several Headers) to ``fetch_events``:

.. ipython:: python

   events = DataBroker.fetch_events(header)

The result is a list of Events, each one representing a measurement or
measurements that took place at a given time. (Exactly what we mean
by "Event" and "a given time" is documented elsewhere in both medium and
excruciating detail.)

Consider this an intermediate step. The data is structured in a generic way
that is wonderfully fleixble but not especially convenient. To get a more
useful view of the data, read on.

Putting the Data into a More Useful Form
----------------------------------------

One level above the DataBroker sits the DataMuxer, an object for merging and
aligning streams of Events from mamy sources into a table. Build a DataMuxer
like so:

.. ipython:: python

   from dataportal.muxer import DataMuxer
   dm = DataMuxer.from_events(events)

The ``events`` can be from one scan or from many scans together. Then, the
simplest task is to simply look at the data from one source -- say, sample
temperature.


.. ipython:: python

   dm['Tsam']

Incidentally, to save a litte typing, ``dm.Tsam`` accomplishes the same thing.
At any rate, the output gives the measured data at each time.

Next, let's obtain a table showing data from multiple sources. Strictly
speaking, measurements recorded by different equipment are not in general
synchronized, but in practice one usually ignores small differences in time.
For instance, we might want to plot "temperature" versus "intensity" even if
the temperature and intesity sensors never happened to take a simultaneous
measurement. Doing so, we would be implicitly *binning* those measurements
in time.

Therefore, plotting one dependent variable against another usually requires 
binning to effectively "align" the measurements against each other in time.
This is the problem that DataMuxer is designed to solve. On the simplest level,
it takes the stream of events and creates the table of data you probably
expected in the first place. But it is also capable of fully exploiting the
asynchronous stream of measurements, grouping them in different ways to answer
different questions.

To begin, we bin the data by centering one bin at each  ``point_det``
measurement.

.. ipython:: python

   binned_data = dm.bin_on('point_det')
   binned_data

Wherever there is ``point_det`` measurement but no ``Tsam`` measurement within
the time window, NaN indicates the missing data. (You may object that "NaN"
is not really the same as "missing." This is a convention borrowed from the
widely-used pandas package, and the reasons for using NaN to mean "missing"
have to do with the limitations of numpy in handling missing data.)

The ``count`` sub-column indicates the number of ``Tsam`` measurements in
each bin. There is exactly one ``point_det`` measurement in every bin, by
definition, so no ``count`` is shown there.

Sometimes, one can interpolate the missing values according to some rule, such
as linear interpolation.

.. ipython:: python

   binned_data = dm.bin_on('point_det', interpolation={'Tsam': 'linear'})
   binned_data

The ``count`` column, still present, indicates which values are measured (1)
and which are interpolated (0).

If instead we bin the other way, defining one bin per ``Tsam`` data point,
we must provide a rule for combining multiple ``point_det`` measurements in
the same bin into one representative value. Now, along with the ``count``
sub-column, other summary statistics are automatically generated.

.. ipython:: python

   binned_data = dm.bin_on('Tsam', agg={'point_det': np.mean})
   binned_data

To discard the extra statistics and keep the values only, use this syntax.
(``xs`` stands for cross-section, a sophisticated pandas method.)

.. ipython:: python

   binned_data.xs('val', level=1, axis=1)

Exporting the Data for Use Outside of Python
--------------------------------------------

The tabular results from the DataMuxer are DataFrames, objects from the widely-
used and well-documented package pandas, and there are many convenient
methods for exporting them to common formats. For example:

.. ipython:: python

   binned_data.to_csv('myfile.csv')
   binned_data.to_excel('myfile.xlsx')

More methods are described in the pandas documention, and can easily be
explored by typing ``binned_data.to_`` <tab>.

This quick-and-dirty export is really only useful if the data of interest
is scalar (e.g., not images) and not very large. For other applications,
different tools should be used. As of this writing, these tools are in
development and not yet documented.

.. ipython:: python
   :suppress:

   # Cleanup
   !rm myfile.csv
   !rm myfile.xlsx

More Ways to Look Up Scans
==========================

To quickly look up recent scans, use the standard Python slicing syntax for
indexing from the end of a list.

.. ipython:: python

   header = DataBroker[-1]  # most recent scan
   header.scan_id 
   header = DataBroker[-2]  # next to last scan
   header.scan_id
   headers = DataBroker[-5:]  # all of the last five scans
   [h.scan_id for h in headers]
   headers = DataBroker[-1000::100]  # sample every 100th of the last (up to) 1000 scans

Or give the scan ID, which is always a positive integer.

.. ipython:: python

   header = DataBroker[4]  # scan ID 4
   header.scan_id

If you know the unique id (uid) of a Header, you can use the first few
characters to find it.

.. ipython:: python

   header = DataBroker['a5fbde']

For advanced searches, use ``find_headers``.

.. ipython:: python

   neds_headers = DataBroker.find_headers(owner='nedbrainard')
   headers_measuring_temperature = DataBroker.find_headers(data_key='Tsam')

Any of these results, whether a single Header or a list of Headers, can be
passed to ``DataBroker.fetch_events()`` as shown in the previous sections above.
