Navigate Metadata in a Run
==========================

In this tutorial we will access secondary measurements and metadata including:

* Hardware configuration readings (e.g. exposure time)
* User-provided context like sample information
* Whether the Run completed with an error (and if so what error)
* Hardware-level timestamps for each measurement

Set up for Tutorial
-------------------

Start your favorite interactive Python environment, such as ``ipython`` or
``jupyter lab``.

For this tutorial, we'll use a catalog of publicly available, openly licensed
sample data. Specifically, it is high-quality transmission XAS data from all
over the periodical table.

This utility downloads it and makes it discoverable to Databroker.

.. ipython:: python

   import databroker.tutorial_utils
   databroker.tutorial_utils.fetch_BMM_example()

Access the catalog and assign it to a variable for convenience.

.. ipython:: python

   import databroker
   catalog = databroker.catalog['bluesky-tutorial-BMM']

Let's take a Run from this Catalog.

.. ipython:: python

   run = catalog[23463]

(Hardware) Configuration
------------------------

The Run may include configurational readings necessary for interpreting the
data. These are typically things that change slowly or not at all during the
Run, like detector exposure time, detector gain settings, or the configured
maximum motor velocity.

First, let's look at the ``I0`` readings in the ``primary`` stream. What are
the configuration readings that might be necessary to interpret this data or
compare it with other data?

.. ipython:: python

   da = run.primary.read()["I0"]
   da.head()

This section at the bottom of that summary

.. code::

   Attributes:
       object:   quadem1

is showing us that ``I0`` was measured by the device ``quadem1``. We can also
access that programmatically like

.. ipython:: python

   da.attrs.get("object")

We can then look up all the configuration readings associated with ``quadem1``
in this stream.

.. ipython:: python

   run.primary.config["quadem1"].read()

If another Run ran the ``quadem1`` detector with a *different* integration
time, we could use this information to normalize the readings and compare them
accurately.

TO DO: Get an example of that.

Let's look at some other readings in the dataset. The ``It`` also comes from
``quadem1``, so those same configuration readings apply.

.. ipython:: python

   ds["It"].attrs

The ``dcm_energy``readings, on the other hand, comes from a different device,
which happens to also be named ``dcm_energy``.

.. ipython:: python

   ds["dcm_energy"].attrs

We can see that no configuration was recorded for that device.

.. ipython:: python

   run.primary.config["dcm_energy"].read()

How It Started
--------------

There are many useful pieces of metadata that we know at the **start**, before
we begin acquiring data or running data processing/analysis. This includes what
we intend to do (i.e. which scan type or which data processing routine), who is
doing it, and any additional context like sample information.

The only fields *guaranteed* by Databroker to be present are ``uid`` (a
globally unique identifier for the Run) and ``time`` (when it started) but
there is often a great deal more.

.. code:: python

   >>> run.metadata["start"]
   Start({
   'XDI': {'Beamline': {'collimation': 'paraboloid mirror, 5 nm Rh on 30 nm Pt',
                      'focusing': 'not in use',
                      'harmonic_rejection': 'flat mirror, Pt stripe, pitch = '
                                            '7.0 mrad relative to beam',
                      'name': 'BMM (06BM) -- Beamline for Materials '
                              'Measurement',
                      'xray_source': 'NSLS-II three-pole wiggler'},
         'Column': {},
         'Detector': {'I0': '10 cm N2', 'Ir': '25 cm N2', 'It': '25 cm N2'},
         'Element': {'edge': 'K', 'symbol': 'Ni'},
         'Facility': {'GUP': 305832,
                      'SAF': 305669,
                      'current': '399.6',
                      'cycle': '2020-1',
                      'energy': '3.0',
                      'mode': 'top-off',
                      'name': 'NSLS-II'},
         'Mono': {'angle_offset': 16.058109,
                  'd_spacing': '3.1353241',
                  'direction': 'forward',
                  'encoder_resolution': 5e-06,
                  'name': 'Si(111)',
                  'scan_mode': 'fixed exit',
                  'scan_type': 'step'},
         'Sample': {'name': 'Ni', 'prep': 'Ni foil in ref'},
         'Scan': {'edge_energy': 8332.800000000001,
                  'experimenters': 'Neil Hyatt, Martin Stennett, Dan Austin, '
                                   'Seb Lawson'},

    ...  # snipped for brevity
    }

How It Ended
------------

There are other things we can only know at the **stop** (end) of an experiment,
including when and how it finished and how many events (rows) of data were
collected in each stream.

.. ipython:: python

   run.metadata["stop"]

We can use this to print the unique IDs of any experiments that failed

.. ipython:: python

   for uid, run in catalog.items():
       if run.metadata["stop"]["exit_status"] != "success":
           print(f"Run {uid} failed!")

or, getting a bit fancier, to tally the number of failures.

.. ipython:: python

   from collections import Counter

   counter = Counter()
   for _, run in catalog.items():
       counter.update({run.metadata["stop"]["exit_status"]: 1})
   counter

TO DO: Obtain an example catalog that has some failures in it so that this
example is not so trivial.

Low-level Hardware Timestamps
-----------------------------

.. note::

   Any *preicse* timing measurements should be in the data itself, not in this
   supplemental hardware timestamp metadata. This should generally be
   considered good for ~0.1 second precision alignment.

Control systems provide us with individually timestamps for every reading.
These should generally *not* be used for data analysis. Any timing readings
necessary for analysis should be recorded as data, as a column in some stream.
These are intended to be used for debugging and troubleshooting.

The timestamps associated with the readings in ``run.primary.read()`` are
available as

.. ipython:: python

   run.primary.timestamps.read()

Configuration readings also come with timestamps. The timestamps associated
with the configuration readings in ``run.primary.config["quadem1"].read()`` are
available as

.. ipython:: python

   run.primary.config_timestamps["quadem1"].read()
