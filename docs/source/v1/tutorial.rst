.. currentmodule:: databroker


********
Tutorial
********

The databroker is a tool to access data from many sources through a unified
interface. It emphasizes rich searching capabilities and handling multiple
concurrent "streams" of data in an organized way.


.. ipython:: python
    :suppress:

    import os
    import yaml
    from databroker.tests.test_config import EXAMPLE as config
    os.makedirs('~/.config/databroker', exist_ok=True)
    path = os.path.expanduser('~/.config/databroker/example.yml')
    with open(path, 'w') as f:
        yaml.dump(config, f)
    from databroker import Broker
    db = Broker.named('example')
    from bluesky import RunEngine
    from bluesky.plans import scan
    from ophyd.sim import det, motor
    RE = RunEngine({})
    RE.subscribe(db.insert)
    for _ in range(5):
        RE(scan([det], motor, 1, 5, 5))

Basic Walkthrough
-----------------

Get a Broker
============

List the names of available configurations.

.. ipython:: python

    from databroker import list_configs

    list_configs()

If this list is empty, no one has created any configuration files yet. See the
section on :doc:`configuration`.

Make a databroker using one of the configurations.

.. ipython:: python

    from databroker import Broker
    db = Broker.named('example')

Load Data as a Table
====================

Load the most recently saved run.

.. ipython:: python

    header = db[-1]

The result, a :class:`Header`, encapsulates the metadata from this run. Loading
the data itself can be a longer process, so it's a separate step. For scalar
data, the most convenient method is:

.. ipython:: python

    header.table()

This object is DataFrame, a spreadsheet-like object provided by the library
`pandas <https://pandas.pydata.org/pandas-docs/stable/>`_.

.. note::

    For Python novices we point out that ``header`` above is an arbitrary
    variable name. It could have been:

    .. code-block:: python

        h = db[-1]
        h.table()

    or even in one line:

    .. code-block:: python

        db[-1].table()

Do Analysis or Export
=====================

DataFrames can be used to perform fast computations on labeled data, such as

.. ipython:: python

    t = header.table()
    t.mean(numeric_only=True)
    t['det'] / t['motor']

or export to a file.

.. ipython:: python

    t.to_csv('data.csv')

.. ipython:: python
    :suppress:

    # Clean up
    !rm data.csv


Load Data Lazily (Good for Image Data)
======================================

The :class:`Header.table` method is just one way to load the data. Another is
:class:`Header.data`, which loads data for one specific field (i.e., one column
of the table) in a "lazy", streaming fashion.

.. ipython:: python

    data = header.data('det')
    data  # This a 'generator' that will load data when we loop through it.
    for point in data:
        # 'Process' the data one point at a time.
        # Here we'll just print it.
        print(point)

The :class:`Header.data` method is suitable for loading image data. See
the :doc:`api` for more methods.

Explore Metadata
================

Everything recorded at the start of the run is in ``header.start``.

.. ipython:: python

    header.start

Information only knowable at the end, like the exit status (success, abort,
fail) is stored in ``header.stop``.

.. ipython:: python

    header.stop

Metadata about the devices involved and their configuration is stored in
``header.descriptors``, but that is quite a lot to dig through, so it's useful
to start with some convenience methods that extract the list of devices or the
fields that they reported:

.. ipython:: python

    header.devices()
    header.fields()

To extract configuration data recorded by a device:

.. ipython:: python

    header.config_data('motor')

(A realistic example might report, for example, exposure_time or zero point.)


Searching
---------

The "slicing" (square bracket) syntax is a quick way to search based on
relative indexing, unique ID, or counting number scan_id. Examples:

.. code-block:: python

    # Get the most recent run.
    header = db[-1]

    # Get the fifth most recent run.
    header = db[-5]

    # Get a list of all five most recent runs, using Python slicing syntax.
    headers = db[-5:]

    # Get a run whose unique ID ("RunStart uid") begins with 'x39do5'.
    header = db['x39do5']

    # Get a run whose integer scan_id is 42. Note that this might not be
    # unique. In the event of duplicates, the most recent match is returned.
    header = db[42]

Calling a Broker like a function (with parentheses) accesses richer searches.
Common search parameters include ``plan_name``, ``motor``, and ``detectors``.
Any user-provided metadata can be used in a search. Examples:

.. code-block:: python

    # Search by plan name.
    headers = db(plan_name='scan')

    # Search for runs involving a motor with the name 'eta'.
    headers = db(motor='eta')

    # Search for runs operated by a given user---assuming this metadata was
    # recorded in the first place!
    headers = db(operator='Dan')

    # Search by time range. (These keywords have a special meaning.)
    headers = db(since='2015-03-05', until='2015-03-10')

Full-text search is also supported, for MongoDB-backed deployments. (Other
deployments will raise :class:`NotImplementedError` if you try this.)

.. code-block:: python

    # Perform text search on all values in the Run Start document.
    headers = db('keyword')

Note that partial words are not matched, but partial phrases are. For example,
'good' will match to 'good sample' but 'goo' will not.

Unlike the "slicing" (square bracket) queries, rich searches can return an
unbounded number of results. To avoid slowness, the results are loaded
"lazily," only as needed. Here's an example of what works and what doesn't.

.. ipython:: python
    :okexcept:

    headers = db(plan_name='scan')
    headers
    headers[2]  # Fails! The results are not a list.
    list(headers)[2]  # This works, but might be slow if the results are large.

Looping through them loads one at a time, conserving memory.

.. ipython:: python

    for header in headers:
        print(header.table()['det'].mean())
