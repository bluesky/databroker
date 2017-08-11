********
Tutorial
********

The databroker is tool for access data from many sources through a unified
interface. It emphasizes rich searching capabilities and handling multiple
concurrent "streams" of data in an organized way.


.. ipython:: python
    :suppress:

    import os
    import yaml
    import uuid
    mds_dir = '/tmp/' + str(uuid.uuid4())
    assets_dir = '/tmp/' + str(uuid.uuid4())
    EXAMPLE = {
        'metadatastore': {
            'module': 'databroker.headersource.sqlite',
            'class': 'MDS',
            'config': {
                'directory': mds_dir,
                'timezone': 'US/Eastern'}
        },
        'assets': {
            'module': 'databroker.assets.sqlite',
            'class': 'Registry',
            'config': {
                'dbpath': assets_dir + '/database.sql',
                'timezone': 'US/Eastern'}
        }
    }
    os.makedirs('~/.config/databroker', exist_ok=True)
    path = os.path.expanduser('~/.config/databroker/example.yml')
    with open(path, 'w') as f:
        yaml.dump(EXAMPLE, f)
    from databroker import Broker
    db = Broker.named('example')
    from bluesky import RunEngine
    from bluesky.plans import scan
    from bluesky.examples import det, motor
    RE = RunEngine({})
    RE.subscribe(db.insert)
    for _ in range(5):
        RE(scan([det], motor, 1, 5, 5))

Basic Walkthrough
-----------------

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

Load the most recently saved run.

.. ipython:: python

    header = db[-1] 

The result, ``header``, encapulates the metadata from this run. Everything
recorded at the start of the run is in ``header.start``.

.. ipython:: python

    header.start

Information only knowlable at the end, like the exit status (success, abort,
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

Loading the data itself can be a longer process, so it's a separate step. For
scalar data that fits into memory, the most convenient method is:

.. ipython:: python

    header.table()

This object is DataFrame, an spread-like object provided by the library
`pandas <https://pandas.pydata.org/pandas-docs/stable/>`_. DataFrames can be
used to perform fast computations on labeled data, such as

.. ipython:: python

    t = header.table()
    t.mean()
    t['det'] / t['motor']

or export to a file.

.. ipython:: python

    t.to_csv('data.csv')

Other methods provide more raw access to the data, in a streaming fashion.

.. ipython:: python

    events = header.events()
    next(events)  # loads just the first data point
    next(events)  # loads the second one
    for event in header.events():
        print(event['data']['motor'])

See the :doc:`api` for more.

Searching
---------

The "slicing" (square bracket) syntax is a quick way to search based on
recently, unique ID, or counting number scan_id. Examples:

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

Calling a Broker like a function (with parentheses) access richer search.
Common serach parameters include ``plan_name``, ``motor``, and ``detectors``.
Any user-provided metadata can be used in a serach. Examples:

.. code-block:: python

    # Serach by plan name.
    headers = db(plan_name='scan')

    # Search for runs involving a motor with the name 'eta'.
    headers = db(motor='eta')

    # Search for runs operated by a given user---assuming this metadata was
    # recorded in the first place!
    headers = db(operator='Dan')

    # Search by time range. (These keywords have a special meaning.)
    headers = db(start_time='2015-03-05', stop_time='2015-03-10')

Full-text search is also supported, for MongoDB-backed deployments.

.. code-block:: python

    # Perform text search on all values in the Run Start document.
    headers = db('keyword')

Note that partial words are not matched, but partial phrases are. For example,
'good' will match to 'good sample' but 'goo' will not.

Unlike the "slicing" (square bracket) queries, rich searches can return an
unbounded number of results. To avoid slowness, the results are loaded
"lazily," only as needed. Here's an example of what works and what doesn't.

.. ipython:: python

    headers = db(plan_name='scan')
    headers
    headers[2]  # Fails! The results are not a list.
    list(headers)[2]  # This works, but might be slow if the results are large.

Loop through them loads one at a time, conserving memory.

.. ipython:: python

    for header in headers:
        print(header.table()['det'].mean())
