Searching for Data
******************

The result of a search is a :ref:`header <headers>`, a bundle of metadata about
a given run. In a later section, :doc:`fetching`, we will use the header to
retrieve the data itself. Headers are also useful for quickly reviewing
metadata and generating summaries and logs.

Search Examples
---------------

In these examples, we will collect data using bluesky and then access it
from the databroker. This illustrates how metadata provided at collection time
can be used to search and filter the data during later analysis.

You do not need to be familiar with bluesky's usage to follow the gist these
examples. For a more detailed understanding, refer to the sections on
`basic usage <https://nsls-ii.github.io/bluesky/plans_intro.html>`_ and
`recording metadata <https://nsls-ii.github.io/bluesky/metadata.html>`_.

By Unique ID
++++++++++++

The surest and most direct way to get particular header is to look it up by its
unique ID. This ID is guaranteed to uniquely identify the run forever.

The RunEngine returns a list of the unique ID(s) when it completes execution.

.. code-block:: python

    # In all these examples we assume a RunEngine instance, RE, is defined.
    # See link to bluesky 'basic usage' documentation above.

    # We also assume that a Broker instance, db, is defined.
    # See previous section on configuration.

    uids = RE(some_plan())
    headers = db[uids]

We could also write down the first 5-6 characters in a uid and use it to look
up the header later. Each unique ID is a randomly-generated string like

.. code-block:: python

    'cf24798b-ed6e-4d44-b529-7199fcec41cc'

but the first 5-6 are virtually always enough to uniquely identify a run. The
databroker accepts a partial uid:

.. code-block:: python

    db['cf2479']

If there is ambiguity (two uids starting with the same couple characters) the
Broker will raise an error. But, again, 5-6 characters are virtually always
sufficient.

By Plan Name, Detector, or Motor
++++++++++++++++++++++++++++++++

Suppose we execute several experiments ("plans", in bluesky jargon) like so.

.. code-block:: python

    from bluesky.plans import count, scan, relative_scan
    from bluesky.examples motor, det  # simulated motor and detector

    RE(count([det]))  # 1
    RE(scan([det], motor, -1, 1, 5))  # 2
    RE(relative_scan([det]), motor, 1, 10, 10)  # 3
    RE(scan([det], motor, -1, 1, 1000))  # 4

We can search by ``plan_name``, which is always automatically recorded in the 
metadata.

.. code-block:: python

    relative_scans = db(plan_name='relative_scan')  # 3
    absolute_scans = db(plan_name='scan')  # 2 and 4

We can also search by ``motors`` or ``detectors``. (All built-in plans provide
this metadata automatically. User-defined plans may or may not provide it.)

.. code-block:: python

    runs_using_motor = db(motors='motor')  # 2, 3, and 4
    runs_using_det = db(detectors='det')  # all

To be precise, ``detectors='det'`` means, "The detector ``det`` is included
in the *list* of detectors used."

We can also narrow the search by certain plan-specific metadata, like the
number of steps in a scan.

.. code-block:: python

    long_scan = db(plan_name='scan', num_steps > 50)  # 4

These may be combined with time-based parameters (presented later below) to
restrict the search to the previous day or week.

By Custom Metadata Fields
+++++++++++++++++++++++++

Again, suppose we execute several plans. This time, we provide some custom
metadata including person operating the equipment and, in some cases, about the
sample and the purpose of each run.

.. code-block:: python

    from bluesky.plans import count, scan, relative_scan
    from bluesky.examples motor, det  # simulated motor and detector

    # This adds {'operator': 'Ken'} to all future runs, unless overridden.
    RE.md['operator'] = 'Ken'

    RE(count([det]), purpose='calibration', sample='A')
    RE(scan([det]), motor, 1, 10, 10, operator='Dan')  # temporarily overrides Ken
    RE(count([det]), sample='A')  # (now back to Ken)
    RE(count([det]), sample='B')

    RE.md['operator'] = 'Dan'

    RE(count([det]), purpose='calibration')
    RE(scan([det]), motor, 1, 10, 10)

    del RE.md['operator']  # clean up by un-setting operator

We can search on any of these custom fields. (The words 'operator' and
'purpose' have no special significance to bluesky or databroker --- arbitrary
fields could have been used.)

.. code-block:: python

    db(sample='A')  # return both runs that used sample A
    db(purpose='calibration', sample='A')  # returns sample A calibration run
    db(purpose='calibration')  # returns the two calibration runs
    db(operator='Dan')  # returns three runs by Dan

Searching by ID or Recency
--------------------------

With Python's slicing syntax, Broker provides a shorthand for common searches.

======================= ==========================================================
syntax                  meaning
======================= ==========================================================
``db[-1]``              most recent header
``db[-5]``              fifth most recent header
``db[-5:]``             all of the last five headers
``db[108]``             header with scan ID 108 (if ambiguous, most recent is found)
``db[[108, 109, 110]]`` headers with scan IDs 108, 109, 110
``db['acsf3rf']``       header with unique ID (uid) beginning with ``acsf3rf``
======================= ==========================================================

Aside: Scan ID vs. Unique ID
----------------------------

Notice that there are two IDs in play: the "scan ID" and the "unique ID." The
scan ID is a counting number. Some users reset it to 1 between experiments, 
so it is not a good unique identifier for data --- it is just a convenience.
In the case of duplicates, Broker returns the most recent match.

As explained above, the unique ID is randomly-generated string that is
statistically guaranteed to uniquely identify a dataset forever. The Broker
accepts a partial unique ID --- the first 5-6 characters are virtually always
enough to identify a data set.

Time-based Queries
------------------

Runs that took place sometime in a given time interval are also supported.

======================================================= ======================================
syntax                                                  meaning
======================================================= ======================================
``db(start_time='2015-01')``                            all headers from January 2015 or later
``db(start_time='2015-01-05', stop_time='2015-01-10')`` between January 5 and 10
======================================================= ======================================

Filters
-------

.. versionadded:: v0.6.0

To restrict seraches by user, project, date, plan_name, or any other parameter,
add a "filter" to the Broker.

.. code-block:: python

    # Restrict future searches.
    db.add_filter(user='Dan')
    db.add_filter(start_time='2015-01')

    db(sample='A')  # becomes db(sample='A', user='Dan', start_time='2015-01')

    # Clear all filters.
    db.clear_filters()

Any query passed to ``db.add_filter()`` is stashed and "AND-ed" with all future
queries. You can also review or alter the filters through the ``db.filters``
property, a list of queries (that is, a list of dicts formatted like MongoDB
queries).

Aliases
-------

.. versionadded:: v0.6.0

To "save" a search for easy resuse, you can create an alias.

.. code-block:: python

    db.alias('cal', purpose='calibration')

    db.cal  # -> db(purpose='calibration')

A "dynamic alias" maps the alias to a function that returns a query.

.. code-block:: python

    # Get headers from the last 24 hours.
    db.dynamic_alias('today',
                     lambda: {'start_time': start_time=time.time() - 24*60*60})

    # Get headers where the 'user' field matches the current logged-in user.
    import getpass
    db.dynamic_alias('mine', lambda: {'user': getpass.getuser()})

Aliases are stored in ``db.aliases`` (a dictionary mapping alias names to
queries or functions that return queries) where they can be reviewed or
deleted.

If the module ``historydict`` is installed, ``db.aliases`` are persisted
between sessions in a dictionary-like object backed by a sqlite database.

Complex Queries
---------------

Finally, for advanced queries, the full MongoDB query language is supported.
Here are just a few examples:

=========================================== ============================================================
syntax                                                          meaning
=========================================== ============================================================
``db(sample={'$exists': True})``            headers that include a custom metadata field labeled 'color'
``db(plan_name={'$ne': 'relative_scan'})``  headers where the type of scan was not a ``relative_scan``
=========================================== ============================================================

See the
`MongoDB query documentation <http://docs.mongodb.org/manual/tutorial/query-documents/>`_
for more.
