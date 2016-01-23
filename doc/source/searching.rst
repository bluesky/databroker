Searching for Data
******************

Searching by ID or Recency
++++++++++++++++++++++++++

Here is a summary of the "Do What I Mean" slicing supported by ``DataBroker``.

=============================== ==========================================================
syntax                          meaning
=============================== ==========================================================
``DataBroker[-1]``              most recent header
``DataBroker[-5]``              fifth most recent header
``DataBroker[-5:]``             all of the last five headers
``DataBroker[108]``             header with scan ID 108 (if ambiguous, most recent is found)
``DataBroker[[108, 109, 110]]`` headers with scan IDs 108, 109, 110
``DataBroker['acsf3rf']``       header with unique ID (uid) beginning with ``acsf3rf``
=============================== ==========================================================

Scan ID vs. Unique ID
+++++++++++++++++++++

Notice that there are two IDs in play: the "scan ID" and the "unique ID." The
scan ID is a counting number. Some users reset it to 1 between experiments, 
so it is not a good unique identifier for data --- it is just a convenience.
In the case of duplicates, DataBroker returns the most recent match.

The unique ID is randomly-generated hash that is statistically guaranteed to
uniquely identify a dataset forever. The DataBroker accepts a partial unique
ID --- the first 6-8 charachters are virtually always enough to identify a
data set. If they are not, the DataBroker will raise an error and request
the full unique ID, or at least more characters of it.

Time-based Queries
++++++++++++++++++

Runs that took place sometime in a given time interval are also supported.

=============================================================== ======================================
syntax                                                          meaning
=============================================================== ======================================
``DataBroker(start_time='2015-01')``                            all headers from January 2015 or later
``DataBroker(start_time='2015-01-05', end_time='2015-01-010')`` between January 5 and 10
=============================================================== ======================================


Complex Queries
+++++++++++++++

Finally, for advanced queries, the full MongoDB query language is supported.
Here are just a few examples:

=============================================================== ============================================================
syntax                                                          meaning
=============================================================== ============================================================
``DataBroker(sample={'$exists': True})``                        headers that include a custom metadata field labeled 'color'
``DataBroker(scan_type={'$ne': 'DeltaScan'})``                  headers where the type of scan was not a ``DeltaScan``
=============================================================== ============================================================

See the
`MongoDB query documentation <http://docs.mongodb.org/manual/tutorial/query-documents/>`_
for more.
