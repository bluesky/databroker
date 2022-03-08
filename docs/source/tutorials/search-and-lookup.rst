.. currentmodule:: databroker

Find Runs in a Catalog
======================

In this tutorial we will:

* Look up a specific Run by some identifier.
* Look up a specific Run based on recency (e.g. "Show me the data I just took").
* Search for Runs using both simple and complex search queries.

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

Access the catalog and assign it to a variable for convenience.

.. ipython:: python

   import databroker
   catalog = databroker.catalog['bluesky-tutorial-BMM']

Look-up
-------

In this section we will look up a Run by its

* Globally unique identifier --- unmemorable, but great for scripts
* Counting-number "scan ID" --- easier to remember, but not necessarily unique
* Recency --- e.g. "the data I just took"

If you know exactly which Run you are looking for, the surest way to get it is
to look it up by its globally unique identifier, its "uid". This is the
recommended way to look up runs *in scripts* but it is not especially
fluid for interactive use.

.. ipython:: python

   catalog['c07e765b-ce5c-4c75-a16e-06f66546c1d4']

The uid may be abbreviated. The first 7 or 8 characters are usually sufficient
to uniquely identify an entry.

.. ipython:: python

   catalog['c07e765']

If the abbreviated uid is ambiguous---if it matches more than one Run---a
``ValueError`` is raised listing the matches. Try ``catalog['a']``, which will
match two Runs in this Catalog and raise that error.

Runs typically also have a counting number identifier, dubbed ``scan_id``. This
is easier to remember. Keep in mind that ``scan_id`` *is not neccesarily unique*,
and Databroker will always give you the most recent match.
Some users are in the habit of resetting ``scan_id`` to 1 at the beginning of
a new experiment or operating cycle. This is why lookup based on the globally
unique identifier is safest for scripts and Jupyter notebooks, especially
long-lived ones.

.. ipython:: python

   catalog[23463]

Finally, it is often convenient to access data by recency, as in "the data that
I just took".

.. ipython:: python

   catalog[-1]

This syntax is meant to feel similar to accessing elements in a list or array
in Python, where  ``a[-N]`` means "``N`` elements from the end of ``a``".

In summary:

================== ===============================================
``catalog["..."]`` Globally unique identifier ("uid")
``catalog[N]``     Counting number "scan ID" N (most recent match)
``catalog[-N]``    Nth most recent Run in the Catalog
================== ===============================================

All of these always return *one* ``BlueskyRun`` or raise an exception.

Search
------

Common search queries can be done with a high-level Python interface.

.. ipython:: python
   :okwarning:

   from databroker.queries import TimeRange
   
   results = catalog.search(TimeRange(since="2020-03-05"))

The result of a search is just another Catalog. It has a subset of the original
Catalog's entries. We can compare the number of search results to the total
number of entries in ``catalog``.

.. ipython:: python
    
   print(f"Results: {len(results)}  Total: {len(catalog)}")

We can iterate through the results for batch processing

.. ipython:: python

   for uid, run in results.items():
       # Do something.
       ...

or access a particular result by using any of the lookup methods in the section
above, such as recency. This is a convenient way to quickly look at one search
result.

.. ipython:: python

   results[-1]

Because ``results`` is just another Catalog, we can search on the search
results to progressively narrow our results.

.. ipython:: python

   narrowed_results = results.search({"num_points": {"$gt": 400}})  # Read on...
   print(f"Narrowed Results: {len(narrowed_results)}  Results: {len(results)}  Total: {len(catalog)}")

Custom queries can be done with the `MongoDB query language`_.
The simplest examples check for equality of a key and value, as in

.. ipython:: python

   results = catalog.search({"XDI.Element.symbol": "Mn"})
   len(results)

The above matches Runs where the 'start' document looks like::

   {
       ...
       "XDI": {"Element": {"symbol": "Mn"}},
       ...
   } 

The allowed keys are totally open-ended as far as Databroker is concerned.
This example is particular to the metadata recorded by the instrument that
it came from.  What's useful in your case will depend on what metadata was
provided when the data was captured. Look at a couple Runs' start documents
to get a sense of the metadata that would be useful in searches.

.. code:: python

   run = catalog[-1]
   run.metadata["start"]

Again, the syntax of a query is that of the `MongoDB query language`_.
It's an expressive language for specifying searches over heterogeneous
metadata.

.. note:: 

   When the data is stored by some means other than MongoDB, databroker uses
   Python libraries that support most of MongoDB's query language without
   actual MongoDB.

Here is an example of a more sophisticated query, doing more than just checking
for equality.

.. ipython:: python

    query = {
        "XDI.Scan.edge_energy": {"$lte": 6539.0},  # less than or equal to
        "XDI.Element.symbol": "Mn",
    }
    results = catalog.search(query)
    len(results)

See the MongoDB documentation linked above to learn other expressions like
``$lte``.

.. _MongoDB query language: https://docs.mongodb.com/manual/reference/operator/query/
