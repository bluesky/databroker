How to download some Catalogs with data samples
===============================================

*I want to download some sample datasets and access them with Databroker.*

As shown at the top of every tutorial, there are some curated datasets
available via this one-liner. This are open `CC0`_-licensed data contributed
by Bluesky users.

.. code:: python

   import databroker.tutorial_utils

   # Catalog of many small Runs
   catalog = databroker.tutorial_utils.fetch_BMM_example()

   # Catalog with one large Run (1 GB uncompressed)
   catalog = databroker.tutorial_utils.fetch_RSOXS_example()

These are the first two of many to come. We are actively curating more.
If you have some to offer, please open an issue on the
`bluesky/data-samples issue tracker`_.

.. _CC0:  https://creativecommons.org/share-your-work/public-domain/cc0/

.. _bluesky/data-samples issue tracker: https://github.com/bluesky/data-samples/issues/new
