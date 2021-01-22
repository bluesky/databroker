How to store data from the Run Engine
=====================================

For small- and medium-sized deployments
---------------------------------------

Subscribe the Run Engine directly to Databroker.

.. code:: python

   RE.subscribe(catalog.v1.insert)

.. note:: 

   You may notice that we are falling back to the ``v1`` API here, where for
   all other things we show and recommend the new ``v2`` API. This is because
   we are still `discussing the design`_ for this in v2. Until that is sorted
   out, this is the officially-supported solution.

This will cause the RunEngine to wait for storage to be successful before
proceed with the next step of data acquisition.

Pro: We are assured that if data is not saved successfully, we will immediately
know and the data acquisition will be aborted. We avoid the scary scenario of
thinking we are saving data when we are not.

Con: By waiting for data to make it all the way into the database, data
acquisition will be marginally slower than if we took a more sophisticated
approach.

For facility-scale deployments
------------------------------

At present, all facilities currently using Bluesky (as far as we are aware) are
using this straightfowrad approach described above but a more sophisticated
alternative is being tested.

In short, put a message bus such as Kafka between the Run Engine and the
database. Tooling for this is under development at
`bluesky-kafka`_. Check back here for updates later in 2021.

.. _discussing the design: https://github.com/bluesky/databroker/issues/605

.. _Kafka: https://kafka.apache.org/

.. _bluesky-kafka: https://github.com/bluesky/bluesky-kafka
