********
Overview
********

Search for data, and retrieve it as SciPy/PyData data structures for
interactive data exploration or in
`a representation suitable for streaming applications <https://nsls-ii.github.io/event-model>`_ .

`Intake <https://intake.readthedocs.io>`_ takes the pain out of data access.
It loads data from a growing variety of formats into familiar Scipy/PyData data
structures.

`Bluesky <https://nsls-ii.github.io/bluesky>`_ is a suite of co-developed
Python packages for data acquisition and management designed to drive
experiments and capture data and metadata from experiments and simulations in a
way that interfaces naturally with open-source software in general and the
scientific Python ecosystem in particular.

DataBroker applies Intake to Bluesky.

Transition Plan
===============

DataBroker began development in 2015, before Intake. Intake has appeared on the
scene as a project with closely-aligned goals and a growing community of users
from diverse scientific fields. Starting with DataBroker's 1.0 release,
DataBroker is pivoting to become a collection of Intake plugins.

This change will proceed in a way that provides a slow and smooth transition
for existing DataBroker users.

DataBroker 1.0 (Pre-released)
-----------------------------

* The module ``databroker.v0`` provides the databroker code exactly as it was
  in previous pre-1.0 releases. This is available as fallback in case of bugs
  in v1.
* The module ``databroker.v1`` provides the same user interface as v0, but
  built on top of intake. Except in rare cases, all code that worked on pre-1.0
  releases should work on ``databroker.v1``. This is the "default" interface in
  DataBroker 1.0; importing ``from databroker ...`` is an alias for ``from
  databroker.v1 ...``.
* The module ``databroker.v2`` is a *very* thin shim around intake.


Both ``databroker.v1.Broker`` and ``databroker.v2.Broker`` have accessors,
``v1`` and ``v2``, that support usage of the other's interface. Example:

.. code:: python

   # Try v2 features from v1.
   from databroker import Broker
   db = Broker.named('MY_CATALOG')
   query = dict(plan_name='count')
   db(**query)  # v1-style search
   db.v1(**query)  # a synonym for the above
   db.v2.serach(query)  # v2-style search

   # Retreat to v1 features from v2.
   from databroker import catalog 
   db = catalog.SOMETHING()
   query = dict(plan_name='count')
   db.search(query)  # v2-style search
   db.v2.search(query)  # a synonym for the above
   db.v1(**query)  # v1-style search

Roadmap Beyond 1.0
------------------

* When the v1 implementation is proven reliable, v0 will be removed, greatly
  reducing the amount of code in databroker. This should not have an effect on
  users, as v1 provides the same interface as v0, just backed by intake
  internally.
* If and when the v2 interface is shown to meet users' needs as well or better
  than v1, it will become the default. To support existing user code, the v1
  interface will continue to be available in ``databroker.v1``.
* After several years of maintaining both v1 and v2, when all critical user
  code has been migrated to v2, v1 may be deprecated and eventually removed.
