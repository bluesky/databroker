********
Overview
********

DataBroker provides access to saved data from Bluesky. It does this by applying
Intake to Bluesky.

`Intake <https://intake.readthedocs.io>`_ takes the pain out of data access.
It loads data from a growing variety of formats into familiar Scipy/PyData data
structures.

`Bluesky <https://blueskyproject.io>`_ is a suite of co-developed
Python packages for data acquisition and management designed to drive
experiments and capture data and metadata from experiments and simulations in a
way that interfaces naturally with open-source software in general and the
scientific Python ecosystem in particular.

.. _transition_plan:

Transition Plan
===============

DataBroker began development in 2015, before Intake. Intake has goals closely
aligned with DataBroker's and a growing community of users from diverse
scientific fields. Starting with DataBroker's 1.0 release, DataBroker is
pivoting to become a distribution of Intake plugins.

This change will proceed in a way that provides a slow and smooth transition
for existing DataBroker users.

DataBroker 1.0 (Pre-released)
-----------------------------

The 1.0 release of DataBroker supports multiple usages to enable a smooth
transition for users.

* The module ``databroker.v0`` provides the databroker code exactly as it was
  in previous pre-1.0 releases. This is available as fallback in case of bugs
  in v1.
* The module ``databroker.v1`` provides the same user interface as v0, but
  built on top of intake instead of databroker's own implementation. With only
  narrow exceptions, code that worked on pre-1.0 releases should work on
  ``databroker.v1``. This is the "default" interface in DataBroker 1.0;
  importing ``from databroker ...`` is an alias for
  ``from databroker.v1 ...``.
* The module ``databroker.v2`` is a *very* thin shim around intake.

The :doc:`v1/index` documentation applies to ``databroker.v0`` and
``databroker.v1``. The :doc:`v2/index` documentation applies to
``databroker.v2``.

Both ``databroker.v1.Broker`` and ``databroker.v2.Broker`` have accessor
attributes, ``v1`` and ``v2``, that support usage of the other's interface,
making it easy to switch between them.

Here we make a v1-style Broker but the ``v2`` accessor to try v2-style
features.

.. code:: python

   from databroker import Broker
   db = Broker.named('MY_CATALOG')  # a databroker.v1.Broker instance
   query = dict(plan_name='count')
   db(**query)  # v1-style search
   db.v1(**query)  # a synonym for the above
   db.v2.search(query)  # v2-style search

Here we make a v2-style Broker but the ``v1`` accessor to fall back to v1-style
usage.

.. code:: python

   from databroker import catalog 
   db = catalog.SOMETHING()  # a databroker.v2.Broker instance
   query = dict(plan_name='count')
   db.search(query)  # v2-style search
   db.v2.search(query)  # a synonym for the above
   db.v1(**query)  # v1-style search

Roadmap Beyond 1.0
------------------

* When the v1 implementation is proven reliable, v0 will be removed, greatly
  reducing the amount of code in databroker. This should not have an effect on
  users, as v1 provides the same interface as v0. The only differences are
  internal: v1 uses intake internally instead of its own separate
  implementation to achieve the same end.
* If and when the v2 interface is shown to meet users' needs as well or better
  than v1, it will become the default. To support existing user code, the v1
  interface will continue to be available in ``databroker.v1``.
* After several years of maintaining both v1 and v2, when all critical user
  code has been migrated to v2, v1 may be deprecated and eventually removed. At
  that point, DataBroker will be just a distribution of intake plugins.
