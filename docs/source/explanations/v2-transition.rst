What are the API versions v0, v1, v2?
=====================================

The Databroker codebase currently contains two completely and mostly indepenent
implementations. One is the original implementation from 2015. The other is a
rewrite performed in 2019--2020, written to leverage the scientific Python
libraries `dask`_, `intake`_, and `xarray`_, which emerged or reached maturity
some time after 2015. Databroker 1.x provides three public interfaces on top of
these two implementations.

=========== ========= ============== ==========================
API version Interface Implementation Who should use it?
=========== ========= ============== ==========================
v2          New       New            All new users
v1          Original  New            Users with old scripts that use original interface
v0          Original  Original       Users who hit bugs in v1/v2 and need a fallback
=========== ========= ============== ==========================

Which one should I use?
-----------------------

If you are a new user, use v2. That is the version covered by the tutorials and
user guides.

As far as we know the only heavy users of the "original" 2015 interface are at
NSLS-II. If you have existing scripts using that original interface, know that
we intend to continue to support it for many years. We do not want to break
your scripts. Consider using v2 for *new* work however, to enjoy its improved
usability and feature set.

How do use them?
----------------

All of the tutorials now use ``databroker.v2``. As they show, this gets you a
v2-style catalog.

.. code:: python

   # v2, recommended for new users
   import databroker
   catalog = databroker.catalog["MY CATALOG NAME"]

This older-style usage getse you a v1-style catalog.

.. code:: python

   # v1, supported for existing users supporting old code
   from databroker import Broker
   db = Broker.named("MY CATALOG NAME")
   
Since there are just different interfaces built on the same underlying (new)
implementation, we can easily to move between them.

.. code:: python

   catalog  # v2
   catalog.v1  # v1
   catalog.v2  # v2  (just returns a reference to itself)
   db  # v1
   db.v1  # v1  (just returns a reference to itself)
   db.v2  # v2

Therefore, code written like

.. code:: python

   def f(catalog_or_db):
       catalog = catalog_or_db.v2  # ensure we have a v2 interface
       ...

will work on both v1-style and v2-style.

Finally, the v0 implementation is available as the battle-tested emergency
fallback in case of show-shoping bugs the younger implementation underlying v1
and v2. You cannot move between v0 and other interfaces. You can invoke v0 like
so:

.. code:: python

   # v0, emergency fallback if v1/v2 is broken
   from databroker.v0 import Broker
   db = Broker.named("MY CATALOG NAME")

In the future, we will remove v0 from the codebase; v1 will be sufficient to
support old user code.

.. _intake: https://intake.readthedocs.io

.. _xarray: https://xarray.pydata.org/

.. _dask: https://dask.org/
