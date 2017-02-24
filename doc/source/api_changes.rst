.. _api_changes:

API changes
***********

Non-backward compatible API changes

dev
---

* The core functions that touch events have a new required argument, ``es``.
  This does not affect the API of the ``Broker`` object, only the functions in
  the ``core`` module.

v0.4.2
------

 - Change ``name`` -> ``stream_name`` in the signature of `get_table`
