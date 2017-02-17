.. _api_changes:

=============
 API changes
=============

Non-backward compatible API changes


v0.8.0
======

``Header``
----------

Change `Header` from a `doct.Document` subclass to a ``attr`` based
class.  A majority of the API has been maintained, however there are
changes in what exceptions are raised when trying to mutate the
instance.

+----------------+--------------------------+---------------------------------------+
| Method         | Old Exception            | New Exception                         |
+================+==========================+=======================================+
| ``h.pop``      | `doc.DocumentIsReadOnly` | `AttributeError`                      |
+----------------+--------------------------+---------------------------------------+
| ``h.update``   | `doc.DocumentIsReadOnly` | `AttributeError`                      |
+----------------+--------------------------+---------------------------------------+
| ``del h.x``    | `doc.DocumentIsReadOnly` | `attr.exceptions.FrozenInstanceError` |
+----------------+--------------------------+---------------------------------------+
| ``del h['x']`` | `doc.DocumentIsReadOnly` | `TypeError`                           |
+----------------+--------------------------+---------------------------------------+
| ``h.x = V``    | `doc.DocumentIsReadOnly` | `attr.exceptions.FrozenInstanceError` |
+----------------+--------------------------+---------------------------------------+
| ``h['x'] * V`` | `doc.DocumentIsReadOnly` | `TypeError`                           |
+----------------+--------------------------+---------------------------------------+

``Header.from_run_start``
-------------------------

Take a `DataBroker` object instead of a `MetadataStore` object.  This
is now tacked on the `Header` object.

Changes to functions in `databroker.core`
-----------------------------------------

Explicitly passed mds/fs have been removed, instead relying on the
DataBroker instance included in the header.

Break up internal structure of databroker
-----------------------------------------

* The core functions that touch events have a new required argument, ``es``.
  This does not affect the API of the ``Broker`` object, only the functions in
  the ``core`` module.

v0.4.2
======

 - Change ``name`` -> ``stream_name`` in the signature of `get_table`
