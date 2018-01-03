==============================================================
  Support bulk event / datum tables as first-class documents
==============================================================

.. contents::
   :local:


Status
======


**Discussion**

Branches and Pull requests
==========================


Abstract
========

On both the ingest and access sides it may not be practical, for
performance reasons, to access *Event* and *Datum* on a
document-by-document basis.  Within a given stream and
*Resource* *Event* and *Datum* documents are consistent and thus can
be represented as dense tables.

We currently have a *bulk_event* schema, however it is an array of
*Event* not a table and does not vectorize in the correct ways.


Detailed description
====================

For both *EventTable* and *DatumTable* the schema is identical to the single
version except in place of scaler values there is an array of scaler values.
These can be represented as:

- nested `dict` with `numpy.array` or `list` at the bottom
- a `dict` of `pandas.DataFrame` with simple column index and a shared row index
- a `pandas.DataFrame` with a MultiIndex



Implementation
==============


Backward compatibility
======================

Existing *Bulk_Events* schema and :meth:`bulk_insert_events` methods will
be deprecated.  :meth:`bulk_register_datum_table` maybe deprecated to ensure
that it is symmetrical with the event insert methods.

Alternatives
============

None
