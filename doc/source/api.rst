.. _api_ref:

.. currentmodule:: dataportal

API reference
=============

Looking Up Scans
----------------

.. autosummary::
   :toctree: generated/

    DataBroker.find_headers

You can also using slicing syntax on ``DataBroker``.

=============================== ==========================================================
syntax                          meaning
=============================== ==========================================================
``DataBroker[-1]``              most recent scan
``DataBroker[-5]``              fifth most recent scan
``DataBroker[-5:]``             all of the last five scans
``DataBroker[108]``             scan with scan ID 108 (if ambiguous, most recent is found)
``DataBroker[[108, 109, 110]]`` scans with scan ID 108, 109, 110
``DataBroker['acsf3rf']``       scan with unique ID (uid) beginning with ``acsf3rf``
=============================== ==========================================================
