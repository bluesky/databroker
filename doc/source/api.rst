.. _api_ref:


API reference
=============

.. currentmodule:: dataportal.broker

Data Broker
-----------

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

.. currentmodule:: dataportal.broker

.. autosummary::
   :toctree: generated/

   DataBroker.fetch_events
   Header
   EventQueue
   LocationError
   IntegrityError

Data Muxer
----------

.. currentmodule:: dataportal.muxer

.. autosummary::
   :toctree: generated/

   DataMuxer
   ColSpec
   dataframe_to_dict
   BinningError
   BadDownsamplerError

Scans
-----

.. currentmodule:: dataportal.scans

.. autosummary::
   :toctree: generated/

   StepScan.find_headers

StepScan also supports the same slicing interface as ``DataBroker`` above.
