.. _api:


API reference
=============

.. currentmodule:: dataportal.broker

Data Broker
-----------

.. autosummary::
   :toctree: generated/

    Header
    get_events
    get_table
    get_images
    DataBroker.__call__
    DataBroker.__getitem__

To search for a specific header use call syntax on ``DataBroker``.

You can also using slicing syntax on ``DataBroker``.

=============================== ==========================================================
syntax                          meaning
=============================== ==========================================================
``DataBroker[-1]``              most recent header
``DataBroker[-5]``              fifth most recent header
``DataBroker[-5:]``             all of the last five headers
``DataBroker[108]``             header with scan ID 108 (if ambiguous, most recent is found)
``DataBroker[[108, 109, 110]]`` headers with scan IDs 108, 109, 110
``DataBroker['acsf3rf']``       header with unique ID (uid) beginning with ``acsf3rf``
=============================== ==========================================================

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
