How Data and Metadata are Organized
===================================

Documents
---------

Data and metadata are bundled into what we dub *documents*, Python dictionaries
organized in a `formally specified <https://github.com/NSLS-II/event-model>`_
way. For every "run" --- loosely speaking, a dataset --- there are four types
of document.

- A Run Start document, containing all of the metadata known at the start.
  Highlights:

    - time --- the start time
    - plan_name --- e.g., ``'scan'`` or ``'count'``
    - uid --- randomly-generated ID that uniquely identifies this run
    - scan_id --- human-friendly integer scan ID (not necessarily unique)
    - any other :doc:`metadata captured execution time <metadata>` from the
      plan or the user

- Event documents, containing the actual measurements. Highlights:

    - time --- a timestamp for this group of readings
    - data --- a dictionary of readings like
      ``{'temperature': 5.0, 'position': 3.0}``
    - timestamps --- a dictionary of individual timestamps for each reading,
      from the hardware

- Event Descriptor documents, with metadata about the measurements in the
  events (units, precision, etc.) and about the configuration of the hardware
  that generated them.

- A Run Stop document, containing metadata known only at the end. Highlights:

    - time --- the time when the run was completed
    - exit_status --- "success", "abort", or "fail"

We refer you
`this section of the bluesky documentation <https://nsls-ii.github.io/bluesky/documents.html>`_
for more details and context.

Headers
-------

The result of a :doc:`search <searching>` is a *header*, which bundles together
the metadata-related documents:

* header.start --- the "Run Start" document
* header.descriptors --- the "Event Descriptor" documents
* header.stop -- the "Run Stop" document

The only documents omitted from ``header`` are the *events*, which contain
(most of) the actual measured data. That may take more time to load, so we load
it in a separate step. See :doc:`fetching`.

Some useful examples:

.. code-block:: python

    # When did this run start and end?
    header.start.time
    header.stop.time

    # What kind of experimental procedure ("plan") was this?
    header.start.plan_name  # e.g., 'scan', 'relative_scan', etc.

    # Did it finish successfully?
    header.stop.exit_status  # 'success', 'fail', or 'abort'

In later, more specific examples, we'll see more specific and useful metadata.

.. note::

    Keys in a header can be accessed in two ways. This are equivalent:

    .. code-block:: python
        
        header['start']['time']
        header.start.time
