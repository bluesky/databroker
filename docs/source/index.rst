.. Packaging Scientific Python documentation master file, created by
   sphinx-quickstart on Thu Jun 28 12:35:56 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Intake Bluesky
==============

Search for data, and retrieve it as SciPy/PyData data structures for
interactive data exploration or in
`a representation suitable for streaming applications <https://nsls-ii.github.io/event-model>`_ .

`Intake <https://intake.readthedocs.io>`_ loads data from a growing variety of
formats into familiar Scipy/PyData data structures.
`Bluesky <https://nsls-ii.github.io/bluesky>`_ is a suite of co-developed
Python packages for data acquisition and management designed to drive
experiments and capture data and metadata from experiments and simulations in a
way that interfaces naturally with open-source software in general and the
scientific Python ecosystem in particular. Intake-Bluesky applies intake to
bluesky.

* Its Catalogs' search functionality leverages the MongoDB query language
  instead of performing plain-text search.
* Intake-Bluesky ships Catalogs that embody the semantics of bluesky's data
  model. A bluesky "run" (e.g. one scan) is represented by a
  :class:`~intake_bluesky.core.BlueskyRun`.  Each logical table of data within
  a given run is represented by a
  :class:`~intake_bluesky.core.BlueskyEventStream`.
* The methods :meth:`~intake_bluesky.core.BlueskyEventStream.read()` and
  :meth:`~intake_bluesky.core.BlueskyEventStream.to_dask()` provide the data in
  SciPy/PyData structures and their "lazy" dask-backed counterparts, as with
  any other intake data source.
* The additional method :meth:`~intake_bluesky.core.BlueskyRun.read_canonical`
  returns a generator suitable for streaming. Its elements satisfy
  `bluesky's data model <https://nsls-ii.github.io/event-model>`_ and can be
  fed into streaming visualization, processing, and serialization tools that
  consume this representation.

Bluesky is unopinionated about file formats. It provides a `variety of
serializers <https://nsls-ii.github.io/suitcase>`_ for encoding the stream of
acquired data to persistent storage. (Note that large detectors may write
directly to disk, in which case bluesky records, in effect, a pointer.)
Different formats and storage may be appropriate for different scientific
domains and scales. A single graduate student might dump their data into local
files, whereas a lab or facility might use a MongoDB instance. Intake-Bluesky
will address the range of possible use cases by implementing an intake driver
for each serializer. Currently supported:

* :class:`~intake_bluesky.mongo_normalized.BlueskyMongoCatalog` --- Backed by
  MongoDB
* :class:`~intake_bluesky.jsonl.BlueskyJSONLCatalog` --- Backed by a set of
  newline-delimited JSON files, illustrating "minimal deployment overhead" use
  case

Intake-Bluesky will also address the use case of reading files *not* produced
by bluesky, retrofitting the semantics of its data model. Thus, a "bucket
of files" such as a directory of TIFFs could be fed through tools that consume
the representation returned by
:meth:`~intake_bluesky.core.BlueskyRun.read_canonical`.

.. note::

   These drivers are currently being developed in intake-bluesky itself, but
   will eventually be split out into separate repositories to isolate
   dependencies and release cycles. This will be done once the interface with
   core is deemed stable.

.. toctree::
   :maxdepth: 2

   installation
   usage
   api
