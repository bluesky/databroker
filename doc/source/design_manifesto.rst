==================
 Design Manifesto
==================

The Nouns
=========


``Header``
----------
 - Attributes
    - ``Start`` Document
    - ``Stop`` Document
        - may want to flip non-existant behavior from ``{}`` -> ``None``
    - Reference back to a ``DataBroker``

 - properties (ex, LAZY):
   - ``streams`` list of streams from all of the event stores
   - ``descriptors`` mapping to lists of descriptors per stream

 - Methods:
   - ``h.stream(stream_name, fill=True) -> generator``
     - generator for just this stream,
     - (name, doc) for all document types
   - ``h.table(stream_name, fill=True) -> DataFrame``
     - Dataframe for just this stream
   - ``h.events(fill=True) -> generator``
     - all documents as (name, doc) pair
     - this should maybe be renamed?
   - ``h.es_given_stream(stream_name) -> EventSource``
  - mimic ``doct.Document`` interface?


The implementation of these methods should live in the ``Header`` class using the resources
provided by the attached ``DataBroker``


``DataBroker``
--------------
 - Attributes:
   - Exactly 1 ``HeaderSource``
   - ``EventSource`` list
 - Methods:
  - mirror out all search from ``HeaderSource``, converts to ``Header`` objects
  - manage filters + search state
  - drop everything else!
  - insert?
    - how to handle ES distribution.

``HeaderSource``
----------------
 - databases, not public:
   - ``Start`` document collection / table
   - ``Stop`` document collection / table

 - Methods
   - provides search capabilities over the start / stop documents
   - ``hs[uid] -> (Start, Stop)``
   - ``hs[scan_id] -> (Start, Stop)``
   - ``hs[-offset] -> (Start, Stop)``
   - ``hs(full=text, search=method) -> (Start, Stop)``

``EventSource``
---------------
 - databases, not public:
   - ``Event`` collection
   - ``Descriptor`` document collection
   - ``Filestore`` collection(s) / object

 - Methods:
   - insert ``es.insert(name, doc)``
   - get streams given a ``Header``
     - ``es.stream_names_given_header(header) -> {stream_names}``

   - get descriptors
     - ``es.descriptors_given_header(header, stream_name=ALL, **kwargs) -> [Descriptor]``

   - get data payload given a ``Header``
    - ``es.documents_given_header(header, stream_name, fill=False, fields=None, **kwargs) -> doc_generator``
    - ``es.table_given_header(header, stream_name, fill=False, fields=None, **kwargs) -> DataFrame``

    The reason to keep both the generator and table versions of this
    is to allow the event stores to optimize for a given access
    pattern.  Some data should be stored in columnar / tabular fashion

   - do de-referencing (maybe in place)
     - ``es.fill_event(ev, in_place=False, fields=None, handler_registry=None, handler_overrides=None)) -> new_Event``
     - ``es.fill_table(tab, in_place=False, fields=None, handler_registry=None, handler_overrides=None)) -> DataFrame``
     - ``es.fill_event_stream(ev_gen, in_place=False, fields=None, handler_registry=None, handler_overides=None)) -> Event_gen``


Helpers
=======

 - ``stream_to_table(doc_generator) -> DataFrame``
 - ``table_to_stream(table, header=None, stream=None) -> doc_generator``
   - this one may be tricky as going to a table may lose the link back to the run
   - particularly if any synthetic columns (ex normalizations) have happened.
 - a few accumulator/buffer objects to aid working with sequences of
   (name, document) pairs



Random concerns
===============

 - should implement a global registry of known ``DataBroker`` /
   components so that un-pickling a header does not recreate all of
   the db connections.  We clearly do not have enough meta-classes.
   The need for this goes away when we move to a fully service model where the only
   state the brokers need to keep is a url and maybe a process-local cache.
 - how to not lose metadata back to descriptor / header when going to a table
 - should we mutate descriptors when keys are added / removed from
   events via filtering / broadcasting
  - if we do this, should probably give new uid to descriptor.  This
    will require doubling down on the idea that for streams of
    documents are always mixed types and of the form ``(name, doc)``
  - we may also want to back off on the aggressive de-normalization of
    the descriptors at every level.  Working always in one process the
    cost of de-normalizing is low because we can share an object
    (which is the reason that `doct.Document` is immutable), however
    if we move to a model where these documents are streamed between
    process (local or not) this can result in massive overheads.  This
    dumps a lot of complexity into the clients, but it is complexity
    that we are already having to deal with (because bluesky spits out
    uids, DataBroker return the documents in-place.).
 - not clear we are not going to end up with two worlds, a document
   streaming one and a DataFrame based one.
 - there is a possible collision when we merge the config from all of
   the objects to do the projection
