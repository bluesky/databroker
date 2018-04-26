=====================================
 Merge EventSource and AssetRegistry
=====================================

.. contents::
   :local:


Status
======

DBEPs go through a number of phases in their lifetime:

- **Discussion**:

Branches and Pull requests
==========================


Abstract
========

While discussing how the internals of the Broker and the current
division of labor between HeaderSource, EventSource, and AssetRegisty
in light of DBEP02 (promoting Resource and Datum to first-class
documents) we decided that the separation between Resource/Datum
collections and the Descriptor/Event collections was not providing any
benefit either practically or theoretically.  This proposes to

- merge the Resource and Datum collections into EventSource
- move the handler-registry and filling responsibility to Broker
- move the file moving logic into top level functions
- add methods to update Resource documents to EventSource


Detailed description
====================

API proposal ::

  from typeing import (List, Optional, Generator, Tuple, Dict)
  from databroker import Header
  import pandas as pd
  from enum import Flag

  @attr.s(frozen=True)
  class TimeStruct(object):
      covert_times = attr.ib()
      timezone = attr.ib()
      localize_times = attr.ib()


  class EventSource:
      @property
      def NoEventDescriptors(self) -> Exception:
          ...

      def __init__(self, **kwargs):
          ...

      # ######
      #
      # This related to meta-data.
      #   - getting the descriptors
      #   - get stream names
      #   - get fields / keys
      #   - get config
      #   - get the resource documents
      #
      # ######
      def stream_names(self, header: Header) -> List[str]:
          ...

      def fields(self, header: Header,
                 stream_name=None : Optional[str]) -> List[str]:
          ...

      def descriptors(self, header: Header,
                      stream_name=None: Optional[str]) -> List[dict]:
          ...

      def resources(self, header: Header) -> List[dict]:
          ...

      # ######
      #
      # This related to getting raw data
      #   - do we want to control event vs bulk event?
      #   - do we want to guarantee insertion order (ex if events came in
      #     stream A, stream B, stream A, stream B do we want to replicate
      #     that or just maintain that we see the descriptor before it's events)
      #
      # ######
      def docs(self, header: Header,
               stream_name=None: Optional[str],
               fields=None: Optional[List[str]],
               fill=True: Optional[bool]
      ) -> Generator[Tuple[str, dict]]:
          """Generator of the documents.

          Yields out everything, (start, descriptor, resource, event, datum, stop)
          """
          ...

      # ######
      #
      # This is related to getting things back as dataframes
      #   - do we want to actually use data frames or do we want to
      #     provide dict (of dicts?) of arrays
      #   - do we want to use muliti-index to include the hardware timestamps?
      #     - if so, what level / order (data/timestamp -> datakeys) or
      #       (datakeys -> data/timestamp)
      #
      # ######
      def table(self, header: Header,
                stream_name: str,
                fields=None : Optional[List[str]],
                time_struct=None: Optional[TimeStruct],
                timestamps=False: Optional[bool]) -> pd.DataFrame:
          ...

      def table_chunks(self, header: Header,
                       stream_name: str,
                       fields=None : Optional[List[str]],
                       time_struct=None: Optional[TimeStruct],
                       chunk_size=None, Optional[int]) -> Generator[pd.DataFrame]:
          ...

      def table_chunk(self, header: Header,
                      stream_name: str,
                      chuck_number: int,
                      chunk_size=None: Optional[int],
                      fields=None: Optional[List[str]],
                      time_struct=None: Optional[TimeStruct],
                      chunk_size=None, Optional[int]) -> pd.DataFrame:
          ...

      # ######
      #
      # related to details of Asset Registry
      #
      # ######
      def set_root_map(self, root_map: Dict[str, str]) -> None:
          ...


  class InsertEV(EventSource):
      def insert(self,
                 name: {'event', 'bulkevent',  'descriptor',
                        'datum', 'bulkdatum', 'resource'},
                 doc: dict) -> None:
          ...


  class FillerBroker(Broker):
      def register_handler(self, key: str, handler: Handler,
                           overwrite=False: bool) -> None:
          ...

      def deregister_handler(self, key: str) -> None:
          ...



Questions
---------

Do we want to provide a 'fill' kwarg?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If we do not provide a fill kwarg then many if the interfaces get
simpler, as we never have to worry outside of the EventSource internal
if a value is 'real' or a pointer.  It also further shields users from
having to know that that Resource or Datum exist.  The way to avoid
pulling up big data payloads is then to just filter out the fields you
do not want.  Because we only provide access to the filled data
through a handful of methods that either pull the data all up or
provide a generator we can be relatively sure about lifetime and
resource management.

If we do provide a fill kwarg then users can end up with unfilled
events so we will need to provide a way to come back and fill those
events.  There are some issues with performance an resource management
if we provide truly random access (either you keep maybe too many
files open / cached data sets up or you waste lots of time opening and
closing files).

An advantage of providing access to the datum uids is you can now ship
un-filled events to worker nodes and have them then reach out to grab
the data.


One possible API is ::

  with header.fill_context() as fc:
      fc.fill(ev_doc, fields)
      fc.fill_table(table, fields)

which lets us have clear grantees about both scope of any given call
and clear lifetime of when we need to have resources open.

Implementation
==============


Backward compatibility
======================



Alternatives
============

Status-quo
