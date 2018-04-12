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


   class EventSource:
       @property
       def NoEventDescriptors(self) -> Exception:
           ...

       def __init__(self, **kwargs):
           ...

       def stream_names_given_header(self, header: Header) -> List[str]:
           ...

       def fields_given_header(self, header: Header,
                               stream_name=None : Optional[str]) -> List[str]:
           ...

       def descriptors_given_header(self, header: Header,
                                    stream_name=None: Optional[str]
                                    ) -> List[dict]:
           ...

       def descriptor_given_uid(self, desc_uid : str) -> dict:
           ...

       def docs_given_header(self, header: Header,
                             stream_name=None: Optional[str],
                             fields=None: Optional[List[str]]
                             ) -> Generator[Tuple[str, dict]]:
           ...

       def table_given_header(self, header: Header,
                              stream_name: str,
                              fields=None : List[str],
                              convert_times=True: bool,
                              timezone=None: Optional[str],
                              localize_times=True: bool) -> pd.DataFrame:
           ...

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



Implementation
==============


Backward compatibility
======================



Alternatives
============

Status-quo
