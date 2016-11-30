==================
 Design Manifesto
==================

The Nouns
=========


``Header``
----------

 - ``Start`` Document
 - ``Stop`` Document
 - ``Descriptors`` list or mapping to lists of descriptors (??)
   - maybe this could be lazy?
 - Reference back to a ``DataBroker``


``DataBroker``
--------------
 - Exactly 1 ``HeaderSource``
 - 1 or more ``EventSource``
 - mirror out

``HeaderSource``
----------------
 - ``Start`` document collection / table
 - ``Stop`` document collection / table
 - provides search capabilities over the start / stop documents
   - ``hs[uid]``
   - ``hs[scan_id]``
   - ``hs[-offset]``
   - ``hs(full=text, search=method)``

``EventSource``
---------------
 - ``Event`` collection
 - ``Descriptor`` document collection
 - ``Filestore`` collection(s)
 - get data payload given a ``Header``
    - ``es.event_gen_given_header(header, fill=False, **kwargs) -> generator``
    - ``es.table_given_header(header, fill=False, **kwargs) -> DataFrame``
 - get descriptors given a ``Header``
   - ``es.get_descriptor(header, **kwargs) -> Descriptor``
