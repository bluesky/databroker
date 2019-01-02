.. currentmodule:: databroker


**********************************
Connection with Archiver Appliance
**********************************

The Archiver Appliance is an EPICS logging system developed 
in a collaboration of SLAC, BNL, and MSU for collecting and 
storing measurements from various control devices. Data retrieval 
is based on the client-server interface using HTTP requests. 
Large-scale accelerators and experimental facilities may maintain 
multiple archivers. Within the databroker application, each archiver 
is represented by an ArchiverEventSource that needs to be configured 
and registred with the Broker. The following sections highlight 
two major topics: ArchiverEventSource configuration and data retrival 
based on the databroker interface.

Configuration
-------------

ArchiverEventSources is configured via :class:`Broker` by
extending the databroker configuration file with a list of archiver
entries. For example, the following cxs_with_archiver.yml file
illustrates the configuration of two ArchiverEventSources, arch_csx
and arch_acc:

.. code-block:: yaml

 event_sources:
 - config:
      name: 'arch_acc'
      url: 'http://arcapp01.cs.nsls2.local:17668'
      timezone: 'US/Eastern'
      pvs:
        pv2: 'UT:SB1-Cu:1{}T:Prmry-I'
   module: 'databroker.eventsource.archiver'
   class: 'ArchiverEventSource'
 - config:
     name: 'arch_csx'
     url: 'http://xf23id-ca.cs.nsls2.local:17668'
     timezone: 'US/Eastern' 
   module: 'databroker.eventsource.archiver'
   class: 'ArchiverEventSource'

According to this file, each ArchiverEventSource is defined 
with four configuration key-value pairs :

* name: user-defined name of the Archiver Appliance archiver
* url: address of the Archiver Appliance Retrieval server
* timezone: time zone
* pvs: dictionary mapping user-defined names to EPICS PVs

A pvs dictionary can be extended via :class:`Broker` as:

.. code-block:: python
  
   db = Broker.named('csx_with_archivers')
   arch_csx  = db.event_sources_by_name['arch_csx']
   arch_csx.pvs.update({'pv1':'XF:23ID-ID{BPM}Val:PosXS-I'})



Data Retrieval 
--------------

After integrating ArchiverEventSources with :class:`Broker`, 
PV data can be retrieved with the standard :meth:`Header.table` 
method:

.. code-block:: python

   # select header
   hdr = db[69209]

   stream_name = 'pv1'
   df = db.table(stream_name) 	
 

