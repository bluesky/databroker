===================
 DBEP 6 : Saving to Analysis Store
===================


.. contents::
   :local:

Status
======

**Discussion**


Branches and Pull requests
==========================
There is a prototype for ISS on the dev branch here:
https://github.com/NSLS-II-ISS/profile_collection/blob/master-dev/startup/50-insert_analysis.py
I have tested it and it works.


Abstract
========
The motivation of this document is to try to consolidate all efforts for
storing derived data generated from beamlines in a more permanent setting.
It should help with:

* avoiding duplicating code
  * making design decisions that will survive most long term use cases



Detailed description
====================


Proposed Handler/Writer Scheme:

The tricky part in implementing this is how to define the writers and file
handlers for the data.
(The rest is just shove→ databroker) This section will focus on just that.

There are two pieces to retrieving/storing data:


1. The Handler: How the data will be retrieved

    It works as usual: resource kwargs initialize the instance
    and datum kwargs retrieve data upon calling the instance.

2. The Writer: How the data will be stored

    The writer should write data that is eventually read by the handler.
    Therefore:
    1. The initialization should require the same exact kwargs that the
        handler's initialization requires. i.e. The information used to
        retrieve should be enough to write and vice versa.
    2. The call should receive a data dictionary and iterate over each
        key. This should then return the datum_kwargs necessary to re-read
        this data. i.e. When we send this data for writing, we need to know
        how to get it back.

This scheme requires just two items for transforming data into a series of
documents:
    1. ``data_dicts``: a list of nested dicionaries:
        ``data_dicts[n][stream_name][key]`` gives one data element (number, np
        array etc)
        where ``n`` is just a number, ``stream_name`` the stream name and key
        the data_key

    2. ``md``: metadata for the start document

Optionally, one can pass a ``writers_dict``. This is a dictionary of writers:

.. code-block:: yaml

    'data_key':
        writer :  the writer class
        resource :
            func :  the function to create the resource
                    of form f(docs, data_key, **additional_kwargs)
            kwargs:  the additional kwargs for the func above
        root : the analysis root
        spec : the spec id for the handler that would read this

where 'data_key' is the key name of the data (i.e. 'pilatus2M_image' etc...).
This should be enough to submit documents to an analysis store.


Data Provenance
===============
Data from analysis store will typically come from a few locations. For example:
1. Data derived from an analysis pipeline which pulled data straight from
   databroker
2. Data ingested from a file in a scientist's directory.

When storing this data, rather than leaving it up to the user, we may want to
enforce saving certain amounts of information.

In the case of data coming from an analysis pipeline derived from data stored
in metadatastore/filestore, saving the uid's of the parents involved may be
enough. For data ingested from files, the file path may be sufficient. We may
want to list all elements needed for some rudimentary provenance:

* parent uid's (if applicable)
* filenames (if applicable)
* source code
    * name
    * version/commit hash
    * URL

It might be worth discussing all the possible provenance we might want to
enforce to first order.

Resources/similar work
======================

    CJ has written a library that takes data and tranforms it into a series of
    documents:
        https://github.com/xpdAcq/SHED/blob/master/shed/translation.py
    As far as I(Julien L.) understand, this does not write data to disk, or
    emit resource/datum documents.


Backwards Compatibility
=======================

Currently, this implementation has the post-asset refactor in mind.  There are
some subtle changes that affect how this may work. One obvious example is that
the resource and datum documents are now first class documents.  I have
currently written the current version to support documents before the asset
refactor, but we should maybe move on from that.
