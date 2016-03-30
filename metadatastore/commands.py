from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six

import datetime
import logging

import pytz

from . import conf
from . import mds

# API compatibility imports
from .core import NoEventDescriptors, NoRunStop, NoRunStart

logger = logging.getLogger(__name__)


_DB_SINGLETON = mds.MDS(conf.connection_config)


def doc_or_uid_to_uid(doc_or_uid):
    """Given Document or uid return the uid

    Parameters
    ----------
    doc_or_uid : dict or str
        If str, then assume uid and pass through, if not, return
        the 'uid' field

    Returns
    -------
    uid : str
        A string version of the uid of the given document

    """
    if not isinstance(doc_or_uid, six.string_types):
        doc_or_uid = doc_or_uid['uid']
    return doc_or_uid


def clear_process_cache():
    """Clear all local caches"""
    _DB_SINGLETON.clear_process_cache()


def db_disconnect():
    """Helper function to deal with stateful connections to mongoengine"""
    _DB_SINGLETON.disconnect()


def db_connect(database, host, port, timezone='US/Eastern', **kwargs):
    """Helper function to deal with stateful connections to mongoengine

    .. warning

       This will silently ignore input if the database is already
       connected, even if the input database, host, or port are
       different than currently connected.  To change the database
       connection you must call `db_disconnect` before attempting to
       re-connect.
    """
    return _DB_SINGLETON.db_connect(database=database, host=host, port=port,
                                    timezone=timezone, **kwargs)


def run_start_given_uid(uid):
    """Given a uid, return the RunStart document

    Parameters
    ----------
    uid : str
        The uid

    Returns
    -------
    run_start : doc.Document
        The RunStart document.

    """
    return _DB_SINGLETON.run_start_given_uid(uid)


def run_stop_given_uid(uid):
    """Given a uid, return the RunStop document

    Parameters
    ----------
    uid : str
        The uid

    Returns
    -------
    run_stop : doc.Document
        The RunStop document fully de-referenced

    """
    return _DB_SINGLETON.run_stop_given_uid(uid)


def descriptor_given_uid(uid):
    """Given a uid, return the EventDescriptor document

    Parameters
    ----------
    uid : str
        The uid

    Returns
    -------
    descriptor : doc.Document
        The EventDescriptor document fully de-referenced
    """
    return _DB_SINGLETON.descriptor_given_uid(uid)


def stop_by_start(run_start):
    """Given a RunStart return it's RunStop

    Raises if no RunStop exists.

    Parameters
    ----------
    run_start : doc.Document or dict or str
        The RunStart to get the RunStop for.  Can be either
        a Document/dict with a 'uid' key or a uid string

    Returns
    -------
    run_stop : doc.Document
        The RunStop document

    Raises
    ------
    NoRunStop
        If no RunStop document exists for the given RunStart
    """
    return _DB_SINGLETON.stop_by_start(run_start)


def descriptors_by_start(run_start):
    """Given a RunStart return a list of it's descriptors

    Raises if no EventDescriptors exist.

    Parameters
    ----------
    run_start : doc.Document or dict or str
        The RunStart to get the EventDescriptors for.  Can be either
        a Document/dict with a 'uid' key or a uid string

    Returns
    -------
    event_descriptors : list
        A list of EventDescriptor documents

    Raises
    ------
    NoEventDescriptors
        If no EventDescriptor documents exist for the given RunStart
    """
    return _DB_SINGLETON.descriptors_by_start(run_start)


def get_events_generator(descriptor):
    """A generator which yields all events from the event stream

    Parameters
    ----------
    descriptor : doc.Document or dict or str
        The EventDescriptor to get the Events for.  Can be either
        a Document/dict with a 'uid' key or a uid string

    Yields
    ------
    event : doc.Document
        All events for the given EventDescriptor from oldest to
        newest
    """
    for ev in _DB_SINGLETON.get_events_generator(descriptor):
        yield ev


def get_events_table(descriptor):
    """All event data as tables

    Parameters
    ----------
    descriptor : dict or str
        The EventDescriptor to get the Events for.  Can be either
        a Document/dict with a 'uid' key or a uid string

    Returns
    -------
    descriptor : doc.Document
        EventDescriptor document
    data_table : dict
        dict of lists of the transposed data
    seq_nums : list
        The sequence number of each event.
    times : list
        The time of each event.
    uids : list
        The uid of each event.
    timestamps_table : dict
        The timestamps of each of the measurements as dict of lists.  Same
        keys as `data_table`.
    """
    return _DB_SINGLETON.get_events_table(descriptor)


# database INSERTION ###################################################

def insert_run_start(time, scan_id, beamline_id, uid, owner='', group='',
                     project='', **kwargs):
    """Insert a RunStart document into the database.

    Parameters
    ----------
    time : float
        The date/time as found at the client side when the run is started
    scan_id : int
        Scan identifier visible to the user and data analysis.  This is not
        a unique identifier.
    beamline_id : str
        Beamline String identifier.
    uid : str
        Globally unique id to identify this RunStart
    owner : str, optional
        A username associated with the RunStart
    group : str, optional
        An experimental group associated with the RunStart
    project : str, optional
        Any project name to help users locate the data

    Returns
    -------
    run_start : str
        uid of the inserted document.  Use `run_start_given_uid` to get
        the full document.

    """
    return _DB_SINGLETON.insert_run_start(time=time, scan_id=scan_id,
                                          beamline_id=beamline_id, uid=uid,
                                          owner=owner, group=group,
                                          project=project, **kwargs)


def insert_run_stop(run_start, time, uid, exit_status='success', reason='',
                    **kwargs):
    """Insert RunStop document into database

    Parameters
    ----------
    run_start : doc.Document or dict or str
        The RunStart to insert the RunStop for.  Can be either
        a Document/dict with a 'uid' key or a uid string
    time : float
        The date/time as found at the client side
    uid : str
        Globally unique id string provided to metadatastore
    exit_status : {'success', 'abort', 'fail'}, optional
        indicating reason run stopped, 'success' by default
    reason : str, optional
        more detailed exit status (stack trace, user remark, etc.)

    Returns
    -------
    run_stop : str
        uid of inserted Document

    Raises
    ------
    RuntimeError
        Only one RunStop per RunStart, raises if you try to insert a second
    """
    return _DB_SINGLETON.insert_run_stop(run_start, time, uid,
                                         exit_status=exit_status,
                                         reason=reason, **kwargs)


def insert_descriptor(run_start, data_keys, time, uid, **kwargs):
    """Insert an EventDescriptor document in to database.

    Parameters
    ----------
    run_start : doc.Document or dict or str
        The RunStart to insert a Descriptor for.  Can be either
        a Document/dict with a 'uid' key or a uid string
    data_keys : dict
        Provides information about keys of the data dictionary in
        an event will contain.  No key name may include '.'.  See
        `DataKey` odm template for schema.
    time : float
        The date/time as found at the client side when an event
        descriptor is created.
    uid : str
        Globally unique id string provided to metadatastore

    Returns
    -------
    descriptor : str
        uid of inserted Document
    """
    return _DB_SINGLETON.insert_descriptor(run_start, data_keys,
                                           time, uid, **kwargs)


insert_event_descriptor = insert_descriptor


def insert_event(descriptor, time, seq_num, data, timestamps, uid):
    """Create an event in metadatastore database backend

    .. warning

       This does not validate that the keys in `data` and `timestamps`
       match the data keys in `descriptor`.

    Parameters
    ----------
    descriptor : doc.Document or dict or str
        The Descriptor to insert event for.  Can be either
        a Document/dict with a 'uid' key or a uid string
    time : float
        The date/time as found at the client side when an event is
        created.
    seq_num : int
        Unique sequence number for the event. Provides order of an event in
        the group of events
    data : dict
        Dictionary of measured values (or external references)
    timestamps : dict
        Dictionary of measured timestamps for each values, having the
        same keys as `data` above
    uid : str
        Globally unique id string provided to metadatastore
    """
    return _DB_SINGLETON.insert_event(descriptor, time, seq_num,
                                      data, timestamps, uid)


def bulk_insert_events(event_descriptor, events, validate=False):
    """Bulk insert many events

    Parameters
    ----------
    event_descriptor : doc.Document or dict or str
        The Descriptor to insert event for.  Can be either
        a Document/dict with a 'uid' key or a uid string
    events : iterable
       iterable of dicts matching the bs.Event schema
    validate : bool, optional
       If it should be checked that each pair of data/timestamps
       dicts has identical keys

    Returns
    -------
    ret : dict
        dictionary of details about the insertion
    """
    return _DB_SINGLETON.bulk_insert_events(descriptor=event_descriptor,
                                            events=events, validate=validate)


# DATABASE RETRIEVAL ##########################################################

def find_run_starts(**kwargs):
    """Given search criteria, locate RunStart Documents.

    Parameters
    ----------
    start_time : time-like, optional
        time-like representation of the earliest time that a RunStart
        was created. Valid options are:
           - timestamps --> time.time()
           - '2015'
           - '2015-01'
           - '2015-01-30'
           - '2015-03-30 03:00:00'
           - datetime.datetime.now()
    stop_time : time-like, optional
        timestamp of the latest time that a RunStart was created. See
        docs for `start_time` for examples.
    beamline_id : str, optional
        String identifier for a specific beamline
    project : str, optional
        Project name
    owner : str, optional
        The username of the logged-in user when the scan was performed
    scan_id : int, optional
        Integer scan identifier

    Returns
    -------
    rs_objects : iterable of doc.Document objects


    Examples
    --------
    >>> find_run_starts(scan_id=123)
    >>> find_run_starts(owner='arkilic')
    >>> find_run_starts(start_time=1421176750.514707, stop_time=time.time()})
    >>> find_run_starts(start_time=1421176750.514707, stop_time=time.time())

    >>> find_run_starts(owner='arkilic', start_time=1421176750.514707,
    ...                stop_time=time.time())

    """
    gen = _DB_SINGLETON.find_run_starts(**kwargs)

    for rs in gen:
        yield rs
find_runstarts = find_run_starts


def find_run_stops(run_start=None, **kwargs):
    """Given search criteria, locate RunStop Documents.

    Parameters
    ----------
    run_start : doc.Document or str, optional
        The RunStart document or uid to get the corresponding run end for
    start_time : time-like, optional
        time-like representation of the earliest time that a RunStop
        was created. Valid options are:
           - timestamps --> time.time()
           - '2015'
           - '2015-01'
           - '2015-01-30'
           - '2015-03-30 03:00:00'
           - datetime.datetime.now()
    stop_time : time-like, optional
        timestamp of the latest time that a RunStop was created. See
        docs for `start_time` for examples.
    exit_status : {'success', 'fail', 'abort'}, optional
        provides information regarding the run success.
    reason : str, optional
        Long-form description of why the run was terminated.
    uid : str, optional
        Globally unique id string provided to metadatastore

    Yields
    ------
    run_stop : doc.Document
        The requested RunStop documents
    """
    gen = _DB_SINGLETON.find_run_stops(run_start=run_start, **kwargs)
    for rs in gen:
        yield rs
find_runstops = find_run_stops


def find_descriptors(run_start=None, **kwargs):
    """Given search criteria, locate EventDescriptor Documents.

    Parameters
    ----------
    run_start : doc.Document or str, optional
        The RunStart document or uid to get the corresponding run end for
    start_time : time-like, optional
        time-like representation of the earliest time that an EventDescriptor
        was created. Valid options are:
           - timestamps --> time.time()
           - '2015'
           - '2015-01'
           - '2015-01-30'
           - '2015-03-30 03:00:00'
           - datetime.datetime.now()
    stop_time : time-like, optional
        timestamp of the latest time that an EventDescriptor was created. See
        docs for `start_time` for examples.
    uid : str, optional
        Globally unique id string provided to metadatastore

    Yields
    -------
    descriptor : doc.Document
        The requested EventDescriptor
    """
    for d in _DB_SINGLETON.find_descriptors(run_start=run_start, **kwargs):
        yield d


# TODO properly mark this as deprecated
find_event_descriptors = find_descriptors


def find_events(descriptor=None, **kwargs):
    """Given search criteria, locate Event Documents.

    Parameters
    -----------
    start_time : time-like, optional
        time-like representation of the earliest time that an Event
        was created. Valid options are:
           - timestamps --> time.time()
           - '2015'
           - '2015-01'
           - '2015-01-30'
           - '2015-03-30 03:00:00'
           - datetime.datetime.now()
    stop_time : time-like, optional
        timestamp of the latest time that an Event was created. See
        docs for `start_time` for examples.
    descriptor : doc.Document or str, optional
       Find events for a given EventDescriptor
    uid : str, optional
        Globally unique id string provided to metadatastore

    Returns
    -------
    events : iterable of doc.Document objects
    """
    gen = _DB_SINGLETON.find_events(descriptor=descriptor, **kwargs)

    for ev in gen:
        yield ev


def find_last(num=1):
    """Locate the last `num` RunStart Documents

    Parameters
    ----------
    num : integer, optional
        number of RunStart documents to return, default 1

    Yields
    ------
    run_start doc.Document
       The requested RunStart documents
    """
    for ev in _DB_SINGLETON.find_last(num):
        yield ev
