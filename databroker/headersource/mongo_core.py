from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six
import warnings

import logging

import pymongo


import numpy as np

import doct as doc
from ..core import format_time as _format_time
from .core import (doc_or_uid_to_uid,   # noqa
                   NoRunStart, NoRunStop, NoEventDescriptors,
                   _cache_run_start, _cache_run_stop, _cache_descriptor,
                   run_start_given_uid, run_stop_given_uid,
                   descriptor_given_uid, stop_by_start, descriptors_by_start,
                   get_events_table)

logger = logging.getLogger(__name__)


def get_events_generator(descriptor, event_col, descriptor_col,
                         descriptor_cache, run_start_col,
                         run_start_cache, convert_arrays=True):
    """A generator which yields all events from the event stream

    Parameters
    ----------
    descriptor : doc.Document or dict or str
        The EventDescriptor to get the Events for.  Can be either
        a Document/dict with a 'uid' key or a uid string
    convert_arrays: boolean, optional
        convert 'array' type to numpy.ndarray; True by default

    Yields
    ------
    event : doc.Document
        All events for the given EventDescriptor from oldest to
        newest
    """
    descriptor_uid = doc_or_uid_to_uid(descriptor)
    descriptor = descriptor_given_uid(descriptor_uid, descriptor_col,
                                      descriptor_cache, run_start_col,
                                      run_start_cache)
    col = event_col
    ev_cur = col.find({'descriptor': descriptor_uid},
                      sort=[('descriptor', pymongo.DESCENDING),
                            ('time', pymongo.ASCENDING)])

    data_keys = descriptor['data_keys']
    external_keys = [k for k in data_keys if 'external' in data_keys[k]]
    for ev in ev_cur:
        # ditch the ObjectID
        del ev['_id']

        # replace descriptor with the defererenced descriptor
        ev['descriptor'] = descriptor
        for k, v in ev['data'].items():
            _dk = data_keys[k]
            # convert any arrays stored directly in mds into ndarray
            if convert_arrays:
                if _dk['dtype'] == 'array' and not _dk.get('external', False):
                    ev['data'][k] = np.asarray(ev['data'][k])

        # note which keys refer to dereferences (external) data
        ev['filled'] = {k: False for k in external_keys}

        # wrap it in our fancy dict
        ev = doc.Document('Event', ev)

        yield ev


# database INSERTION ###################################################

def insert_run_start(run_start_col, run_start_cache,
                     time, uid, **kwargs):
    """Insert a RunStart document into the database.

    Parameters
    ----------
    time : float
        The date/time as found at the client side when the run is started
    uid : str
        Globally unique id string provided to metadatastore
    scan_id : int, optional
        Scan identifier visible to the user and data analysis.  This is not
        a unique identifier.
    owner : str, optional
        A username associated with the RunStart
    group : str, optional
        An experimental group associated with the RunStart
    project : str, optional
        Any project name to help users locate the data
    sample : str or dict, optional
    kwargs
        additional optional or custom fields

    Returns
    -------
    run_start : str
        uid of the inserted document.  Use `run_start_given_uid` to get
        the full document.

    """
    if 'custom' in kwargs:
        warnings.warn("custom kwarg is deprecated")
        custom = kwargs.pop('custom')
        if any(k in kwargs for k in custom):
            raise TypeError("duplicate keys in kwargs and custom")
        kwargs.update(custom)

    col = run_start_col
    run_start = dict(time=time, uid=uid, **kwargs)

    col.insert_one(run_start)

    _cache_run_start(run_start, run_start_cache)
    logger.debug('Inserted RunStart with uid %s', run_start['uid'])

    return uid


def insert_run_stop(run_start_col, run_start_cache,
                    run_stop_col, run_stop_cache,
                    run_start, time, uid, exit_status, reason=None,
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
    if 'custom' in kwargs:
        warnings.warn("custom kwarg is deprecated")
        custom = kwargs.pop('custom')
        if any(k in kwargs for k in custom):
            raise TypeError("duplicate keys in kwargs and custom")
        kwargs.update(custom)

    run_start_uid = doc_or_uid_to_uid(run_start)
    run_start = run_start_given_uid(run_start_uid, run_start_col,
                                    run_start_cache)
    try:
        stop_by_start(run_start_uid,
                      run_stop_col, run_stop_cache,
                      run_start_col, run_start_cache)
    except NoRunStop:
        pass
    else:
        raise RuntimeError("Runstop already exits for {!r}".format(run_start))

    col = run_stop_col
    run_stop = dict(run_start=run_start_uid, time=time, uid=uid,
                    exit_status=exit_status, **kwargs)
    if reason is not None and reason != '':
        run_stop['reason'] = reason

    col.insert_one(run_stop)
    _cache_run_stop(run_stop, run_stop_cache, run_start_col, run_start_cache)
    logger.debug("Inserted RunStop with uid %s referencing RunStart "
                 " with uid %s", run_stop['uid'], run_start['uid'])

    return uid


def insert_descriptor(run_start_col, run_start_cache, descriptor_col,
                      descriptor_cache, run_start, data_keys, time, uid,
                      **kwargs):
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
    if 'custom' in kwargs:
        warnings.warn("custom kwarg is deprecated")
        custom = kwargs.pop('custom')
        if any(k in kwargs for k in custom):
            raise TypeError("duplicate keys in kwargs and custom")
        kwargs.update(custom)

    for k in data_keys:
        if '.' in k:
            raise ValueError("Key names can not contain '.' (period).")

    data_keys = {k: dict(v) for k, v in data_keys.items()}
    run_start_uid = doc_or_uid_to_uid(run_start)

    col = descriptor_col

    descriptor = dict(run_start=run_start_uid, data_keys=data_keys,
                      time=time, uid=uid, **kwargs)
    # TODO validation
    col.insert_one(descriptor)

    descriptor = _cache_descriptor(descriptor, descriptor_cache,
                                   run_start_col, run_start_cache)

    logger.debug("Inserted EventDescriptor with uid %s referencing "
                 "RunStart with uid %s", descriptor['uid'], run_start_uid)

    return uid
insert_event_descriptor = insert_descriptor


def insert_event(event_col, descriptor, time, seq_num, data, timestamps, uid,
                 validate):
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
    if validate:
        raise NotImplementedError("insert event validation not written yet")
    # convert data to storage format
    # make sure we really have a uid
    descriptor_uid = doc_or_uid_to_uid(descriptor)

    col = event_col

    event = dict(descriptor=descriptor_uid, uid=uid,
                 data=data, timestamps=timestamps, time=time,
                 seq_num=seq_num)

    col.insert_one(event)

    logger.debug("Inserted Event with uid %s referencing "
                 "EventDescriptor with uid %s", event['uid'],
                 descriptor_uid)
    return uid

BAD_KEYS_FMT = """Event documents are malformed, the keys on 'data' and
'timestamps do not match:\n data: {}\ntimestamps:{}"""


def bulk_insert_events(event_col, descriptor, events, validate):
    """Bulk insert many events

    Parameters
    ----------
    event_descriptor : doc.Document or dict or str
        The Descriptor to insert event for.  Can be either
        a Document/dict with a 'uid' key or a uid string
    events : iterable
       iterable of dicts matching the bs.Event schema
    validate : bool
       If it should be checked that each pair of data/timestamps
       dicts has identical keys

    Returns
    -------
    ret : dict
        dictionary of details about the insertion
    """
    descriptor_uid = doc_or_uid_to_uid(descriptor)

    def event_factory():
        for ev in events:
            # check keys, this could be expensive
            if validate:
                if ev['data'].keys() != ev['timestamps'].keys():
                    raise ValueError(
                        BAD_KEYS_FMT.format(ev['data'].keys(),
                                            ev['timestamps'].keys()))

            ev_out = dict(descriptor=descriptor_uid, uid=ev['uid'],
                          data=ev['data'], timestamps=ev['timestamps'],
                          time=ev['time'],
                          seq_num=ev['seq_num'])
            yield ev_out

    bulk = event_col.initialize_ordered_bulk_op()
    for ev in event_factory():
        bulk.insert(ev)

    return bulk.execute()


def _transform_data(data, timestamps):
    """
    Transform from Document spec:
        {'data': {'key': <value>},
         'timestamps': {'key': <timestamp>}}
    to storage format:
        {'data': {<key>: (<value>, <timestamp>)}.
    """
    return {k: (data[k], timestamps[k]) for k in data}


# DATABASE RETRIEVAL ##########################################################


def find_run_starts(run_start_col, run_start_cache, tz, **kwargs):
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
    # now try rest of formatting
    _format_time(kwargs, tz)
    rs_objects = run_start_col.find(kwargs,
                                    sort=[('time', pymongo.DESCENDING)])

    for rs in rs_objects:
        yield _cache_run_start(rs, run_start_cache)


def find_run_stops(start_col, start_cache,
                   stop_col, stop_cache, tz,
                   run_start=None, **kwargs):
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
    # if trying to find by run_start, there can be only one
    # normalize the input and get the run_start oid
    if run_start:
        run_start_uid = doc_or_uid_to_uid(run_start)
        kwargs['run_start'] = run_start_uid

    _format_time(kwargs, tz)
    col = stop_col
    run_stop = col.find(kwargs, sort=[('time', pymongo.ASCENDING)])

    for rs in run_stop:
        yield _cache_run_stop(rs, stop_cache, start_col, start_cache)


def find_descriptors(start_col, start_cache,
                     descriptor_col, descriptor_cache,
                     tz,
                     run_start=None, **kwargs):
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
    if run_start:
        run_start_uid = doc_or_uid_to_uid(run_start)
        kwargs['run_start'] = run_start_uid

    _format_time(kwargs, tz)

    col = descriptor_col
    event_descriptor_objects = col.find(kwargs,
                                        sort=[('time', pymongo.ASCENDING)])

    for event_descriptor in event_descriptor_objects:
        yield _cache_descriptor(event_descriptor, descriptor_cache,
                                start_col, start_cache)


def find_events(start_col, start_cache,
                descriptor_col, descriptor_cache,
                event_col, tz, descriptor=None, **kwargs):
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
    # Some user-friendly error messages for an easy mistake to make
    if 'event_descriptor' in kwargs:
        raise ValueError("Use 'descriptor', not 'event_descriptor'.")

    if descriptor:
        descriptor_uid = doc_or_uid_to_uid(descriptor)
        kwargs['descriptor'] = descriptor_uid

    _format_time(kwargs, tz)
    col = event_col
    events = col.find(kwargs,
                      sort=[('descriptor', pymongo.DESCENDING),
                            ('time', pymongo.ASCENDING)],
                      no_cursor_timeout=True)

    try:
        for ev in events:
            ev.pop('_id', None)
            # pop the descriptor oid
            desc_uid = ev.pop('descriptor')
            # replace it with the defererenced descriptor
            ev['descriptor'] = descriptor_given_uid(desc_uid, descriptor_col,
                                                    descriptor_cache,
                                                    start_col, start_cache)

            # wrap it our fancy dict
            ev = doc.Document('Event', ev)
            yield ev
    finally:
        events.close()


def find_last(start_col, start_cache, num):
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
    col = start_col
    for rs in col.find().sort('time', pymongo.DESCENDING).limit(num):
        yield _cache_run_start(rs, start_cache)
