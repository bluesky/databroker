from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six
import warnings

import logging

import pymongo


import numpy as np

from ..utils import format_time as _format_time
from .core import (doc_or_uid_to_uid,   # noqa
                   NoRunStart, NoRunStop, NoEventDescriptors,
                   _cache_run_start, _cache_run_stop, _cache_descriptor,
                   run_start_given_uid, run_stop_given_uid,
                   descriptor_given_uid, stop_by_start, descriptors_by_start,
                   get_events_table, insert_run_start, insert_run_stop,
                   insert_descriptor, insert_event, BAD_KEYS_FMT)
from ..utils import sanitize_np, apply_to_dict_recursively
from event_model import MismatchedDataKeys

logger = logging.getLogger(__name__)


def get_events_generator(descriptor, event_col, descriptor_col,
                         descriptor_cache, run_start_col,
                         run_start_cache, convert_arrays=True):
    """A generator which yields all events from the event stream

    Parameters
    ----------
    descriptor : dict or str
        The EventDescriptor to get the Events for.  Can be either
        a dict with a 'uid' key or a uid string
    convert_arrays: boolean, optional
        convert 'array' type to numpy.ndarray; True by default

    Yields
    ------
    event : dict
        All events for the given EventDescriptor from oldest to
        newest
    """
    descriptor_uid = doc_or_uid_to_uid(descriptor)
    descriptor = descriptor_given_uid(descriptor_uid, descriptor_col,
                                      descriptor_cache)
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
        ev['descriptor'] = descriptor_uid
        for k, v in ev['data'].items():
            try:
                _dk = data_keys[k]
            except KeyError as err:
                raise MismatchedDataKeys(
                    "The documents are not valid.  Either because they "
                    "were recorded incorrectly in the first place, "
                    "corrupted since, or exercising a yet-undiscovered "
                    "bug in a reader. event['data'].keys() "
                    "must equal descriptor['data_keys'].keys(). "
                    f"event['data'].keys(): {ev['data'].keys()}, "
                    "descriptor['data_keys'].keys(): "
                    f"{descriptor['data_keys'].keys()}") from err
            # convert any arrays stored directly in mds into ndarray
            if convert_arrays:
                if _dk['dtype'] == 'array' and not _dk.get('external', False):
                    ev['data'][k] = np.asarray(ev['data'][k])

        # note which keys refer to dereferences (external) data
        ev['filled'] = {k: False for k in external_keys}

        yield ev


# database INSERTION ###################################################

def bulk_insert_events(event_col, descriptor, events, validate):
    """Bulk insert many events

    Parameters
    ----------
    event_descriptor : dict or str
        The Descriptor to insert event for.  Can be either
        a dict with a 'uid' key or a uid string
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
            data = dict(ev['data'])
            # Replace any filled data with the datum_id stashed in 'filled'.
            for k, v in six.iteritems(ev.get('filled', {})):
                if v:
                    data[k] = v
            # Convert any numpy types to native Python types.
            apply_to_dict_recursively(data, sanitize_np)
            timestamps = dict(ev['timestamps'])
            apply_to_dict_recursively(timestamps, sanitize_np)
            # check keys, this could be expensive
            if validate:
                if data.keys() != timestamps.keys():
                    raise ValueError(
                        BAD_KEYS_FMT.format(data.keys(),
                                            timestamps.keys()))

            ev_out = dict(descriptor=descriptor_uid, uid=ev['uid'],
                          data=data, timestamps=timestamps,
                          time=ev['time'],
                          seq_num=ev['seq_num'])
            yield ev_out

    bulk = [pymongo.InsertOne(ev) for ev in event_factory()]
    return event_col.bulk_write(bulk, ordered=True)

# DATABASE RETRIEVAL ##########################################################


def find_run_starts(run_start_col, run_start_cache, tz, **kwargs):
    """Given search criteria, locate RunStart Documents.

    Parameters
    ----------
    since : time-like, optional
        time-like representation of the earliest time that a RunStart
        was created. Valid options are:
           - timestamps --> time.time()
           - '2015'
           - '2015-01'
           - '2015-01-30'
           - '2015-03-30 03:00:00'
           - datetime.datetime.now()
    until : time-like, optional
        timestamp of the latest time that a RunStart was created. See
        docs for `since` for examples.
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
    rs_objects : iterable of dicts


    Examples
    --------
    >>> find_run_starts(scan_id=123)
    >>> find_run_starts(owner='arkilic')
    >>> find_run_starts(since=1421176750.514707, until=time.time()})
    >>> find_run_starts(since=1421176750.514707, until=time.time())

    >>> find_run_starts(owner='arkilic', since=1421176750.514707,
    ...                until=time.time())

    """
    # now try rest of formatting
    _format_time(kwargs, tz)
    rs_objects = run_start_col.find(kwargs,
                                    sort=[('time', pymongo.DESCENDING)])

    for rs in rs_objects:
        yield _cache_run_start(rs, run_start_cache)


def find_run_stops(stop_col, stop_cache, tz,
                   run_start=None, **kwargs):
    """Given search criteria, locate RunStop Documents.

    Parameters
    ----------
    run_start : dict or str, optional
        The RunStart document or uid to get the corresponding run end for
    since : time-like, optional
        time-like representation of the earliest time that a RunStop
        was created. Valid options are:
           - timestamps --> time.time()
           - '2015'
           - '2015-01'
           - '2015-01-30'
           - '2015-03-30 03:00:00'
           - datetime.datetime.now()
    until : time-like, optional
        timestamp of the latest time that a RunStop was created. See
        docs for `since` for examples.
    exit_status : {'success', 'fail', 'abort'}, optional
        provides information regarding the run success.
    reason : str, optional
        Long-form description of why the run was terminated.
    uid : str, optional
        Globally unique id string provided to metadatastore

    Yields
    ------
    run_stop : dict
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
        yield _cache_run_stop(rs, stop_cache)


def find_descriptors(descriptor_col, descriptor_cache,
                     tz,
                     run_start=None, **kwargs):
    """Given search criteria, locate EventDescriptor Documents.

    Parameters
    ----------
    run_start : dict or str, optional
        The RunStart document or uid to get the corresponding run end for
    since : time-like, optional
        time-like representation of the earliest time that an EventDescriptor
        was created. Valid options are:
           - timestamps --> time.time()
           - '2015'
           - '2015-01'
           - '2015-01-30'
           - '2015-03-30 03:00:00'
           - datetime.datetime.now()
    until : time-like, optional
        timestamp of the latest time that an EventDescriptor was created. See
        docs for `since` for examples.
    uid : str, optional
        Globally unique id string provided to metadatastore

    Yields
    -------
    descriptor : dict
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
        yield _cache_descriptor(event_descriptor, descriptor_cache)



def find_events(start_col, start_cache,
                descriptor_col, descriptor_cache,
                event_col, tz, descriptor=None, **kwargs):
    """Given search criteria, locate Event Documents.

    Parameters
    -----------
    since : time-like, optional
        time-like representation of the earliest time that an Event
        was created. Valid options are:
           - timestamps --> time.time()
           - '2015'
           - '2015-01'
           - '2015-01-30'
           - '2015-03-30 03:00:00'
           - datetime.datetime.now()
    until : time-like, optional
        timestamp of the latest time that an Event was created. See
        docs for `since` for examples.
    descriptor : dict or str, optional
       Find events for a given EventDescriptor
    uid : str, optional
        Globally unique id string provided to metadatastore

    Returns
    -------
    events : iterable of dict objects
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
                                                    descriptor_cache)

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
    run_start : dict
       The requested RunStart documents
    """
    col = start_col
    for rs in col.find().sort('time', pymongo.DESCENDING).limit(num):
        yield _cache_run_start(rs, start_cache)
