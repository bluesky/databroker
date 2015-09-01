from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six
from functools import wraps
from itertools import count

import datetime
import logging

import boltons.cacheutils
import pytz

import pymongo

import bson
from bson import ObjectId
from bson.dbref import DBRef

from mongoengine import connect
import mongoengine.connection

from . import conf
from .odm_templates import (RunStart, RunStop,
                            EventDescriptor, Event, DataKey, ALIAS)
from . import doc


logger = logging.getLogger(__name__)


__all__ = ['insert_run_start', 'insert_event',
           'insert_run_stop', 'insert_event_descriptor', 'find_run_stops',
           'find_event_descriptors', 'find_last',
           'find_events', 'find_run_starts', 'db_connect', 'db_disconnect',
           'reorganize_event']

# process local caches of 'header' documents these are storing object indexed
# on Objected because that is what the reference fields in mongo are
# implemented as.   Should move to uids asap
_RUNSTART_CACHE_OID = boltons.cacheutils.LRU(max_size=1000)
_RUNSTOP_CACHE_OID = boltons.cacheutils.LRU(max_size=1000)
_EVENTDESC_CACHE_OID = boltons.cacheutils.LRU(max_size=1000)

# never drop these
_RUNSTART_UID_to_OID_MAP = dict()
_RUNSTOP_UID_to_OID_MAP = dict()
_EVENTDESC_UID_to_OID_MAP = dict()


def _ensure_connection(func):
    @wraps(func)
    def inner(*args, **kwargs):
        database = conf.connection_config['database']
        host = conf.connection_config['host']
        port = int(conf.connection_config['port'])
        db_connect(database=database, host=host, port=port)
        return func(*args, **kwargs)
    return inner


def _cache_runstart(rs):
    """De-reference and cache a RunStart document

    """
    # TODO actually do this de-reference
    rs.pop('beamline_config_id', None)
    oid = rs.pop('_id')
    rs = doc.Document('RunStart', rs)

    # populate both caches
    _RUNSTART_CACHE_OID[oid] = rs
    _RUNSTART_UID_to_OID_MAP[rs['uid']] = oid
    return rs


def _cache_runstop(runstop):
    oid = runstop.pop('_id')
    # do the run-start de-reference
    start_oid = runstop.pop('run_start_id')
    runstop['run_start'] = _runstart_given_oid(start_oid)
    runstop = doc.Document('RunStop', runstop)

    # update the two caches
    _RUNSTOP_CACHE_OID[oid] = runstop
    _RUNSTOP_UID_to_OID_MAP[runstop['uid']] = oid
    return runstop


def _cache_eventdescriptor(ev_desc):
    oid = ev_desc.pop('_id')
    # do the runstart referencing
    start_oid = ev_desc.pop('run_start_id')
    ev_desc['run_start'] = _runstart_given_oid(start_oid)

    ev_desc = doc.Document('EventDescriptor', ev_desc)

    # update both caches
    _EVENTDESC_CACHE_OID[oid] = ev_desc
    _EVENTDESC_UID_to_OID_MAP[ev_desc['uid']] = oid
    return ev_desc


@_ensure_connection
def _runstart_given_oid(oid):
    try:
        return _RUNSTART_CACHE_OID[oid]
    except KeyError:
        pass
    rs = RunStart.objects.as_pymongo().get(id=oid)
    return _cache_runstart(rs)


@_ensure_connection
def runstart_given_uid(uid):
    try:
        oid = _RUNSTART_UID_to_OID_MAP[uid]
        return _RUNSTART_CACHE_OID[oid]
    except KeyError:
        pass
    rs = RunStart.objects.as_pymongo().get(uid=uid)
    return _cache_runstart(rs)


@_ensure_connection
def _runstop_given_oid(oid):
    try:
        return _RUNSTOP_CACHE_OID[oid]
    except KeyError:
        pass
    # get the raw runstop
    runstop = RunStop.objects.as_pymongo().get(id=oid)
    # pop off the oid
    return _cache_runstop(runstop)


@_ensure_connection
def runstop_given_uid(uid):
    try:
        oid = _RUNSTOP_UID_to_OID_MAP[uid]
        return _RUNSTOP_CACHE_OID[oid]
    except KeyError:
        pass
    # get the raw runstop
    runstop = RunStop.objects.as_pymongo().get(uid=uid)
    return _cache_runstop(runstop)


@_ensure_connection
def _event_desc_given_oid(oid):
    try:
        return _EVENTDESC_CACHE_OID[oid]
    except KeyError:
        pass

    ev_desc = EventDescriptor.objects.as_pymongo().get(id=oid)
    return _cache_eventdescriptor(ev_desc)


@_ensure_connection
def event_desc_given_uid(uid):
    try:
        oid = _EVENTDESC_UID_to_OID_MAP[uid]
        return _EVENTDESC_CACHE_OID[oid]
    except KeyError:
        pass

    ev_desc = EventDescriptor.objects.as_pymongo().get(uid=uid)
    return _cache_eventdescriptor(ev_desc)


def get_runstart(input_id):
    """Get a single runstart document given a uid or oid

    Parameters
    ----------
    input_id : str or ObjectId
        Unique identified for the runstart to find.  ObjectId
        search will be deprecated in the future.

    Returns
    -------
    rs : doc.Document
        Dottable dictionary of RunStart document
    """
    if isinstance(input_id, bson.ObjectId):
        return _runstart_given_oid(input_id)
    elif isinstance(input_id, six.string_types):
        return runstart_given_uid(input_id)
    else:
        raise TypeError("Input must be ObjectId or str")


def get_runstop(input_id):
    pass


def get_runstop_by_runstart(runstart_id):
    pass


def get_eventdescriptor(input_id):
    pass


def get_eventdescriptor_by_runstart(runstart_id):
    pass


def fetch_events_generator(desc_uid):
    col = Event._get_collection()

    desc = event_desc_given_uid(desc_uid)
    eid = _EVENTDESC_UID_to_OID_MAP[desc_uid]

    ev_cur = col.find({'descriptor_id': eid}).sort(
                            [('time', pymongo.DESCENDING)])

    for ev in ev_cur:
        # ditch the ObjectID
        ev.pop('_id')
        # pop the descriptor oid
        ev.pop('descriptor_id')
        # replace it with the defererenced descriptor
        ev['descriptor'] = desc
        # pop the data
        data = ev.pop('data')
        # replace it with the friendly paired dicts
        ev['data'], ev['timestamps'] = [{k: v[j] for k, v in data.items()}
                                        for j in range(2)]
        # wrap it our fancy dict
        ev = doc.Document('Event', ev)

        yield ev


def db_disconnect():
    """Helper function to deal with stateful connections to mongoengine"""
    mongoengine.connection.disconnect(ALIAS)
    for collection in [RunStart, RunStop, EventDescriptor,
                       Event, DataKey]:
        collection._collection = None


def db_connect(database, host, port):
    """Helper function to deal with stateful connections to mongoengine"""
    return connect(db=database, host=host, port=port, alias=ALIAS)


def format_data_keys(data_key_dict):
    """Helper function that allows ophyd to send info about its data keys
    to metadatastore and have metadatastore format them into whatever the
    current spec dictates. This functions formats the data key info for
    the event descriptor

    Parameters
    ----------
    data_key_dict : dict
        The format that ophyd is sending to metadatastore
        {'data_key1': {
            'dtype': dtype_value,
            'source': source_value,
            'shape': shape_value},
         'data_key2': {...}
        }

    Returns
    -------
    formatted_dict : dict
        Data key info for the event descriptor that is formatted for the
        current metadatastore spec.The current metadatastore spec is:
        {'data_key1':
         'data_key2': mds.odm_templates.DataKeys
        }
    """
    data_key_dict = {key_name: (
                     DataKey(**data_key_description) if
                     not isinstance(data_key_description, DataKey) else
                     data_key_description)
                     for key_name, data_key_description
                     in six.iteritems(data_key_dict)}
    return data_key_dict


# database INSERTION ###################################################

@_ensure_connection
def insert_run_start(time, scan_id, beamline_id, uid,
                     owner=None, group=None, project=None, custom=None):
    """Provide a head for a sequence of events. Entry point for an
    experiment's run.

    Parameters
    ----------
    time : float
        The date/time as found at the client side when an event is
        created.
    scan_id : int
        Unique scan identifier visible to the user and data analysis
    beamline_id : str
        Beamline String identifier. Not unique, just an indicator of
        beamline code for multiple beamline systems
    uid : str
        Globally unique id string provided to metadatastore
    owner : str, optional
        A username associated with the entry
    group : str, optional
        A group (e.g., UNIX group) associated with the entry
    project : str, optional
        Any project name to help users locate the data
    custom: dict, optional
        Any additional information that data acquisition code/user wants
        to append to the Header at the start of the run.

    Returns
    -------
    run_start: mongoengine.Document
        Inserted mongoengine object

    """
    if custom is None:
        custom = {}
    if owner is None:
        owner = ''
    if group is None:
        group = ''
    if project is None:
        project = ''

    run_start = RunStart(time=time, scan_id=scan_id,
                         uid=uid,
                         beamline_id=beamline_id,
                         owner=owner, group=group, project=project,
                         **custom)

    run_start.save(validate=True, write_concern={"w": 1})
    logger.debug('Inserted RunStart with uid %s', run_start.uid)

    return uid


@_ensure_connection
def insert_run_stop(run_start, time, uid, exit_status='success',
                    reason=None, custom=None):
    """ Provide an end to a sequence of events. Exit point for an
    experiment's run.

    Parameters
    ----------
    run_start : metadatastore.documents.Document or str
        if Document:
            The metadatastore RunStart document
        if str:
            uid of RunStart object to associate with this record
    time : float
        The date/time as found at the client side when an event is
        created.
    uid : str
        Globally unique id string provided to metadatastore
    exit_status : {'success', 'abort', 'fail'}, optional
        indicating reason run stopped, 'success' by default
    reason : str, optional
        more detailed exit status (stack trace, user remark, etc.)
    custom : dict, optional
        Any additional information that data acquisition code/user wants
        to append to the Header at the end of the run.

    Returns
    -------
    run_stop : mongoengine.Document
        Inserted mongoengine object
    """
    if custom is None:
        custom = {}

    run_start = runstart_given_uid(run_start)
    runstart_oid = _RUNSTART_UID_to_OID_MAP[run_start['uid']]
    rs_ref = DBRef('RunStart', runstart_oid)

    run_stop = RunStop(run_start=rs_ref, reason=reason, time=time,
                       uid=uid,
                       exit_status=exit_status, **custom)

    run_stop.save(validate=True, write_concern={"w": 1})
    logger.debug("Inserted RunStop with uid %s referencing RunStart "
                 " with uid %s", run_stop.uid, run_start.uid)

    return uid


@_ensure_connection
def insert_event_descriptor(run_start, data_keys, time, uid,
                            custom=None):
    """ Create an event_descriptor in metadatastore database backend

    Parameters
    ----------
    run_start : metadatastore.documents.Document or str
        if Document:
            The metadatastore RunStart document
        if str:
            uid of RunStart object to associate with this record
    data_keys : dict
        Provides information about keys of the data dictionary in
        an event will contain
    time : float
        The date/time as found at the client side when an event
        descriptor is created.
    uid : str
        Globally unique id string provided to metadatastore
    custom : dict, optional
        Any additional information that data acquisition code/user wants
        to append to the EventDescriptor.

    Returns
    -------
    ev_desc : EventDescriptor
        The document added to the collection.

    """
    if custom is None:
        custom = {}
    data_keys = format_data_keys(data_keys)

    run_start = runstart_given_uid(run_start)
    runstart_oid = _RUNSTART_UID_to_OID_MAP[run_start['uid']]
    rs_ref = DBRef('RunStart', runstart_oid)
    event_descriptor = EventDescriptor(run_start=rs_ref,
                                       data_keys=data_keys, time=time,
                                       uid=uid,
                                       **custom)

    event_descriptor.save(validate=True, write_concern={"w": 1})
    logger.debug("Inserted EventDescriptor with uid %s referencing "
                 "RunStart with uid %s", event_descriptor.uid, run_start.uid)

    return uid


@_ensure_connection
def insert_event(descriptor, time, seq_num, data, timestamps, uid):
    """Create an event in metadatastore database backend

    Parameters
    ----------
    descriptor : metadatastore.documents.Document or str
        if Document:
            The metadatastore EventDescriptor document
        if str:
            uid of EventDescriptor object to associate with this record
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

    Note
    ----
    In the immediate aftermath of a change to how Documents are exchanged
    (not stored, but exchanged) timestamps is optional. If not included,
    data is expected to include (value, timestamp) tuples.
    """
    val_ts_tuple = _transform_data(data, timestamps)

    # Allow caller to beg forgiveness rather than ask permission w.r.t
    # EventDescriptor creation.
    if descriptor is None:
        raise EventDescriptorIsNoneError()
    descriptor = event_desc_given_uid(descriptor)
    desc_oid = _EVENTDESC_UID_to_OID_MAP[descriptor['uid']]
    event = Event(descriptor_id=desc_oid, uid=uid,
                  data=val_ts_tuple, time=time, seq_num=seq_num)

    event.save(validate=True, write_concern={"w": 1})

    logger.debug("Inserted Event with uid %s referencing "
                 "EventDescriptor with uid %s", event.uid,
                 descriptor.uid)
    return uid

BAD_KEYS_FMT = """Event documents are malformed, the keys on 'data' and
'timestamps do not match:\n data: {}\ntimestamps:{}"""


@_ensure_connection
def bulk_insert_events(event_descriptor, events, validate=False):
    """Bulk insert many events

    Parameters
    ----------
    event_descriptor : str
        The event descriptor uid that these events are associated with

    events : iterable
       iterable of dicts matching the bs.Event schema

    """

    descriptor = event_desc_given_uid(event_descriptor)
    desc_oid = _EVENTDESC_UID_to_OID_MAP[descriptor['uid']]

    def event_factory():
        for ev in events:
            # check keys, this could be expensive
            if validate:
                if ev['data'].keys() != ev['timestamps'].keys():
                    raise ValueError(
                        BAD_KEYS_FMT.format(ev['data'].keys(),
                                            ev['timestamps'].keys()))

            # transform the data to the storage format
            val_ts_tuple = _transform_data(ev['data'], ev['timestamps'])
            # create the mongoengine object, do this by hand if it goes
            # faster
            ev_out = dict(descriptor_id=desc_oid, uid=ev['uid'],
                          data=val_ts_tuple, time=ev['time'],
                          seq_num=ev['seq_num'])
            yield ev_out

    bulk = Event._get_collection().initialize_ordered_bulk_op()
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


class EventDescriptorIsNoneError(ValueError):
    """Special error that ophyd looks for when it passes a `None` event
    descriptor. Ophyd will then create an event descriptor and create the event
    again
    """
    pass


# DATABASE RETRIEVAL ##########################################################

def _format_time(search_dict):
    """Helper function to format the time arguments in a search dict

    Expects 'start_time' and 'stop_time'

    ..warning: Does in-place mutation of the search_dict
    """
    time_dict = {}
    start_time = search_dict.pop('start_time', None)
    stop_time = search_dict.pop('stop_time', None)
    if start_time:
        time_dict['$gte'] = _normalize_human_friendly_time(start_time)
    if stop_time:
        time_dict['$lte'] = _normalize_human_friendly_time(stop_time)
    if time_dict:
        search_dict['time'] = time_dict


# human friendly timestamp formats we'll parse
_TS_FORMATS = [
    '%Y-%m-%d %H:%M:%S',
    '%Y-%m-%d %H:%M',  # these 2 are not as originally doc'd,
    '%Y-%m-%d %H',     # but match previous pandas behavior
    '%Y-%m-%d',
    '%Y-%m',
    '%Y']

# build a tab indented, '-' bulleted list of supported formats
# to append to the parsing function docstring below
_doc_ts_formats = '\n'.join('\t- {}'.format(_) for _ in _TS_FORMATS)


def _normalize_human_friendly_time(val):
    """Given one of :
    - string (in one of the formats below)
    - datetime (eg. datetime.datetime.now()), with or without tzinfo)
    - timestamp (eg. time.time())
    return a timestamp (seconds since jan 1 1970 UTC).

    Non string/datetime.datetime values are returned unaltered.
    Leading/trailing whitespace is stripped.
    Supported formats:
    {}
    """
    # {} is placeholder for formats; filled in after def...

    tz = conf.connection_config['timezone']  # e.g., 'US/Eastern'
    zone = pytz.timezone(tz)  # tz as datetime.tzinfo object
    epoch = pytz.UTC.localize(datetime.datetime(1970, 1, 1))
    check = True

    if isinstance(val, six.string_types):
        # unix 'date' cmd format '%a %b %d %H:%M:%S %Z %Y' works but
        # doesn't get TZ?

        # Could cleanup input a bit? remove leading/trailing [ :,-]?
        # Yes, leading/trailing whitespace to match pandas behavior...
        # Actually, pandas doesn't ignore trailing space, it assumes
        # the *current* month/day if they're missing and there's
        # trailing space, or the month is a single, non zero-padded digit.?!
        val = val.strip()

        for fmt in _TS_FORMATS:
            try:
                ts = datetime.datetime.strptime(val, fmt)
                break
            except ValueError:
                pass

        try:
            if isinstance(ts, datetime.datetime):
                val = ts
                check = False
            else:
                raise TypeError('expected datetime.datetime,'
                                ' got {:r}'.format(ts))

        except NameError:
            raise ValueError('failed to parse time: ' + repr(val))

    if check and not isinstance(val, datetime.datetime):
        return val

    if val.tzinfo is None:
        # is_dst=None raises NonExistent and Ambiguous TimeErrors
        # when appropriate, same as pandas
        val = zone.localize(val, is_dst=None)

    return (val - epoch).total_seconds()


# fill in the placeholder we left in the previous docstring
_normalize_human_friendly_time.__doc__ = (
    _normalize_human_friendly_time.__doc__.format(_doc_ts_formats)
    )


def _normalize_object_id(kwargs, key):
    """Ensure that an id is an ObjectId, not a string.

    ..warning: Does in-place mutation of the search_dict
    """
    try:
        kwargs[key] = ObjectId(kwargs[key])
    except KeyError:
        # This key wasn't used by the query; that's fine.
        pass
    except TypeError:
        # This key was given a more complex query.
        pass
    # Database errors will still raise.


@_ensure_connection
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
    uid : str, optional
        Globally unique id string provided to metadatastore
    _id : str or ObjectId, optional
        The unique id generated by mongo

    Returns
    -------
    rs_objects : iterable of metadatastore.document.Document objects

    Note
    ----
    All documents that the RunStart Document points to are dereferenced.
    These include RunStop, and Sample.

    Examples
    --------
    >>> find_run_starts(scan_id=123)
    >>> find_run_starts(owner='arkilic')
    >>> find_run_starts(start_time=1421176750.514707, stop_time=time.time()})
    >>> find_run_starts(start_time=1421176750.514707, stop_time=time.time())

    >>> find_run_starts(owner='arkilic', start_time=1421176750.514707,
    ...                stop_time=time.time())

    """
    _normalize_object_id(kwargs, '_id')
    _format_time(kwargs)

    rs_objects = RunStart.objects(__raw__=kwargs).as_pymongo()

    return (_cache_runstart(rs) for rs in rs_objects.order_by('-time'))


@_ensure_connection
def find_run_stops(run_start=None, **kwargs):
    """Given search criteria, locate RunStop Documents.

    Parameters
    ----------
    run_start : metadatastore.document.Document or str, optional
        The metadatastore run start document or the metadatastore uid to get
        the corresponding run end for
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
    _id : str or ObjectId, optional
        The unique id generated by mongo

    Returns
    -------
    run_stop : iterable of metadatastore.document.Document objects
    """
    _format_time(kwargs)
    # get the actual mongo document
    if run_start:
        run_start = runstart_given_uid(run_start)
        run_start = _RUNSTART_UID_to_OID_MAP[run_start['uid']]
        kwargs['run_start_id'] = run_start.id

    _normalize_object_id(kwargs, '_id')
    _normalize_object_id(kwargs, 'run_start_id')
    run_stop = RunStop.objects(__raw__=kwargs).as_pymongo()

    return (_cache_runstop(rs) for rs in run_stop.order_by('-time'))


@_ensure_connection
def find_event_descriptors(run_start=None, **kwargs):
    """Given search criteria, locate EventDescriptor Documents.

    Parameters
    ----------
    run_start : metadatastore.document.Document or uid, optional
        if ``Document``:
            The metadatastore run start document or the metadatastore uid to get
            the corresponding run end for
        if ``str``:
            Globally unique id string provided to metadatastore for the
            RunStart Document.
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
    _id : str or ObjectId, optional
        The unique id generated by mongo

    Returns
    -------
    event_descriptor : iterable of metadatastore.document.Document objects
    """
    _format_time(kwargs)

    _normalize_object_id(kwargs, '_id')
    _normalize_object_id(kwargs, 'run_start_id')
    event_descriptor_objects = EventDescriptor.objects(__raw__=kwargs)

    event_descriptor_objects = event_descriptor_objects.as_pymongo()
    for event_descriptor in event_descriptor_objects.order_by('-time'):
        yield _cache_eventdescriptor(event_descriptor)


@_ensure_connection
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
    descriptor : metadatastore.document.Document or uid, optional
        if Document:
            The metadatastore run start document or the metadatastore uid to get
            the corresponding run end for
        if uid:
            Globally unique id string provided to metadatastore for the
            EventDescriptor Document.
    uid : str, optional
        Globally unique id string provided to metadatastore
    _id : str or ObjectId, optional
        The unique id generated by mongo

    Returns
    -------
    events : iterable of metadatastore.document.Document objects
    """
    # Some user-friendly error messages for an easy mistake to make
    if 'event_descriptor' in kwargs:
        raise ValueError("Use 'descriptor', not 'event_descriptor'.")
    if 'event_descriptor_id' in kwargs:
        raise ValueError("Use 'descriptor_id', not 'event_descriptor_id'.")

    _format_time(kwargs)

    _normalize_object_id(kwargs, '_id')
    events = Event.objects(__raw__=kwargs).order_by('-time')
    events = events.as_pymongo()

    for ev in events:
        # ditch the ObjectID
        ev.pop('_id')
        # pop the descriptor oid
        desc_oid = ev.pop('descriptor_id')
        # replace it with the defererenced descriptor
        ev['descriptor'] = _event_desc_given_oid(desc_oid)
        # pop the data
        data = ev.pop('data')
        # replace it with the friendly paired dicts
        ev['data'], ev['timestamps'] = [{k: v[j] for k, v in data.items()}
                                        for j in range(2)]
        # wrap it our fancy dict
        ev = doc.Document('Event', ev)

        yield ev


@_ensure_connection
def find_last(num=1):
    """Locate the last `num` RunStart Documents

    Parameters
    ----------
    num : integer, optional
        number of RunStart documents to return, default 1

    Returns
    -------
    run_start: iterable of metadatastore.document.Document objects
    """
    c = count()
    for rs in RunStart.objects.as_pymongo().order_by('-time'):
        if next(c) == num:
            raise StopIteration
        yield _cache_runstart(rs)


def _replace_dict_keys(input_dict, src, dst):
    """
    Helper function to replace forbidden chars in dictionary keys

    Parameters
    ----------
    input_dict : dict
        The dict to have it's keys replaced

    src : str
        the string to be replaced

    dst : str
        The string to replace the src string with

    Returns
    -------
    ret : dict
        The dictionary with all instances of 'src' in the key
        replaced with 'dst'

    """
    return {k.replace(src, dst): v for
            k, v in six.iteritems(input_dict)}


def _src_dst(direction):
    """
    Helper function to turn in/out into src/dst pair

    Parameters
    ----------
    direction : {'in', 'out'}
        The direction to do conversion (direction relative to mongodb)

    Returns
    -------
    src, dst : str
        The source and destination strings in that order.
    """
    if direction == 'in':
        src, dst = '.', '[dot]'
    elif direction == 'out':
        src, dst = '[dot]', '.'
    else:
        raise ValueError('Only in/out allowed as direction params')

    return src, dst


def _replace_descriptor_data_key_dots(ev_desc, direction='in'):
    """Replace the '.' with [dot].

    Relevant because PVs can have dots in their names

    Parameters
    ---------

    event_descriptor: metadatastore.odm_templates.EventDescriptor
    EvenDescriptor instance

    direction: str
    If 'in' ->  replace . with [dot]
    If 'out' -> replace [dot] with .

    """
    src, dst = _src_dst(direction)
    ev_desc.data_keys = _replace_dict_keys(ev_desc.data_keys,
                                           src, dst)
    return ev_desc


def _replace_event_data_key_dots(event, direction='in'):
    """Replace the '.' with [dot].

    Relevant because PVs can have dots in their names

    Parameters
    ---------

    event_descriptor: metadatastore.database.event_descriptor.EventDescriptor
    EvenDescriptor instance

    direction: str
    If 'in' ->  replace . with [dot]
    If 'out' -> replace [dot] with .

    """
    src, dst = _src_dst(direction)
    event.data = _replace_dict_keys(event.data,
                                    src, dst)
    return event


def reorganize_event(event_document):
    """Reorganize Event attributes, unnormalizing 'data'.

    Convert from Event.data = {'data_key': (value, timestamp)}
    to Event.data = {'data_key': value}
    and Event.timestamps = {'data_key': timestamp}

    Parameters
    ----------
    event_document : metadatastore.document.Document

    Returns
    -------
    event_document
    """
    doc = event_document  # for brevity
    pairs = [((k, v[0]), (k, v[1])) for k, v in six.iteritems(doc.data)]
    doc.data, doc.timestamps = [dict(tuples) for tuples in zip(*pairs)]
    return doc
