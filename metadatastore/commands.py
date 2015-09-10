from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six
from functools import wraps
from itertools import count

import datetime
import logging

import boltons.cacheutils
import pytz

from bson.dbref import DBRef

from mongoengine import connect
import mongoengine.connection

from . import conf
from .odm_templates import (RunStart, RunStop, EventDescriptor, Event, DataKey,
                            ALIAS)
from . import doc


logger = logging.getLogger(__name__)


# process local caches of 'header' documents these are storing object indexed
# on ObjectId because that is how the reference fields are implemented.
# Should move to uids asap
_RUNSTART_CACHE_OID = boltons.cacheutils.LRU(max_size=1000)
_RUNSTOP_CACHE_OID = boltons.cacheutils.LRU(max_size=1000)
_DESCRIPTOR_CACHE_OID = boltons.cacheutils.LRU(max_size=1000)

# never drop these
_RUNSTART_UID_to_OID_MAP = dict()
_RUNSTOP_UID_to_OID_MAP = dict()
_DESCRIPTOR_UID_to_OID_MAP = dict()


class NoRunStop(Exception):
    pass


class NoEventDescriptors(Exception):
    pass


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
    _RUNSTART_CACHE_OID.clear()
    _RUNSTOP_CACHE_OID.clear()
    _DESCRIPTOR_CACHE_OID.clear()

    _RUNSTART_UID_to_OID_MAP.clear()
    _RUNSTOP_UID_to_OID_MAP.clear()
    _DESCRIPTOR_UID_to_OID_MAP.clear()


def db_disconnect():
    """Helper function to deal with stateful connections to mongoengine"""
    mongoengine.connection.disconnect(ALIAS)
    for collection in [RunStart, RunStop, EventDescriptor, Event, DataKey]:
        collection._collection = None


def db_connect(database, host, port):
    """Helper function to deal with stateful connections to mongoengine

    .. warning

       This will silently ignore input if the database is already
       connected, even if the input database, host, or port are
       different than currently connected.  To change the database
       connection you must call `db_disconnect` before attempting to
       re-connect.
    """
    return connect(db=database, host=host, port=port, alias=ALIAS)


def _ensure_connection(func):
    """Decorator to ensure that the DB connection is open

    This uses the values in `conf.connection_config` to get connection
    parameters.

    .. warning

       This has no effect if the connection already exists, simply changing
       the values in `conf.connection_config` is not sufficient to change what
       data base is being used.  Calling `db_disconnect()` will clear the
       current connection, but you probably should not be doing this.
    """
    @wraps(func)
    def inner(*args, **kwargs):
        database = conf.connection_config['database']
        host = conf.connection_config['host']
        port = int(conf.connection_config['port'])
        db_connect(database=database, host=host, port=port)
        return func(*args, **kwargs)
    return inner


def _cache_runstart(runstart):
    """De-reference and cache a RunStart document

    The de-referenced Document is cached against the
    ObjectId and the uid -> ObjectID mapping is stored.

    Parameters
    ----------
    runstart : dict
        raw pymongo dictionary. This is expected to have
        an entry `_id` with the ObjectId used by mongo.

    Returns
    -------
    runstart : doc.Document
        Document instance for this RunStart document.
        The ObjectId has been stripped.
    """
    runstart = dict(runstart)
    # TODO actually do this de-reference for documents that have it
    # There is no known actually usage of this document and it is not being
    # created going forward
    runstart.pop('beamline_config_id', None)

    # get the mongo ObjectID
    oid = runstart.pop('_id')

    # convert the remaining document do a Document object
    runstart = doc.Document('RunStart', runstart)

    # populate cache and set up uid->oid mapping
    _RUNSTART_CACHE_OID[oid] = runstart
    _RUNSTART_UID_to_OID_MAP[runstart['uid']] = oid

    return runstart


def _cache_runstop(runstop):
    """De-reference and cache a RunStop document

    The de-referenced Document is cached against the
    ObjectId and the uid -> ObjectID mapping is stored.

    Parameters
    ----------
    runstop : dict
        raw pymongo dictionary. This is expected to have
        an entry `_id` with the ObjectId used by mongo.

    Returns
    -------
    runstop : doc.Document
        Document instance for this RunStop document.
        The ObjectId has been stripped.
    """
    runstop = dict(runstop)
    # pop off the ObjectId of this document
    oid = runstop.pop('_id')

    # do the run-start de-reference
    start_oid = runstop.pop('run_start_id')
    runstop['run_start'] = _runstart_given_oid(start_oid)

    # create the Document object
    runstop = doc.Document('RunStop', runstop)

    # update the cache and uid->oid mapping
    _RUNSTOP_CACHE_OID[oid] = runstop
    _RUNSTOP_UID_to_OID_MAP[runstop['uid']] = oid

    return runstop


def _cache_descriptor(descriptor):
    """De-reference and cache a RunStop document

    The de-referenced Document is cached against the
    ObjectId and the uid -> ObjectID mapping is stored.

    Parameters
    ----------
    descriptor : dict
        raw pymongo dictionary. This is expected to have
        an entry `_id` with the ObjectId used by mongo.

    Returns
    -------
    descriptor : doc.Document
        Document instance for this EventDescriptor document.
        The ObjectId has been stripped.
    """
    descriptor = dict(descriptor)
    # pop the ObjectID
    oid = descriptor.pop('_id')

    # do the runstart referencing
    start_oid = descriptor.pop('run_start_id')
    descriptor['run_start'] = _runstart_given_oid(start_oid)

    # create the Document instance
    descriptor = doc.Document('EventDescriptor', descriptor)

    # update cache and setup uid->oid mapping
    _DESCRIPTOR_CACHE_OID[oid] = descriptor
    _DESCRIPTOR_UID_to_OID_MAP[descriptor['uid']] = oid

    return descriptor


@_ensure_connection
def _runstart_given_oid(oid):
    """Get RunStart document given an ObjectId

    This is an internal function as ObjectIds should not be
    leaking out to user code.

    When we migrate to using uid as the primary key this function
    will be removed.

    Parameters
    ----------
    oid : ObjectId
        Mongo's unique identifier for the document.  This is currently
        used to implement foreign keys

    Returns
    -------
    runstart : doc.Document
        The RunStart document.
    """
    try:
        return _RUNSTART_CACHE_OID[oid]
    except KeyError:
        pass
    rs = RunStart._get_collection().find_one({'_id': oid})
    return _cache_runstart(rs)


@_ensure_connection
def _descriptor_given_oid(oid):
    """Get EventDescriptor document given an ObjectId

    This is an internal function as ObjectIds should not be
    leaking out to user code.

    When we migrate to using uid as the primary key this function
    will be removed.

    Parameters
    ----------
    oid : ObjectId
        Mongo's unique identifier for the document.  This is currently
        used to implement foreign keys

    Returns
    -------
    descriptor : doc.Document
        The EventDescriptor document.
    """
    try:
        return _DESCRIPTOR_CACHE_OID[oid]
    except KeyError:
        pass

    descriptor = EventDescriptor._get_collection().find_one({'_id': oid})
    return _cache_descriptor(descriptor)


@_ensure_connection
def runstart_given_uid(uid):
    """Given a uid, return the RunStart document

    Parameters
    ----------
    uid : str
        The uid

    Returns
    -------
    runstart : doc.Document
        The RunStart document.

    """
    try:
        oid = _RUNSTART_UID_to_OID_MAP[uid]
        return _RUNSTART_CACHE_OID[oid]
    except KeyError:
        pass
    runstart = RunStart._get_collection().find_one({'uid': uid})
    return _cache_runstart(runstart)


@_ensure_connection
def runstop_given_uid(uid):
    """Given a uid, return the RunStop document

    Parameters
    ----------
    uid : str
        The uid

    Returns
    -------
    runstop : doc.Document
        The RunStop document fully de-referenced

    """
    try:
        oid = _RUNSTOP_UID_to_OID_MAP[uid]
        return _RUNSTOP_CACHE_OID[oid]
    except KeyError:
        pass
    # get the raw runstop
    runstop = RunStop._get_collection().find_one({'uid': uid})
    return _cache_runstop(runstop)


@_ensure_connection
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
    try:
        oid = _DESCRIPTOR_UID_to_OID_MAP[uid]
        return _DESCRIPTOR_CACHE_OID[oid]
    except KeyError:
        pass

    descriptor = EventDescriptor._get_collection().find_one({'uid': uid})
    return _cache_descriptor(descriptor)


@_ensure_connection
def runstop_by_runstart(runstart):
    """Given a RunStart return it's RunStop

    Raises if no RunStop exists.

    Parameters
    ----------
    runstart : doc.Document or dict or str
        The RunStart to get the RunStop for.  Can be either
        a Document/dict with a 'uid' key or a uid string

    Returns
    -------
    runstop : doc.Document
        The RunStop document

    Raises
    ------
    NoRunStop
        If no RunStop document exists for the given RunStart
    """
    runstart_uid = doc_or_uid_to_uid(runstart)
    # make sure the cache is actually populated
    runstart_given_uid(runstart_uid)
    oid = _RUNSTART_UID_to_OID_MAP[runstart_uid]

    runstop = RunStop._get_collection().find_one(
        {'run_start_id': oid})

    if runstop is None:
        raise NoRunStop("No run stop exists for {!r}".format(runstart))

    return _cache_runstop(runstop)


@_ensure_connection
def descriptors_by_runstart(runstart):
    """Given a RunStart return a list of it's descriptors

    Raises if no EventDescriptors exist.

    Parameters
    ----------
    runstart : doc.Document or dict or str
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
    # normalize the input and get the runstart oid
    runstart_uid = doc_or_uid_to_uid(runstart)
    runstart_given_uid(runstart_uid)
    oid = _RUNSTART_UID_to_OID_MAP[runstart_uid]

    # query the database for any event descriptors which
    # refer to the given runstart
    descriptors = EventDescriptor._get_collection().find(
        {'run_start_id': oid})

    # loop over the found documents, cache, and dereference
    rets = [_cache_descriptor(descriptor) for descriptor in descriptors]

    # if nothing found, raise
    if not rets:
        raise NoEventDescriptors("No EventDescriptors exists "
                                 "for {!r}".format(runstart))

    # return the list of event descriptors
    return rets


@_ensure_connection
def fetch_events_generator(descriptor):
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
    descriptor_uid = doc_or_uid_to_uid(descriptor)
    descriptor = descriptor_given_uid(descriptor_uid)

    oid = _DESCRIPTOR_UID_to_OID_MAP[descriptor_uid]

    col = Event._get_collection()
    ev_cur = col.find({'descriptor_id': oid},
                      sort=[('time', 1)])

    for ev in ev_cur:
        # ditch the ObjectID
        del ev['_id']
        # del the descriptor oid
        del ev['descriptor_id']
        # replace it with the defererenced descriptor
        ev['descriptor'] = descriptor
        data = ev.pop('data')
        ev['timestamps'] = {k: v[1] for k, v in data.items()}
        ev['data'] = {k: v[0] for k, v in data.items()}

        # wrap it in our fancy dict
        ev = doc.Document('Event', ev)

        yield ev


def _transpose(in_data, keys, field):
    """Turn a list of dicts into dict of lists

    Parameters
    ----------
    in_data : list
        A list of dicts which contain at least one dict.
        All of the inner dicts must have at least the keys
        in `keys`

    keys : list
        The list of keys to extract

    field : str
        The field in the outer dict to use

    Returns
    -------
    transpose : dict
        The transpose of the data
    """
    out = {k: [None] * len(in_data) for k in keys}
    for j, ev in enumerate(in_data):
        dd = ev[field]
        for k in keys:
            out[k][j] = dd[k]
    return out


@_ensure_connection
def fetch_events_table(descriptor):
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
    desc_uid = doc_or_uid_to_uid(descriptor)
    descriptor = descriptor_given_uid(desc_uid)
    # this will get more complicated once transpose caching layer is in place
    all_events = list(fetch_events_generator(desc_uid))

    # get event sequence numbers
    seq_nums = [ev['seq_num'] for ev in all_events]

    # get event times
    times = [ev['time'] for ev in all_events]

    # get uids
    uids = [ev['uid'] for ev in all_events]

    keys = list(descriptor['data_keys'])

    # get data values
    data_table = _transpose(all_events, keys, 'data')

    # get timestamps
    timestamps_table = _transpose(all_events, keys, 'timestamps')

    # return the whole lot
    return descriptor, data_table, seq_nums, times, uids, timestamps_table


# database INSERTION ###################################################

@_ensure_connection
def insert_runstart(time, scan_id, beamline_id, uid, owner='', group='',
                    project='', custom=None):
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
    custom : dict, optional
        Any additional information that data acquisition code/user wants
        to append to the RunStart at the start of the run. These keys will be
        unpacked into the top level of the RunStart that is inserted into the
        database.

    Returns
    -------
    runstart : str
        uid of the inserted document.  Use `runstart_given_uid` to get
        the full document.

    """
    if custom is None:
        custom = {}

    runstart = RunStart(time=time, scan_id=scan_id, uid=uid,
                        beamline_id=beamline_id, owner=owner, group=group,
                        project=project, **custom)

    runstart = runstart.save(validate=True, write_concern={"w": 1})

    _cache_runstart(runstart.to_mongo().to_dict())
    logger.debug('Inserted RunStart with uid %s', runstart.uid)

    return uid


@_ensure_connection
def insert_runstop(run_start, time, uid, exit_status='success', reason='',
                   custom=None):
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
    custom : dict, optional
        Any additional information that data acquisition code/user wants
        to append to the Header at the end of the run.  These keys will be
        unpacked into the top level of the RunStop that is inserted
        into the database.

    Returns
    -------
    runstop : str
        uid of inserted Document

    Raises
    ------
    RuntimeError
        Only one RunStop per RunStart, raises if you try to insert a second
    """
    if custom is None:
        custom = {}
    runstart_uid = doc_or_uid_to_uid(run_start)
    runstart = runstart_given_uid(runstart_uid)
    try:
        runstop_by_runstart(runstart_uid)
    except NoRunStop:
        pass
    else:
        raise RuntimeError("Runstop already exits for {!r}".format(runstart))
    # look up the oid
    runstart_oid = _RUNSTART_UID_to_OID_MAP[runstart_uid]
    # create a reference field
    rs_ref = DBRef('RunStart', runstart_oid)

    runstop = RunStop(run_start=rs_ref, reason=reason, time=time,
                      uid=uid,
                      exit_status=exit_status, **custom)

    runstop = runstop.save(validate=True, write_concern={"w": 1})
    _cache_runstop(runstop.to_mongo().to_dict())
    logger.debug("Inserted RunStop with uid %s referencing RunStart "
                 " with uid %s", runstop.uid, runstart['uid'])

    return uid


@_ensure_connection
def insert_descriptor(run_start, data_keys, time, uid,
                      custom=None):
    """Insert an EventDescriptor document in to database.

    Parameters
    ----------
    run_start : doc.Document or dict or str
        The RunStart to insert a Descriptor for.  Can be either
        a Document/dict with a 'uid' key or a uid string
    data_keys : dict
        Provides information about keys of the data dictionary in
        an event will contain.  No key name may include '.'
    time : float
        The date/time as found at the client side when an event
        descriptor is created.
    uid : str
        Globally unique id string provided to metadatastore
    custom : dict, optional
        Any additional information that data acquisition code/user wants
        to append to the EventDescriptor.  These keys will be unpacked into
        the top level of the EventDescriptor that is inserted into the
        database.

    Returns
    -------
    descriptor : str
        uid of inserted Document
    """
    if custom is None:
        custom = {}

    for k in data_keys:
        if '.' in k:
            raise ValueError("Key names can not contain '.' (period).")
    # needed to make ME happy
    data_keys = {k: DataKey(**v) for k, v in data_keys.items()}

    runstart_uid = doc_or_uid_to_uid(run_start)
    # get document to make sure it is in the cache
    runstart_given_uid(runstart_uid)
    runstart_oid = _RUNSTART_UID_to_OID_MAP[runstart_uid]
    rs_ref = DBRef('RunStart', runstart_oid)
    descriptor = EventDescriptor(run_start=rs_ref, data_keys=data_keys,
                                 time=time, uid=uid, **custom)

    descriptor = descriptor.save(validate=True, write_concern={"w": 1})

    descriptor = _cache_descriptor(descriptor.to_mongo().to_dict())

    logger.debug("Inserted EventDescriptor with uid %s referencing "
                 "RunStart with uid %s", descriptor['uid'], runstart_uid)

    return uid
insert_event_descriptor = insert_descriptor


@_ensure_connection
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
    # convert data to storage format
    val_ts_tuple = _transform_data(data, timestamps)
    # make sure we really have a uid
    descriptor_uid = doc_or_uid_to_uid(descriptor)
    # get descriptor to make sure it is in the cache
    descriptor = descriptor_given_uid(descriptor_uid)
    # get the ObjectID so for reference field
    desc_oid = _DESCRIPTOR_UID_to_OID_MAP[descriptor_uid]
    # create the Event document
    event = Event(descriptor_id=desc_oid, uid=uid,
                  data=val_ts_tuple, time=time, seq_num=seq_num)

    event.save(validate=True, write_concern={"w": 1})

    logger.debug("Inserted Event with uid %s referencing "
                 "EventDescriptor with uid %s", event.uid,
                 descriptor_uid)
    return uid

BAD_KEYS_FMT = """Event documents are malformed, the keys on 'data' and
'timestamps do not match:\n data: {}\ntimestamps:{}"""


@_ensure_connection
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
    descriptor_uid = doc_or_uid_to_uid(event_descriptor)
    descriptor = descriptor_given_uid(descriptor_uid)

    desc_oid = _DESCRIPTOR_UID_to_OID_MAP[descriptor['uid']]

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


@_ensure_connection
def find_runstarts(**kwargs):
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
    >>> find_runstarts(scan_id=123)
    >>> find_runstarts(owner='arkilic')
    >>> find_runstarts(start_time=1421176750.514707, stop_time=time.time()})
    >>> find_runstarts(start_time=1421176750.514707, stop_time=time.time())

    >>> find_runstarts(owner='arkilic', start_time=1421176750.514707,
    ...                stop_time=time.time())

    """
    # now try rest of formatting
    _format_time(kwargs)

    rs_objects = RunStart.objects(__raw__=kwargs).as_pymongo()

    return (_cache_runstart(rs) for rs in rs_objects.order_by('-time'))
find_run_starts = find_runstarts


@_ensure_connection
def find_runstops(runstart=None, **kwargs):
    """Given search criteria, locate RunStop Documents.

    Parameters
    ----------
    runstart : doc.Document or str, optional
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
    runstop : doc.Document
        The requested RunStop documents
    """
    # if trying to find by runstart, there can be only one
    if runstart:
        def inner():
            try:
                yield runstop_by_runstart(runstart)
            except NoRunStop:
                return
        return inner()

    _format_time(kwargs)
    runstop = RunStop.objects(__raw__=kwargs).as_pymongo()

    return (_cache_runstop(rs) for rs in runstop.order_by('-time'))
find_run_stops = find_runstops


@_ensure_connection
def find_descriptors(runstart=None, **kwargs):
    """Given search criteria, locate EventDescriptor Documents.

    Parameters
    ----------
    runstart : doc.Document or str, optional
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
    if runstart:
        runstart = doc_or_uid_to_uid(runstart)
        runstart = runstart_given_uid(runstart)
        runstart = _RUNSTART_UID_to_OID_MAP[runstart['uid']]
        kwargs['run_start_id'] = runstart

    _format_time(kwargs)

    event_descriptor_objects = EventDescriptor.objects(__raw__=kwargs)

    event_descriptor_objects = event_descriptor_objects.as_pymongo()
    for event_descriptor in event_descriptor_objects.order_by('-time'):
        yield _cache_descriptor(event_descriptor)

# TODO properly mark this as deprecated
find_event_descriptors = find_descriptors


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
        descriptor = doc_or_uid_to_uid(descriptor)
        descriptor = descriptor_given_uid(descriptor)
        descriptor = _DESCRIPTOR_UID_to_OID_MAP[descriptor['uid']]
        kwargs['descriptor_id'] = descriptor

    _format_time(kwargs)

    events = Event.objects(__raw__=kwargs).order_by('time')
    events = events.as_pymongo()

    for ev in events:
        del ev['_id']
        # pop the descriptor oid
        desc_oid = ev.pop('descriptor_id')
        # replace it with the defererenced descriptor
        ev['descriptor'] = _descriptor_given_oid(desc_oid)
        data = ev.pop('data')
        # re-format the data
        ev['timestamps'] = {k: v[1] for k, v in data.items()}
        ev['data'] = {k: v[0] for k, v in data.items()}
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

    Yields
    ------
    runstart doc.Document
       The requested RunStart documents
    """
    c = count()
    for rs in RunStart.objects.as_pymongo().order_by('-time'):
        if next(c) == num:
            raise StopIteration
        yield _cache_runstart(rs)
