from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import warnings
import six  # noqa
import logging
import doct as doc
import numpy as np
from ..core import format_time as _format_time

logger = logging.getLogger(__name__)


class HeaderSourceShim(object):
    '''Shim class to turn a mds object into a HeaderSource

    This will presumably be deleted if this API makes it's way back down
    into the implementations
    '''
    def __init__(self, mds):
        self.mds = mds

    def __call__(self, text_search=None, filters=None, **kwargs):
        if filters is None:
            filters = []
        if text_search is not None:
            query = {'$and': [{'$text': {'$search': text_search}}] + filters}
        else:
            # Include empty {} here so that '$and' gets at least one query.
            _format_time(kwargs, self.mds.config['timezone'])
            query = {'$and': [{}] + [kwargs] + filters}

        starts = self.mds.find_run_starts(**query)
        return ((s, safe_get_stop(self, s)) for s in starts)

    def __getitem__(self, k):
        from ..core import search

        return search(k, self)

    def insert(self, name, doc):
        return self.mds.insert(name, doc)

    def find_last(self, num):
        return self.mds.find_last(num)

    def find_run_starts(self, *args, **kwargs):
        return self.mds.find_run_starts(*args, **kwargs)

    def stop_by_start(self, s):
        return self.mds.stop_by_start(s)

    @property
    def NoRunStart(self):
        return self.mds.NoRunStart

    @property
    def NoRunStop(self):
        return self.mds.NoRunStop


# singletons defined as they are defined in pymongo
ASCENDING = 1
DESCENDING = -1


def _format_regex(d):
    for k, v in six.iteritems(d):
        if k == '$regex':
            # format regex for monoquery
            d[k] = '/{0}/'.format(v)
        else:
            # recurse if v is a dict
            if hasattr(v, 'items'):
                _format_regex(v)


class NoRunStop(Exception):
    pass


class NoRunStart(Exception):
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


def _cache_run_start(run_start, run_start_cache):
    """De-reference and cache a RunStart document

    The de-referenced Document is cached against the
    ObjectId and the uid -> ObjectID mapping is stored.

    Parameters
    ----------
    run_start : dict
        raw pymongo dictionary. This is expected to have
        an entry `_id` with the ObjectId used by mongo.

    Returns
    -------
    run_start : doc.Document
        Document instance for this RunStart document.
        The ObjectId has been stripped.
    """
    run_start = dict(run_start)
    # TODO actually do this de-reference for documents that have it
    # There is no known actually usage of this document and it is not being
    # created going forward
    run_start.pop('beamline_config_id', None)

    # get the mongo ObjectID
    oid = run_start.pop('_id', None)

    # convert the remaining document to a Document object
    run_start = doc.Document('RunStart', run_start)

    run_start_cache[run_start['uid']] = run_start
    run_start_cache[oid] = run_start
    return run_start


def _cache_run_stop(run_stop, run_stop_cache,
                    run_start_col, run_start_cache):
    """De-reference and cache a RunStop document

    The de-referenced Document is cached against the
    ObjectId and the uid -> ObjectID mapping is stored.

    Parameters
    ----------
    run_stop : dict
        raw pymongo dictionary. This is expected to have
        an entry `_id` with the ObjectId used by mongo.

    Returns
    -------
    run_stop : doc.Document
        Document instance for this RunStop document.
        The ObjectId has been stripped.
    """
    run_stop = dict(run_stop)
    # pop off the ObjectId of this document
    oid = run_stop.pop('_id', None)
    try:
        run_start_uid = run_stop['run_start']
    except KeyError:
        run_start_uid = run_stop.pop('run_start_id')
    # do the run-start de-reference
    run_stop['run_start'] = run_start_given_uid(run_start_uid,
                                                run_start_col,
                                                run_start_cache)

    # create the Document object
    run_stop = doc.Document('RunStop', run_stop)

    run_stop_cache[run_stop['uid']] = run_stop
    run_stop_cache[oid] = run_stop

    return run_stop


def _cache_descriptor(descriptor, descritor_cache,
                      run_start_col, run_start_cache):
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
    oid = descriptor.pop('_id', None)
    try:
        run_start_uid = descriptor['run_start']
    except KeyError:
        run_start_uid = descriptor.pop('run_start_id')

    # do the run_start referencing
    descriptor['run_start'] = run_start_given_uid(run_start_uid,
                                                  run_start_col,
                                                  run_start_cache)

    # create the Document instance
    descriptor = doc.Document('EventDescriptor', descriptor)

    descritor_cache[descriptor['uid']] = descriptor
    descritor_cache[oid] = descriptor

    return descriptor


def run_start_given_uid(uid, run_start_col, run_start_cache):
    """Given a uid, return the RunStart document

    Parameters
    ----------
    uid : str
        The uid

    run_start_col : pymongo.Collection
        The collection to search for documents

    run_start_cache : MutableMapping
        Mutable mapping to serve as a local cache

    Returns
    -------
    run_start : doc.Document
        The RunStart document.

    """
    try:
        return run_start_cache[uid]
    except KeyError:
        pass

    run_start = run_start_col.find_one({'uid': uid})

    if run_start is None:
        raise NoRunStart("No runstart with uid {!r}".format(uid))
    return _cache_run_start(run_start, run_start_cache)


def run_stop_given_uid(uid, run_stop_col, run_stop_cache,
                       run_start_col, run_start_cache):
    """Given a uid, return the RunStop document

    Parameters
    ----------
    uid : str
        The uid

    run_stop_col : pymongo.Collection
        The collection to search for documents

    run_stop_cache : MutableMapping
        Mutable mapping to serve as a local cache

    Returns
    -------
    run_stop : doc.Document
        The RunStop document fully de-referenced

    """
    try:
        return run_stop_cache[uid]
    except KeyError:
        pass
    # get the raw run_stop
    run_stop = run_stop_col.find_one({'uid': uid})

    return _cache_run_stop(run_stop, run_stop_cache,
                           run_start_col, run_start_cache)


def descriptor_given_uid(uid, descriptor_col, descriptor_cache,
                         run_start_col, run_start_cache):
    """Given a uid, return the EventDescriptor document

    Parameters
    ----------
    uid : str
        The uid

    descriptor_col : pymongo.Collection
        The collection to search for documents

    descriptor_cache : MutableMapping
        Mutable mapping to serve as a local cache

    Returns
    -------
    descriptor : doc.Document
        The EventDescriptor document fully de-referenced
    """
    try:
        return descriptor_cache[uid]
    except KeyError:
        pass
    descriptor = descriptor_col.find_one({'uid': uid})

    return _cache_descriptor(descriptor, descriptor_cache, run_start_col,
                             run_start_cache)


def stop_by_start(run_start, run_stop_col, run_stop_cache,
                  run_start_col, run_start_cache):
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
    run_start_uid = doc_or_uid_to_uid(run_start)
    run_stop = run_stop_col.find_one({'run_start': run_start_uid})
    if run_stop is None:
        raise NoRunStop("No run stop exists for {!r}".format(run_start))

    return _cache_run_stop(run_stop, run_stop_cache,
                           run_start_col, run_start_cache)


def descriptors_by_start(run_start, descriptor_col, descriptor_cache,
                         run_start_col, run_start_cache):
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
    # normalize the input and get the run_start oid
    run_start_uid = doc_or_uid_to_uid(run_start)

    # query the database for any event descriptors which
    # refer to the given run_start
    descriptors = descriptor_col.find({'run_start': run_start_uid})
    # loop over the found documents, cache, and dereference
    rets = [_cache_descriptor(descriptor, descriptor_cache,
                              run_start_col, run_start_cache)
            for descriptor in descriptors]

    # if nothing found, raise
    if not rets:
        raise NoEventDescriptors("No EventDescriptors exists "
                                 "for {!r}".format(run_start))

    # return the list of event descriptors
    return rets


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
                      sort=[('time', ASCENDING)])

    data_keys = descriptor['data_keys']
    external_keys = [k for k in data_keys if 'external' in data_keys[k]]
    for ev in ev_cur:
        # ditch the ObjectID
        ev.pop('_id', None)

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


def get_events_table(descriptor, event_col, descriptor_col,
                     descriptor_cache, run_start_col, run_start_cache):
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
    descriptor = descriptor_given_uid(desc_uid, descriptor_col,
                                      descriptor_cache, run_start_col,
                                      run_start_cache)
    # this will get more complicated once transpose caching layer is in place
    all_events = list(get_events_generator(desc_uid, event_col,
                                           descriptor_col,
                                           descriptor_cache,
                                           run_start_col,
                                           run_start_cache))

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

    return event_col.insert(event_factory())


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
    _format_regex(kwargs)
    rs_objects = run_start_col.find(kwargs,
                                    sort=[('time', DESCENDING)])

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
    run_stop = col.find(kwargs, sort=[('time', ASCENDING)])

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
                                        sort=[('time', ASCENDING)])

    for event_descriptor in event_descriptor_objects:
        yield _cache_descriptor(event_descriptor, descriptor_cache,
                                start_col, start_cache)


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
    gen = col.find({}, sort=[('time', DESCENDING)])
    for _ in range(num):
        yield _cache_run_start(next(gen), start_cache)


class MDSROTemplate(object):
    def __init__(self, config, version=1, auth=False):
        if auth:
            raise NotImplementedError("This implementation of MDS does not "
                                      "support authentication.")
        self._RUNSTART_CACHE = {}
        self._RUNSTOP_CACHE = {}
        self._DESCRIPTOR_CACHE = {}
        self.config = config
        self._api = None
        self.version = version

    def reset_caches(self):
        self._RUNSTART_CACHE.clear()
        self._RUNSTOP_CACHE.clear()
        self._DESCRIPTOR_CACHE.clear()

    def __getstate__(self):
        return self.version, self.config

    def __setstate__(self, state):
        self._RUNSTART_CACHE = {}
        self._RUNSTOP_CACHE = {}
        self._DESCRIPTOR_CACHE = {}
        self._api = None
        self.version, self.config = state

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, val):
        if self._api is not None:
            raise RuntimeError("Can not change api version at runtime")
        self._api = _API_MAP[val]
        self._version = val

    @property
    def NoRunStart(self):
        return self._api.NoRunStart

    @property
    def NoRunStop(self):
        return self._api.NoRunStop

    @property
    def NoEventDescriptors(self):
        return self._api.NoEventDescriptors

    def reconfigure(self, config):
        self.config = config

    def clear_process_cache(self):
        """Clear all local caches"""
        self._RUNSTART_CACHE.clear()
        self._RUNSTOP_CACHE.clear()
        self._DESCRIPTOR_CACHE.clear()

    def run_start_given_uid(self, uid):
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
        return self._api.run_start_given_uid(uid, self._runstart_col,
                                             self._RUNSTART_CACHE)

    def run_stop_given_uid(self, uid):
        """Given a uid, return the RunStop document

        Parameters
        ----------
        uid : str
            The uid

        Returns
        -------
        run_stop : doc.Document
            The RunStop document.

        """
        return self._api.run_stop_given_uid(uid,
                                            self._runstop_col,
                                            self._RUNSTOP_CACHE,
                                            self._runstart_col,
                                            self._RUNSTART_CACHE)

    def descriptor_given_uid(self, uid):
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
        return self._api.descriptor_given_uid(uid, self._descriptor_col,
                                              self._DESCRIPTOR_CACHE,
                                              self._runstart_col,
                                              self._RUNSTART_CACHE)

    def stop_by_start(self, run_start):
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
        return self._api.stop_by_start(run_start,
                                       self._runstop_col,
                                       self._RUNSTOP_CACHE,
                                       self._runstart_col,
                                       self._RUNSTART_CACHE)

    def descriptors_by_start(self, run_start):
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
        return self._api.descriptors_by_start(run_start,
                                              self._descriptor_col,
                                              self._DESCRIPTOR_CACHE,
                                              self._runstart_col,
                                              self._RUNSTART_CACHE)

    def get_events_generator(self, descriptor, convert_arrays=True):
        """A generator which yields all events from the event stream

        Parameters
        ----------
        descriptor : doc.Document or dict or str
            The EventDescriptor to get the Events for.  Can be either
            a Document/dict with a 'uid' key or a uid string
        convert_arrays : boolean
            convert 'array' type to numpy.ndarray; True by default

        Yields
        ------
        event : doc.Document
            All events for the given EventDescriptor from oldest to
            newest
        """
        evs = self._api.get_events_generator(descriptor,
                                             self._event_col,
                                             self._descriptor_col,
                                             self._DESCRIPTOR_CACHE,
                                             self._runstart_col,
                                             self._RUNSTART_CACHE,
                                             convert_arrays=convert_arrays)

        # when we drop 2.7, this can be
        # yield from evs
        for ev in evs:
            yield ev

    def get_events_table(self, descriptor):
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
        return self._api.get_events_table(descriptor,
                                          self._event_col,
                                          self._descriptor_col,
                                          self._DESCRIPTOR_CACHE,
                                          self._runstart_col,
                                          self._RUNSTART_CACHE)

    def find_run_starts(self, **kwargs):
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
        >>> find_run_starts(start_time=1421176750, stop_time=time.time()})
        >>> find_run_starts(start_time=1421176750, stop_time=time.time())

        >>> find_run_starts(owner='arkilic', start_time=1421176750.514707,
        ...                stop_time=time.time())

        """
        gen = self._api.find_run_starts(self._runstart_col,
                                        self._RUNSTART_CACHE,
                                        self.config['timezone'],
                                        **kwargs)
        for rs in gen:
            yield rs

    def find_run_stops(self, **kwargs):
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
        gen = self._api.find_run_stops(self._runstart_col,
                                       self._RUNSTART_CACHE,
                                       self._runstop_col,
                                       self._RUNSTOP_CACHE,
                                       self.config['timezone'],
                                       **kwargs)
        for rs in gen:
            yield rs

    def find_descriptors(self, **kwargs):
        """Given search criteria, locate EventDescriptor Documents.

        Parameters
        ----------
        run_start : doc.Document or str, optional
            The RunStart document or uid to get the corresponding run end for
        start_time : time-like, optional
            time-like representation of the earliest time that an
            EventDescriptor was created. Valid options are:
               - timestamps --> time.time()
               - '2015'
               - '2015-01'
               - '2015-01-30'
               - '2015-03-30 03:00:00'
               - datetime.datetime.now()
        stop_time : time-like, optional
            timestamp of the latest time that an EventDescriptor was created.
            See docs for `start_time` for examples.
        uid : str, optional
            Globally unique id string provided to metadatastore

        Yields
        -------
        descriptor : doc.Document
            The requested EventDescriptor
        """
        gen = self._api.find_descriptors(self._runstart_col,
                                         self._RUNSTART_CACHE,
                                         self._descriptor_col,
                                         self._DESCRIPTOR_CACHE,
                                         self.config['timezone'],
                                         **kwargs)
        for desc in gen:
            yield desc

    def find_last(self, num=1):
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

        for ev in self._api.find_last(self._runstart_col,
                                      self._RUNSTART_CACHE,
                                      num=num):
            yield ev


class MDSTemplate(MDSROTemplate):
    _INS_METHODS = {'start': 'insert_run_start',
                    'stop': 'insert_run_stop',
                    'descriptor': 'insert_descriptor',
                    'event': 'insert_event',
                    'bulk_events': 'bulk_insert_events'}

    def insert_run_start(self, time, uid, **kwargs):
        '''Insert a Start document

        All extra keyword arguments are passed through to the database
        as fields in the Start document.

        Parameters
        ----------
        time : float
            The date/time as found at the client side when the run is started
        uid : str
            Globally unique id to identify this RunStart
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
            passed through

        Returns
        -------
        run_start : str
            uid of the inserted document.  Use `run_start_given_uid` to get
            the full document.
        '''
        return self._api.insert_run_start(self._runstart_col,
                                          self._RUNSTART_CACHE,
                                          time=time,
                                          uid=uid,
                                          **kwargs)

    def insert_run_stop(self, run_start, time, uid, exit_status='success',
                        reason='', **kwargs):
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
        return self._api.insert_run_stop(self._runstart_col,
                                         self._RUNSTART_CACHE,
                                         self._runstop_col,
                                         self._RUNSTOP_CACHE,
                                         run_start=run_start,
                                         time=time, uid=uid,
                                         exit_status=exit_status,
                                         reason=reason, **kwargs)

    def insert_descriptor(self, run_start, data_keys, time, uid, **kwargs):
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
        return self._api.insert_descriptor(self._runstart_col,
                                           self._RUNSTART_CACHE,
                                           self._descriptor_col,
                                           self._DESCRIPTOR_CACHE,
                                           run_start=run_start,
                                           data_keys=data_keys,
                                           time=time, uid=uid,
                                           **kwargs)

    def insert_event(self, descriptor, time, seq_num, data, timestamps, uid,
                     validate=False):
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
        return self._api.insert_event(self._event_col,
                                      descriptor=descriptor,
                                      time=time, seq_num=seq_num,
                                      data=data,
                                      timestamps=timestamps,
                                      uid=uid,
                                      validate=validate)

    def bulk_insert_events(self, descriptor, events, validate=False):
        return self._api.bulk_insert_events(self._event_col,
                                            descriptor=descriptor,
                                            events=events,
                                            validate=validate)

    def insert(self, name, doc):
        if name != 'bulk_events':
            getattr(self, self._INS_METHODS[name])(**doc)
        else:
            for desc_uid, events in doc.items():
                # If events is empty, mongo chokes.
                if not events:
                    continue
                self.bulk_insert_events(desc_uid, events)


def safe_get_stop(hs, s):
    try:
        return hs.stop_by_start(s)
    except hs.NoRunStop:
        return None
