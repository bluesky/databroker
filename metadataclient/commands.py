import requests
import datetime
import pytz
import six
from functools import wraps
import ujson
from metadataclient import (conf, utils)
from metadataclient.conf import _server_path


def server_connect(host, port, protocol='http'):
    """The server here refers the metadataservice server itself, not the mongo server.
    For MongoDB server information, check metadataservice.

    Parameters
    ----------
    host: str
        name/address of the mongo server
    port: int
        port number of mongo server
    protocol: str, optional
        in case we want to introduce ssl in the future.

     Returns
    -------
    _server_path: str
        Full server path that is set globally. Useful for monkeypatching
        overwrites config and updates this global server path.
    """
    _server_path = protocol + '://' + host + ':' + str(port)
    return str(_server_path)


def server_disconnect():
    """ Rolls back to default configuration from yaml file"""
    protocol = conf.connection_config['protocol']
    host = conf.connection_config['host']
    port = conf.connection_config['port']
    server_connect(host=host, port=port, protocol=protocol)


def _ensure_connection(func):
    """Ensures connection to the tornado server, not mongo instance itself.
    Essentially, re-using what we have done for the library but this time for tornado backend
    instead of mongodb daemon
    """
    @wraps(func)
    def inner(*args, **kwargs):
        protocol = conf.connection_config['protocol']
        host = conf.connection_config['host']
        port = conf.connection_config['port']
        server_connect(host=host, port=port, protocol=protocol)
        return func(*args, **kwargs)
    return inner


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
    return str(doc_or_uid)


class NoRunStop(Exception):
    pass


class NoEventDescriptors(Exception):
    pass


class NoRunStart(Exception):
    pass


class MetadataServiceError(Exception):
    pass


@_ensure_connection
def runstart_given_uid(uid):
    """ uid based search for run_start documents.

    Parameters
    ----------
    uid : str
        Unique identifier for the document.

    Returns
    -------
    run_start : metadataservice.utils.Document
        The RunStart document.
    """
    # TODO: Add capped collection caching lookup
    return next(find_run_starts(uid=uid)) #already returns document


@_ensure_connection
def _run_start_given_oid(oid):
    """No support for this anymore. Implemented in case it leaked somewhere in the stack"""
    raise MetadataServiceError('No ObjectId based search is supported. DEPRECATED')


@_ensure_connection
def _descriptor_given_oid(oid):
    """No support for this anymore. Implemented in case it leaked somewhere in the stack"""
    raise MetadataServiceError('No ObjectId based search is supported. DEPRECATED')


@_ensure_connection
def runstop_given_uid(uid):
    """Given a uid, return the RunStop document.

    Parameters
    ----------
    uid : str
        The uid

    Returns
    -------
    run_stop : utils.Document
        The RunStop document fully de-referenced
    """
    run_stop = dict(next(find_run_stops(uid=uid)))
    start_oid = run_stop.pop('run_start_id')
    run_stop['run_start'] = _run_start_given_oid(start_oid)
    return utils.Document('RunStop', run_stop)


@_ensure_connection
def run_start_given_uid(uid):
    """Given a uid, return the RunStart document

    Parameters
    ----------
    uid : str
        The uid

    Returns
    -------
    run_start : utils.Document
        The RunStart document.

    """
    return utils.Document('RunStart', next(find_run_starts(uid=uid)))


@_ensure_connection
def run_stop_given_uid(uid):
    """Given a uid, return the RunStop document

    Parameters
    ----------
    uid : str
        The uid

    Returns
    -------
    run_stop : utils.Document
        The RunStop document fully de-referenced

    """
    return next(find_run_stops(uid=uid))


@_ensure_connection
def descriptor_given_uid(uid):
    """Given a uid, return the EventDescriptor document

    Parameters
    ----------
    uid : str
        The uid

    Returns
    -------
    EventDescriptor : utils.Document
        The EventDescriptor document fully de-referenced
    """

    return utils.Document('EventDescriptor',
                          next(find_descriptors(uid=uid)))


@_ensure_connection
def stop_by_start(run_start):
    """Given a RunStart return it's RunStop
    Raises if no RunStop exists.

    Parameters
    ----------
    run_start : utils.Document or dict or str
        The RunStart to get the RunStop for.  Can be either
        a Document/dict with a 'uid' key or a uid string.

    Returns
    -------
    run_stop : utils.Document
        The RunStop document

    Raises
    ------
    NoRunStop
        If no RunStop document exists for the given RunStart.
    """
    rstart = doc_or_uid_to_uid(run_start)
    try:
        run_stop = next(find_run_stops(run_start=rstart))
        return utils.Document('RunStop', run_stop)
    except StopIteration:
        raise NoRunStop("No run stop exists for {!r}".format(run_start))


@_ensure_connection
def descriptors_by_start(run_start):
    """Given a RunStart return a list of it's descriptors

    Raises if no EventDescriptors exist.

    Parameters
    ----------
    run_start : utils.Document or dict or str
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
    run_start_id = doc_or_uid_to_uid(run_start)
    res = find_descriptors(run_start=run_start_id)
    e_descs = [utils.Document('EventDescriptor', r) for r in res]

    if not e_descs:
        raise NoEventDescriptors("No EventDescriptors exists "
                                 "for {!r}".format(run_start))
    # temp solution to get databroker working with this.
    if len(e_descs) == 1:
        return e_descs[0]
    else:
        return e_descs


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
    event : utils.Document
        All events for the given EventDescriptor from oldest to
        newest
    """
    desc = descriptor_given_uid(descriptor.uid)
    raw_ev_gen = find_events(descriptor_id=str(desc['_id']))
    for ev in raw_ev_gen:
        ev['descriptor'] = dict(desc)
        yield utils.Document('Event', ev)


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
    # TODO: Implement this and ADD TO UNIT TESTS
    raise NotImplementedError("Implement this and ADD TO UNIT TESTS")


@_ensure_connection
def find_run_starts(**kwargs):
    """Given search criteria, locate RunStart Documents.
    As we describe in design document, time here is strictly the time
    server entry was created, not IOC timestamp. For the difference, refer
    to: nsls-ii.github.io
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
    
    Yields utils.Document
    -------
        RunStart documents if query returned something

    Note
    ----
    All documents that the RunStart Document points to are dereferenced.
    These include RunStop, BeamlineConfig, and Sample.

    Examples
    --------
    >>> find_run_starts(scan_id=123)
    >>> find_run_starts(owner='arkilic')
    >>> find_run_starts(start_time=1421176750.514707, stop_time=time.time()})
    >>> find_run_starts(start_time=1421176750.514707, stop_time=time.time())
    >>> find_run_starts(owner='arkilic', start_time=1421176750.514707,
    ...                stop_time=time.time())
    """
    _format_time(kwargs)
    query = kwargs
    r = requests.get(_server_path + "/run_start",
                     params=ujson.dumps(query))
    # r.raise_for_status()
    # TODO: Done this way to replicate library behavior, we need to change this to raise_status?
    if r.status_code != 200:
        return None
    content = ujson.loads(r.text)
    if not content:
        return None
    else:
        for c in content:
            yield utils.Document('RunStart', c)


def find_last(num=1):
    """Return the last 'num' many headers"""
    query = dict(num=num)
    r = requests.get(_server_path + "/run_start",
                     params=ujson.dumps(query))
    if r.status_code != 200:
        return None
    content = ujson.loads(r.text)
    if not content:
        return None
    else:
        for c in content:
            yield utils.Document('RunStart', c)

@_ensure_connection
def find_run_stops(run_start=None, **kwargs):
    """Given search criteria, query for RunStop Documents.
    Parameters
    ----------
    run_start : utils.Document or str, optional
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
    
    Yields utils.Document
    ------
        RunStop documents if found in the query
    """
    _format_time(kwargs)
    query = kwargs
    rstart = None
    if run_start:
        query['run_start'] = doc_or_uid_to_uid(run_start)
        rstart = next(find_run_starts(uid=query['run_start']))
    r = requests.get(_server_path + "/run_stop",
                     params=ujson.dumps(query))
    if r.status_code != 200:
        return None
    content = ujson.loads(r.text)
    if not content:
        return None
    else:
        for c in content:
            if rstart is None:
                rstart = next(find_run_starts(uid=c['run_start']))
            c['run_start'] = rstart
            yield utils.Document('RunStop',c)
        
@_ensure_connection
def find_events(descriptor=None, **kwargs):
    """
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
    Yields
    -------
    events : iterable of utils.Document objects
    """
    # TODO: Add more tests!!! Make sure descriptor is returned for each event

    if 'event_descriptor' in kwargs:
        raise ValueError("Use 'descriptor', not 'event_descriptor'.")
    query = kwargs
    desc = None
    if descriptor:
        desc_uid = doc_or_uid_to_uid(descriptor)
        query['descriptor'] = desc_uid
    _format_time(query)
    r = requests.get(_server_path + "/event",
                     params=ujson.dumps(query),
                     stream=True)
    r.raise_for_status()
    content = ujson.loads(r.text)
    if not content:
        return None
    else:
        if descriptor:
            desc = next(find_descriptors(uid=doc_or_uid_to_uid(descriptor)))
        for c in content:
            if not  desc:
                # Obvious bottleneck!!!
                # Fix using local caching!!!!
                desc = next(find_descriptors(uid=c['descriptor']))
            c['descriptor'] = desc
            yield utils.Document('Event', c)


@_ensure_connection
def find_descriptors(run_start=None, **kwargs):
    """Given search criteria, locate EventDescriptor Documents.

    Parameters
    ----------
    run_start : metadatastore.document.Document or uid, optional
        The metadatastore run start document or the metadatastore uid to get
        the corresponding run end for
    run_start_uid : str
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
    
    Returns
    -------
    content : iterable of list of json documents
        We need lists to be able to JSON encode multiple dicts. We can return an iterator of
         iterator?
    """
    _format_time(kwargs)
    query = kwargs
    rstart = None
    if run_start:
        query['run_start'] = doc_or_uid_to_uid(run_start)
        try:
            rstart = next(find_run_starts(uid=query['run_start']))
        except StopIteration:
            raise NoRunStart()    
    r = requests.get(_server_path + "/event_descriptor",
                     params=ujson.dumps(query))
    if r.status_code != 200:
        return None
    else:
        content = ujson.loads(r.text)
        if not content:
            return None
        else:    
            for c in content:
                if rstart is None:
                    rstart = next(find_run_starts(uid=c['run_start']))
                c['run_start'] = rstart
                yield utils.Document('EventDescriptor',c)
        
                

@_ensure_connection
def insert_event(descriptor, time, seq_num, data, timestamps, uid):
    "Insert a single event. Server handles this in bulk"
    event = dict(time=time, seq_num=seq_num,
                 data=data, timestamps=timestamps, uid=uid)
    desc_uid = doc_or_uid_to_uid(descriptor)
    event['descriptor'] = desc_uid

    ev = ujson.dumps(event)
    r = requests.post(_server_path + '/event', data=ev)
    r.raise_for_status()
    return uid


def _transform_data(data, timestamps):
    """
    Transform from Document spec:
        {'data': {'key': <value>},
         'timestamps': {'key': <timestamp>}}
    to storage format:
        {'data': {<key>: (<value>, <timestamp>)}.
    """
    return {k: (data[k], timestamps[k]) for k in data}


@_ensure_connection
def get_events_generator(descriptor):
    """A generator which yields all events from the event stream
    Parameters
    ----------
    descriptor : doc.Document or dict or str
        The EventDescriptor to get the Events for.  Can be either
        a Document/dict with a 'uid' key or a uid string
    Yields
    ------
    event : utils.Document
        All events for the given EventDescriptor from oldest to
        newest
    """
    descriptor_uid = doc_or_uid_to_uid(descriptor)
    ev_cur = find_events(descriptor=descriptor_uid)
    for ev in ev_cur:
        ev = dict(ev)
        ev['descriptor'] = next(find_descriptors(uid=descriptor_uid))
        yield utils.Document('Event', ev)


@_ensure_connection
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
    desc_uid = doc_or_uid_to_uid(descriptor)
    descriptor = descriptor_given_uid(desc_uid)
    # this will get more complicated once transpose caching layer is in place
    all_events = list(get_events_generator(desc_uid))
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
    ret : str
        text of details about the insertion
    """
    BAD_KEYS_FMT = """Event documents are malformed, the keys on 'data' and
                     'timestamps do not match:\n data: {}\ntimestamps:{}"""
    for ev in events:
        desc_id = doc_or_uid_to_uid(event_descriptor)
        ev['descriptor'] = desc_id
        if validate:
            if ev['data'].keys() != ev['timestamps'].keys():
                raise ValueError(
                    BAD_KEYS_FMT.format(ev['data'].keys(),
                                        ev['timestamps'].keys()))

            # transform the data to the storage format
            val_ts_tuple = _transform_data(ev['data'], ev['timestamps'])
            ev_out = dict(descriptor=desc_id, uid=ev['uid'],
                  data=val_ts_tuple, time=ev['time'],
                  seq_num=ev['seq_num'])
            ev = ev_out

    chunk_size = 500
    data_len = len(events)
    chunk_count = data_len // chunk_size + bool(data_len % chunk_size)
    chunks = utils.grouper(events, chunk_count)
    for c in chunks:
        payload = ujson.dumps(list(c))
        r = requests.post(_server_path + '/event', data=payload, timeout=None)
        r.raise_for_status()
        # TODO: Add unittest for this failure
    r.text

@_ensure_connection
def insert_descriptor(run_start, data_keys, time, uid,
                      custom=None):
    """ Create an event_descriptor in metadatastore server backend

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
    uid : str, optional
        Globally unique id string provided to metadatastore
    custom : dict, optional
        Any additional information that data acquisition code/user wants
        to append to the EventDescriptor.Again, custom not unpacked.

    Returns
    -------
    ev_desc : EventDescriptor
        The document added to the collection.

    """
    payload = dict(data_keys=data_keys,
                   time=time, uid=uid, custom=custom)
    rs_uid = doc_or_uid_to_uid(run_start)
    payload['run_start'] = rs_uid
    r = requests.post(_server_path + '/event_descriptor', 
                      data=ujson.dumps(payload))
    if r.status_code != 200:
        raise ValueError('Bad event descriptor')
    return uid


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
        to append to the Header at the start of the run. These are not unpacked to avoid
        confusion. Data on the way in should be identical to data retrieved for consistency(any data
        science book, chapter 1. Read CAP theorem for more info). 
        
    Returns
    ----------
    utils.Document
        The run_start document that is successfully saved in mongo

    """
    data = dict(time=time, scan_id=scan_id, beamline_id=beamline_id, uid=uid,
                 owner=owner, group=group, project=project)
    data = {k: v if v is not None else '' for k,v in data.items()}

    if custom:
        z = data.copy()
        z.update(custom)
        payload = ujson.dumps(z)
    else:
        data['custom'] = {}
        payload = ujson.dumps(data)
    r = requests.post(_server_path + '/run_start', data=payload)
    r.raise_for_status()
    return uid


@_ensure_connection
def insert_run_stop(run_start, time, uid, exit_status='success',
                    reason='', custom=None):
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
    uid : str, optional
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
    uid : str
        uid of the inserted document
    """
    params = dict(time=time, uid=uid, exit_status=exit_status,
                  reason=reason)
    params['run_start'] = doc_or_uid_to_uid(run_start)
    if custom:
        z = params.copy()
        z.update(custom)
        payload = ujson.dumps(z)
    else:
        params['custom'] = {}
        payload = ujson.dumps(params)
    r = requests.post(_server_path + '/run_stop', data=payload)
    if r.status_code == 500:
        raise RuntimeError('RunStop already exists for this RunStart')
    return uid


def format_events():
    raise NotImplementedError('You got me, turns out somebody needs this')


def format_data_keys():
    raise NotImplementedError('You got me, turns out somebody needs this')


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


def monitor_run_start():
    raise NotImplementedError('Not this cycle. Code works, needs capped collection support')
    r = requests.get(_server_path + '/run_start_capped')
    content = ujson.loads(r.text)
    yield utils.Document('RunStart', content)


def _insert2cappedstart(time, scan_id, config, beamline_id, beamline_config={}, uid=None,
                    owner=None, group=None, project=None, custom=None):
    raise NotImplementedError('Not this cycle. Code works, needs capped collection support')
    data = locals()
    if custom:
        data.update(custom)
    payload = ujson.dumps(data)
    r = requests.post(_server_path + '/run_start_capped', data=payload)
    if r.status_code != 200:
        raise Exception("Server cannot complete the request", r)
    else:
        return r.json()


def _insert2cappedstop(run_start, time, uid=None, config={}, exit_status='success',
                    reason=None, custom=None):
    raise NotImplementedError('Not this cycle. Code works, needs capped collection support')
    payload = ujson.dumps(locals())
    r = requests.post(_server_path + '/run_stop', data=payload)
    if r.status_code != 200:
        raise Exception("Server cannot complete the request", r)
    else:
        return r.json()


def monitor_run_stop():
    r = requests.get(_server_path + '/run_stop_capped')
    content = ujson.loads(r.text)
    yield utils.Document('RunStop', content)


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


# human friendly timestamp formats we'll parse
_TS_FORMATS = [
    '%Y-%m-%d %H:%M:%S',
    '%Y-%m-%d %H:%M',  # these 2 are not as originally doc'd,
    '%Y-%m-%d %H',  # but match previous pandas behavior
    '%Y-%m-%d',
    '%Y-%m',
    '%Y']

# build a tab indented, '-' bulleted list of supported formats
# to append to the parsing function docstring below
_doc_ts_formats = '\n'.join('\t- {}'.format(_) for _ in _TS_FORMATS)

# fill in the placeholder we left in the previous docstring
_normalize_human_friendly_time.__doc__ = (
    _normalize_human_friendly_time.__doc__.format(_doc_ts_formats)
)

# TODO: Add when descriptor is None, use cached descriptor!
# TODO: Fix descriptor replace (related to above)
# TODO: Add server_disconnect that rolls all config back to default
# TODO: Add capped collection caching layer for the client
# TODO: Add fast read/write capped collection, different than caching capped collection
# TODO: Add timeouts to servers in order to stop ppl from abusing data sources
