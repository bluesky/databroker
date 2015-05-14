from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import six
from functools import wraps
from itertools import count
from .odm_templates import (RunStart, BeamlineConfig, RunStop,
                            EventDescriptor, Event, DataKey, ALIAS)
from .document import Document
import datetime
import logging
from metadatastore import conf
from mongoengine import connect,  ReferenceField
import mongoengine.connection

import datetime
import pytz

import uuid
from bson import ObjectId

logger = logging.getLogger(__name__)


def _ensure_connection(func):
    @wraps(func)
    def inner(*args, **kwargs):
        database = conf.connection_config['database']
        host = conf.connection_config['host']
        port = conf.connection_config['port']
        db_connect(database=database, host=host, port=port)
        return func(*args, **kwargs)
    return inner


def db_disconnect():
    mongoengine.connection.disconnect(ALIAS)
    for collection in [RunStart, BeamlineConfig, RunStop, EventDescriptor,
                       Event, DataKey]:
        collection._collection = None


def db_connect(database, host, port):
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
            'source': source_value,
            'dtype': dtype_value,
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


def format_events(event_dict):
    """Helper function for ophyd to format its data dictionary in whatever
    flavor of the week metadatastore's spec says. This insulates ophyd from
    changes to the mds spec

    Currently formats the dictionary as {key: [value, timestamp]}

    Parameters
    ----------
    event_dict : dict
        The format that ophyd is sending to metadatastore
        {'data_key1': {
            'timestamp': timestamp_value, # should be a float value!
            'value': data_value
         'data_key2': {...}
        }

    Returns
    -------
    formatted_dict : dict
        The event dict formatted according to the current metadatastore spec.
        The current metadatastore spec is:
        {'data_key1': [data_value, timestamp_value],
         'data_key2': [...],
        }
    """
    return {key: [data_dict['value'], data_dict['timestamp']]
            for key, data_dict in six.iteritems(event_dict)}


# database INSERTION ###################################################

@_ensure_connection
def insert_run_start(time, scan_id, beamline_id, beamline_config, uid=None,
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
    beamline_config : str
        uid of beamline config corresponding to a given run
    uid : str, optional
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
    if uid is None:
        uid = str(uuid.uuid4())
    if custom is None:
        custom = {}
    if owner is None:
        owner = ''
    if group is None:
        group = ''
    if project is None:
        project = ''

    beamline_config = BeamlineConfig.objects(uid=beamline_config).first()
    run_start = RunStart(time=time, scan_id=scan_id,
                         time_as_datetime=_todatetime(time), uid=uid,
                         beamline_id=beamline_id,
                         beamline_config=beamline_config,
                         owner=owner, group=group, project=project,
                         **custom)

    run_start.save(validate=True, write_concern={"w": 1})
    logger.debug('Inserted RunStart with uid %s', run_start.uid)

    return uid


@_ensure_connection
def insert_run_stop(run_start, time, uid=None, exit_status='success',
                    reason=None, custom=None):
    """ Provide an end to a sequence of events. Exit point for an
    experiment's run.

    Parameters
    ----------
    run_start : str
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
    run_stop : mongoengine.Document
        Inserted mongoengine object
    """
    if uid is None:
        uid = str(uuid.uuid4())
    if custom is None:
        custom = {}
    run_start = RunStart.objects(uid=run_start).first()
    run_stop = RunStop(run_start=run_start, reason=reason, time=time,
                       time_as_datetime=_todatetime(time), uid=uid,
                       exit_status=exit_status, **custom)

    run_stop.save(validate=True, write_concern={"w": 1})
    logger.debug("Inserted RunStop with uid %s referencing RunStart "
                 " with uid %s", run_stop.uid, run_start.uid)

    return uid


@_ensure_connection
def insert_beamline_config(config_params, time, uid=None):
    """ Create a beamline_config  in metadatastore database backend

    Parameters
    ----------
    config_params : dict
        Name/value pairs that indicate beamline configuration
        parameters during capturing of data
    time : float
        The date/time as found at the client side when the
        beamline configuration is created.
    uid : str, optional
        Globally unique id string provided to metadatastore

    Returns
    -------
    blc : BeamlineConfig
        The document added to the collection
    """
    if uid is None:
        uid = str(uuid.uuid4())
    beamline_config = BeamlineConfig(config_params=config_params,
                                     time=time,
                                     uid=uid)
    beamline_config.save(validate=True, write_concern={"w": 1})
    logger.debug("Inserted BeamlineConfig with uid %s",
                 beamline_config.uid)

    return uid


@_ensure_connection
def insert_event_descriptor(run_start, data_keys, time, uid=None,
                            custom=None):
    """ Create an event_descriptor in metadatastore database backend

    Parameters
    ----------
    run_start : str
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
        to append to the EventDescriptor.

    Returns
    -------
    ev_desc : EventDescriptor
        The document added to the collection.

    """
    if uid is None:
        uid = str(uuid.uuid4())
    if custom is None:
        custom = {}
    data_keys = format_data_keys(data_keys)
    run_start = RunStart.objects(uid=run_start).first()
    event_descriptor = EventDescriptor(run_start=run_start,
                                       data_keys=data_keys, time=time,
                                       uid=uid,
                                       time_as_datetime=_todatetime(time),
                                       **custom)

    event_descriptor = _replace_descriptor_data_key_dots(event_descriptor,
                                                          direction='in')

    event_descriptor.save(validate=True, write_concern={"w": 1})
    logger.debug("Inserted EventDescriptor with uid %s referencing "
                 "RunStart with uid %s", event_descriptor.uid, run_start.uid)

    return uid


@_ensure_connection
def insert_event(event_descriptor, time, data, seq_num, uid=None):
    """Create an event in metadatastore database backend

    Parameters
    ----------
    event_descriptor : str
        uid of EventDescriptor object to associate with this record
    time : float
        The date/time as found at the client side when an event is
        created.
    data : dict
        Dictionary that contains the name value fields for the data associated
        with an event
    seq_num : int
        Unique sequence number for the event. Provides order of an event in
        the group of events
    uid : str, optional
        Globally unique id string provided to metadatastore
    """
    m_data = _validate_data(data)

    # Allow caller to beg forgiveness rather than ask permission w.r.t
    # EventDescriptor creation.
    if event_descriptor is None:
        raise EventDescriptorIsNoneError()

    if uid is None:
        uid = str(uuid.uuid4())

    event_descriptor = EventDescriptor.objects(uid=event_descriptor).first()
    event = Event(descriptor_id=event_descriptor, uid=uid,
                  data=m_data, time=time, seq_num=seq_num)

    event = _replace_event_data_key_dots(event, direction='in')
    event.save(validate=True, write_concern={"w": 1})
    logger.debug("Inserted Event with uid %s referencing "
                 "EventDescriptor with uid %s", event.uid,
                 event_descriptor.uid)
    return uid


def _validate_data(data):
    m_data = dict()
    for k, v in six.iteritems(data):
        if isinstance(v, (list, tuple)):
            if len(v) == 2:
                m_data[k] = list(v)
            else:
                raise ValueError('List must contain value and timestamp')
        else:
            raise TypeError('Data fields must be lists!')
    return m_data


class EventDescriptorIsNoneError(ValueError):
    """Special error that ophyd looks for when it passes a `None` event
    descriptor. Ophyd will then create an event descriptor and create the event
    again
    """
    pass


# DATABASE RETRIEVAL ##########################################################

# TODO: Update all query routine documentation
class _AsDocument(object):
    """
    A caching layer to avoid creating reference objects for _every_
    """
    def __init__(self):
        self._cache = dict()

    def __call__(self, mongoengine_object):
        return Document.from_mongo(mongoengine_object, self._cache)


class _AsDocumentRaw(object):
    """
    A caching layer to avoid creating reference objects for _every_
    """
    def __init__(self):
        self._cache = dict()

    def __call__(self, name, input_dict, dref_fields):

        return Document.from_dict(name, input_dict, dref_fields, self._cache)


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
    _normalize_object_id(kwargs, '_id')
    _format_time(kwargs)
    _as_document = _AsDocument()

    rs_objects = RunStart.objects(__raw__=kwargs).order_by('-time')
    rs_objects = rs_objects.no_dereference()
    return (_as_document(rs) for rs in rs_objects)


@_ensure_connection
def find_beamline_configs(**kwargs):
    """Given search criteria, locate BeamlineConfig Documents.

    Parameters
    ----------
    start_time : time-like, optional
        time-like representation of the earliest time that a BeamlineConfig
        was created. Valid options are:
           - timestamps --> time.time()
           - '2015'
           - '2015-01'
           - '2015-01-30'
           - '2015-03-30 03:00:00'
           - datetime.datetime.now()
    stop_time : time-like, optional
        timestamp of the latest time that a BeamlineConfig was created. See
            docs for `start_time` for examples.
    uid : str, optional
        Globally unique id string provided to metadatastore
    _id : str or ObjectId, optional
        The unique id generated by mongo

    Returns
    -------
    beamline_configs : iterable of metadatastore.document.Document objects
    """
    _as_document = _AsDocument()
    _format_time(kwargs)
    # ordered by _id because it is not guaranteed there will be time in cbonfig
    beamline_configs = BeamlineConfig.objects(__raw__=kwargs).order_by('-_id')
    beamline_configs = beamline_configs.no_dereference()
    return (_as_document(bc) for bc in beamline_configs)


@_ensure_connection
def find_run_stops(**kwargs):
    """Given search criteria, locate RunStop Documents.

    Parameters
    ----------
    run_start : metadatastore.document.Document, optional
        The run start to get the corresponding run end for
    run_start_id : str or ObjectId, optional
        The unique id generated by mongo for the RunStart Document.
        NOTE: If both `run_start` and `run_start_id` are provided,
              `run_start.id` supersedes `run_start_id`
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

    try:
        kwargs['run_start_id'] = kwargs.pop('run_start').id
    except KeyError:
        pass
    _normalize_object_id(kwargs, '_id')
    _normalize_object_id(kwargs, 'run_start_id')
    run_stop = RunStop.objects(__raw__=kwargs).order_by('-time')
    run_stop = run_stop.no_dereference()

    _as_document = _AsDocument()

    return (_as_document(rs) for rs in run_stop)


@_ensure_connection
def find_event_descriptors(**kwargs):
    """Given search criteria, locate EventDescriptor Documents.

    Parameters
    ----------
    run_start : mongoengine.Document.Document, optional
        RunStart object EventDescriptor points to
    run_start_id : str or ObjectId, optional
        The unique id generated by mongo for the RunStart Document.
        NOTE: If both `run_start` and `run_start_id` are provided,
              `run_start.id` supersedes `run_start_id`
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
    _as_document = _AsDocument()
    try:
        kwargs['run_start_id'] = kwargs.pop('run_start').id
    except KeyError:
        pass
    _normalize_object_id(kwargs, '_id')
    _normalize_object_id(kwargs, 'run_start_id')
    event_descriptor_objects = EventDescriptor.objects(__raw__=kwargs)

    event_descriptor_objects = event_descriptor_objects.no_dereference()
    for event_descriptor in event_descriptor_objects.order_by('-time'):
        event_descriptor = _replace_descriptor_data_key_dots(event_descriptor,
                                                             direction='out')
        yield _as_document(event_descriptor)


@_ensure_connection
def find_events(**kwargs):
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
    descriptor : mongoengine.Document, optional
        event descriptor object
    descriptor_id : str or ObjectId
        The unique id generated by mongo for the EventDescriptor
        NOTE: If both `descriptor` and `descriptor_id` are provided,
              `descriptor.id` supersedes `descriptor_id`
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
    try:
        kwargs['descriptor_id'] = kwargs.pop('descriptor').id
    except KeyError:
        pass
    _normalize_object_id(kwargs, '_id')
    _normalize_object_id(kwargs, 'descriptor_id')
    events = Event.objects(__raw__=kwargs).order_by('-time')
    events = events.as_pymongo()
    dref_dict = dict()
    name = Event.__name__
    for n, f in Event._fields.items():
        if isinstance(f, ReferenceField):
            lookup_name = f.db_field
            dref_dict[lookup_name] = f

    _as_document = _AsDocumentRaw()
    return (reorganize_event(_as_document(name, ev, dref_dict))
            for ev in events)


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
    _as_document = _AsDocument()
    for rs in RunStart.objects.order_by('-time'):
        if next(c) == num:
            raise StopIteration
        yield _as_document(rs)


def _todatetime(time_stamp):
    if isinstance(time_stamp, float):
        return datetime.datetime.fromtimestamp(time_stamp)
    else:
        raise TypeError('Timestamp format is not correct!')


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
    """Replace the '.' with [dot]

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
    """Replace the '.' with [dot]

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
