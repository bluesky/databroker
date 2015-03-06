from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import six
from functools import wraps
from .odm_templates import (RunStart, BeamlineConfig, RunStop,
                            EventDescriptor, Event, DataKey, ALIAS)
from .document import Document
import datetime
import logging
from metadatastore import conf
from mongoengine import connect
import mongoengine.connection
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
def insert_run_start(time, beamline_id, beamline_config=None, owner=None,
                     scan_id=None, custom=None, uid=None):
    """ Provide a head for a sequence of events. Entry point for an
    experiment's run.

    Parameters
    ----------
    time : float
        The date/time as found at the client side when an event is
        created.
    beamline_id: str
        Beamline String identifier. Not unique, just an indicator of
        beamline code for multiple beamline systems
    beamline_config: metadatastore.odm_temples.BeamlineConfig, optional
        Foreign key to beamline config corresponding to a given run
    owner: str, optional
        Specifies the unix user credentials of the user creating the entry
    scan_id : int, optional
        Unique scan identifier visible to the user and data analysis
    custom: dict, optional
        Additional parameters that data acquisition code/user wants to
        append to a given header. Name/value pairs

    Returns
    -------
    run_start: mongoengine.Document
        Inserted mongoengine object

    """
    if uid is None:
        uid = str(uuid.uuid4())
    if custom is None:
        custom = {}

    run_start = RunStart(time=time, scan_id=scan_id, owner=owner,
                         time_as_datetime=__todatetime(time), uid=uid,
                         beamline_id=beamline_id,
                         beamline_config=beamline_config
                         if beamline_config else None,
                         **custom)

    run_start.save(validate=True, write_concern={"w": 1})

    return run_start


@_ensure_connection
def insert_run_stop(run_start, time, exit_status='success',
                    reason=None, uid=None):
    """ Provide an end to a sequence of events. Exit point for an
    experiment's run.

    Parameters
    ----------
    run_start : metadatastore.odm_temples.RunStart
        Foreign key to corresponding RunStart
    time : timestamp
        The date/time as found at the client side when an event is
        created.
    reason : str, optional
        provides information regarding the run success. 20 characters max

    Returns
    -------
    run_stop : mongoengine.Document
        Inserted mongoengine object
    """
    if uid is None:
        uid = str(uuid.uuid4())
    run_stop = RunStop(run_start=run_start, reason=reason, time=time,
                       time_as_datetime=__todatetime(time), uid=uid,
                       exit_status=exit_status)

    run_stop.save(validate=True, write_concern={"w": 1})

    return run_stop


@_ensure_connection
def insert_beamline_config(config_params, time, uid=None):
    """ Create a beamline_config  in metadatastore database backend

    Parameters
    ----------
    config_params : dict, optional
        Name/value pairs that indicate beamline configuration
        parameters during capturing of data

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

    return beamline_config


@_ensure_connection
def insert_event_descriptor(run_start, data_keys, time, uid=None):
    """ Create an event_descriptor in metadatastore database backend

    Parameters
    ----------
    run_start: metadatastore.odm_templates.RunStart
        RunStart object created prior to a RunStart
    data_keys : dict
        Provides information about keys of the data dictionary in
        an event will contain
    time : timestamp
        The date/time as found at the client side when an event
        descriptor is created.

    Returns
    -------
    ev_desc : EventDescriptor
        The document added to the collection.

    """
    if uid is None:
        uid = str(uuid.uuid4())
    data_keys = format_data_keys(data_keys)
    event_descriptor = EventDescriptor(run_start=run_start,
                                       data_keys=data_keys, time=time,
                                       uid=uid,
                                       time_as_datetime=__todatetime(time))

    event_descriptor = __replace_descriptor_data_key_dots(event_descriptor,
                                                          direction='in')

    event_descriptor.save(validate=True, write_concern={"w": 1})

    return event_descriptor


@_ensure_connection
def insert_event(event_descriptor, time, data, seq_num, uid=None):
    """Create an event in metadatastore database backend

    Parameters
    ----------
    event_descriptor : metadatastore.odm_templates.EventDescriptor
        EventDescriptor object that specific event entry is going
        to point(foreign key)
    time : timestamp
        The date/time as found at the client side when an event is
        created.
    data : dict
        Dictionary that contains the name value fields for the data associated
        with an event
    seq_num : int
        Unique sequence number for the event. Provides order of an event in
        the group of events
    """
    m_data = __validate_data(data)

    # mostly here to notify ophyd that an event descriptor needs to be created
    if event_descriptor is None:
        raise EventDescriptorIsNoneError()

    if uid is None:
        uid = str(uuid.uuid4())

    event = Event(descriptor_id=event_descriptor, uid=uid,
                  data=m_data, time=time, seq_num=seq_num)

    event = __replace_event_data_key_dots(event, direction='in')
    event.save(validate=True, write_concern={"w": 1})
    return event


def __validate_data(data):
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
def __as_document(mongoengine_object):
    return Document(mongoengine_object)


@_ensure_connection
def find_run_start(limit=50, **kwargs):
    """ Given search criteria, locate RunStart objects

    Parameters
    ----------
    limit : int, optional
        Maximum number of RunStart Documents to be returned.
    start_time : float, optional
        timestamp of the earliest time that a RunStart was created
    stop_time : float, optional
        timestamp of the latest time that a RunStart was created
    beamline_id : str, optional
        String identifier for a specific beamline
    project : str, optional
        Project name
    owner : str, optional
        The username of the logged-in user when the scan was performed
    scan_id : int, optional
        Integer scan identifier
    group : str, optional
        ???
    sample : ???, optional
        ???
    uid : str, optional
        Hashed unique identifier
    _id : str or ObjectId, optnal
        The id of the RunStart mongo document

    Returns
    -------
    rs_objects : list of metadatastore.document.Document
        Combined (dereferenced) documents: RunStart, BeamlineConfig, Sample
        Note: All documents that the RunStart Document points to are
              dereferenced

    Examples
    --------
    >>> find_run_start(scan_id=123)
    >>> find_run_start(owner='arkilic')
    >>> find_run_start(start_time=1421176750.514707, stop_time=time.time()})
    >>> find_run_start(start_time=1421176750.514707, stop_time=time.time())

    >>> find_run_start(owner='arkilic', start_time=1421176750.514707,
    ...                stop_time=time.time())

    """
    time_dict = {}
    start_time = kwargs.pop('start_time', None)
    stop_time = kwargs.pop('stop_time', None)
    if start_time:
        time_dict['$gte'] = start_time
    if stop_time:
        time_dict['$lte'] = stop_time
    if time_dict:
        kwargs['time'] = time_dict
    rs_objects = RunStart.objects(__raw__=kwargs).order_by('-time')[:limit]
    return [__as_document(rs) for rs in rs_objects]


@_ensure_connection
def find_beamline_config(**kwargs):
    """Return beamline config objects given a unique mongo _id

    Parameters
    ----------
    kwargs: dict
        Any Mongo query on beamline config can be represented
        as a dict and submitted

    Returns
    -------
    beamline_config : metadatastore.document.Document
        The beamline config object
    """
    # ordered by _id because it is not guaranteed there will be time in cbonfig
    beamline_configs = BeamlineConfig.objects(__raw__=kwargs).order_by('-_id')
    return [__as_document(bc) for bc in beamline_configs]


@_ensure_connection
def find_run_stop(**kwargs):
    """Return a run_stop object given a run_start document

    Parameters
    ----------
    run_start : metadatastore.document.Document, optional
        The run start to get the corresponding run end for
    exit_status : {'success', 'fail', 'abort'}, optional
        provides information regarding the run success.
    time : float, optional
        The date/time as found at the client side when an event is
        created.
    reason : str, optional
        Long-form description of why the run was terminated.
    _id : str or ObjectID, optional
        The mongo id of the run_stop Document that you are interested in

    Returns
    -------
    run_stop : list of metadatastore.document.Document
        The run stop objects containing the `exit_status` enum, the `time` the
        run ended and the `reason` the run ended and a pointer to their run
        headers
    """
    try:
        # ensure that the id field is an ObjectId, otherwise monogo will
        # not be able to search on it
        kwargs['run_start_id'] = ObjectId(kwargs.pop('run_start').id)
    except KeyError:
        pass
    try:
        # ensure that the run_stop '_id' is an ObjectId
        kwargs['_id'] = ObjectId(kwargs['_id'])
    except KeyError:
        pass
    # do the actual search and return a QuerySet object
    run_stop = RunStop.objects(__raw__=kwargs).order_by('-time')
    # turn the QuerySet object into a list of Document object
    return [__as_document(rs) for rs in run_stop]


@_ensure_connection
def find_event_descriptor(**kwargs):
    """Return beamline config objects given a unique mongo id

    Parameters
    ----------
    run_start : mongoengine.Document.Document
        RunStart object EventDescriptor points to

    Returns
    -------
    event_descriptor : list
        List of metadatastore.document.Document.
    """
    event_descriptor_list = list()
    try:
        kwargs['run_start_id'] = kwargs.pop('run_start').id
    except KeyError:
        pass
    event_descriptor_objects = EventDescriptor.objects(__raw__=kwargs)
    for event_descriptor in event_descriptor_objects.order_by('-time'):
        event_descriptor = __replace_descriptor_data_key_dots(event_descriptor,
                                                              direction='out')
        event_descriptor_list.append(event_descriptor)
    return [__as_document(evd) for evd in event_descriptor_list]


@_ensure_connection
def find_event(**kwargs):
    """

    Parameters
    -----------
    limit : int
        number of events returned
    time : dict, optional
        time of the event. dict keys must be start and stop
    descriptor : mongoengine.Document, optional
        event descriptor object
    scan_id: int
        Non-unique human friendly scan identifier
    owner: str
        Unix user credentials that created run_start
    beamline_id: str
        alias for beamline
    time: dict
        run_start create time.
        Refer to design docs for timestamp vs. time convetions
        Usage: time={'start': float, 'end': float}

    Returns
    -------
    events : list
        List of metadatastore.document.Document
    """
    query_dict = dict()
    run_start = None
    run_start_id = None
    results = None
    descriptor_id = None
    try:
        run_start = kwargs.pop('run_start')
    except KeyError:
        pass
    try:
        run_start_id = kwargs.pop('run_start_id')
    except KeyError:
        pass
    try:
        descriptor = kwargs.pop('event_descriptor')
    except KeyError:
        pass
    if (run_start) and (run_start_id):
        raise ValueError('find_event either via run_start object or id')
    if ((run_start) or (run_start_id)) and descriptor_id:
        raise ValueError('find_event either via run_start or event_desc')
    if run_start:
        e_desc_ids = list()
        e_descs = find_event_descriptor(run_start=run_start)
        e_desc_ids = [ObjectId(entry.id) for entry in e_descs]
        query_dict['descriptor_id'] = {'$in': e_desc_ids}
        query_dict.update(kwargs)
        results = Event.objects(__raw__=query_dict).order_by('-time')
    elif run_start_id:
        e_desc_ids = list()
        e_descs = find_event_descriptor(run_start_id=ObjectId(run_start_id))
        e_desc_ids = [ObjectId(entry.id) for entry in e_descs]
        query_dict['descriptor_id'] = {'$in': e_desc_ids}
        query_dict.update(kwargs)
        results = Event.objects(__raw__=query_dict).order_by('-time')
    elif descriptor:
        query_dict['descriptor_id'] = descriptor.id
        query_dict.update(kwargs)
        results = Event.objects(__raw__=query_dict).order_by('-time')
    else:
        query_dict.update(kwargs)
        results = Event.objects(__raw__=query_dict).order_by('-time')
    return [__as_document(ev) for ev in results]


@_ensure_connection
def find_last(num=1):
    """Indexed on time.

    Returns the last created header not modified!!

    Returns
    -------
    run_start: list
        Returns a list of the last ``num`` headers created.
        List of metadatastore.document.Document objects
        **NOTE**: DOES NOT RETURN THE EVENTS.
    """
    rs_objects = [rs for rs in RunStart.objects.order_by('-time')[:num]]
    return [__as_document(rs) for rs in rs_objects]


def __todatetime(time_stamp):
    if isinstance(time_stamp, float):
        return datetime.datetime.fromtimestamp(time_stamp)
    else:
        raise TypeError('Timestamp format is not correct!')


def __replace_dict_keys(input_dict, src, dst):
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


def __src_dst(direction):
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


def __replace_descriptor_data_key_dots(ev_desc, direction='in'):
    """Replace the '.' with [dot]

    Parameters
    ---------

    event_descriptor: metadatastore.odm_templates.EventDescriptor
    EvenDescriptor instance

    direction: str
    If 'in' ->  replace . with [dot]
    If 'out' -> replace [dot] with .

    """
    src, dst = __src_dst(direction)
    ev_desc.data_keys = __replace_dict_keys(ev_desc.data_keys,
                                            src, dst)
    return ev_desc


def __replace_event_data_key_dots(event, direction='in'):
    """Replace the '.' with [dot]

    Parameters
    ---------

    event_descriptor: metadatastore.database.event_descriptor.EventDescriptor
    EvenDescriptor instance

    direction: str
    If 'in' ->  replace . with [dot]
    If 'out' -> replace [dot] with .

    """
    src, dst = __src_dst(direction)
    event.data = __replace_dict_keys(event.data,
                                     src, dst)
    return event
