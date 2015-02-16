from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import six
from .odm_templates import (RunStart, BeamlineConfig, RunEnd,
                            EventDescriptor, Event, DataKey)
from .document import Document
import datetime
import logging
import metadataStore
from mongoengine import connect
import uuid


logger = logging.getLogger(__name__)


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
    flavor of the week metadataStore's spec says. This insulates ophyd from
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


def db_connect(func):
    def inner(*args, **kwargs):
        db = metadataStore.conf.mds_config['database']
        host = metadataStore.conf.mds_config['host']
        port = metadataStore.conf.mds_config['port']
        logger.debug('connecting to db: %s, host: %s, port: %s',
                     db, host, port)
        connect(db=db, host=host, port=port)
        return func(*args, **kwargs)
    return inner

####################### DATABASE INSERTION ###############################

@db_connect
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
    beamline_config: metadataStore.odm_temples.BeamlineConfig, optional
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


@db_connect
def insert_run_end(run_start, time, exit_status='success',
                   reason=None, uid=None):
    """ Provide an end to a sequence of events. Exit point for an
    experiment's run.

    Parameters
    ----------
    run_start : metadataStore.odm_temples.RunStart
        Foreign key to corresponding RunStart
    time : timestamp
        The date/time as found at the client side when an event is
        created.
    reason : str, optional
        provides information regarding the run success. 20 characters max

    Returns
    -------
    run_start : mongoengine.Document
        Inserted mongoengine object
    """
    if uid is None:
        uid = str(uuid.uuid4())
    end_run = RunEnd(run_start=run_start, reason=reason,
                            time=time, time_as_datetime=__todatetime(time),
                            uid=uid, exit_status=exit_status)

    end_run.save(validate=True, write_concern={"w": 1})

    return end_run


@db_connect
def insert_beamline_config(config_params, time, uid=None):
    """ Create a beamline_config  in metadataStore database backend

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


@db_connect
def insert_event_descriptor(run_start, data_keys, time, uid=None):
    """ Create an event_descriptor in metadataStore database backend

    Parameters
    ----------
    run_start: metadataStore.odm_templates.RunStart
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


@db_connect
def insert_event(event_descriptor, time, data, seq_num, uid=None):
    """Create an event in metadataStore database backend

    Parameters
    ----------
    event_descriptor : metadataStore.odm_templates.EventDescriptor
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

    # TODO: seq_no is not optional according to opyhd folks. To be discussed!!
    # talk to @dchabot & @swilkins
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


########################## DATABASE RETRIEVAL ##################################

def __add_event_descriptors(run_start_list):
    for run_start in run_start_list:
        setattr(run_start, 'event_descriptors',
                find_event_descriptor(run_start))

def __as_document(mongoengine_object):
    return Document(mongoengine_object)


@db_connect
def find_run_start(limit=50, **kwargs):
    """ Given search criteria, locate the RunStart object

    Parameters
    ----------
    limit : int
        Number of header objects to be returned

    Other Parameters
    ----------------
    scan_id : int
        Scan identifier. Not unique
    owner : str
        User name identifier associated with a scan
    create_time : dict
        header insert time. Keys must be start and end to
        give a range to the search
    beamline_id : str
        String identifier for a specific beamline
    unique_id : str
        Hashed unique identifier

    Returns
    -------
    br_objects : metadataStore.document.Document
        Combined documents: RunStart, BeamlineConfig, Sample and
        EventDescriptors

    Usage
    ------
    >>> find_run_start(scan_id=123)
    >>> find_run_start(owner='arkilic')
    >>> find_run_start(time={'start': 1421176750.514707,
    ...                      'end': time.time()})
    >>> find_run_start(time={'start': 1421176750.514707,
    ...                      'end': time.time()})

    >>> find_run_start(owner='arkilic', time={'start': 1421176750.514707,
    ...                                       'end': time.time()})

    """
    search_dict = dict()

    try:
        search_dict['scan_id'] = kwargs.pop('scan_id')
    except KeyError:
        pass

    try:
        search_dict['unique_id'] = kwargs.pop('unique_id')
    except KeyError:
        pass

    try:
        search_dict['owner'] = kwargs.pop('owner')
    except KeyError:
        pass

    try:
        search_dict['beamline_id'] = kwargs.pop('beamline_id')
    except KeyError:
        pass

    try:
        time_dict = kwargs.pop('time')
        if not isinstance(time_dict, dict):
            raise ValueError('Wrong format. time must include '
                             'start and end keys for range. Must be a dict')
        else:
            if 'start' in time_dict and 'end' in time_dict:
                search_dict['time'] = {'$gte': time_dict['start'],
                                       '$lte': time_dict['end']}
            else:
                raise ValueError('time must include start '
                                 'and end keys for range search')
    except KeyError:
        pass

    if search_dict:
        br_objects = RunStart.objects(
            __raw__=search_dict).order_by('-_id')[:limit]
    else:
        br_objects = list()

    __add_event_descriptors(br_objects)

    return [__as_document(bre) for bre in br_objects]

@db_connect
def find_beamline_config(_id):
    """Return beamline config objects given a unique mongo _id

    Parameters
    ----------
    _id: bson.ObjectId

    Returns
    -------
    beamline_config : metadataStore.document.Document
        The beamline config object
    """
    beamline_config = BeamlineConfig.objects(id=_id).order_by('-_id')
    return __as_document(beamline_config)

@db_connect
def find_event_descriptor(run_start):
    """Return beamline config objects given a unique mongo id

    Parameters
    ----------
    run_start : bson.ObjectId

    Returns
    -------
    event_descriptor : list
        List of metadataStore.document.Document.
    """
    event_descriptor_list = list()
    for event_descriptor in EventDescriptor.objects\
                    (run_start=run_start.id).order_by('-_id'):
        event_descriptor = __replace_descriptor_data_key_dots(event_descriptor,
                                                              direction='out')
        event_descriptor_list.append(event_descriptor)
    return [__as_document(evd) for evd in event_descriptor_list]

@db_connect
def fetch_events(limit=1000, **kwargs):
    """

    Parameters
    -----------
    limit : int
        number of events returned
    time : dict, optional
        time of the event. dict keys must be start and end
    descriptor : mongoengine.Document, optional
        event descriptor object

    Returns
    -------
    events : list
        List of metadataStore.document.Document
    """
    search_dict = dict()
    try:
        time_dict = kwargs.pop('time')
        if not isinstance(time_dict, dict):
            raise ValueError('Wrong format. time must include '
                             'start and end keys for range. Must be a dict')
        else:
            if 'start' in time_dict and 'end' in time_dict:
                search_dict['time'] = {'$gte': time_dict['start'],
                                       '$lte': time_dict['end']}
            else:
                raise ValueError('time must include start '
                                 'and end keys for range search')
    except KeyError:
        pass

    try:
        desc = kwargs.pop('descriptor')
        search_dict['descriptor_id'] = desc.id
    except KeyError:
        pass
    result = Event.objects(__raw__=search_dict).order_by('-_id')[:limit]
    return [__as_document(res) for res in result]

@db_connect
def find_event(run_start):
    """Returns a set of events given a RunStart object

    Parameters
    ---------
    run_start: mongoengine.Document
        RunStart object that events possibly fall under
    limit: int
        Number of headers to be returned

    Returns
    -------
    events: list
        Set of events encapsulated within a RunStart's scope.
        metadataStore.document.Document
    """
    descriptors = EventDescriptor.objects(run_start=run_start.id)
    descriptors = descriptors.order_by('-_id')
    events = [find_event_given_descriptor(descriptor)
              for descriptor in descriptors]
    return events

@db_connect
def find_event_given_descriptor(event_descriptor):
    """Return all Event(s) associated with an EventDescriptor

    Parameters
    ----------
    event_descriptor: metadataStore.database.EventDescriptor
        EventDescriptor instance that a set of events point back to

    Returns
    -------
    event_list : list
        Nested list. top level is each event descriptor. next level is events
        [[ev0, ev1, ...], [ev0, ev1, ...]]
    """
    event_list = list()
    for event in Event.objects(
            descriptor=event_descriptor.id).order_by('-_id'):
        event = __replace_event_data_key_dots(event, direction='out')
        event_list.append(event)

    return [__as_document(ev) for ev in event_list]

@db_connect
def find(data=True, limit=50, **kwargs):
    """
    Returns dictionary of objects
    Headers keyed on unique scan_id in header_scan_id format
    data flag is set to True by default since users
    intuitively expect data back

    Parameters
    ---------

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
    result: metadataStore.document.Document
        Returns RunStart objects
        One can access events for this given RunStart as:
        run_start_object.events
    """
    raise NotImplementedError()
    br_objects = find_run_start(limit, **kwargs)

    if data:
        result = list
        if br_objects:
            for br in br_objects:
                br.events = find_event(br)
                result.append(br)
    return br_objects

@db_connect
def find_last(num=1):
    """Indexed on ObjectId NOT end_time.

    Returns the last created header not modified!!

    Returns
    -------
    run_start: list
        Returns a list of the last ``num`` headers created.
        List of metadataStore.document.Document objects
        **NOTE**: DOES NOT RETURN THE EVENTS.
    """
    br_objects = [br_obj for br_obj in RunStart.objects.order_by('-_id')[0:num]]
    __add_event_descriptors(br_objects)
    return [__as_document(br) for br in br_objects]


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

    event_descriptor: metadataStore.odm_templates.EventDescriptor
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

    event_descriptor: metadataStore.database.event_descriptor.EventDescriptor
    EvenDescriptor instance

    direction: str
    If 'in' ->  replace . with [dot]
    If 'out' -> replace [dot] with .

    """
    src, dst = __src_dst(direction)
    event.data = __replace_dict_keys(event.data,
                                     src, dst)
    return event
