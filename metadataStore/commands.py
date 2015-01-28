from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six


__author__ = 'arkilic'

from metadataStore.odm_templates import BeginRunEvent, BeamlineConfig, EndRunEvent, EventDescriptor, Event
import datetime
from metadataStore.conf import host, port, database
from mongoengine import connect
import time


def insert_begin_run(start_time, beamline_id, beamline_config=None,**kwargs):
    """ Provide a head for a sequence of events. Entry point for an experiment's run.

    Parameters
    ----------

    scan_id : int
        Unique scan identifier visible to the user and data analysis

    owner: str
        Specifies the unix user credentials of the user creating the entry

    start_time: time
        Start time of series of events in unix timestamp format

    datetime_start_time: datetime
        Start time of series of events in datetime format. Auto convert from unix

    beamline_id: str
        Beamline String identifier. Not unique, just an indicator of beamline code for multiple beamline systems

    beamline_config: bson.ObjectId
        Foreign key to beamline config corresponding to a given run

    custom: dict
        Additional parameters that data acquisition code/user wants to append to a given header. Name/value pairs

    """
    connect(db=database, host=host, port=port)

    datetime_start_time = __convert2datetime(start_time)
    begin_run = BeginRunEvent(start_time=start_time, datetime_start_time=datetime_start_time,
                              beamline_id=beamline_id)

    if beamline_config is not None:
        begin_run.beamline_config = beamline_config.id

    kwargs.pop('scan_id', None)
    kwargs.pop('owner', None)
    kwargs.pop('custom', None)

    #If anyone has a better way to do this, let me know. 3 pops seems to yield to cleanest code.

    if kwargs:
        raise KeyError('Invalid argument(s)..: ', kwargs.keys())

    begin_run.save(validate=True, write_concern={"w": 1})

    return begin_run


def save_beamline_config(config_params=None):
    """ Create a beamline_config  in metadataStore database backend

    Parameters
    ----------
    config_params : dict
        Name/value pairs that indicate beamline configuration
        parameters during capturing of data

    Returns
    -------
    blc : BeamlineConfig
        The document added to the collection
    """

    connect(db=database, host=host, port=port)

    beamline_config = BeamlineConfig(config_params=config_params)
    beamline_config.save(validate=True, write_concern={"w": 1})

    return beamline_config


def save_event_descriptor(begin_run_event, event_type_id, descriptor_name, data_keys, **kwargs):
    """ Create an event_descriptor in metadataStore database backend

    Parameters
    ----------

    event_type_id : int
        Integer identifier for a scan, sweep, etc.

    data_keys : dict
        Provides information about keys of the data dictionary in
        an event will contain

    descriptor_name : str
        An identifier string for an event. e.g. ascan, dscan,
        hscan, home, sweep,etc.

    Other Parameters
    ----------------
    type_descriptor : dict, optional
        Additional name/value pairs can be added to an
        event_descriptor using this flexible field

    Returns
    -------
    ev_desc : EventDescriptor
        The document added to the collection.

    """
    connect(db=database, host=host, port=port)

    event_descriptor = EventDescriptor(event_type_id=event_type_id,
                                       data_keys=data_keys,
                                       descriptor_name=descriptor_name)

    event_descriptor.type_descriptor = kwargs.pop('type_descriptor', None)

    event_descriptor = __replace_descriptor_data_key_dots(event_descriptor,
                                                          direction='in')

    if kwargs:
        raise KeyError('Invalid argument(s)..: ', kwargs.keys())

    event_descriptor.save(validate=True, write_concern={"w": 1})

    return event_descriptor


def save_event(begin_run_event, event_descriptor, beamline_id, seq_no,
               timestamp=None, data=None, **kwargs):
    """Create an event in metadataStore database backend

    Parameters
    ----------

    header : mongoengine.Document
        Header object that specific event entry is going to point(foreign key)

    event_descriptor : mongoengine.Document
        EventDescriptor object that specific event entry is going
        to point(foreign key)

    seq_no : int
        Unique sequence number for the event. Provides order of an event in
        the group of events

    data : dict
        Dictionary that contains the name value fields for the data associated
        with an event

    Other Parameters
    ----------------

    owner: str
    Specifies the unix user credentials of the user creating the entry

    description: str
    Text description of specific event

    """
    connect(db=database, host=host, port=port)

    event = Event(header=begin_run_event.id, descriptor_id=event_descriptor.id,
                  seq_no=seq_no, timestamp=timestamp,
                  beamline_id=beamline_id, data=data)

    event.owner = kwargs.pop('owner', None)

    event.description = kwargs.pop('description', None)
    event.datetime_timestamp = kwargs.pop('datetime_timestamp', None)

    event = __replace_event_data_key_dots(event, direction='in')

    if kwargs:
        raise KeyError('Invalid argument(s)..: ', kwargs.keys())

    event.save(validate=True, write_concern={"w": 1})

    return event


def find_begin_run(limit=50, **kwargs):
    """
    Parameters
    ----------

    limit: int
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

    Usage

    >>> find_header(scan_id=123)
    >>> find_header(owner='arkilic')
    >>> find_header(create_time={'start': 1421176750.514707,
    ...                          'end': time.time()})
    >>> find_header(create_time={'start': 1421176750.514707,
    ...                          'end': time.time()})

    >>> find_header(owner='arkilic', create_time={'start': 1421176750.514707,
    ...                                          'end': time.time()})

    """
    connect(db=database, host=host, port=port)
    search_dict = dict()

    # Do not want to pop if not in kwargs. Otherwise, breaks the mongo query
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
        st_time_dict = kwargs.pop('create_time')
        if not isinstance(st_time_dict, dict):
            raise ValueError('Wrong format. create_time must include '
                             'start and end keys for range. Must be a dict')
        else:
            if 'start' in st_time_dict and 'end' in st_time_dict:
                search_dict['create_time'] = {'$gte': st_time_dict['start'],
                                                  '$lte': st_time_dict['end']}
            else:
                raise ValueError('create_time must include start '
                                     'and end keys for range search')
    except KeyError:
        pass

    if search_dict:
        header_objects = BeginRunEvent.objects(
               __raw__=search_dict).order_by('-_id')[:limit]
    else:
        header_objects = list()
    return header_objects


def find_beamline_config(_id):
    """Return beamline config objects given a unique mongo id

    Parameters
    ----------
    _id: bson.ObjectId

    """
    connect(db=database, host=host, port=port)
    return BeamlineConfig.objects(id=_id).order_by('-_id')


def find_event_descriptor(_id):
    """Return beamline config objects given a unique mongo id

    Parameters
    ----------
    _id: bson.ObjectId

    """
    event_descriptor_list = list()
    connect(db=database, host=host, port=port)
    for event_descriptor in EventDescriptor.objects(id=_id).order_by('-_id'):
        event_descriptor = __replace_descriptor_data_key_dots(event_descriptor,
                                                              direction='out')
        event_descriptor_list.append(event_descriptor)
    return event_descriptor_list


def find_event(header):
    # TODO: Other search parameters for events?
    connect(db=database, host=host, port=port)
    event_list = list()
    for event in Event.objects(header=header.id).order_by('-_id'):
        event = __replace_event_data_key_dots(event, direction='out')
        event_list.append(event)
    return event_list


def find_event_given_descriptor(event_descriptor):
    """Return all Event(s) associated with an EventDescriptor

    Parameters
    ----------

    event_descriptor: metadataStore.database.EventDescriptor
    EventDescriptor instance

    """
    connect(db=database, host=host, port=port)
    event_list = list()
    for event in Event.objects(
            descriptor=event_descriptor.id).order_by('-_id'):
        event = __replace_event_data_key_dots(event, direction='out')
        event_list.append(event)

    return event_list


def find(data=True, limit=50, **kwargs):
    """
    Returns dictionary of objects
    Headers keyed on unique scan_id in header_scan_id format
    data flag is set to True by default since users
    intuitively expect data back

    Parameters
    ---------

    scan_id: int

    owner: str

    beamline_id: str

    status: str

    create_time: dict
    create_time={'start': float, 'end': float}

    """
    header_objects = find_begin_run(limit, **kwargs)

    if data:
        beamline_config_objects = dict()
        event_descriptor_objects = dict()
        event_objects = dict()
        # Queryset instance returned by mongoengine not iterable,
        # hence manual recursion
        if header_objects:
            for header in header_objects:
                event_objects[header.id] = find_event(header)
    return header_objects, event_objects


def find_last():
    """Indexed on ObjectId NOT end_time.

    Returns the last created header not modified!!

    Returns
    -------

    header: metadataStore.database.header.Header
        Returns the last header created. DOES NOT RETURN THE EVENTS.


    """
    connect(db=database, host=host, port=port)

    return BeginRunEvent.objects.order_by('-_id')[0:1][0]


def search_events_broker(beamline_id, start_time, end_time):
    """Return a set of events given

    Parameters
    ----------

    beamline_id: str
        string identifier for a beamline and its sections

    start_time: float
        Event time stamp range start time

    end_time: float
        Event time stamp range end time

    """
    event_query_dict = dict()
    event_query_dict['beamline_id'] = beamline_id
    event_query_dict['event_timestamp'] = {'$gte': start_time,
                                           '$lte': end_time}

    result = Event.objects(__raw__=event_query_dict).order_by('-_id')
    # I did not set any limits to the query because we might
    # return quite a lot of events.
    return result


def __convert2datetime(time_stamp):
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
    I know the name is long. Bite me, it is private routine and I have an IDE

    Parameters
    ---------

    event_descriptor: metadataStore.database.event_descriptor.EventDescriptor
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
    I know the name is long. Bite me, it is private routine and I have an IDE

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
