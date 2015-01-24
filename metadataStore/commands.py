__author__ = 'arkilic'

from metadataStore.database.header import Header
from metadataStore.database.beamline_config import BeamlineConfig
from metadataStore.database.event_descriptor import EventDescriptor
from metadataStore.database.event import Event
import datetime
from metadataStore.conf import host, port, database
from mongoengine import connect
import time
#TODO: Add logger


def save_header(event_descriptor, beamline_config, unique_id, scan_id, create_time, **kwargs):
    """Create a header in metadataStore database backend

    Parameters
    ----------
    scan_id : int
    Unique scan identifier visible to the user and data analysis

    start_time: time
    Start time of series of events that are recorded by the header

    end_time: time
    End time of series of events that are recorded by the header


    kwargs
    -----------

    owner: str
    Specifies the unix user credentials of the user creating the entry

    beamline_id: str
    Beamline String identifier. Not unique, just an indicator of beamline code for multiple beamline systems

    status: str
    Provides an information regarding header. Choice: In Progress/Complete

    custom: dict
    Additional parameters that data acquisition code/user wants to append to a given header. Name/value pairs

    """

    connect(db=database, host=host, port=port)

    datetime_create_time = __convert2datetime(create_time)

    header = Header(event_descriptor=event_descriptor.id, beamline_config=beamline_config.id, scan_id=scan_id,
                    create_time=create_time, datetime_create_time=datetime_create_time, unique_id=unique_id)

    header.owner = kwargs.pop('owner', None)

    header.beamline_id = kwargs.pop('beamline_id', None)

    header.custom = kwargs.pop('custom', None)

    if kwargs:
        raise KeyError('Invalid argument(s)..: ', kwargs.keys())

    header.save(validate=True, write_concern={"w": 1})

    return header


def save_beamline_config(config_params=None):
    """ Create a beamline_config  in metadataStore database backend

    Parameters
    ----------
    header: mongoengine.Document
    Header object that specific beamline_config entry is going to point(foreign key)

    config_params: dict
    Name/value pairs that indicate beamline configuration parameters during capturing of

    """

    connect(db=database, host=host, port=port)

    beamline_config = BeamlineConfig(config_params=config_params)
    beamline_config.save(validate=True, write_concern={"w": 1})

    return beamline_config


def save_event_descriptor(event_type_id, descriptor_name, data_keys, **kwargs):
    """ Create an event_descriptor in metadataStore database backend

    Parameters
    ----------

    header: mongoengine.Document
    Header object that specific beamline_config entry is going to point(foreign key)

    event_type_id:int
    Integer identifier for a scan, sweep, etc.

    data_keys: list
    Provides information about keys of the data dictionary in an event will contain

    descriptor_name: str
    Unique identifier string for an event. e.g. ascan, dscan, hscan, home, sweep,etc.

    kwargs
    ----------
    type_descriptor:dict
    Additional name/value pairs can be added to an event_descriptor using this flexible field

    """
    connect(db=database, host=host, port=port)

    event_descriptor = EventDescriptor(event_type_id=event_type_id, data_keys=data_keys,
                                       descriptor_name=descriptor_name)

    event_descriptor.type_descriptor = kwargs.pop('type_descriptor', None)

    event_descriptor = __replace_descriptor_data_key_dots(event_descriptor, direction='in')

    if kwargs:
        raise KeyError('Invalid argument(s)..: ', kwargs.keys())

    event_descriptor.save(validate=True, write_concern={"w": 1})

    return event_descriptor


def save_event(header, event_descriptor, seq_no, timestamp=None, data=None, **kwargs):
    """Create an event in metadataStore database backend

    Parameters
    ----------

    header: mongoengine.Document
    Header object that specific event entry is going to point(foreign key)

    event_descriptor: mongoengine.Document
    EventDescriptor object that specific event entry is going to point(foreign key)

    seq_no:int
    Unique sequence number for the event. Provides order of an event in the group of events

    data:dict
    Dictionary that contains the name value fields for the data associated with an event

    kwargs
    ----------

    owner: str
    Specifies the unix user credentials of the user creating the entry

    description: str
    Text description of specific event

    """
    connect(db=database, host=host, port=port)

    event = Event(header=header.id, descriptor_id=event_descriptor.id, seq_no=seq_no, timestamp=timestamp,
                  data=data)

    event.owner = kwargs.pop('owner', None)

    event.description = kwargs.pop('description', None)
    event.datetime_timestamp = kwargs.pop('datetime_timestamp', None)

    event = __replace_event_data_key_dots(event, direction='in')

    if kwargs:
        raise KeyError('Invalid argument(s)..: ', kwargs.keys())

    event.save(validate=True, write_concern={"w": 1})

    return event


def find_header(limit=50, **kwargs):
    """
    Parameters
    ----------

    limit:
    kwargs:

    Usage

    >>> find_header(scan_id=123)
    >>> find_header(owner='arkilic')
    >>> find_header(start_time={'start': 1421176750.514707,
    ...                          'end': time.time()})
    >>> find_header(end_time={'start': 1421176750.514707,
    ...                          'end': time.time()})

    >>> find_header(owner='arkilic', start_time={'start': 1421176750.514707,
    ...                                          'end': time.time()})

    """
    connect(db=database, host=host, port=port)
    #TODO: Add unique id to the search
    search_dict = dict()

    #Do not want to pop if not in kwargs. Otherwise, breaks the mongo query
    try:
        search_dict['scan_id'] = kwargs.pop('scan_id')
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
            raise TypeError('Wrong format. create_time must include start and end keys for range. Must be a dict')
        else:
            if 'start' in st_time_dict.keys():
                if 'end' in st_time_dict.keys():
                    search_dict['create_time'] = {'$gte': st_time_dict['start'], '$lte': st_time_dict['end']}
                else:
                    raise AttributeError('create_time must include start and end keys for range search')
            else:
                raise AttributeError('create_time  must include start and end keys for range search')
    except KeyError:
        pass

    if search_dict:
        header_objects = Header.objects(__raw__=search_dict).order_by('-_id')[:limit]
    else:
        header_objects = list()
    return header_objects


def find_beamline_config(_id):
    connect(db=database, host=host, port=port)
    return BeamlineConfig.objects(id=_id).order_by('-_id')


def find_event_descriptor(_id):
    event_descriptor_list = list()
    connect(db=database, host=host, port=port)
    for event_descriptor in EventDescriptor.objects(id=_id).order_by('-_id'):
        event_descriptor = __replace_descriptor_data_key_dots(event_descriptor, direction='out')
        event_descriptor_list.append(event_descriptor)
    return event_descriptor_list


def find_event(header):
    #TODO: Other search parameters for events?
    connect(db=database, host=host, port=port)
    event_list = list()
    for event in Event.objects(header=header.id).order_by('-_id'):
        event = __replace_event_data_key_dots(event,direction='out')
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
    for event in Event.objects(descriptor=event_descriptor.id).order_by('-_id'):
        event = __replace_event_data_key_dots(event,direction='out')
        event_list.append(event)

    return event_list


def find(data=True, limit=50, **kwargs):
    """
    Returns dictionary of objects
    Headers keyed on unique scan_id in header_scan_id format
    data flag is set to True by default since users intuitively expect data back

    Parameters
    ---------

    scan_id: int

    owner: str

    beamline_id: str

    status: str

    start_time: dict
    start_time={'start': float, 'end': float}

    end_time: dict
    end_time={'start': float, 'end': float}

    """
    header_objects = find_header(limit, **kwargs)

    if data:
        beamline_config_objects = dict()
        event_descriptor_objects = dict()
        event_objects = dict()
        #Queryset instance returned by mongoengine not iterable, hence manual recursion
        if header_objects:
            for header in header_objects:
                event_objects[header.id] = find_event(header)
    return header_objects, event_objects


def find_last():
    """Indexed on ObjectId NOT end_time. Returns the last created header not modified!!

    Returns
    -------

    header: metadataStore.database.header.Header
        Returns the last header created


    """
    connect(db=database, host=host, port=port)

    return Header.objects.order_by('-_id')[0:1][0]

def __convert2datetime(time_stamp):
    if isinstance(time_stamp, float):
        return datetime.datetime.fromtimestamp(time_stamp)
    else:
        raise TypeError('Timestamp format is not correct!')


def __replace_descriptor_data_key_dots(event_descriptor, direction='in'):
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
    modified_data_keys = list()
    if direction is 'in':
        for data_key in event_descriptor.data_keys:
            if '.' in data_key:
                modified_data_keys.append(data_key.replace('.', '[dot]'))
            else:
                modified_data_keys.append(data_key)
        event_descriptor.data_keys = modified_data_keys
    elif direction is 'out':
        for data_key in event_descriptor.data_keys:
            if '[dot]' in data_key:
                modified_data_keys.append(data_key.replace('[dot]', '.'))
            else:
                modified_data_keys.append(data_key)
        event_descriptor.data_keys = modified_data_keys
    else:
        raise ValueError('Only in/out allowed as direction params')
    return event_descriptor

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
    modified_data_dict = dict()

    if direction is 'in':
        for key, value in event.data.iteritems():
            if '.' in key:
                modified_data_dict[key.replace('.', '[dot]')] = value
            else:
                modified_data_dict[key] = value
        event.data = modified_data_dict

    elif direction is 'out':
        for key, value in event.data.iteritems():
            if '[dot]' in key:
                modified_data_dict[key.replace('[dot]', '.')] = value
            else:
                modified_data_dict[key] = value
        event.data = modified_data_dict
    else:
        raise ValueError('Only in/out allowed as direction params')
    return event