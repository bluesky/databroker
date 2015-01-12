__author__ = 'arkilic'

from metadataStore.database.header import Header
from metadataStore.database.beamline_config import BeamlineConfig
from metadataStore.database.event_descriptor import EventDescriptor
from metadataStore.database.event import Event
import datetime
from metadataStore.conf import host, port, database
from mongoengine import connect


def save_header(scan_id, owner, start_time, end_time, **kwargs):
    """Create a header in metadataStore database backend

    Parameters
    ----------
    scan_id : int
    Unique scan identifier visible to the user and data analysis

    owner: str
    Specifies the unix user credentials of the user creating the entry

    start_time: time
    Start time of series of events that are recorded by the header

    end_time: time
    End time of series of events that are recorded by the header


    kwargs
    -----------

    beamline_id: str
    Beamline String identifier. Not unique, just an indicator of beamline code for multiple beamline systems

    status: str
    Provides an information regarding header. Choice: In Progress/Complete

    custom: dict
    Additional parameters that data acquisition code/user wants to append to a given header. Name/value pairs

    """
    try:
        connect(db=database, host=host, port=port)
    except:
        raise

    datetime_start_time = __convert2datetime(start_time)
    datetime_end_time = __convert2datetime(end_time)

    header = Header(scan_id=scan_id, owner=owner, start_time=start_time, end_time=end_time,
                    datetime_start_time=datetime_start_time,
                    datetime_end_time=datetime_end_time)

    for key, value in kwargs.iteritems():
        if key is 'beamline_id':
            header.beamline_id = value
        elif key is 'status':
            header.status = value
        elif key is 'custom':
            header.custom = value
        else:
            raise KeyError('Invalid argument..: ', key)

    try:
        header.save(validate=True, write_concern={"w": 1})
    except:
        raise

    return header


def save_beamline_config(header, config_params=None):
    """ Create a beamline_config  in metadataStore database backend

    Parameters
    ----------
    header: mongoengine.Document
    Header object that specific beamline_config entry is going to point(foreign key)

    config_params: dict
    Name/value pairs that indicate beamline configuration parameters during capturing of

    """
    try:
        connect(db=database, host=host, port=port)
    except:
        raise
    beamline_config = BeamlineConfig(header_id=header.id, config_params=config_params)
    try:
        beamline_config.save(validate=True, write_concern={"w": 1})
    except:
        raise

    return beamline_config


def save_event_descriptor(header, event_type_id, descriptor_name, data_keys, **kwargs):
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
    #TODO: replace . with [dot] in and out of the database

    try:
        connect(db=database, host=host, port=port)
    except:
        raise

    event_descriptor = EventDescriptor(header_id=header.id, event_type_id=event_type_id, data_keys=data_keys,
                                       descriptor_name=descriptor_name)

    for key, value in kwargs.iteritems():
        if key is 'type_descriptor':
            event_descriptor.type_descriptor = value
        else:
            raise KeyError('Invalid argument..: ', key)

    try:
        event_descriptor.save(validate=True, write_concern={"w": 1})
    except:
        raise

    return event_descriptor


def save_event(header, event_descriptor, seq_no, data=None, **kwargs):
    #TODO: replace . with [dot] in and out of the database

    try:
        connect(db=database, host=host, port=port)
    except:
        raise

    event = Event(header_id=header.id, descriptor_id=event_descriptor.id, seq_no=seq_no,
                  data=data)

    for key, value in kwargs.iteritems():
        if key is 'owner':
            event.owner = value
        elif key is 'description':
            event.description = value
        else:
            raise KeyError('Invalid key...:', key)

    try:
        event.save(validate=True, write_concern={"w": 1})
    except:
        raise

    return event


def find():
    pass


def find2():
    pass


def find_last():
    pass

def __convert2datetime(time_stamp):
    if isinstance(time_stamp, float):
        return datetime.datetime.fromtimestamp(time_stamp)
    else:
        raise TypeError('Timestamp format is not correct!')
