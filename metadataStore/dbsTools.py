__author__ = 'arkilic'
    #TODO: Check mongoengine save goodies
    #TODO: Check validatio=True flag functionality
from metadataStore.database.header import Header
from metadataStore.database.beamline_config import BeamlineConfig
from metadataStore.database.event_descriptor import EventDescriptor
from metadataStore.database.event import Event
import datetime
from metadataStore.conf import host, port, database
from mongoengine import connect
import time


def save_header(scan_id, owner, start_time, end_time, **kwargs):
    """

    :param scan_id:
    :param owner:
    :param start_time:
    :param end_time:
    :param kwargs:
    :return:
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
    """

    :param header_id:
    :param config_params:
    :return:
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


def save_event_descriptor(header, event_type_id, data_keys, **kwargs):
    """

    :param header:
    :param event_type_id:
    :param data_keys:
    :param kwargs:
    :return:
    """
    try:
        connect(db=database, host=host, port=port)
    except:
        raise

    event_descriptor = EventDescriptor(header_id=header.id, event_type_id=event_type_id, data_keys=data_keys)

    for key, value in kwargs.iteritems():
        if key is 'descriptor_name':
            event_descriptor.descriptor_name = value
        elif key is 'type_descriptor':
            event_descriptor.type_descriptor = value
        else:
            raise KeyError('Invalid argument..: ', key)

    try:
        event_descriptor.save(validate=True, write_concern={"w": 1})
    except:
        raise

    return event_descriptor


def save_event(header, event_descriptor, seq_no, data=None, **kwargs):
    """

    :param header:
    :param event_descriptor:
    :param seq_no:
    :param kwargs:
    :return:
    """
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

import random

h = save_header(scan_id=random.randint(0,1000), owner='arkilic', start_time=time.time(),
                end_time=time.time(), custom={'data':123})
print h.id

b = save_beamline_config(header=h)

ed = save_event_descriptor(header=h, event_type_id=1, data_keys=['arman'])

print Event(header_id=h.id, descriptor_id=ed.id, seq_no=1).save().id