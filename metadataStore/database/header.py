__author__ = 'arkilic'

from mongoengine import Document
from metadataStore.database.event_descriptor import EventDescriptor
from metadataStore.database.beamline_config import BeamlineConfig
from mongoengine import DateTimeField, StringField, DictField, IntField, FloatField, ReferenceField, DENY
import time
from getpass import getuser
from datetime import datetime


class Header(Document):
    """

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


    beamline_id: str
    Beamline String identifier. Not unique, just an indicator of beamline code for multiple beamline systems

    status: str
    Provides an information regarding header. Choice: In Progress/Complete

    custom: dict
    Additional parameters that data acquisition code/user wants to append to a given header. Name/value pairs

    """
    default_time_stamp = time.time()

    unique_id = StringField(required=True, unique=True)

    event_descriptor = ReferenceField(EventDescriptor,reverse_delete_rule=DENY, required=True,
                                      db_field='descriptor_id')

    beamline_config = ReferenceField(BeamlineConfig, reverse_delete_rule=DENY, required=True,
                                     db_field='beamline_config_id')

    scan_id = IntField(required=True, unique=False)

    owner = StringField(default=getuser(), required=True, unique=False)

    create_time = FloatField(default=default_time_stamp, required=True)

    datetime_create_time = DateTimeField(default=datetime.fromtimestamp(default_time_stamp),
                                         required=True)

    beamline_id = StringField(max_length=20, unique=False)

    custom = DictField(unique=False, required=False)

    meta = {'indexes': ['-scan_id', '-_id', '-owner', '-create_time']}
