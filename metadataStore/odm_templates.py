__author__ = 'arkilic'

__author__ = 'arkilic'

from mongoengine import Document
from mongoengine import DateTimeField, StringField, DictField, IntField, FloatField, ReferenceField, DENY
import time
from getpass import getuser
from datetime import datetime


class BeamlineConfig(Document):
    """

    """
    config_params = DictField(required=False, unique=False)
    meta = {'indexes': ['-_id']}


class BeginRunEvent(Document):
    """

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
    default_time_stamp = time.time()

    start_time = FloatField(default=default_time_stamp, required=True)

    datetime_start_time = DateTimeField(default=datetime.fromtimestamp(default_time_stamp),
                                        required=True)

    beamline_id = StringField(max_length=20, unique=False)

    scan_id = IntField(required=False, unique=False)

    beamline_config = ReferenceField(BeamlineConfig, reverse_delete_rule=DENY, required=False,
                                     db_field='beamline_config_id')

    owner = StringField(default=getuser(), required=True, unique=False)

    custom = DictField(unique=False, required=False) #Keeping this a dict for the time being instead of new collection

    meta = {'indexes': ['-_id', '-owner', '-start_time']}


class EndRunEvent(Document):
    """
    begin_run_event: bson.ObjectId
        Foreign key to corresponding BeginRunEvent

    stop_time: float
        Unix timestamp for end_run_event

    datetime_stop_time: datetime
        datetime timestamp for end_run_event. Auto-convert from stop_time

    end_run_reason: str
        provides information regarding the run success.

    """
    begin_run_event = ReferenceField(BeginRunEvent, reverse_delete_rule=DENY, required=False,
                                     db_field='begin_run_id')

    default_time_stamp = time.time()

    stop_time = FloatField(default=default_time_stamp, required=True)

    datetime_stop_time = DateTimeField(default=datetime.fromtimestamp(default_time_stamp),
                                       required=True)

    end_run_reason = StringField(max_length=10, required=False)

    custom = DictField(required=False)


class EventDescriptor(Document):
    """

    """
    event_type_id = IntField(required=True)
    data_keys = DictField(required=True)
    descriptor_name = StringField(max_length=10, required=False, unique=False)
    type_descriptor = DictField(required=False)
    meta = {'indexes': [('-event_type_id', '-descriptor_name')]}


class Event(Document):
    """

    """

    default_timestamp = time.time()
    header = ReferenceField(BeginRunEvent,reverse_delete_rule=DENY, required=True,
                            db_field='begin_run_id')

    descriptor = ReferenceField(EventDescriptor,reverse_delete_rule=DENY,
                                      required=True, db_field='descriptor_id')

    seq_no = IntField(min_value=0, required=True)

    owner = StringField(max_length=10, required=True, default=getpass.getuser())

    description = StringField(max_length=20, required=False)

    data = DictField(required=False)

    event_timestamp = FloatField(required=True, default=default_timestamp)

    datetime_timestamp = DateTimeField(required=True,
                                       default= datetime.fromtimestamp(default_timestamp))

    beamline_id = StringField(required=True)

    meta = {'indexes': ['-header', '-descriptor', '-_id', '-event_timestamp']}
