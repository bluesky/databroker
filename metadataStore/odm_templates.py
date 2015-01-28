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
    """ Provide a head for a sequence of events. Entry point for an experiment's run.

    Attributes
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
                                     db_field='beamline_config_id') #one to many relationship constructed

    owner = StringField(default=getuser(), required=True, unique=False)

    custom = DictField(unique=False, required=False) #Keeping this a dict for the time being instead of new collection

    meta = {'indexes': ['-_id', '-owner', '-start_time']}


class EndRunEvent(Document):
    """Indicates the end of a series of events

    Attributes
    ----------

    begin_run_event: bson.ObjectId
        Foreign key to corresponding BeginRunEvent

    stop_time: float
        Unix timestamp for end_run_event

    datetime_stop_time: datetime
        datetime timestamp for end_run_event. Auto-convert from stop_time

    end_run_reason: str
        provides information regarding the run success.

    """
    begin_run_event = ReferenceField(BeginRunEvent, reverse_delete_rule=DENY, required=True,
                                     db_field='begin_run_id') #foreign key to beginning of event sequence

    default_time_stamp = time.time()

    stop_time = FloatField(default=default_time_stamp, required=True)

    datetime_stop_time = DateTimeField(default=datetime.fromtimestamp(default_time_stamp),
                                       required=True)
    end_run_reason = StringField(max_length=10, required=False)
    custom = DictField(required=False)
    meta = {'indexes': ['-_id', '-stop_time', '-end_run_reason', 'begin_run_event']}


class EventDescriptor(Document):
    """ Provides information regarding the upcoming series of events

    Attributes
    ----------

    event_type_id: int
        Required integer identifier for an event type

    begin_run_event: metadataStore.odm_templates.BeginRunEvent
        BeginRunEvent object created prior to a BeginRunEvent
    """
    event_type_id = IntField(required=True)

    begin_run_event = ReferenceField(BeginRunEvent, reverse_delete_rule=DENY, required=True,
                                     db_field='begin_run_id')

    data_keys = DictField(required=True)
    descriptor_name = StringField(max_length=10, required=False, unique=False)
    type_descriptor = DictField(required=False)
    meta = {'indexes': [('-event_type_id', '-begin_run_event')]}


class Event(Document):
    """ Stores the experimental data. All events point to BeginRunEvent, in other words, to BeginRunEvent serves as an
    entry point for all events. Within each event, one can access both BeginRunEvent and EventDescriptor. Make sure an
    event does not exceed 16 MB in size. This is difficult since this tool is geared for metadata only. If more storage
    is required, please use fileStore.

    Attributes
    ----------

    begin_run: metadataStore.odm_templates.BeginRunEvent
        BeginRunEvent object used to refer back from an event to the head of various events

    descriptor: metadataStore.odm_templates.EventDescriptor
        Foreign key to EventDescriptor that provides info regarding a set of events of the same type

    seq_no: int
        Sequence number pointing out the

    owner: str
        Unix user info regarding event creation

    description: str
        Text description for human friendly notes regarding an event

    data: dict
        Data dictionary where experimental data is stored.


    """

    default_timestamp = time.time()
    begin_run = ReferenceField(BeginRunEvent,reverse_delete_rule=DENY, required=True,
                            db_field='begin_run_id')

    descriptor = ReferenceField(EventDescriptor,reverse_delete_rule=DENY,
                                      required=True, db_field='descriptor_id')

    seq_no = IntField(min_value=0, required=True)

    owner = StringField(max_length=10, required=True, default=getuser())

    description = StringField(max_length=20, required=False)

    data = DictField(required=False)

    event_timestamp = FloatField(required=True, default=default_timestamp)

    datetime_timestamp = DateTimeField(required=True,
                                       default= datetime.fromtimestamp(default_timestamp))

    beamline_id = StringField(required=True)

    meta = {'indexes': ['-begin_run', '-descriptor', '-_id', '-event_timestamp']}
