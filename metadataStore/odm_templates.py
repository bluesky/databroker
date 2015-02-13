__author__ = 'arkilic'

from mongoengine import Document, DynamicDocument, DynamicEmbeddedDocument
from mongoengine import (DateTimeField, StringField, DictField, IntField, FloatField, ListField,
                         ReferenceField, EmbeddedDocumentField, DENY, MapField)
from getpass import getuser


class BeamlineConfig(DynamicDocument):
    """
    Attributes
    ----------
    config_params: dict
        Custom configuration parameters for a given run. Avoid using '.' in field names.
        If you're interested in doing so, let me know @arkilic
        This has a one-to-many relationship with BeginRunEvent documents
    """
    config_params = DictField(required=False, unique=False)
    uid = StringField(required=True, unique=True)
    time = FloatField(required=True)
    meta = {'indexes': ['-_id']}


class BeginRunEvent(DynamicDocument):
    """ Provide a head for a sequence of events. Entry point for
    an experiment's run. BeamlineConfig is NOT required to create a BeginRunEvent
    The only prereq is an EventDescriptor that identifies the nature of event that is
    starting and

    Attributes
    ----------
    uid: str
        Globally unique id for this run
    time : timestamp
        The date/time as found at the client side when an begin_run_event is
        created.
    beamline_id : str
        Beamline String identifier. Not unique, just an indicator of
        beamline code for multiple beamline systems
    project : str, optional
        Name of project that this run is part of
    owner : str
        Specifies the unix user credentials of the user creating the entry
    group : str
        Unix group to associate this data with
    scan_id : int
         scan identifier visible to the user and data analysis
    beamline_config: bson.ObjectId
        Foreign key to beamline config corresponding to a given run
    sample : dict
        Information about the sample, may be a UID to another collection
    """
    uid = StringField(required=True, unique=True)
    time = FloatField(required=True)
    project = StringField(required=False)
    beamline_id = StringField(max_length=20, unique=False, required=True)
    scan_id = IntField(required=True)
    beamline_config = ReferenceField(BeamlineConfig, reverse_delete_rule=DENY,
                                     required=False,
                                     db_field='beamline_config_id')
    owner = StringField(default=getuser(), required=True, unique=False)
    group = StringField(required=False, unique=False, default=None)
    sample = DictField(required=False)  # lightweight sample placeholder.
    meta = {'indexes': ['-_id', '-owner', '-time', '-scan_id', '-uid']}


class EndRunEvent(DynamicDocument):
    """Indicates the end of a series of events

    Attributes
    ----------
    begin_run_event : bson.ObjectId
        Foreign key to corresponding BeginRunEvent
    exit_status : {'success', 'fail', 'abort'}
        provides information regarding the run success.
    time : float
        The date/time as found at the client side when an event is
        created.
    reason : str, optional
        Long-form description of why the run ended.
        20 char max.
    """
    time = FloatField(required=True)
    begin_run_event = ReferenceField(BeginRunEvent, reverse_delete_rule=DENY,
                                     required=True, db_field='begin_run_id')

    exit_status = StringField(max_length=10, required=False, default='success',
                              choices=('success', 'abort', 'fail'))
    reason = StringField(required=False)
    meta = {'indexes': ['-_id', '-time', '-exit_status', '-begin_run_event']}
    uid = StringField(required=True, unique=True)


class DataKey(DynamicEmbeddedDocument):
    """Describes the objects in the data property of Event documents.
    Be aware that this is DynamicEmbeddedDoc Append fields rather than
    custom dict field

    Attributes
    ----------
    dtype : str
        The type of data in the event
    shape : list
        The shape of the data.  None and empty list mean scalar data.
    source : str
        The source (ex piece of hardware) of the data.
    external : str, optional
        Where the data is stored if it is stored external to the events.
    """
    dtype = StringField(required=True,
                        choices=('integer', 'number', 'array',
                                 'boolean', 'string'))
    shape = ListField(field=IntField())  # defaults to empty list
    source = StringField(required=True)
    external = StringField(required=False)


class EventDescriptor(DynamicDocument):
    """ Describes the objects in the data property of Event documents

    Attributes
    ----------
    begin_run_event : str
        Globally unique ID to the begin_run document this descriptor is associtaed with.
    uid : str
        Globally unique ID for this event descriptor.
    time : float
        Creation time of the document as unix epoch time.
    data_keys : mongoengine.DynamicEmbeddedDocument
        Describes the objects in the data property of Event documents
    """
    begin_run_event = ReferenceField(BeginRunEvent, reverse_delete_rule=DENY,
                                     required=True, db_field='begin_run_id')
    uid = StringField(required=True, unique=True)
    time = FloatField(required=True)
    data_keys = MapField(EmbeddedDocumentField(DataKey), required=True)
    meta = {'indexes': ['-begin_run_event', '-time']}


class Event(Document):
    """ Stores the experimental data. All events point to
    BeginRunEvent, in other words, to BeginRunEvent serves as an
    entry point for all events. Within each event, one can access
    both BeginRunEvent and EventDescriptor. Make sure an event does
    not exceed 16 MB in size. This is essential since this tool is
    geared for metadata only. If more storage is required, please
    use fileStore.

    Attributes
    ----------
    descriptor : mongoengine.DynamicDocument
        EventDescriptor foreignkey
    uid : str
        Globally unique identifier for this Event
    seq_num: int
        Sequence number to identify the location of this Event in the Event stream
    data: dict
        Dict of lists that contain e.g. data = {'motor1': [value, timestamp]}
    time : float
        The event time.  This maybe different than the timestamps on each of the data entries

    """
    descriptor = ReferenceField(EventDescriptor,reverse_delete_rule=DENY,
                                required=True, db_field='descriptor_id')
    uid = StringField(required=True, unique=True)
    seq_num = IntField(min_value=0, required=True)
    # TODO validate on this better
    data = DictField(required=True)
    time = FloatField(required=True)
    time_as_datetime = DateTimeField(required=False)
    meta = {'indexes': ['-descriptor', '-_id']}
