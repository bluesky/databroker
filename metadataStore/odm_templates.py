__author__ = 'arkilic'

from mongoengine import Document, DynamicDocument,EmbeddedDocument, DynamicEmbeddedDocument
from mongoengine import (DateTimeField, StringField, DictField, IntField, FloatField, ListField,
                         ReferenceField, EmbeddedDocumentField, DynamicField, DENY)
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
        The date/time as found at the client side when an event is
        created.
    beamline_id: str
        Beamline String identifier. Not unique, just an indicator of
        beamline code for multiple beamline systems
    owner: str, optional
        Specifies the unix user credentials of the user creating the entry
    scan_id : int, optional
         scan identifier visible to the user and data analysis
    beamline_config: bson.ObjectId, optional
        Foreign key to beamline config corresponding to a given run
    custom: dict, optional
        Additional parameters that data acquisition code/user wants to
        append to a given header. Name/value pairs
    """
    uid = StringField(required=True)
    time = FloatField(required=True)
    time_as_datetime = DateTimeField()
    beamline_id = StringField(max_length=20, unique=False, required=True)
    scan_id = StringField(required=False, unique=False)
    beamline_config = ReferenceField(BeamlineConfig, reverse_delete_rule=DENY,
                                     required=False,
                                     db_field='beamline_config_id')
    # one-to-many relationship constructed between BeginRunEvent and BeamlineConfig
    owner = StringField(default=getuser(), required=True, unique=False)
    custom = DictField(unique=False, required=False, default=dict())
    # Keeping custom a dict for the time being instead of new collection

    meta = {'indexes': ['-_id', '-owner', '-time']}


class EndRunEvent(DynamicDocument):
    """Indicates the end of a series of events

    Attributes
    ----------
    begin_run_event : bson.ObjectId
        Foreign key to corresponding BeginRunEvent
    reason : str
        provides information regarding the run success.
    time : timestamp
        The date/time as found at the client side when an event is
        created.
    """
    time = FloatField(required=True)
    time_as_datetime = DateTimeField()
    begin_run_event = ReferenceField(BeginRunEvent, reverse_delete_rule=DENY,
                                     required=True, db_field='begin_run_id')

    reason = StringField(max_length=10, required=False, default='Complete')
    custom = DictField(required=False)
    meta = {'indexes': ['-_id', '-time', '-reason', '-begin_run_event']}


class DataKeys(DynamicEmbeddedDocument):
    """Describes the objects in the data property of Event documents

    """
    # I like the idea of DataKeys being an embedded document since it is one to one relationship with event_descriptors
    # and mongoengine documentation suggests there are quite a few benefits of this data model
    # http://docs.mongodb.org/manual/core/data-model-design/#data-modeling-embedding
    dtype = StringField(required=True)
    shape = ListField(required=True)
    source = StringField(required=True)
    external = StringField()


class EventDescriptor(DynamicDocument):
    """ Describes the objects in the data property of Event documents
    Attributes
    ----------
    Gotta update this!!!! Incoming...

    """
    begin_run_event = ReferenceField(BeginRunEvent, reverse_delete_rule=DENY,
                                     required=True, db_field='begin_run_id')
    uid = StringField(required=True)
    keys = DictField(required=True)
    time = FloatField(required=True)
    time_as_datetime = DateTimeField()
    data_keys = EmbeddedDocumentField(DataKeys, required=True)
    meta = {'indexes': ['-begin_run_event', '-time']}


class Event(DynamicDocument):
    """ Stores the experimental data. All events point to
    BeginRunEvent, in other words, to BeginRunEvent serves as an
    entry point for all events. Within each event, one can access
    both BeginRunEvent and EventDescriptor. Make sure an event does
    not exceed 16 MB in size. This is essential since this tool is
    geared for metadata only. If more storage is required, please
    use fileStore.

    Attributes
    ----------
    Update after refactor! Incoming...

    """
    descriptor = ReferenceField(EventDescriptor,reverse_delete_rule=DENY,
                                required=True, db_field='descriptor_id')
    seq_num = IntField(min_value=0, required=True)
    data = DictField(ListField,required=True)
    time = FloatField(required=True)
    time_as_datetime = DateTimeField(required=False)
    meta = {'indexes': ['-descriptor', '-_id', '-time']}
