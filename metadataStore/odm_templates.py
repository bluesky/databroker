__author__ = 'arkilic'

from mongoengine import Document, DENY
from mongoengine import DateTimeField, StringField, DictField, IntField, FloatField, ReferenceField
from getpass import getuser


class BeamlineConfig(Document):
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


class BeginRunEvent(Document):
    """ Provide a head for a sequence of events. Entry point for
    an experiment's run. BeamlineConfig is NOT required to create a BeginRunEvent
    The only prereq is an EventDescriptor that identifies the nature of event that is
    starting and

    Attributes
    ----------
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


class EndRunEvent(Document):
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


class EventDescriptor(Document):
    """ Provides information regarding the upcoming series of events
    whose contents are indicated via such EventDescriptor.

    Attributes
    ----------
    begin_run_event: metadataStore.odm_templates.BeginRunEvent
        BeginRunEvent object created prior to a BeginRunEvent
    data_keys : dict
        e.g.
        {'key_name' : {'source' : 'PV', 'external' : 'FILESTORE'}}

    """
    begin_run_event = ReferenceField(BeginRunEvent, reverse_delete_rule=DENY,
                                     required=True, db_field='begin_run_id')
    data_keys = DictField(required=True)
    event_type = StringField(required=False)
    time = FloatField(required=True)
    time_as_datetime = DateTimeField()
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
    descriptor : metadataStore.odm_templates.EventDescriptor
        Foreign key to EventDescriptor that provides info regarding a
        set of events of the same type

    data : dict
        Data dictionary where experimental data is stored.

    time : timestamp
        The date/time as found at the client side when an event is
        created.

    seq_no : int, optional
        Sequence number pointing out the
    """
    descriptor = ReferenceField(EventDescriptor,reverse_delete_rule=DENY,
                                required=True, db_field='descriptor_id')
    seq_no = IntField(min_value=0, required=True)
    data = DictField(required=True)
    time = FloatField(required=True)
    time_as_datetime = DateTimeField(required=False)
    meta = {'indexes': ['-descriptor', '-_id', '-time']}
