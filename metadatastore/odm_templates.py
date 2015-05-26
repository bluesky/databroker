"""
ODM templates for use with metadatstore
"""
from mongoengine import Document, DynamicDocument, DynamicEmbeddedDocument
from mongoengine import (StringField, DictField, IntField, FloatField,
                         ListField, ReferenceField, EmbeddedDocumentField,
                         DENY, MapField)
from getpass import getuser

ALIAS = 'mds'


__all__ = ['BeamlineConfig', 'RunStart', 'RunStop', 'DataKey',
           'EventDescriptor', 'Event']


class BeamlineConfig(DynamicDocument):
    """
    Attributes
    ----------
    config_params: dict
        Custom configuration parameters for a given run. Avoid using '.'
        in field names.
        If you're interested in doing so, let me know @arkilic
        This has a one-to-many relationship with RunStart documents
    """
    config_params = DictField(required=False, unique=False)
    uid = StringField(required=True, unique=True)
    time = FloatField(required=True)
    meta = {'indexes': ['-_id', '-uid'], 'db_alias': ALIAS}


class RunStart(DynamicDocument):
    """ Provide a head for a sequence of events. Entry point for
    an experiment's run. BeamlineConfig is NOT required to create a RunStart
    The only prereq is an EventDescriptor that identifies the nature of
    event that is starting and

    Attributes
    ----------
    uid: str
        Globally unique id for this run
    time : timestamp
        The date/time as found at the client side when an run_start is
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
                                     required=True,
                                     db_field='beamline_config_id')
    owner = StringField(default=getuser(), required=True, unique=False)
    group = StringField(required=False, unique=False, default=None)
    sample = DictField(required=False)  # lightweight sample placeholder.
    meta = {'indexes': ['-_id', '-owner', '-time', '-scan_id', '-uid'],
            'db_alias': ALIAS}


class RunStop(DynamicDocument):
    """Indicates the end of a series of events

    Attributes
    ----------
    run_start : bson.ObjectId
        Foreign key to corresponding RunStart
    exit_status : {'success', 'fail', 'abort'}
        indicating reason run stopped
    reason : str
        more detailed exit status (stack trace, user remark, etc.)
    time : float
        The date/time as found at the client side when an event is
        created.
    uid : str
        Globally unique id string provided to metadatastore
    reason : str, optional
        Long-form description of why the run was terminated.
    """
    time = FloatField(required=True)
    run_start = ReferenceField(RunStart, reverse_delete_rule=DENY,
                               required=True, db_field='run_start_id')

    exit_status = StringField(max_length=10, required=False, default='success',
                              choices=('success', 'abort', 'fail'))
    reason = StringField(required=False)
    uid = StringField(required=True, unique=True)
    meta = {'indexes': ['-_id', '-time', '-exit_status', '-run_start', '-uid'],
            'db_alias': ALIAS}


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
    meta = {'db_alias': ALIAS}


class EventDescriptor(DynamicDocument):
    """ Describes the objects in the data property of Event documents

    Attributes
    ----------
    run_start : str
        Globally unique ID to the run_start document this descriptor is associated with.
    uid : str
        Globally unique ID for this event descriptor.
    time : float
        Creation time of the document as unix epoch time.
    data_keys : mongoengine.DynamicEmbeddedDocument
        Describes the objects in the data property of Event documents
    """
    run_start = ReferenceField(RunStart, reverse_delete_rule=DENY,
                               required=True, db_field='run_start_id')
    uid = StringField(required=True, unique=True)
    time = FloatField(required=True)
    data_keys = MapField(EmbeddedDocumentField(DataKey), required=True)
    meta = {'indexes': ['-run_start', '-time', '-uid'], 'db_alias': ALIAS}


class Event(Document):
    """ Stores the experimental data. All events point to
    RunStart, in other words, to RunStart serves as an
    entry point for all events. Within each event, one can access
    both RunStart and EventDescriptor. Make sure an event does
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
        Sequence number to identify the location of this Event in
        the Event stream
    data: dict
        Dict of lists that contain e.g. data = {'motor1': [value, timestamp]}
    time : float
        The event time.  This maybe different than the timestamps
        on each of the data entries

    """
    descriptor = ReferenceField(EventDescriptor, reverse_delete_rule=DENY,
                                required=True, db_field='descriptor_id')
    uid = StringField(required=True, unique=True)
    seq_num = IntField(min_value=0, required=True)
    # TODO validate on this better
    data = DictField(required=True)
    time = FloatField(required=True)
    meta = {'indexes': ['-descriptor', '-_id', '-uid'], 'db_alias': ALIAS}
