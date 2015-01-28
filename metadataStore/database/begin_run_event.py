__author__ = 'arkilic'

from mongoengine import Document
from metadataStore.database.beamline_config import BeamlineConfig
from mongoengine import DateTimeField, StringField, DictField, IntField, FloatField, ReferenceField, DENY
import time
from getpass import getuser
from datetime import datetime


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
