__author__ = 'arkilic'

from mongoengine import Document
from mongoengine import DateTimeField, StringField, DictField, IntField, FloatField
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
    #TODO: Per discussion with Stuart delete end_time and modify start_time to creation_time

    default_time_stamp = time.time()

    scan_id = IntField(required=True, unique=True)

    owner = StringField(default=getuser(), required=True, unique=False)

    start_time = FloatField(default=default_time_stamp, required=True)

    end_time = FloatField(default=default_time_stamp, required=True)

    datetime_start_time = DateTimeField(default=datetime.fromtimestamp(default_time_stamp),
                                        required=True)

    datetime_end_time = DateTimeField(default=datetime.fromtimestamp(default_time_stamp),
                                      required=True)

    beamline_id = StringField(max_length=20, unique=False)

    status = StringField(choices=('In progress', 'Complete'), unique=False)

    custom = DictField(unique=False, required=False)

    meta = {'indexes': ['-scan_id', '-_id', '-owner', '-start_time', '-end_time']}
