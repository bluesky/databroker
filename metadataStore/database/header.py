__author__ = 'arkilic'

from mongoengine import Document
from mongoengine import DateTimeField, StringField, DictField, IntField, FloatField
import time
from getpass import getuser
from datetime import datetime


class Header(Document):
    """

    """
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

    meta = {'indexes': [('-scan_id', '_id', '-owner', '-start_time', '-end_time')]}
