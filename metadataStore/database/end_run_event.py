__author__ = 'arkilic'

from mongoengine import Document
from mongoengine import DateTimeField, StringField, DictField, IntField, FloatField, ReferenceField, DENY
from metadataStore.database.begin_run_event import BeginRunEvent
import time
import datetime

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