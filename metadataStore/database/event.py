from bson.json_util import default

__author__ = 'arkilic'

from metadataStore.database.header import Header
from metadataStore.database.event_descriptor import EventDescriptor
from mongoengine import DENY, Document
from mongoengine import ReferenceField, IntField, StringField, DictField, FloatField, DateTimeField
import getpass
import time
from datetime import datetime


class Event(Document):
    """

    """

    default_timestamp = time.time()
    header_id = ReferenceField(Header,reverse_delete_rule=DENY, required=True,
                               db_field='header_id')

    descriptor_id = ReferenceField(EventDescriptor,reverse_delete_rule=DENY,
                                      required=True, db_field='descriptor_id')

    seq_no = IntField(min_value=0, required=True)

    owner = StringField(max_length=10, required=True, default=getpass.getuser())

    description = StringField(max_length=20, required=False)

    data = DictField(required=False)

    timestamp = FloatField(required=True, default=default_timestamp)

    datetime_timestamp = DateTimeField(required=True,
                                       default= datetime.fromtimestamp(default_timestamp))
    meta = {'indexes': ['-header_id', '-descriptor_id', '-_id', '-timestamp']}