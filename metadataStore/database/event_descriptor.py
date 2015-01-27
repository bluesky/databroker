__author__ = 'arkilic'

from mongoengine import Document, DENY, StringField
from mongoengine import ListField, DictField, IntField


class EventDescriptor(Document):
    """

    """
    event_type_id = IntField(required=True)
    data_keys = DictField(required=True)
    descriptor_name = StringField(max_length=10, required=False, unique=False)
    type_descriptor = DictField(required=False)
    meta = {'indexes': [('-event_type_id','-descriptor_name')]}
