__author__ = 'arkilic'

from metadataStore.database.header import Header
from metadataStore.database.event_descriptor import EventDescriptor
from mongoengine import DENY, Document
from mongoengine import ReferenceField, IntField, StringField, DictField
import getpass

class Event(Document):
    """

    """
    header_id = ReferenceField(Header,reverse_delete_rule=DENY, required=True,
                               db_field='header_id')

    descriptor_id = ReferenceField(EventDescriptor,reverse_delete_rule=DENY,
                                      required=True, db_field='descriptor_id')

    seq_no = IntField(min_value=0, required=True)

    owner = StringField(max_length=10, required=True, default=getpass.getuser())

    description = StringField(max_length=20, required=False)

    data = DictField(required=False)

    meta = {'indexes': [('-header_id', '-descriptor_id', '_id')]}