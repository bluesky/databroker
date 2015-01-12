__author__ = 'arkilic'


from mongoengine import Document, DENY, StringField
from mongoengine import ListField, DictField, IntField, ReferenceField
from metadataStore.database.header import Header


class EventDescriptor(Document):
    """

    """

    header_id = ReferenceField(Header,reverse_delete_rule=DENY, required=True,
                            db_field='header_id')
    event_type_id = IntField(required=True)
    data_keys = ListField(required=True)
    descriptor_name = StringField(max_length=10, required=False, unique=True)
    type_descriptor = DictField(required=False)
    meta = {'indexes': [('-header_id', '-descriptor_name')]}



