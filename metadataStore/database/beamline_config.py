__author__ = 'arkilic'

from mongoengine import DictField, ReferenceField, Document, DENY
from metadataStore.database.header import Header


class BeamlineConfig(Document):
    """

    """
    header_id = ReferenceField(Header, required=True, reverse_delete_rule=DENY)
    config_params = DictField(required=False, unique=False)
    meta = {'indexes': [('-header_id', '-_id')]}