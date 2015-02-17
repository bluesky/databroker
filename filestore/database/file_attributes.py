__author__ = 'arkilic'

from mongoengine import (Document, FloatField, DateTimeField, StringField, DictField, ReferenceField, \
                         IntField, BooleanField, DENY)
from filestore.database.file_base import FileBase
import time
from datetime import datetime


class FileAttributes(Document):
    """

    Parameters
    ----------


    """
    file_base = ReferenceField(FileBase, reverse_delete_rule=DENY, required=True,
                               db_field='file_base_id')
    shape = StringField(unique=False, required=True)
    dtype = StringField(unique=False, required=True)
    total_bytes = IntField(min_value=0, required=False, default=0)
    hashed_data = StringField(required=False)
    last_access = FloatField(required=False, default=time.time())
    datetime_last_access = DateTimeField(required=False)
    in_use = BooleanField(required=False, default=False)
    custom_attributes = DictField(required=False)
    collection_version = FloatField(required=False, min_value=0)
    meta = {'indexes': ['-_id', '-shape', '-dtype']}
#TODO: add documentation
