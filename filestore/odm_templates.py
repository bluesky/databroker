from __future__ import absolute_import, division, print_function

from mongoengine import (Document, FloatField, DateTimeField, StringField,
                         DictField, ReferenceField, IntField, BooleanField,
                         DENY)

import time

ALIAS = 'fs'

class Resource(Document):
    """

    Parameters
    ----------

    spec : str
        File spec used to primarily parse the contents into
        analysis environment

    file_path : str
        Url to the physical location of the file

    custom : dict
        custom name/value container in case additional info save is required

    """

    spec = StringField(max_length=10, required=True, unique=False)
    file_path = StringField(max_length=100, required=True, unique=False)
    custom = DictField(required=False)
    meta = {'indexes': ['-file_path', '-_id'], 'db_alias': ALIAS}


class ResoureAttributes(Document):
    """

    Parameters
    ----------


    """
    resource = ReferenceField(Resource, reverse_delete_rule=DENY,
                              required=True,
                              db_field='resource_id')
    shape = StringField(unique=False, required=True)
    dtype = StringField(unique=False, required=True)
    total_bytes = IntField(min_value=0, required=False, default=0)
    hashed_data = StringField(required=False)
    last_access = FloatField(required=False, default=time.time())
    datetime_last_access = DateTimeField(required=False)
    in_use = BooleanField(required=False, default=False)
    custom_attributes = DictField(required=False)
    meta = {'indexes': ['-_id', '-shape', '-dtype'], 'db_alias': ALIAS}

# TODO: add documentation


class Nugget(Document):
    """Correlation lookup table between events and files.
    Primarily for dataBroker logic

    Parameters
    ----------
    file_id : filestore.resource.Resource
        Resource object required to create an event link.
        id field is used to obtain the foreignkey

    event_id : str
        metadataStore unqiue event identifier in string format.

    link_parameters : dict
        custom dictionary required for appending name/value pairs as desired
    """
    resource = ReferenceField(Resource,
                              reverse_delete_rule=DENY,
                              required=True)
    event_id = StringField(required=True, unique=True)
    link_parameters = DictField(required=False)
    meta = {'indexes': ['-_id', '-event_id', '-resource'], 'db_alias': ALIAS}
