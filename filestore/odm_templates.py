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

    resource_path : str
        Url to the physical location of the file

    resource_kwargs : dict
        resource_kwargs name/value container in case additional info save is required

    """

    spec = StringField(max_length=10, required=True, unique=False)
    resource_path = StringField(max_length=100, required=True, unique=False)
    resource_kwargs = DictField(required=False)
    meta = {'indexes': ['-resource_path', '-_id'], 'db_alias': ALIAS}


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


class Datum(Document):
    """Correlation lookup table between events and files.
    Primarily for dataBroker logic

    Parameters
    ----------

    resource : Resource or Resource.id
        Resource object

    datum_id : str
        Unique identifier for this datum.  This is the value stored in
        metadatastore and is the value passed to `retrieve_datum` to get
        the data back out.

    datum_kwargs : dict
        resource_kwargs dictionary required for appending name/value pairs as desired
    """
    resource = ReferenceField(Resource,
                              reverse_delete_rule=DENY,
                              required=True)
    datum_id = StringField(required=True, unique=True)
    datum_kwargs = DictField(required=False)
    meta = {'indexes': ['-_id', '-datum_id', '-resource'], 'db_alias': ALIAS}
