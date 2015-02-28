import six
import mongoengine
from mongoengine.base.datastructures import BaseDict, BaseList
from bson.objectid import ObjectId
from datetime import datetime
from itertools import chain


def _normalize(in_val):
    """
    Helper function for cleaning up the mongoegine documents to be safe.

    Converts Mongoengine.Document to mds.Document objects recursively

    Converts:

     -  mongoengine.base.datastructures.BaseDict -> dict
     -  mongoengine.base.datastructures.BaseList -> list

    Parameters
    ----------
    in_val : object
        Object to be sanitized

    Returns
    -------
    ret : object
        The 'sanitized' object

    """
    if isinstance(in_val, mongoengine.Document):
        return Document(in_val)
    elif isinstance(in_val, BaseDict):
        return {_normalize(k): _normalize(v) for k, v in six.iteritems(in_val)}
    elif isinstance(in_val, BaseList):
        return [_normalize(v) for v in in_val]
    elif isinstance(in_val, ObjectId):
        return str(in_val)
    return in_val


class Document(object):
    """
    Copy the data out of a mongoengine.Document, including nested Documents,
    but do not copy any of the mongo-specific methods or attributes.
    """
    def __init__(self, mongo_document):
        """
        Parameters
        ----------
        mongo_document : mongoengine.Document
        """
        self._name = mongo_document.__class__.__name__
        fields = set(chain(mongo_document._fields.keys(),
                           mongo_document._data.keys()))
        for field in fields:
            attr = getattr(mongo_document, field)

            attr = _normalize(attr)

            setattr(self, field, attr)
            # For debugging, add a human-friendly time_as_datetime attribute.
            if hasattr(self, 'time'):
                self.time_as_datetime = datetime.fromtimestamp(self.time)

    def __repr__(self):
        return "<{0} Document>".format(self._name)
