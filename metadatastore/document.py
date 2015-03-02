import six
import mongoengine
from mongoengine.base.datastructures import BaseDict, BaseList
from mongoengine.base.document import BaseDocument
from bson.objectid import ObjectId
from datetime import datetime
from itertools import chain
from collections import MutableMapping


def _normalize(in_val):
    """
    Helper function for cleaning up the mongoegine documents to be safe.

    Converts Mongoengine.Document to mds.Document objects recursively

    Converts:

     -  mongoengine.base.datastructures.BaseDict -> dict
     -  mongoengine.base.datastructures.BaseList -> list
     -  ObjectID -> str

    Parameters
    ----------
    in_val : object
        Object to be sanitized

    Returns
    -------
    ret : object
        The 'sanitized' object

    """
    if isinstance(in_val, BaseDocument):
        return Document(in_val)
    elif isinstance(in_val, BaseDict):
        return {_normalize(k): _normalize(v) for k, v in six.iteritems(in_val)}
    elif isinstance(in_val, BaseList):
        return [_normalize(v) for v in in_val]
    elif isinstance(in_val, ObjectId):
        return str(in_val)
    return in_val


class Document(MutableMapping):
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
        self._fields = set()
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

    def __setattr__(self, k, v):
        self.__dict__[k] = v
        if not k.startswith('_'):
            self._fields.add(k)

    def __delattr__(self, k):
        del self.__dict__[k]
        if not k.startswith('_'):
            self._fields.remove(k)

    def __repr__(self):
        return "<{0} Document>".format(self._name)

    def __iter__(self):
        return iter(self._fields)

    def __getitem__(self, key):
        try:
            return getattr(self, key)
        except AttributeError:
            raise KeyError()

    def __delitem__(self, key):
        delattr(self, key)

    def __setitem__(self, key, val):
        setattr(self, key, val)

    def __len__(self):
        return len(self._fields)

    def __contains__(self, key):
        return key in self._fields
