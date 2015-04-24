import six
import mongoengine
from mongoengine.base.datastructures import BaseDict, BaseList
from mongoengine.base.document import BaseDocument
from bson.objectid import ObjectId
from bson.dbref import DBRef
from datetime import datetime
from itertools import chain
from collections import MutableMapping


def _normalize(in_val, cache):
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

    cache : dict-like
        Cache of already seen objects in the DB so that we do not
        have to de-reference and build them again.

    Returns
    -------
    ret : object
        The 'sanitized' object

    """
    if isinstance(in_val, BaseDocument):
        return Document.from_mongo(in_val, cache)
    elif isinstance(in_val, BaseDict):
        return {_normalize(k, cache): _normalize(v, cache)
                for k, v in six.iteritems(in_val)}
    elif isinstance(in_val, BaseList):
        return [_normalize(v, cache) for v in in_val]
    elif isinstance(in_val, ObjectId):
        return str(in_val)
    return in_val


class Document(MutableMapping):
    """A dictionary where d.key is the same as d['key']
    and attributes/keys beginning with '_' are skipped
    in iteration."""

    def __init__(self):
        self._fields = set()

    def __setattr__(self, k, v):
        self.__dict__[k] = v
        if not k.startswith('_'):
            self._fields.add(k)
        assert hasattr(self, k)
        assert k in self.__dict__

    def __delattr__(self, k):
        del self.__dict__[k]
        if not k.startswith('_'):
            self._fields.remove(k)
        assert k not in self._fields

    def __iter__(self):
        return iter(self._fields)

    def __getitem__(self, key):
        try:
            return getattr(self, key)
        except AttributeError:
            raise KeyError(key)

    def __delitem__(self, key):
        delattr(self, key)

    def __setitem__(self, key, val):
        setattr(self, key, val)

    def __len__(self):
        return len(self._fields)

    def __contains__(self, key):
        return key in self._fields

    @classmethod
    def from_mongo(cls, mongo_document, cache=None):
        """
        Copy the data out of a mongoengine.Document, including nested
        Documents, but do not copy any of the mongo-specific methods or
        attributes.

        Parameters
        ----------
        mongo_document : mongoengine.Document


        cache : dict-like, optional
            Cache of already seen objects in the DB so that we do not
            have to de-reference and build them again.

        """
        if cache is None:
            cache = dict()
        document = Document()
        document._name = mongo_document.__class__.__name__
        fields = set(chain(mongo_document._fields.keys(),
                           mongo_document._data.keys()))

        for field in fields:
            attr = getattr(mongo_document, field)
            if isinstance(attr, DBRef):
                oid = attr.id
                try:
                    attr = cache[oid]
                except KeyError:
                    # do de-reference
                    mongo_document.select_related()
                    # grab the attribute again
                    attr = getattr(mongo_document, field)
                    # normalize it
                    attr = _normalize(attr, cache)
                    # and stash for later use
                    cache[oid] = attr
            else:
                attr = _normalize(attr, cache)

            document[field] = attr
        # For debugging, add a human-friendly time_as_datetime attribute.
        if 'time' in document:
            document.time_as_datetime = datetime.fromtimestamp(
                    document.time)
        return document

    def __repr__(self):
        return "<{0} Document>".format(self._name)
