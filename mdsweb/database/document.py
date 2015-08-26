import copy
import operator
import numbers
from collections import Hashable
from functools import partial

import motor
from bson import json_util, ObjectId
from bson.dbref import DBRef
from bson.son import SON

__all__ = ('BaseDocument')

class BaseDocument(object):
    _dynamic = False
    _created = True

    def __init__(self, *args, **values):
        """
        Initialise a document or embedded document

        :param __auto_convert: Try and will cast python objects to Object types
        :param values: A dictionary of values for the document
        """
        if args:
            # Combine positional arguments with named arguments.
            # We only want named arguments.
            field = iter(self._fields_ordered)
            # If its an automatic id field then skip to the first defined field
            if self._auto_id_field:
                next(field)
            for value in args:
                name = next(field)
                if name in values:
                    raise TypeError("Multiple values for keyword argument '" + name + "'")
                values[name] = value
        __auto_convert = values.pop("__auto_convert", True)
        self._data = {}
        self._dynamic_fields = SON()

         # Assign default values to instance
        for key, field in self._fields.iteritems():
            if self._db_field_map.get(key, key) in values:
                continue
            value = getattr(self, key, None)
            setattr(self, key, value)