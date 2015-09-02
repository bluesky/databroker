from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import tornado.web
from bson import json_util
import itertools 
import six
import datetime
from bson.objectid import ObjectId
__author__ = 'arkilic'


def _unpack_params(handler):
    """Unpacks the queries from the body of the header
    Parameters
    ----------
    handler: tornado.web.RequestHandler
        Handler for incoming request to collection

    Returns: dict
    -------
        Unpacked query in dict format.
    """
    if not isinstance(handler, tornado.web.RequestHandler):
        raise TypeError('Cannot unpack the query params. Handler required')
    query = json_util.loads(list(handler.request.arguments.keys())[0])
    return query


def _normalize_object_id(kwargs, key):
    """Ensure that an id is an ObjectId, not a string.

    ..warning: Does in-place mutation of the document
    """
    try:
        kwargs[key] = ObjectId(kwargs[key])
    except KeyError:
        # This key wasn't used by the query; that's fine.
        pass
    except TypeError:
        # This key was given a more complex query.
        pass
    # Database errors will still raise.