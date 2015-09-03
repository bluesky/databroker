from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import tornado.web
import ujson
import datetime
import six
from bson.objectid import ObjectId
__author__ = 'arkilic'


def _verify_handler(handler):
    if isinstance(handler, tornado.web.RequestHandler):
        return True
    else:
        return False


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
    if _verify_handler(handler):
        return ujson.loads(list(handler.request.arguments.keys())[0])
    else:
        return None


def _return2client(handler, payload):
    data = _stringify_data(payload)
    if _verify_handler(handler):
        try:
            handler.write(ujson.dumps(data))
        except ValueError:
            handler.write('json conversion failed')
    return


def _stringify_data(docs):
    stringed = dict()
    if isinstance(docs, list):
        for _ in docs:
            for k, v in six.iteritems(_):
                if isinstance(v, ObjectId):
                    stringed[k] = str(v)
                elif isinstance(v, datetime.datetime):
                    stringed[k] = str(v)
                elif isinstance(v, dict):
                    stringed[k] = _stringify_data(v)
                else:
                    stringed[k] = v
    elif isinstance(docs, dict):
        for k, v in six.iteritems(docs):
                if isinstance(v, ObjectId):
                    stringed[k] = str(v)
                elif isinstance(v, datetime.datetime):
                    stringed[k] = str(v)
                elif isinstance(v, dict):
                    stringed[k] = _stringify_data(v)
                else:
                    stringed[k] = v
    elif isinstance(docs, ObjectId):
        stringed = str(docs)
    else:
        stringed = docs
    return stringed


def _write(handler, payload):
    if not isinstance(handler, tornado.web.RequestHandler):
        raise TypeError('Cannot unpack the query params. Handler required')