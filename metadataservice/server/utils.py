from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import tornado.web
from pkg_resources import resource_filename as rs_fn
import ujson
import datetime
import six
from bson.objectid import ObjectId


SCHEMA_PATH = 'schema'
SCHEMA_NAMES = {'run_start': 'run_start.json',
                'run_stop': 'run_stop.json',
                'event': 'event.json',
                'bulk_events': 'bulk_events.json',
                'descriptor': 'event_descriptor.json'}
fn = '{}/{{}}'.format(SCHEMA_PATH)
schemas = {}
for name, filename in SCHEMA_NAMES.items():
    with open(rs_fn('metadataservice', resource_name=fn.format(filename))) as fin:
        schemas[name] = ujson.load(fin)


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
    if isinstance(handler, tornado.web.RequestHandler):
        return ujson.loads(list(handler.request.arguments.keys())[0])
    else:
        raise TypeError("Handler provided must be of tornado.web.RequestHandler type")


def _return2client(handler, payload):
    """Dump the result back to client's open socket. No need to worry about package size
    or socket behavior as tornado handles this for us

    Parameters
    -----------
    handler: tornado.web.RequestHandler
        Request handler for the collection of operation(post/get)
    payload: dict, str, list
        Information to be sent to the client
    """
    handler.write(ujson.dumps(_stringify_data(payload)))


def _stringify_data(docs):
    """ujson does not allow encoding/decoding of any object other than
    the basic python types. Therefore, we need to convert datetime and oid
    into string. If nested dictionary, it is handled here as well.

    Parameters
    -----------
    docs: list or dict
        Query results to be 'stringified'
    Returns
    -----------
    stringed: list or dict
        Stringified list or dictionary
    """
    if isinstance(docs, list):
        new_docs = []
        tmp = dict
        for d in docs:
            for k, v  in d.items():
                if isinstance(v, ObjectId):
                    d[k] = 'N/A'
                elif isinstance(v, datetime.datetime):
                    tmp = str(v)
                    d[k] = tmp
                elif isinstance(v, dict):
                    d[k] = _stringify_data(v)
        return docs
    elif isinstance(docs, dict):
        for k, v in docs.items():
            if isinstance(v, ObjectId):
                docs[k] = 'N/A'
            elif isinstance(v, datetime.date):
                tmp = str(v)
                docs[k] = tmp
            elif isinstance(v, dict):
                docs[k] = _stringify_data(v)
        return docs
    else:
        raise TypeError("Unsupported type ", type(docs))
