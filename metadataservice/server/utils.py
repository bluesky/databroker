from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import tornado.web
import ujson
import datetime
import six
from bson.objectid import ObjectId
__author__ = 'arkilic'


__all__ = ['_verify_handler', '_unpack_params',
           '_return2client', '_stringify_data']




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
        #TODO: Add exception handling
        handler.write(ujson.dumps(data))


def _stringify_data(docs):
    # TODO: Clean up this code!
    if isinstance(docs, list):
        stringed = list()
        for _ in docs:
            tmp = dict()
            for k, v in six.iteritems(_):
                if isinstance(v, ObjectId):
                    tmp[k] = str(v)
                elif isinstance(v, datetime.datetime):
                    tmp[k] = str(v)
                elif isinstance(v, dict):
                    tmp[k] = _stringify_data(v)
                else:
                    tmp[k] = v
                stringed.append(tmp)
    elif isinstance(docs, dict):
        stringed = dict()
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
