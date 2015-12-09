from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import tornado.web
from pkg_resources import resource_filename as rs_fn
import ujson


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


def db_connect(database, host, port):
    """Helper function to deal with stateful connections to motor.
    Connection established lazily.

    Parameters
    ----------
    database: str
        The name of database pymongo creates and/or connects
    host: str
        Name/address of the server that mongo daemon lives
    port: int
        Port num of the server

    Returns motor.MotorDatabase
    -------
        Async server object which comes in handy as server has to juggle multiple clients
        and makes no difference for a single client compared to pymongo
    """

    client = pymongo.Connection(host=host, port=port)
    database = client[database]
    return database


def _unpack_params(handler):
    """Unpacks the queries from the body of the header
    Parameters
    ----------
    handler: tornado.web.RequestHandler
        Handler for incoming request to collection

    Returns dict
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
    payload: dict, list
        Information to be sent to the client
    """
    handler.write('[')
    if isinstance(payload, dict):
        del(payload['_id'])
        handler.write(ujson.dumps(payload))
    else:
        d = next(payload)
        while True:
            try:
                del(d['_id'])
                handler.write(ujson.dumps(d))
                d = next(payload)
                handler.write(',')
            except StopIteration:
                break
    handler.write(']')
    handler.finish()