""" Startup script for the server."""
import argparse
# TODO: Replace this with a better startup mechanism
import tornado.web
from metadataservice.server.engine import (RunStartHandler, RunStopHandler,
                                           EventDescriptorHandler,
                                           EventHandler, loop, db_connect)
# CappedRunStartHandler, CappedRunStopHandler,

from metadataservice.server.conf import load_configuration


def start_server():
    config = {k: v for k, v in load_configuration('metadataservice', 'MDS',
                                                  ['host', 'port', 'timezone',
                                                   'database'],
                                                  allow_missing=True).items()
              if v is not None}

    parser = argparse.ArgumentParser()
    parser.add_argument('--database', dest='database', type=str,
                        help='name of database to use')
    parser.add_argument('--host', dest='host', type=str,
                        help='host to use')
    parser.add_argument('--timezone', dest='timezone', type=str,
                        help='Local timezone')
    parser.add_argument('--port', dest='port', type=int,
                        help='port to use')
    args = parser.parse_args()
    if args.database is not None:
        config['database'] = args.database
    if args.host is not None:
        config['host'] = args.host
    if args.timezone is not None:
        config['timezone'] = args.timezone
    if args.port is not None:
        config['port'] = args.port

    db = db_connect(config['database'],
                    config['host'],
                    config['port'])
    print(db)
    application = tornado.web.Application([
        (r'/run_start', RunStartHandler), (r'/run_stop', RunStopHandler),
        (r'/event_descriptor', EventDescriptorHandler),
        (r'/event', EventHandler)
         ], db=db)
    application.listen(7770)
    loop.start()
