""" Startup script for the server."""
import argparse
#TODO: Replace this with a better startup mechanism
import tornado.web
from metadataservice.server.engine import (RunStartHandler, RunStopHandler,
                                           EventDescriptorHandler, EventHandler,
                                           # CappedRunStartHandler, CappedRunStopHandler,
                                           loop, db_connect)
from metadataservice.server.conf import load_configuration


if __name__ == "__main__":

    config = {k: v for k, v in load_configuration('metadataservice', 'MDS',
                                                  ['host', 'port', 'timezone'],
                                                  allow_missing=True).items()
              if v is not None}

    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--database', dest='database', type=str,
                        help='name of database to use')
    args = parser.parse_args()
    if args.database is not None:
        config['database'] = args.database

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
