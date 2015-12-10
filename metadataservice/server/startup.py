""" Startup script for the server."""
#TODO: Replace this with a better startup mechanism
import tornado.web
from metadataservice.server.engine import (RunStartHandler, RunStopHandler,
                                           EventDescriptorHandler, EventHandler,
                                           # CappedRunStartHandler, CappedRunStopHandler,
                                           loop, db_connect)
from metadataservice.server.conf import connection_config


db = db_connect(connection_config['database'],
                connection_config['host'],
                connection_config['port'])

if __name__ == "__main__":
    # start server in main thread
    application = tornado.web.Application([
        (r'/run_start', RunStartHandler), (r'/run_stop', RunStopHandler),
        (r'/event_descriptor', EventDescriptorHandler),
        (r'/event', EventHandler)
         ], db=db)
    application.listen(7770)
    loop.start()