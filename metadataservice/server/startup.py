import tornado.web
from metadataservice.server.engine import (db_connect, RunStartHandler, RunStopHandler,
                                           EventDescriptorHandler, EventHandler,
                                           CappedRunStartHandler, CappedRunStopHandler, 
                                           loop)
from metadataservice.server.conf import connection_config


db = db_connect(connection_config['database'],
                connection_config['host'],
                connection_config['port'])

if __name__ == "__main__":
    application = tornado.web.Application([
        (r'/run_start', RunStartHandler), (r'/run_stop', RunStopHandler),
        (r'/event_descriptor',EventDescriptorHandler),
        (r'/event',EventHandler),
        (r'/run_start_capped', CappedRunStartHandler),
        (r'/run_stop_capped', CappedRunStopHandler)], db=db)
    application.listen(7770)
    loop.start()