from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import tornado.ioloop
import tornado.web
from tornado import gen
import simplejson as json
import six
import motor
from mdsweb.server import utils

__author__ = 'arkilic'

def db_connect(database ,host, port):
    """Helper function to deal with stateful connections to motor. Connection established lazily.
    Asycnc so do not treat like mongonengine connection pool.
    Tornado needs both client and server so this routine returns both. Once we figure out how to use tornado and motor
    properly, we may need to fix this.

    Parameters
    ----------
    server: str
        The name of the server that data is stored
    host: str
        Name/address of the server that mongo daemon lives
    port: int
        Port num of the server

    Returns
    -------
    db: motor.MotorDatabase
        Async server object
    """
    client = motor.MotorClient()
    db = client[database]
    return db


class RunStartHandler(tornado.web.RequestHandler):
    """Handler for run_start insert and query operations"""
    @tornado.web.asynchronous
    @gen.coroutine
    def get(self):
        """Query run_start documents"""
        query = utils._unpack_params(self)
        start = query.pop('range_floor')
        stop = query.pop('range_ceil')
        print(query)
        cursor =  db.run_start.find(query)[start:stop]
        while (yield cursor.fetch_next):
            doc = cursor.next_object()
            utils._stringify_oid_fields(doc)
            self.write(doc)
        self.finish()



    @tornado.web.asynchronous
    @gen.coroutine
    def post(self):
        """Insert a run_start document"""
        # placeholder dummy!
        db = self.settings['db']
        data = json.loads(self.request.body)
        #TODO: Add validation once database is implemented
        result = yield db.messages.insert({'msg': data})#async insert



db = db_connect("datastore2", '127.0.0.1', 27017) #TODO: Replace with configured one
application = tornado.web.Application([
    (r'/run_start', RunStartHandler)], db=db)
application.listen(7777)
tornado.ioloop.IOLoop.instance().start()