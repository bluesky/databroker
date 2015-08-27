from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import tornado.ioloop
import tornado.web
from tornado import gen
import simplejson as json
import pymongo
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
    database = client[database]
    return database


class RunStartHandler(tornado.web.RequestHandler):
    """Handler for run_start insert and query operations"""
    @tornado.web.asynchronous
    @gen.coroutine
    def get(self):
        # TODO: Add sort by time!
        """Query run_start documents"""
        query = utils._unpack_params(self)
        start = query.pop('range_floor')
        stop = query.pop('range_ceil')
        cursor = db.run_start.find(query).sort('time', pymongo.DESCENDING)[start:stop]
        docs = yield cursor.to_list(None)
        for d in docs: #something with to_list is odd. cannot do single line for here.
            utils._stringify_oid_fields(d)
        self.write(json.dumps(docs))
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
        self.finish()

db = db_connect("datastore2", '127.0.0.1', 27017) #TODO: Replace with configured one
application = tornado.web.Application([
    (r'/run_start', RunStartHandler)], db=db)
application.listen(7777)
tornado.ioloop.IOLoop.instance().start()