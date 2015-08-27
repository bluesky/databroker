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
        doc = yield db.run_start.find_one(query)
        print(query, doc)
        #utils.Indexable(crsr)
        #TODO: Run the query
        #TODO: Slice the cursor
        #TODO: Given request range, return documents as Json


    @tornado.web.asynchronous
    @gen.coroutine
    def post(self):
        """Insert a run_start document"""
        # placeholder dummy!
        db = self.settings['db']
        data = json.loads(self.request.body)
        result = yield db.messages.insert({'msg': data})#async insert
        utils.dumps(result)
        print(result)


db = db_connect(database='cache', host='127.0.0.1', port=27017)

db = db_connect("datastore2", '127.0.0.1', 27017) #TODO: Replace with configured one
application = tornado.web.Application([
    (r'/run_start', RunStartHandler)], db=db)
application.listen(7777)
tornado.ioloop.IOLoop.instance().start()