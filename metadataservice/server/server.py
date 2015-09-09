from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import tornado.ioloop
import tornado.web
from metadataservice.schema.validate import schemas
import jsonschema
from tornado import gen
import pymongo
import pymongo.errors
import motor
import ujson
from metadataservice.server import utils
__author__ = 'arkilic'

# READ THE DOCS and COMMENTS before grabbing your pitchforks and torches. A lot going on here!!
# Using both json_util and ujson because they both have their moments. 
# Will figure out a direction soon
# ujson is 3-5 orders of magnitude fast but doesn't encode bson neatly
# json_util does encode/decode neatly but painfully slow


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
    client = motor.MotorClient("localhost", 28000, replicaset="rs0")
    client.write_concern = {'w': 0, 'wtimeout': 1000}
    database = client[database]
    return database


class RunStartHandler(tornado.web.RequestHandler):
    """Handler for run_start insert and query operations"""
    @tornado.web.asynchronous
    @gen.coroutine
    def get(self):
        """Query run_start documents"""
        query = utils._unpack_params(self)
        start = query.pop('range_floor')
        stop = query.pop('range_ceil')
        cursor = db.run_start.find(query).sort('time', pymongo.ASCENDING)[start:stop]
        docs = yield cursor.to_list(None)
        payload = utils._stringify_data(docs)
        utils._return2client(self, payload)
        self.finish()

    @tornado.web.asynchronous
    @gen.coroutine
    def post(self):
        """Insert a run_start document"""
        db = self.settings['db']
        data = ujson.loads(self.request.body.decode("utf-8"))
        #TODO: Add validation once database is implemented
        jsonschema.validate(data, schemas['run_start'])
        result = yield db.run_start.insert(data)#async insert
        utils._return2client(self, result)
        self.finish()


class EventDescriptorHandler(tornado.web.RequestHandler):
    """Handler for run_start insert and query operations"""
    @tornado.web.asynchronous
    @gen.coroutine
    def get(self):
        # TODO: Add sort by time!
        """Query event_descriptor documents"""
        query = utils._unpack_params(self)
        start = query.pop('range_floor')
        stop = query.pop('range_ceil')
        cursor = db.event_descriptor.find(query).sort('time', pymongo.ASCENDING)[start:stop]
        docs = yield cursor.to_list(None)
        payload = utils._stringify_data(docs)
        utils._return2client(self, payload)
        self.finish()

    @tornado.web.asynchronous
    @gen.coroutine
    def post(self):
        """Insert an event_descriptor document"""
        db = self.settings['db']
        data = ujson.loads(self.request.body.decode("utf-8"))
        #TODO: Add validation once database is implemented
        jsonschema.validate(data, schemas['descriptor'])
        result = yield db.event_descriptor.insert(data)#async insert
        utils._return2client(self, result)
        self.finish()


class RunStopHandler(tornado.web.RequestHandler):
    """Handler for run_start insert and query operations"""
    @tornado.web.asynchronous
    @gen.coroutine
    def get(self):
        # TODO: Add sort by time!
        """Query run_start documents"""
        query = utils._unpack_params(self)
        start = query.pop('range_floor')
        stop = query.pop('range_ceil')    
        cursor = db.run_stop.find(query).sort('time', pymongo.ASCENDING)[start:stop]
        docs = yield cursor.to_list(None)
        payload = utils._stringify_data(docs)
        utils._return2client(self, payload)
        self.finish()

    @tornado.web.asynchronous
    @gen.coroutine
    def post(self):
        """Insert a run_start document"""
        db = self.settings['db']
        data = ujson.loads(self.request.body.decode("utf-8"))
        #TODO: Add validation once database is implemented
        jsonschema.validate(data, schemas['run_stop'])
        result = yield db.run_stop.insert(data)#async insert
        utils._return2client(self, result)
        self.finish()

class EventHandler(tornado.web.RequestHandler):
    """Handler for run_start insert and query operations"""
    @tornado.web.asynchronous
    @gen.coroutine
    def get(self):
        # TODO: Add sort by time!
        """Query event documents"""
        query = utils._unpack_params(self)
        start = query.pop('range_floor')
        stop = query.pop('range_ceil')
        cursor = db.event_descriptor.find(query).sort('time', pymongo.ASCENDING)[start:stop]
        docs = yield cursor.to_list(None)
        payload = utils._stringify_data(docs)
        utils._return2client(self, payload)
        self.finish()
    
    @tornado.web.asynchronous
    @gen.coroutine
    def post(self):
        """Insert a run_start document"""
        db = self.settings['db']
        data = ujson.loads(self.request.body.decode("utf-8"))
        #TODO: Add validation once we figure out how to do this in bluesky
        bulk = db.event.initialize_unordered_bulk_op()
        for _ in data:
            bulk.insert(_)
        try:
            yield bulk.execute() #add timeout etc.!
        except pymongo.errors.BulkWriteError as err:
            print(err)
            utils._return2client(err)
        self.finish()

db = db_connect("datastore2", '127.0.0.1', 28000) #TODO: Replace with configured one

application = tornado.web.Application([
    (r'/run_start', RunStartHandler), (r'/run_stop', RunStopHandler),
    (r'/event_descriptor',EventDescriptorHandler),
    (r'/event',EventHandler)], db=db)
application.listen(7770)
tornado.ioloop.IOLoop.instance().start()
