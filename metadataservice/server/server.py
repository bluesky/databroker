from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import tornado.ioloop
import tornado.web
import time
from tornado import gen
from bson import ujson
import pymongo
import motor
from metadataservice.server import utils
__author__ = 'arkilic'

# READ THE DOCS and COMMENTS before grabbing your pitchforks and torches. A lot going on here!!


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
        """Query run_start documents"""
        query = utils._unpack_params(self)
        start = query.pop('range_floor')
        stop = query.pop('range_ceil')
        if start ==0 and stop ==1:
            docs = yield db.run_start.find_one(query)
            self.write(ujson.dumps(docs))
        else:
            cursor = db.run_start.find(query).sort('time', pymongo.DESCENDING)[start:stop]
            docs = yield cursor.to_list(None)
            self.write(ujson.dumps(docs))
        self.finish()

    @tornado.web.asynchronous
    @gen.coroutine
    def post(self):
        """Insert a run_start document"""
        db = self.settings['db']
        data = ujson.loads(self.request.body.decode("utf-8"))
        #TODO: Add validation once database is implemented
        result = yield db.run_start.insert(data)#async insert
        self.write(ujson.dumps(result))
        self.finish()


class BeamlineConfigHandler(tornado.web.RequestHandler):
    """Handler for run_start insert and query operations"""
    @tornado.web.asynchronous
    @gen.coroutine
    def get(self):
        # TODO: Add sort by time!
        """Query beamline_config documents"""
        query = utils._unpack_params(self)
        start = query.pop('range_floor')
        stop = query.pop('range_ceil')
        if start ==0 and stop ==1:
            docs = yield db.beamline_config.find_one(query)
            self.write(ujson.dumps(docs))
        else:
            cursor = db.beamline_config.find(query).sort('time', pymongo.DESCENDING)[start:stop]
            docs = yield cursor.to_list(None)
            self.write(ujson.dumps(docs))
        self.finish()

    @tornado.web.asynchronous
    @gen.coroutine
    def post(self):
        """Insert a beamline_config document"""
        db = self.settings['db']
        data = ujson.loads(self.request.body.decode("utf-8"))
        #TODO: Add validation once database is implemented
        result = yield db.beamline_config.insert(data)#async insert
        self.write(ujson.dumps(result))
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
        if start==0 and stop==1:
            docs = yield db.event_descriptor.find_one(query)
            self.write(ujson.dumps(docs))
        else:
            cursor = db.event_descriptor.find(query).sort('time', pymongo.DESCENDING)[start:stop]
            docs = yield cursor.to_list(None)
            self.write(ujson.dumps(docs))
        self.finish()

    @tornado.web.asynchronous
    @gen.coroutine
    def post(self):
        """Insert an event_descriptor document"""
        db = self.settings['db']
        data = ujson.loads(self.request.body.decode("utf-8"))
        #TODO: Add validation once database is implemented
        result = yield db.event_descriptor.insert(data)#async insert
        self.write(ujson.dumps(result))
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
        if start ==0 and stop ==1:
            docs = yield db.run_stop.find_one(query)
            self.write(ujson.dumps(docs))
        else:
            cursor = db.run_stop.find(query).sort('time', pymongo.DESCENDING)[start:stop]
            docs = yield cursor.to_list(None)
            self.write(ujson.dumps(docs))
        self.finish()

    @tornado.web.asynchronous
    @gen.coroutine
    def post(self):
        """Insert a run_start document"""
        db = self.settings['db']
        data = ujson.loads(self.request.body.decode("utf-8"))
        #TODO: Add validation once database is implemented
        result = yield db.run_stop.insert(data)#async insert
        self.write(ujson.dumps(result))
        self.finish()

class EventHandler(tornado.web.RequestHandler):
    """Handler for run_start insert and query operations"""
    @tornado.web.asynchronous
    @gen.coroutine
    def get(self):
        # TODO: Add sort by time!
        """Query event documents"""
        import ujson
        query = utils._unpack_params(self)
        start = query.pop('range_floor')
        stop = query.pop('range_ceil')
        cursor = db.event_descriptor.find(query).sort('time', pymongo.DESCENDING)[start:stop]
        docs = yield cursor.to_list(None)
#         self.write(ujson.dumps(docs))
        self.finish()
    
    @tornado.web.asynchronous
    @gen.coroutine
    def post(self):
        """Insert a run_start document"""
        db = self.settings['db']
        data = ujson.loads(self.request.body.decode("utf-8"))
        #TODO: Add validation once database is implemented
        bulk = db.event.initialize_unordered_bulk_op()
        start_time = time.time()
        for _ in data:
            bulk.insert(_)
        try:
            yield bulk.execute() #add timeout etc.!
        except pymongo.errors.BulkWriteError as err:
            print(err)
            self.write(ujson.dumps(err))
        stop_time = time.time()
        print('insert time: {}'.format(stop_time - start_time))
        self.finish()

db = db_connect("datastore2", '127.0.0.1', 27017) #TODO: Replace with configured one
application = tornado.web.Application([
    (r'/run_start', RunStartHandler), (r'/beamline_config',BeamlineConfigHandler),
    (r'/run_stop', RunStopHandler), (r'/event_descriptor',EventDescriptorHandler),
    (r'/event',EventHandler)], db=db)
application.listen(7771)
tornado.ioloop.IOLoop.instance().start()
