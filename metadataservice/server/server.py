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

""".. note:: ultra-json is 3-5 orders of magnitude faster since it runs outside GIL.
.. note:: bson.json_util does encode/decode neatly but painfully slow normalize object fields manually.
.. warning:: Early alpha. Might go under some changes. Handlers and dataapi are unlikely to change.
"""


def db_connect(database ,host, port, replicaset=None, write_concern="majority",
               write_timeout=1000):
    """Helper function to deal with stateful connections to motor.
    Connection established lazily. Asycnc so do not treat like mongonengine connection pool.

    Parameters
    ----------
    server: str
        The name of the server that data is stored
    host: str
        Name/address of the server that mongo daemon lives
    port: int
        Port num of the server
    replicaset: str
        Name of the replica set. Configured within mongo deployment.
    write_concern: int
        Traditional mongo write concern. Int denotes number of replica set writes
        to be verified

    write_timeout: int
        Time before write fails. Affects the package size in bulk insert so use wisely

    Returns
    -------
    db: motor.MotorDatabase
        Async server object which comes in handy as server has to juggle multiple clients
        and makes no difference for a single client compared to pymongo
    """
    client = motor.MotorClient(host, port, replicaset=replicaset)
    client.write_concern = {'w': write_concern, 'wtimeout': write_timeout}
    database = client[database]
    return database


class RunStartHandler(tornado.web.RequestHandler):
    """Handler for run_start insert and query operations.
    Uses traditional RESTful lingo. get for querying and post for inserts
    
   Methods
    -------
    get()
	Query run_start documents.Very thin as we do not want to create a
        bottleneck dealing with multiple clients. self.write() dumps the json to
        socket. Client keeps connection open until server kills the socket with
        self.finish(), otherwise keeps hanging wasting resources
    post()
	Insert a run_start document.Same validation method as bluesky, secondary
        safety net. Any changes done here or BlueSky must be implemented here and
        BlueSky.Any data acquisition script can utilize metadataservice as long as 
        it follows this format. 
    """
    @tornado.web.asynchronous
    @gen.coroutine
    def get(self):
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
        db = self.settings['db']
        data = ujson.loads(self.request.body.decode("utf-8"))
        jsonschema.validate(data, schemas['run_start'])
        yield db.run_start.insert(data)
        utils._return2client(self, data)
        self.finish()


class EventDescriptorHandler(tornado.web.RequestHandler):
    """Handler for event_descriptor insert and query operations.
    Uses traditional RESTful lingo. get for querying and post for inserts
    
    Methods
    -------
    get()
        Query event_descriptor documents.Very thin as we do not want to create a
        bottleneck dealing with multiple clients. self.write() dumps the json to
        socket. Client keeps connection open until server kills the socket with
        self.finish(), otherwise keeps hanging wasting resources
    post()
        Insert a event_descriptor document.Same validation method as bluesky, secondary
        safety net. Any changes done here or BlueSky must be implemented here and
        BlueSky.Any data acquisition script can utilize metadataservice as long as 
        it follows this format. 
    """
    @tornado.web.asynchronous
    @gen.coroutine
    def get(self):
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
        db = self.settings['db']
        data = ujson.loads(self.request.body.decode("utf-8"))
        jsonschema.validate(data, schemas['descriptor'])
        yield db.event_descriptor.insert(data)#async insert
        utils._return2client(self, data)
        self.finish()


class RunStopHandler(tornado.web.RequestHandler):
    """Handler for run_stop insert and query operations.
    Uses traditional RESTful lingo. get for querying and post for inserts
    
    Methods
    -------
    get()
        Query run_stop documents.Very thin as we do not want to create a
        bottleneck dealing with multiple clients. self.write() dumps the json to
        socket. Client keeps connection open until server kills the socket with
        self.finish(), otherwise keeps hanging wasting resources
    post()
        Insert a run_stop document.Same validation method as bluesky, secondary
        safety net. Any changes done here or BlueSky must be implemented here and
        BlueSky.Any data acquisition script can utilize metadataservice as long as 
        it follows this format. 
    """
    @tornado.web.asynchronous
    @gen.coroutine
    def get(self):
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
        data = ujson.loads(self.request.body.decode("utf-8"))
        jsonschema.validate(data, schemas['run_stop'])
        yield db.run_stop.insert(data)
        utils._return2client(self, data)
        self.finish()

class EventHandler(tornado.web.RequestHandler):
    """Handler for event insert and query operations.
    Uses traditional RESTful lingo. get for querying and post for inserts
    
    Methods
    -------
    get()
        Query event documents.Very thin as we do not want to create a
        bottleneck dealing with multiple clients. self.write() dumps the json to
        socket. Client keeps connection open until server kills the socket with
        self.finish(), otherwise keeps hanging wasting resources
    post()
        Insert a event document.Same validation method as bluesky, secondary
        safety net. Any changes done here or BlueSky must be implemented here and
        BlueSky.Any data acquisition script can utilize metadataservice as long as 
        it follows this format. 
    """
    @tornado.web.asynchronous
    @gen.coroutine
    def get(self):
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
	db = self.settings['db']
        data = ujson.loads(self.request.body.decode("utf-8"))
        #TODO: Add validation once we figure out how to do this in BS
        if isinstance(data, list):
	    # unordered. in seqnum I trust
	    bulk = db.event.initialize_unordered_bulk_op()
            for _ in data:
                bulk.insert(_)
            try:
                yield bulk.execute() #add timeout etc.!
            except pymongo.errors.BulkWriteError as err:
                utils._return2client(err)
        else:
	    yield db.event.insert(data)
	    utils._return2client(self, data)
        self.finish()


#TODO: Replace with configured one
db = db_connect('datastore2', '127.0.0.1', 28000)
application = tornado.web.Application([
    (r'/run_start', RunStartHandler), (r'/run_stop', RunStopHandler),
    (r'/event_descriptor',EventDescriptorHandler),
    (r'/event',EventHandler)], db=db)
application.listen(7770)
tornado.ioloop.IOLoop.instance().start()
