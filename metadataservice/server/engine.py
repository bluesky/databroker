from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import tornado.ioloop
import tornado.web
from tornado import gen

import datetime
import pymongo
import motor
from bson.objectid import ObjectId

import ujson
import jsonschema

from metadataservice.server import utils


# TODO: Write tests specifically for the server side(existing ones are garbage!)

CACHE_SIZE = 100000
loop = tornado.ioloop.IOLoop.instance()

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
        Time tolerance before write fails. Affects the package size in bulk insert so use wisely

    Returns motor.MotorDatabase
    -------
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
        Query run_start documents. Query params are jsonified for type preservation
    post()
        Insert a run_start document.Same validation method as bluesky, secondary
        safety net.
    """
    @tornado.web.asynchronous
    @gen.coroutine
    def get(self):
        database = self.settings['db']
        query = utils._unpack_params(self)
        try:
            start = query.pop('range_floor')
            stop = query.pop('range_ceil')
        except KeyError:
            raise tornado.web.HTTPError(400,
                            "range_ceil and range_floor required parameters")
        _id = query.pop('_id', None)
        if _id:
            query['_id'] = ObjectId(_id)
        docs = yield database.run_start.find(query).sort(
            'time', pymongo.ASCENDING)[start:stop].to_list(None)
            #to_list is async so no forcing to make synchronous
        if docs == [] and start == 0:
            raise tornado.web.HTTPError(500, reason='No results found for query')
        else:
            utils._return2client(self, docs)
            self.finish()

    @tornado.web.asynchronous
    @gen.coroutine
    def post(self):
        database = self.settings['db']
        data = ujson.loads(self.request.body.decode("utf-8"))
        jsonschema.validate(data, utils.schemas['run_start'])
        result = yield database.run_start.insert(data)
        database.run_start.create_index([('time', pymongo.ASCENDING), 
                                         ('scan_id', pymongo.ASCENDING)])
        if not result:
            raise tornado.web.HTTPError(500)
        else:
            utils._return2client(self, data)
   
    @tornado.web.asynchronous
    @gen.coroutine
    def put(self):
        raise tornado.web.HTTPError(404, 
                                    status='Not allowed on server')

    @tornado.web.asynchronous
    @gen.coroutine
    def delete(self):
        raise tornado.web.HTTPError(404,
                                    status='Not allowed on server')


class EventDescriptorHandler(tornado.web.RequestHandler):
    """Handler for event_descriptor insert and query operations.
    Uses traditional RESTful lingo. get for querying and post for inserts
    
    Methods
    -------
    get()
        Query event_descriptor documents. Get params are json encoded in order to preserve type
    post()
        Insert a event_descriptor document.Same validation method as bluesky, secondary
        safety net.
    """
    @tornado.web.asynchronous
    @gen.coroutine
    def get(self):
        database = self.settings['db']
        query = utils._unpack_params(self)
        try:
            start = query.pop('range_floor')
            stop = query.pop('range_ceil')
        except KeyError:
            raise tornado.web.HTTPError(400,
                                        "range_ceil and range_floor required parameters")
        docs = yield database.event_descriptor.find(query).sort(
            'time', pymongo.ASCENDING)[start:stop].to_list(None)
        if docs == [] and start == 0:
            raise tornado.web.HTTPError(500, 
                                        reason='No results found for query')
        else:
            utils._return2client(self, docs)
            self.finish()


    @tornado.web.asynchronous
    @gen.coroutine
    def post(self):
        database = self.settings['db']
        data = ujson.loads(self.request.body.decode("utf-8"))
        jsonschema.validate(data, utils.schemas['descriptor'])
        result = yield database.event_descriptor.insert(data)
        database.event_descriptor.create_index([('run_start', pymongo.ASCENDING), 
                                                ('time', pymongo.ASCENDING)])
        if not result:
            raise tornado.web.HTTPError(500)
        else:
            utils._return2client(self, data)
            
    @tornado.web.asynchronous
    @gen.coroutine
    def put(self):
        raise tornado.web.HTTPError(404,
                                    status='Not allowed on server')

    @tornado.web.asynchronous
    @gen.coroutine
    def delete(self):
        raise tornado.web.HTTPError(404)


class RunStopHandler(tornado.web.RequestHandler):
    """Handler for run_stop insert and query operations.
    Uses traditional RESTful lingo. get for querying and post for inserts
    
    Methods
    -------
    get()
        Query run_stop documents. Get params are json encoded in order to preserve type
    post()
        Insert a run_stop document.Same validation method as bluesky, secondary
        safety net.
    """
    @tornado.web.asynchronous
    @gen.coroutine
    def get(self):
        database = self.settings['db']
        query = utils._unpack_params(self)
        try:
            start = query.pop('range_floor')
            stop = query.pop('range_ceil')
        except KeyError:
            raise tornado.web.HTTPError(400,
                                        "range_ceil and range_floor required parameters")
        docs = yield database.run_stop.find(query).sort(
            'time', pymongo.ASCENDING)[start:stop].to_list(None)
        if not docs and start == 0:
            raise tornado.web.HTTPError(404, 
                                        status_code='No results for given query' + str(query))
        else:
            utils._return2client(self, docs)
            self.finish()

    @tornado.web.asynchronous
    @gen.coroutine
    def post(self):
        database = self.settings['db']
        data = ujson.loads(self.request.body.decode("utf-8"))
        jsonschema.validate(data, utils.schemas['run_stop'])
        result = yield database.run_stop.insert(data)
        database.run_stop.create_index([('time', pymongo.ASCENDING), 
                                        ('exit_status', pymongo.ASCENDING), 
                                        ('run_start', pymongo.ASCENDING)])
        if not result:
            raise tornado.web.HTTPError(500)
        else:
            utils._return2client(self, data)

    @tornado.web.asynchronous
    @gen.coroutine
    def put(self):
        raise tornado.web.HTTPError(404)

    @tornado.web.asynchronous
    @gen.coroutine
    def delete(self):
        raise tornado.web.HTTPError(404)


class EventHandler(tornado.web.RequestHandler):
    """Handler for event insert and query operations.
    Uses traditional RESTful lingo. get for querying and post for inserts
    
    Methods
    -------
    get()
        Query event documents. Get params are json encoded in order to preserve type
    post()
        Insert a event document.Same validation method as bluesky, secondary
        safety net.
    """
    @tornado.web.asynchronous
    @gen.coroutine
    def get(self):
        database = self.settings['db']
        query = utils._unpack_params(self)
        try:
            start = query.pop('range_floor')
            stop = query.pop('range_ceil')
        except KeyError:
            raise tornado.web.HTTPError(400,
                                        "range_ceil and range_floor required parameters")
        docs = yield database.event_descriptor.find(query).sort(
            'time', pymongo.ASCENDING)[start:stop].to_list(None)
        if not docs and start == 0:
            raise tornado.web.HTTPError(404,
                                        status='No results for given query')
        else:
            utils._return2client(self, docs)
            self.finish()

    @tornado.web.asynchronous
    @gen.coroutine
    def post(self):
        database = self.settings['db']
        data = ujson.loads(self.request.body.decode("utf-8"))
        if isinstance(data, list):
            jsonschema.validate(data, utils.schemas['bulk_events'])
            bulk = yield database.event.initialize_unordered_bulk_op()
            for _ in data:
                bulk.insert(_)
            try:
                yield bulk.execute()
            except pymongo.errors.BulkWriteError as err:
                raise tornado.web.HTTPError(500, str(err))
            database.event.create_index([('time', pymongo.ASCENDING),
                                          ('descriptor', pymongo.ASCENDING)])
        else:
            jsonschema.validate(data, utils.schemas['event'])
            result = yield database.event.insert(data)
            if not result:
                raise tornado.web.HTTPError(500)
    
    @tornado.web.asynchronous
    @gen.coroutine
    def put(self):
        raise tornado.web.HTTPError(404)

    @tornado.web.asynchronous
    @gen.coroutine
    def delete(self):
        raise tornado.web.HTTPError(404)


class CappedRunStartHandler(tornado.web.RequestHandler):
    """Handler for capped run_start collection that is used for caching 
    and monitoring.
    """
    @tornado.web.asynchronous
    @gen.coroutine
    def get(self):
        database = self.settings['db']
        cursor = database.run_start_capped.find({},
                                                await_data=True,
                                                tailable=True)
        while True:
            if (yield cursor.fetch_next):
                tmp = cursor.next_object() #pop from cursor all old entries
            else:
                break 
        while cursor.alive:
            if (yield cursor.fetch_next):
                result = cursor.next_object()
                utils._return2client(self, result)
                break
            else:
                yield gen.Task(loop.add_timeout, datetime.timedelta(seconds=1))
            
    @tornado.web.asynchronous
    @gen.coroutine
    def post(self):
        database = self.settings['db']
        data = ujson.loads(self.request.body.decode("utf-8"))
        jsonschema.validate(data, utils.schemas['run_start'])
        try:
            result = yield database.run_start_capped.insert(data)
        except pymongo.errors.CollectionInvalid:
            database.create_collection('run_start_capped', capped=True, 
                                       size=CACHE_SIZE)
        if not result:
            raise tornado.web.HTTPError(500)
        else:
            utils._return2client(self, data)

    @tornado.web.asynchronous
    @gen.coroutine
    def put(self):
        raise tornado.web.HTTPError(404)

    @tornado.web.asynchronous
    @gen.coroutine
    def delete(self):
        raise tornado.web.HTTPError(404)

class CappedRunStopHandler(tornado.web.RequestHandler):
    """Handler for capped run_stop collection that is used for caching 
    and monitoring.
    """
    @tornado.web.asynchronous
    @gen.coroutine
    def get(self):
        database = self.settings['db']
        cursor = database.run_stop_capped.find({},
                                               await_data=True,
                                               tailable=True)
        while True:
            # burn in, get all in the collection beforehand
            if (yield cursor.fetch_next):
                # burn in
                tmp = cursor.next_object() 
            else:
                break
        while cursor.alive:
            if (yield cursor.fetch_next):
                result = cursor.next_object()
                
                utils._return2client(self, result)
                break
            else:
                yield gen.Task(loop.add_timeout, 
                               datetime.timedelta(seconds=1))
            
    @tornado.web.asynchronous
    @gen.coroutine
    def post(self):
        database = self.settings['db']
        data = ujson.loads(self.request.body.decode("utf-8"))
        jsonschema.validate(data, utils.schemas['run_start'])
        try:
            result = yield database.run_stop_capped.insert(data)
        except pymongo.errors.CollectionInvalid:
            # try to create the collection if it doesn't exist
            database.create_collection('run_start_capped', capped=True, size=CACHE_SIZE)
        if not result:
            raise tornado.web.HTTPError(500,
                                        status='Insert to CappedRunStop collection failed')
        else:
            utils._return2client(self, data)

    @tornado.web.asynchronous
    @gen.coroutine
    def put(self):
        raise tornado.web.HTTPError(404,
                                    status='Not allowed on server')

    @tornado.web.asynchronous
    @gen.coroutine
    def delete(self):
        raise tornado.web.HTTPError(404,
                                    status='Not allowed on server')