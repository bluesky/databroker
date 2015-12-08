from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import tornado.ioloop
import tornado.web
from tornado import gen

import pymongo


import ujson
import jsonschema

from metadataservice.server import utils

# TODO: Write tests specifically for the server side(existing ones are garbage!)
# TODO: Add indexing to all collections!

CACHE_SIZE = 100000
loop = tornado.ioloop.IOLoop.instance()


class DefaultHandler(tornado.web.RequestHandler):
    """DefaultHandler which takes care of CORS for @hslepicka js gui."""
    @tornado.web.asynchronous
    def set_default_headers(self):
        self.set_header('Access-Control-Allow-Origin', '*')
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.set_header('Access-Control-Max-Age', 1000)
        self.set_header('Access-Control-Allow-Headers', '*')
        self.set_header('Content-type', 'application/json')

    def data_received(self, chunk):
        """Abstract method, here to show it exists explicitly. Useful for streaming client"""
        pass


class RunStartHandler(DefaultHandler):
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
        _id = query.pop('_id', None)
        if _id:
            raise tornado.web.HTTPError(500, 'No ObjectId search supported')
        docs = database.run_start.find(query)
        if not docs:
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
        result = database.run_start.insert(data)
        # database.run_start.create_index([('uid', pymongo.ASCENDING)],
        #                                unique=True, background=True)
        #database.run_start.create_index([('time', pymongo.ASCENDING),
        #                                 ('scan_id', pymongo.ASCENDING)],
        #                               background=True)
        #database.run_start.create_index([('owner', pymongo.ASCENDING)],
        #                                background=True, sparse=True)
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


class EventDescriptorHandler(DefaultHandler):
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
        docs = database.event_descriptor.find(query)
        if not docs:
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
        result = database.event_descriptor.insert(data)
        #database.event_descriptor.create_index([('uid', pymongo.ASCENDING)],
        #                                unique=True, background=True) 
        #database.event_descriptor.create_index([('run_start', pymongo.ASCENDING),
        #                                        ('time', pymongo.ASCENDING)],
        #                                       unique=False, background=True) 
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


class RunStopHandler(DefaultHandler):
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
        docs = database.run_stop.find(query)
        if not docs:
            raise tornado.web.HTTPError(404, 
                                        'No results for given query' + str(query))
        else:
            utils._return2client(self, docs)
            self.finish()

    @tornado.web.asynchronous
    @gen.coroutine
    def post(self):
        database = self.settings['db']
        data = ujson.loads(self.request.body.decode("utf-8"))
        docs =  database.run_stop.find({'run_start': data['run_start']})
        try:
            res = next(docs)
            raise tornado.web.HTTPError(500,
                                        'A run_stop already created for given run_start')
        except StopIteration:
            pass
        jsonschema.validate(data, utils.schemas['run_stop'])
        result = database.run_stop.insert(data)
        #database.run_stop.create_index([('uid', pymongo.ASCENDING)],
        #                                unique=True, background=True) 
        #database.run_stop.create_index([('run_start', pymongo.ASCENDING)],
        #                                       unique=True, background=True)
        #database.run_stop.create_index([('time', pymongo.ASCENDING), 
        #                                ('exit_status', pymongo.ASCENDING)],
        #                               background=True)
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


class EventHandler(DefaultHandler):
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
        docs = database['event'].find(query)
        if not docs:
            raise tornado.web.HTTPError(404,
                                        status_code='No results for given query' + str(query))
        else:
            self.write('[')
            d = next(docs)
            while True:
                try:
                    del(d['_id'])
                    self.write(ujson.dumps(d))
                    d = next(docs)
                    self.write(',')
                except StopIteration:
                    break
            self.write(']')
        self.finish()

    @tornado.web.asynchronous
    @gen.coroutine
    def post(self):
        database = self.settings['db']
        data = ujson.loads(self.request.body.decode("utf-8"))
        if isinstance(data, list):
            jsonschema.validate(data, utils.schemas['bulk_events'])
            bulk = database.event.initialize_unordered_bulk_op()
            for _ in data:
                bulk.insert(_)
            try:
                bulk.execute()
            except pymongo.errors.BulkWriteError as err:
                raise tornado.web.HTTPError(500, str(err))
            #database.event.create_index([('time', pymongo.ASCENDING),
            #                              ('descriptor', pymongo.ASCENDING)],
            #                            background=True)
        else:
            jsonschema.validate(data, utils.schemas['event'])
            result = database.event.insert(data)
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

# TODO: Include capped collection support in the next cycle.
# class CappedRunStartHandler(DefaultHandler):
#     """Handler for capped run_start collection that is used for caching
#     and monitoring.
#     """
#     @tornado.web.asynchronous
#     @gen.coroutine
#     def get(self):
#         database = self.settings['db']
#         cursor = database.run_start_capped.find({},
#                                                 await_data=True,
#                                                 tailable=True)
#         while True:
#             if (yield cursor.fetch_next):
#                 tmp = cursor.next_object() #pop from cursor all old entries
#             else:
#                 break
#         while cursor.alive:
#             if (yield cursor.fetch_next):
#                 result = cursor.next_object()
#                 utils._return2client(self, result)
#                 break
#             else:
#                 yield gen.Task(loop.add_timeout, datetime.timedelta(seconds=1))
#
#     @tornado.web.asynchronous
#     @gen.coroutine
#     def post(self):
#         database = self.settings['db']
#         data = ujson.loads(self.request.body.decode("utf-8"))
#         jsonschema.validate(data, utils.schemas['run_start'])
#         try:
#             result = yield database.run_start_capped.insert(data)
#         except pymongo.errors.CollectionInvalid:
#             database.create_collection('run_start_capped', capped=True,
#                                        size=CACHE_SIZE)
#         if not result:
#             raise tornado.web.HTTPError(500)
#         else:
#             utils._return2client(self, data)
#
#     @tornado.web.asynchronous
#     @gen.coroutine
#     def put(self):
#         raise tornado.web.HTTPError(404)
#
#     @tornado.web.asynchronous
#     @gen.coroutine
#     def delete(self):
#         raise tornado.web.HTTPError(404)
#
# class CappedRunStopHandler(DefaultHandler):
#     """Handler for capped run_stop collection that is used for caching
#     and monitoring.
#     """
#     @tornado.web.asynchronous
#     @gen.coroutine
#     def get(self):
#         database = self.settings['db']
#         cursor = database.run_stop_capped.find({},
#                                                await_data=True,
#                                                tailable=True)
#         while True:
#             # burn in, get all in the collection beforehand
#             if (yield cursor.fetch_next):
#                 # burn in
#                 tmp = cursor.next_object()
#             else:
#                 break
#         while cursor.alive:
#             if (yield cursor.fetch_next):
#                 result = cursor.next_object()
#                 utils._return2client(self, result)
#                 break
#             else:
#                 yield gen.Task(loop.add_timeout,
#                                datetime.timedelta(seconds=1))
#     @tornado.web.asynchronous
#     @gen.coroutine
#     def post(self):
#         database = self.settings['db']
#         data = ujson.loads(self.request.body.decode("utf-8"))
#         jsonschema.validate(data, utils.schemas['run_start'])
#         try:
#             result = yield database.run_stop_capped.insert(data)
#         except pymongo.errors.CollectionInvalid:
#             # try to create the collection if it doesn't exist
#             database.create_collection('run_start_capped', capped=True, size=CACHE_SIZE)
#         if not result:
#             raise tornado.web.HTTPError(500,
#                                         status='Insert to CappedRunStop collection failed')
#         else:
#             utils._return2client(self, data)
#
#     @tornado.web.asynchronous
#     @gen.coroutine
#     def put(self):
#         raise tornado.web.HTTPError(404,
#                                     status='Not allowed on server')
#
#     @tornado.web.asynchronous
#     @gen.coroutine
#     def delete(self):
#         raise tornado.web.HTTPError(404,
#                                     status='Not allowed on server')
