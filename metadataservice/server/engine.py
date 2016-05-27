from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import tornado.ioloop
import tornado.web
from tornado import gen
from metadatastore.mds import MDS, MDSRO
import pymongo
import pymongo.errors as perr
from .utils import report_error
import ujson
import jsonschema

from metadataservice.server import utils

loop = tornado.ioloop.IOLoop.instance()


class MetadataServiceException(Exception):
    pass


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
    queryable()
        Identifies whether client provided function is fair game for get()
    get()
        Query run_start documents.
    post()
        Insert a run_start document.Same validation method as bluesky, secondary
        safety net.
    insertable()
        Identifies whether client provided function is fair game for post()
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.queryables = ['find_run_starts', 'run_start_given_uid']

    @gen.coroutine
    def queryable(self, func):
        if func in self.queryables:
            yield True
        else:
            raise report_error(500, 'Not a valid query routine', func)

    @tornado.web.asynchronous
    @gen.coroutine
    def get(self):
        mdsro = self.settings['mdsro'] #MDSRO
        request = utils.unpack_params(self)
        print(mdrso, 'got here')
        try:
            func = request['signature']
        except KeyError:
            raise report_error(500,
                               'No valid query function provided!')
        try:
            query = request['query']
        except KeyError:
            raise report_error(500,
                               'A query string must be provided')
        docs = yield mdsro.func(query)
        utils.return2client(self, docs)

    @tornado.web.asynchronous
    @gen.coroutine
    def post(self):
        database = self.settings['db']
        data = ujson.loads(self.request.body.decode("utf-8"))
        jsonschema.validate(data, utils.schemas['run_start'])
        try:
            result = database.run_start.insert(data)
        except perr.PyMongoError:
            raise tornado.web.HTTPError(500,
                                        status='Unable to insert the document')

        database.run_start.create_index([('uid', pymongo.DESCENDING)],
                                       unique=True, background=True)
        database.run_start.create_index([('time', pymongo.DESCENDING),
                                        ('scan_id', pymongo.DESCENDING)],
                                        unique=False)

        if not result:
            raise tornado.web.HTTPError(500)
        else:
            utils.return2client(self, data)

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
        Query event_descriptor documents. Get params are json encoded in order
        to preserve type
    post()
        Insert a event_descriptor document.Same validation method as bluesky,
        secondary safety net.
    """
    @tornado.web.asynchronous
    @gen.coroutine
    def get(self):
        database = self.settings['db']
        query = utils.unpack_params(self)
        docs = database.event_descriptor.find(query)
        if not docs:
            raise tornado.web.HTTPError(500,
                                        reason='No results found for query')
        else:
            utils.return2client(self, docs)

    @tornado.web.asynchronous
    @gen.coroutine
    def post(self):
        database = self.settings['db']
        data = ujson.loads(self.request.body.decode("utf-8"))
        jsonschema.validate(data, utils.schemas['descriptor'])

        try:
            result = database.event_descriptor.insert(data)
        except perr.PyMongoError:
            raise tornado.web.HTTPError(500,
                                        status='Unable to insert the document')
        database.event_descriptor.create_index([('run_start', pymongo.DESCENDING)],
                                               unique=False)
        database.event_descriptor.create_index([('uid', pymongo.DESCENDING)],
                                               unique=True)
        database.event_descriptor.create_index([('time', pymongo.DESCENDING)],
                                               unique=False)
        if not result:
            raise tornado.web.HTTPError(500)
        else:
            utils.return2client(self, data)

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
        query = utils.unpack_params(self)
        docs = database.run_stop.find(query)
        if not docs:
            raise report_error(500,
                               'No results for given query {}'.format(query))
        else:
            utils.return2client(self, docs)

    @tornado.web.asynchronous
    @gen.coroutine
    def post(self):
        database = self.settings['db']
        data = ujson.loads(self.request.body.decode("utf-8"))
        docs = database.run_stop.find({'run_start': data['run_start']})
        try:
            res = next(docs)
            raise tornado.web.HTTPError(500,
                                        'A run_stop already created for given run_start')
        except StopIteration:
            pass
        jsonschema.validate(data, utils.schemas['run_stop'])

        try:
            result = database.run_stop.insert(data)
        except perr.PyMongoError:
            raise tornado.web.HTTPError(500,
                                        status='Unable to insert the document')
        database.run_stop.create_index([('run_start', pymongo.DESCENDING),
                                        ('uid', pymongo.DESCENDING)],
                                       unique=True)
        database.run_stop.create_index([('time', pymongo.DESCENDING)],
                                       unique=False)
        if not result:
            raise tornado.web.HTTPError(500)
        else:
            utils.return2client(self, data)

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
        query = utils.unpack_params(self)
        docs = database['event'].find(query)
        if not docs:
            raise tornado.web.HTTPError(500,
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
                if _ is not None:
                    bulk.insert(_)
            try:
                bulk.execute()
            except pymongo.errors.BulkWriteError as err:
                raise tornado.web.HTTPError(500, str(err))
            database.event.create_index([('time', pymongo.DESCENDING),
                                         ('descriptor', pymongo.DESCENDING)])
            database.event.create_index([('uid', pymongo.DESCENDING)], unique=True)
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
