import tornado.ioloop
import tornado.web
from tornado import gen
import pymongo.errors as perr

import ujson
import jsonschema

from metadataservice.server import utils

loop = tornado.ioloop.IOLoop.instance()


class MetadataServiceException(Exception):
    pass

# TODO: Client side methods for insert() and find()


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
        """Abstract method, here to show it exists explicitly.
        Useful for streaming client"""
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
        Identifies whether client provided function name is fair game for post(). If so,
        it returns the appropriate handle from metadatastore
    Queryable()
        Identifies whether client provided function name is fair game for get(). If so,
        it returns the appropriate handle from metadatastore
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        mdsro = self.settings['mdsro']
        mdsrw = self.settings['mdsrw']
        self.queryables = {'find_run_starts': mdsro.find_run_starts,
                           'run_start_given_uid': mdsro.run_start_given_uid}
        self.insertables = {'insert_run_start': mdsrw.insert_run_start}

    def queryable(self, func):
        if func in self.queryables:
            return  self.queryables[func]
        else:
            raise report_error(500, 'Not a valid query routine', func)

    def insertable(self, func):
        if func in self.insertables:
            return self.insertables[func]
        else:
            raise utils.report_error(500, 'Not a valid insert routine', func)

    @tornado.web.asynchronous
    def get(self):
        request = utils.unpack_params(self)
        try:
            sign = request['signature']
            func = self.queryable(sign)
        except KeyError:
            raise utils.report_error(400,
                                     'No valid query function provided!')
        try:
            query = request['query']
        except KeyError:
            raise utils.report_error(400,
                                     'A query string must be provided')
        docs_gen = func(**query)
        utils.transmit_list(self, list(docs_gen))

    @tornado.web.asynchronous
    def post(self):
        payload = ujson.loads(self.request.body.decode("utf-8"))
        try:
            data = payload.pop('data')
        except KeyError:
            raise utils.report_error(400, 'No data provided to insert ')
        try:
            sign = payload.pop('signature')
        except KeyError:
            raise utils.report_error(400, 'No signature provided for insert')
        func = self.insertable(sign)
        try:
            func(**data)
        except RuntimeError as err:
            raise utils.report_error(500, err, data)
        self.write(ujson.dumps({"status": True}))
        self.finish()

    @tornado.web.asynchronous
    def put(self):
        raise tornado.web.HTTPError(403,
                                    status='Not allowed on server')

    @tornado.web.asynchronous
    def delete(self):
        raise tornado.web.HTTPError(403,
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
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        mdsro = self.settings['mdsro']
        mdsrw = self.settings['mdsrw']
        self.queryables = {'descriptor_given_uid': mdsro.descriptor_given_uid,
                           'descriptor_by_start': mdsro.descriptor_by_start,
                           'find_descriptors': mdsro.find_descriptors}
        self.insertables = {'insert_descriptor': mdsrw.insert_descriptor}

    def queryable(self, func):
        if func in self.queryables:
            return  self.queryables[func]
        else:
            raise report_error(400, 'Not a valid query routine', func)


    @tornado.web.asynchronous
    def get(self):
        request = utils.unpack_params(self)
        try:
             sign = request['signature']
             func = self.queryable(sign)
        except KeyError:
            raise utils._compose_error(400,
                                       'No valid query function provided!')
        try:
            query = request['query']
        except KeyError:
            raise utils.report_error(400,
                                     'A query string must be provided')
        docs_gen = func(**query)
        utils.transmit_list(self, list(docs_gen))

    @tornado.web.asynchronous
    def post(self):
        pass

    @tornado.web.asynchronous
    def put(self):
        raise tornado.web.HTTPError(403,
                                    status='Not allowed on server')

    @tornado.web.asynchronous
    @gen.coroutine
    def delete(self):
        raise tornado.web.HTTPError(403, status='Not allowed on server')


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
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        mdsro = self.settings['mdsro']
        mdsrw = self.settings['mdsrw']
        self.queryables = {'stop_by_start': mdsro.stop_by_start,
                           'run_stop_given_uid': mdsro.run_stop_given_uid,
                           'find_run_stops': mdsro.find_run_stops}
        self.insertables = {'insert_run_stop': mdsrw.insert_run_stop}

    def queryable(self, func):
        if func in self.queryables:
            return  self.queryables[func]
        else:
            raise report_error(500, 'Not a valid query routine', func)


    @tornado.web.asynchronous
    def get(self):
        request = utils.unpack_params(self)
        try:
             sign = request['signature']
             func = self.queryable(sign)
        except KeyError:
            raise utils._compose_error(400,
                                       'No valid query function provided!')
        try:
            query = request['query']
        except KeyError:
            raise utils._compose_error(400,
                                       'A query string must be provided')
        docs_gen = func(**query)
        utils.transmit_list(self, list(docs_gen))

    @tornado.web.asynchronous
    def post(self):
        pass

    @tornado.web.asynchronous
    def put(self):
        raise tornado.web.HTTPError(403, 'Not allowed in the server')

    @tornado.web.asynchronous
    def delete(self):
        raise tornado.web.HTTPError(403, 'Not allowed in the server')


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
    def __init__(self, *args, **kwargs):
        # TODO: Special case get_events_generator/table
        super().__init__(*args, **kwargs)
        mdsro = self.settings['mdsro']
        mdsrw = self.settings['mdsrw']
        self.queryables = {'get_events_generator': mdsro.get_events_generator,
                           'get_events_table': mdsro.get_events_table,
                           'find_events': mdsro.find_events}
        self.insertables = {'insert_descriptor': mdsrw.insert_descriptor,
                            'bulk_insert_events': mdsrw.bulk_insert_events}

    def queryable(self, func):
        if func in self.queryables:
            return  self.queryables[func]
        else:
            raise report_error(400, 'Not a valid query routine', func)


    @tornado.web.asynchronous
    def get(self):
        request = utils.unpack_params(self)
        try:
             sign = request['signature']
             func = self.queryable(sign)
        except KeyError:
            raise utils._compose_error(400,
                                       'No valid query function provided!')
        try:
            query = request['query']
        except KeyError:
            raise utils.report_error(400,
                                     'A query string must be provided')
        docs_gen = func(**query)
        utils.transmit_list(self, list(docs_gen))

    @tornado.web.asynchronous
    def post(self):
        pass

    @tornado.web.asynchronous
    def put(self):
        raise tornado.web.HTTPError(403, 'Not allowed in the server')

    @tornado.web.asynchronous
    def delete(self):
        raise tornado.web.HTTPError(403, 'Not allowed in the server')
