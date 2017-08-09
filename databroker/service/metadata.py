import tornado.ioloop
import tornado.web
import ujson
import json
import types

from ..headersource.core import NoRunStop, NoEventDescriptors, NoRunStart

loop = tornado.ioloop.IOLoop.instance()


def unpack_params(handler):
    """Unpacks the queries from the body of the header
    Parameters
    ----------
    handler: tornado.web.RequestHandler
        Handler for incoming request to collection

    Returns dict
    -------
        Unpacked query in dict format.
    """
    if isinstance(handler, tornado.web.RequestHandler):
        return ujson.loads(list(handler.request.arguments.keys())[0])
    else:
        raise TypeError("Handler provided must be of "
                        "tornado.web.RequestHandler type")


def report_error(code, status, m_str=''):
    """Compose and raise an HTTPError message"""
    fmsg = str(status) + ' ' + str(m_str)
    raise tornado.web.HTTPError(status_code=code, reason=fmsg)


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

    def queryable(self, func):
        try:
            return self.queryables[func]
        except KeyError as err:
            report_error(500, 'Provided query method {} is not supported'.format(func))

    def insertable(self, func):
        try:
            return self.insertables[func]
        except KeyError as err:
            report_error(500, 'Not a valid insert routine', func)

    @tornado.web.asynchronous
    def get(self):
        request = unpack_params(self)
        try:
            sign = request['signature']
        except KeyError:
            report_error(400,
                               'No valid signature function provided!')
        try:
            func = self.queryable(sign)
        except AttributeError as err:
            report_error(500, err)
        try:
            query = request['query']
        except KeyError:
            report_error(400,
                               'A query string must be provided')
        try:
            docs_gen = func(**query)
        except (NoRunStop, NoRunStart, NoEventDescriptors):
           docs_gen = []
        if isinstance(docs_gen, (dict, list)):
            self.write(json.dumps(docs_gen))
        elif isinstance(docs_gen, types.GeneratorType):
            self.write(json.dumps(list(docs_gen)))
        self.finish()

    @tornado.web.asynchronous
    def post(self):
        payload = ujson.loads(self.request.body.decode("utf-8"))
        try:
            data = payload.pop('data')
        except KeyError:
            report_error(400, 'No data provided to insert ')
        try:
            sign = payload.pop('signature')
        except KeyError:
            report_error(400, 'No signature provided for insert')
        try:
            func = self.insertable(sign)
        except AttributeError as err:
            report_error(500, err)
        try:
            func(**data)
        except (RuntimeError, TypeError, KeyError) as err:
            report_error(500, err, data)
        self.write(ujson.dumps({"status": True}))
        self.finish()

    @tornado.web.asynchronous
    def put(self):
        report_error(403, 'Not allowed on server')

    @tornado.web.asynchronous
    def delete(self):
        report_error(403, 'Not allowed on server')


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
    queryable()
        Identifies whether client provided function name is fair game for get(). If so,
        it returns the appropriate handle from metadatastore
    """
    def __init__(self, *args, **kwargs):
        super(RunStartHandler, self).__init__(*args, **kwargs)
        mdsro = self.settings['mdsro']
        mdsrw = self.settings['mdsrw']
        self.queryables = {'find_run_starts': mdsro.find_run_starts,
                           'run_start_given_uid': mdsro.run_start_given_uid,
                           'find_last': mdsro.find_last}
        self.insertables = {'insert_run_start': mdsrw.insert_run_start}


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
    insertable()
        Identifies whether client provided function name is fair game for post(). If so,
        it returns the appropriate handle from metadatastore
    queryable()
        Identifies whether client provided function name is fair game for get(). If so,
        it returns the appropriate handle from metadatastore
    """
    def __init__(self, *args, **kwargs):
        super(EventDescriptorHandler, self).__init__(*args, **kwargs)
        mdsro = self.settings['mdsro']
        mdsrw = self.settings['mdsrw']
        self.queryables = {'descriptor_given_uid': mdsro.descriptor_given_uid,
                           'descriptors_by_start': mdsro.descriptors_by_start,
                           'find_descriptors': mdsro.find_descriptors}
        self.insertables = {'insert_descriptor': mdsrw.insert_descriptor}


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
    insertable()
        Identifies whether client provided function name is fair game for post(). If so,
        it returns the appropriate handle from metadatastore
    queryable()
        Identifies whether client provided function name is fair game for get(). If so,
        it returns the appropriate handle from metadatastore
    """
    def __init__(self, *args, **kwargs):
        super(RunStopHandler, self).__init__(*args, **kwargs)
        mdsro = self.settings['mdsro']
        mdsrw = self.settings['mdsrw']
        self.queryables = {'stop_by_start': mdsro.stop_by_start,
                           'run_stop_given_uid': mdsro.run_stop_given_uid,
                           'find_run_stops': mdsro.find_run_stops}
        self.insertables = {'insert_run_stop': mdsrw.insert_run_stop}


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
    insertable()
        Identifies whether client provided function name is fair game for post(). If so,
        it returns the appropriate handle from metadatastore
    queryable()
        Identifies whether client provided function name is fair game for get(). If so,
        it returns the appropriate handle from metadatastore
    """
    def __init__(self, *args, **kwargs):
        super(EventHandler, self).__init__(*args, **kwargs)
        mdsro = self.settings['mdsro']
        mdsrw = self.settings['mdsrw']
        self.queryables = {'get_events_generator': mdsro.get_events_generator,
                           'get_events_table': mdsro.get_events_table,
                           'find_events': mdsro.find_events}
        self.insertables = {'insert_event': mdsrw.insert_event,
                            'bulk_insert_events': mdsrw.bulk_insert_events
                            }
