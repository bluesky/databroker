import os
import sqlite3
import six
import re
from six.moves import queue
import threading
from collections import defaultdict
from contextlib import contextmanager
from .mongoquery import JSONCollection
from .base import MDSTemplate, MDSROTemplate
from .core import ASCENDING, DESCENDING
from ..utils import ensure_path_exists

LIST_TABLES = "SELECT name FROM sqlite_master WHERE type='table';"
CREATE_TABLE = "CREATE TABLE %s "
INSERT = "INSERT INTO ? VALUES "  # the rest is generated by qmarks func below
SELECT_EVENT_STREAM = "SELECT * FROM %s "

if six.PY2:

    class TimeoutError(Exception):
        pass


@contextmanager
def cursor(connection):
    """
    a context manager for a sqlite cursor

    Example
    -------
    >>> with cursor(conn) as c:
    ...     c.execute(query)
    """
    c = connection.cursor()
    try:
        yield c
    except BaseException as e:
        connection.rollback()
        raise e
    else:
        connection.commit()
    finally:
        c.close()


def qmarks(num):
    "Generate string like (?, ?, ?)"
    return '(' + '?, ' * (num - 1) + '?)'


class RunStartCollection(JSONCollection):
    def __init__(self, event_col, *args, **kwargs):
        self._event_col = event_col
        super(RunStartCollection, self).__init__(*args, **kwargs)

    def insert_one(self, doc):
        self._event_col.new_runstart(doc)
        super(RunStartCollection, self).insert_one(doc, fk='uid')


class DescriptorCollection(JSONCollection):
    def __init__(self, event_col, *args, **kwargs):
        self._event_col = event_col
        super(DescriptorCollection, self).__init__(*args, **kwargs)

    def insert_one(self, doc):
        self._event_col.new_descriptor(doc)
        super(DescriptorCollection, self).insert_one(doc)


class EventCollection(object):
    def __init__(self, dirpath):
        self._runstarts = {}
        self._descriptors = {}
        self._dirpath = dirpath
        # Create a special thread for interacting with sqlite. This thread will
        # create all connections and do all insertions.
        self.__process_request_queue_thread = threading.Thread(
            target=self.__process_request_queue,
            name='process-request-queue')
        # In Python 2, this must be set by attribute, not in Thread.__init__.
        self.__process_request_queue_thread.daemon = True
        self.__request_queue = queue.Queue()
        self.__shutdown_event = threading.Event()
        self.__process_request_queue_thread.start()

    def reconnect(self):
        for fn in os.listdir(self._dirpath):
            # Cache connections to every sqlite file.
            match = re.match(r'([0-9a-z-]+)\.sqlite', fn)
            if match is None:
                # skip unrecognized file
                continue
            uid, = match.groups()
            fp = os.path.join(self._dirpath, fn)
            conn = sqlite3.connect(fp, timeout=30.0)
            # Return rows as objects that support getitem.
            conn.row_factory = sqlite3.Row
            self._runstarts[uid] = conn

            # Build a mapping of descriptor uids to run start uids.
            with cursor(self._runstarts[uid]) as c:
                c.execute(LIST_TABLES)
                for descriptor_uid in c.fetchall():
                    duid = descriptor_uid['name'][5:].replace('_', '-')
                    self._descriptors[duid] = uid

    @classmethod
    def columns(cls, keys):
        sorted_keys = list(sorted(keys))
        safe_keys = [key.replace('-', '_') for key in sorted_keys]
        columns = tuple(['uid', 'seq_num', 'time'] +
                        ['data_' + key for key in safe_keys] +
                        ['timestamps_' + key for key in safe_keys])
        return columns

    def new_runstart(self, doc):
        # Use a threading.Event (nothing to do with an Event document) to
        # detect when the document has been inserted.
        success_event = threading.Event()
        ret = {}
        self.__request_queue.put((success_event, 'start', doc, ret))
        # Timeout after 5 seconds because that is how long sqlite takes to
        # timeout, by default.
        success = success_event.wait(timeout=5)
        if not success:
            raise TimeoutError("insertion failed")
        excp = ret.get('exception')
        if excp is not None:
            raise excp

    def new_descriptor(self, doc):
        success_event = threading.Event()
        ret = {}
        self.__request_queue.put((success_event, 'descriptor', doc, ret))
        success = success_event.wait(timeout=5)
        if not success:
            raise TimeoutError("descriptor %s insertion failed" % id(doc))
        excp = ret.get('exception')
        if excp is not None:
            raise excp

    def find(self, query, sort=None):
        # FIXME: sort is a no-op
        if list(query.keys()) != ['descriptor']:
            raise NotImplementedError("Only queries based on descriptor uid "
                                      "are supported.")
        results_queue = queue.Queue()
        ret = {}
        self.__request_queue.put((None, 'query', (query, results_queue), ret))
        results = results_queue.get(timeout=5)

        for result in results:
            yield result
        excp = ret.get('exception')
        if excp is not None:
            raise excp

    def _find(self, request):
        query, results_queue = request
        desc_uid = query['descriptor']
        table_name = 'desc_' + desc_uid.replace('-', '_')
        with cursor(self._runstarts[self._descriptors[desc_uid]]) as c:
            c.execute(SELECT_EVENT_STREAM % table_name)
            raw = c.fetchall()
        rows_as_dicts = [dict(row) for row in raw]
        results = []
        for row in rows_as_dicts:
            event = {}
            event['uid'] = row.pop('uid')
            event['seq_num'] = row.pop('seq_num')
            event['time'] = row.pop('time')
            event['data'] = {}
            event['timestamps'] = {}
            for k, v in row.items():
                if k.startswith('data_'):
                    new_key = k[len('data_'):]
                    event['data'][new_key] = v
                else:
                    new_key = k[len('timestamps_'):]
                    event['timestamps'][new_key] = v
            results.append(event)
        results_queue.put(results)

    def find_one(self, query):
        # not used on event_col
        raise NotImplementedError()

    def insert_one(self, doc):
        self.insert([doc])

    def insert_many(self, docs):
        self.insert(docs)

    def insert(self, docs):
        success_event = threading.Event()
        ret = {}
        self.__request_queue.put((success_event, 'bulk_event', docs, ret))
        success = success_event.wait(timeout=5)
        if not success:
            raise TimeoutError("event %s insertion failed" % id(docs))
        excp = ret.get('exception')
        if excp is not None:
            raise excp

    def __process_request_queue(self):
        self.reconnect()
        while not self.__shutdown_event.is_set():
            try:
                item = self.__request_queue.get(timeout=0.5)
            except queue.Empty:
                # Check whether we are shutting down (and should therefore
                # terminate this loop) and then resume waiting on the
                # queue.
                continue
            try:
                success_event, name, payload, ret = item
                if name == 'bulk_event':
                    self._insert_events(payload)
                elif name == 'descriptor':
                    self._insert_descriptor(payload)
                elif name == 'start':
                    self._insert_start(payload)
                elif name == 'query':
                    self._find(payload)
                else:
                    raise NotImplementedError()
            except Exception as e:
                ret['exception'] = e
            if success_event is not None:
                success_event.set()

            # Signal to thread that put into __request_queue that the insertion is
            # done.

    def _insert_events(self, docs):
        values = defaultdict(list)
        ordered_keys = {}
        columns = {}
        for doc in docs:
            # Stash an arbitrary but consistent order for the keys.
            uid = doc['descriptor']
            if uid not in ordered_keys:
                ordered_keys[uid] = sorted(doc['data'])
                columns[uid] = self.columns(doc['data'])

            value = tuple([doc['uid']] + [doc['seq_num']] + [doc['time']] +
                          [doc['data'][k] for k in ordered_keys[uid]] +
                          [doc['timestamps'][k] for k in ordered_keys[uid]])
            values[uid].append(value)
        for desc_uid in values:
            table_name = 'desc_' + desc_uid.replace('-', '_')
            cols = columns[desc_uid]
            run_start_uid = self._descriptors[desc_uid]
            run_start = self._runstarts[run_start_uid]
            with cursor(run_start) as c:
                c.executemany("INSERT INTO %s (%s) VALUES %s" %
                              (table_name, ','.join(cols), qmarks(len(cols))),
                              values[desc_uid])

    def _insert_descriptor(self, doc):
        uid = doc['uid']
        table_name = 'desc_' + uid.replace('-', '_')
        run_start_uid = doc['run_start']
        columns = self.columns(doc['data_keys'])
        with cursor(self._runstarts[run_start_uid]) as c:
            c.execute(CREATE_TABLE % table_name
                      + '(' + ','.join(columns) + ')')
        self._descriptors[uid] = run_start_uid

    def _insert_start(self, doc):
        uid = doc['uid']
        fp = os.path.join(self._dirpath, '{}.sqlite'.format(uid))
        conn = sqlite3.connect(fp, timeout=30.0)
        conn.row_factory = sqlite3.Row
        self._runstarts[uid] = conn

    def __del__(self):
        self.__shutdown_event.set()  # Poison the __process_request_queue_thread.
        self.__process_request_queue_thread.join()


class _CollectionMixin(object):
    def __init__(self, *args, **kwargs):
        self._config = None
        super(_CollectionMixin, self).__init__(*args, **kwargs)
        self.__event_col = None
        self.__descriptor_col = None
        self.__runstart_col = None
        self.__runstop_col = None
        ensure_path_exists(self._config['directory'])

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, val):
        self._config = val
        self.__event_col = None
        self.__descriptor_col = None
        self.__runstart_col = None
        self.__runstop_col = None

    @property
    def _runstart_col(self):
        if self.__runstart_col is None:
            fp = os.path.join(self.config['directory'], 'run_starts.json')
            self.__runstart_col = RunStartCollection(self._event_col, fp)
        return self.__runstart_col

    @property
    def _runstop_col(self):
        if self.__runstop_col is None:
            fp = os.path.join(self.config['directory'], 'run_stops.json')
            self.__runstop_col = JSONCollection(fp)
        return self.__runstop_col

    @property
    def _descriptor_col(self):
        self._event_col
        if self.__descriptor_col is None:
            fp = os.path.join(self.config['directory'],
                              'event_descriptors.json')
            self.__descriptor_col = DescriptorCollection(self._event_col, fp)
        return self.__descriptor_col

    @property
    def _event_col(self):
        if self.__event_col is None:
            self.__event_col = EventCollection(self.config['directory'])
        return self.__event_col


class MDSRO(_CollectionMixin, MDSROTemplate):
    pass


class MDS(_CollectionMixin, MDSTemplate):
    pass
