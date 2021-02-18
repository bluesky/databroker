from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from contextlib import contextmanager
import json
import six  # noqa
from six.moves import queue
import sqlite3
import threading

from .base_registry import (RegistryTemplate, BaseRegistryRO, _ChainMap,
                            RegistryMovingTemplate)


RESOURCE_VERSION = 'v2'

LIST_TABLES = "SELECT name FROM sqlite_master WHERE type='table';"

CREATE_RESOURCES_TABLE = """
CREATE TABLE Resources_{}(
    uid TEXT PRIMARY KEY NOT NULL,
    spec TEXT NOT NULL,
    resource_path TEXT NOT NULL,
    root TEXT NOT NULL,
    path_semantics TEXT NOT NULL,
    resource_kwargs BLOB NOT NULL,
    run_start TEXT NOT NULL
);""".format(RESOURCE_VERSION)
CREATE_DATUMS_TABLE = """
CREATE TABLE Datums(
    datum_id TEXT PRIMARY KEY NOT NULL,
    datum_kwargs BLOB NOT NULL,
    resource TEXT NOT NULL,
    FOREIGN KEY(resource) REFERENCES Resources_{}(uid)
);""".format(RESOURCE_VERSION)
CREATE_RESOURCE_UPDATES_TABLE = """
CREATE TABLE ResourceUpdates(
    resource TEXT,
    old BLOB NOT NULL,
    new BLOB NOT NULL,
    time FLOAT NOT NULL,
    cmd TEXT NOT NULL,
    cmd_kwargs BLOB NOT NULL,
    FOREIGN KEY(resource) REFERENCES Resources_{}(uid)
);""".format(RESOURCE_VERSION)

INSERT_DATUM = """
INSERT INTO Datums (datum_id, datum_kwargs, resource)
VALUES (?, ?, ?);"""
INSERT_RESOURCE = """
INSERT INTO Resources_{} (uid, spec, resource_path, root, path_semantics,
                       resource_kwargs, run_start)
VALUES (?, ?, ?, ?, ?, ?, ?);""".format(RESOURCE_VERSION)
OLD_INSERT_RESOURCE = """
INSERT INTO Resources_{} (uid, spec, resource_path, root, path_semantics,
                       resource_kwargs, run_start)
VALUES (?, ?, ?, ?, ?, ?, ?);""".format(RESOURCE_VERSION)

SELECT_RESOURCE = "SELECT * FROM Resources_{} WHERE uid=?;".format(
    RESOURCE_VERSION)
OLD_SELECT_RESOURCE = "SELECT * FROM Resources WHERE uid=?;"
SELECT_DATUM_BY_UID = "SELECT * FROM Datums WHERE datum_id=?;"
SELECT_DATUM_BY_RESOURCE = "SELECT * FROM Datums WHERE resource=?;"

UPDATE_RESOURCE = """
UPDATE Resources_{}
SET
spec=?,
resource_path=?,
root=?,
resource_kwargs=?
WHERE uid=?;""".format(RESOURCE_VERSION)
OLD_UPDATE_RESOURCE = """
UPDATE Resources
SET
spec=?,
resource_path=?,
root=?,
resource_kwargs=?
WHERE uid=?;"""
INSERT_RESOURCE_UPDATE = """
INSERT INTO ResourceUpdates (resource, old, new, time, cmd, cmd_kwargs)
VALUES (?, ?, ?, ?, ?, ?);"""
SELECT_RESOURCE_UPDATES = """
SELECT * FROM ResourceUpdates
WHERE resource=?
ORDER BY time;"""


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
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()
    finally:
        c.close()


class _CursorWrapper(object):
    def __init__(self, work_queue):
        self._wq = work_queue
        self.cursor()

    def __getattr__(self, key):
        def inner(*args, **kwargs):
            finished_event = threading.Event()
            ret = {}
            self._wq.put((finished_event, key, args, kwargs, ret))
            success = finished_event.wait(timeout=.1)
            if not success:
                raise TimeoutError("{key} call timed out".format(key=key))
            excp = ret.get('exception')
            if excp is not None:
                raise excp
            return ret.get('return', None)
        return inner


class _ConnWrapper(object):
    def __init__(self, fp):
        self._fp = fp
        self._cursor_lock = threading.RLock()
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

    def cursor(self):
        self._c = _CursorWrapper(self.__request_queue)
        return self._c

    def __getattr__(self, key):
        def inner(*args, **kwargs):
            finished_event = threading.Event()
            ret = {}
            self.__request_queue.put((finished_event, key, args, kwargs, ret))
            success = finished_event.wait(timeout=.1)
            if not success:
                raise TimeoutError("{key} call timed out".format(key=key))
            excp = ret.get('exception')
            if excp is not None:
                raise excp
            return ret.get('return', None)
        return inner

    def close(self):
        self._c.close()
        self._c = None

    def __process_request_queue(self):
        conn = sqlite3.connect(self._fp, timeout=30.0)
        # Return rows as objects that support getitem.
        conn.row_factory = sqlite3.Row

        with cursor(conn) as c:
            c.execute(LIST_TABLES)
            tables = set([row['name'] for row in c.fetchall()])
        if tables == set():
            with cursor(conn) as c:
                c.execute(CREATE_RESOURCES_TABLE)
                c.execute(CREATE_DATUMS_TABLE)
                c.execute(CREATE_RESOURCE_UPDATES_TABLE)
        else:
            have_tables = False
            for res in ['Resources_{}'.format(RESOURCE_VERSION),
                        'Resources']:
                EXPECTED_TABLES = [res, 'Datums', 'ResourceUpdates']
                if tables == set(EXPECTED_TABLES):
                    have_tables = True
                    break
            if not have_tables:
                raise RuntimeError("Database exists at {} but does not "
                                   "have expected schema. Expected "
                                   "tables: {}; found tables: {}".format(
                                       self._fp, EXPECTED_TABLES, tables))
        cur_cursor = None
        while not self.__shutdown_event.is_set():
            finished_event = None
            try:
                item = self.__request_queue.get(timeout=0.5)
            except queue.Empty:
                # Check whether we are shutting down (and should therefore
                # terminate this loop) and then resume waiting on the
                # queue.
                continue
            try:
                finished_event, name, args, kwargs, ret = item
            except ValueError:
                print("did not get the right number of values in {item}".
                      format(item=item))
                continue
            try:
                # handle connection level stuff
                if name == 'cursor':
                    if cur_cursor is not None:
                        raise RuntimeError
                    cur_cursor = conn.cursor()
                elif name == 'rollback':
                    conn.rollback()
                elif name == 'commit':
                    conn.commit()
                # and special case close as it touching local sate
                elif name == 'close':
                    if cur_cursor is not None:
                        cur_cursor.close()
                    cur_cursor = None
                # pass everything else through to the cursor
                else:
                    if cur_cursor is None:
                        raise RuntimeError
                    ret['return'] = getattr(cur_cursor, name)(*args, **kwargs)
            except Exception as e:
                ret['exception'] = e
            finally:
                if finished_event is not None:
                    # Signal to thread that put into __request_queue
                    # that we are done trying to handle it
                    finished_event.set()


class RegistryDatabase(object):
    def __init__(self, fp):
        self._fp = fp
        self.reconnect()

    def reconnect(self):
        self.conn = _ConnWrapper(self._fp)

    def disconnect(self):
        self.conn.close()
        self.conn = None


def shadow_with_json(d, keys):
    """Shadow keys of a dict with JSON-string replacements."""
    return _ChainMap({key: json.dumps(d[key]) for key in keys}, d)


class DatumCollection(object):
    def __init__(self, conn):
        self._conn = conn

    def insert_one(self, datum):
        datum = shadow_with_json(datum, ['datum_kwargs'])
        keys = ['datum_id', 'datum_kwargs', 'resource']
        with cursor(self._conn) as c:
            c.execute(INSERT_DATUM, [datum[k] for k in keys])

    def insert(self, datums):
        datums = map(lambda d: shadow_with_json(d, ['datum_kwargs']), datums)
        keys = ['datum_id', 'datum_kwargs', 'resource']
        with cursor(self._conn) as c:
            c.executemany(INSERT_DATUM, ([d[k] for k in keys] for d in datums))

    def find_one(self, query):
        with cursor(self._conn) as c:
            c.execute(SELECT_DATUM_BY_UID, (query['datum_id'],))
            raw = c.fetchone()
        if raw is None:
            return None
        doc = dict(raw)
        doc['datum_kwargs'] = json.loads(doc['datum_kwargs'])
        return doc

    def find(self, query):
        with cursor(self._conn) as c:
            c.execute(SELECT_DATUM_BY_RESOURCE, (query['resource'],))
            raw = c.fetchall()
        for row in raw:
            doc = dict(row)
            doc['datum_kwargs'] = json.loads(doc['datum_kwargs'])
            yield doc


class ResourceUpdatesCollection(object):
    _JSONIFY_KEYS = ['old', 'new', 'cmd_kwargs']

    def __init__(self, conn):
        self._conn = conn

    def insert_one(self, log_object):
        log_object = shadow_with_json(log_object, self._JSONIFY_KEYS)
        keys = ['resource', 'old', 'new', 'time', 'cmd', 'cmd_kwargs']
        with cursor(self._conn) as c:
            c.execute(INSERT_RESOURCE_UPDATE, [log_object[k] for k in keys])

    def find(self, query):
        with cursor(self._conn) as c:
            c.execute(SELECT_RESOURCE_UPDATES, (query['resource'],))
            raw = c.fetchall()
        for row in raw:
            doc = dict(row)
            for key in self._JSONIFY_KEYS:
                doc[key] = json.loads(doc[key])
            yield doc


class ResourceCollection(object):
    def __init__(self, conn):
        self._conn = conn

    def insert_one(self, resource):
        resource = shadow_with_json(resource, ['resource_kwargs'])
        keys = ['uid', 'spec', 'resource_path', 'root', 'path_semantics',
                'resource_kwargs']
        data = [resource[k] for k in keys]
        data.append(resource.get('run_start', 'THISISNOTARUNSTART'))
        # Check if we inserted properly, else raise
        inserted = False
        for insert in [INSERT_RESOURCE, OLD_INSERT_RESOURCE]:
            try:
                with cursor(self._conn) as c:
                    c.execute(insert, data)
            except sqlite3.ProgrammingError:
                pass
            else:
                inserted = True
                break
        if not inserted:
            raise RuntimeError('No databases were found to insert into')


    def replace_one(self, query, resource):
        resource = shadow_with_json(resource, ['resource_kwargs'])
        keys = ['spec', 'resource_path', 'root', 'resource_kwargs', 'uid']

        # Check if we inserted properly, else raise
        inserted = False
        for cmd in [UPDATE_RESOURCE, OLD_UPDATE_RESOURCE]:
            try:
                with cursor(self._conn) as c:
                    c.execute(cmd, [resource[k] for k in keys])
            except sqlite3.ProgrammingError:
                pass
            else:
                inserted = True
                break
        if not inserted:
            raise RuntimeError('No databases were found to update')

    def find_one(self, query):
        # Cycle through the resource tables if we can't find something look
        # it up in an older one.
        for select in [SELECT_RESOURCE, OLD_SELECT_RESOURCE]:
            with cursor(self._conn) as c:
                c.execute(select, (query['uid'],))
                raw = c.fetchone()
                if raw is not None:
                    break
        if raw is None:
            return None
        doc = dict(raw)
        if doc['run_start'] == 'THISISNOTARUNSTART':
            doc.pop('run_start')
        doc['resource_kwargs'] = json.loads(doc['resource_kwargs'])
        return doc


class RegistryRO(BaseRegistryRO):
    REQ_CONFIG = ('dbpath', )

    def __init__(self, *args, **kwargs):
        self._config = None
        super(RegistryRO, self).__init__(*args, **kwargs)
        self.__db = None
        self.__resource_col = None
        self.__resource_update_col = None
        self.__datum_col = None

    def disconnect(self):
        self.__resource_col = None
        self.__resource_update_col = None
        self.__datum_col = None
        if self.__db:
            self.__db.disconnect()
        self.__db = None

    @property
    def _db(self):
        if self.__db is None:
            self.__db = RegistryDatabase(self.config['dbpath'])
        return self.__db

    @property
    def _resource_col(self):
        if self.__resource_col is None:
            self.__resource_col = ResourceCollection(self._db.conn)
        return self.__resource_col

    @property
    def _resource_update_col(self):
        if self.__resource_update_col is None:
            self.__resource_update_col = ResourceUpdatesCollection(
                self._db.conn)
        return self.__resource_update_col

    @property
    def _datum_col(self):
        if self.__datum_col is None:
            self.__datum_col = DatumCollection(self._db.conn)
        return self.__datum_col

    @property
    def DuplicateKeyError(self):
        return sqlite3.IntegrityError


class Registry(RegistryRO, RegistryTemplate):
    pass


class RegistryMoving(Registry, RegistryMovingTemplate):
    pass
