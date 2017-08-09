from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import six  # noqa
import sqlite3
import json
from contextlib import contextmanager
from .base_registry import (RegistryTemplate, BaseRegistryRO, _ChainMap,
                            RegistryMovingTemplate)


LIST_TABLES = "SELECT name FROM sqlite_master WHERE type='table';"
CREATE_RESOURCES_TABLE = """
CREATE TABLE Resources(
    uid TEXT PRIMARY KEY NOT NULL,
    spec TEXT NOT NULL,
    resource_path TEXT NOT NULL,
    root TEXT NOT NULL,
    path_semantics TEXT NOT NULL,
    resource_kwargs BLOB NOT NULL
);"""
CREATE_DATUMS_TABLE = """
CREATE TABLE Datums(
    datum_id TEXT PRIMARY KEY NOT NULL,
    datum_kwargs BLOB NOT NULL,
    resource TEXT NOT NULL,
    FOREIGN KEY(resource) REFERENCES Resources(uid)
);"""
CREATE_RESOURCE_UPDATES_TABLE = """
CREATE TABLE ResourceUpdates(
    resource TEXT,
    old BLOB NOT NULL,
    new BLOB NOT NULL,
    time FLOAT NOT NULL,
    cmd TEXT NOT NULL,
    cmd_kwargs BLOB NOT NULL,
    FOREIGN KEY(resource) REFERENCES Resources(uid)
);"""
INSERT_DATUM = """
INSERT INTO Datums (datum_id, datum_kwargs, resource)
VALUES (?, ?, ?);"""
INSERT_RESOURCE = """
INSERT INTO Resources (uid, spec, resource_path, root, path_semantics,
                       resource_kwargs)
VALUES (?, ?, ?, ?, ?, ?);"""
SELECT_RESOURCE = "SELECT * FROM Resources WHERE uid=?;"
SELECT_DATUM_BY_UID = "SELECT * FROM Datums WHERE datum_id=?;"
SELECT_DATUM_BY_RESOURCE = "SELECT * FROM Datums WHERE resource=?;"
UPDATE_RESOURCE = """
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
    except:
        connection.rollback()
        raise
    else:
        connection.commit()
    finally:
        c.close()


class RegistryDatabase(object):
    def __init__(self, fp):
        self._fp = fp
        self.reconnect()

    def reconnect(self):
        conn = sqlite3.connect(self._fp)
        # Return rows as objects that support getitem.
        conn.row_factory = sqlite3.Row
        self.conn = conn

        with cursor(self.conn) as c:
            c.execute(LIST_TABLES)
            tables = set([row['name'] for row in c.fetchall()])
        if tables == set():
            with cursor(self.conn) as c:
                c.execute(CREATE_RESOURCES_TABLE)
                c.execute(CREATE_DATUMS_TABLE)
                c.execute(CREATE_RESOURCE_UPDATES_TABLE)
        else:
            EXPECTED_TABLES = ['Resources', 'Datums', 'ResourceUpdates']
            if tables != set(EXPECTED_TABLES):
                raise RuntimeError("Database exists at {} but does not "
                                   "have expected schema. Expected "
                                   "tables: {}; found tables: {}".format(
                                       self._fp, EXPECTED_TABLES, tables))

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
        with cursor(self._conn) as c:
            c.execute(INSERT_RESOURCE, [resource[k] for k in keys])

    def replace_one(self, query, resource):
        resource = shadow_with_json(resource, ['resource_kwargs'])
        keys = ['spec', 'resource_path', 'root', 'resource_kwargs', 'uid']
        with cursor(self._conn) as c:
            c.execute(UPDATE_RESOURCE, [resource[k] for k in keys])

    def find_one(self, query):
        with cursor(self._conn) as c:
            c.execute(SELECT_RESOURCE, (query['uid'],))
            raw = c.fetchone()
        if raw is None:
            return None
        doc = dict(raw)
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
