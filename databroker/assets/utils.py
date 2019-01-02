from __future__ import absolute_import
import uuid


def install_sentinels(config, version):
    '''Install version sentinel collection

    Parameters
    ----------
    config : dict
        Must have keys {'host', 'database'}, may have key 'port'

    version : int
        The version of the schema this db uses
    '''
    from pymongo import MongoClient

    conn = MongoClient(config['host'],
                       config.get('port', None))
    db = conn.get_database(config['database'])
    sentinel = db.get_collection('sentinel')

    versioned_collection = ['resource', 'datum']
    for col_name in versioned_collection:
        val = sentinel.find_one({'collection': col_name})
        if val is not None:
            raise RuntimeError('This database already has sentinel '
                               '{!r}'.format(val))
        sentinel.insert_one({'collection': col_name, 'version': version})


def create_test_database(host, port=None, version=1,
                         db_template='FS_test_db_{uid}'):
    '''Create and initialize an asset database for testing

    Parameters
    ----------
    host : str
       Host of mongo sever

    port : int, optional
        port running mongo server on host

    version : int, optional
        The schema version to initialize the db with

    db_template : str, optional
        Template for database name.  Must have one format entry with the key
        'uid'.

    Returns
    -------
    config : dict
        Configuration dictionary for the new database.

    '''
    db_name = db_template.format(uid=str(uuid.uuid4()))
    config = {'host': host, 'port': port, 'database': db_name}
    install_sentinels(config, version)

    return config

try:
    from collections import ChainMap as _ChainMap
except ImportError:
    class _ChainMap(object):
        def __init__(self, primary, fallback=None):
            if fallback is None:
                fallback = {}
            self.fallback = fallback
            self.primary = primary

        def __getitem__(self, k):
            try:
                return self.primary[k]
            except KeyError:
                return self.fallback[k]

        def __setitem__(self, k, v):
            self.primary[k] = v

        def __contains__(self, k):
            return k in self.primary or k in self.fallback

        def __delitem__(self, k):
            del self.primary[k]

        def pop(self, k, v):
            return self.primary.pop(k, v)

        @property
        def maps(self):
            return [self.primary, self.fallback]

        @property
        def parents(self):
            return self.fallback

        def new_child(self, m=None):
            if m is None:
                m = {}

            return _ChainMap(m, self)

        def __iter__(self):
            for k in set(self.primary) | set(self.fallback):
                yield k

        def __len__(self):
            return len(set(self.primary) | set(self.fallback))

        def get(self, k, dflt=None):
            try:
                return self[k]
            except KeyError:
                return dflt
