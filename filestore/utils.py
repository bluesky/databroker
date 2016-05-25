from __future__ import absolute_import
import six
import os
from pymongo import MongoClient


if six.PY2:
    # http://stackoverflow.com/a/5032238/380231
    def _make_sure_path_exists(path):
        import errno
        try:
            os.makedirs(path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise
else:
    # technically, this won't work with py3.1, but no one uses that
    def _make_sure_path_exists(path):
        return os.makedirs(path, exist_ok=True)


def install_sentinels(config, version):
    '''Install version sentinel collection

    Parameters
    ----------
    config : dict
        Must have keys {'host', 'database'}, may have key 'port'

    version : int
        The version of the schema this db uses
    '''
    conn = MongoClient(config['host'],
                       config.get('port', None))
    db = conn.get_database(config['database'])
    sentinel = db.get_collection('sentinel')

    versioned_collection = ['resource', 'datum']
    for col_name in versioned_collection:
        val = sentinel.find_one({'collection': col_name})
        if val is not None:
            raise RuntimeError('This database already has sentinel '
                               '{!r}'.forman(val))
        sentinel.insert_one({'collection': col_name, 'version': version})
