from __future__ import absolute_import
import uuid
from pymongo import MongoClient


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
                               '{!r}'.format(val))
        sentinel.insert_one({'collection': col_name, 'version': version})


def create_test_database(host, port=None, version=1,
                         db_template='FS_test_db_{uid}'):
    '''Create and initialize a filestore database for testing

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
