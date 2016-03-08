import uuid
from metadatastore.api import db_connect, db_disconnect
from metadatastore import conf
from copy import deepcopy

conn = None
testing_config = {
    'database': "mds_testing_disposable_{}".format(str(uuid.uuid4())),
    'host': 'localhost',
    'port': 27017,
    'timezone': 'US/Eastern'}

old_connection_info = None


def mds_setup():
    "Create a fresh database with unique (random) name."
    global conn
    global old_connection_info
    old_connection_info = deepcopy(conf.connection_config)
    conf.connection_config = testing_config
    db_disconnect()
    conn = db_connect(**testing_config)


def mds_teardown():
    "Drop the fresh database and disconnect."
    global old_connection_info
    conf.connection_config = deepcopy(old_connection_info)
    conn.drop_database(testing_config['database'])
    db_disconnect()
