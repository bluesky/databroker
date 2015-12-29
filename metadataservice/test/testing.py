import uuid
from copy import deepcopy

conn = None
testing_config = {
    'database': "mds_testing_disposable_{}".format(str(uuid.uuid4())),
    'host': 'localhost',
    'port': 7770,
    'timezone': 'US/Eastern'}

old_connection_info = None


def mds_setup():
    "Create a fresh database with unique (random) name."
    global conn
    global old_connection_info
    old_connection_info = deepcopy(conf.connection_config)
    conf.connection_config = testing_config
    conn = server_connect(testing_config['database'], testing_config['host'],
                      testing_config['port'])


def mds_teardown():
    "Drop the fresh database and disconnect."
    global old_connection_info
    conf.connection_config = deepcopy(old_connection_info)
    #TODO: Shutdown tornado server created
