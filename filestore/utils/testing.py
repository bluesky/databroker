import uuid
from filestore.api import db_connect, db_disconnect
import filestore.conf as fconf

conn = None
db_name = "fs_testing_disposable_{}".format(str(uuid.uuid4()))
old_conf = dict(fconf.connection_config)


def fs_setup():
    "Create a fresh database with unique (random) name."
    global conn
    db_disconnect()
    test_conf = dict(database=db_name, host='localhost',
                     port=27017)
    conn = db_connect(**test_conf)
    old_conf.clear()
    old_conf.update(fconf.connection_config)
    fconf.connection_config.clear()
    fconf.connection_config.update(test_conf)


def fs_teardown():
    "Drop the fresh database and disconnect."
    conn.drop_database(db_name)
    db_disconnect()
    fconf.connection_config.clear()
    fconf.connection_config.update(old_conf)
