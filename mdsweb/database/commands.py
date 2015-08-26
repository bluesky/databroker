from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import motor
from functools import wraps
from mdsweb import conf
from .dbschema import *
#TODO: Implement dbschema classes


def db_connect(database ,host, port):
    """Helper function to deal with stateful connections to motor. Connection established lazily.
    Asycnc so do not treat like mongonengine connection pool.
    Tornado needs both client and database so this routine returns both. Once we figure out how to use tornado and motor
    properly, we may need to fix this.

    Parameters
    ----------
    database: str
        The name of the database that data is stored
    host: str
        Name/address of the server that mongo daemon lives
    port: int
        Port num of the server

    Returns
    -------
    client: motor.MotorClient
        Asycnchronous motor client
    db: motor.MotorDatabase
        Async database object

    """
    client = motor.MotorClient()
    db = client[database]
    return (client, db)


def _ensure_connection(func):
    @wraps(func)
    def inner(*args, **kwargs):
        database = conf.connection_config['database']
        host = conf.connection_config['host']
        port = conf.connection_config['port']
        client, db = db_connect(database, host, port)
        client._ensure_connected()



def insert_run_starts():
    pass

def insert_run_stop():
    pass

def insert_beamline_config():
    pass

def insert_event_descriptor():
    pass

def insert_event():
    pass

def _bulk_event_write():
    pass

def _get_and_dump_header():
    pass

def _transpose_and_dump_event():
    pass

#TODO: Pass 'db' to tornado Application to make it available as request handler.
#TODO: Add a callback to run_stop that grabs all related documents and dumps them into the primary data store
#TODO: Authentication/authorization into mongo


