import uuid
from subprocess import Popen
from pymongo import MongoClient
import time as ttime

TESTING_CONFIG = {
    'database': "mds_testing_disposable_{}".format(str(uuid.uuid4())),
    'host': 'localhost',
    'mongo_server': 'localhost',
    'mongo_port': 27017,
    'timezone': 'US/Eastern'}

SERVICE_PROCESS = None


def mds_setup():
    global SERVICE_PROCESS

    tc = dict(TESTING_CONFIG)

    SERVICE_PROCESS = Popen(['start_mdservice',
                             '--database={}'.format(tc['database']),
                             '--host={}'.format(tc['mongo_server']),
                             '--port={}'.format(tc['mongo_port'])])

    # sleep to make sure server starts
    ttime.sleep(1)


def mds_teardown():
    global SERVICE_PROCESS
    if SERVICE_PROCESS is not None:
        SERVICE_PROCESS.terminate()
    SERVICE_PROCESS = None

    conn = MongoClient(TESTING_CONFIG['mongo_server'],
                       TESTING_CONFIG.get('mongo_port', None))
    conn.drop_database(TESTING_CONFIG['database'])
