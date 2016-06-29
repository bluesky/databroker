import uuid
import requests
import uuid
import time as ttime
from subprocess import Popen
import os
from pymongo import MongoClient

testing_config = dict(mongohost='localhost', mongoport=27017,
                      database='mds_test'+str(uuid.uuid4()),
                      serviceport=9009, tzone='US/Eastern')


def mds_setup():
    global proc
    f = os.path.dirname(os.path.realpath(__file__))
    proc = Popen(["python", "../../startup.py", "--mongo-host", testing_config["mongohost"],
                  "--mongo-port", str(testing_config['mongoport']), "--database", testing_config['database'],
                  "--timezone", testing_config['tzone'], "--service-port",
           str(testing_config['serviceport'])], cwd=f)
    print('Started the server with configuration..:{}'.format(testing_config))
    ttime.sleep(5) # make sure the process is started

def mds_teardown():
    proc2 = Popen(['kill', '-9', str(proc.pid)])
    ttime.sleep(5) # make sure the process is killed
    conn = MongoClient(host=testing_config['mongohost'], port=testing_config['mongoport'])
    conn.drop_database(testing_config['database'])
    ttime.sleep(2)
