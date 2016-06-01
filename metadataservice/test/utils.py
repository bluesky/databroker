import uuid
import requests
import uuid
import time as ttime
from subprocess import Popen
import os


testing_config = dict(mongohost='localhost', mongoport=27017,
                      database='mds_test'+str(uuid.uuid4()),
                      serviceport=9009, tzone='US/Eastern')


def mds_setup():
    global proc
    f = os.path.dirname(os.path.realpath(__file__))
    proc = Popen(["python", "../../startup.py", "--mongohost", "localhost", "--mongoport",
           "27017", "--database", "mds_test", "--tzone", "timezone", "--serviceport",
           "8899"], cwd=f)
    ttime.sleep(1) # make sure the process is started
    # TODO: Drop the database created using pymongo here!

def mds_teardown():
    proc2 = Popen(['kill', '-9', str(proc.pid)])
    ttime.sleep(1) # make sure the process is killed
