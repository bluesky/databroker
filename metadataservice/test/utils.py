import uuid
import requests
import uuid
import time as ttime

testing_config = dict(mongohost='localhost', mongoport=27017,
                      database='mds_test'+str(uuid.uuid4()),
                      serviceport=9001, tzone='US/Eastern')


def mds_setup():
    global proc
    proc = Popen(["python", "startup.py", "--mongohost", "localhost", "--mongoport", 
           "27017", "--database", "mds_test", "--tzone", "timezone", "--serviceport", 
           "8899"])
    ttime.sleep(1) # make sure the process is started
    # TODO: Drop the database created using pymongo here!

def mds_teardown():
    proc2 = Popen(['kill', '-9', str(proc.pid)])
    ttime.sleep(1) # make sure the process is killed
