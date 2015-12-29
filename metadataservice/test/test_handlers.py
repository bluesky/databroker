# Smoketest the api

from collections import deque
import time as ttime
import datetime
import pytz
from nose.tools import assert_equal, assert_raises, raises, assert_true

run_start_uid = None
document_insert_time = None

def tearDown():
    pass

def setup():
    global run_start_uid, document_insert_time
    document_insert_time = ttime.time()
    







if __name__ == "__main__":
    import nose
    nose.runmodule(argv=['-s', '--with-doctest'], exit=False)
