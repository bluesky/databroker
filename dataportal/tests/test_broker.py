from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six
import uuid
import logging
import time as ttime
import mongoengine
import logging
from collections import Iterable
from datetime import datetime
import numpy as np
import pandas as pd
from .. import sources
from ..sources import channelarchiver as ca
from ..sources import switch
from ..broker import DataBroker as db
import mongoengine.connection
from mongoengine.context_managers import switch_db

from nose.tools import make_decorator
from nose.tools import (assert_equal, assert_raises, assert_true,
                        assert_false)


from metadatastore.odm_templates import (BeamlineConfig, EventDescriptor,
                                         Event, RunStart, RunStop)
import metadatastore.commands as mdsc
logger = logging.getLogger(__name__)

db_name = str(uuid.uuid4())
dummy_db_name = str(uuid.uuid4())
blc = None


def setup():
    global blc
    logger.debug('setting up')
    # need to make 'default' connection to point to no-where, just to be safe
    mongoengine.connection.disconnect()
    mongoengine.connect(dummy_db_name)
    mongoengine.connection.disconnect(alias='test_db')  # whaaaat
    # connect to the db we are actually going to use
    mongoengine.connect(db_name, alias='test_db')
    blc = mdsc.insert_beamline_config({}, ttime.time())

    switch(channelarchiver=False, metadatastore=True, filestore=True)
    start, end = '2015-01-01 00:00:00', '2015-01-01 00:01:00'
    simulated_ca_data = generate_ca_data(['ch1', 'ch2'], start, end)
    ca.insert_data(simulated_ca_data)
    
    # Make some well-controlled test entries.
    insert_run_start = sources.metadatastore.api.insert_run_start
    insert_beamline_config = sources.metadatastore.api.insert_beamline_config
    for i in range(5):
        insert_run_start(time=float(i), scan_id=i + 1,
                         owner='nedbrainard', beamline_id='example',
                         beamline_config=insert_beamline_config({}, time=0.))


def teardown():
    logger.debug('tearing down')
    conn = mongoengine.connection.get_connection('test_db')
    conn.drop_database(db_name)
    conn.drop_database(dummy_db_name)
    switch(channelarchiver=True, metadatastore=True, filestore=True)


def test_basic_usage():
    header = db[-1]
    header = db.find_headers(owner='nedbrainard')
    header = db.find_headers(owner='this owner does not exist')
    events = db.fetch_events(header)


def test_indexing():
    header = db[-1]
    is_list = isinstance(header, Iterable)
    assert_false(is_list)
    scan_id = header.scan_id
    assert_equal(scan_id, 5)

    header = db[-2]
    is_list = isinstance(header, Iterable)
    assert_false(is_list)
    scan_id = header.scan_id
    assert_equal(scan_id, 4)

    f = lambda: db[-13]
    assert_raises(IndexError, f)

    headers = db[-5:]
    is_list = isinstance(headers, Iterable)
    assert_true(is_list)
    num = len(headers)
    assert_equal(num, 5)

    header = db[-6:]
    assert_true(is_list)
    num = len(headers)
    assert_equal(num, 5)

    headers = db[-1:]
    assert_true(is_list)
    num = len(headers)
    assert_equal(num, 1)
    header, = headers
    scan_id = header.scan_id
    assert_equal(scan_id, 5)

    headers = db[-2:-1]
    assert_true(is_list)
    num = len(headers)
    print(headers)
    assert_equal(num, 1)
    header, = headers
    scan_id = header.scan_id
    assert_equal(scan_id, 4)

    headers = db[-3:-1]
    scan_ids = [h.scan_id for h in headers]
    assert_equal(scan_ids, [4, 3])


def test_lookup():
    header = db[3]
    scan_id = header.scan_id
    assert_equal(scan_id, 3)


def generate_ca_data(channels, start_time, end_time):
    timestamps = pd.date_range(start_time, end_time, freq='T').to_series()
    timestamps = list(timestamps.dt.to_pydatetime())  # list of datetime objects
    values = list(np.arange(len(timestamps)))
    return {channel: (timestamps, values) for channel in channels}
