from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six
import uuid
import logging
import time as ttime
import logging
from collections import Iterable
from datetime import datetime
import numpy as np
import pandas as pd
from .. import sources
from ..sources import channelarchiver as ca
from ..sources import switch
from ..broker import EventQueue, DataBroker as db
from ..examples.sample_data import temperature_ramp, image_and_scalar

from nose.tools import make_decorator
from nose.tools import (assert_equal, assert_raises, assert_true,
                        assert_false)


from metadatastore.odm_templates import (BeamlineConfig, EventDescriptor,
                                         Event, RunStart, RunStop)
from metadatastore.api import insert_run_start, insert_beamline_config
from metadatastore.utils.testing import mds_setup, mds_teardown
from filestore.utils.testing import fs_setup, fs_teardown
logger = logging.getLogger(__name__)


blc = None


def setup():
    mds_setup()
    fs_setup()
    blc = insert_beamline_config({}, ttime.time())

    switch(channelarchiver=False)
    start, end = '2015-01-01 00:00:00', '2015-01-01 00:01:00'
    simulated_ca_data = generate_ca_data(['ch1', 'ch2'], start, end)
    ca.insert_data(simulated_ca_data)

    owners = ['docbrown', 'nedbrainard']
    num_entries = 5
    for owner in owners:
        for i in range(num_entries):
            logger.debug('{}: {} of {}'.format(owner, i+1, num_entries))
            rs = insert_run_start(time=ttime.time(), scan_id=i + 1,
                                  owner=owner, beamline_id='example',
                                  beamline_config=blc)
            # insert some events into mds
            temperature_ramp.run(run_start=rs, make_run_stop=(i!=0))
            if i == 0:
                # only need to do images once, it takes a while...
                image_and_scalar.run(run_start=rs, make_run_stop=True)


def teardown():
    fs_teardown()
    mds_teardown()


def test_basic_usage():
    for i in range(5):
        insert_run_start(time=float(i), scan_id=i + 1,
                         owner='nedbrainard', beamline_id='example',
                         beamline_config=insert_beamline_config({}, time=0.))
    header_1 = db[-1]
    header_ned = db.find_headers(owner='nedbrainard')
    header_null = db.find_headers(owner='this owner does not exist')
    events_1 = db.fetch_events(header_1)
    events_ned = db.fetch_events(header_ned)
    events_null = db.fetch_events(header_null)


def test_event_queue():
    scan_id = np.random.randint(1e12)  # unique enough for government work
    rs = insert_run_start(time=0., scan_id=scan_id,
                          owner='queue-tester', beamline_id='example',
                          beamline_config=insert_beamline_config({}, time=0.))
    header = db.find_headers(scan_id=scan_id)
    queue = EventQueue(header)
    # Queue should be empty until we update.
    f = lambda: queue.pop()
    assert_raises(IndexError, f)
    queue.update()
    # This should add an empty list to the queue.
    empty_bundle = queue.pop()
    assert_equal(len(empty_bundle), 0)
    events = temperature_ramp.run(rs)
    # This should add a bundle of Events to the queue.
    queue.update()
    first_bundle = queue.pop()
    assert_equal(len(first_bundle), len(events))
    more_events = temperature_ramp.run(rs)
    # Queue should be empty until we update.
    assert_raises(IndexError, f)
    queue.update()
    second_bundle = queue.pop()
    assert_equal(len(second_bundle), len(more_events))
    # Add Events from a different example into the same Header.
    other_events = image_and_scalar.run(rs)
    queue.update()
    third_bundle = queue.pop()
    assert_equal(len(third_bundle), len(other_events))


def test_indexing():
    for i in range(5):
        insert_run_start(time=float(i), scan_id=i + 1,
                         owner='nedbrainard', beamline_id='example',
                         beamline_config=insert_beamline_config({}, time=0.))
    header = db[-1]
    is_list = isinstance(header, list)
    assert_false(is_list)
    scan_id = header.scan_id
    assert_equal(scan_id, 5)

    header = db[-2]
    is_list = isinstance(header, list)
    assert_false(is_list)
    scan_id = header.scan_id
    assert_equal(scan_id, 4)

    f = lambda: db[-100000]
    assert_raises(IndexError, f)

    headers = db[-5:]
    is_list = isinstance(headers, list)
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

    # fancy indexing, by location
    headers = db[[-3, -1, -2]]
    scan_ids = [h.scan_id for h in headers]
    assert_equal(scan_ids, [3, 5, 4])

    # fancy indexing, by scan id
    headers = db[[3, 1, 2]]
    scan_ids = [h.scan_id for h in headers]
    assert_equal(scan_ids, [3, 1, 2])


def test_lookup():
    for i in range(5):
        insert_run_start(time=float(i), scan_id=i + 1,
                         owner='docbrown', beamline_id='example',
                         beamline_config=insert_beamline_config({}, time=0.))
    for i in range(5):
        insert_run_start(time=float(i), scan_id=i + 1,
                         owner='nedbrainard', beamline_id='example',
                         beamline_config=insert_beamline_config({}, time=0.))
    header = db[3]
    scan_id = header.scan_id
    owner = header.owner
    assert_equal(scan_id, 3)
    # This should be the most *recent* Scan 3. There is ambiguity.
    assert_equal(owner, 'nedbrainard')


def generate_ca_data(channels, start_time, end_time):
    timestamps = pd.date_range(start_time, end_time, freq='T').to_series()
    timestamps = list(timestamps.dt.to_pydatetime())  # list of datetime objects
    values = list(np.arange(len(timestamps)))
    return {channel: (timestamps, values) for channel in channels}
