from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six
import uuid
import logging
import time as ttime
import numpy as np
import pandas as pd
from ..broker import DataBroker as db, get_events, get_table
from ..examples.sample_data import temperature_ramp, image_and_scalar
from nose.tools import (assert_equal, assert_raises, assert_true,
                        assert_false)


from metadatastore.api import (insert_run_start, insert_descriptor,
                               find_run_starts)
from metadatastore.utils.testing import mds_setup, mds_teardown
from filestore.utils.testing import fs_setup, fs_teardown
logger = logging.getLogger(__name__)


blc = None


def setup():
    mds_setup()
    fs_setup()

    owners = ['docbrown', 'nedbrainard']
    num_entries = 5
    for owner in owners:
        for i in range(num_entries):
            logger.debug('{}: {} of {}'.format(owner, i+1, num_entries))
            rs = insert_run_start(time=ttime.time(), scan_id=i + 1,
                                  owner=owner, beamline_id='example',
                                  uid=str(uuid.uuid4()))
            # insert some events into mds
            temperature_ramp.run(run_start_uid=rs, make_run_stop=(i != 0))
            if i == 0:
                # only need to do images once, it takes a while...
                image_and_scalar.run(run_start_uid=rs, make_run_stop=True)


def teardown():
    fs_teardown()
    mds_teardown()


def test_basic_usage():
    for i in range(5):
        insert_run_start(time=float(i), scan_id=i + 1,
                         owner='nedbrainard', beamline_id='example',
                         uid=str(uuid.uuid4()))
    header_1 = db[-1]

    header_ned = db(owner='nedbrainard')
    header_ned = db.find_headers(owner='nedbrainard')  # deprecated API
    header_null = db(owner='this owner does not exist')
    # smoke test
    db.fetch_events(header_1)
    db.fetch_events(header_ned)
    db.fetch_events(header_null)
    get_events(header_1)
    get_events(header_ned)
    get_events(header_null)
    get_table(header_1)
    get_table(header_ned)
    get_table(header_null)


def test_indexing():
    for i in range(5):
        insert_run_start(time=float(i), scan_id=i + 1,
                         owner='nedbrainard', beamline_id='example',
                         uid=str(uuid.uuid4()))

    header = db[-1]
    is_list = isinstance(header, list)
    assert_false(is_list)
    scan_id = header['start']['scan_id']
    assert_equal(scan_id, 5)

    header = db[-2]
    is_list = isinstance(header, list)
    assert_false(is_list)
    scan_id = header['start']['scan_id']
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
    scan_id = header['start']['scan_id']
    assert_equal(scan_id, 5)

    headers = db[-2:-1]
    assert_true(is_list)
    num = len(headers)
    assert_equal(num, 1)
    header, = headers
    scan_id = header['start']['scan_id']
    assert_equal(scan_id, 4)

    headers = db[-3:-1]
    scan_ids = [h['start']['scan_id'] for h in headers]
    assert_equal(scan_ids, [4, 3])

    # fancy indexing, by location
    headers = db[[-3, -1, -2]]
    scan_ids = [h['start']['scan_id'] for h in headers]
    assert_equal(scan_ids, [3, 5, 4])

    # fancy indexing, by scan id
    headers = db[[3, 1, 2]]
    scan_ids = [h['start']['scan_id'] for h in headers]
    assert_equal(scan_ids, [3, 1, 2])


def test_scan_id_lookup():
    rd1 = [insert_run_start(time=float(i), scan_id=i + 1 + 314159,
                            owner='docbrown', beamline_id='example',
                            uid=str(uuid.uuid4())) for i in range(5)]

    rd2 = [insert_run_start(time=float(i)+1, scan_id=i + 1 + 314159,
                            owner='nedbrainard', beamline_id='example',
                            uid=str(uuid.uuid4())) for i in range(5)]
    header = db[3 + 314159]
    scan_id = header['start']['scan_id']
    owner = header['start']['owner']
    assert_equal(scan_id, 3 + 314159)
    assert_equal(rd2[2], header['start']['uid'])
    # This should be the most *recent* Scan 3 + 314159. There is ambiguity.
    assert_equal(owner, 'nedbrainard')


def test_uid_lookup():
    uid = str(uuid.uuid4())
    uid2 = uid[0] + str(uuid.uuid4())[1:]  # same first character as uid
    rs1 = insert_run_start(time=100., scan_id=1, uid=uid,
                           owner='drstrangelove', beamline_id='example')
    insert_run_start(time=100., scan_id=1, uid=uid2,
                     owner='drstrangelove', beamline_id='example')
    # using full uid
    actual_uid = db[uid]['start']['uid']
    assert_equal(actual_uid, uid)
    assert_equal(rs1, uid)

    # using first 6 chars
    actual_uid = db[uid[:6]]['start']['uid']
    assert_equal(actual_uid, uid)
    assert_equal(rs1, uid)

    # using first char (will error)
    assert_raises(ValueError, lambda: db[uid[0]])


def test_data_key():
    rs1_uid = insert_run_start(time=100., scan_id=1,
                               owner='nedbrainard', beamline_id='example',
                               uid=str(uuid.uuid4()))
    rs2_uid = insert_run_start(time=200., scan_id=2,
                               owner='nedbrainard', beamline_id='example',
                               uid=str(uuid.uuid4()))
    rs1, = find_run_starts(uid=rs1_uid)
    rs2, = find_run_starts(uid=rs2_uid)
    data_keys = {'fork': {'source': '_', 'dtype': 'number'},
                 'spoon': {'source': '_', 'dtype': 'number'}}
    insert_descriptor(run_start=rs1_uid, data_keys=data_keys,
                            time=100.,
                            uid=str(uuid.uuid4()))
    insert_descriptor(run_start=rs2_uid, data_keys=data_keys, time=200.,
                            uid=str(uuid.uuid4()))
    result1 = db(data_key='fork')
    result2 = db(data_key='fork', start_time=150)
    assert_equal(len(result1), 2)
    assert_equal(len(result2), 1)
    actual = result2[0]['start']['uid']
    assert_equal(actual, str(rs2.uid))


def generate_ca_data(channels, start_time, end_time):
    timestamps = pd.date_range(start_time, end_time, freq='T').to_series()
    timestamps = list(timestamps.dt.to_pydatetime())
    values = list(np.arange(len(timestamps)))
    return {channel: (timestamps, values) for channel in channels}
