from __future__ import absolute_import, division, print_function
import six
import uuid
import logging
from itertools import count
import time as ttime
from databroker import (DataBroker as db, get_events, get_table, stream,
                        get_fields, restream, process)
from ..examples.sample_data import (temperature_ramp, image_and_scalar,
                                    step_scan)
from nose.tools import (assert_equal, assert_raises, assert_true,
                        assert_false, raises)


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
    rs = insert_run_start(time=ttime.time(), scan_id=105,
                          owner='stepper', beamline_id='example',
                          uid=str(uuid.uuid4()))
    step_scan.run(run_start_uid=rs)
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

    # get events for multiple headers
    get_events([header_1, header_ned])

    # test time shift issue GH9
    table = get_table(db[105])
    assert_true(table.notnull().all().all())

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



def _search_helper(query):
    # basically assert that the search does not raise anything
    db[query]


def test_search_for_smoke():
    # smoketest the search with a set
    uid1 = db[-1]['start']['uid'][:8]
    uid2 = db[-2]['start']['uid'][:8]
    uid3 = db[-3]['start']['uid'][:8]
    queries = [
        {-1, -2},
        (-1, -2),
        -1,
        uid1,
        six.text_type(uid1),
        [uid1, [uid2, uid3]],
        [-1, uid1, slice(-5, 0)],
        slice(-5, 0, 2),
        slice(-5, 0),
    ]
    for query in queries:
        yield _search_helper, query


@raises(ValueError)
def _raiser_helper(key):
    db[key]


def test_raise_conditions():
    raising_keys = [
        slice(1, None, None), # raise because trying to slice by scan id
        slice(-1, 2, None),  # raise because slice stop value is > 0
        slice(None, None, None), # raise because slice has slice.start == None
        4500,  # raise on not finding a header by a scan id
        str(uuid.uuid4()),  # raise on not finding a header by uuid
    ]
    for raiser in raising_keys:
        yield _raiser_helper, raiser


def test_stream():
    _stream(restream)
    _stream(stream)  # old name


def _stream(func):
    rs = insert_run_start(time=ttime.time(), scan_id=105,
                          owner='stepper', beamline_id='example',
                          uid=str(uuid.uuid4()))
    step_scan.run(run_start_uid=rs)
    s = func(db[rs])
    name, doc = next(s)
    assert name == 'start'
    assert 'owner' in doc
    name, doc = next(s)
    assert name == 'descriptor'
    assert 'data_keys' in doc
    last_item  = 'event', {'data'}  # fake Event to prime loop
    for item in s:
        name, doc = last_item
        assert name == 'event'
        assert 'data' in doc  # Event
        last_item = item
    name, doc = last_item
    assert name == 'stop'
    assert 'exit_status' in doc # Stop


def test_process():
    rs = insert_run_start(time=ttime.time(), scan_id=105,
                          owner='stepper', beamline_id='example',
                          uid=str(uuid.uuid4()))
    step_scan.run(run_start_uid=rs)
    c = count()
    def f(name, doc):
        next(c)

    process(db[rs], f)
    assert next(c) == len(list(restream(db[rs])))


def test_get_fields():
    rs = insert_run_start(time=ttime.time(), scan_id=105,
                          owner='stepper', beamline_id='example',
                          uid=str(uuid.uuid4()))
    step_scan.run(run_start_uid=rs)
    h = db[rs]
    actual = get_fields(h)
    assert actual == set(['Tsam', 'point_det'])
