from __future__ import absolute_import, division, print_function

import collections
import tempfile
import os
import logging
import sys
import string
import time as ttime
import uuid
from datetime import date, timedelta
import itertools
from databroker import (wrap_in_doct, wrap_in_deprecated_doct,
                        DeprecatedDoct, Broker, temp, ALL)
from .test_config import EXAMPLE
import doct
import copy

import pytest
import six
import numpy as np
import event_model

from databroker._core import DOCT_NAMES
from databroker.tests.utils import get_uids

if sys.version_info >= (3, 5):
    from bluesky.plans import count
    from bluesky.plan_stubs import trigger_and_read, configure
    from bluesky.preprocessors import (monitor_during_wrapper,
                                       run_decorator,
                                       baseline_wrapper,
                                       stage_wrapper,
                                       pchain)

logger = logging.getLogger(__name__)


def test_empty_fixture(db):
    "Test that the db pytest fixture works."
    assert len(list(db())) == 0


def test_uid_roundtrip(db, RE, hw):
    RE.subscribe(db.insert)
    uid, = get_uids(RE(count([hw.det])))
    h = db[uid]
    assert h['start']['uid'] == uid


def test_header_equality(db, RE, hw):
    RE.subscribe(db.insert)
    uid, = get_uids(RE(count([hw.det])))
    uid2, = get_uids(RE(count([hw.det])))
    h = db[uid]
    h2 = db[uid2]

    assert h != []
    assert h != h2
    assert h == db[uid]


def test_no_descriptor_name(db, RE, hw):
    def local_insert(name, doc):
        doc.pop('name', None)
        return db.insert(name, doc)
    RE.subscribe(local_insert)
    uid, = get_uids(RE(count([hw.det])))
    h = db[uid]
    db.get_fields(h, name='primary')
    assert h['start']['uid'] == uid
    assert len(h.descriptors) == 1
    assert h.stream_names == ['primary']


def test_uid_list_multiple_headers(db, RE, hw):
    RE.subscribe(db.insert)
    uids = get_uids(RE(pchain(count([hw.det]), count([hw.det]))))
    headers = db[uids]
    assert uids == tuple([h['start']['uid'] for h in headers])


def test_no_descriptors(db, RE):
    RE.subscribe(db.insert)
    uid, = get_uids(RE(count([])))
    header = db[uid]
    assert [] == header.descriptors


def test_get_events(db, RE, hw):
    RE.subscribe(db.insert)
    uid, = get_uids(RE(count([hw.det])))
    h = db[uid]
    assert len(list(db.get_events(h))) == 1
    assert len(list(h.documents())) == 1 + 3

    RE.subscribe(db.insert)
    uid, = get_uids(RE(count([hw.det], num=7)))
    h = db[uid]
    assert len(list(db.get_events(h))) == 7
    assert len(list(h.documents())) == 7 + 3


def test_get_events_multiple_headers(db, RE, hw):
    RE.subscribe(db.insert)
    headers = db[get_uids(RE(pchain(count([hw.det]), count([hw.det]))))]
    assert len(list(db.get_events(headers))) == 2


def test_filtering_stream_name(db, RE, hw):
    from ophyd import sim
    # one event stream
    RE.subscribe(db.insert)
    uid, = get_uids(RE(count([hw.det], num=7), bc=1))
    h = db[uid]
    assert len(list(h.descriptors)) == 1
    assert list(h.stream_names) == ['primary']
    assert len(list(db.get_events(h, stream_name='primary'))) == 7
    assert len(db.get_table(h, stream_name='primary')) == 7
    assert len(list(db.get_events(h, stream_name='primary',
                                  fields=['det']))) == 7
    assert len(db.get_table(h, stream_name='primary', fields=['det'])) == 7
    assert len(list(h.documents(stream_name='primary'))) == 7 + 3
    assert len(h.table(stream_name='primary')) == 7
    assert len(list(h.documents(stream_name='primary',
                                fields=['det']))) == 7 + 3
    assert len(h.table(stream_name='primary', fields=['det'])) == 7
    assert len(db.get_table(h, stream_name='primary',
                            fields=['det', 'bc'])) == 7

    # two event streams: 'primary' and 'd_monitor'
    d = sim.SynPeriodicSignal(name='d', period=.5)
    uid, = get_uids(RE(monitor_during_wrapper(count([hw.det], num=7, delay=0.1),
                                     [d])))
    h = db[uid]
    assert len(list(h.descriptors)) == 2
    assert set(h.stream_names) == set(['primary', 'd_monitor'])
    assert len(list(db.get_events(h, stream_name='primary'))) == 7
    assert len(list(h.documents(stream_name='primary'))) == 7 + 3

    assert len(db.get_table(h, stream_name='primary')) == 7

    assert len(db.get_table(h)) == 7  # 'primary' by default
    assert len(h.table(stream_name='primary')) == 7
    assert len(h.table()) == 7  # 'primary' by default

    # TODO sort out why the monitor does not fire during the test
    # assert len(list(db.get_events(h))) == 8  # ALL streams by default

    # assert len(list(h.documents())) == 8 + 3  # ALL streams by default
    # assert len(list(db.get_table(h, stream_name='d_monitor'))) == 1
    # assert len(list(h.table(stream_name='d_monitor'))) == 1
    # assert len(list(h.documents(stream_name='d_monitor'))) == 1 + 3
    # assert len(list(db.get_events(h, stream_name='d_monitor'))) == 1


def test_table_index_name(db, RE, hw):
    RE.subscribe(db.insert)
    uid, = get_uids(RE(count([hw.det], 5)))
    h = db[uid]

    name = h.table().index.name
    assert name == 'seq_num'


def test_get_events_filtering_field(db, RE, hw):
    RE.subscribe(db.insert)
    uid, = get_uids(RE(count([hw.det], num=7)))
    h = db[uid]
    assert len(list(db.get_events(h, fields=['det']))) == 7
    assert len(list(h.documents(fields=['det']))) == 7 + 3

    uids = get_uids(RE(pchain(count([hw.det1], num=7),
                     count([hw.det2], num=3))))
    headers = db[uids]

    assert len(list(db.get_events(headers, fields=['det1']))) == 7
    assert len(list(db.get_events(headers, fields=['det2']))) == 3


def test_indexing(db_empty, RE, hw):
    db = db_empty
    RE.subscribe(db.insert)
    uids = []
    for i in range(10):
        uids.extend(get_uids(RE(count([hw.det]))))

    assert uids[-1] == db[-1]['start']['uid']
    assert uids[-2] == db[-2]['start']['uid']
    assert uids[-1:] == [h['start']['uid'] for h in db[-1:]]
    assert uids[-2:] == [h['start']['uid'] for h in reversed(db[-2:])]
    assert uids[-2:-1] == [h['start']['uid'] for h in reversed(db[-2:-1])]
    assert uids[-5:-1] == [h['start']['uid'] for h in reversed(db[-5:-1])]

    with pytest.raises(ValueError):
        # not allowed to slice into unspecified past
        db[:-5]

    with pytest.raises(IndexError):
        # too far back
        db[-11]


def test_full_text_search(db_empty, RE, hw):
    db = db_empty
    RE.subscribe(db.insert)

    uid, = get_uids(RE(count([hw.det]), foo='some words'))
    RE(count([hw.det]))

    try:
        list(db('some words'))
    except NotImplementedError:
        raise pytest.skip("This backend does not support $text.")

    assert len(list(db('some words'))) == 1
    header, = db('some words')
    assert header['start']['uid'] == uid

    # Full text search does *not* apply to keys.
    assert len(list(db('foo'))) == 0


def test_table_alignment(db, RE, hw):
    # test time shift issue GH9
    RE.subscribe(db.insert)
    uid, = get_uids(RE(count([hw.det])))
    table = db.get_table(db[uid])
    assert table.notnull().all().all()


def test_scan_id_lookup(db, RE, hw):
    RE.subscribe(db.insert)

    RE.md.clear()
    uid1, = get_uids(RE(count([hw.det]), marked=True))  # scan_id=1

    assert uid1 == db[1]['start']['uid']

    RE.md.clear()
    uid2, = get_uids(RE(count([hw.det])))  # scan_id=1 again

    # Now we find uid2 for scan_id=1, but we can get the old one by
    # being more specific.
    assert uid2 == db[1]['start']['uid']
    assert uid1 in [run['start']['uid'] for run in db(scan_id=1, marked=True)]


# Flaky because
# https://github.com/bluesky/databroker/issues/431


@pytest.mark.flaky(reruns=10, reruns_delay=0)
def test_partial_uid_lookup(db, RE, hw):
    RE.subscribe(db.insert)

    # Create enough runs that there are two that begin with the same char.
    for _ in range(50):
        RE(count([hw.det]))

    with pytest.raises(ValueError):
        # Some letter will happen to be the first letter of more than one uid.
        for first_letter in string.ascii_lowercase:
            db[first_letter]


def test_partial_uid_lookup2(db):
    key_parts = ['a'*6, 'b'*6]

    run_bundle_A = event_model.compose_run(uid=''.join(key_parts))
    db.insert('start', run_bundle_A.start_doc)
    db.insert('stop', run_bundle_A.compose_stop())
    run_bundle_B = event_model.compose_run(uid=''.join(key_parts[::-1]))
    db.insert('start', run_bundle_B.start_doc)
    db.insert('stop', run_bundle_B.compose_stop())

    hA = db[key_parts[0]]
    assert dict(hA.start) == run_bundle_A.start_doc

    hB = db[key_parts[1]]
    assert dict(hB.start) == run_bundle_B.start_doc


def test_find_by_float_time(db_empty, RE, hw):
    db = db_empty
    RE.subscribe(db.insert)

    before, = get_uids(RE(count([hw.det])))
    ttime.sleep(0.25)
    t = ttime.time()
    during, = get_uids(RE(count([hw.det])))
    ttime.sleep(0.25)
    after, = get_uids(RE(count([hw.det])))

    assert len(list(db())) == 3

    # We'll find the one by specifying a time window around its start time.
    header, = db(since=t - 0.1, until=t + 0.2)
    assert header['start']['uid'] == during

    # Test the old names
    with pytest.warns(UserWarning):
        header, = db(start_time=t - 0.1, stop_time=t + 0.2)
    assert header['start']['uid'] == during


def test_find_by_string_time(db_empty, RE, hw):
    db = db_empty
    RE.subscribe(db.insert)
    uid, = get_uids(RE(count([hw.det])))

    yesterday = date.fromtimestamp(db[uid].start['time']) + timedelta(days=-1)
    tomorrow = yesterday + timedelta(days=2)
    day_after_tom = yesterday + timedelta(days=3)

    yesterday_str = yesterday.strftime('%Y-%m-%d')
    tomorrow_str = tomorrow.strftime('%Y-%m-%d')
    day_after_tom_str = day_after_tom.strftime('%Y-%m-%d')

    assert len(list(db(since=yesterday_str, until=tomorrow_str))) == 1
    assert len(list(db(since=tomorrow_str,
                       until=day_after_tom_str))) == 0


def test_data_key(db_empty, RE, hw):
    db = db_empty
    RE.subscribe(db.insert)
    RE(count([hw.det1]))
    RE(count([hw.det1, hw.det2]))
    result1 = list(db(data_key='det1'))
    result2 = list(db(data_key='det2'))
    assert len(result1) == 2
    assert len(result2) == 1


# flaky because of https://github.com/bluesky/databroker/issues/431
@pytest.mark.flaky(reruns=5, reruns_delay=0)
def test_search_for_smoke(db, RE, hw):
    RE.subscribe(db.insert)
    for _ in range(5):
        RE(count([hw.det]))
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
        db[query]


def test_alias(db, RE, hw):
    RE.subscribe(db.insert)

    uid, = get_uids(RE(count([hw.det])))
    RE(count([hw.det]))

    # basic usage of alias
    db.alias('foo', uid=uid)
    assert list(db.foo) == list(db(uid=uid))

    # can't set alias to existing attribute name
    with pytest.raises(ValueError):
        db.alias('get_events', uid=uid)
    with pytest.raises(ValueError):
        db.dynamic_alias('get_events', lambda: {'uid': uid})

    # basic usage of dynamic alias
    db.dynamic_alias('bar', lambda: {'uid': uid})
    assert list(db.bar) == list(db(uid=uid))

    # normal AttributeError still works
    with pytest.raises(AttributeError):
        db.this_is_not_a_thing


def test_filters(db_empty, RE, hw):
    db = db_empty
    RE.subscribe(db.insert)
    RE(count([hw.det]), user='Ken')
    dan_uid, = get_uids(RE(count([hw.det]), user='Dan', purpose='calibration'))
    ken_calib_uid, = get_uids(RE(count([hw.det]), user='Ken', purpose='calibration'))

    assert len(list(db())) == 3
    db.add_filter(user='Dan')
    assert len(db.filters) == 1
    assert len(list(db())) == 1
    header, = db()
    assert header['start']['uid'] == dan_uid

    db.clear_filters()
    assert len(db.filters) == 0

    assert len(list(db(purpose='calibration'))) == 2
    db.add_filter(user='Ken')
    assert len(list(db(purpose='calibration'))) == 1
    header, = db(purpose='calibration')

    assert header['start']['uid'] == ken_calib_uid

    db.clear_filters()
    db.add_filter(since='2017')
    db.add_filter(since='2017')
    assert len(db.filters) == 1
    db.add_filter(since='2016', until='2017')
    assert len(db.filters) == 2
    assert db.filters['since'] == '2016'

    list(db())  # after search, time content keeps the same
    assert db.filters['since'] == '2016'

    # Check again using old names (start_time, stop_time)
    db.clear_filters()
    db.add_filter(start_time='2017')
    db.add_filter(start_time='2017')
    assert len(db.filters) == 1
    db.add_filter(start_time='2016', stop_time='2017')
    assert len(db.filters) == 2
    assert db.filters['start_time'] == '2016'
    with pytest.warns(UserWarning):
        list(db())

@pytest.mark.parametrize(
    'key',
    [slice(1, None, None),  # raise because trying to slice by scan id
     slice(-1, 2, None),    # raise because slice stop value is > 0
     slice(None, None, None),  # raise because slice has slice.start == None
     ],
    ids=['slice by scan id',
         'positve slice stop',
         'no start']
    )
def test_raise_value_error_conditions(key, db, RE, hw):
    RE.subscribe(db.insert)
    for _ in range(5):
        RE(count([hw.det]))

    with pytest.raises(ValueError):
        db[key]

@pytest.mark.parametrize(
    'key',
    [4500,  # raise on not finding a header by a scan id
     str(uuid.uuid4()),  # raise on not finding a header by uuid
     ],
    ids=['no scan id',
         'no uuid']
    )
def test_raise_key_error_conditions(key, db, RE, hw):
    RE.subscribe(db.insert)
    for _ in range(5):
        RE(count([hw.det]))

    with pytest.raises(KeyError):
        db[key]



@pytest.mark.parametrize('method_name', ['restream', 'stream'])
def test_stream(method_name, db, RE, hw):
    RE.subscribe(db.insert)
    uid, = get_uids(RE(count([hw.det]), owner='Dan'))
    s = getattr(db, method_name)(db[uid])
    name, doc = next(s)
    assert name == 'start'
    assert 'owner' in doc
    name, doc = next(s)
    assert name == 'descriptor'
    assert 'data_keys' in doc
    last_item = 'event', {'data'}  # fake Event to prime loop
    for item in s:
        name, doc = last_item
        assert name == 'event'
        assert 'data' in doc  # Event
        last_item = item
    name, doc = last_item
    assert name == 'stop'
    assert 'exit_status' in doc  # Stop


def test_process(db, RE, hw):
    RE.subscribe(db.insert)
    uid, = get_uids(RE(count([hw.det])))
    c = itertools.count()

    def f(name, doc):
        next(c)

    db.process(db[uid], f)
    assert next(c) == len(list(db.restream(db[uid])))


def test_get_fields(db, RE, hw):
    RE.subscribe(db.insert)
    uid, = get_uids(RE(count([hw.det1, hw.det2])))
    actual = db.get_fields(db[uid])
    expected = set(['det1', 'det2'])
    assert actual == expected
    actual = db[uid].fields()
    assert actual == expected
    actual = db[uid].fields('primary')
    assert actual == expected


def test_configuration(db, RE):
    from ophyd import Device, sim, Component as C

    class SynWithConfig(Device):
        a = C(sim.Signal, value=0)
        b = C(sim.Signal, value=2)
        d = C(sim.Signal, value=1)

    det = SynWithConfig(name='det')
    det.a.name = 'a'
    det.b.name = 'b'
    det.d.name = 'd'
    det.read_attrs = ['a', 'b']
    det.configuration_attrs = ['d']

    RE.subscribe(db.insert)
    uid, = get_uids(RE(count([det]), c=3))
    h = db[uid]

    # check that config is not included by default
    ev = next(db.get_events(h))
    assert set(ev['data'].keys()) == set(['a', 'b'])

    # find config in descriptor['configuration']
    ev = next(db.get_events(h, fields=['a', 'b']))
    assert 'b' in ev['data']
    assert ev['data']['b'] == 2
    assert 'b' in ev['timestamps']

    # find config in start doc
    ev = next(db.get_events(h, fields=['a', 'c']))
    assert 'c' in ev['data']
    assert ev['data']['c'] == 3
    assert 'c' in ev['timestamps']

    # find config in stop doc
    ev = next(db.get_events(h, fields=['a', 'exit_status']))
    assert 'exit_status' in ev['data']
    assert ev['data']['exit_status'] == 'success'
    assert 'exit_status' in ev['timestamps']


def test_stream_name(db, RE, hw):
    # subscribe db.insert
    RE.subscribe(db.insert)

    # custom plan that will generate two streams
    @run_decorator()
    def myplan(dets):
        ''' Simple plan to trigger two detectors.

        Meant for test only.
        '''
        for msg in trigger_and_read([dets[0]], name='primary'):
            yield msg
        for msg in trigger_and_read([dets[1]], name='secondary'):
            yield msg

    # this test is meaningless (will always pass)
    # if our two detectors have the same name. Ensure this is true
    assert hw.det.name != hw.det2.name

    rs_uid, = get_uids(RE(myplan([hw.det, hw.det2])))
    h = db[rs_uid]

    assert h.fields() == {'det', 'det2'}
    assert h.fields(stream_name='primary') == {'det'}
    assert h.fields(stream_name='secondary') == {'det2'}


def test_external_access_without_handler(db, RE, hw):
    from ophyd.sim import NumpySeqHandler

    RE.subscribe(db.insert)
    rs_uid, = get_uids(RE(count([hw.img], 2)))


    # Clear the handler registry. We'll reinstate the relevant handler below.
    for spec in list(db.reg.handler_reg):
        db.reg.deregister_handler(spec)

    h = db[rs_uid]

    # Get unfilled event.
    ev, ev2 = db.get_events(h, fields=['img'])
    assert isinstance(ev['data']['img'], str)
    assert not ev['filled']['img']

    # Get filled event -- error because no handler is registered.
    with pytest.raises(KeyError):
        ev, ev2 = db.get_events(h, fields=['img'], fill=True)

    # Get filled event -- error because no handler is registered.
    with pytest.raises(KeyError):
        list(db.get_images(h, 'img'))

    # Use a one-off handler registry.
    # This functionality used to be supported, but has been removed, so the
    # test here just verifies that it raised the expected type of error.
    if hasattr(db, 'v1') or hasattr(db, 'v2'):
        with pytest.raises(NotImplementedError):
            ev, ev2 = db.get_events(
                h, fields=['img'], fill=True,
                handler_registry={'NPY_SEQ': NumpySeqHandler})


def test_external_access_with_handler(db, RE, hw):
    from ophyd.sim import NumpySeqHandler

    RE.subscribe(db.insert)
    rs_uid, = get_uids(RE(count([hw.img], 2)))


    # For some db fixtures, this is already registered and is therefore a
    # no-op.
    db.reg.register_handler('NPY_SEQ', NumpySeqHandler)

    h = db[rs_uid]

    EXPECTED_SHAPE = (10, 10)  # via ophyd.sim.img

    ev, ev2 = db.get_events(h, fields=['img'], fill=True)
    assert ev['data']['img'].shape == EXPECTED_SHAPE
    ims = db.get_images(h, 'img')[0]
    assert ims.shape == EXPECTED_SHAPE
    assert ev['filled']['img']

    ev, ev2 = db.get_events(h, fields=['img'])
    assert ev is not ev2
    assert ev['filled'] is not ev2['filled']
    assert not ev['filled']['img']
    datum = ev['data']['img']

    if hasattr(db, 'v1') or hasattr(db, 'v2'):
        with pytest.raises(NotImplementedError):
            ev_ret, = db.fill_events([ev], h.descriptors, inplace=True)

    if hasattr(db, 'v1') or hasattr(db, 'v2'):
        with pytest.raises(NotImplementedError):
            ev2_filled = db.fill_event(ev2, inplace=False)

    # table with fill=False (default)
    table = db.get_table(h, fields=['img'])
    datum_id = table['img'].iloc[0]
    assert isinstance(datum_id, str)

    # table with fill=True
    table = db.get_table(h, fields=['img'], fill=True)
    img = table['img'].iloc[0]
    assert not isinstance(img, str)
    assert img.shape == EXPECTED_SHAPE


def test_export(broker_factory, RE, hw):
    from ophyd import sim
    db1 = broker_factory()
    db2 = broker_factory()
    RE.subscribe(db1.insert)

    # test mds only
    uid, = get_uids(RE(count([hw.det])))
    db1.export(db1[uid], db2)
    assert db2[uid] == db1[uid]
    assert list(db2.get_events(db2[uid])) == list(db1.get_events(db1[uid]))

    # test file copying
    if not hasattr(db1.reg, 'copy_files'):
        raise pytest.skip("This Registry does not implement copy_files.")

    dir1 = tempfile.mkdtemp()
    dir2 = tempfile.mkdtemp()
    detfs = sim.SynSignalWithRegistry(name='detfs',
                                      func=lambda: np.ones((5, 5)),
                                      save_path=dir1)
    uid, = get_uids(RE(count([detfs])))

    db1.reg.register_handler('NPY_SEQ', sim.NumpySeqHandler)
    db2.reg.register_handler('NPY_SEQ', sim.NumpySeqHandler)

    (from_path, to_path), = db1.export(db1[uid], db2, new_root=dir2)
    assert os.path.dirname(from_path) == dir1
    assert os.path.dirname(to_path) == dir2
    assert db2[uid] == db1[uid]
    image1, = db1.get_images(db1[uid], 'detfs')
    image2, = db2.get_images(db2[uid], 'detfs')


def test_export_noroot(broker_factory, RE, tmpdir, hw):
    from ophyd import sim

    class BrokenSynRegistry(sim.SynSignalWithRegistry):

        def stage(self):
            self._file_stem = sim.short_uid()
            self._path_stem = os.path.join(self.save_path, self._file_stem)
            self._datum_counter = itertools.count()
            # This is temporarily more complicated than it will be in the future.
            # It needs to support old configurations that have a registry.
            resource = {'spec': self._spec,
                        'root': '',
                        'resource_path': self._path_stem,
                        'resource_kwargs': {},
                        'path_semantics': os.name}
            self._resource_uid = sim.new_uid()
            resource['uid'] = self._resource_uid
            self._asset_docs_cache.append(('resource', resource))

    dir1 = str(tmpdir.mkdir('a'))
    dir2 = str(tmpdir.mkdir('b'))
    db1 = broker_factory()
    db2 = broker_factory()

    detfs = BrokenSynRegistry(name='detfs',
                              func=lambda: np.ones((5, 5)),
                              save_path=dir1)
    db1.reg.register_handler('NPY_SEQ', sim.NumpySeqHandler)
    db2.reg.register_handler('NPY_SEQ', sim.NumpySeqHandler)
    RE.subscribe(db1.insert)
    uid, = get_uids(RE(count([detfs], num=3)))

    file_pairs = db1.export(db1[uid], db2, new_root=dir2)
    for from_path, to_path in file_pairs:
        assert os.path.dirname(from_path) == dir1
        assert os.path.dirname(to_path) == os.path.join(dir2, dir1[1:])

    assert db2[uid] == db1[uid]
    image1s = db1.get_images(db1[uid], 'detfs')
    image2s = db2.get_images(db2[uid], 'detfs')
    for im1, im2 in zip(image1s, image2s):
        assert np.array_equal(im1, im2)


def test_export_size_smoke(broker_factory, RE, tmpdir):
    from ophyd import sim
    db1 = broker_factory()
    RE.subscribe(db1.insert)

    # test file copying
    if not hasattr(db1.fs, 'copy_files'):
        raise pytest.skip("This Registry does not implement copy_files.")

    detfs = sim.SynSignalWithRegistry(name='detfs',
                                      func=lambda: np.ones((5, 5)),
                                      save_path=str(tmpdir.mkdir('a')))

    uid, = get_uids(RE(count([detfs])))

    db1.reg.register_handler('NPY_SEQ', sim.NumpySeqHandler)
    size = db1.export_size(db1[uid])
    assert size > 0.


def test_results_multiple_iters(db, RE, hw):
    RE.subscribe(db.insert)
    RE(count([hw.det]))
    RE(count([hw.det]))
    res = db()
    first = list(res)  # First pass through Result is lazy.
    second = list(res)  # The Result object's tee should cache results.
    third = list(res)  # The Result object's tee should cache results.
    assert first == second == third


@pytest.mark.skip(reason='headers are rich objects now')
def test_dict_header(db, RE, hw):
    # Ensure that we aren't relying on h being a doct as opposed to a dict.
    RE.subscribe(db.insert)
    uid, = get_uids(RE(count([hw.det])))
    h = db[uid]
    expected = list(db.get_events(h))
    actual = list(db.get_events(dict(h)))
    assert actual == expected

    h['start']
    h['stop']
    h['descriptors']
    with pytest.raises(KeyError):
        h['events']


def test_config_data(db, RE, hw):
    # simple case: one Event Descriptor, one stream
    RE.subscribe(db.insert)
    from ophyd import Device, sim, Component as C

    class SynWithConfig(Device):
        x = C(sim.Signal, value=0)
        y = C(sim.Signal, value=2)
        z = C(sim.Signal, value=3)

    det = SynWithConfig(name='det')
    det.x.name = 'x'
    det.y.name = 'y'
    det.z.name = 'z'
    det.read_attrs = ['x']
    det.configuration_attrs = ['y', 'z']

    uid, = get_uids(RE(count([det])))
    h = db[uid]
    actual = h.config_data('det')
    expected = {'primary': [{'y': 2, 'z': 3}]}
    assert actual == expected

    # generate two Event Descriptors in the primary stream
    @run_decorator()
    def plan():
        # working around 'yield from' here which breaks py2
        for msg in configure(det, {'z': 3}):  # no-op
            yield msg
        for msg in trigger_and_read([det]):
            yield msg
        # changing the config after a read generates a new Event Descriptor
        for msg in configure(det, {'z': 4}):
            yield msg
        for msg in trigger_and_read([det]):
            yield msg

    uid, = get_uids(RE(plan()))
    h = db[uid]
    actual = h.config_data('det')
    expected = {'primary': [{'y': 2, 'z': 3}, {'y': 2, 'z': 4}]}
    assert actual == expected

    # generate two streams, primary and baseline -- one Event Descriptor each
    uid, = get_uids(RE(baseline_wrapper(count([det]), [det])))
    h = db[uid]
    actual = h.config_data('det')
    expected = {'primary': [{'y': 2, 'z': 4}],
                'baseline': [{'y': 2, 'z': 4}]}
    assert actual == expected


def test_events(db, RE, hw):
    RE.subscribe(db.insert)
    uid, = get_uids(RE(count([hw.det])))
    h = db[uid]
    events = list(h.events())
    assert len(events) == 1


def _get_docs(h):
    # Access documents in all the public ways.
    docs = []
    docs.extend(h.descriptors)
    docs.extend([pair[1] for pair in h.documents()])
    docs.extend(list(h.events()))
    docs.extend(list(h.db.get_events(h)))
    docs.append(h.stop)
    docs.append(h.start)
    return docs


def test_prepare_hook_default(db, RE, hw):
    RE.subscribe(db.insert)
    uid, = get_uids(RE(count([hw.det])))

    # check default -- returning a subclass of doct.Document that warns
    # when you use getattr for getitem
    assert db.prepare_hook == wrap_in_deprecated_doct

    h = db[uid]
    for doc in _get_docs(h):
        assert isinstance(doc, DeprecatedDoct)
        assert isinstance(doc, doct.Document)


def test_prepare_hook_old_style(db, RE, hw):
    # configure to return old-style doct.Document objects
    db.prepare_hook = wrap_in_doct

    RE.subscribe(db.insert)
    uid, = get_uids(RE(count([hw.det])))

    # Test Broker.__getitem__ and Broker.__call__ means of creating Headers.
    for h in (db[uid], list(db())[0]):
        for doc in _get_docs(h):
            assert not isinstance(doc, DeprecatedDoct)
            assert isinstance(doc, doct.Document)


def test_prepare_hook_deep_copy(db, RE, hw):
    # configure to return plain dicts
    db.prepare_hook = lambda name, doc: copy.deepcopy(dict(doc))

    RE.subscribe(db.insert)
    uid, = get_uids(RE(count([hw.det])))

    for h in (db[uid], list(db())[0]):
        for doc in _get_docs(h):
            assert not isinstance(doc, DeprecatedDoct)
            assert not isinstance(doc, doct.Document)


def test_prepare_hook_raw(db, RE, hw):
    # configure to return plain dicts
    db.prepare_hook = lambda name, doc: doc

    RE.subscribe(db.insert)
    uid, = get_uids(RE(count([hw.det])))

    for h in (db[uid], list(db())[0]):
        for doc in _get_docs(h):
            assert not isinstance(doc, DeprecatedDoct)
            assert not isinstance(doc, doct.Document)
            assert isinstance(doc, dict)


def test_deprecated_doct():
    ev = DeprecatedDoct('stuff', {
        'data': {'det': 1.0},
        'descriptor': '120324df-5d7b-4764-94ec-7031cdcbd6e6',
        'filled': {},
        'seq_num': 1,
        'time': 1502356802.8381739,
        'timestamps': {'det': 1502356802.701149},
        'uid': 'f295e8a7-b688-4c22-9f43-0ce6fdd56dbd'})

    with pytest.warns(UserWarning):
        ev.data
        ev._name

    with pytest.warns(None) as record:
        ev['data']
        ev.values()
        ev._repr_html_()
        with pytest.raises(AttributeError):
            ev.nonexistent
    assert not record  # i.e. assert no warning


def test_ingest_array_data(db_empty, RE):
    db = db_empty
    RE.subscribe(db.insert)
    # These will blow up if the event source backing db cannot ingest numpy
    # arrays. (For example, the pymongo-backed db has to convert them to plain
    # lists.)

    class Detector:
        def __init__(self, name, array):
            self.name = name
            self.parent = None
            self.array = array

        def read(self):
            return {'x': {'value': self.array, 'timestamp': ttime.time()}}

        def describe(self):
            return {'x': {'shape': self.array.shape,
                          'dtype': 'array',
                          'source': 'test'}}

        def describe_configuration(self):
            return {}

        def read_configuration(self):
            return {}

    # 1D array in event['data']
    det = Detector('det', np.array([1, 2, 3]))
    RE(count([det]))

    # 3D array in event['data']
    det = Detector('det', np.ones((3, 3, 3)))
    RE(count([det]))


def test_ingest_array_metadata(db, RE):
    RE.subscribe(db.insert)

    # These will blow up if the header source backing db cannot ingest numpy
    # arrays. (For example, the pymongo-backed db has to convert them to plain
    # lists.)

    # 1D array in start document.
    RE(count([]), mask=np.array([1, 2, 3]))

    # 1D array in nested keys in start document.
    RE(count([]), nested=dict(mask=np.array([1, 2, 3])))
    RE(count([]), deeply=dict(nested=dict(mask=np.array([1, 2, 3]))))

    # 3D array in start document.
    RE(count([]), mask=np.ones((3, 3, 3)))


def test_deprecated_stream_method(db, RE, hw):
    RE.subscribe(db.insert)
    uid, = get_uids(RE(count([hw.det])))
    h = db[uid]

    # h.stream() is the same as h.documents() but it warns
    expected = list(h.documents())
    with pytest.warns(UserWarning):
        actual = list(h.stream())
    assert actual == expected


def test_data_method(db, RE, hw):
    RE.subscribe(db.insert)
    uid, = get_uids(RE(count([hw.det, hw.det2], 5)))
    h = db[uid]

    actual = list(h.data('det'))
    expected = [1, 1, 1, 1, 1]
    assert actual == expected

    # Check that this works twice. This once exposed a bug in caching logic.
    actual = list(h.data('det'))
    expected = [1, 1, 1, 1, 1]
    assert actual == expected


def test_auto_register():
    db_auto = Broker.from_config(EXAMPLE)
    db_manual = Broker.from_config(EXAMPLE, auto_register=False)
    assert 'AD_HDF5' in db_auto.reg.handler_reg
    assert 'AD_HDF5' not in db_manual.reg.handler_reg


def test_sanitize_does_not_modify_array_data_in_place(db_empty):
    db = db_empty
    doc = {'uid': '0', 'time': 0, 'stuff': np.ones((3, 3))}
    assert isinstance(doc['stuff'], np.ndarray)
    db.insert('start', doc)
    assert isinstance(doc['stuff'], np.ndarray)

    doc = {'uid': '1', 'time': 0, 'run_start': '0',
           'data_keys': {'det': {'dtype': 'array',
                                 'shape': (3, 3),
                                 'source': ''}},
           'object_keys': {'det': ['det']},
           'configuration': {'det': {'thing': np.ones((3, 3))}}}
    assert isinstance(doc['configuration']['det']['thing'], np.ndarray)
    db.insert('descriptor', doc)
    assert isinstance(doc['configuration']['det']['thing'], np.ndarray)

    doc = {'uid': '2', 'time': 0, 'descriptor': '1', 'seq_num': 1,
           'data': {'det': np.ones((3, 3))},
           'timestamps': {'det': 0}}
    assert isinstance(doc['data']['det'], np.ndarray)
    db.insert('event', doc)
    assert isinstance(doc['data']['det'], np.ndarray)

    docs = [{'uid': '3', 'time': 0, 'descriptor': '1', 'seq_num': 2,
             'data': {'det': np.ones((3, 3))},
             'timestamps': {'det': 0}},
            {'uid': '4', 'time': 0, 'descriptor': '1', 'seq_num': 2,
             'data': {'det': np.ones((3, 3))},
             'timestamps': {'det': 0}}]
    assert isinstance(doc['data']['det'], np.ndarray)
    db.insert('bulk_events', {'1': docs})
    assert isinstance(doc['data']['det'], np.ndarray)


    doc = {'uid': '5', 'time': 0, 'run_start': '0', 'exit_status': 'success',
           'stuff': np.ones((3, 3))}
    assert isinstance(doc['stuff'], np.ndarray)
    db.insert('stop', doc)
    assert isinstance(doc['stuff'], np.ndarray)


def test_fill_and_multiple_streams(db, RE, tmpdir, hw):
    from ophyd import sim
    RE.subscribe(db.insert)
    detfs1 = sim.SynSignalWithRegistry(name='detfs1',
                                       func=lambda: np.ones((5, 5)),
                                       save_path=str(tmpdir.mkdir('a')))
    detfs2 = sim.SynSignalWithRegistry(name='detfs2',
                                       func=lambda: np.ones((5, 5)),
                                       save_path=str(tmpdir.mkdir('b')))

    # In each event stream, put one 'fillable' (external-writing)
    # detector and one simple detector.
    primary_dets = [detfs1, hw.det1]
    baseline_dets = [detfs2, hw.det2]

    uid, = get_uids(RE(stage_wrapper(baseline_wrapper(count(primary_dets),
                                             baseline_dets),
                            baseline_dets)))

    db.reg.register_handler('NPY_SEQ', sim.NumpySeqHandler)
    h = db[uid]
    list(h.documents(stream_name=ALL, fill=False))
    list(h.documents(stream_name=ALL, fill=True))


def test_repr_html(db, RE, hw):
    RE.subscribe(db.insert)
    uid, = get_uids(RE(count([hw.det], 5)))
    h = db[uid]

    # smoke test
    h._repr_html_()


def test_externals(db, RE, hw):
    def external_fetcher(start, stop):
        return start['uid']

    RE.subscribe(db.insert)
    uid, = get_uids(RE(count([hw.det], 5)))
    db.external_fetchers['suid'] = external_fetcher

    h = db[uid]

    assert h.ext.suid == h.start['uid']


def test_monitoring(db, RE, hw):
    # A monitored signal emits Events from its subscription thread, which
    # silently failed on the sqlite backend until it was refactored to make
    # this test pass.
    import bluesky.plan_stubs as bps
    import bluesky.plans as bp
    from bluesky.preprocessors import SupplementalData

    RE.subscribe(db.insert)
    sd = SupplementalData()
    RE.preprocessors.append(sd)
    sd.monitors.append(hw.rand)
    RE(bp.count([hw.det], 5, delay=1))
    assert len(db[-1].table('rand_monitor')) > 1


def test_interlace_gens():
    from databroker.eventsource.shim import interlace_gens
    a = ({'time': i} for i in range(10) if i % 2 == 0)
    b = ({'time': i} for i in range(10) if i % 2 == 1)
    c = ({'time': i} for i in range(100, 110))
    d = interlace_gens(a, b, c)
    expected = list(range(10)) + list(range(100, 110))
    for z, zz in zip(d, expected):
        assert z['time'] == zz


def test_order(db, RE, hw):
    from ophyd import sim
    RE.subscribe(db.insert)
    d = sim.SynPeriodicSignal(name='d', period=.5)
    uid, = get_uids(RE(monitor_during_wrapper(count([hw.det], num=7, delay=0.1), [d])))

    t0 = None
    for name, doc in db[uid].documents():
        # TODO: include datums in here at some point
        if name in ['event']:
            t1 = doc['time']
            if t0:
                assert t1 > t0
            t0 = t1


def test_res_datum(db, RE, hw):
    from ophyd.sim import NumpySeqHandler
    import copy
    db.prepare_hook = lambda name, doc: copy.deepcopy(dict(doc))
    for spec in NumpySeqHandler.specs:
        db.reg.register_handler(spec, NumpySeqHandler)
    L = []
    RE.subscribe(db.insert)
    RE.subscribe(lambda *x: L.append(x))

    uid, = get_uids(RE(count([hw.img], num=7, delay=0.1)))

    unique_names = set(name for name, _ in db[uid].documents())
    assert unique_names == set(DOCT_NAMES.keys())
    # This is a way of comparing collections where duplicates do matter (i.e.
    # it's not a `set`) but order does not.
    actual = collections.Counter(name for name, _ in db[uid].documents())
    expected = collections.Counter(name for name, _ in L)
    assert actual == expected


def test_filtering_fields(db, RE, hw):
    from bluesky.preprocessors import run_decorator
    from bluesky.plan_stubs import trigger_and_read

    RE.subscribe(db.insert)

    m1, m2 = hw.motor1, hw.motor2
    m1.acceleration.put(2)
    m2.acceleration.put(3)

    @run_decorator()
    def round_robin_plan():
        for j in range(7):
            yield from trigger_and_read([m1], 'a')
            yield from trigger_and_read([m2], 'b')

    uid, = get_uids(RE(round_robin_plan()))
    h = db[uid]

    for fields in (
            [m1.name, m1.acceleration.name],
            [m2.name, m2.acceleration.name]):

        for name, doc in h.documents(
                fields=fields):
            if name == 'event':
                assert set(doc['data']) == set(fields)


def resource_roundtrip(broker_factory, RE, hw):
    db = broker_factory()
    db2 = broker_factory()
    from ophyd.sim import NumpySeqHandler
    import copy
    db.prepare_hook = lambda name, doc: copy.deepcopy(dict(doc))
    for spec in NumpySeqHandler.specs:
        db.reg.register_handler(spec, NumpySeqHandler)
    RE.subscribe(db.insert)
    RE.subscribe(lambda *x: L.append(x))

    uid, = get_uids(RE(count([hw.img], num=7, delay=0.1)))

    for nd in db[-1].documents():
        db2.insert(*nd)
