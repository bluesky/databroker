from __future__ import absolute_import, division, print_function

import tempfile
import os
import glob
import logging
import sys
import string
import time as ttime
import uuid
from datetime import datetime, date, timedelta
import itertools
from databroker import (wrap_in_doct, wrap_in_deprecated_doct,
                        DeprecatedDoct, Broker, temp_config)
import doct
import copy

import pytest
import six
import numpy as np

if sys.version_info >= (3, 0):
    from bluesky.examples import (det, det1, det2, Reader, ReaderWithRegistry,
                                  ReaderWithRegistryHandler)
    from bluesky.plans import (count, pchain, monitor_during_wrapper,
                               configure, trigger_and_read, run_decorator,
                               baseline_wrapper)

logger = logging.getLogger(__name__)

py3 = pytest.mark.skipif(sys.version_info < (3, 0), reason="requires python 3")


@py3
def test_empty_fixture(db):
    "Test that the db pytest fixture works."
    assert len(list(db())) == 0


@py3
def test_uid_roundtrip(db, RE):
    RE.subscribe(db.insert)
    uid, = RE(count([det]))
    h = db[uid]
    assert h['start']['uid'] == uid


@py3
def test_no_descriptor_name(db, RE):
    def local_insert(name, doc):
        doc.pop('name', None)
        return db.insert(name, doc)
    RE.subscribe(local_insert)
    uid, = RE(count([det]))
    h = db[uid]
    db.get_fields(h, name='primary')
    assert h['start']['uid'] == uid
    assert len(h.descriptors) == 1
    assert h.stream_names == ['primary']


@py3
def test_uid_list_multiple_headers(db, RE):
    RE.subscribe(db.insert)
    uids = RE(pchain(count([det]), count([det])))
    headers = db[uids]
    assert uids == [h['start']['uid'] for h in headers]


@py3
def test_no_descriptors(db, RE):
    RE.subscribe(db.insert)
    uid, = RE(count([]))
    header = db[uid]
    assert [] == header.descriptors


@py3
def test_get_events(db, RE):
    RE.subscribe(db.insert)
    uid, = RE(count([det]))
    h = db[uid]
    assert len(list(db.get_events(h))) == 1
    assert len(list(h.documents())) == 1 + 3

    RE.subscribe(db.insert)
    uid, = RE(count([det], num=7))
    h = db[uid]
    assert len(list(db.get_events(h))) == 7
    assert len(list(h.documents())) == 7 + 3


@py3
def test_get_events_multiple_headers(db, RE):
    RE.subscribe(db.insert)
    headers = db[RE(pchain(count([det]), count([det])))]
    assert len(list(db.get_events(headers))) == 2


@py3
def test_filtering_stream_name(db, RE):

    # one event stream
    RE.subscribe(db.insert)
    uid, = RE(count([det], num=7), bc=1)
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
    assert len(list(h.documents(stream_name='primary', fields=['det']))) == 7 + 3
    assert len(h.table(stream_name='primary', fields=['det'])) == 7
    assert len(db.get_table(h, stream_name='primary',
                            fields=['det', 'bc'])) == 7

    # two event streams: 'primary' and 'd_monitor'
    d = Reader('d', fields={'d': lambda: 1}, monitor_intervals=[0.5],
               loop=RE.loop)
    uid, = RE(monitor_during_wrapper(count([det], num=7, delay=0.1), [d]))
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


@py3
def test_table_index_name(db, RE):
    RE.subscribe(db.insert)
    uid, = RE(count([det], 5))
    h = db[uid]

    name = h.table().index.name
    assert name == 'seq_num'


@py3
def test_get_events_filtering_field(db, RE):
    RE.subscribe(db.insert)
    uid, = RE(count([det], num=7))
    h = db[uid]
    assert len(list(db.get_events(h, fields=['det']))) == 7
    assert len(list(h.documents(fields=['det']))) == 7 + 3

    with pytest.raises(ValueError):
        list(db.get_events(h, fields=['not_a_field']))

    uids = RE(pchain(count([det1], num=7), count([det2], num=3)))
    headers = db[uids]

    assert len(list(db.get_events(headers, fields=['det1']))) == 7
    assert len(list(db.get_events(headers, fields=['det2']))) == 3


@py3
def test_indexing(db_empty, RE):
    db = db_empty
    RE.subscribe(db.insert)
    uids = []
    for i in range(10):
        uids.extend(RE(count([det])))

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


@py3
def test_full_text_search(db_empty, RE):
    db = db_empty
    RE.subscribe(db.insert)

    uid, = RE(count([det]), foo='some words')
    RE(count([det]))

    try:
        list(db('some words'))
    except NotImplementedError:
        raise pytest.skip("This backend does not support $text.")

    assert len(list(db('some words'))) == 1
    header, = db('some words')
    assert header['start']['uid'] == uid

    # Full text search does *not* apply to keys.
    assert len(list(db('foo'))) == 0


@py3
def test_table_alignment(db, RE):
    # test time shift issue GH9
    RE.subscribe(db.insert)
    uid, = RE(count([det]))
    table = db.get_table(db[uid])
    assert table.notnull().all().all()


@py3
def test_scan_id_lookup(db, RE):
    RE.subscribe(db.insert)

    RE.md.clear()
    uid1, = RE(count([det]), marked=True)  # scan_id=1

    assert uid1 == db[1]['start']['uid']

    RE.md.clear()
    uid2, = RE(count([det]))  # scan_id=1 again

    # Now we find uid2 for scan_id=1, but we can get the old one by
    # being more specific.
    assert uid2 == db[1]['start']['uid']
    assert uid1 == list(db(scan_id=1, marked=True))[0]['start']['uid']


@py3
def test_partial_uid_lookup(db, RE):
    RE.subscribe(db.insert)

    # Create enough runs that there are two that begin with the same char.
    for _ in range(50):
        RE(count([det]))

    with pytest.raises(ValueError):
        # Some letter will happen to be the first letter of more than one uid.
        for first_letter in string.ascii_lowercase:
            db[first_letter]


@py3
def test_find_by_float_time(db_empty, RE):
    db = db_empty
    RE.subscribe(db.insert)

    before, = RE(count([det]))
    ttime.sleep(0.25)
    t = ttime.time()
    during, = RE(count([det]))
    ttime.sleep(0.25)
    after, = RE(count([det]))

    assert len(list(db())) == 3

    # We'll find the one by specifying a time window around its start time.
    header, = db(start_time=t - 0.1, stop_time=t + 0.2)
    assert header['start']['uid'] == during


@py3
def test_find_by_string_time(db_empty, RE):
    db = db_empty
    RE.subscribe(db.insert)

    uid, = RE(count([det]))
    today = date.today()
    tomorrow = date.today() + timedelta(days=1)
    today_str = today.strftime('%Y-%m-%d')
    tomorrow_str = tomorrow.strftime('%Y-%m-%d')
    day_after_tom = date.today() + timedelta(days=2)
    day_after_tom_str = day_after_tom.strftime('%Y-%m-%d')
    assert len(list(db(start_time=today_str, stop_time=tomorrow_str))) == 1
    assert len(list(db(start_time=tomorrow_str,
                       stop_time=day_after_tom_str))) == 0


@py3
def test_data_key(db_empty, RE):
    db = db_empty
    RE.subscribe(db.insert)
    RE(count([det1]))
    RE(count([det1, det2]))
    result1 = list(db(data_key='det1'))
    result2 = list(db(data_key='det2'))
    assert len(result1) == 2
    assert len(result2) == 1


@py3
def test_search_for_smoke(db, RE):
    RE.subscribe(db.insert)
    for _ in range(5):
        RE(count([det]))
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


@py3
def test_alias(db, RE):
    RE.subscribe(db.insert)

    uid, = RE(count([det]))
    RE(count([det]))

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


@py3
def test_filters(db_empty, RE):
    db = db_empty
    RE.subscribe(db.insert)
    RE(count([det]), user='Ken')
    dan_uid, = RE(count([det]), user='Dan', purpose='calibration')
    ken_calib_uid, = RE(count([det]), user='Ken', purpose='calibration')

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
    db.add_filter(start_time='2017')
    db.add_filter(start_time='2017')
    assert len(db.filters) == 1
    db.add_filter(start_time='2016', stop_time='2017')
    assert len(db.filters) == 2
    assert db.filters['start_time'] == '2016'

    db()  # after search, time content keeps the same
    assert db.filters['start_time'] == '2016'

@py3
@pytest.mark.parametrize(
    'key',
    [slice(1, None, None), # raise because trying to slice by scan id
     slice(-1, 2, None),  # raise because slice stop value is > 0
     slice(None, None, None),  # raise because slice has slice.start == None
     4500,  # raise on not finding a header by a scan id
     str(uuid.uuid4()),  # raise on not finding a header by uuid
     ],
    ids=['slice by scan id',
         'positve slice stop',
         'no start',
         'no scan id',
         'no uuid']
    )
def test_raise_conditions(key, db, RE):
    RE.subscribe(db.insert)
    for _ in range(5):
        RE(count([det]))

    with pytest.raises(ValueError):
        db[key]


@py3
def test_stream(db, RE):
    _stream('restream', db, RE)
    _stream('stream', db, RE)  # old name


def _stream(method_name, db, RE):
    RE.subscribe(db.insert)
    uid = RE(count([det]), owner='Dan')
    s = getattr(db, method_name)(db[uid])
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


@py3
def test_process(db, RE):
    uid = RE.subscribe(db.insert)
    uid = RE(count([det]))
    c = itertools.count()
    def f(name, doc):
        next(c)

    db.process(db[uid], f)
    assert next(c) == len(list(db.restream(db[uid])))


@py3
def test_get_fields(db, RE):
    RE.subscribe(db.insert)
    uid, = RE(count([det1, det2]))
    actual = db.get_fields(db[uid])
    expected = set(['det1', 'det2'])
    assert actual == expected
    actual = db[uid].fields()
    assert actual == expected
    actual = db[uid].fields('primary')
    assert actual == expected


@py3
def test_configuration(db, RE):
    det_with_conf = Reader('det_with_conf', {'a': lambda: 1, 'b': lambda: 2})
    RE.subscribe(db.insert)
    uid, = RE(count([det_with_conf]), c=3)
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


@py3
def test_handler_options(db, RE):
    datum_id = str(uuid.uuid4())
    datum_id2 = str(uuid.uuid4())
    desc_uid = str(uuid.uuid4())
    event_uid = str(uuid.uuid4())
    event_uid2 = str(uuid.uuid4())

    # Side-band resource and datum documents.
    res = db.reg.insert_resource('foo', '', {'x': 1})
    db.reg.insert_datum(res, datum_id, {'y': 2})

    res2 = db.reg.insert_resource('foo', '', {'x': 1})
    db.reg.insert_datum(res2, datum_id2, {'y': 2})

    # Generate a normal run.
    RE.subscribe(db.insert)
    rs_uid, = RE(count([det]))

    # Side band an extra descriptor and event into this run.
    data_keys = {'image': {'dtype': 'array', 'source': '', 'shape': [5, 5],
                           'external': 'FILESTORE://simulated'}}
    db.mds.insert_descriptor(run_start=rs_uid, data_keys=data_keys,
                             time=ttime.time(), uid=desc_uid,
                             name='injected')
    db.mds.insert_event(descriptor=desc_uid, time=ttime.time(), uid=event_uid,
                        data={'image': datum_id},
                        timestamps={'image': ttime.time()}, seq_num=0)

    db.mds.insert_event(descriptor=desc_uid, time=ttime.time(), uid=event_uid2,
                        data={'image': datum_id2},
                        timestamps={'image': ttime.time()}, seq_num=0)

    h = db[rs_uid]

    # Get unfilled event.
    ev, ev2 = db.get_events(h, stream_name='injected', fields=['image'])
    assert ev['data']['image'] == datum_id
    assert not ev['filled']['image']

    # Get filled event -- error because no handler is registered.
    with pytest.raises(KeyError):
        ev, ev2 = db.get_events(h, stream_name='injected',
                                fields=['image'], fill=True)

    # Get filled event -- error because no handler is registered.
    with pytest.raises(KeyError):
        list(db.get_images(h, 'image', stream_name='injected'))

    class ImageHandler(object):
        RESULT = np.zeros((5, 5))

        def __init__(self, resource_path, **resource_kwargs):
            self._res = resource_kwargs

        def __call__(self, **datum_kwargs):
            return self.RESULT

    class DummyHandler(object):
        def __init__(*args, **kwargs):
            pass

        def __call__(*args, **kwrags):
            return 'dummy'

    # Use a one-off handler registry.
    ev, ev2 = db.get_events(h,
                            stream_name='injected', fields=['image'],
                            fill=True,
                            handler_registry={'foo': ImageHandler})
    assert ev['data']['image'].shape == ImageHandler.RESULT.shape
    assert ev['filled']['image']

    # Statefully register the handler.
    db.reg.register_handler('foo', ImageHandler)

    ev, ev2 = db.get_events(h,
                            stream_name='injected', fields=['image'],
                            fill=True)
    assert ev['data']['image'].shape == ImageHandler.RESULT.shape
    ims = db.get_images(h, 'image', stream_name='injected')[0]
    assert ims.shape == ImageHandler.RESULT.shape
    assert ev['filled']['image']

    ev, ev2 = db.get_events(h, stream_name='injected', fields=['image'])
    assert ev is not ev2
    assert ev['filled'] is not ev2['filled']
    assert not ev['filled']['image']
    datum = ev['data']['image']
    ev_ret, = db.fill_events([ev], h.descriptors, inplace=True)
    assert ev['filled']['image'] == datum
    assert not ev2['filled']['image']
    ev2_filled = db.fill_event(ev2, inplace=False)
    assert ev2_filled['filled']['image'] == ev2['data']['image']

    # Override the stateful registry with a one-off handler.
    # This maps onto the *data key*, not the resource spec.
    # ev, ev2 = db.get_events(h, fields=['image'], fill=True,
    #                         handler_overrides={'image': DummyHandler})
    # assert ev['data']['image'] == 'dummy'
    # assert ev['filled']['image']

    # res = db.get_table(h, fields=['image'], stream_name='injected', fill=True,
    #                    handler_registry={'foo': DummyHandler})
    # assert res['image'].iloc[0] == 'dummy'
    # assert ev['filled']['image']

    # res = db.get_table(h, fields=['image'], stream_name='injected', fill=True,
    #                    handler_overrides={'image': DummyHandler})
    # assert res['image'].iloc[0] == 'dummy'
    # assert ev['filled']['image']

    # # Register the DummyHandler statefully so we can test overriding with
    # # ImageHandler for the get_images method below.
    # db.reg.register_handler('foo', DummyHandler, overwrite=True)

    # res = db.get_images(h, 'image', handler_registry={'foo': ImageHandler})
    # assert res[0].shape == ImageHandler.RESULT.shape

    # res = db.get_images(h, 'image', handler_override=ImageHandler)
    # assert res[0].shape == ImageHandler.RESULT.shape


@py3
def test_export(broker_factory, RE):
    db1 = broker_factory()
    db2 = broker_factory()
    RE.subscribe(db1.mds.insert)

    # test mds only
    uid, = RE(count([det]))
    db1.export(db1[uid], db2)
    assert db2[uid] == db1[uid]
    assert list(db2.get_events(db2[uid])) == list(db1.get_events(db1[uid]))

    # test file copying
    if not hasattr(db1.fs, 'copy_files'):
        raise pytest.skip("This Registry does not implement copy_files.")

    dir1 = tempfile.mkdtemp()
    dir2 = tempfile.mkdtemp()
    detfs = ReaderWithRegistry('detfs', {'image': lambda: np.ones((5, 5))},
                                reg=db1.fs, save_path=dir1)
    uid, = RE(count([detfs]))

    db1.fs.register_handler('RWFS_NPY', ReaderWithRegistryHandler)
    db2.fs.register_handler('RWFS_NPY', ReaderWithRegistryHandler)

    (from_path, to_path), = db1.export(db1[uid], db2, new_root=dir2)
    assert os.path.dirname(from_path) == dir1
    assert os.path.dirname(to_path) == dir2
    assert db2[uid] == db1[uid]
    image1, = db1.get_images(db1[uid], 'image')
    image2, = db2.get_images(db2[uid], 'image')


@py3
def test_export_noroot(broker_factory, RE):
    from bluesky.utils import short_uid
    from bluesky.examples import GeneralReaderWithRegistry

    class LocalWriter(GeneralReaderWithRegistry):
        def stage(self):
            self._file_stem = short_uid()
            self._path_stem = os.path.join(self.save_path, self._file_stem)
            self._resource_id = self.reg.insert_resource(
                self._spec,
                os.path.join(self.save_path, self._file_stem),
                {})

    dir1 = tempfile.mkdtemp()
    db1 = broker_factory()
    db1.fs.register_handler('RWFS_NPY', ReaderWithRegistryHandler)

    detfs = LocalWriter('detfs', {'image': lambda: np.ones((5, 5))},
                        reg=db1.fs, save_path=dir1, save_ext='npy')

    RE.subscribe(db1.mds.insert)
    uid, = RE(count([detfs], num=3))

    db2 = broker_factory()
    db2.fs.register_handler('RWFS_NPY', ReaderWithRegistryHandler)

    dir2 = tempfile.mkdtemp()
    file_pairs = db1.export(db1[uid], db2, new_root=dir2)
    for from_path, to_path in file_pairs:
        assert os.path.dirname(from_path) == dir1
        assert os.path.dirname(to_path) == os.path.join(dir2, dir1[1:])

    assert db2[uid] == db1[uid]
    image1s = db1.get_images(db1[uid], 'image')
    image2s = db2.get_images(db2[uid], 'image')
    for im1, im2 in zip(image1s, image2s):
        assert np.array_equal(im1, im2)


@py3
def test_export_size_smoke(broker_factory, RE):

    db1 = broker_factory()
    RE.subscribe(db1.mds.insert)

    # test file copying
    if not hasattr(db1.fs, 'copy_files'):
        raise pytest.skip("This Registry does not implement copy_files.")

    dir1 = tempfile.mkdtemp()
    detfs = ReaderWithRegistry('detfs', {'image': lambda: np.ones((5, 5))},
                               reg=db1.fs, save_path=dir1)
    uid, = RE(count([detfs]))

    db1.fs.register_handler('RWFS_NPY', ReaderWithRegistryHandler)
    size = db1.export_size(db1[uid])
    assert size > 0.


@py3
def test_results_multiple_iters(db, RE):
    RE.subscribe(db.insert)
    RE(count([det]))
    RE(count([det]))
    res = db()
    first = list(res)  # First pass through Result is lazy.
    second = list(res)  # The Result object's tee should cache results.
    third = list(res)  # The Result object's tee should cache results.
    assert first == second == third


@py3
def test_dict_header(db, RE):
    # Ensure that we aren't relying on h being a doct as opposed to a dict.
    RE.subscribe(db.insert)
    uid, = RE(count([det]))
    h = db[uid]
    expected = list(db.get_events(h))
    actual = list(db.get_events(dict(h)))
    assert actual == expected


@py3
def test_config_data(db, RE):
    # simple case: one Event Descriptor, one stream
    RE.subscribe(db.insert)
    det = Reader('det', {'x': lambda: 1}, conf={'y': 2, 'z': 3})
    uid, = RE(count([det]))
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

    uid, = RE(plan())
    h = db[uid]
    actual = h.config_data('det')
    expected = {'primary': [{'y': 2, 'z': 3}, {'y': 2, 'z': 4}]}
    assert actual == expected

    # generate two streams, primary and baseline -- one Event Descriptor each
    uid, = RE(baseline_wrapper(count([det]), [det]))
    h = db[uid]
    actual = h.config_data('det')
    expected = {'primary': [{'y': 2, 'z': 4}],
                'baseline': [{'y': 2, 'z': 4}]}
    assert actual == expected


@py3
def test_events(db, RE):
    RE.subscribe(db.insert)
    uid, = RE(count([det]))
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


@py3
def test_prepare_hook_default(db, RE):
    RE.subscribe(db.insert)
    uid, = RE(count([det]))

    # check default -- returning a subclass of doct.Document that warns
    # when you use getattr for getitem
    assert db.prepare_hook == wrap_in_deprecated_doct

    h = db[uid]
    for doc in _get_docs(h):
        assert isinstance(doc, DeprecatedDoct)
        assert isinstance(doc, doct.Document)


@py3
def test_prepare_hook_old_style(db, RE):
    # configure to return old-style doct.Document objects
    db.prepare_hook = wrap_in_doct

    RE.subscribe(db.insert)
    uid, = RE(count([det]))

    # Test Broker.__getitem__ and Broker.__call__ means of creating Headers.
    for h in (db[uid], list(db())[0]):
        for doc in _get_docs(h):
            assert not isinstance(doc, DeprecatedDoct)
            assert isinstance(doc, doct.Document)


@py3
def test_prepare_hook_deep_copy(db, RE):
    # configure to return plain dicts
    db.prepare_hook = lambda name, doc: copy.deepcopy(doc)

    RE.subscribe(db.insert)
    uid, = RE(count([det]))

    for h in (db[uid], list(db())[0]):
        for doc in _get_docs(h):
            assert not isinstance(doc, DeprecatedDoct)
            assert not isinstance(doc, doct.Document)


@py3
def test_prepare_hook_raw(db, RE):
    # configure to return plain dicts
    db.prepare_hook = lambda name, doc: doc

    RE.subscribe(db.insert)
    uid, = RE(count([det]))

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

    with pytest.warns(None) as record:
        ev['data']
        ev.values()
        with pytest.raises(AttributeError):
            ev.nonexistent
    assert not record  # i.e. assert no warning


@py3
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


@py3
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


@py3
def test_deprecated_stream_method(db, RE):
    RE.subscribe(db.insert)
    uid, = RE(count([det]))
    h = db[uid]

    # h.stream() is the same as h.documents() but it warns
    expected = list(h.documents())
    with pytest.warns(UserWarning):
        actual = list(h.stream())
    assert actual == expected


@py3
def test_data_method(db, RE):
    RE.subscribe(db.insert)
    uid, = RE(count([det], 5))
    h = db[uid]

    actual = list(h.data('det'))
    expected = [1, 1, 1, 1, 1]
    assert actual == expected


def test_auto_register():
    db_auto = Broker.from_config(temp_config())
    db_manual = Broker.from_config(temp_config(), auto_register=False)
    assert db_auto.reg.handler_reg
    assert not db_manual.reg.handler_reg


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


@py3
def test_extraneous_filled_stripped_on_insert(db, RE):

    # TODO It would be better if this errored, but at the moment
    # this would required looking up the event descriptor.

    # Hack the Event so it does not match its Event Descriptor.
    def insert(name, doc):
        if name == 'event':
            doc['filled'] = {'det': False}
            assert 'filled' in doc
        db.insert(name, doc)

    RE.subscribe(insert)

    uid, = RE(count([det]))
    h = db[uid]

    # expect event['filled'] == {}
    for ev in h.events():
        assert not ev['filled']


@py3
def test_filled_false_stripped_on_insert(db, RE):
    # Hack the Event and the Descriptor consistently.
    def insert(name, doc):
        if name == 'event':
            doc['filled'] = {'det': False}
            doc['data']['det'] = 'DATUM_ID_PLACEHOLDER'
            assert 'filled' in doc
        elif name == 'descriptor':
            doc['data_keys']['det']['external'] = 'PLACEHOLDER'
        db.insert(name, doc)

    RE.subscribe(insert)

    uid, = RE(count([det]))
    h = db[uid]

    # expect event['filled'] == {'det': False}
    for ev in h.events():
        assert 'det' in ev['filled']
        assert not ev['filled']['det']
        assert ev['data']['det'] == 'DATUM_ID_PLACEHOLDER'


@py3
def test_filled_true_rotated_on_insert(db, RE):
    # Hack the Event and the Descriptor consistently.
    def insert(name, doc):
        if name == 'event':
            doc['filled'] = {'det': 'DATUM_ID_PLACEHOLDER'}
            assert 'filled' in doc
        elif name == 'descriptor':
            doc['data_keys']['det']['external'] = 'PLACEHOLDER'
        db.insert(name, doc)

    RE.subscribe(insert)

    uid, = RE(count([det]))
    h = db[uid]

    # expect event['filled'] == {'det': False}
    for ev in h.events():
        assert 'det' in ev['filled']
        assert not ev['filled']['det']
        assert ev['data']['det'] == 'DATUM_ID_PLACEHOLDER'


@py3
def test_repr_html(db, RE):
    RE.subscribe(db.insert)
    uid, = RE(count([det], 5))
    h = db[uid]

    # smoke test
    h._repr_html_()
