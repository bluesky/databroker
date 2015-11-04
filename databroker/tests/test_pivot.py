from nose.tools import assert_equal, assert_in
from databroker.pivot import pivot_timeseries, zip_events, reset_time
from numpy.testing import assert_array_equal
import numpy as np
import time as ttime


def _pivot_data_helper(M, N):
    """
    make some synthetic data
    """
    desc = {'uid': 'fake desc',
            'data_keys': dict(),
            'run_start': 'run_start',
            'time': ttime.time()}
    for k in 'abc':
        desc['data_keys'][k] = {'source': 'syn',
                                'dtype': 'array',
                                'shape': (M,)}
    for k in 'def':
        desc['data_keys'][k] = {'source': 'syn',
                                'dtype': 'number',
                                'shape': ()}
    for k in 'ghi':
        desc['data_keys'][k] = {'source': 'syn',
                                'dtype': 'array',
                                'shape': (M, 2, 2)}
    for j in range(N):
        data = {k: [m + j*M + n for m in range(M)]
                for k, n in zip('abc', range(3))}
        data.update({k: ord(k) + j for k in 'def'})
        data.update({k: np.asarray([(m + j*M + n) * np.ones((2, 2))
                                    for m in range(M)])
                     for k, n in zip('ghi', range(3))})

        ts = {k: ttime.time() for k in data}
        ev = {'uid': str(j),
              'data': data,
              'timestamps': ts,
              'time': ttime.time(),
              'descriptor': desc,
              'seq_no': j, }
        yield ev


def _zip_data_helper(key_lists, N):
    """
    make some synthetic data
    """
    def _inner_gen(keys, N, offset=0):

        desc = {'uid': 'fake desc',
                'data_keys': dict(),
                'run_start': 'run_start',
                'time': ttime.time()}
        for k in keys:
            desc['data_keys'][k] = {'source': 'syn',
                                    'dtype': 'number',
                                    'shape': ()}
        for j in range(N):
            data = {k: offset + j + n for n, k in
                    enumerate(keys)}

            ts = {k: ttime.time() for k in data}
            ev = {'uid': str(j),
                  'data': data,
                  'timestamps': ts,
                  'time': ttime.time(),
                  'descriptor': desc,
                  'seq_no': j, }
            yield ev
    return [_inner_gen(keys, N) for keys in key_lists]


def _reset_time_data_helper():
    desc = {'uid': 'fake desc',
            'data_keys': dict(),
            'run_start': 'run_start',
            'time': ttime.time()}
    for j in range(12):
        data = {'a': j, 'b': -j}
        ts = {'a': j, 'b': j + 300}
        ev = {'uid': str(j),
              'data': data,
              'timestamps': ts,
              'time': ttime.time(),
              'descriptor': desc,
              'seq_no': j, }
        yield ev


def test_pivot_smoke():
    M, N = 3, 10
    evs = list(_pivot_data_helper(M, N))
    ev_dict = {ev['uid']: ev for ev in evs}
    pevs = list(pivot_timeseries(evs, 'abg', 'cdh'))
    for j, ev in enumerate(pevs):
        assert_equal(j, ev['seq_no'])
        assert_equal(ev['data']['fr_no'], j % M)

        desc = ev['descriptor']
        for k in 'abgcdh':
            assert_in(k, desc['data_keys'])
        assert_equal(desc['data_keys']['a']['shape'], ())
        assert_equal(desc['data_keys']['b']['shape'], ())
        assert_equal(desc['data_keys']['c']['shape'], (3, ))
        assert_equal(desc['data_keys']['g']['shape'], (2, 2))
        assert_equal(desc['data_keys']['h']['shape'], (3, 2, 2))

        for k in 'cdh':
            assert_equal(desc['data_keys'][k]['source'], 'syn')
            assert_array_equal(ev['data'][k], evs[j // M]['data'][k])

        for k in 'abg':
            src = desc['data_keys'][k]['source']
            assert_equal(src, str(j // M))
            source_ev = ev_dict[src]
            assert_array_equal(ev['data'][k],
                               source_ev['data'][k][ev['data']['fr_no']])


def test_zip_events_smoke():
    dd = _zip_data_helper(('abc', 'def'), 10)
    for ev in zip_events(*dd):
        assert_equal(set('abcdef'), set(ev['descriptor']['data_keys']))
        assert_equal(set('abcdef'), set(ev['data']))
        assert_equal(set('abcdef'), set(ev['timestamps']))


def test_zip_events_lazy():
    dd = _zip_data_helper(('abc', 'def'), 10)
    for ev in zip_events(*dd, lazy=False):
        assert_equal(set('abcdef'), set(ev['descriptor']['data_keys']))
        assert_equal(set('abcdef'), set(ev['data']))
        assert_equal(set('abcdef'), set(ev['timestamps']))


def test_reset_time_smoke():
    evs = list(_reset_time_data_helper())
    revs = list(reset_time(evs, 'a'))
    for ev, rev in zip(evs, revs):
        assert_equal(rev['time'], rev['timestamps']['a'])
        assert_equal(ev['data'], rev['data'])
        assert_equal(ev['timestamps'], rev['timestamps'])
