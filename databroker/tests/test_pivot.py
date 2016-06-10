import time as ttime

import numpy as np
from numpy.testing import assert_array_equal

from databroker.pivot import pivot_timeseries, zip_events, reset_time


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
        assert j == ev['seq_no']
        assert ev['data']['_ind'] == j % M

        desc = ev['descriptor']
        for k in 'abgcdh':
            assert k in desc['data_keys']
        assert desc['data_keys']['a']['shape'] == ()
        assert desc['data_keys']['b']['shape'] == ()
        assert desc['data_keys']['c']['shape'] == (3, )
        assert desc['data_keys']['g']['shape'] == (2, 2)
        assert desc['data_keys']['h']['shape'] == (3, 2, 2)
        assert set(desc['data_keys']) == set(ev['data'])
        assert set(desc['data_keys']) == set(ev['timestamps'])
        for k in 'cdh':
            assert desc['data_keys'][k]['source'] == 'syn'
            assert_array_equal(ev['data'][k], evs[j // M]['data'][k])

        for k in 'abg':
            src = desc['data_keys'][k]['source']
            assert src == str(j // M)
            source_ev = ev_dict[src]
            assert_array_equal(ev['data'][k],
                               source_ev['data'][k][ev['data']['_ind']])


def test_zip_events_smoke():
    dd = _zip_data_helper(('abc', 'def'), 10)
    for ev in zip_events(*dd):
        assert set('abcdef') == set(ev['descriptor']['data_keys'])
        assert set('abcdef') == set(ev['data'])
        assert set('abcdef') == set(ev['timestamps'])


def test_zip_events_lazy():
    dd = _zip_data_helper(('abc', 'def'), 10)
    for ev in zip_events(*dd, lazy=False):
        assert set('abcdef') == set(ev['descriptor']['data_keys'])
        assert set('abcdef') == set(ev['data'])
        assert set('abcdef') == set(ev['timestamps'])


def test_reset_time_smoke():
    evs = list(_reset_time_data_helper())
    revs = list(reset_time(evs, 'a'))
    for ev, rev in zip(evs, revs):
        assert rev['time'] == rev['timestamps']['a']
        assert ev['data'] == rev['data']
        assert ev['timestamps'] == rev['timestamps']
