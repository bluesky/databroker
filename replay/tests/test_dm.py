from __future__ import (absolute_import, division,
                        unicode_literals, print_function)
import six
import numpy as np

from replay.pipeline.pipeline import DataMuggler
from datetime import datetime
from nose.tools import assert_true, assert_equal
from numpy.testing import assert_array_equal

def test_maxframes():
    col_list = [('a', 'ffill', 0),
                ('b', 'ffill', 1),
                ('c', 'ffill', 2)]

    dm = DataMuggler(col_list, max_frames=5)
    print(dm.max_frames)
    for j in range(12):
        ts = datetime.now()
        data_dict = {'a': j, 'b': np.ones(2) * j,
                     'c': np.ones((2, 2)) * j}

        dm.append_data(ts, data_dict)

        a = dm.get_values('a', [])[1]['a']
        b = dm.get_values('b', [])[1]['b']
        c = dm.get_values('c', [])[1]['c']

        assert_equal(len(a), j+1)
        assert_equal(a[-1], j)
        print(len(b))
        assert_true(len(b) <= 5)
        assert_equal(int(np.mean(b[-1])), j)
        assert_true(len(c) <= 5)
        assert_equal(int(np.mean(c[-1])), j)


def test_get_row():
    col_list = [('a', 'ffill', 0),
                ('b', 'ffill', 1),
                ('c', 'ffill', 2)]

    dm = DataMuggler(col_list)
    ts_list = []
    for j in range(12):
        ts = datetime.now()
        ts_list.append(ts)
        data_dict = {'a': j, 'b': np.ones(2) * j,
                     'c': np.ones((2, 2)) * j}

        dm.append_data(ts, data_dict)

    print(dm._dataframe)

    for j, ts in enumerate(ts_list):
        res = dm.get_row(ts, ['a', 'b', 'c'])
        assert_equal(res['a'], j)
        assert_array_equal(res['b'], np.ones(2) * j)
        assert_array_equal(res['c'], np.ones((2, 2)) * j)


def test_get_col():
    col_list = [('a', 'ffill', 0),
                ('b', 'ffill', 1),
                ('c', 'ffill', 2)]

    dm = DataMuggler(col_list)
    for j in range(12):
        ts = datetime.now()
        data_dict = {'b': np.ones(2) * j,
                     'c': np.ones((2, 2)) * j}
        if j % 2:
            data_dict['a'] = j

        dm.append_data(ts, data_dict)

    ts_lst, col_vals = dm.get_column('a')
    target_vals = [j * 2 + 1 for j in range(6)]
    assert_array_equal(col_vals, target_vals)
