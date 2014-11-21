from __future__ import (absolute_import, division,
                        unicode_literals, print_function)
import six
import numpy as np

from replay.pipeline.pipeline import DataMuggler
from datetime import datetime
from nose.tools import assert_true, assert_equal


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
