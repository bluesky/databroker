from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import numpy as np
from numpy.testing import assert_array_equal
from .utils import insert_syn_data_with_resource
from .utils import insert_syn_data


def test_read_old_in_new(fs_v01):
    fs0, fs1 = fs_v01
    shape = (25, 32)
    # save data using old schema
    mod_ids = insert_syn_data(fs0, 'syn-mod', shape, 10)

    for j, r_id in enumerate(mod_ids):
        # get back using new schema
        data = fs1.retrieve(r_id)
        known_data = np.mod(np.arange(np.prod(shape)), j + 1).reshape(shape)
        assert_array_equal(data, known_data)


def test_read_old_in_new_resource(fs_v01):
    fs0, fs1 = fs_v01
    shape = [25, 32]
    # save data using old schema
    mod_ids, res = insert_syn_data_with_resource(fs0, 'syn-mod',
                                                 shape, 10)

    resource = fs0.resource_given_uid(res)
    assert resource == res
    assert res == fs1.resource_given_uid(res)

    for j, d_id in enumerate(mod_ids):
        # get back using new schema
        d_res = fs1.resource_given_eid(d_id)
        assert d_res == resource
