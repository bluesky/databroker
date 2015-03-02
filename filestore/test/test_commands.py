from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six
import numpy as np
import uuid
import mongoengine
import mongoengine.connection


import filestore.retrieve
from filestore.api import (insert_resource, insert_datum, retrieve,
                           register_handler)
from filestore.odm_templates import Datum
from filestore.utils.testing import fs_setup, fs_teardown
from numpy.testing import assert_array_equal
from nose.tools import assert_raises

from .t_utils import SynHandlerMod


def setup():
    fs_setup()
    # register the dummy handler to use
    register_handler('syn-mod', SynHandlerMod)


def teardown():
    fs_teardown()

    del filestore.retrieve._h_registry['syn-mod']


def _insert_syn_data(f_type, shape, count):
    fb = insert_resource(f_type, None, {'shape': shape})
    ret = []
    for k in range(count):
        r_id = str(uuid.uuid4())
        insert_datum(fb, r_id, {'n': k + 1})
        ret.append(r_id)
    return ret


def test_round_trip():
    shape = (25, 32)
    mod_ids = _insert_syn_data('syn-mod', shape, 10)

    for j, r_id in enumerate(mod_ids):
        data = retrieve(r_id)
        known_data = np.mod(np.arange(np.prod(shape)), j + 1).reshape(shape)
        assert_array_equal(data, known_data)


def test_non_exist():
    assert_raises(Datum.DoesNotExist, retrieve, 'aardvark')
