from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six
import numpy as np
import uuid
import mongoengine
import mongoengine.connection


import filestore.commands as fc
import filestore.retrieve as fsr
from filestore.odm_templates import Datum, ALIAS
from numpy.testing import assert_array_equal
from nose.tools import assert_raises

from .t_utils import SynHandlerMod

db_name = str(uuid.uuid4())
conn = None


def setup():
    global conn
    # make sure nothing is connected
    fc.db_disconnect()
    # make sure it _is_ connected
    conn = mongoengine.connect(db_name, host='localhost',
                        alias=ALIAS)

    # register the dummy handler to use
    fsr.register_handler('syn-mod', SynHandlerMod)


def teardown():
    fc.db_disconnect()
    # if we know about a connection, drop the database
    if conn:
        conn.drop_database(db_name)

    del fsr._h_registry['syn-mod']


def _insert_syn_data(f_type, shape, count):
    fb = fc.insert_resource(f_type, '',
                           {'shape': shape})
    ret = []
    for k in range(count):
        r_id = str(uuid.uuid4())
        fc.insert_datum(fb, r_id, {'n': k + 1})
        ret.append(r_id)
    return ret


def test_round_trip():
    shape = (25, 32)
    mod_ids = _insert_syn_data('syn-mod', shape, 10)

    for j, r_id in enumerate(mod_ids):
        data = fc.retrieve(r_id)
        known_data = np.mod(np.arange(np.prod(shape)), j + 1).reshape(shape)
        assert_array_equal(data, known_data)


def test_non_exist():
    assert_raises(Datum.DoesNotExist, fc.retrieve, 'aardvark')
