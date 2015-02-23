from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six
import numpy as np
import uuid
import mongoengine
import mongoengine.connection
from nose.tools import make_decorator
from mongoengine.context_managers import switch_db

import filestore.commands as fc
import filestore.retrieve as fsr
from filestore.odm_templates import Resource, Datum
from numpy.testing import assert_array_equal
from nose.tools import assert_raises

from .t_utils import SynHandlerMod

db_name = str(uuid.uuid4())
dummy_db_name = str(uuid.uuid4())


def setup():
    # need to make 'default' connection to point to no-where, just to be safe
    # need this to exist so the context managers work
    mongoengine.connect(dummy_db_name, 'fs')

    # connect to the db we are actually going to use
    mongoengine.connect(db_name, alias='test_db')
    # register the dummy handler to use
    fsr.register_handler('syn-mod', SynHandlerMod)


def teardown():
    conn = mongoengine.connection.get_connection('test_db')
    conn.drop_database(db_name)
    conn.drop_database(dummy_db_name)
    del fsr._h_registry['syn-mod']


def context_decorator(func):
    def inner():
        with switch_db(Resource, 'test_db'), \
          switch_db(Datum, 'test_db'):
            func()

    return make_decorator(func)(inner)


def _insert_syn_data(f_type, shape, count):
    fb = fc.insert_resource(f_type, '',
                           {'shape': shape})
    ret = []
    for k in range(count):
        r_id = str(uuid.uuid4())
        fc.insert_datum(fb, r_id, {'n': k + 1})
        ret.append(r_id)
    return ret


@context_decorator
def test_round_trip():
    shape = (25, 32)
    mod_ids = _insert_syn_data('syn-mod', shape, 10)

    for j, r_id in enumerate(mod_ids):
        data = fc.retrieve(r_id)
        known_data = np.mod(np.arange(np.prod(shape)), j + 1).reshape(shape)
        assert_array_equal(data, known_data)


@context_decorator
def test_non_exist():
    assert_raises(Datum.DoesNotExist, fc.retrieve, 'aardvark')
