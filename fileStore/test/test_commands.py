from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six
import numpy as np
import uuid
import mongoengine
import mongoengine.connection
from nose.tools import make_decorator
from mongoengine.context_managers import switch_db

import fileStore.commands as fc
import fileStore.retrieve as fsr
from fileStore.database.file_base import FileBase
from fileStore.database.file_event_link import FileEventLink
from numpy.testing import assert_array_equal

from .t_utils import SynHandlerMod, SynHandlerEcho

db_name = str(uuid.uuid4())
dummy_db_name = str(uuid.uuid4())


def setup():
    # need to make 'default' connection to point to no-where, just to be safe
    mongoengine.connect(dummy_db_name)
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
        with switch_db(FileBase, 'test_db'), \
          switch_db(FileEventLink, 'test_db'):
            func()

    return make_decorator(func)(inner)


def _insert_syn_data(f_type, shape, count):
    fb = fc.save_file_base(f_type, '',
                           {'shape': shape})
    ret = []
    for k in range(count):
        r_id = str(uuid.uuid4())
        fc.save_file_event_link(fb, r_id, {'n': k + 1})
        ret.append(r_id)
    return ret


@context_decorator
def test_round_trip():
    shape = (25, 32)
    mod_ids = _insert_syn_data('syn-mod', shape, 10)

    for j, r_id in enumerate(mod_ids):
        data = fc.retrieve_data(r_id)
        known_data = np.mod(np.arange(np.prod(shape)), j + 1).reshape(shape)
        assert_array_equal(data, known_data)
