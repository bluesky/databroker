from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six
import numpy as np
import uuid
import itertools

from ..api import (insert_resource, insert_datum, retrieve,
                           register_handler, deregister_handler,
                           bulk_insert_datum)
from numpy.testing import assert_array_equal

from .utils import SynHandlerMod, fs_setup, fs_teardown
import pymongo.errors
import pytest


def setup_module(module):
    fs_setup()
    # register the dummy handler to use
    register_handler('syn-mod', SynHandlerMod)


def teardown_module(module):
    fs_teardown()
    deregister_handler('syn-mod')


def _insert_syn_data(f_type, shape, count):
    fb = insert_resource(f_type, None, {'shape': shape})
    ret = []
    res_map_cycle = itertools.cycle((lambda x: x,
                                     lambda x: x['id'],
                                     lambda x: str(x['id'])))
    for k, rmap in zip(range(count), res_map_cycle):
        r_id = str(uuid.uuid4())
        insert_datum(rmap(fb), r_id, {'n': k + 1})
        ret.append(r_id)
    return ret


def _insert_syn_data_bulk(f_type, shape, count):
    fb = insert_resource(f_type, None, {'shape': shape})
    d_uid = [str(uuid.uuid4()) for k in range(count)]
    d_kwargs = [{'n': k + 1} for k in range(count)]
    bulk_insert_datum(fb, d_uid, d_kwargs)

    return d_uid


@pytest.mark.parametrize('func', [_insert_syn_data, _insert_syn_data_bulk])
def test_insert_funcs(func):
    shape = (25, 32)
    mod_ids = func('syn-mod', shape, 10)

    for j, r_id in enumerate(mod_ids):
        data = retrieve(r_id)
        known_data = np.mod(np.arange(np.prod(shape)), j + 1).reshape(shape)
        assert_array_equal(data, known_data)


def test_non_exist():
    from .. import api
    with pytest.raises(api._FS_SINGLETON.DatumNotFound):
        retrieve('aardvark')


def test_non_unique_fail():
    shape = (25, 32)
    fb = insert_resource('syn-mod', None, {'shape': shape})
    r_id = str(uuid.uuid4())
    insert_datum(str(fb['id']), r_id, {'n': 0})
    with pytest.raises(pymongo.errors.DuplicateKeyError):
        insert_datum(str(fb['id']), r_id, {'n': 1})


def test_index():
    from .. import api
    fs = api._FS_SINGLETON

    indx = fs._datum_col.index_information()

    assert len(indx) == 3
    index_fields = set(v['key'][0][0] for v in indx.values())
    assert index_fields == {'_id', 'datum_id', 'resource'}
