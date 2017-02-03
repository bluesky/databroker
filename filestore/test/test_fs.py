from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import pytest

import os.path
import uuid
import numpy as np
import pymongo
from numpy.testing import assert_array_equal

from .utils import (insert_syn_data, insert_syn_data_bulk, install_sentinels)


@pytest.mark.parametrize('func', [insert_syn_data, insert_syn_data_bulk])
def test_insert_funcs(func, fs):
    shape = (25, 32)
    mod_ids = func(fs, 'syn-mod', shape, 10)

    for j, r_id in enumerate(mod_ids):
        data = fs.retrieve(r_id)
        known_data = np.mod(np.arange(np.prod(shape)), j + 1).reshape(shape)
        assert_array_equal(data, known_data)


def test_non_exist(fs):

    with pytest.raises(fs.DatumNotFound):
        fs.retrieve('aardvark')


def test_non_unique_fail(fs):
    shape = (25, 32)
    fb = fs.insert_resource('syn-mod', None, {'shape': shape})
    r_id = str(uuid.uuid4())
    fs.insert_datum(str(fb['id']), r_id, {'n': 0})
    with pytest.raises(fs.DuplicateKeyError):
        fs.insert_datum(str(fb['id']), r_id, {'n': 1})


def test_index(fs):

    indx = fs._datum_col.index_information()

    assert len(indx) == 3
    index_fields = set(v['key'][0][0] for v in indx.values())
    assert index_fields == {'_id', 'datum_id', 'resource'}


def test_root(fs):
    print(fs._db)
    res = fs.insert_resource('root-test', 'foo', {}, root='bar')
    dm = fs.insert_datum(res, str(uuid.uuid4()), {})
    if fs.version == 1:
        assert res['root'] == 'bar'

    def local_handler(rpath):
        return lambda: rpath

    with fs.handler_context({'root-test': local_handler}) as fs:
        path = fs.retrieve(dm['datum_id'])

    assert path == os.path.join('bar', 'foo')


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


def test_double_sentinel(fs):
    with pytest.raises(RuntimeError):
        install_sentinels(fs.config, fs.version)
