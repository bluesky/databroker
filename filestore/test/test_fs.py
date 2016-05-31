from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import pytest

import os.path
import uuid
import numpy as np
import pymongo
from numpy.testing import assert_array_equal

import filestore.fs
from filestore.core import DatumNotFound
from .utils import SynHandlerMod, insert_syn_data, insert_syn_data_bulk
from filestore.utils import install_sentinels


@pytest.fixture(params=[0, 1], scope='function')
def fs(request):
    '''Provide a function level scoped FileStore instance talking to
    temporary database on localhost:27017 with both v0 and v1.

    '''
    version = request.param
    db_name = "fs_testing_disposable_{}".format(str(uuid.uuid4()))
    test_conf = dict(database=db_name, host='localhost',
                     port=27017)
    install_sentinels(test_conf, version)
    fs = filestore.fs.FileStore(test_conf, version=version)
    fs.register_handler('syn-mod', SynHandlerMod)

    def delete_dm():
        print("DROPPING DB")
        fs._connection.drop_database(db_name)

    request.addfinalizer(delete_dm)

    return fs


@pytest.fixture(scope='function')
def fs_v01(request):
    '''Provide a function level scoped FileStore instance talking to
    temporary database on localhost:27017 with both v0 and v1.

    '''
    db_name = "fs_testing_disposable_{}".format(str(uuid.uuid4()))
    test_conf = dict(database=db_name, host='localhost',
                     port=27017)
    # v0 does not check!
    install_sentinels(test_conf, 1)
    fs1 = filestore.fs.FileStore(test_conf, version=1)
    fs0 = filestore.fs.FileStore(test_conf, version=0)
    fs1.register_handler('syn-mod', SynHandlerMod)
    fs0.register_handler('syn-mod', SynHandlerMod)

    def delete_dm():
        print("DROPPING DB")
        fs1._connection.drop_database(db_name)

    request.addfinalizer(delete_dm)

    return fs0, fs1


@pytest.mark.parametrize('func', [insert_syn_data, insert_syn_data_bulk])
def test_insert_funcs(func, fs):
    shape = (25, 32)
    mod_ids = func(fs, 'syn-mod', shape, 10)

    for j, r_id in enumerate(mod_ids):
        data = fs.retrieve(r_id)
        known_data = np.mod(np.arange(np.prod(shape)), j + 1).reshape(shape)
        assert_array_equal(data, known_data)


def test_non_exist(fs):
    with pytest.raises(DatumNotFound):
        fs.retrieve('aardvark')


def test_non_unique_fail(fs):
    shape = (25, 32)
    fb = fs.insert_resource('syn-mod', None, {'shape': shape})
    r_id = str(uuid.uuid4())
    fs.insert_datum(str(fb['id']), r_id, {'n': 0})
    with pytest.raises(pymongo.errors.DuplicateKeyError):
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
