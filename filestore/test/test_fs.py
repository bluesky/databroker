from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six
import pytest

import os.path
import uuid
import itertools
import numpy as np
import pymongo
from numpy.testing import assert_array_equal

import filestore.fs
from filestore.core import DatumNotFound
from .utils import SynHandlerMod


def _insert_syn_data(fs, f_type, shape, count):
    fb = fs.insert_resource(f_type, None, {'shape': shape})
    ret = []
    res_map_cycle = itertools.cycle((lambda x: x,
                                     lambda x: x['id'],
                                     lambda x: str(x['id'])))
    for k, rmap in zip(range(count), res_map_cycle):
        r_id = str(uuid.uuid4())
        fs.insert_datum(rmap(fb), r_id, {'n': k + 1})
        ret.append(r_id)
    return ret


def _insert_syn_data_bulk(fs, f_type, shape, count):
    fb = fs.insert_resource(f_type, None, {'shape': shape})
    d_uid = [str(uuid.uuid4()) for k in range(count)]
    d_kwargs = [{'n': k + 1} for k in range(count)]
    fs.bulk_insert_datum(fb, d_uid, d_kwargs)

    return d_uid


@pytest.fixture(params=[0, 1], scope='function')
def fs(request):
    db_name = "fs_testing_disposable_{}".format(str(uuid.uuid4()))
    test_conf = dict(database=db_name, host='localhost',
                     port=27017)
    fs = filestore.fs.FileStore(test_conf,
                                version=request.param)
    fs.register_handler('syn-mod', SynHandlerMod)

    def delete_dm():
        print("DROPPING DB")
        fs._connection.drop_database(db_name)

    request.addfinalizer(delete_dm)

    return fs


@pytest.mark.parametrize('func', [_insert_syn_data, _insert_syn_data_bulk])
def test_insert_funcs(func, fs):
    shape = (25, 32)
    mod_ids = func(fs, 'syn-mod', shape, 10)

    for j, r_id in enumerate(mod_ids):
        data = fs.get_datum(r_id)
        known_data = np.mod(np.arange(np.prod(shape)), j + 1).reshape(shape)
        assert_array_equal(data, known_data)


def test_non_exist(fs):
    with pytest.raises(DatumNotFound):
        fs.get_datum('aardvark')


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


def test_chroot(fs):
    print(fs._db)
    res = fs.insert_resource('chroot-test', 'foo', {}, chroot='bar')
    dm = fs.insert_datum(res, str(uuid.uuid4()), {})
    if fs.version == 1:
        assert res['chroot'] == 'bar'

    def local_handler(rpath):
        return lambda: rpath

    with fs.handler_context({'chroot-test': local_handler}) as fs:
        path = fs.get_datum(dm['datum_id'])

    assert path == os.path.join('bar', 'foo')
