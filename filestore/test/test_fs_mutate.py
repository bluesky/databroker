from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import pytest
from itertools import chain, product
import os.path
import uuid
import numpy as np


import filestore.fs
from filestore.handlers_base import HandlerBase


@pytest.fixture(scope='function')
def fs_v1(request):
    '''Provide a function level scoped FileStore instance talking to
    temporary database on localhost:27017. v1 only

    '''
    db_name = "fs_testing_disposable_{}".format(str(uuid.uuid4()))
    test_conf = dict(database=db_name, host='localhost',
                     port=27017)
    fs = filestore.fs.FileStoreMoving(test_conf,
                                      version=1)

    def delete_dm():
        print("DROPPING DB")
        fs._connection.drop_database(db_name)

    request.addfinalizer(delete_dm)

    return fs


def _verify_shifted_resource(last_res, new_res):
    '''Check that resources are identical except for root/rpath'''
    for k in set(chain(new_res, last_res)):
        if k not in ('root', 'resource_path'):
            assert new_res[k] == last_res[k]
        else:
            assert new_res[k] != last_res[k]

    n_fp = os.path.join(new_res['root'],
                        new_res['resource_path'])
    l_fp = os.path.join(last_res['root'],
                        last_res['resource_path']).rstrip('/')
    assert n_fp == l_fp


@pytest.mark.parametrize("step,sign", product([1, 3, 5, 7], [1, -1]))
def test_root_shift(fs_v1, step, sign):
    fs = fs_v1
    n_paths = 15

    def num_paths(start, stop):
        return os.path.join(*(str(_)
                              for _ in range(start, stop)))
    if sign > 0:
        root = '/'
        rpath = num_paths(0, n_paths)
    elif sign < 0:
        root = '/' + num_paths(0, n_paths)
        rpath = ''

    last_res = fs.insert_resource('root-test',
                                  rpath,
                                  {'a': 'fizz', 'b': 5},
                                  root=root)
    for n, j in enumerate(range(step, n_paths, step)):
        new_res, log, _ = fs.shift_root(last_res, sign * step)
        assert last_res == log['old']

        if sign > 0:
            left_count = j
        elif sign < 0:
            left_count = n_paths - j

        assert new_res['root'] == '/' + num_paths(0, left_count)
        assert new_res['resource_path'] == num_paths(left_count, n_paths)
        _verify_shifted_resource(last_res, new_res)
        last_res = new_res


class FileMoveTestingHandler(HandlerBase):
    specs = {'npy_series'} | HandlerBase.specs

    def __init__(self, fpath, fmt):
        self.fpath = fpath
        self.fmt = fmt

    def __call__(self, point_number):
        fname = os.path.join(self.fpath,
                             self.fmt.format(point_number=point_number))
        return np.load(fname)

    def get_file_list(self, datumkw_gen):
        return [os.path.join(self.fpath,
                             self.fmt.format(**dkw))
                for dkw in datumkw_gen]


@pytest.fixture()
def moving_files(fs_v1, tmpdir):
    tmpdir = str(tmpdir)
    cnt = 15
    shape = (7, 13)

    local_path = '2016/04/28/aardvark'
    fmt = 'cub_{point_number:05}.npy'
    res = fs_v1.insert_resource('npy_series',
                                local_path,
                                {'fmt': fmt},
                                root=tmpdir)
    datum_uids = []
    fnames = []
    os.makedirs(os.path.join(tmpdir, local_path))
    for j in range(cnt):
        fpath = os.path.join(tmpdir, local_path,
                             fmt.format(point_number=j))
        np.save(fpath, np.ones(shape) * j)
        d = fs_v1.insert_datum(res, str(uuid.uuid4()),
                               {'point_number': j})
        datum_uids.append(d['datum_id'])
        fnames.append(fpath)

    return fs_v1, res, datum_uids, shape, cnt, fnames


def test_moving(moving_files):
    fs, res, datum_uids, shape, cnt, fnames = moving_files
    fs.register_handler('npy_series', FileMoveTestingHandler)

    # sanity check on the way in
    for j, d_id in enumerate(datum_uids):
        datum = fs.get_datum(d_id)
        assert np.prod(shape) * j == np.sum(datum)

    old_root = res['root']
    new_root = os.path.join(old_root, 'archive')
    for f in fnames:
        assert os.path.exists(f)

    res2, log, _ = fs.change_root(res, new_root)
    print(res2['root'])
    for f in fnames:
        assert os.path.exists(f.replace(old_root, new_root))
        assert not os.path.exists(f)

    # sanity check on the way out
    for j, d_id in enumerate(datum_uids):
        datum = fs.get_datum(d_id)
        assert np.prod(shape) * j == np.sum(datum)
