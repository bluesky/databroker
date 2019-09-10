from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import pytest
from itertools import chain, product
import os.path
import uuid
import numpy as np

from ..handlers_base import HandlerBase


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


def num_paths(start, stop):
    return os.path.join(*(str(_)
                          for _ in range(start, stop)))


@pytest.mark.flaky(reruns=5, reruns_delay=2)
@pytest.mark.parametrize("step,sign", product([1, 3, 5, 7], [1, -1]))
def test_root_shift(fs_v1, step, sign):
    fs = fs_v1
    n_paths = 15

    if sign > 0:
        root = ''
        rpath = '/' + num_paths(0, n_paths)
    elif sign < 0:
        root = '/' + num_paths(0, n_paths)
        rpath = ''

    last_res = fs.insert_resource('root-test',
                                  rpath,
                                  {'a': 'fizz', 'b': 5},
                                  root=root,
                                  run_start=str(uuid.uuid4()))
    for n, j in enumerate(range(step, n_paths, step)):
        new_res, log = fs.shift_root(last_res, sign * step)
        assert last_res == log['old']

        if sign > 0:
            left_count = j
        elif sign < 0:
            left_count = n_paths - j

        assert new_res['root'] == '/' + num_paths(0, left_count)
        assert new_res['resource_path'] == num_paths(left_count, n_paths)
        _verify_shifted_resource(last_res, new_res)
        last_res = new_res


@pytest.mark.flaky(reruns=5, reruns_delay=2)
@pytest.mark.parametrize("root", ['', '///', None])
def test_pathological_root(fs_v1, root):
    fs = fs_v1
    rpath = '/foo/bar/baz'
    last_res = fs.insert_resource('root-test',
                                  rpath,
                                  {'a': 'fizz', 'b': 5},
                                  root=root,
                                  run_start=str(uuid.uuid4()))
    new_res, _ = fs.shift_root(last_res, 2)
    assert new_res['root'] == '/foo/bar'
    assert new_res['resource_path'] == 'baz'


def test_history(fs_v1):
    fs = fs_v1
    rpath = num_paths(0, 15)
    root = '/'
    shift_count = 5
    last_res = fs.insert_resource('root-test',
                                  rpath,
                                  {'a': 'fizz', 'b': 5},
                                  root=root,
                                  run_start=str(uuid.uuid4()))
    for j in range(shift_count):
        new_res, log = fs.shift_root(last_res, 1)

    last_time = 0
    cnt = 0
    for doc in fs.get_history(last_res['uid']):
        assert doc['time'] > last_time
        assert doc['cmd'] == 'shift_root'
        assert doc['cmd_kwargs'] == {'shift': 1}
        assert doc['old'] == last_res

        last_res = doc['new']
        last_time = doc['time']
        cnt += 1

    assert cnt == shift_count


@pytest.mark.parametrize('shift', [-5, 5])
def test_over_step(fs_v1, shift):
    fs = fs_v1
    last_res = fs.insert_resource('root-test',
                                  'a/b',
                                  {'a': 'fizz', 'b': 5},
                                  root='/c',
                                  run_start=str(uuid.uuid4()))
    with pytest.raises(RuntimeError):
        fs.shift_root(last_res, shift)


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
def moving_files(request, fs_v1, tmpdir):
    tmpdir = str(tmpdir)
    cnt = 15
    shape = (7, 13)

    local_path = '2016/04/28/aardvark'
    fmt = 'cub_{point_number:05}.npy'
    res = fs_v1.insert_resource('npy_series',
                                local_path,
                                {'fmt': fmt},
                                root=tmpdir,
                                run_start=str(uuid.uuid4()))

    datum_ids = []
    fnames = []
    os.makedirs(os.path.join(tmpdir, local_path))
    for j in range(cnt):
        fpath = os.path.join(tmpdir, local_path,
                             fmt.format(point_number=j))
        np.save(fpath, np.ones(shape) * j)
        d = fs_v1.insert_datum(res, '{}/{}'.format(res['uid'], j),
                               {'point_number': j})
        datum_ids.append(d['datum_id'])
        fnames.append(fpath)

    return fs_v1, res, datum_ids, shape, cnt, fnames


@pytest.mark.parametrize("remove", [True, False])
def test_moving(moving_files, remove):
    fs, res, datum_ids, shape, cnt, fnames = moving_files
    fs.register_handler('npy_series', FileMoveTestingHandler)

    # sanity check on the way in
    for j, d_id in enumerate(datum_ids):
        datum = fs.retrieve(d_id)
        assert np.prod(shape) * j == np.sum(datum)

    old_root = res['root']
    new_root = os.path.join(old_root, 'archive')
    for f in fnames:
        assert os.path.exists(f)

    res2, log = fs.move_files(res, new_root, remove_origin=remove)
    print(res2['root'])
    for f in fnames:
        if old_root:
            assert os.path.exists(f.replace(old_root, new_root))
        else:
            assert os.path.exists(os.path.join(new_root, f[1:]))
        if remove:
            assert not os.path.exists(f)
        else:
            assert os.path.exists(f)

    # sanity check on the way out
    for j, d_id in enumerate(datum_ids):
        datum = fs.retrieve(d_id)
        assert np.prod(shape) * j == np.sum(datum)


def test_no_root(fs_v1, tmpdir):
    fs = fs_v1
    fs.register_handler('npy_series', FileMoveTestingHandler)

    local_path = 'aardvark'
    fmt = 'cub_{point_number:05}.npy'
    res = fs.insert_resource('npy_series',
                             os.path.join(str(tmpdir),
                                          local_path),
                             {'fmt': fmt},
                             run_start=str(uuid.uuid4()))
    fs_v1.move_files(res, '/foobar')


def test_get_resource(moving_files):
    fs, res, datum_ids, shape, cnt, fnames = moving_files
    for d in datum_ids:
        d_res = fs.resource_given_datum_id(d)
        assert d_res == res
        print(d_res, res)


def test_temporary_root(fs_v1):
    fs = fs_v1
    print(fs._db)
    fs.set_root_map({'bar': 'baz', 'bar2' : 'baz2'})
    print(fs.root_map)
    print(fs._handler_cache)
    res = fs.insert_resource('root-test', 'foo', {}, root='bar',
                             run_start=str(uuid.uuid4()))
    dm = fs.insert_datum(res, res['uid'] + '/0', {})
    if fs.version == 1:
        assert res['root'] == 'bar'


    def local_handler(rpath):
        return lambda: rpath

    with fs.handler_context({'root-test': local_handler}) as fs:
        print(fs._handler_cache)
        assert not len(fs._handler_cache)
        path = fs.retrieve(dm['datum_id'])

    assert path == os.path.join('baz', 'foo')

    # test two root maps work
    res = fs.insert_resource('root-test', 'foo', {}, root='bar2',
                             run_start=str(uuid.uuid4()))
    dm = fs.insert_datum(res, res['uid'] + '/0', {})
    if fs.version == 1:
        assert res['root'] == 'bar2'

    with fs.handler_context({'root-test': local_handler}) as fs:
        print(fs._handler_cache)
        assert not len(fs._handler_cache)
        path = fs.retrieve(dm['datum_id'])

    assert path == os.path.join('baz2', 'foo')
