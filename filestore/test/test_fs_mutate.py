from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import pytest
from itertools import chain, product
import os.path
import uuid


import filestore.fs


@pytest.fixture(scope='function')
def fs_v1(request):
    '''Provide a function level scoped FileStore instance talking to
    temporary database on localhost:27017. v1 only

    '''
    db_name = "fs_testing_disposable_{}".format(str(uuid.uuid4()))
    test_conf = dict(database=db_name, host='localhost',
                     port=27017)
    fs = filestore.fs.FileStore(test_conf,
                                version=1)

    def delete_dm():
        print("DROPPING DB")
        fs._connection.drop_database(db_name)

    request.addfinalizer(delete_dm)

    return fs


def _verify_shifted_resource(last_res, new_res):
    '''Check that resources are identical except for chroot/rpath'''
    for k in set(chain(new_res, last_res)):
        if k not in ('chroot', 'resource_path'):
            assert new_res[k] == last_res[k]
        else:
            assert new_res[k] != last_res[k]

    n_fp = os.path.join(new_res['chroot'],
                        new_res['resource_path'])
    l_fp = os.path.join(last_res['chroot'],
                        last_res['resource_path']).rstrip('/')
    assert n_fp == l_fp


class TestChroot:

    @pytest.mark.parametrize("step,sign", product([1, 3, 5, 7], [1, -1]))
    def test_chroot_shift(self, fs_v1, step, sign):
        fs = fs_v1
        n_paths = 15

        def num_paths(start, stop):
            return os.path.join(*(str(_)
                                  for _ in range(start, stop)))
        if sign > 0:
            chroot = '/'
            rpath = num_paths(0, n_paths)
        elif sign < 0:
            chroot = '/' + num_paths(0, n_paths)
            rpath = ''

        last_res = fs.insert_resource('chroot-test',
                                      rpath,
                                      {'a': 'fizz', 'b': 5},
                                      chroot=chroot)
        for j in range(step, n_paths, step):
            new_res, log, _ = fs.shift_chroot(last_res, sign * step)
            assert last_res == log['old']

            if sign > 0:
                left_count = j
            elif sign < 0:
                left_count = n_paths - j

            assert new_res['chroot'] == '/' + num_paths(0, left_count)
            assert new_res['resource_path'] == num_paths(left_count, n_paths)
            _verify_shifted_resource(last_res, new_res)
            last_res = new_res
