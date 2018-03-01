from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import pytest

import os.path
import uuid
import numpy as np
from numpy.testing import assert_array_equal

from .utils import insert_syn_data, insert_syn_data_bulk


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


def test_root(fs):
    print(fs._db)
    res = fs.insert_resource('root-test', 'foo', {}, root='bar')
    dm = fs.insert_datum(res, res['uid'] + '/0', {})
    if fs.version == 1:
        assert res['root'] == 'bar'

    def local_handler(rpath):
        return lambda: rpath

    with fs.handler_context({'root-test': local_handler}) as fs:
        path = fs.retrieve(dm['datum_id'])

    assert path == os.path.join('bar', 'foo')
