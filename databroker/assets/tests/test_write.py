# ######################################################################
# Copyright (c) 2014, Brookhaven Science Associates, Brookhaven        #
# National Laboratory. All rights reserved.                            #
#                                                                      #
# Redistribution and use in source and binary forms, with or without   #
# modification, are permitted provided that the following conditions   #
# are met:                                                             #
#                                                                      #
# * Redistributions of source code must retain the above copyright     #
#   notice, this list of conditions and the following disclaimer.      #
#                                                                      #
# * Redistributions in binary form must reproduce the above copyright  #
#   notice this list of conditions and the following disclaimer in     #
#   the documentation and/or other materials provided with the         #
#   distribution.                                                      #
#                                                                      #
# * Neither the name of the Brookhaven Science Associates, Brookhaven  #
#   National Laboratory nor the names of its contributors may be used  #
#   to endorse or promote products derived from this software without  #
#   specific prior written permission.                                 #
#                                                                      #
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS  #
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT    #
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS    #
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE       #
# COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,           #
# INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES   #
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR   #
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)   #
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,  #
# STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OTHERWISE) ARISING   #
# IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE   #
# POSSIBILITY OF SUCH DAMAGE.                                          #
########################################################################

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six
import logging

import numpy as np

from .. import file_writers as fs_write
from .. import handlers as fs_read
import pytest
import uuid
from numpy.testing import assert_array_equal

import tempfile
import os

logger = logging.getLogger(__name__)

CLEAN_FILES = True
BASE_PATH = None

BASE_PATH = tempfile.mkdtemp()


def _data_dec(func):
    shape = (7, 5)
    dd = np.arange(np.prod(shape)).reshape(*shape)

    return pytest.mark.parametrize('dd',
                                   [dd, dd.astype(float),
                                    np.ones(15), dd.astype(int)])(func)


@pytest.mark.flaky(reruns=5, reruns_delay=2)
@_data_dec
@pytest.mark.parametrize('base_path', [None, BASE_PATH],
                         ids=['None', 'tmpdir'])
def test_np_save(dd, base_path, fs):

    datum_id = fs_write.save_ndarray(dd, fs, base_path)
    with fs.handler_context({'npy': fs_read.NpyHandler}):
        ret = fs.retrieve(datum_id)

    assert_array_equal(dd, ret)


def test_file_exist_fail(fs):

    test_path = os.path.join(BASE_PATH, str(uuid.uuid4()) + '.npy')
    test_w = fs_write.NpyWriter(test_path, fs)
    test_w.add_data(5)
    with pytest.raises(IOError):
        fs_write.NpyWriter(test_path, fs)


def test_multi_write_fail(fs):
    test_path = os.path.join(BASE_PATH, str(uuid.uuid4()) + '.npy')
    test_w = fs_write.NpyWriter(test_path, fs)
    test_w.add_data(5)
    with pytest.raises(RuntimeError):
        test_w.add_data(6)


def test_event_custom_fail(fs):
    test_path = os.path.join(BASE_PATH, str(uuid.uuid4()) + '.npy')
    test_w = fs_write.NpyWriter(test_path, fs)
    with pytest.raises(ValueError):
        test_w.add_data(6, None, {'aardvark': 3.14})


def test_file_custom_fail(fs):
    test_path = os.path.join(BASE_PATH, str(uuid.uuid4()) + '.npy')
    with pytest.raises(ValueError):
        fs_write.NpyWriter(test_path, fs, {'aardvark': 3.14})


def test_sneaky_write_fail(fs):
    test_path = os.path.join(BASE_PATH, str(uuid.uuid4()) + '.npy')
    test_w = fs_write.NpyWriter(test_path, fs=fs)
    with open(test_path, 'w') as fout:
        fout.write('aardvark')
    with pytest.raises(IOError):
        test_w.add_data(5)


def test_custom(fs):
    fsa = fs
    test_path = os.path.join(BASE_PATH, str(uuid.uuid4()) + '.npy')
    dd = np.random.rand(500, 500)
    with fs_write.NpyWriter(test_path,
                            resource_kwargs={'mmap_mode': 'r'},
                            fs=fs) as f:
        datum_id = f.add_data(dd)
    with fsa.handler_context({'npy': fs_read.NpyHandler}):
        ret = fsa.retrieve(datum_id)

    assert_array_equal(dd, ret)
