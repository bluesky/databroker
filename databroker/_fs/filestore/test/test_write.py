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
from itertools import chain, repeat
from .. import api as fsa
from .. import file_writers as fs_write
from .. import handlers as fs_read
from .utils import fs_setup, fs_teardown
import pytest
import uuid
from numpy.testing import assert_array_equal

import tempfile
import shutil
import os

logger = logging.getLogger(__name__)

CLEAN_FILES = True
BASE_PATH = None
db_name = str(uuid.uuid4())
dummy_db_name = str(uuid.uuid4())

db_name = str(uuid.uuid4())
conn = None


def setup_module(module):
    fs_setup()

    global BASE_PATH
    BASE_PATH = tempfile.mkdtemp()


def teardown_module(module):
    fs_teardown()
    if CLEAN_FILES:
        shutil.rmtree(BASE_PATH)


def _npsave_helper(dd, base_path):
    eid = fs_write.save_ndarray(dd, base_path)
    with fsa.handler_context({'npy': fs_read.NpyHandler}):
        ret = fsa.retrieve(eid)

    assert_array_equal(dd, ret)


def test_np_save():
    shape = (7, 5)
    dd = np.arange(np.prod(shape)).reshape(*shape)

    for d, bp in zip([dd, dd.astype('float'), np.ones(15)],
                     chain([None, ], repeat(BASE_PATH))):
        yield _npsave_helper, d, bp


def test_file_exist_fail():

    test_path = os.path.join(BASE_PATH, str(uuid.uuid4()) + '.npy')
    test_w = fs_write.NpyWriter(test_path)
    test_w.add_data(5)
    with pytest.raises(IOError):
        fs_write.NpyWriter(test_path)


def test_multi_write_fail():
    test_path = os.path.join(BASE_PATH, str(uuid.uuid4()) + '.npy')
    test_w = fs_write.NpyWriter(test_path)
    test_w.add_data(5)
    with pytest.raises(RuntimeError):
        test_w.add_data(6)


def test_event_custom_fail():
    test_path = os.path.join(BASE_PATH, str(uuid.uuid4()) + '.npy')
    test_w = fs_write.NpyWriter(test_path)
    with pytest.raises(ValueError):
        test_w.add_data(6, None, {'aardvark': 3.14})


def test_file_custom_fail():
    test_path = os.path.join(BASE_PATH, str(uuid.uuid4()) + '.npy')
    with pytest.raises(ValueError):
        fs_write.NpyWriter(test_path, {'aardvark': 3.14})


def test_sneaky_write_fail():
    test_path = os.path.join(BASE_PATH, str(uuid.uuid4()) + '.npy')
    test_w = fs_write.NpyWriter(test_path)
    with open(test_path, 'w') as fout:
        fout.write('aardvark')
    with pytest.raises(IOError):
        test_w.add_data(5)


def test_give_uid():
    test_path = os.path.join(BASE_PATH, str(uuid.uuid4()) + '.npy')
    uid = str(uuid.uuid4())
    with fs_write.NpyWriter(test_path) as fout:
        eid = fout.add_data([1, 2, 3], uid)
    assert uid == eid


def test_custom():
    test_path = os.path.join(BASE_PATH, str(uuid.uuid4()) + '.npy')
    dd = np.random.rand(500, 500)
    with fs_write.NpyWriter(test_path, resource_kwargs={'mmap_mode': 'r'}) as f:
        eid = f.add_data(dd)
    with fsa.handler_context({'npy': fs_read.NpyHandler}):
        ret = fsa.retrieve(eid)

    assert_array_equal(dd, ret)
