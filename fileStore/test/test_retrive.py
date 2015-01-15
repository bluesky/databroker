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
logger = logging.getLogger(__name__)

from fileStore.retrieve import HandlerBase
from fileStore.database.file_base import FileBase
from fileStore.database.file_event_link import FileEventLink
import fileStore.retrieve as fsr
import numpy as np
from nose.tools import assert_equal, assert_true, assert_raises
from contextlib import contextmanager


class SynHandlerMod(HandlerBase):
    """
    A handler for synthetic data which will return a ramp % n reshaped
    to the frame size for frame n

    Parameters
    ----------
    shape : tuple
        The shape of the frame
    """
    def __init__(self, fpath, shape):
        self._shape = tuple(int(v) for v in shape)
        self._N = np.prod(self._shape)

    def __call__(self, n):
        return np.mod(np.arange(self._N), n).reshape(self._shape)

mock_base = FileBase(spec='syn-mod',
                     file_path='',
                     custom={'shape': (5, 7)})

mock_event = {n: FileEventLink(file_base=mock_base,
                               event_id=n,
                               link_parameters={'n': n})
                               for n in range(1, 50)}


@contextmanager
def fsr_reg_context():
    fsr.register_handler('syn-mod', SynHandlerMod)
    yield
    del fsr._h_registry['syn-mod']


def test_get_handler_global():

    with fsr_reg_context():

        fs_doc = mock_base
        handle = fsr.get_spec_handler(fs_doc)

        assert_true(isinstance(handle, SynHandlerMod))


def _help_test_data(event_doc):
    data = fsr.get_data(event_doc, {'syn-mod': SynHandlerMod})

    assert_true(np.all(data < event_doc.event_id))


def test_get_data():
    for v in mock_event.values():
        yield _help_test_data, v


def test_context():
    with SynHandlerMod('', (4, 2)) as hand:
        for j in range(1, 5):
            assert_true(np.all(hand(j) < j))


def test_register_fail():
    with fsr_reg_context():
        assert_raises(RuntimeError, fsr.register_handler,
                      'syn-mod', SynHandlerMod)
