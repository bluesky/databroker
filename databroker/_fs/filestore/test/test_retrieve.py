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


from .. import api as fsa
from .utils import fs_setup, fs_teardown
import numpy as np

from .utils import SynHandlerMod, SynHandlerEcho
import uuid
import pytest
logger = logging.getLogger(__name__)


def setup_module(module):
    fs_setup()


def teardown_module(module):
    fs_teardown()


def test_get_handler_global():
    h_cache = fsa._FS_SINGLETON._handler_cache
    mock_base = dict(spec='syn-mod',
                     resource_path='',
                     resource_kwargs={'shape': (5, 7)})

    res = fsa.insert_resource(**mock_base)
    cache_key = (str(res['id']), SynHandlerMod.__name__)
    with fsa.handler_context({'syn-mod': SynHandlerMod}):

        handle = fsa.get_spec_handler(res['id'])

        assert isinstance(handle, SynHandlerMod)
        assert cache_key in h_cache

    assert cache_key not in h_cache


def test_overwrite_global():
    h_cache = fsa._FS_SINGLETON._handler_cache
    mock_base = dict(spec='syn-mod',
                     resource_path='',
                     resource_kwargs={'shape': (5, 7)})

    res = fsa.insert_resource(**mock_base)

    cache_key = (str(res['id']), SynHandlerMod.__name__)
    with fsa.handler_context({'syn-mod': SynHandlerMod}):
        fsa.get_spec_handler(res['id'])
        assert cache_key in h_cache
        fsa.register_handler('syn-mod', SynHandlerEcho, overwrite=True)
        assert cache_key not in h_cache


def test_context():
    with SynHandlerMod('', (4, 2)) as hand:
        for j in range(1, 5):
            assert np.all(hand(j) < j)


def test_register_fail():
    with fsa.handler_context({'syn-mod': SynHandlerMod}):
        # shouldn't raise, it is a no-op as it is regiristering
        # the same class with the same name
        fsa.register_handler('syn-mod', SynHandlerMod)
        # should raise as it is trying to change the registered class
        with pytest.raises(RuntimeError):
            fsa.register_handler('syn-mod', SynHandlerEcho)


def test_context_manager_replace():
    with fsa.handler_context({'syn-mod': SynHandlerMod}):
        assert fsa._FS_SINGLETON.handler_reg['syn-mod'] is SynHandlerMod
        with fsa.handler_context({'syn-mod': SynHandlerEcho}):
            assert fsa._FS_SINGLETON.handler_reg['syn-mod'] is SynHandlerEcho
        assert fsa._FS_SINGLETON.handler_reg['syn-mod'] is SynHandlerMod
    assert 'syn-mod' not in fsa._FS_SINGLETON.handler_reg


def test_deregister():
    test_reg = fsa._FS_SINGLETON.handler_reg
    test_spec_name = str(uuid.uuid4())
    fsa.register_handler(test_spec_name, SynHandlerMod)
    assert test_reg[test_spec_name] is SynHandlerMod
    fsa.deregister_handler(test_spec_name)
    assert test_spec_name not in test_reg
