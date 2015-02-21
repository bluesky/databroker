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


from filestore.odm_templates import Resource, Dattum

import filestore.retrieve as fsr
import numpy as np
from nose.tools import assert_true, assert_raises, assert_false

from .t_utils import SynHandlerMod, SynHandlerEcho
import uuid

logger = logging.getLogger(__name__)


mock_base = Resource(spec='syn-mod',
                     resource_path='',
                     resource_kwargs={'shape': (5, 7)})

mock_event = {n: Dattum(resource=mock_base,
                               event_id=n,
                               dattum_kwargs={'n': n})
                               for n in range(1, 3)}


def test_get_handler_global():

    with fsr.handler_context({'syn-mod': SynHandlerMod}):

        dattum = mock_base
        handle = fsr.get_spec_handler(dattum)

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
    with fsr.handler_context({'syn-mod': SynHandlerMod}):
        # shouldn't raise, it is a no-op as it is regiristering
        # the same class with the same name
        fsr.register_handler('syn-mod', SynHandlerMod)
        # should raise as it is trying to change the registered class
        assert_raises(RuntimeError, fsr.register_handler,
                      'syn-mod', SynHandlerEcho)


def test_context_manager_replace():
    with fsr.handler_context({'syn-mod': SynHandlerMod}):
        assert_true(fsr._h_registry['syn-mod'] is SynHandlerMod)
        with fsr.handler_context({'syn-mod': SynHandlerEcho}):
            assert_true(fsr._h_registry['syn-mod'] is SynHandlerEcho)
        assert_true(fsr._h_registry['syn-mod'] is SynHandlerMod)
    assert_false('syn-mod' in fsr._h_registry)


def test_deregister():
    test_spec_name = str(uuid.uuid4())
    fsr.register_handler(test_spec_name, SynHandlerMod)
    assert_true(fsr._h_registry[test_spec_name] is SynHandlerMod)
    fsr.deregister_handler(test_spec_name)
    assert_false(test_spec_name in fsr._h_registry)
