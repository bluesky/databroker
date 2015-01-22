# ######################################################################
# Copyright (c) 2015, Brookhaven Science Associates, Brookhaven        #
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
import uuid
logger = logging.getLogger(__name__)

from ._dummies import _events, _ev_desc, DummyEventDescriptor, DummyEvent


def save_event_descriptor(header=None, event_type_id=None,
                          descriptor_name=None,
                          data_keys=None, type_descriptor=None):
    """ Create an event_descriptor in metadataStore database backend

    Parameters
    ----------

    header : mongoengine.Document
        Header object that specific beamline_config entry is going to
        point(foreign key)

    event_type_id : int
        Integer identifier for a scan, sweep, etc.

    data_keys : list
        Provides information about keys of the data dictionary in an event
        will contain

    descriptor_name : str
        Unique identifier string for an event. e.g. ascan, dscan, hscan,
        home, sweep,etc.

    Other Parameters
    ----------------
    type_descriptor : dict
        Additional name/value pairs can be added to an event_descriptor
        using this flexible field

    """
    uid = str(uuid.uuid4())
    dd = DummyEventDescriptor(keys=data_keys,
                              uid=uid)
    _ev_desc[uid] = dd
    return dd


def save_event(header=None, event_descriptor=None, seq_no=None,
               timestamp=None, data=None, **kwargs):
    """Create an event in metadataStore database backend

    Parameters
    ----------

    header : mongoengine.Document
        Header object that specific event entry is going to point(foreign key)

    event_descriptor : mongoengine.Document
        EventDescriptor object that specific event entry is going to
        point(foreign key)

    seq_no : int
        Unique sequence number for the event. Provides order of an event in
        the group of events

    data : dict
        Dictionary that contains the name value fields for the data associated
        with an event

    Other Parameters
    ----------------

    owner : str
        Specifies the unix user credentials of the user creating the entry

    description : str
        Text description of specific event

    """
    uid = str(uuid.uuid4())
    dd = DummyEvent(uid=uid,
                    data=data,
                    ev_desc=event_descriptor,
                    seqno=seq_no,
                    timestamp=timestamp)
    _events[uid] = dd
    return dd
