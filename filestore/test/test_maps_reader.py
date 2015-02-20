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

from metadataStore.api.collection import (insert_begin_run,
                                          insert_event,
                                          insert_event_descriptor,
                                          insert_beamline_config)
import filestore.retrieve as fsr
import filestore.commands as fsc

from filestore.file_readers import HDFMapsSpectrumHandler as HDFM

import os.path as op
import numpy as np
from nose.tools import assert_true, assert_raises, assert_false

import uuid

import logging
logger = logging.getLogger(__name__)


fsr.register_handler('hdf_maps', HDFM)


def save_hdf_data(data, base_path=None):
    """
    Save a array as hdf format to disk.
    Defaults to saving files in :path:`~/.fs_cache/YYYY-MM-DD.h5`

    Parameters
    ----------
    data : ndarray
        The data to be saved
    base_path : str, optional
        The base-path to use for saving files.  If not given
        default to `~/.fs_cache`.  Will add a sub-directory for
        each day in this path.
    """

    if base_path is None:
        base_path = op.join(op.expanduser('~'), '.fs_cache',
                            str(datetime.date.today()))
    #_make_sure_path_exists(base_path)
    fpath = op.join(base_path, str(uuid.uuid4()) + '.npy')
    with NpyWriter(fpath) as fout:
        eid = fout.add_data(data)

    return eid


def get_data(x, y):
    """
    Get data for given x, y index.

    Parameters
    ----------
    x : int
        vertical index
    y : int
        horizontal index

    Returns
    -------
    unicode:
        id number of event
    """
    uid = str(uuid.uuid1())
    file_path = '/Users/Li/Downloads/xrf_data/2xfm_0103.h5'
    custom = {'dset_path': 'mca_arr'}

    fb = fsc.save_file_base('hdf_maps', file_path, custom)
    evl = fsc.save_file_event_link(fb, uid, link_parameters={'x': x, 'y': y})
    return evl.event_id


def save_hdf_data():
    blc = insert_beamline_config()
    begin_run = insert_begin_run(time=0., scan_id=1, beamline_id='csx',
                                 uid=str(uuid.uuid4()),
                                 beamline_config=blc)

    # data keys entry
    data_keys = {'x_pos': dict(source='MCA:pos_x', dtype='number'),
                 'y_pos': dict(source='MCA:pos_y', dtype='number'),
                 'xrf_spectrum': dict(source='MCA:spectrum', dtype='array',
                                      shape=(5,), external='FILESTORE:')}

    # save the event descriptor
    e_desc = insert_event_descriptor(
        begin_run_event=begin_run, data_keys=data_keys, time=0.,
        uid=str(uuid.uuid4()))

    # number of positions to record
    num1 = 5

    events = []
    for i in range(num1):
        x_pos = i
        y_pos = 0

        spectrum = get_data(i, 0)

        # Put in actual ndarray data, as broker would do.
        data1 = {'xrf_spectrum': (spectrum, error(i)),
                 'x_pos': (x_pos, error(i)),
                 'y_pos': (y_pos, error(i))}

        event = insert_event(event_descriptor=e_desc, seq_num=i,
                             time=error(i), data=data1, uid=str(uuid.uuid4()))
        events.append(event)
    return events


def test_retrieve_data():
    evts = save_hdf_data()
    data = fsc.retrieve_data(evts[0]['data']['xrf_spectrum'][0])
    print(data)


def error(val, sigma=0.01):
    """Return a copy of the input plus noise

    Parameters
    ----------
    val : number or ndarrray
    sigma : width of Gaussian from which noise values are drawn

    Returns
    -------
    noisy_val : number or ndarray
        same shape as input val
    """
    if np.isscalar(val):
        return val + sigma * np.random.randn()
    else:
        return val + sigma * np.random.randn(val.shape)
