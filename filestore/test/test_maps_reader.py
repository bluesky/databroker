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

import h5py
import os.path as op
import numpy as np
from nose.tools import assert_true, assert_raises, assert_false, assert_equal
import datetime
import uuid

import logging
logger = logging.getLogger(__name__)


fsr.register_handler('hdf_maps', HDFM)


def save_syn_data(eid, data, base_path=None):
    """
    Save a array as hdf format to disk.
    Defaults to saving files in :path:`~/.fs_cache/YYYY-MM-DD.h5`

    Parameters
    ----------
    eid : unicode
        id for file name
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
    fpath = op.join(base_path, str(eid) + '.h5')

    with h5py.File(fpath, 'w') as f:
        # create a group for maps to hold the data
        mapsGrp = f.create_group('MAPS')
        # now set a comment
        mapsGrp.attrs['comments'] = 'MAPS group'

        entryname = 'mca_arr'
        comment = 'These are raw spectrum data.'
        ds_data = mapsGrp.create_dataset(entryname, data=data)
        ds_data.attrs['comments'] = comment
    return fpath


def get_data(ind_v, ind_h):
    """
    Get data for given x, y index.

    Parameters
    ----------
    ind_v : int
        vertical index
    ind_h : int
        horizontal index

    Returns
    -------
    unicode:
        id number of event
    """

    uid = str(uuid.uuid1())

    # generate 3D random number with a given shape
    syn_data = np.random.randn(20, 1, 10)
    file_path = save_syn_data(uid, syn_data)

    custom = {'dset_path': 'mca_arr'}

    fb = fsc.save_file_base('hdf_maps', file_path, custom)
    evl = fsc.save_file_event_link(fb, uid, link_parameters={'x': ind_v, 'y': ind_h})
    return evl.event_id


def test_data_io():
    """
    Save data to db and run test when data is retrieved.
    """
    blc = insert_beamline_config()
    begin_run = insert_begin_run(time=0., scan_id=1, beamline_id='csx',
                                 uid=str(uuid.uuid4()),
                                 beamline_config=blc)

    # data keys entry
    data_keys = {'x_pos': dict(source='MCA:pos_x', dtype='number'),
                 'y_pos': dict(source='MCA:pos_y', dtype='number'),
                 'xrf_spectrum': dict(source='MCA:spectrum', dtype='array',
                                      #shape=(5,),
                                      external='FILESTORE:')}

    # save the event descriptor
    e_desc = insert_event_descriptor(
        begin_run_event=begin_run, data_keys=data_keys, time=0.,
        uid=str(uuid.uuid4()))

    # number of positions to record
    num = 5

    for i in range(num):
        v_pos = 0
        h_pos = i

        spectrum = get_data(v_pos, h_pos)

        # Put in actual ndarray data, as broker would do.
        data1 = {'xrf_spectrum': (spectrum, error(i)),
                 'v_pos': (v_pos, error(i)),
                 'h_pos': (h_pos, error(i))}

        event = insert_event(event_descriptor=e_desc, seq_num=i,
                             time=error(i), data=data1, uid=str(uuid.uuid4()))

        # test on retrieve data for all data sets
        yield _test_retrieve_data, event


def _test_retrieve_data(evt):
    data = fsc.retrieve_data(evt['data']['xrf_spectrum'][0])
    assert_equal(data.size, 20)


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
