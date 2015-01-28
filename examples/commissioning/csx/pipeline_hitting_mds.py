from __future__ import print_function, division
import os
import logging

import six
import yaml
logger = logging.getLogger(__name__)
import sys

from skxray.fitting.api import model_list as valid_models
from replay.pipeline.pipeline import (DataMuggler)
from replay.model.scalar_model import ScalarCollection
from replay.model.cross_section_model import CrossSectionModel
from replay.model.fitting_model import MultiFitController
from replay.model.histogram_model import HistogramModel
from enaml.qt.qt_application import QtApplication
import enaml
from datetime import datetime
import time
import json
from metadataStore.api import analysis

from metadataStore.api.collection import create_event
from pprint import pprint

dm = None
view = None

sleep_time = 0.25

prev_hdr_id = None
prev_max_seqno = -1

def init_ui(data_muggler):
    """ Do the enaml import and set up of the UI

    Parameters
    ----------
    data_muggler : replay.pipeline.DataMuggler
    """
    global view
    with enaml.imports():
        from pipeline_hitting_mds import PipelineView

    c_c_combo_fitter = MultiFitController(valid_models=valid_models)
    scalar_collection = ScalarCollection()
    scalar_collection.data_muggler = data_muggler
    scalar_collection.multi_fit_controller = c_c_combo_fitter
    view = PipelineView()
    # provide the pipeline view with its attributes
    view.grab_latest = grab_latest
    view.scalar_collection=scalar_collection
    view.multi_fit_controller = c_c_combo_fitter
    return view


def grab_latest():
    # global dm
    # # grab the most recent run header
    # original_hdr_id = ''
    # while not original_hdr_id:
    #     # this is going to raise an exception if mds is empty, just not sure
    #     # what it will raise
    #     header, ev_desc, events, beamline_configs = analysis.find_latest()
    #     original_hdr_id = header['_id']
    #     time.sleep(sleep_time)
    #
    # # this is the song that never ends
    # while True:
    global prev_hdr_id
    global dm
    global prev_max_seqno
    # grab the latest data
    header, ev_desc, events, beamline_configs = analysis.find_last()
    current_hdr_id = header['_id']
    # print('line 76: view.make_new_dm: {}'.format(view.make_new_dm))
    # print('line 76: prev_hdr_id, current_hdr_id: {}, {}'.format(
    #     prev_hdr_id, current_hdr_id))
    if prev_hdr_id != current_hdr_id:
        if view.make_new_dm:
            prev_hdr_id = current_hdr_id
            # create a new data muggler
            keys = []
            for e in ev_desc:
                keys.extend(e['data_keys'])
            keys = ev_desc[0]['data_keys']
            col_info = [(key, 'ffill', 0) for key in keys]
            dm = DataMuggler(col_info)
            view.scalar_collection.data_muggler = dm
            prev_hdr_id = current_hdr_id
            prev_max_seqno = -1
        else:
            view.currently_watching = False
            return

    # make sure that I only keep searching while the header id remains
    # the same
    # if not original_hdr_id == current_hdr_id:
    #     break
    # dm.clear()
    cur_max_seq_no = prev_max_seqno
    for e in events:
        # print('event\n-----')
        # pprint(e)
        seq_no = e['seq_no']
        if seq_no < prev_max_seqno:
            continue
        elif seq_no > cur_max_seq_no:
            cur_max_seq_no = seq_no
        dm.append_data(e['data']['time'], e['data'])
    prev_max_seqno = cur_max_seq_no

    view.make_new_dm = False
    # stash the header id


def init():
    # init the UI
    view = init_ui(dm)
    view.scalar_collection.data_muggler = None
    # init the header and event pvs

    # add the proper callbacks to the pvs
    view.show()


if __name__ == '__main__':
    app = QtApplication()
    init()
    app.start()