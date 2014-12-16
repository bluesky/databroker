from __future__ import print_function, division
import logging

import six

import enaml
from enaml.qt.qt_application import QtApplication


logger = logging.getLogger(__name__)

from skxray.fitting.api import model_list as valid_models
from replay.pipeline.pipeline import (DataMuggler)
from replay.model.scalar_model import ScalarCollection
from replay.model.fitting_model import MultiFitController
from metadataStore.api import analysis
from pprint import pprint
import datetime

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
        from replay.gui.pipeline_hitting_mds import PipelineView, MplConfigs

    c_c_combo_fitter = MultiFitController(valid_models=valid_models)
    scalar_collection = ScalarCollection()
    scalar_collection.data_muggler = data_muggler
    scalar_collection.multi_fit_controller = c_c_combo_fitter
    view = PipelineView()
    configs = MplConfigs()
    configs.config_model = scalar_collection._conf
    # provide the pipeline view with its attributes
    view.plot_options = configs
    view.grab_latest = grab_latest
    view.get_current_scanid = get_current_scanid
    view.scalar_collection=scalar_collection
    view.multi_fit_controller = c_c_combo_fitter
    return view

def get_data(scan_id):
    # grab the latest data
    ret = analysis.find2(scan_id=scan_id, data=True)
    header = list(six.itervalues(ret['headers']))[0]
    events = list(six.itervalues(ret['events']))
    ev_desc = list(six.itervalues(ret['event_descriptors']))
    beamline_configs = list(six.itervalues(ret['beamline_configs']))

    return header, events, ev_desc, beamline_configs


def get_current_scanid():
    header = analysis.find_last()[0]
    pprint(header)
    return header['scan_id']


def grab_latest(scan_id):
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
    header, events, ev_desc, beamline_configs = get_data(scan_id)
    current_hdr_id = header['_id']
    # print('line 76: view.make_new_dm: {}'.format(view.make_new_dm))
    # print('line 76: prev_hdr_id, current_hdr_id: {}, {}'.format(
    #     prev_hdr_id, current_hdr_id))
    if prev_hdr_id != current_hdr_id:
        if view.make_new_dm:
            # prev_hdr_id = current_hdr_id
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
        try:
            key = e['time']
        except KeyError:
            try:
                key = e['data']['time']
            except KeyError:
                key = e['seq_no']
                
        dm.append_data(key, e['data'])
    prev_max_seqno = cur_max_seq_no

    view.make_new_dm = False
    # stash the header id
    start_time = datetime.datetime.fromtimestamp(header['start_time'])
    scan_id = header['scan_id']
    view.scalar_collection._conf.title = 'Scan id {}. {}'.format(scan_id,
                                                                 start_time)


def main():
    app = QtApplication()
    # init the UI
    view = init_ui(dm)
    view.scalar_collection.data_muggler = None

    # add the proper callbacks to the pvs
    view.show()
    # init the header and event pvs
    view.btn_scanid.clicked()
    view.btn_watch_mds.checked = True
    # view.btn_watch_mds.clicked()
    # view.btn_watch_mds.toggled()
    app.start()

if __name__ == '__main__':
    main()