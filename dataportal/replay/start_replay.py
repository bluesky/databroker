from __future__ import print_function, division
import logging

import six

import enaml
from enaml.qt.qt_application import QtApplication


logger = logging.getLogger(__name__)

from skxray.fitting.api import model_list as valid_models
from dataportal.muggler.data import DataMuggler
from dataportal.replay.model.scalar_model import ScalarCollection
from dataportal.replay.model.fitting_model import MultiFitController
from metadataStore.api import analysis
from pprint import pprint
from collections import deque
import datetime

# some globals
dm = None
view = None

sleep_time = 0.25

prev_hdr_id = None
# nested dictionary to spare data muggler a bunch of event parsing
known_events = {}

# RUNTIME
def get_data(header):
    """Grab events from the metadatastore

    Parameters
    ----------
    header : mongoengine header object

    Returns
    -------
    events : list
        The list of all events associated with this header
    """
    return analysis.find_event(header)


def get_current_scanid():
    """Get the latest scan_id

    Returns
    -------
    scan_id : int
        scan_id of the header
    """
    return analysis.find_last().scan_id


def grab_latest(scan_id):
    # implementing against this spec: https://github.com/NSLS-II/docs/blob/master/source/arch/metadatastore-format.rst
    global prev_hdr_id
    global dm
    global known_events

    header = analysis.find_header(scan_id=scan_id)[0]
    current_hdr_id = header.id
    events = get_data(header)

    # check to see if the begin_run_event has changed underneath us
    if prev_hdr_id != current_hdr_id:
        # check if the GUI is asking for a new data muggler
        if view.make_new_dm:
            known_events.clear()
            dm = DataMuggler(events)
            view.scalar_collection.data_muggler = dm
            prev_hdr_id = current_hdr_id
            try:
                # set the x axis for the scalar view
                view.scalar_collection.x = header.custom.plotx
            except AttributeError:
                # plotx is not in the header
                pass
            try:
                ploty = header.custom.plotx
            except AttributeError:
                # ploty is not in the header
                ploty = []
            # set the data sets to plot on the y axis
            for y in view.scalar_collection.col_names:
                is_plotting = False
                if y in ploty:
                    is_plotting = True
                view.scalar_collection.scalar_models[y].is_plotting = is_plotting
        else:
            # turn off the automatic data updating
            view.currently_watching = False
            return

    for e in events:
        # make sure that the event_descriptor is represented in known_events
        try:
            known_events[e.ev_desc]
        except KeyError:
            known_events[e.ev_desc] = deque()
        # check to see if the event 'e' already exists in known_events
        if e.time not in known_events[e.ev_desc]:
            # data muggler doesn't know about this event yet
            known_events[e.ev_desc].append(e.time)
            dm.append_event(e)

    # turn off the UI toggle to make a new data muggler
    view.make_new_dm = False

    # grab some bits to format the title
    start_time = datetime.datetime.fromtimestamp(header.time)
    scan_id = header.scan_id

    # set the title of the plot in replay to be something reasonably sensible
    view.scalar_collection._conf.title = 'Scan id {}. {}'.format(scan_id,
                                                                 start_time)

# UI SETUP

def init_ui():
    """ Do the enaml import and set up of the UI

    Parameters
    ----------
    data_muggler : dataportal.muggler.data.DataMuggler
    """
    global view
    with enaml.imports():
        from dataportal.replay.gui.pipeline_hitting_mds import (PipelineView,
                                                                MplConfigs)

    c_c_combo_fitter = MultiFitController(valid_models=valid_models)
    scalar_collection = ScalarCollection()
    scalar_collection.data_muggler = dm
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


def main():
    # boilerplate
    app = QtApplication()
    # init the UI
    view = init_ui()
    view.scalar_collection.data_muggler = None
    view.show()

    # init the header and event pvs
    view.btn_scanid.clicked()
    view.btn_watch_mds.checked = True
    # boilerplace
    app.start()

if __name__ == '__main__':
    main()
