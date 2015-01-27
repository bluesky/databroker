from __future__ import print_function, division
import os
import logging

import six
import yaml
logger = logging.getLogger(__name__)
import sys
import epics
gajillion = 10000000000
os.environ['EPICS_CA_MAX_ARRAY_BYTES'] = str(gajillion)
epics.ca.AUTOMONITOR_MAXLENGTH = gajillion

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

from epics import PV

from metadataStore.api.collection import create_event
from pprint import pprint

pv_dict = {}
dm = None
view = None
header_pv = None
event_pv = None

header_PV_name = "XF:23ID-CT{Replay}Val:RunHdr-I"
event_PV_name = "XF:23ID-CT{Replay}Val:EventHdr-I"


def create_dm(raw_json_str):
    global dm
    header = None
    event_descriptors = None
    # turn it into a dictionary
    header = json.loads(raw_json_str)
    # grab the header and event descriptor data from the PV
    try:
        header = header['header']
    except KeyError as ke:
        print('...but there is no run header present')
    try:
        event_descriptors = header['event_descriptors']
    except KeyError as ke:
        print('No event descriptor present')

    data_keys = []
    # get the data keys from the event descriptor
    try:
        iter(event_descriptors)
    except TypeError as te:
        # event_descriptors is not iterable
        event_descriptors = [event_descriptors,]
    for ev_desc in event_descriptors:
        data_keys.append(list(six.iterkeys(ev_desc['data_keys'])))
    # create a new data muggler
    data_keys = [(key, 'ffill', 0) for key in data_keys]
    dm = DataMuggler(data_keys)
    # put the new data muggler into the scalar collection
    view.scalar_collection.dm = dm


def header_callback(value, **kw):
    """
    Header and event descriptor should already be created by the data
    broker before they are published to the header PV, as this callback will
    not create PVs.

    :param value:
    :param kw:
    :return:
    """

    # grab the data from the PV
    raw_data = kw['char_value']
    create_dm(raw_data)


def event_callback(value, **kw):
    # grab the raw json string
    raw_json_str = kw['char_value']
    # format it into a python dictionary
    event = json.loads(raw_json_str)
    # grab the data from the event
    data = event['data']
    # append it to the data muggler
    dm.append(data)
    print("I got an event: {}".format(event))


def init_ui(data_muggler):
    """ Do the enaml import and set up of the UI

    Parameters
    ----------
    data_muggler : replay.pipeline.DataMuggler
    """
    with enaml.imports():
        from pipeline_watching_PVs import PipelineView

    c_c_combo_fitter = MultiFitController(valid_models=valid_models)
    scalar_collection = ScalarCollection()
    scalar_collection.data_muggler = data_muggler
    scalar_collection.multi_fit_controller = c_c_combo_fitter
    view = PipelineView()
    # provide the pipeline view with its attributes
    view.scalar_collection=scalar_collection
    view.multi_fit_controller = c_c_combo_fitter
    return view

def read_pv_config(yaml_file=None):
    if yaml_file is None:
        yaml_file = os.path.join((os.path.dirname(os.path.realpath(__file__))),
                                 'pvs.yaml')
    # read yaml modules
    with open(yaml_file, 'r') as modules:
        import_dict = yaml.load(modules)
        print('import_dict: {0}'.format(import_dict))
        return import_dict


def init():
    pv_dict = read_pv_config()
    global dm, view, header_pv, event_pv

    # init the UI
    view = init_ui(dm)
    view.scalar_collection.data_muggler = None
    # init the header and event pvs
    header_pv = PV(header_PV_name, auto_monitor=True)
    event_pv = PV(event_PV_name, auto_monitor=True)

    # add the proper callbacks to the pvs
    header_pv.add_callback(header_callback)
    event_pv.add_callback(event_callback)
    print('header_pv.get(): {}'.format(header_pv.get()))
    print('dir(header_pv): {}'.format(dir(header_pv)))
    create_dm(header_pv.get())

    view.show()


if __name__ == '__main__':
    app = QtApplication()
    init()
    app.start()