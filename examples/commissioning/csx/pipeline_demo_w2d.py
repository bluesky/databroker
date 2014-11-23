from __future__ import print_function, division
import six

import os
import yaml
import logging
logger = logging.getLogger(__name__)

import epics
gajillion = 10000000000
os.environ['EPICS_CA_MAX_ARRAY_BYTES'] = str(gajillion)
epics.ca.AUTOMONITOR_MAXLENGTH = gajillion

from skxray.fitting.api import model_list as valid_models
from replay.pipeline.pipeline import (DataMuggler, PipelineComponent,
                                      MuggleWatcherLatest, DmImgSequence)
from replay.model.scalar_model import ScalarCollection
from replay.model.cross_section_model import CrossSectionModel
from replay.model.fitting_model import MultiFitController
from replay.model.histogram_model import HistogramModel
from enaml.qt.qt_application import QtApplication
import enaml
from replay.pipeline.pipeline import SocketListener
from datetime import datetime
from calendar import timegm
import time
import numpy as np

pv_dict = {}
dm = None
scalar_pvs = []
line_pvs = []
im_pvs = []

def outer(dm, pv_dict):
    def process(**kwargs):
        """ Process an update from the camonitor

        Parameters
        ----------
        kwargs: magical dictionary of awesome from epics
            Valid keys:
            'access', 'cb_info', 'char_value', 'chid', 'count', 'enum_strs',
            'ftype', 'host', 'lower_alarm_limit', 'lower_ctrl_limit',
            'lower_disp_limit', 'lower_warning_limit', 'nelm', 'precision',
            'pvname', 'read_access', 'severity', 'status', 'timestamp', 'type',
            'typefull', 'units', 'upper_alarm_limit', 'upper_ctrl_limit',
            'upper_disp_limit', 'upper_warning_limit', 'value', 'write_access'
        """
        # print(sorted(list(six.iterkeys(kwargs))))
        # for k, v in six.iteritems(kwargs):
        #     print('{}: {}'.format(k, v))
        global count
        from datetime import datetime
        import time
        # convert the timestamp to a datetime object
        ts = datetime(*time.gmtime(kwargs['timestamp'])[:6])
        pv_name = kwargs['pvname']
        value = kwargs['value']
        try:
            if len(value) > 1:
                shape = pv_dict[pv_name]['shape']
                value = kwargs['value'].reshape(shape)
        except TypeError:
            pass
        dm.append_data(ts, {pv_name: value})
    return process

def create_pvs(scalar_pvs, line_pvs, image_pvs):
    """

    Parameters
    ----------
    pv_names : list
        list of pv_names from which to get the pv's to monitor

    Returns
    -------
    pv_dict : dict
        dict of pv_names formatted as
        { 'pv_to_watch' : {'dim': col_dim,
                           'pv': epics_pv,
                           'shape': (xdim, ydim),
                          }
        }
        col_dim: 0 is scalar, 1 is line, 2 is image, higher is nonsense,
        because live 3d data? come on...
    """
    for pv_name in scalar_pvs + line_pvs + image_pvs:
        try:
            print('pv_name: {}'.format(pv_name))
            pv_to_watch = epics.PV(pv_name).char_value
            pv = epics.PV(pv_to_watch, auto_monitor=True)
            this_pv = {'pv': pv}
            if pv_name in scalar_pvs:
                this_pv['dim'] = 0
            elif pv_name in line_pvs:
                this_pv['dim'] = 1
            elif pv_name in image_pvs:
                this_pv['dim'] = 2
                pv_base = pv_to_watch.rsplit(':', 1)[0] + ':'
                x_dim = int(epics.PV(pv_base + 'ArraySize0_RBV').get())
                y_dim = int(epics.PV(pv_base + 'ArraySize1_RBV').get())
                this_pv['shape'] = (y_dim, x_dim)
            else:
                raise ValueError('pv: {} is not in scalar, line or image pvs'
                                 ''.format(pv_name))
            pv_dict[pv_to_watch] = this_pv
        except AttributeError:
            # thrown if pv_name doesn't have a pv associated with it
            pass

    return pv_dict


def grab_data(pv_dict):
    """ Get the data associated with each PV when Replay spools up

    Parameters
    ----------
    pv_dict : dict
        The pv_dict formatted in `create_pvs()`

    Returns
    -------
    data_dict : dict
        {pv_name : data}
    """
    data_dict = {}
    for pv_name, pv_nest in six.iteritems(pv_dict):
        pv_val = pv_nest['pv'].get()
        if pv_nest['dim'] == 2:
            pv_val = pv_val.reshape(pv_nest['shape'])
        data_dict[pv_name] = pv_val

    return data_dict


def start_observation():
    print('Start observation')
    print('pv_dict: {}'.format(pv_dict))
    process = outer(dm, pv_dict)
    for pv_name, pv_nest in six.iteritems(pv_dict):
        pv_nest['pv'].add_callback(process)


def stop_observation():
    print('Stop observation')
    for pv_name, pv_nest in six.iteritems(pv_dict):
        pv_nest['pv'].clear_callbacks()


def clear_datamuggler():
    dm.clear()
    print('Data muggler cleared. Current data muggler content: {}'.format(dm))


def init_datamuggler():
    """ Initialize the data_muggler

    Parameters
    ----------
    scalar_pvs : list
        List of PVs that should be treated as scalars
    line_pvs : list
        List of PVs that should be treated as vectors
    im_pvs : list
        List of PVs that should be treated as images
    """
    global dm
    global pv_dict
    pv_dict = create_pvs(scalar_pvs, line_pvs, im_pvs)
    # create the data muggler
    dm_keys = []
    for pv_name, pv_nest in six.iteritems(pv_dict):
        dim = pv_nest['dim']
        fill = 'ffill'
        if dim > 1:
            fill = 'bfill'
        dm_keys.append((pv_name, fill, dim))
    dm = DataMuggler(dm_keys)
    # init the data
    data_dict = grab_data(pv_dict)
    # shove the first round of data into the data muggler
    dm.append_data(datetime.utcnow(), data_dict)


def init_ui(data_muggler):
    """ Do the enaml import and set up of the UI

    Parameters
    ----------
    data_muggler : replay.pipeline.DataMuggler
    """
    with enaml.imports():
        from pipeline_w2d import PipelineView

    histogram_model = HistogramModel()
    c_c_combo_fitter = MultiFitController(valid_models=valid_models)
    scalar_collection = ScalarCollection()
    scalar_collection.data_muggler = data_muggler
    scalar_collection.multi_fit_controller = c_c_combo_fitter
    cs_model = CrossSectionModel(data_muggler=data_muggler,
                                 histogram_model=histogram_model)
    view = PipelineView(histogram_model=histogram_model)
    histogram_model.cmap = cs_model.cmap
    # provide the pipeline view with its attributes
    view.scalar_collection=scalar_collection
    view.multi_fit_controller = c_c_combo_fitter
    view.cs_model = cs_model
    view.start_observation = start_observation
    view.stop_observation = stop_observation
    view.clear_data = clear_datamuggler
    view.reinit_data = init_datamuggler
    cmap = cs_model.cmap
    cs_model.cmap = 'gray'
    cs_model.cmap = cmap
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
    global dm, scalar_pvs, line_pvs, im_pvs
    pv_trigger = 'XF:23ID-CT{Replay}Val:trigger-I'
    scalar_pvs = ['XF:23ID-CT{{Replay}}Val:{}-I'.format(idx)
                  for idx in range(0, 6)]
    line_pvs = []
    im_pvs = ['XF:23ID-CT{{Replay}}Val:{}-I'.format(idx)
              for idx in range(6, 10)]
    common_pvs = ['XF:23ID1-OP{Slt:3-Ax:X}Mtr.RBV',
                  'XF:23ID1-ES{Dif-Cam:Beam}Stats5:Total_RBV',
                  'XF:23ID1-OP{Slt:3-Ax:X}Mtr.RBV',
                  'XF:23ID1-ES{Dif-Cam:Beam}image1:ArrayData']

    init_datamuggler()
    print('data_muggler.keys(dim=0): {}'.format(dm.keys(dim=0)))
    print('data_muggler.keys(dim=2): {}'.format(dm.keys(dim=2)))
    # init the UI
    view = init_ui(dm)
    view.scalar_collection.data_muggler = None
    # view.scalar_collection.data_muggler = dm
    view.show()


if __name__ == '__main__':
    app = QtApplication()
    init()
    app.start()