
from __future__ import print_function, division
import six

from skxray.fitting.api import model_list as valid_models
from replay.pipeline.pipeline import (DataMuggler, PipelineComponent,
                                      MuggleWatcherLatest, DmImgSequence)
from replay.model.scalar_model import ScalarCollection
from replay.model.cross_section_model import CrossSectionModel
from replay.model.fitting_model import MultiFitController
from enaml.qt.qt_application import QtApplication
import enaml
from replay.pipeline.pipeline import SocketListener
from datetime import datetime, timedelta
import os
from calendar import timegm
import time
# this must appear before the cothread import
os.environ['EPICS_BASE'] = '/usr/lib/epics'
os.environ['EPICS_CA_MAX_ARRAY_BYTES'] = '10000000000'

import cothread
import cothread.catools as ca

cothread.iqt()

# set up mugglers
# keys = [('s{}'.format(idx), 'ffill', 0) for idx in range(1, 8)]
# keys.append(('p0', 'ffill', 0))

PV_TRIGGER = 'XF:23ID-CT{Replay}Val:trigger-I'
PVS = ['XF:23ID-CT{{Replay}}Val:{}-I'.format(idx) for idx in range(0, 0)]
IM_pvs = ['XF:23ID-CT{{Replay}}Val:{}-I'.format(9), ]
dm_keys = [(pv, 'ffill', 0) for pv in PVS]
dm_keys.extend((pv, 'bfill', 2) for pv in IM_pvs)
dm_keys.append(('count', 'ffill', 0))

dm = DataMuggler(dm_keys)

count = 1

dm_map = {}

def process(pv_value):
    global count
    if pv_value.name == PV_TRIGGER:
        if pv_value == 'start':
            # clear the data muggler
            clear_datamuggler()
            # start the PV observation
            start_observation()
        elif pv_value == 'stop':
            stop_observation()
    name = dm_map[pv_value.name]

    time_stamp = datetime(*time.gmtime(pv_value.timestamp)[:6])
    if name in IM_pvs:
        pv_name = pv_value.name
        if 'ArrayData' in pv_name:
            base = pv_name[:-9]
            x_dim = int(ca.caget(base+'ArraySize0_RBV'))
            y_dim = int(ca.caget(base+'ArraySize1_RBV'))
            value = np.asarray(pv_value).reshape((x_dim, y_dim))
        else:
            print('inconceivable')
    elif name in PVS:
        value = float(pv_value)
        print('value: {}'.format(pv_value))
    else:
        print('inconceivable')

    print('name: {}'.format(name))
    print('time_stamp: {}'.format(time_stamp))

    dm.append_data(time_stamp, {name: value, 'count': count})
    count = count + 1
    print("hello!")

subscription_obj = []

def start_observation():
    print('start_observation')
    pvs_to_watch = []
    init_data = {}
    for pv in PVS:
        pv_val = ca.caget(pv)
        pv_name = ''.join(chr(_) for _ in pv_val).strip()
        if len(pv_name) > 0:
            print('pv_name: {}'.format(pv_name))
            dm_map[pv_name] = pv
            pvs_to_watch.append(pv_name)
            init_data[pv] = ca.caget(pv_name)
    dm.append_data(datetime.utcnow(), init_data)
    for pv_name in pvs_to_watch:
        subscription_obj.append(ca.camonitor(str(pv_name), process, format=ca.FORMAT_TIME))

def stop_observation():
    print('stop_observation')
    for obj in subscription_obj:
        obj.close()
    del subscription_obj[:]

def clear_datamuggler():
    print("clear_datamuggler")
    dm.clear()
    dm_map.clear()

app = QtApplication()

with enaml.imports():
    from pipeline_w2d import PipelineView

multi_fit_controller = MultiFitController(valid_models=valid_models)
scalar_collection = ScalarCollection(data_muggler=dm,
                                     multi_fit_controller=multi_fit_controller)
cs_model = CrossSectionModel(data_muggler=dm)
view = PipelineView(scalar_collection=scalar_collection,
                    multi_fit_controller=multi_fit_controller)
view.cs_model = cs_model
view.start_observation = start_observation
view.stop_observation = stop_observation
view.clear_data = clear_datamuggler
view.show()

#app.start()
cothread.WaitForQuit()
