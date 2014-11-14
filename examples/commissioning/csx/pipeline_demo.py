
from __future__ import print_function, division
import six
import broker.config as cfg
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
import time
# this must appear before the cothread import
os.environ['EPICS_BASE'] = '/usr/lib/epics'

import cothread
import cothread.catools as ca

cothread.iqt()

# set up mugglers
# keys = [('s{}'.format(idx), 'ffill', 0) for idx in range(1, 8)]
# keys.append(('p0', 'ffill', 0))

pvs = {'XF:23IDA-VA:1{FS:1-CCG:1}P:Raw-I': 'vac1',
       'FE:C23A-VA{FVS:2}P-I': 'vac2'}

keys = [(pv_alias, 'ffill', 0) for pv, pv_alias in six.iteritems(pvs)]
keys.append(('count', 'ffill', 0))
dm = DataMuggler(keys)

data_dict = {'count': 0}
for pv_name, pv_alias in six.iteritems(pvs):
    data_dict[pv_alias] = ca.caget(pv_name)
cur_time = datetime.utcnow() - timedelta(seconds=1)
dm.append_data(cur_time, data_dict)

count = 1

def process_callback(value):
    global pvs
    global count
    name = pvs[value.name]
    time_stamp = datetime.utcnow()
    print('name: {}'.format(name))
    print('time_stamp: {}'.format(time_stamp))
    print('value: {}'.format(value))
    dm.append_data(time_stamp, {name: value, 'count': count})
    count = count + 1
    print("hello!")


# M = [ca.camonitor(pv, process_callback, format=ca.FORMAT_TIME) for pv, pv_alias in six.iteritems(pvs)]
for pv, pv_alias in six.iteritems(pvs):
    ca.camonitor(pv, process_callback, format=ca.FORMAT_TIME)
# data_source = SocketListener(cfg.SEND_HOST, cfg.SEND_PORT)
# data_source.event.connect(print_socket_value)

app = QtApplication()

with enaml.imports():
    from pipeline import PipelineView

multi_fit_controller = MultiFitController(valid_models=valid_models)
scalar_collection = ScalarCollection(data_muggler=dm,
                                     multi_fit_controller=multi_fit_controller)

view = PipelineView(scalar_collection=scalar_collection,
                    multi_fit_controller=multi_fit_controller)
view.show()
# data_source.start()

app.start()
cothread.WaitForQuit()