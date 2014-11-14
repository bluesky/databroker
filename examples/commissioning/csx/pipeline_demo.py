from __future__ import print_function, division
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

# set up mugglers
keys = [('s{}'.format(idx), 'ffill', 0) for idx in range(1, 8)]
keys.append(('p0', 'ffill', 0))
dm = DataMuggler(keys)

def print_socket_value(time_stamp, data):
    print('{}: {}'.format(time_stamp, data))
    dm.append_data(time_stamp, data)

data_source = SocketListener(cfg.SEND_HOST, cfg.SEND_PORT)
data_source.event.connect(print_socket_value)

app = QtApplication()

with enaml.imports():
    from pipeline import PipelineView

multi_fit_controller = MultiFitController(valid_models=valid_models)
scalar_collection = ScalarCollection(data_muggler=dm,
                                     multi_fit_controller=multi_fit_controller)

view = PipelineView(scalar_collection=scalar_collection,
                    multi_fit_controller=multi_fit_controller)
view.show()
data_source.start()

app.start()
