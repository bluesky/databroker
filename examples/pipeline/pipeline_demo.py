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

"""
# set up pipe line components
# multiply the image by 5 because we can
p1 = PipelineComponent(lambda msg, data: (msg,
                                          {'img': data['img'] * 5,
                                           }))


def rough_center(img, axis):
    ret = np.mean(np.argmax(img, axis=axis))
    return ret

# find the max and estimate (badly) the center of the blob
p2 = PipelineComponent(lambda msg, data: (msg,
                                          {'max':
                                             np.max(data['img']),
                                          'x': rough_center(data['img'],
                                                                 axis=0),
                                          'y': rough_center(data['img'],
                                                                 axis=1),

                                          }))


def roi_callback(value):
    print('roi_callback. value = {}'.format(value))


class RegionOfInterestModel(Atom):
    value = Float(1)
    callback = Typed(object)

    @observe('value')
    def value_updated(self, changed):
        self.callback(self.value)


# hook up everything
# input
frame_source.event.connect(dm.append_data)
# first DataMuggler in to top of pipeline
mw.sig.connect(p1.sink_slot)
# p1 output -> p2 input
p1.source_signal.connect(p2.sink_slot)
# p2 output -> dm2
p2.source_signal.connect(dm.append_data)
"""

# set up mugglers
keys = [('s {}'.format(idx), 'ffill', 0) for idx in range(1, 8)]
dm = DataMuggler(keys)

data_source = SocketListener(cfg.HOST, cfg.PORT)
data_source.event.connect(dm.append_data)

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
