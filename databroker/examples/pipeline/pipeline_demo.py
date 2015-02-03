

from __future__ import print_function, division
from skxray.fitting.api import model_list as valid_models

from replay.pipeline.pipeline import (DataMuggler, PipelineComponent,
                                      MuggleWatcherLatest, DmImgSequence)

from atom.api import Atom, Float, Typed, observe

from frame_source import FrameSourcerBrownian

import numpy as np

from replay.model.scalar_model import ScalarCollection
from replay.model.cross_section_model import CrossSectionModel
from replay.model.fitting_model import MultiFitController
from enaml.qt.qt_application import QtApplication
import enaml


# used below
img_size = (150, 150)
period = 150
I_func_sin = lambda count: (1 + .5*np.sin(2 * count * np.pi / period))
center = 500
sigma = center / 4
I_func_gaus = lambda count: (1 + np.exp(-((count - center)/sigma) ** 2))


def scale_fluc(scale, count):
    if not count % 50:
        return scale - .5
    if not count % 25:
        return scale + .5
    return None

frame_source = FrameSourcerBrownian(img_size, delay=1, step_scale=.5,
                                    I_fluc_function=I_func_gaus,
                                    step_fluc_function=scale_fluc,
                                    max_count=center*2
                                    )

# set up mugglers
# (name, fill_type, #num dims)
dm = DataMuggler((('T', 'pad', 0),
                  ('img', 'bfill', 2),
                  ('count', 'bfill', 0),
                  ('max', 'bfill', 0),
                  ('x', 'bfill', 0),
                  ('y', 'bfill', 0),
                  )
                 )

# construct a watcher for the image + count on the main DataMuggler
mw = MuggleWatcherLatest(dm, 'img', ['count', 'T'])

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


app = QtApplication()

with enaml.imports():
    from pipeline import PipelineView

img_seq = DmImgSequence(data_muggler=dm, data_name='img')
cs_model = CrossSectionModel(data_muggler=dm, name='img',
                                        sliceable_data=img_seq)
roi_model = RegionOfInterestModel(callback=roi_callback)
from replay.model.fitting_model import FitController
multi_fit_controller = MultiFitController(valid_models=valid_models)
scalar_collection = ScalarCollection(data_muggler=dm,
                                     fit_controller=multi_fit_controller)
scalar_collection.scalar_models['count'].is_plotting = False
scalar_collection.scalar_models['T'].is_plotting = False
scalar_collection.scalar_models['x'].is_plotting = False
scalar_collection.scalar_models['y'].is_plotting = False
scalar_collection.fit_target = 'max'

view = PipelineView(scalar_collection=scalar_collection,
                    cs_model=cs_model,
                    roi_model=roi_model,
                    multi_fit_controller=multi_fit_controller)
view.show()
frame_source.start()

app.start()
