from __future__ import print_function, division
from enaml.qt import QtCore
import numpy as np
from skxray import core
from datetime import datetime


# stolen from other live demo
class FrameSourcerBrownian(QtCore.QObject):
    """
    A QObject that has a timer and will emit synthetic data
    of a dot moving around under brownian motion with varying intensity

    Parameters
    ----------
    im_shape : tuple
        The shape of the image.  The synthetic images gets emitted with the
        label 'img'

    step_scale : float, optional
        The size of the random steps.  This value get emitted with the label
        'T'

    decay : float, optional
        The size of the spot

    delay : int, optional
        The timer delay in ms

    parent : QObject, optional
        Qt parent

    max_count : int, optional
        After this many images stop self.  Default to MAXINT64

    I_fluc_function : callable
        Determine the maximum intensity of the spot as a function of count

        Signature of ::

            def func(count):
                return I(count)

    step_fluc_function : callable
         Determine if step should change and new step value.  Either return
         the new step value or None.  If the new step is None, then don't emit
         a 'T' event, other wise change the temperature and emit the event

         Signature of ::

             def func(step, count):
                 if not change_step(count):
                     return new_step(step, count)
                 else:
                     return None
    """
    event = QtCore.Signal(object, dict)

    def __init__(self, im_shape, step_scale=1, decay=30,
                 delay=500, parent=None, max_count=None,
                 I_fluc_function=None, step_fluc_function=None):
        QtCore.QObject.__init__(self, parent)
        self._im_shape = np.asarray(im_shape)
        self._scale = step_scale
        self._decay = decay
        self._delay = delay
        if max_count is None:
            max_count = np.iinfo(np.int64).max
        self._max_count = max_count

        if I_fluc_function is None:
            I_fluc_function = lambda x: 1

        self._I_func = I_fluc_function

        if step_fluc_function is None:
            step_fluc_function = lambda step, count: None

        self._scale_func = step_fluc_function

        if self._im_shape.ndim != 1 and len(self._im_shape) != 2:
            raise ValueError("image shape must be 2 dimensional "
                             "you passed in {}".format(im_shape))
        self._cur_position = np.array(np.asarray(im_shape) / 2, dtype=np.float)

        self.timer = QtCore.QTimer(parent=self)
        self.timer.timeout.connect(self.get_next_frame)
        self._count = 0

    @QtCore.Slot()
    def get_next_frame(self):
        self._count += 1

        new_scale = self._scale_func(self._scale, self._count)
        if new_scale is not None:
            self._scale = new_scale
            self.event.emit(datetime.now(), {'T': self._scale})

        im = self.gen_next_frame()
        self.event.emit(datetime.now(), {'img': im, 'count': self._count})

        if self._count > self._max_count:
            self.stop()
        print('fired {}, scale: {}, cur_pos: {}'.format(self._count,
                                                        self._scale,
                                                        self._cur_position))

        return True

    def gen_next_frame(self):
        # add a random step
        step = np.random.randn(2) * self._scale
        self._cur_position += step
        # clip it
        self._cur_position = np.array([np.clip(v, 0, mx) for
                                       v, mx in zip(self._cur_position,
                                                    self._im_shape)])
        R = core.pixel_to_radius(self._im_shape,
                                 self._cur_position).reshape(self._im_shape)
        I = self._I_func(self._count)
        im = np.exp((-R**2 / self._decay)) * I
        return im

    @QtCore.Slot()
    def start(self):
        self._count = 0
        # make sure we have a starting temperature event
        self.event.emit(datetime.now(), {'T': self._scale})
        self.timer.start(self._delay)

    @QtCore.Slot()
    def stop(self):
        self.timer.stop()
