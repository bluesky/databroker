from __future__ import print_function, division
import numpy as np
from skxray import core

# stolen from other live demo
class FrameSourcerBrownian(object):
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

    def __init__(self, im_shape, step_scale=1, decay=30,
                 I_fluc_function=None, step_fluc_function=None):
        self._im_shape = np.asarray(im_shape)
        self._scale = step_scale
        self._decay = decay

        if I_fluc_function is None:
            I_fluc_function = lambda x: 1

        self._I_func = I_fluc_function

        if step_fluc_function is None:
            step_fluc_function = lambda step, count: None

        self._scale_func = step_fluc_function

        if self._im_shape.ndim != 1 and len(self._im_shape) != 2:
            raise ValueError("image shape must be 2 dimensional "
                             "you passed in {}".format(im_shape))
        self.reset()


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

    def reset(self):
        self._count = 0
        self._cur_position = np.array(np.asarray(self._im_shape) / 2, dtype=np.float)