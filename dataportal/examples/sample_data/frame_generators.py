from __future__ import print_function, division
import numpy as np
from skxray import core


def brownian(im_shape, step_scale=1, decay=30,
             I_fluc_function=None, step_fluc_function=None):
    """
    Return a generator that yields simulated images
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
    # This code was ported from a class-based implementation,
    # so the style of what follows is a little tortured but functional.
    im_shape = np.asarray(im_shape)
    scale = step_scale

    if I_fluc_function is None:
        I_fluc_function = lambda x: np.random.randn()

    I_func = I_fluc_function

    if step_fluc_function is None:
        step_fluc_function = lambda step, count: None

    if im_shape.ndim != 1 and len(im_shape) != 2:
        raise ValueError("image shape must be 2 dimensional "
                         "you passed in {}".format(im_shape))

    cur_position = np.asarray(im_shape) // 2
    count = 0
    while True:
        # add a random step
        step = np.random.randn(2) * scale
        cur_position += step
        # clip it
        cur_position = np.array([np.clip(v, 0, mx) for
                                 v, mx in zip(cur_position, im_shape)])
        R = core.radial_grid(cur_position,
                                 im_shape)
        I = I_func(count)
        im = np.exp((-R**2 / decay)) * I
        count += 1
        yield im
