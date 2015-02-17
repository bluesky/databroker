from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six

from filestore.retrieve import HandlerBase
import numpy as np


class SynHandlerMod(HandlerBase):
    """
    A handler for synthetic data which will return a ramp % n reshaped
    to the frame size for frame n

    Parameters
    ----------
    shape : tuple
        The shape of the frame
    """
    def __init__(self, fpath, shape):
        self._shape = tuple(int(v) for v in shape)
        self._N = np.prod(self._shape)

    def __call__(self, n):
        return np.mod(np.arange(self._N), n).reshape(self._shape)


class SynHandlerEcho(HandlerBase):
    """
    A handler for synthetic data which will return a constant field
    of value `n` of the given frame siz.

    Parameters
    ----------
    shape : tuple
        The shape of the frame
    """
    def __init__(self, fpath, shape):
        self._shape = tuple(int(v) for v in shape)
        self._N = np.prod(self._shape)

    def __call__(self, n):
        return np.ones(self._shape) * n
