from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import six

import logging

from .handlers_base import HandlerBase

logger = logging.getLogger(__name__)


class AreaDetectorTiffPathOnlyHandler(HandlerBase):
    """Replacement for AreaDetectorTiffHandler that returns the path not data
    """

    def __init__(self, fpath, template, filename, frame_per_point=1):
        self._path = fpath
        self._fpp = frame_per_point
        self._template = template
        self._filename = filename

    def __call__(self, point_number):
        start, stop = point_number * self._fpp, (point_number + 1) * self._fpp
        return [self._template % (self._path, self._filename, n)
                for n in range(start, stop)]


class RawHandler(HandlerBase):
    "Return (filepath, resource_dict, datum_dict) for debugging."
    def __init__(self, fpath, **kwargs):
        self.fpath = fpath
        self.resource_kwargs = kwargs

    def __call__(self, *args, **kwargs):
        if len(args) > 0:
            raise ValueError("This handler does not accept positional args.")
        return self.fpath, self.resource_kwargs, kwargs
