from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import six

import logging

from .retrieve import HandlerBase
from .handlers import IntegrityError

logger = logging.getLogger(__name__)


class AreaDetectorTiffHandlerPath(HandlerBase):
    specs = {'AD_TIFF'} | HandlerBase.specs

    def __init__(self, fpath, template, filename, frame_per_point=1):
        self._path = fpath
        self._fpp = frame_per_point
        self._template = template.replace('_%6.6d', '_{frm:06d}')
        self._filename = self._template % (self._path, filename)

    def __call__(self, point_number):
        start, stop = point_number * self._fpp, (point_number + 1) * self._fpp
        if stop > len(self._image_sequence):
            raise IntegrityError("Seeking Frame {0} out of {1} frames.".format(
                stop, len(self._image_sequence)))
        slc = slice(start, stop)
        return list(self._filename.format(frm=frm)
                    for frm in range(*slc.indices(stop + 1)))
