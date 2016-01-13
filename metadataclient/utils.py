from __future__ import (absolute_import, division, print_function)
import six
from itertools import zip_longest


def grouper(iterable, n, fillvalue=None):
        """Collect data into fixed-length chunks or blocks"""
        args = [iter(iterable)] * n
        return zip_longest(*args, fillvalue=fillvalue)


class NoRunStop(Exception):
    pass


class NoEventDescriptors(Exception):
    pass
