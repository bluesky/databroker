from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import six  # noqa
import logging
from ..utils import format_time as _format_time

logger = logging.getLogger(__name__)


class HeaderSourceShim(object):
    '''Shim class to turn a mds object into a HeaderSource

    This will presumably be deleted if this API makes it's way back down
    into the implementations
    '''
    def __init__(self, mds):
        self.mds = mds

    def __call__(self, text_search=None, filters=None, **kwargs):
        if filters is None:
            filters = {}
        else:
            filters = dict(filters)
            _format_time(filters, self.mds.config['timezone'])
        if text_search is not None:
            query = {'$and': [{'$text': {'$search': text_search}}, filters]}
        else:
            # Include empty {} here so that '$and' gets at least one query.
            kwargs = dict(kwargs)
            _format_time(kwargs, self.mds.config['timezone'])
            query = {'$and': [kwargs, filters]}
        starts = self.mds.find_run_starts(**query)
        return ((s, safe_get_stop(self, s)) for s in starts)

    def __getitem__(self, k):
        from ..core import search

        return search(k, self)

    def insert(self, name, doc):
        return self.mds.insert(name, doc)

    def find_last(self, num):
        return self.mds.find_last(num)

    def find_run_starts(self, *args, **kwargs):
        return self.mds.find_run_starts(*args, **kwargs)

    def stop_by_start(self, s):
        return self.mds.stop_by_start(s)

    @property
    def NoRunStart(self):
        return self.mds.NoRunStart

    @property
    def NoRunStop(self):
        return self.mds.NoRunStop


def safe_get_stop(hs, s):
    try:
        return hs.stop_by_start(s)
    except hs.NoRunStop:
        return {}
