from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six

from contextlib import contextmanager

import boltons.cacheutils
from .handlers_base import HandlerRegistry, DuplicateHandler

from .odm_templates import Resource


class FileStoreRO(object):
    def __init__(self, config, handler_reg=None):
        self.config = config

        self.handler_reg = HandlerRegistry(
            handler_reg if handler_reg is not None else {})

        self._datum_cache = boltons.cacheutils.LRU(max_size=1000000)
        self._handler_cache = boltons.cacheutils.LRU()
        self._resource_cache = boltons.cacheutils.LRU(on_miss=self._r_on_miss)

    def _r_on_miss(self, k):
        col = self._resource_col
        return col.find_one({'_id': k})

    def get_data(self, eid):
        pass

    def register_handler(self, key, handler, overwrite=False):
        try:
            self.handler_reg.register_handler(key, handler)
        except DuplicateHandler:
            if overwrite:
                self.deregister_handler(key)
                self.register_handler(key, handler)
            else:
                raise

    def deregister_handler(self, key):
        handler = self.handler_reg.deregister_handler(key)
        if handler is not None:
            name = handler.__name__
            for k in list(self._handler_cache):
                if k[1] == name:
                    del self._handler_cache[k]

    @contextmanager
    def handler_context(self, temp_handlers):
        remove_list = []
        replace_list = []
        for k, v in six.iteritems(temp_handlers):
            if k not in self.handler_reg:
                remove_list.append(k)
            else:
                old_h = self.handler_reg.pop(k)
                replace_list.append((k, old_h))
            self.register_handler(k, v)

        yield self
        for k in remove_list:
            self.deregister_handler(k)
        for k, v in replace_list:
            self.deregister_handler(k)
            self.register_handler(k, v)

    @property
    def _resource_col(self):
        # TODO NUKE THIS!
        return Resource._get_collection()


class FileStore(FileStoreRO):
    def insert_resource(self, spec, resource_path, resource_kwargs):
        pass

    def insert_datum(self, resource, datum_id, datum_kwargs):
        pass

    def bulk_insert_datum(self, resource, datum_ids, datum_kwarg_list):
        pass
