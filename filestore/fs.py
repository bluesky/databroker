from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six
from pkg_resources import resource_filename
from contextlib import contextmanager
import json
import logging

from pymongo import MongoClient
from bson import ObjectId

import boltons.cacheutils

from .handlers_base import HandlerRegistry, DuplicateHandler
from .core import (bulk_insert_datum as _bulk_insert_datum,
                   insert_datum as _insert_datum,
                   insert_resource as _insert_resource,
                   get_datum as _get_datum)

logger = logging.getLogger(__name__)


class FileStoreRO(object):

    KNOWN_SPEC = dict()
    # load the built-in schema
    for spec_name in ['AD_HDF5', 'AD_SPE']:
        tmp_dict = {}
        resource_name = 'json/{}_resource.json'.format(spec_name)
        datum_name = 'json/{}_datum.json'.format(spec_name)
        with open(resource_filename('filestore', resource_name), 'r') as fin:
            tmp_dict['resource'] = json.load(fin)
        with open(resource_filename('filestore', datum_name), 'r') as fin:
            tmp_dict['datum'] = json.load(fin)
        KNOWN_SPEC[spec_name] = tmp_dict

    def __init__(self, config, handler_reg=None):
        self.config = config

        self.handler_reg = HandlerRegistry(
            handler_reg if handler_reg is not None else {})

        self._datum_cache = boltons.cacheutils.LRU(max_size=1000000)
        self._handler_cache = boltons.cacheutils.LRU()
        self._resource_cache = boltons.cacheutils.LRU(on_miss=self._r_on_miss)
        self.__db = None
        self.__conn = None
        self.__datum_col = None
        self.__res_col = None
        self.known_spec = dict(self.KNOWN_SPEC)

    def disconnect(self):
        self.__db = None
        self.__conn = None
        self.__datum_col = None
        self.__res_col = None

    def reconfigure(self, config):
        self.disconnect()
        self.config = config
        print(config)

    def _r_on_miss(self, k):
        col = self._resource_col
        return col.find_one({'_id': k})

    def get_datum(self, eid):
        return _get_datum(self._datum_col, eid,
                          self._datum_cache, self.get_spec_handler,
                          logger)

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
    def _db(self):
        if self.__db is None:
            conn = self._connection
            self.__db = conn.get_database(self.config['database'])
        return self.__db

    @property
    def _resource_col(self):
        if self.__res_col is None:
            self.__res_col = self._db.get_collection('resource')

        return self.__res_col

    @property
    def _datum_col(self):
        if self.__datum_col is None:
            self.__datum_col = self._db.get_collection('datum')

        return self.__datum_col

    @property
    def _connection(self):
        if self.__conn is None:
            self.__conn = MongoClient(self.config['host'],
                                      self.config.get('port', None))
        return self.__conn

    def get_spec_handler(self, resource):
        """
        Given a document from the base FS collection return
        the proper Handler

        This should get memozied or shoved into a class eventually
        to minimize open/close thrashing.

        Parameters
        ----------
        resource : ObjectId
            ObjectId of a resource document

        Returns
        -------

        handler : callable
            An object that when called with the values in the event
            document returns the externally stored data

        """
        hr = self.handler_reg

        handle_registry = hr

        resource = self._resource_cache[resource]
        h_cache = self._handler_cache
        spec = resource['spec']
        handler = handle_registry[spec]
        key = (str(resource['_id']), handler.__name__)
        if key in h_cache:
            return h_cache[key]
        kwargs = resource['resource_kwargs']
        rpath = resource['resource_path']
        ret = handler(rpath, **kwargs)
        h_cache[key] = ret
        return ret


class FileStore(FileStoreRO):
    def insert_resource(self, spec, resource_path, resource_kwargs):
        col = self._resource_col

        return _insert_resource(col, spec, resource_path, resource_kwargs,
                                self.known_spec)

    def insert_datum(self, resource, datum_id, datum_kwargs):
        col = self._datum_col

        try:
            resource['spec']
        except (AttributeError, TypeError):
            res_col = self._resource_col
            resource = res_col.find_one({'_id': ObjectId(resource)})
            resource['id'] = resource['_id']
        if datum_kwargs is None:
            datum_kwargs = {}

        return _insert_datum(col, resource, datum_id, datum_kwargs,
                             self.known_spec)

    def bulk_insert_datum(self, resource, datum_ids, datum_kwarg_list):
        col = self._datum_col
        return _bulk_insert_datum(col, resource, datum_ids, datum_kwarg_list)
