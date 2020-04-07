from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import six   # noqa
import logging
import pymongo

from pymongo import MongoClient
from databroker.v1 import _get_mongo_database

from . import mongo_core

from .base_registry import (BaseRegistryRO,
                            RegistryTemplate,
                            RegistryMovingTemplate)

logger = logging.getLogger(__name__)


class RegistryRO(BaseRegistryRO):
    '''Base Registry object that knows how to read the database.

    Parameters
    ----------
    config : dict
       Much have keys {'database', 'host'} and may have a 'port'

    handler_reg : dict, optional
       Mapping between spec names and handler classes

    root_map : dict, optional
        str -> str mapping to account for temporarily moved/copied/remounted
        files.  Any resources which have a ``root`` in ``root_map``
        will have the resource path updated before being handed to the
        Handler in ``get_spec_handler``

    '''
    _API_MAP = {1: mongo_core}
    REQ_CONFIG = ('database',)
    OPT_CONFIG = ('port', 'uri')

    def __init__(self, config, handler_reg=None, root_map=None):
        super(RegistryRO, self).__init__(config,
                                         handler_reg=handler_reg,
                                         root_map=root_map)
        self.__db = None
        self.__datum_col = None
        self.__res_col = None
        self.__res_update_col = None
        self._resource_index = False
        self._datum_index = False

    def disconnect(self):
        self.__db = None
        self.__datum_col = None
        self.__res_col = None

    def _create_resource_index(self):
        if not self._resource_index:
            self._resource_col.create_index('resource_id')
            self._resource_update_col.create_index([
                ('resource', pymongo.DESCENDING),
                ('time', pymongo.DESCENDING)
            ])
            self._resource_index = True

    def _create_datum_index(self):
        if not self._datum_index:
            self._datum_col.create_index('datum_id', unique=True)
            self._datum_col.create_index('resource')
            self._datum_index = True

    @property
    def _db(self):
        if self.__db is None:
            self.__db = _get_mongo_database(self.config)
            if self.version > 0:
                sentinel = self.__db.get_collection('sentinel')
                versioned_collection = ['resource', 'datum']
                for col_name in versioned_collection:
                    val = sentinel.find_one({'collection': col_name})
                    if val is None:
                        raise RuntimeError('there is no version sentinel for '
                                           'the {} collection'.format(col_name)
                                           )
                    if val['version'] != self.version:
                        raise RuntimeError('DB version {!r} does not match'
                                           'API version of FS {} for the '
                                           '{} collection'.format(
                                               val, self.version, col_name))
        return self.__db

    @property
    def _resource_col(self):
        if self.__res_col is None:
            self.__res_col = self._db.get_collection('resource')
        return self.__res_col

    @property
    def _resource_update_col(self):
        if self.__res_update_col is None:
            self.__res_update_col = self._db.get_collection('resource_update')
        return self.__res_update_col

    @property
    def _datum_col(self):
        if self.__datum_col is None:
            self.__datum_col = self._db.get_collection('datum')
        return self.__datum_col

    @property
    def _connection(self):
        return self._db.client

    @property
    def DuplicateKeyError(self):
        return self._api.DuplicateKeyError


class Registry(RegistryRO, RegistryTemplate):
    """Registry object that knows how to create new documents.
       Overriding base class methods to make index creation more lazy.
       This allows databroker to work with a read only mongo account.
    """
    def insert_datum(self, resource, datum_id, datum_kwargs,
                     ignore_duplicate_error=False):
        self._create_datum_index()
        return super().insert_datum(resource, datum_id, datum_kwargs,
                                    ignore_duplicate_error=ignore_duplicate_error)

    def bulk_insert_datum(self, resource, datum_ids, datum_kwarg_list):
        self._create_datum_index()
        return super().bulk_insert_datum(resource, datum_ids, datum_kwarg_list)

    def bulk_register_datum_table(self, resource_uid, dkwargs_table,
                                  validate=False):
        self._create_datum_index()
        return super().bulk_register_datum_table(resource_uid, dkwargs_table,
                                                 validate=validate)

    def bulk_register_datum_list(self, resource_uid, dkwargs_list,
                                 validate=False):
        self._create_datum_index()
        return super().bulk_register_datum_list(resource_uid, dkwargs_list,
                                                validate=validate)

    def register_datum(self, resource_uid, datum_kwargs, validate=False):
        self._create_datum_index()
        return super().register_datum(resource_uid, datum_kwargs,
                                      validate=validate)

    def register_resource(self, spec, root, rpath, rkwargs,
                          path_semantics='posix', run_start=None):
        self._create_resource_index()
        return super().register_resource(spec, root, rpath, rkwargs,
                                         path_semantics=path_semantics,
                                         run_start=run_start)

    def insert_resource(self, spec, resource_path, resource_kwargs, root=None,
                        path_semantics='posix', uid=None, run_start=None,
                        id=None, ignore_duplicate_error=False):
        self._create_resource_index()
        return super().insert_resource(spec, resource_path, resource_kwargs, root=root,
                                       path_semantics=path_semantics, uid=uid, run_start=run_start,
                                       id=id, ignore_duplicate_error=ignore_duplicate_error)


class RegistryMoving(Registry, RegistryMovingTemplate):
    '''Registry object that knows how to move files.'''
