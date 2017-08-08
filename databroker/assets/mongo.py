from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import six   # noqa
import logging
import pymongo

from pymongo import MongoClient

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
    REQ_CONFIG = ('database', 'host')
    OPT_CONFIG = ('port',)

    def __init__(self, config, handler_reg=None, root_map=None):
        super(RegistryRO, self).__init__(config,
                                         handler_reg=handler_reg,
                                         root_map=root_map)
        self.__db = None
        self.__conn = None
        self.__datum_col = None
        self.__res_col = None
        self.__res_update_col = None

    def disconnect(self):
        self.__db = None
        self.__conn = None
        self.__datum_col = None
        self.__res_col = None

    @property
    def _db(self):
        if self.__db is None:
            conn = self._connection
            self.__db = conn.get_database(self.config['database'])
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
            self.__res_col.create_index('resource_id')

        return self.__res_col

    @property
    def _resource_update_col(self):
        if self.__res_update_col is None:
            self.__res_update_col = self._db.get_collection('resource_update')
            self.__res_update_col.create_index([
                ('resource', pymongo.DESCENDING),
                ('time', pymongo.DESCENDING)
            ])

        return self.__res_update_col

    @property
    def _datum_col(self):
        if self.__datum_col is None:
            self.__datum_col = self._db.get_collection('datum')
            self.__datum_col.create_index('datum_id', unique=True)
            self.__datum_col.create_index('resource')

        return self.__datum_col

    @property
    def _connection(self):
        if self.__conn is None:
            self.__conn = MongoClient(self.config['host'],
                                      self.config.get('port', None))
        return self.__conn

    @property
    def DuplicateKeyError(self):
        return self._api.DuplicateKeyError


class Registry(RegistryRO, RegistryTemplate):
    '''Registry object that knows how to create new documents.'''


class RegistryMoving(Registry, RegistryMovingTemplate):
    '''Registry object that knows how to move files.'''
