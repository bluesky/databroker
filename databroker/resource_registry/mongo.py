from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import logging
import os
import os.path
import shutil
from contextlib import contextmanager

import warnings
import boltons.cacheutils
import pymongo
import six
from pymongo import MongoClient

from . import mongo_core
from ..utils import _make_sure_path_exists

from .base import (_ChainMap, FileStoreTemplateRO,
                   FileStoreTemplate, FileStoreMovingTemplate)

logger = logging.getLogger(__name__)


class FileStoreRO(FileStoreTemplateRO):
    '''Base FileStore object that knows how to read the database.

    Parameters
    ----------
    config : dict
       Much have keys {'database', 'collection', 'host'} and may have a 'port'

    handler_reg : dict, optional
       Mapping between spec names and handler classes

    version : int, optional
        schema version of the database.
        Defaults to 1

    root_map : dict, optional
        str -> str mapping to account for temporarily moved/copied/remounted
        files.  Any resources which have a ``root`` in ``root_map``
        will have the resource path updated before being handed to the
        Handler in ``get_spec_handler``

    '''
    _API_MAP = {1: mongo_core}

    def __init__(self, config, handler_reg=None, version=1, root_map=None):
        self.config = config
        self._api = None
        self.version = version

        if handler_reg is None:
            handler_reg = {}
        self.handler_reg = _ChainMap(handler_reg)

        if root_map is None:
            root_map = {}
        self.root_map = root_map

        self._datum_cache = boltons.cacheutils.LRU(max_size=1000000)
        self._handler_cache = boltons.cacheutils.LRU()
        self._resource_cache = boltons.cacheutils.LRU(on_miss=self._r_on_miss)
        self.__db = None
        self.__conn = None
        self.__datum_col = None
        self.__res_col = None
        self.__res_update_col = None
        self.known_spec = dict(self.KNOWN_SPEC)

    def disconnect(self):
        self.__db = None
        self.__conn = None
        self.__datum_col = None
        self.__res_col = None

    def reconfigure(self, config):
        self.disconnect()
        self.config = config

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


class FileStore(FileStoreRO, FileStoreTemplate):
    '''FileStore object that knows how to create new documents.'''


class FileStoreMoving(FileStore, FileStoreMovingTemplate):
    '''FileStore object that knows how to move files.'''
