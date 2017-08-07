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
from .base import _ChainMap, FileStoreTemplateRO

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


class FileStore(FileStoreRO):
    '''FileStore object that knows how to create new documents.'''
    def insert_resource(self, spec, resource_path, resource_kwargs, root=None):
        '''
         Parameters
         ----------

         spec : str
             spec used to determine what handler to use to open this
             resource.

         resource_path : str or None
             Url to the physical location of this resource

         resource_kwargs : dict, optional
             resource_kwargs name/value pairs of additional kwargs to be
             passed to the handler to open this resource.

         root : str, optional
             The 'root' part of the resource path.


        '''
        if root is None:
            root = ''

        col = self._resource_col

        return self._api.insert_resource(col, spec, resource_path,
                                         resource_kwargs,
                                         self.known_spec,
                                         root=root)

    def insert_datum(self, resource, datum_id, datum_kwargs):
        '''insert a datum for the given resource

        Parameters
        ----------

        resource : Resource or Resource.id
            Resource object

        datum_id : str
            Unique identifier for this datum.  This is the value stored in
            metadatastore and is the value passed to `retrieve` to get
            the data back out.

        datum_kwargs : dict
            dict with any kwargs needed to retrieve this specific
            datum from the resource.

        '''
        col = self._datum_col

        return self._api.insert_datum(col, resource, datum_id, datum_kwargs,
                                      self.known_spec, self._resource_col)

    def bulk_insert_datum(self, resource, datum_ids, datum_kwarg_list):
        col = self._datum_col

        return self._api.bulk_insert_datum(col, resource, datum_ids,
                                           datum_kwarg_list)

    def shift_root(self, resource_or_uid, shift):
        '''Shift directory levels between root and resource_path

        This is useful because the root can be change via `change_root`.

        Parameters
        ----------
        resource_or_uid : Document or str
            The resource to change the root/resource_path allocation
            of absolute path.

        shift : int
            The amount to shift the split.  Positive numbers move more
            levels into the root and negative values move levels into
            the resource_path

        '''
        if self.version == 0:
            raise NotImplementedError('V0 has no notion of root')

        resource = self.resource_given_uid(resource_or_uid)

        def safe_join(inp):
            if not inp:
                return ''
            return os.path.join(*inp)
        actual_resource = self.resource_given_uid(resource)
        if not isinstance(resource, six.string_types):
            if dict(actual_resource) != dict(resource):
                raise RuntimeError('The resource you hold and the resource '
                                   'the data base holds do not match '
                                   'yours: {!r} db: {!r}'.format(
                                       resource, actual_resource))
        resource = dict(actual_resource)
        resource.setdefault('root', '')
        full_path = os.path.join(resource['root'], resource['resource_path'])
        abs_path = full_path and full_path[0] == os.sep
        root = [_ for _ in resource['root'].split(os.sep) if _]
        rpath = [_ for _ in resource['resource_path'].split(os.sep) if _]

        if shift > 0:
            # to the right
            if shift > len(rpath):
                raise RuntimeError('Asked to shift farther to right '
                                   '({}) than there are directories '
                                   'in the resource_path ({})'.format(
                                       shift, len(rpath)))
            new_root = safe_join(root + rpath[:shift])
            new_rpath = safe_join(rpath[shift:])
        else:
            # sometime to the left
            shift = len(root) + shift
            if shift < 0:
                raise RuntimeError('Asked to shift farther to left '
                                   '({}) than there are directories '
                                   'in the root ({})'.format(
                                       shift-len(root), len(root)))
            new_root = safe_join(root[:shift])
            new_rpath = safe_join((root[shift:] + rpath))
        if abs_path:
            new_root = os.sep + new_root

        new = dict(resource)
        new['root'] = new_root
        new['resource_path'] = new_rpath

        update_col = self._resource_update_col
        resource_col = self._resource_col
        return self._api.update_resource(update_col, resource_col,
                                         actual_resource, new,
                                         cmd_kwargs=dict(shift=shift),
                                         cmd='shift_root')


class FileStoreMoving(FileStore):
    '''FileStore object that knows how to move files.'''
    def change_root(self, resource_or_uid, new_root, remove_origin=True,
                    verify=False, file_rename_hook=None):
        '''Change the root directory of a given resource

        The registered handler must have a `get_file_list` method and the
        process running this method must have read/write access to both the
        source and destination file systems.

        Internally the resource level directory information is stored
        as two parts: the root and the resource_path.  The 'root' is
        the non-semantic component (typically a mount point) and the
        'resource_path' is the 'semantic' part of the file path.  For
        example, it is common to collect data into paths that look like
        ``/mnt/DATA/2016/04/28``.  In this case we could split this as
        ``/mnt/DATA`` as the 'root' and ``2016/04/28`` as the resource_path.



        Parameters
        ----------
        resource_or_uid : Document or str
            The resource to move the files of

        new_root : str
            The new 'root' to copy the files into

        remove_origin : bool, optional (True)
            If the source files should be removed

        verify : bool, optional (False)
            Verify that the move happened correctly.  This currently
            is not implemented and will raise if ``verify == True``.

        file_rename_hook : callable, optional
            If provided, must be a callable with signature ::

               def hook(file_counter, total_number, old_name, new_name):
                   pass

            This will be run in the inner loop of the file copy step and is
            run inside of an unconditional try/except block.

        See Also
        --------
        `FileStore.shift_root`


        .. Warning

           This will change documents in your data base, move files
           and possibly delete files.  Be sure you know what you are
           doing.

        '''
        resource = dict(self.resource_given_uid(resource_or_uid))

        file_lists = self.copy_files(resource, new_root, verify,
                                     file_rename_hook)

        # update the database
        new_resource = dict(resource)
        new_resource['root'] = new_root

        update_col = self._resource_update_col
        resource_col = self._resource_col
        ret = self._api.update_resource(update_col, resource_col,
                                        old=resource,
                                        new=new_resource,
                                        cmd_kwargs=dict(
                                            remove_origin=remove_origin,
                                            verify=verify,
                                            new_root=new_root),
                                        cmd='change_root')

        # remove original files
        if remove_origin:
            for f_old, _ in file_lists:
                os.unlink(f_old)

        # nuke caches
        uid = resource['uid']
        self._resource_cache.pop(uid, None)
        for k in list(self._handler_cache):
            if k[0] == uid:
                del self._handler_cache[k]

        return ret
