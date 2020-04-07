from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six
from contextlib import contextmanager
import logging
import os.path
import shutil
import os
import boltons.cacheutils
from . import core
import warnings
from ..utils import ensure_path_exists
from pkg_resources import resource_filename
import json
from .utils import _ChainMap
from .handlers_base import DuplicateHandler


logger = logging.getLogger(__name__)


class BaseRegistryRO(object):
    """This is the base-class for asset registries.

    It is indented to be sub-classed by specific implementations that
    will proved their required collections.

    Most of the methods on this class are shims which defer to
    functions on `self._api` which provides the actual implementation.
    See `cls._API_MAP` for how to control this.

    This class provides the exception objects it will raise as
    attributes, this is to support raising custom Exception classes
    that mixin the underlying Exceptions from the database layer.

    This class provides the implementation of the handler registry.

    This class provides a set of caches for the documents and handlers.

    This class provides management of the 'root' and 'root_map'.
    """
    # ### Database implementation source
    # map to the underlying database implementation
    # this may change in the future to be done in a more standard way, but
    # doing it this ways allows run-time support for more than 1 version of the
    # underlying database layout.
    _API_MAP = {1: core}

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, val):
        if self._api is not None:
            raise RuntimeError("Can not change api version at runtime")
        self._api = self._API_MAP[val]
        self._version = val

    # ### Exceptions

    # An exception for use in registering handlers #
    DuplicateHandler = DuplicateHandler

    @property
    def DatumNotFound(self):
        return self._api.DatumNotFound

    # ### known schemas

    KNOWN_SPEC = dict()
    for spec_name in ['AD_HDF5', 'AD_SPE']:
        tmp_dict = {}
        base_name = 'schemas/'
        resource_name = '{}{}_resource.json'.format(base_name, spec_name)
        datum_name = '{}{}_datum.json'.format(base_name, spec_name)
        with open(resource_filename('databroker.assets',
                                    resource_name), 'r') as fin:
            tmp_dict['resource'] = json.load(fin)
        with open(resource_filename('databroker.assets',
                                    datum_name), 'r') as fin:
            tmp_dict['datum'] = json.load(fin)
        KNOWN_SPEC[spec_name] = tmp_dict

    # ## Configuration management

    # required configuration, sub-classes can over-ride this to do validation
    REQ_CONFIG = ()
    # optional configuration, mostly for documentation
    OPT_CONFIG = ()

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, config):
        if not all(k in config for k in self.REQ_CONFIG):
            raise RuntimeError('The provided config {c!r} must have {r} '
                               'keys and is missing {m}'.format(
                                   c=config,
                                   r=self.REQ_CONFIG,
                                   m=set(self.REQ_CONFIG) - set(config)))
        self._config = config

    def disconnect(self):
        pass

    def reconfigure(self, config):
        '''Reconfigure the Registry object

        This clears all the local caches and starts from scratch.

        Parameters
        ----------
        config : dict
        '''
        self.disconnect()
        self.clear_process_cache()
        self.config = config
        self._api = None
        self.version = config.get('version', 1)

    def clear_process_cache(self):
        self._datum_cache.clear()
        self._handler_cache.clear()
        self._resource_cache.clear()

    # ## INIT
    def __init__(self, config, handler_reg=None, root_map=None):
        # set up configuration + version
        self.config = config
        self._api = None
        self.version = config.get('version', 1)

        # set up the initial handler registry
        self.handler_reg = _ChainMap(handler_reg or {})

        # set up the initial root_map
        self.root_map = root_map or {}

        # ## set up the caches
        def _r_on_miss(k):
            col = self._resource_col
            ret = self._api.resource_given_uid(col, k)
            if ret is None:
                raise RuntimeError('did not find resource {!r}'.format(k))
            return ret

        self._datum_cache = boltons.cacheutils.LRU(max_size=1000000)
        self._handler_cache = boltons.cacheutils.LRU()
        self._resource_cache = boltons.cacheutils.LRU(on_miss=_r_on_miss)

        # copy the class level known spec to an instance attribute
        self.known_spec = dict(self.KNOWN_SPEC)

    # ## Rootmap
    def set_root_map(self, root_map):
        '''Set the root map

        Parameters
        ----------
        root_map : dict
            str -> str mapping to account for temporarily
            moved/copied/remounted files.  Any resources which have a
            ``root`` in ``root_map`` will have the resource path
            updated before being handed to the Handler in
            ``get_spec_handler``
        '''
        self.root_map = root_map

    # ## Hi-level API
    # Users typically should not need anything outside of these methods
    def retrieve(self, datum_id):
        return self._api.retrieve(self._datum_col, datum_id,
                                  self._datum_cache, self.get_spec_handler,
                                  logger)

    def get_datum(self, datum_id):
        warnings.warn('get_datum is deprecated, use retrieve instead',
                      stacklevel=2)
        return self.retrieve(datum_id)

    def register_handler(self, key, handler, overwrite=False):
        if (not overwrite) and (key in self.handler_reg):
            if self.handler_reg[key] is handler:
                return
            raise self.DuplicateHandler(
                "You are trying to register a second handler "
                "for spec {}, {}".format(key, self))

        self.deregister_handler(key)
        self.handler_reg[key] = handler

    def deregister_handler(self, key):
        handler = self.handler_reg.pop(key, None)
        if handler is not None:
            name = handler.__name__
            for k in list(self._handler_cache):
                if k[1] == name:
                    del self._handler_cache[k]

    @contextmanager
    def handler_context(self, temp_handlers):
        stash = self.handler_reg
        self.handler_reg = self.handler_reg.new_child(temp_handlers)
        try:
            yield self
        finally:
            popped_reg = self.handler_reg.maps[0]
            self.handler_reg = stash
            for handler in popped_reg.values():
                name = handler.__name__
                for k in list(self._handler_cache):
                    if k[1] == name:
                        del self._handler_cache[k]

    # ## Mid-level API (for internal use)
    # Do mapping between a resource document -> a usable Handler object
    def get_spec_handler(self, resource):
        """
        Given a document from the registry_template FS collection return
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
        resource = self._resource_cache[resource]

        h_cache = self._handler_cache

        spec = resource['spec']
        handler = self.handler_reg[spec]

        key = (str(resource['uid']), handler.__name__)

        try:
            return h_cache[key]
        except KeyError:
            pass

        kwargs = resource['resource_kwargs']
        rpath = resource['resource_path']
        root = resource.get('root', '')
        root = self.root_map.get(root, root)
        if root:
            rpath = os.path.join(root, rpath)
        ret = handler(rpath, **kwargs)
        h_cache[key] = ret
        return ret

    def get_file_list(self, resource_or_uid, datum_kwarg_gen):
        """Given a resource or resource uid and an iterable of datum kwargs,
        return filepaths.


        DO NOT USE FOR COPYING OR MOVING. This is for debugging only.
        See the methods for moving and copying on the Registry object.
        """
        actual_resource = self.resource_given_uid(resource_or_uid)
        return self._api.get_file_list(actual_resource, datum_kwarg_gen,
                                       self.get_spec_handler)

    def get_history(self, resource_uid):
        '''Generator of all updates to the given resource

        Parameters
        ----------
        resource_uid : Document or str
            The resource to get the history of

        Yields
        ------
        update : Document
        '''
        if self.version == 0:
            raise NotImplementedError('No history in v0 schema')

        for doc in self._api.get_resource_history(
                self._resource_update_col, resource_uid):
            yield doc

    # ## Lo-level API: Direct access to documents (for internal use)
    def resource_given_datum_id(self, datum_id):
        '''Given a datum datum_id return its Resource document
        '''
        if self.version == 0:
            raise NotImplementedError('V0 has no notion of root so can not '
                                      'change it so no need for this method')

        res = self._api.resource_given_datum_id(self._datum_col, datum_id,
                                           self._datum_cache, logger)
        return self._resource_cache[res]

    def resource_given_uid(self, uid):
        col = self._resource_col
        return self._api.resource_given_uid(col, uid)

    def datum_gen_given_resource(self, resource_or_uid):
        """Given resource or resource uid return associated datum documents.
        """
        actual_resource = self.resource_given_uid(resource_or_uid)
        datum_gen = self._api.get_datum_by_res_gen(self._datum_col,
                                                   actual_resource['uid'])
        return datum_gen

    def get_datum_from_datum_id(self, datum_id):
        return self._api._get_datum_from_datum_id(self._datum_col, datum_id,
                                  self._datum_cache, logger)

    # ## File-related API
    # This may move to a mix-in class or something
    def copy_files(self, resource_or_uid, new_root,
                   verify=False, file_rename_hook=None):
        """
        Copy files associated with a resource to a new directory.

        The registered handler must have a `get_file_list` method and the
        process running this method must have read/write access to both the
        source and destination file systems.

        This method does *not* update the assets dataregistry_template.

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
        `RegistryMoving.shift_root`
        `RegistryMoving.change_root`
        """
        if self.version == 0:
            raise NotImplementedError('V0 has no notion of root so can not '
                                      'change it')
        if verify:
            raise NotImplementedError('Verification is not implemented yet')

        def rename_hook_wrapper(hook):
            if hook is None:
                def noop(n, total, old_name, new_name):
                    return
                return noop

            def safe_hook(n, total, old_name, new_name):
                try:
                    hook(n, total, old_name, new_name)
                except:
                    pass
            return safe_hook

        file_rename_hook = rename_hook_wrapper(file_rename_hook)

        # get list of files
        resource = dict(self.resource_given_uid(resource_or_uid))

        datum_gen = self.datum_gen_given_resource(resource)
        datum_kwarg_gen = (datum['datum_kwargs'] for datum in datum_gen)
        file_list = self.get_file_list(resource, datum_kwarg_gen)

        # check that all files share the same root
        old_root = resource.get('root')
        if not old_root:
            warnings.warn("There is no 'root' in this resource which "
                          "is required to be able to change the root. "
                          "Please use `fs.shift_root` to move some of "
                          "the path from the 'resource_path' to the "
                          "'root'.  For now assuming '/' as root")
            old_root = os.path.sep

        for f in file_list:
            if not f.startswith(old_root):
                raise RuntimeError('something is very wrong, the files '
                                   'do not all share the same root, ABORT')

        # sort out where new files should go
        new_file_list = [os.path.join(new_root,
                                      os.path.relpath(f, old_root))
                         for f in file_list]
        N = len(new_file_list)
        # copy the files to the new location
        for n, (fin, fout) in enumerate(zip(file_list, new_file_list)):
            # copy files
            file_rename_hook(n, N, fin, fout)
            ensure_path_exists(os.path.dirname(fout))
            shutil.copy2(fin, fout)

        return zip(file_list, new_file_list)


class RegistryTemplate(BaseRegistryRO):
    '''Registry object that knows how to create new documents.'''

    # ## Hi-level API: insertion
    def register_resource(self, spec, root, rpath, rkwargs,
                          path_semantics='posix',
                          run_start=None):
        '''Register a Resource with this Registry.

        Parameters
        ----------
        spec : str
            The type of asset being registered.

        root : Path or str
            The 'root' or non-semantic part of the rpath

        rpath : str
            Url to the physical location of this resource.
            Must start with 'root'

        rkwargs : dict
            resource_kwargs name/value pairs of additional kwargs to be
            passed to the handler to open this resource.

        path_semantics : {'posix', 'windows'}, optional
            The type of separator used in the path.

        Returns
        -------
        uid : str
            The uid of the created resource.

        '''
        if root is None:
            root = ''

        rs_kwarg = {}
        if run_start is not None:
            rs_kwarg['run_start'] = run_start

        col = self._resource_col

        return self._api.insert_resource(
            col, spec, rpath,
            rkwargs,
            self.known_spec,
            root=root,
            path_semantics=path_semantics,
            duplicate_exc=self.DuplicateKeyError,
            **rs_kwarg)['uid']

    def register_datum(self, resource_uid, datum_kwargs, validate=False):
        '''Register a datum with the Registry.

        Parameters
        ----------
        resource_uid : str
            The uid of the resource that this Datum is part of.

        datum_kwargs : Dict[str, Any]
            The dictionary of keyword arguments to call the Handler with
            to get this Datum back at retrieve time.

        validate : bool, optional
            If we should attempt to validate against a known spec.

            If `True` and we do not have a schema for the spec, fail.

        Returns
        -------
        uid : str
            Datum uid to put put into the Event data.

        '''
        if validate:
            raise RuntimeError('validate not implemented yet')
        col = self._datum_col

        return self._api.register_datum(col, resource_uid,
                                        datum_kwargs)

    def bulk_register_datum_list(self, resource_uid, dkwargs_list,
                                 validate=False):
        '''Bulk register datum with the registry

        Parameters
        ----------
        resource_uid : str
            The uid of the resource that this Datum is part of.

        dkwargs_list : list[Dict[str, Any]]
            A list of dictionaries of keyword arguments to call the
            Handler with to get this Datum back at retrieve time.

            All Datum will be associated with

        validate : bool, optional
            If we should attempt to validate against a known spec.

            If `True` and we do not have a schema for the spec, fail.

        Returns
        -------
        uids : iterable
            Datum uids to put put into the Event data for each
            entry

        '''
        if validate:
            raise RuntimeError('validate not implemented yet')
        col = self._datum_col

        return self._api.bulk_register_datum_list(col, resource_uid,
                                                  dkwargs_list)

    def bulk_register_datum_table(self, resource_uid, dkwargs_table,
                                  validate=False):
        '''Bulk register datum with the registry

        Parameters
        ----------
        resource_uid : str
            The uid of the resource that this Datum is part of.

        dkwargs_table : Dict[str, list] or pd.DataFrame
            A list of dictionaries of keyword arguments to call the
            Handler with to get this Datum back at retrieve time.

            All Datum will be associated with

        validate : bool, optional
            If we should attempt to validate against a known spec.

            If `True` and we do not have a schema for the spec, fail.

        Returns
        -------
        uids : iterable
            Datum uids to put put into the Event data for each
            entry

        '''
        if validate:
            raise RuntimeError('validate not implemented yet')
        return self._api.bulk_register_datum_table(
            self._datum_col,
            resource_uid, dkwargs_table,
            validate)

    # ## API for export
    def ingest_resource(self, resource_doc):
        pass

    def bulk_ingest_datum(self, resource_uid, datum_doc_iterable,
                          validate=False):
        pass

    # ## OLD API
    def insert_resource(self, spec, resource_path, resource_kwargs, root=None,
                        path_semantics='posix', uid=None, run_start=None,
                        id=None, ignore_duplicate_error=False):
        """Insert resource into a databroker.

        Parameters
        ----------
        spec : str
            The resource data spec
        resource_path : str
            The path to the resource files
        resource_kwargs : dict
            The kwargs for the resource
        root : str
            The root of the file path
        path_semantics : str, optional
            The name of the path semantics, e.g. ``posix`` for Linux systems
        uid : str, optional
            The unique ID for the resource
        run_start : str, optional
            The unique ID for the start document the resource is associated with
        id : str, optional
            Dummy variable so that we round trip resources, same as ``uid``

        Returns
        -------
        resource_object : dict
            The resource
        """
        if root is None:
            root = ''

        col = self._resource_col

        return self._api.insert_resource(
            col, spec, resource_path,
            resource_kwargs,
            self.known_spec,
            root=root,
            path_semantics=path_semantics,
            uid=uid,
            run_start=run_start,
            ignore_duplicate_error=ignore_duplicate_error,
            duplicate_exc=self.DuplicateKeyError)

    def insert_datum(self, resource, datum_id, datum_kwargs,
                     ignore_duplicate_error=False):
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

        return self._api.insert_datum(
            col, resource, datum_id, datum_kwargs,
            self.known_spec, self._resource_col,
            ignore_duplicate_error=ignore_duplicate_error,
            duplicate_exc=self.DuplicateKeyError)

    def bulk_insert_datum(self, resource, datum_ids, datum_kwarg_list):
        col = self._datum_col
        return self._api.bulk_insert_datum(col, resource, datum_ids,
                                           datum_kwarg_list)

    # ## Hi-level API: updates
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
                                   'the registry holds do not match '
                                   'yours: {!r} registry: {!r}'.format(
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

    def correct_root(self, resource_or_uid, new_root, verify=False):
        """Change the root of a resource

        This is for the case where externally we know that the files were moved
        but the registry records were not updated during the move. Eg data
        was transferred to another computer via ``rsync`` or ``scp``.

        Parameters
        ---------
        resource_or_uid : Document or str
            The resource to move the root of

        new_root : str
            The new 'root' to change the Registry element to

        verify : bool, optional (False)
            Verify that the move happened correctly.  This currently
            is not implemented and will raise if ``verify == True``.

        """

        if verify:
            raise NotImplementedError('verify is not implemented yet')

        resource = self.resource_given_uid(resource_or_uid)
        # update the dataregistry_template
        new_resource = dict(resource)
        new_resource['root'] = new_root

        update_col = self._resource_update_col
        resource_col = self._resource_col
        ret = self._api.update_resource(update_col, resource_col,
                                        old=resource,
                                        new=new_resource,
                                        cmd_kwargs=dict(
                                            verify=verify,
                                            new_root=new_root),
                                        cmd='correct_root')

        return ret


class RegistryMovingTemplate(RegistryTemplate):
    '''Registry object that knows how to move files.'''
    def move_files(self, resource_or_uid, new_root, remove_origin=True,
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
        `Registry.shift_root`


        .. Warning

           This will change documents in your data base, move files
           and possibly delete files.  Be sure you know what you are
           doing.

        '''
        resource = dict(self.resource_given_uid(resource_or_uid))

        try:
            file_lists = self.copy_files(resource, new_root, verify,
                                         file_rename_hook)
        except:
            # TODO clean up partially copied files if this fails
            raise
        try:
            updates = self.correct_root(resource, new_root, verify)
        except:
            # TODO clean up copied files if this fails
            raise

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

        return updates
