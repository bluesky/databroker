from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import six
import logging
from contextlib import contextmanager

from .mongo import FileStore
from .conf import connection_config
from .core import DatumNotFound

logger = logging.getLogger(__name__)

VERSION = 1
_FS_SINGLETON = FileStore(connection_config, version=VERSION)


def db_disconnect():
    _FS_SINGLETON.disconnect()


def db_connect(database, host, port):
    _FS_SINGLETON.reconfigure(dict(database=database,
                                   host=host,
                                   port=port))
    assert _FS_SINGLETON.config['database'] == database
    return _FS_SINGLETON._connection


@contextmanager
def handler_context(temp_handlers):
    """
    Context manager for temporarily updating the global handler registry.
    This is an alternative to passing a registry in
    as a kwarg.  The global registry is returned to it's prior state
    after the context manager exits.

    Parameters
    ----------
    temp_handlers : dict
        spec_name : HandlerClass pairs.

    Examples
    --------
    To use a different handler for a call to `retrieve` use
    the context manager to add (and possibly over-ride existing
    handlers) temporarily:

       with handler_context({'syn-spec', SynHandler}):
           FS.retrieve(EID)

    """
    with _FS_SINGLETON.handler_context(temp_handlers) as fs:
        yield fs


def register_handler(key, handler, overwrite=False):
    """
    Register a handler to be associated with a specific file
    specification key.  This controls the dispatch to the
    Handler classes based on the `spec` key of the `Resource`
    documents.

    Parameters
    ----------
    key : str
        Name of the spec as it will appear in the FS documents

    handler : callable
        This needs to be a callable which when called with the
        free parameters from the FS documents

    overwrite : bool, optional
        If False, raise an exception when re-registering an
        existing key.  Default is False

    See Also
    --------
    `deregister_handler`

    """
    _FS_SINGLETON.register_handler(key, handler, overwrite)


def deregister_handler(key):
    """
    Remove handler to module-level handler

    Parameters
    ----------
    key : str
        The spec label to remove

    See Also
    --------
    `register_handler`

    """
    _FS_SINGLETON.deregister_handler(key)


def get_spec_handler(resource, handler_registry=None):
    """
    Given a document from the base FS collection return
    the proper Handler

    This should get memozied or shoved into a class eventually
    to minimize open/close thrashing.

    Parameters
    ----------
    resource : ObjectId
        ObjectId of a resource document

    handler_registry : HandleRegistry or dict, optional
        Mapping between spec <-> handler classes, if None, use
        module-level registry

    Returns
    -------

    handler : callable
        An object that when called with the values in the event
        document returns the externally stored data

    """
    handler_registry = handler_registry if handler_registry is not None else {}
    with _FS_SINGLETON.handler_context(handler_registry) as fs:
        return fs.get_spec_handler(resource)


def get_data(eid, handler_registry=None):
    """
    Given a document from the events collection, get the externally
    stored data.

    This may get wrapped up in a class instance, not intended for public
    usage as-is

    Parameters
    ----------
    eid : str
        The datum ID (as stored in MDS)

    handler_registry : HandleRegistry or dict, optional
        Mapping between spec <-> handler classes, if None, use
        module-level registry

    Returns
    -------
    data : ndarray
        The data in ndarray form.
    """
    if handler_registry is None:
        handler_registry = {}
    with _FS_SINGLETON.handler_context(handler_registry) as fs:
        return fs.retrieve(eid)


retrieve = get_data


def insert_resource(spec, resource_path, resource_kwargs=None, root=''):
    resource_kwargs = resource_kwargs if resource_kwargs is not None else {}
    return _FS_SINGLETON.insert_resource(spec, resource_path,
                                         resource_kwargs, root)

insert_resource.__doc__ = _FS_SINGLETON.insert_resource.__doc__


def insert_datum(resource, datum_id, datum_kwargs):
    """

    Parameters
    ----------

    resource : Resource or Resource.id
        Resource object

    datum_id : str
        Unique identifier for this datum.  This is the value stored in
        metadatastore and is the value passed to `retrieve` to get
        the data back out.

    datum_kwargs : dict
        dict with any kwargs needed to retrieve this specific datum from the
        resource.

    """
    return _FS_SINGLETON.insert_datum(resource, datum_id, datum_kwargs)


def bulk_insert_datum(resource, datum_ids, datum_kwarg_list):
    return _FS_SINGLETON.bulk_insert_datum(resource, datum_ids,
                                           datum_kwarg_list)


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
    return _FS_SINGLETON.set_root_map(root_map)
