from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six
import logging
from contextlib import contextmanager

from .fs import FileStore

# needed for API
from .core import DatumNotFound
from .handlers_base import HandlerBase, HandlerRegistry, DuplicateHandler

from .conf import connection_config
logger = logging.getLogger(__name__)


_FS_SINGLETON = FileStore(connection_config)
_HANDLER_CACHE = _FS_SINGLETON._handler_cache


@contextmanager
def handler_context(temp_handlers):
    """
    Context manager for temporarily updating the global HandlerRegistry.
    This is an alternative to passing HandlerRegistry objects in
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


def get_spec_handler(resource, handle_registry=None):
    """
    Given a document from the base FS collection return
    the proper Handler

    This should get memozied or shoved into a class eventually
    to minimize open/close thrashing.

    Parameters
    ----------
    resource : ObjectId
        ObjectId of a resource document

    handle_registry : HandleRegistry or dict, optional
        Mapping between spec <-> handler classes, if None, use
        module-level registry

    Returns
    -------

    handler : callable
        An object that when called with the values in the event
        document returns the externally stored data

    """
    handle_registry = handle_registry if handle_registry is not None else {}
    with _FS_SINGLETON.handler_context(handle_registry) as fs:
        return fs.get_spec_handler(resource)


def get_data(eid, handle_registry=None):
    """
    Given a document from the events collection, get the externally
    stored data.

    This may get wrapped up in a class instance, not intended for public
    usage as-is

    Parameters
    ----------
    eid : str
        The resource ID (as stored in MDS)

    handle_registry : HandleRegistry or dict, optional
        Mapping between spec <-> handler classes, if None, use
        module-level registry

    Returns
    -------
    data : ndarray
        The data in ndarray form.
    """
    if handle_registry is None:
        handle_registry = {}
    with _FS_SINGLETON.handler_context(handle_registry) as fs:
        return fs.get_datum(eid)
