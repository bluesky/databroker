from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six
import logging
from contextlib import contextmanager
import boltons.cacheutils
from .odm_templates import Resource, Datum
from .fs import FileStore

# needed for API
from .handlers_base import HandlerBase, HandlerRegistry, DuplicateHandler

logger = logging.getLogger(__name__)


_FS_SINGLETON = FileStore({})
_DATUM_CACHE = _FS_SINGLETON._datum_cache
_HANDLER_CACHE = _FS_SINGLETON._handler_cache
_RESOURCE_CACHE = _FS_SINGLETON._resource_cache


class DatumNotFound(Datum.DoesNotExist):
    pass


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
    if handle_registry is None:
        handle_registry = _FS_SINGLETON.handler_reg

    resource = _RESOURCE_CACHE[resource]

    spec = resource['spec']
    handler = handle_registry[spec]
    key = (str(resource['_id']), handler.__name__)
    if key in _HANDLER_CACHE:
        return _HANDLER_CACHE[key]
    kwargs = resource['resource_kwargs']
    rpath = resource['resource_path']
    ret = handler(rpath, **kwargs)
    _HANDLER_CACHE[key] = ret
    return ret


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
    col = Datum._get_collection()

    return _get_data(col, eid, handle_registry)


def _get_data(col, eid, handle_registry):
    try:
        datum = _DATUM_CACHE[eid]
    except KeyError:
        keys = ['datum_kwargs', 'resource']
        # find the current document
        edoc = col.find_one({'datum_id': eid})
        if edoc is None:
            raise DatumNotFound(
                "No datum found with datum_id {!r}".format(eid))
        # save it for later
        datum = {k: edoc[k] for k in keys}

        res = edoc['resource']
        count = 0
        for dd in col.find({'resource': res}):
            count += 1
            d_id = dd['datum_id']
            if d_id not in _DATUM_CACHE:
                _DATUM_CACHE[d_id] = {k: dd[k] for k in keys}
        if count > _DATUM_CACHE.max_size:
            logger.warn("More datum in a resource than your "
                        "datum cache can hold.")

    handler = get_spec_handler(datum['resource'], handle_registry)
    return handler(**datum['datum_kwargs'])
