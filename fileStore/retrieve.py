from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six
import logging
logger = logging.getLogger(__name__)

from . import (EID_KEY, SPEC_KEY, FID_KEY, FPATH_KEY,
               BASE_CUSTOM_KEY, EVENT_CUSTOM_KEY)


class HandlerBase(object):
    """
    Base-class for Handlers to provide the boiler plate to
    make them usable in context managers by provding stubs of
    ``__enter__``, ``__exit__`` and ``close``
    """
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        pass


class HandlerRegistry(dict):
    """
    Sub-class of dict to serve as a registry of available handlers.

    Doing this as a sub-class to give more readable API and to allow
    for more sophisticated validation on the way in later
    """
    def register_handler(self, key, handler):
        """
        Register a new handler

        Parameters
        ----------
        key : str
            Name of the spec as it will appear in the FS documents

        handler : callable
            This needs to be a callable which when called with the
            free parameters from the FS documents
        """
        if key in self:
            raise RuntimeError("You are trying to register a second handler "
                               "for spec {}, {}".format(key, self))

        self[key] = handler


# singleton module-level registry, not 100% with
# this design choice
_h_registry = HandlerRegistry()


def register_handler(key, handler):
    """
    connivance function to add handler to module-level handler
    registry so users don't have to know about the singleton
    """
    _h_registry.register_handler(key, handler)


def get_spec_handler(base_fs_document, handle_registry=None):
    """
    Given a document from the base FS collection return
    the proper Handler

    This should get memozied or shoved into a class eventually
    to minimize open/close thrashing.

    Parameters
    ----------
    base_fs_document : dict
        Required to have the key 'spec' and 'fileid', all others
        are blindly passed to the Handler

    handle_registry : HandleRegistry or dict, optional
        Mapping between spec <-> handler classes, if None, use
        module-level registry

    Returns
    -------
    fid : str
        A unique id for this resource

    handler : callable
        An object that when called with the values in the event
        document returns the externally stored data

    """

    if handle_registry is None:
        handle_registry = _h_registry

    fs_doc = dict(base_fs_document)
    spec = fs_doc.pop(SPEC_KEY)
    fid = fs_doc.pop(FID_KEY)
    kwargs = fs_doc.pop(BASE_CUSTOM_KEY)
    fpath = fs_doc.pop(FPATH_KEY)
    return fid, handle_registry[spec](fpath, **kwargs)


def get_data(events_fs_doc, get_handler_method):
    """
    Given a document from the events collection, get the externally
    stored data.

    This may get wrapped up in a class instance, not intended for public
    usage as-is

    Parameters
    ----------
    events_fs_doc : dict
        Document from the events collection

    get_handler_method : callable
        A function which takes a fid and returns a handler.  This should
        eventually be optional(?) and default to hitting the mongodb.
    """

    fs_doc = dict(events_fs_doc)
    eid = fs_doc.pop(EID_KEY)
    fid = fs_doc.pop(FID_KEY)
    kwargs = fs_doc.pop(EVENT_CUSTOM_KEY)
    handler = get_handler_method(fid)
    return eid, handler(**kwargs)
