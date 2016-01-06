from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six
import logging


class HandlerBase(object):
    """
    Base-class for Handlers to provide the boiler plate to
    make them usable in context managers by provding stubs of
    ``__enter__``, ``__exit__`` and ``close``
    """
    specs = set()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        pass


class DuplicateHandler(RuntimeError):
    pass


class HandlerRegistry(dict):
    """
    Sub-class of dict to serve as a registry of available handlers.

    Doing this as a sub-class to give more readable API and to allow
    for more sophisticated validation on the way in later
    """
    def register_handler(self, key, handler, overwrite=False):
        """
        Register a new handler

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
        """
        if (not overwrite) and (key in self):
            if self[key] is handler:
                return
            raise DuplicateHandler(
                "You are trying to register a second handler "
                "for spec {}, {}".format(key, self))

        self[key] = handler

    def deregister_handler(self, key):
        """
        Remove a handler from the registry.  No-op to remove
        a non-existing key.

        Parameters
        ----------
        key : str
            The spec label to remove
        """
        return self.pop(key, None)
