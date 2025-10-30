"""
This module vendors the Dispatcher from bluesky.run_engine, in order
to avoid a bluesky dependency, since bluesky-tiled-plugins is frequently
used in data analysis environments where a bluesky dependency would be
superfluous.

This dispatcher could in the future be move upstream to event_model
where it could be shared by bluesky and bluesky-tiled-plugins.

That code has been extremely stable for about ten years, so divergence
is not a pressing concern.
"""
import sys
import types
from itertools import count
from warnings import warn
from weakref import WeakKeyDictionary, ref

from event_model import DocumentNames


class Dispatcher:
    """Dispatch documents to user-defined consumers on the main thread."""

    def __init__(self):
        self.cb_registry = CallbackRegistry(allowed_sigs=DocumentNames)
        self._counter = count()
        self._token_mapping = dict()  # noqa: C408

    def process(self, name, doc):
        """
        Dispatch document ``doc`` of type ``name`` to the callback registry.

        Parameters
        ----------
        name : {'start', 'descriptor', 'event', 'stop'}
        doc : dict
        """
        exceptions = self.cb_registry.process(name, name.name, doc)
        for exc, traceback in exceptions:  # noqa: B007
            warn(  # noqa: B028
                "A %r was raised during the processing of a %s "  # noqa: UP031
                "Document. The error will be ignored to avoid "
                "interrupting data collection. To investigate, "
                "set RunEngine.ignore_callback_exceptions = False "
                "and run again." % (exc, name.name)
            )

    def subscribe(self, func, name="all"):
        """
        Register a callback function to consume documents.

        .. versionchanged :: 0.10.0
            The order of the arguments was swapped and the ``name``
            argument has been given a default value, ``'all'``. Because the
            meaning of the arguments is unambiguous (they must be a callable
            and a string, respectively) the old order will be supported
            indefinitely, with a warning.

        .. versionchanged :: 0.10.0
            The order of the arguments was swapped and the ``name``
            argument has been given a default value, ``'all'``. Because the
            meaning of the arguments is unambiguous (they must be a callable
            and a string, respectively) the old order will be supported
            indefinitely, with a warning.

        Parameters
        ----------
        func: callable
            expecting signature like ``f(name, document)``
            where name is a string and document is a dict
        name : {'all', 'start', 'descriptor', 'event', 'stop'}, optional
            the type of document this function should receive ('all' by
            default).

        Returns
        -------
        token : int
            an integer ID that can be used to unsubscribe

        See Also
        --------
        :meth:`Dispatcher.unsubscribe`
            an integer token that can be used to unsubscribe
        """
        if callable(name) and isinstance(func, str):
            name, func = func, name
            warn(  # noqa: B028
                "The order of the arguments has been changed. Because the "
                "meaning of the arguments is unambiguous, the old usage will "
                "continue to work indefinitely, but the new usage is "
                "encouraged: call subscribe(func, name) instead of "
                "subscribe(name, func). Additionally, the 'name' argument "
                "has become optional. Its default value is 'all'."
            )
        if name == "all":
            private_tokens = []
            for key in DocumentNames:
                private_tokens.append(self.cb_registry.connect(key, func))
            public_token = next(self._counter)
            self._token_mapping[public_token] = private_tokens
            return public_token

        name = DocumentNames[name]
        private_token = self.cb_registry.connect(name, func)
        public_token = next(self._counter)
        self._token_mapping[public_token] = [private_token]
        return public_token

    def unsubscribe(self, token):
        """
        Unregister a callback function using its integer ID.

        Parameters
        ----------
        token : int
            the integer ID issued by :meth:`Dispatcher.subscribe`

        See Also
        --------
        :meth:`Dispatcher.subscribe`
        """
        for private_token in self._token_mapping.pop(token, []):
            self.cb_registry.disconnect(private_token)

    def unsubscribe_all(self):
        """Unregister all callbacks from the dispatcher."""
        for public_token in list(self._token_mapping.keys()):
            self.unsubscribe(public_token)

    @property
    def ignore_exceptions(self):
        return self.cb_registry.ignore_exceptions

    @ignore_exceptions.setter
    def ignore_exceptions(self, val):
        self.cb_registry.ignore_exceptions = val


class CallbackRegistry:
    """
    See matplotlib.cbook.CallbackRegistry. This is a simplified since
    ``bluesky`` is python3.4+ only!
    """

    def __init__(self, ignore_exceptions=False, allowed_sigs=None):
        self.ignore_exceptions = ignore_exceptions
        self.allowed_sigs = allowed_sigs
        self.callbacks = dict()  # noqa: C408
        self._cid = 0
        self._func_cid_map = {}

    def __getstate__(self):
        # We cannot currently pickle the callables in the registry, so
        # return an empty dictionary.
        return {}

    def __setstate__(self, state):
        # re-initialise an empty callback registry
        self.__init__()

    def connect(self, sig, func):
        """Register ``func`` to be called when ``sig`` is generated

        Parameters
        ----------
        sig
        func

        Returns
        -------
        cid : int
            The callback index. To be used with ``disconnect`` to deregister
            ``func`` so that it will no longer be called when ``sig`` is
            generated
        """
        if self.allowed_sigs is not None:
            if sig not in self.allowed_sigs:
                raise ValueError(f"Allowed signals are {self.allowed_sigs}")
        self._func_cid_map.setdefault(sig, WeakKeyDictionary())
        # Note proxy not needed in python 3.
        # TODO rewrite this when support for python2.x gets dropped.
        # Following discussion with TC: weakref.WeakMethod can not be used to
        #   replace the custom 'BoundMethodProxy', because it does not accept
        #   the 'destroy callback' as a parameter. The 'destroy callback' is
        #   necessary to automatically unsubscribe CB registry from the callback
        #   when the class object is destroyed and this is the main purpose of
        #   BoundMethodProxy.
        proxy = _BoundMethodProxy(func)
        if proxy in self._func_cid_map[sig]:
            return self._func_cid_map[sig][proxy]

        proxy.add_destroy_callback(self._remove_proxy)
        self._cid += 1
        cid = self._cid
        self._func_cid_map[sig][proxy] = cid
        self.callbacks.setdefault(sig, dict())  # noqa: C408
        self.callbacks[sig][cid] = proxy
        return cid

    def _remove_proxy(self, proxy):
        # need the list because `del self._func_cid_map[sig]` mutates the dict
        for sig, proxies in list(self._func_cid_map.items()):
            try:
                # Here we need to delete the last reference to proxy (in 'self.callbacks[sig]')
                #   The respective entries in 'self._func_cid_map' are deleted automatically,
                #   since 'self._func_cid_map[sig]' entries are WeakKeyDictionary objects.
                del self.callbacks[sig][proxies[proxy]]
            except KeyError:
                pass

            # Remove dictionary items for signals with no assigned callbacks
            if len(self.callbacks[sig]) == 0:
                del self.callbacks[sig]
                del self._func_cid_map[sig]

    def disconnect(self, cid):
        """Disconnect the callback registered with callback id *cid*

        Parameters
        ----------
        cid : int
            The callback index and return value from ``connect``
        """
        for eventname, callbackd in self.callbacks.items():  # noqa: B007
            try:
                # This may or may not remove entries in 'self._func_cid_map'.
                del callbackd[cid]
            except KeyError:
                continue
            else:
                # Look for cid in 'self._func_cid_map' as well. It may still be there.
                for sig, functions in self._func_cid_map.items():  # noqa: B007
                    for function, value in list(functions.items()):
                        if value == cid:
                            del functions[function]
                return

    def process(self, sig, *args, **kwargs):
        """Process ``sig``

        All of the functions registered to receive callbacks on ``sig``
        will be called with ``args`` and ``kwargs``

        Parameters
        ----------
        sig
        args
        kwargs
        """
        if self.allowed_sigs is not None:
            if sig not in self.allowed_sigs:
                raise ValueError(f"Allowed signals are {self.allowed_sigs}")
        exceptions = []
        if sig in self.callbacks:
            for cid, func in list(self.callbacks[sig].items()):  # noqa: B007
                try:
                    func(*args, **kwargs)
                except ReferenceError:
                    self._remove_proxy(func)
                except Exception as e:
                    if self.ignore_exceptions:
                        exceptions.append((e, sys.exc_info()[2]))
                    else:
                        raise
        return exceptions


class _BoundMethodProxy:
    """
    Our own proxy object which enables weak references to bound and unbound
    methods and arbitrary callables. Pulls information about the function,
    class, and instance out of a bound method. Stores a weak reference to the
    instance to support garbage collection.
    @organization: IBM Corporation
    @copyright: Copyright (c) 2005, 2006 IBM Corporation
    @license: The BSD License
    Minor bugfixes by Michael Droettboom
    """

    def __init__(self, cb):
        self._hash = hash(cb)
        self._destroy_callbacks = []
        try:
            # This branch is successful if 'cb' bound method and class method,
            #   but destroy_callback mechanism works only for bound methods,
            #   since cb.__self__ points to class instance only for
            #   bound methods, not for class methods. Therefore destroy_callback
            #   will not be called for class methods.
            try:
                self.inst = ref(cb.__self__, self._destroy)
            except TypeError:
                self.inst = None
            self.func = cb.__func__
            self.klass = cb.__self__.__class__

        except AttributeError:
            # 'cb' is a function, callable object or static method.
            # No weak reference is created, strong reference is stored instead.
            self.inst = None
            self.func = cb
            self.klass = None

    def add_destroy_callback(self, callback):
        self._destroy_callbacks.append(_BoundMethodProxy(callback))

    def _destroy(self, wk):
        for callback in self._destroy_callbacks:
            try:
                callback(self)
            except ReferenceError:
                pass

    def __getstate__(self):
        d = self.__dict__.copy()
        # de-weak reference inst
        inst = d["inst"]
        if inst is not None:
            d["inst"] = inst()
        return d

    def __setstate__(self, statedict):
        self.__dict__ = statedict
        inst = statedict["inst"]
        # turn inst back into a weakref
        if inst is not None:
            self.inst = ref(inst)

    def __call__(self, *args, **kwargs):
        """
        Proxy for a call to the weak referenced object. Take
        arbitrary params to pass to the callable.
        Raises `ReferenceError`: When the weak reference refers to
        a dead object
        """
        if self.inst is not None and self.inst() is None:
            raise ReferenceError
        elif self.inst is not None:
            # build a new instance method with a strong reference to the
            # instance

            mtd = types.MethodType(self.func, self.inst())

        else:
            # not a bound method, just return the func
            mtd = self.func
        # invoke the callable and return the result
        return mtd(*args, **kwargs)

    def __eq__(self, other):
        """
        Compare the held function and instance with that held by
        another proxy.
        """
        try:
            if self.inst is None:
                return self.func == other.func and other.inst is None
            else:
                return self.func == other.func and self.inst() == other.inst()
        except Exception:
            return False

    def __ne__(self, other):
        """
        Inverse of __eq__.
        """
        return not self.__eq__(other)

    def __hash__(self):
        return self._hash
